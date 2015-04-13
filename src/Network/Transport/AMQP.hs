{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
{--|
  A Network Transport Layer for `distributed-process`
  based on AMQP and single-owner queues
--}

module Network.Transport.AMQP (
    createTransport
  , AMQPParameters(..)
  ) where

import Network.Transport.AMQP.Internal.Types

import qualified Network.AMQP as AMQP
import qualified Data.Text as T
import qualified Data.Set as Set
import Data.UUID.V4
import Data.List (foldl1')
import Data.UUID (toString, toWords)
import Data.Bits
import Data.IORef
import Data.Monoid
import qualified Data.Map.Strict as Map
import Data.ByteString (ByteString)
import Data.Foldable
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.String.Conv
import Data.Serialize
import Network.Transport
import Network.Transport.Internal (mapIOException, asyncWhenCancelled)
import Control.Concurrent (yield, threadDelay)
import Control.Concurrent.Async as Async
import Control.Concurrent.MVar
import Control.Monad
import Control.Exception
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)

import Lens.Family2

--------------------------------------------------------------------------------
-- Utility functions
--------------------------------------------------------------------------------

encode' :: AMQPMessage -> BL.ByteString
encode' = encodeLazy

--------------------------------------------------------------------------------
decode' :: AMQP.Message -> Either String AMQPMessage
decode' = decodeLazy . AMQP.msgBody

--------------------------------------------------------------------------------
onValidEndPoint :: LocalEndPoint -> (ValidLocalEndPointState -> IO ()) -> IO ()
onValidEndPoint lep f = withMVar (localState lep) $ \case
   LocalEndPointValid v -> f v
   _ -> return ()

--------------------------------------------------------------------------------
onValidRemote :: RemoteEndPoint -> (ValidRemoteEndPointState -> IO ()) -> IO ()
onValidRemote rep f = withMVar (remoteState rep) $ \case
  RemoteEndPointValid v -> f v
  _ -> return ()

--------------------------------------------------------------------------------
apiNewEndPoint :: AMQPInternalState
               -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint is@AMQPInternalState{..} = do
  try . asyncWhenCancelled closeEndPoint $ do
    let AMQPParameters{..} = istate_params
    modifyMVar istate_tstate $ \tst -> case tst of
      TransportClosed -> throwIO $ TransportError NewEndPointFailed "Transport is closed."
      TransportValid vst@(ValidTransportState cnn oldMap) -> do
        -- If a valid transport was found, create a new LocalEndPoint
        (ep@LocalEndPoint{..}, chan) <- endPointCreate (Map.size oldMap) is
        let newEp = EndPoint {
              receive       = readChan chan
            , address       = localAddress
            , connect       = apiConnect is ep
            , closeEndPoint = apiCloseEndPoint is ep
            , newMulticastGroup     = return . Left $ newMulticastGroupError
            , resolveMulticastGroup = return . Left . const resolveMulticastGroupError
            }
        return (TransportValid $ 
                 over tstateEndPoints (Map.insert localAddress ep) vst
               , newEp)
  where
    newMulticastGroupError =
      TransportError NewMulticastGroupUnsupported "Multicast not supported"
    resolveMulticastGroupError =
      TransportError ResolveMulticastGroupUnsupported "Multicast not supported"

-------------------------------------------------------------------------------
endPointCreate :: Int -> AMQPInternalState -> IO (LocalEndPoint, Chan Event)
endPointCreate newId is@AMQPInternalState{..} = do
  newChannel <- AMQP.openChannel (transportConnection istate_params)
  AMQP.addChannelExceptionHandler newChannel print
  uuid <- toS . toString <$> nextRandom
  -- Each new `EndPoint` has a new Rabbit queue. Think of it as a
  -- heavyweight connection. Subsequent connections are multiplexed
  -- using Rabbit's exchanges.

  let qName = maybe uuid toS (transportEndpoint istate_params)

  -- TODO: In case we do not randomise the endpoint name, there is the
  -- risk that creating 2 endpoints for a Transport with fixed address
  -- will cause the former to share the same queue.
  (ourEndPoint,_,_) <- AMQP.declareQueue newChannel $ AMQP.newQueue {
      AMQP.queueName = qName <> ":" <> T.pack (show newId)
      , AMQP.queuePassive = False
      , AMQP.queueDurable = False
      , AMQP.queueExclusive = False
      }
  ch <- newChan
  opened <- newIORef True
  let newState = ValidLocalEndPointState ch newChannel opened newCounter Map.empty
  st <- newMVar (LocalEndPointValid newState)
  let ep = LocalEndPoint (toAddress ourEndPoint) st
  startReceiver is ep
  return (ep, ch)


--------------------------------------------------------------------------------
toExchangeName :: EndPointAddress -> IO T.Text
toExchangeName eA = do
  uuid <- toString <$> nextRandom
  return $ fromAddress eA <> ":" <> T.pack uuid


--------------------------------------------------------------------------------
startReceiver :: AMQPInternalState -> LocalEndPoint -> IO ()
startReceiver tr@AMQPInternalState{..} lep@LocalEndPoint{..} = do
  -- TODO: Catch the exception thrown when the _localChannel will be shutdown.
  old <- readMVar localState
  case old of
    LocalEndPointValid ValidLocalEndPointState{..} -> do
      void $ AMQP.consumeMsgs _localChannel (fromAddress localAddress) AMQP.NoAck $ \(msg,_) -> do
        case decode' msg of
          Left _ -> return ()
          Right v@(MessageData cId rawMsg) -> do
              print v
              writeChan _localChan $ Received cId rawMsg
          Right v@(MessageConnect theirAddress) -> do
              print v
              void $ createOrGetRemoteEndPoint tr lep theirAddress
          Right v@(MessageInitConnection theirAddress theirId rel) -> do
              print v
              join $ modifyMVar localState $ \case
                LocalEndPointValid vst@ValidLocalEndPointState{..} -> do
                    case Map.lookup theirAddress _localRemotes of
                      Nothing -> return (LocalEndPointValid vst
                                        , throwIO $ InvariantViolated (RemoteEndPointLookupFailed theirAddress))
                      Just rep -> modifyMVar (remoteState rep) $ \case
                        RemoteEndPointFailed ->
                          throwIO $ InvariantViolated (RemoteEndPointMustBeValid theirAddress)
                        RemoteEndPointClosed ->
                          throwIO $ InvariantViolated (RemoteEndPointMustBeValid theirAddress)
                        RemoteEndPointClosing{} ->
                          throwIO $ InvariantViolated (RemoteEndPointMustBeValid theirAddress)
                        w@RemoteEndPointValid{} -> do
                          conn <- AMQPConnection <$> pure lep
                                                 <*> pure rep
                                                 <*> pure rel
                                                 <*> newMVar (AMQPConnectionValid $ ValidAMQPConnection Nothing i)
                                                 <*> newEmptyMVar
                          w' <- registerRemoteEndPoint (succ i) w
                          return ( w'
                                 , ( LocalEndPointValid (set localConnections (Counter (succ i) (Map.insert (succ i) conn m)) vst)
                                   , return ())
                                 )
                        z@(RemoteEndPointPending w) -> do
                          conn <- AMQPConnection <$> pure lep
                                                 <*> pure rep
                                                 <*> pure rel
                                                 <*> newMVar (AMQPConnectionValid $ ValidAMQPConnection Nothing i)
                                                 <*> newEmptyMVar
                          modifyIORef w (\xs -> (registerRemoteEndPoint (succ i))  : xs)
                          return ( z
                                 , ( LocalEndPointValid (set localConnections (Counter (succ i) (Map.insert (succ i) conn m)) vst)
                                   , return ())
                                 )

                  where
                    (Counter i m) = vst ^. localConnections
                _ -> throwIO $ InvariantViolated (LocalEndPointMustBeValid localAddress)
            where
              registerRemoteEndPoint :: ConnectionId -> RemoteEndPointState -> IO RemoteEndPointState
              registerRemoteEndPoint i RemoteEndPointFailed = do
                writeChan _localChan (ConnectionOpened i rel theirAddress)
                writeChan _localChan (ConnectionClosed i)
                return RemoteEndPointFailed
              registerRemoteEndPoint i RemoteEndPointClosed = do
                writeChan _localChan (ConnectionOpened i rel theirAddress)
                writeChan _localChan (ConnectionClosed i)
                return RemoteEndPointClosed
              registerRemoteEndPoint i (RemoteEndPointClosing x) = do
                writeChan _localChan (ConnectionOpened i rel theirAddress)
                writeChan _localChan (ConnectionClosed i)
                return $ RemoteEndPointClosing x
              registerRemoteEndPoint _ RemoteEndPointPending{} = 
                throwIO $ InvariantViolated $ RemoteEndPointCannotBePending (theirAddress)
              registerRemoteEndPoint i (RemoteEndPointValid v@(ValidRemoteEndPointState exg ch _ s _)) = do
                publish ch exg (MessageInitConnectionOk localAddress theirId i)
                writeChan _localChan (ConnectionOpened i rel theirAddress)
                return $ RemoteEndPointValid v { _remoteIncomingConnections = Set.insert i s }

          --
          -- MessageCloseConnection
          --
          Right v@(MessageCloseConnection idx) -> do
            print v
            join $ modifyMVar localState $ \case
              LocalEndPointValid v ->
                  case v ^. localConnectionAt idx of
                    Nothing  -> return (LocalEndPointValid v, return ())
                    Just conn -> do
                      old <- swapMVar (_connectionState conn) AMQPConnectionFailed
                      let setter = over (localConnections . cntValue) (Map.delete idx)
                      return ( LocalEndPointValid $ setter v
                             , case old of
                                 AMQPConnectionFailed -> return ()
                                 AMQPConnectionInit -> return  () -- throwIO InvariantViolation
                                 AMQPConnectionClosed -> return ()
                                 AMQPConnectionValid (ValidAMQPConnection{}) -> do
                                    writeChan _localChan (ConnectionClosed idx)
                                    connectionCleanup (_connectionRemoteEndPoint conn) idx)
              LocalEndPointClosed -> return (LocalEndPointClosed, return ())

          --
          -- MessageInitConnectionOk
          --
          Right v@(MessageInitConnectionOk theirAddress ourId theirId) -> do
            join $ withMVar localState $ \case
              LocalEndPointValid v -> 
                case v ^. localRemoteAt theirAddress of
                  Nothing  -> return (return ()) -- TODO: send message to the host
                  Just rep -> modifyMVar (remoteState rep) $ \case
                    RemoteEndPointFailed -> return (RemoteEndPointFailed, return ())
                    RemoteEndPointClosed -> 
                      throwIO $ InvariantViolated $ RemoteEndPointShouldBeValidOrClosed theirAddress
                    RemoteEndPointClosing{} -> 
                      throwIO $ InvariantViolated $ RemoteEndPointShouldBeValidOrClosed theirAddress
                    t@(RemoteEndPointValid (ValidRemoteEndPointState exg ch (Counter x m) s z)) -> return $
                      case ourId `Map.lookup` m of
                          Nothing -> (t, return ()) -- TODO: send message to the hostv
                          Just c  ->
                            (RemoteEndPointValid (ValidRemoteEndPointState exg ch (Counter x (ourId `Map.delete` m)) s (z+1))
                            , do modifyMVar_ (_connectionState c) $ \case
                                   AMQPConnectionFailed -> return AMQPConnectionFailed
                                   AMQPConnectionInit -> return $ AMQPConnectionValid (ValidAMQPConnection (Just exg) theirId)
                                   AMQPConnectionClosed -> do
                                       publish ch exg (MessageCloseConnection theirId)
                                       return AMQPConnectionClosed
                                   AMQPConnectionValid _ -> 
                                     throwIO $ (IncorrectState "RemoteEndPoint should be closed")
                                 void $ tryPutMVar (_connectionReady c) ()
                            )
                    RemoteEndPointPending p -> 
                      return (RemoteEndPointPending p
                             , throwIO $ (IncorrectState "RemoteEndPoint should be closed"))
              LocalEndPointClosed -> return $ return ()

          --
          -- MessageEndPointClose
          --
          Right v@(MessageEndPointClose theirAddress True) -> do
            print v
            getRemoteEndPoint lep theirAddress >>=
              traverse_ (\rep -> do
                onValidRemote rep $ \v@ValidRemoteEndPointState{..} ->
                  publish _localChannel _remoteExchange (MessageEndPointCloseOk localAddress)
                remoteEndPointClose True lep rep)

          Right v@(MessageEndPointClose theirAddress False) -> do
            print v
            getRemoteEndPoint lep theirAddress >>=
              traverse_ (\rep -> do
                mst <- cleanupRemoteEndPoint lep rep Nothing
                case mst of
                  Nothing -> return ()
                  Just st -> onValidEndPoint lep $ \vst -> do
                    writeChan (vst ^. localChan) $
                        ErrorEvent $ TransportError (EventConnectionLost theirAddress) "Exception on remote side"
                    closeRemoteEndPoint lep rep st)

          --
          -- MessageEndPointCloseOk
          --
          Right v@(MessageEndPointCloseOk theirAddress) -> do
            print v
            getRemoteEndPoint lep theirAddress >>=
              traverse_ (\rep -> do
                state <- swapMVar (remoteState rep) RemoteEndPointClosed
                closeRemoteEndPoint lep rep state)

    _ -> throwIO $ TransportError NewEndPointFailed "startReceiver: Endpoint was closed"

--------------------------------------------------------------------------------
-- TODO: Not sure it's needed in AMQP.
-- | Close, all network connections.
closeRemoteEndPoint :: LocalEndPoint 
                    -> RemoteEndPoint 
                    -> RemoteEndPointState 
                    -> IO ()
closeRemoteEndPoint lep rep state = step1 >> step2 state
  where
   step1 = modifyMVar_ (localState lep) $ \case
     LocalEndPointValid v -> return $
       LocalEndPointValid (over localRemotes (Map.delete (remoteAddress rep)) v)
     c -> return c
   step2 (RemoteEndPointValid v) = AMQP.closeChannel (v ^. remoteChannel)
   step2 (RemoteEndPointClosing (ClosingRemoteEndPoint (AMQPExchange exg) ch rd)) = do
     _ <- readMVar rd
     AMQP.deleteExchange ch exg
   step2 _ = return ()


--------------------------------------------------------------------------------
connectionCleanup :: RemoteEndPoint -> ConnectionId -> IO ()
connectionCleanup rep cid = modifyMVar_ (remoteState rep) $ \case
   RemoteEndPointValid v -> return $
     RemoteEndPointValid $ over remoteIncomingConnections (Set.delete cid) v
   c -> return c

--------------------------------------------------------------------------------
-- | Asynchronous operation, shutdown of the remote end point may take a while
apiCloseEndPoint :: AMQPInternalState
                 -> LocalEndPoint
                 -> IO ()
apiCloseEndPoint tr@AMQPInternalState{..} lep@LocalEndPoint{..} = do
    -- we don't close endpoint here because other threads,
    -- should be able to access local endpoint state
    old <- readMVar localState
    case old of
      LocalEndPointValid ValidLocalEndPointState{..} -> do
        -- close channel, no events will be received
        writeChan _localChan EndPointClosed
        let queue = fromAddress localAddress
        _ <- AMQP.deleteQueue _localChannel queue
        AMQP.closeChannel _localChannel
      LocalEndPointClosed -> return ()

    void $ swapMVar localState LocalEndPointClosed
    modifyMVar_ istate_tstate $ \case
      TransportClosed  -> return TransportClosed
      TransportValid v -> return
        $ TransportValid (over tstateEndPoints (Map.delete localAddress) v)

--------------------------------------------------------------------------------
toAddress :: T.Text -> EndPointAddress
toAddress = EndPointAddress . toS

--------------------------------------------------------------------------------
fromAddress :: EndPointAddress -> T.Text
fromAddress = toS . endPointAddressToByteString

--------------------------------------------------------------------------------
apiConnect :: AMQPInternalState
           -> LocalEndPoint
           -> EndPointAddress  -- ^ Remote address
           -> Reliability      -- ^ Reliability (ignored)
           -> ConnectHints     -- ^ Hints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect is@AMQPInternalState{..} lep@LocalEndPoint{..} theirAddress reliability _ = do
  print "Inside apiConnect"
  let ourAddress = localAddress
  lst <- readMVar localState
  case lst of
    LocalEndPointValid v@ValidLocalEndPointState{..} -> do
      mapIOException (TransportError ConnectFailed . show) $ do
        eRep <- createOrGetRemoteEndPoint is lep theirAddress
        case eRep of
          Left _ -> return $ Left $ TransportError ConnectFailed "LocalEndPoint is closed."
          Right rep -> do
            print "apiConnect: creating connection"
            conn <- AMQPConnection <$> pure lep
                                   <*> pure rep
                                   <*> pure reliability
                                   <*> newMVar AMQPConnectionInit
                                   <*> newEmptyMVar
            let apiConn = Connection
                  { send = apiSend _localChannel conn
                  , close = apiClose conn
                  }
            join $ modifyMVar (remoteState rep) $ \w -> case w of
              RemoteEndPointClosed -> do
                return ( RemoteEndPointClosed
                       , return $ Left $ TransportError ConnectFailed "Transport is closed.")
              RemoteEndPointClosing x -> do
                return ( RemoteEndPointClosing x
                       , return $ Left $ TransportError ConnectFailed "RemoteEndPoint closed.")
              RemoteEndPointValid _ -> do
                print "Performing handshake with remote"
                newState <- handshake conn w
                return (newState, waitReady conn apiConn)
              RemoteEndPointPending z -> do
                modifyIORef z (\zs -> handshake conn : zs)
                return ( RemoteEndPointPending z, waitReady conn apiConn)
              RemoteEndPointFailed ->
                return ( RemoteEndPointFailed
                       , return $ Left $ TransportError ConnectFailed "RemoteEndPoint failed.")
    _ -> throwIO $ TransportError ConnectFailed "apiConnect: LocalEndPointClosed"
  where
    waitReady conn apiConn = join $ withMVar (_connectionState conn) $ \case
      AMQPConnectionInit{}   -> return $ yield >> waitReady conn apiConn
      AMQPConnectionValid{}  -> afterP $ Right apiConn
      AMQPConnectionFailed{} -> afterP $ Left $ TransportError ConnectFailed "Connection failed."
      AMQPConnectionClosed{} -> afterP $ Left $ TransportError ConnectFailed "Connection closed"
    handshake _ RemoteEndPointClosed      = return RemoteEndPointClosed
    handshake _ RemoteEndPointPending{}   = 
      throwIO $ TransportError ConnectFailed "Connection pending."
    handshake _ (RemoteEndPointClosing x) = return $ RemoteEndPointClosing x
    handshake _ RemoteEndPointFailed      = return RemoteEndPointFailed
    handshake conn (RemoteEndPointValid (ValidRemoteEndPointState exg ch (Counter i m) s z)) = do
        print $ "inside handshake: " ++ show exg
        publish ch exg (MessageInitConnection localAddress i' reliability)
        return $ RemoteEndPointValid (ValidRemoteEndPointState exg ch (Counter i' (Map.insert i' conn m)) s z)
      where i' = succ i

--------------------------------------------------------------------------------
getRemoteEndPoint :: LocalEndPoint -> EndPointAddress -> IO (Maybe RemoteEndPoint)
getRemoteEndPoint ourEp theirAddr = do
    withMVar (localState ourEp) $ \case
      LocalEndPointValid v -> return $ theirAddr `Map.lookup` _localRemotes v
      LocalEndPointClosed  -> return Nothing

--------------------------------------------------------------------------------
createOrGetRemoteEndPoint :: AMQPInternalState
                          -> LocalEndPoint
                          -> EndPointAddress
                          -> IO (Either InvariantViolated RemoteEndPoint)
createOrGetRemoteEndPoint is@AMQPInternalState{..} ourEp theirAddr = do
  print "Inside createOrGetRemoteEndPoint"
  join $ modifyMVar (localState ourEp) $ \case
    LocalEndPointValid v@ValidLocalEndPointState{..} -> do
      opened <- readIORef _localOpened
      if opened
      then do
        case v ^. localRemoteAt theirAddr of
          Nothing -> create v
          Just rep -> do
            withMVar (remoteState rep) $ \case
              RemoteEndPointFailed -> do
                  print $ "RemoteEndPointFailed: creating a new one"
                  create v
              _ -> return (LocalEndPointValid v, return $ Right rep)
      else return (LocalEndPointValid v, return $ Left $ IncorrectState "EndPointClosing")
    LocalEndPointClosed ->
      return  ( LocalEndPointClosed
              , return $ Left $ IncorrectState "EndPoint is closed"
              )
  where
    create v = do
      print "Inside create (new remote)"
      newChannel <- AMQP.openChannel (transportConnection istate_params)
      newExchange <- toExchangeName theirAddr
      print $ "New exchange: " <> newExchange
      AMQP.declareExchange newChannel $ AMQP.newExchange {
          AMQP.exchangeName = newExchange
          , AMQP.exchangeType = "direct"
          , AMQP.exchangePassive = False
          , AMQP.exchangeDurable = False
          , AMQP.exchangeAutoDelete = True --TODO: Be prepared to change this.
          }
      print $ "before binding to " <> (fromAddress theirAddr)
      AMQP.bindQueue newChannel (fromAddress theirAddr) newExchange mempty

      print $ "after binding to " <> (fromAddress theirAddr)
      state <- newMVar . RemoteEndPointPending =<< newIORef []
      opened <- newIORef False
      let rep = RemoteEndPoint theirAddr state opened
      return ( LocalEndPointValid 
             $ over localRemotes (Map.insert theirAddr rep) v
             , initialize newChannel (AMQPExchange newExchange) rep >> return (Right rep))

    ourAddr = localAddress ourEp

    initialize amqpChannel exg rep = do
      -- TODO: Shall I need to worry about AMQP Finalising my exchange?
      publish amqpChannel exg $ MessageConnect ourAddr
      let v = ValidRemoteEndPointState exg amqpChannel newCounter Set.empty 0
      print "Inside initialize, before modifyMVar_"
      modifyMVar_ (remoteState rep) $ \case
        RemoteEndPointPending p -> do
            z <- foldM (\y f -> f y) (RemoteEndPointValid v) . Prelude.reverse =<< readIORef p
            modifyIORef (remoteOpened rep) (const True)
            return z
        RemoteEndPointValid _   -> 
          throwIO $ InvariantViolated (RemoteEndPointMustBeValid theirAddr)
        RemoteEndPointClosed    -> return RemoteEndPointClosed
        RemoteEndPointClosing j -> return (RemoteEndPointClosing j)
        RemoteEndPointFailed    -> return RemoteEndPointFailed

--------------------------------------------------------------------------------
publish :: AMQP.Channel 
        -> AMQPExchange
        -> AMQPMessage 
        -> IO ()
publish chn (AMQPExchange e) msg = do
    AMQP.publishMsg chn e mempty --TODO: Do we need a routing key?
                    (AMQP.newMsg { AMQP.msgBody = encode' msg
                                 , AMQP.msgDeliveryMode = Just AMQP.NonPersistent
                                 })

--------------------------------------------------------------------------------
-- TODO: Deal with exceptions and error at the broker level.
apiSend :: AMQP.Channel
        -> AMQPConnection
        -> [ByteString] 
        -> IO (Either (TransportError SendErrorCode) ())
apiSend localChannel (AMQPConnection us them _ st _) m = try $ do
  msgs <- try $ mapM_ evaluate m
  case msgs of
    Left ex ->  do cleanup
                   throwIO $ TransportError SendFailed (show (ex::SomeException))
    -- TODO: Check that, in case of AMQP-raised exceptions, we are still
    -- doing the appropriate cleanup.
    Right b -> send_ m

  where
   send_ :: [ByteString] -> IO ()
   send_ msg = join $ withMVar (remoteState them) $ \x -> case x of
     RemoteEndPointPending{} -> 
       return $ yield >> (send_ msg)
     RemoteEndPointFailed -> 
       throwIO $ TransportError SendFailed "Remote end point is failed."
     RemoteEndPointClosed -> 
       throwIO $ TransportError SendFailed "Remote end point is closed."
     RemoteEndPointClosing{} -> 
       throwIO $ TransportError SendFailed "Remote end point is closing."
     RemoteEndPointValid v   -> withMVar st $ \case
       AMQPConnectionInit   -> return $ yield >> (send_ msg)
       AMQPConnectionClosed -> 
         throwIO $ TransportError SendClosed "Connection is closed"
       AMQPConnectionFailed -> 
         throwIO $ TransportError SendFailed "Connection is failed"
       AMQPConnectionValid (ValidAMQPConnection (Just exg) idx) -> do
         return $ publish localChannel exg (MessageData idx msg)
   cleanup = do
     void $ cleanupRemoteEndPoint us them
       (Just $ \v -> publish localChannel (_remoteExchange v) $ 
                     (MessageEndPointClose (localAddress us) False))
     onValidEndPoint us $ \v -> do
       writeChan (_localChan v) $ ErrorEvent $ TransportError
                 (EventConnectionLost (remoteAddress them)) "Exception on send."

--------------------------------------------------------------------------------
apiClose :: AMQPConnection -> IO ()
apiClose (AMQPConnection _ them _ st _) = do
  print "Inside API Close"
  join $ modifyMVar st $ \case
    AMQPConnectionValid (ValidAMQPConnection _ idx) -> do
      return (AMQPConnectionClosed, do
        modifyMVar_ (remoteState them) $ \case
          v@RemoteEndPointValid{}   -> notify idx v
          v@(RemoteEndPointPending p) -> modifyIORef p (\xs -> notify idx : xs) >> return v
          v -> return v
        )
    _ -> return (AMQPConnectionClosed, return ())
  where
    notify _ RemoteEndPointFailed    = return RemoteEndPointFailed
    notify _ (RemoteEndPointClosing x) = return $ RemoteEndPointClosing x
    notify _ RemoteEndPointClosed    = return RemoteEndPointClosed
    notify _ RemoteEndPointPending{} = 
      throwIO $ InvariantViolated (RemoteEndPointMustBeValid (remoteAddress them))
    notify idx w@(RemoteEndPointValid (ValidRemoteEndPointState exg ch _ _ _)) = do
      publish ch exg (MessageCloseConnection idx)
      return w

--------------------------------------------------------------------------------
-- | Close all endpoint connections, return previous state in case
-- if it was alive, for further cleanup actions.
cleanupRemoteEndPoint :: LocalEndPoint
                      -> RemoteEndPoint
                      -> (Maybe (ValidRemoteEndPointState -> IO ()))
                      -> IO (Maybe RemoteEndPointState)
cleanupRemoteEndPoint lep rep actions = modifyMVar (localState lep) $ \case
    LocalEndPointValid v -> do
      modifyIORef (remoteOpened rep) (const False)
      oldState <- swapMVar (remoteState rep) newState
      case oldState of
        RemoteEndPointValid w -> do
          let cn = w ^. (remotePendingConnections . cntValue)
          traverse_ (\c -> void $ swapMVar (_connectionState c) AMQPConnectionFailed) cn
          cn' <- foldM
               (\c' idx -> case idx `Map.lookup` cn of
                   Nothing -> return c'
                   Just c  -> do
                     void $ swapMVar (_connectionState c) AMQPConnectionFailed
                     return $ over cntValue (Map.delete idx) c'
               )
               (v ^. localConnections)
               (Set.toList (_remoteIncomingConnections w))
          case actions of
            Nothing -> return ()
            Just f  -> f w
          return ( LocalEndPointValid (set localConnections cn' v)
                 , Just oldState)
        _ -> return (LocalEndPointValid v, Nothing)
    c -> return (c, Nothing)
  where
    newState = RemoteEndPointFailed

--------------------------------------------------------------------------------
remoteEndPointClose :: Bool -> LocalEndPoint -> RemoteEndPoint -> IO ()
remoteEndPointClose silent lep rep = do
  modifyIORef (remoteOpened rep) (const False)
  join $ modifyMVar (remoteState rep) $ \o -> case o of
    RemoteEndPointFailed        -> return (o, return ())
    RemoteEndPointClosed        -> return (o, return ())
    RemoteEndPointClosing (ClosingRemoteEndPoint _ _ l) -> return (o, void $ readMVar l)
    RemoteEndPointPending _ -> do
      let err = (error "Pending actions should not be executed")
      closing err err o 
      -- TODO: store socket, or delay
    RemoteEndPointValid v   -> closing (_remoteExchange v) (_remoteChannel v) o
 where
   closing exg ch old = do
     lock <- newEmptyMVar
     return (RemoteEndPointClosing (ClosingRemoteEndPoint exg ch lock), go lock old)
   go lock old@(RemoteEndPointValid (ValidRemoteEndPointState c amqpCh _ s i)) = do
     -- close all connections
     void $ cleanupRemoteEndPoint lep rep Nothing
     withMVar (localState lep) $ \case
       LocalEndPointClosed -> return ()
       LocalEndPointValid v@ValidLocalEndPointState{..} -> do
         -- notify about all connections close (?) do we really want it?
         traverse_ (writeChan _localChan . ConnectionClosed) (Set.toList s)
         -- if we have outgoing connections, then we have connection error
         when (i > 0) $ writeChan _localChan
                      $ ErrorEvent $ TransportError (EventConnectionLost (remoteAddress rep)) "Remote end point closed."

     unless silent $ do
       publish amqpCh c (MessageEndPointClose (localAddress lep) True)
       yield
       void $ Async.race (readMVar lock) (threadDelay 1000000)
       void $ tryPutMVar lock ()
       closeRemoteEndPoint lep rep old
       return ()
   go _ _ = return ()

--------------------------------------------------------------------------------
createTransport :: AMQPParameters -> IO Transport
createTransport params@AMQPParameters{..} = do
  let validTState = ValidTransportState transportConnection Map.empty
  tState <- newMVar (TransportValid validTState)
  let iState = AMQPInternalState params tState
  return Transport {
    newEndPoint = apiNewEndPoint iState
  , closeTransport = apiCloseTransport iState
  }

--------------------------------------------------------------------------------
apiCloseTransport :: AMQPInternalState -> IO ()
apiCloseTransport is = do
  old <- readMVar $ istate_tstate is
  case old of
    TransportClosed -> return ()
    -- Do not close the externally-passed AMQP connection,
    -- or it will compromise users sharing it!
    TransportValid (ValidTransportState _ mp) -> traverse_ (apiCloseEndPoint is) mp
  void $ swapMVar (istate_tstate is) TransportClosed

--------------------------------------------------------------------------------
afterP :: a -> IO (IO a)
afterP = return . return

