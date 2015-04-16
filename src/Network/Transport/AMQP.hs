{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE CPP #-}

#if ! MIN_VERSION_base(4,7,0)
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
#endif

{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-} -- Cut noise for post-AMP imports

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
import qualified Network.AMQP.Types as AMQP
import qualified Data.Text as T
import qualified Data.Set as Set
import           Data.UUID.V4
import           Data.UUID (toString, toWords)
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TMChan
import           Data.Bits
import           Data.IORef
import           Data.Monoid
import qualified Data.Map.Strict as Map
import           Data.ByteString (ByteString)
import           Data.Foldable hiding (mapM_)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import           Data.String.Conv
import           Data.Serialize
import           Network.Transport
import           Network.Transport.Internal (mapIOException, asyncWhenCancelled)
import           Control.Applicative
import           Control.Concurrent (yield, threadDelay)
import           Control.Concurrent.Async as Async
import           Control.Concurrent.MVar
import           Control.Monad
import           Lens.Family2
import           System.IO (hPutStrLn , stderr)
import           Text.Printf
import           Control.Monad.Catch hiding (catches, Handler(..))
import           Control.Exception (mapException, catches, throwIO, Handler(..), evaluate)

#if ! MIN_VERSION_base(4,7,0)
import           GHC.MVar
import           GHC.Base
#endif

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
apiNewEndPoint is@AMQPInternalState{..} = try $ mapAMQPException (TransportError NewEndPointFailed . show) $ do
  let AMQPParameters{..} = istate_params
  modifyMVar istate_tstate $ \tst -> case tst of
    TransportClosed -> throwM $ TransportError NewEndPointFailed "Transport is closed."
    TransportValid vst@(ValidTransportState _ oldMap) -> do
      -- If a valid transport was found, create a new LocalEndPoint
      (ep@LocalEndPoint{..}, chan) <- endPointCreate (Map.size oldMap) is
      let newEp = EndPoint {
            receive       = atomically $ do
              mx <- readTMChan chan
              case mx of
                Nothing -> error "receive: channel is closed"
                Just x  -> return x
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
endPointCreate :: Int -> AMQPInternalState -> IO (LocalEndPoint, TMChan Event)
endPointCreate newId is@AMQPInternalState{..} = do
  -- Each new `EndPoint` has a new Rabbit queue. Think of it as a
  -- heavyweight connection. Subsequent connections are multiplexed
  -- using Rabbit's exchanges.
  uuid <- toS . toString <$> nextRandom
  let qName = maybe uuid toS (transportEndpoint istate_params)
  newChannel <- AMQP.openChannel (transportConnection istate_params)

  -- TODO: In case we do not randomise the endpoint name, there is the
  -- risk that creating 2 endpoints for a Transport with fixed address
  -- will cause the former to share the same queue.
  (ourEndPoint,_,_) <- AMQP.declareQueue newChannel $ AMQP.newQueue {
      AMQP.queueName = qName <> ":" <> T.pack (show newId)
      , AMQP.queuePassive = False
      , AMQP.queueDurable = False
      , AMQP.queueExclusive = False
      , AMQP.queueHeaders = queueHeaders
      }

  newExchange <- toExchangeName (toAddress ourEndPoint)
  AMQP.declareExchange newChannel $ AMQP.newExchange {
      AMQP.exchangeName = newExchange
      , AMQP.exchangeType = "direct"
      , AMQP.exchangePassive = False
      , AMQP.exchangeDurable = False
      , AMQP.exchangeAutoDelete = True
      }

  AMQP.bindQueue newChannel ourEndPoint newExchange mempty

  chOut <- newTMChanIO
  lep <- LocalEndPoint <$> pure (toAddress ourEndPoint)
                       <*> pure (AMQPExchange newExchange)
                       <*> newEmptyMVar
                       <*> newEmptyMVar
  opened <- newIORef True
  mask $ \restore -> do
    restore (startReceiver is lep chOut newChannel)
    AMQP.addChannelExceptionHandler newChannel $ \_ -> do
      finaliseEndPoint lep False
    putMVar (localState lep) $ LocalEndPointValid
            (ValidLocalEndPointState chOut newChannel opened newCounter Map.empty)
    return (lep, chOut)
  where
    queueHeaders = AMQP.FieldTable (Map.fromList [("x-expires", AMQP.FVInt32 1000)])

--------------------------------------------------------------------------------
finaliseEndPoint :: LocalEndPoint -> Bool -> IO ()
finaliseEndPoint ourEp requestedByUser = do
  join $ withMVar (localState ourEp) $ \case
    LocalEndPointClosed  -> afterP ()
    LocalEndPointValid v@ValidLocalEndPointState{..} -> do
      writeIORef _localOpened False
      return $ do
        void $ Async.mapConcurrently (remoteEndPointClose False ourEp)
             $ v ^. localRemotes
  when requestedByUser $ void $ tryPutMVar (localDone ourEp) ()
  void $ swapMVar (localState ourEp) LocalEndPointClosed

--------------------------------------------------------------------------------
toExchangeName :: EndPointAddress -> IO T.Text
toExchangeName eA = do
  uuid <- toString <$> nextRandom
  return $ fromAddress eA <> ":" <> T.pack uuid


--------------------------------------------------------------------------------
startReceiver :: AMQPInternalState 
              -> LocalEndPoint
              -> TMChan Event
              -> AMQP.Channel 
              -> IO ()
startReceiver tr@AMQPInternalState{..} lep@LocalEndPoint{..} _localChan ch = do
  void $ AMQP.consumeMsgs ch (fromAddress localAddress) AMQP.NoAck $ \(msg,_) -> do
    case decode' msg of
      Left _ -> return ()
      Right PoisonPill -> do
          finaliseEndPoint lep True
      Right (MessageData cId rawMsg) -> do
          atomically $ writeTMChan _localChan $ Received cId rawMsg
      Right (MessageConnect theirAddress) -> do
          void $ createOrGetRemoteEndPoint tr lep theirAddress
      Right (MessageInitConnection theirAddress theirId rel) -> do
          join $ modifyMVar localState $ \case
            LocalEndPointValid vst@ValidLocalEndPointState{..} -> do
                case Map.lookup theirAddress _localRemotes of
                  Nothing -> return (LocalEndPointValid vst
                                    , throwM $ InvariantViolated (RemoteEndPointLookupFailed theirAddress))
                  Just rep -> modifyMVar (remoteState rep) $ \case
                    RemoteEndPointFailed ->
                      throwM $ InvariantViolated (RemoteEndPointMustBeValid theirAddress)
                    RemoteEndPointClosed ->
                      throwM $ InvariantViolated (RemoteEndPointMustBeValid theirAddress)
                    RemoteEndPointClosing{} ->
                      throwM $ InvariantViolated (RemoteEndPointMustBeValid theirAddress)
                    w@RemoteEndPointValid{} -> do
                      conn <- AMQPConnection <$> pure lep
                                             <*> pure rep
                                             <*> pure rel
                                             <*> newMVar (AMQPConnectionValid $ ValidAMQPConnection Nothing Nothing i)
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
                                             <*> newMVar (AMQPConnectionValid $ ValidAMQPConnection Nothing Nothing i)
                                             <*> newEmptyMVar
                      modifyIORef w (\xs -> (registerRemoteEndPoint (succ i))  : xs)
                      return ( z
                             , ( LocalEndPointValid (set localConnections (Counter (succ i) (Map.insert (succ i) conn m)) vst)
                               , return ())
                             )

              where
                (Counter i m) = vst ^. localConnections
            _ -> throwM $ InvariantViolated (LocalEndPointMustBeValid localAddress)
        where
          registerRemoteEndPoint :: ConnectionId -> RemoteEndPointState -> IO RemoteEndPointState
          registerRemoteEndPoint i RemoteEndPointFailed = do
            atomically $ do
                writeTMChan _localChan (ConnectionOpened i rel theirAddress)
                writeTMChan _localChan (ConnectionClosed i)
            return RemoteEndPointFailed
          registerRemoteEndPoint i RemoteEndPointClosed = do
            atomically $ do
                writeTMChan _localChan (ConnectionOpened i rel theirAddress)
                writeTMChan _localChan (ConnectionClosed i)
            return RemoteEndPointClosed
          registerRemoteEndPoint i (RemoteEndPointClosing x) = do
            atomically $ do
                writeTMChan _localChan (ConnectionOpened i rel theirAddress)
                writeTMChan _localChan (ConnectionClosed i)
            return $ RemoteEndPointClosing x
          registerRemoteEndPoint _ RemoteEndPointPending{} = 
            throwM $ InvariantViolated $ RemoteEndPointCannotBePending (theirAddress)
          registerRemoteEndPoint i (RemoteEndPointValid vst@(ValidRemoteEndPointState exg chl _ s _)) = do
            publish chl exg (MessageInitConnectionOk localAddress theirId i)
            atomically $ writeTMChan _localChan (ConnectionOpened i rel theirAddress)
            return $ RemoteEndPointValid vst { _remoteIncomingConnections = Set.insert i s }

      --
      -- MessageCloseConnection
      --
      Right (MessageCloseConnection idx) -> do
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
                             AMQPConnectionInit -> return  () -- throwM InvariantViolation
                             AMQPConnectionClosed -> return ()
                             AMQPConnectionValid (ValidAMQPConnection{}) -> do
                                atomically $ writeTMChan _localChan (ConnectionClosed idx)
                                connectionCleanup (_connectionRemoteEndPoint conn) idx)
          LocalEndPointClosed -> return (LocalEndPointClosed, return ())

      --
      -- MessageInitConnectionOk
      --
      Right (MessageInitConnectionOk theirAddress ourId theirId) -> do
        join $ withMVar localState $ \case
          LocalEndPointValid vst -> 
            case vst ^. localRemoteAt theirAddress of
              Nothing  -> return (return ()) -- TODO: send message to the host
              Just rep -> modifyMVar (remoteState rep) $ \case
                RemoteEndPointFailed -> return (RemoteEndPointFailed, return ())
                RemoteEndPointClosed -> 
                  throwM $ InvariantViolated $ RemoteEndPointShouldBeValidOrClosed theirAddress
                RemoteEndPointClosing{} -> 
                  throwM $ InvariantViolated $ RemoteEndPointShouldBeValidOrClosed theirAddress
                t@(RemoteEndPointValid (ValidRemoteEndPointState exg chl (Counter x m) s z)) -> return $
                  case ourId `Map.lookup` m of
                      Nothing -> (t, return ()) -- TODO: send message to the hostv
                      Just c  ->
                        (RemoteEndPointValid (ValidRemoteEndPointState exg chl (Counter x (ourId `Map.delete` m)) s (z+1))
                        , do modifyMVar_ (_connectionState c) $ \case
                               AMQPConnectionFailed -> return AMQPConnectionFailed
                               AMQPConnectionInit -> 
                                 return $ AMQPConnectionValid (ValidAMQPConnection (Just exg) (Just chl) theirId)
                               AMQPConnectionClosed -> do
                                   publish chl exg (MessageCloseConnection theirId)
                                   return AMQPConnectionClosed
                               AMQPConnectionValid _ -> 
                                 throwM $ (IncorrectState "RemoteEndPoint should be closed")
                             void $ tryPutMVar (_connectionReady c) ()
                        )
                RemoteEndPointPending p -> 
                  return (RemoteEndPointPending p
                         , throwM $ (IncorrectState "RemoteEndPoint should be closed"))
          LocalEndPointClosed -> return $ return ()

      --
      -- MessageEndPointClose
      --
      Right (MessageEndPointClose theirAddress True) -> do
        getRemoteEndPoint lep theirAddress >>=
          traverse_ (\rep -> do
            onValidRemote rep $ \ValidRemoteEndPointState{..} ->
              publish _remoteChannel _remoteExchange (MessageEndPointCloseOk localAddress)
            remoteEndPointClose True lep rep)

      Right (MessageEndPointClose theirAddress False) -> do
        getRemoteEndPoint lep theirAddress >>=
          traverse_ (\rep -> do
            mst <- cleanupRemoteEndPoint lep rep Nothing
            case mst of
              Nothing -> return ()
              Just st -> onValidEndPoint lep $ \vst -> do
                atomically $ writeTMChan (vst ^. localChan) $
                    ErrorEvent $ TransportError (EventConnectionLost theirAddress) "MessageEndPointClose: Exception on remote side"
                closeRemoteEndPoint lep rep st)

      --
      -- MessageEndPointCloseOk
      --
      Right (MessageEndPointCloseOk theirAddress) -> do
        getRemoteEndPoint lep theirAddress >>=
          traverse_ (\rep -> do
            state <- swapMVar (remoteState rep) RemoteEndPointClosed
            closeRemoteEndPoint lep rep state)

--------------------------------------------------------------------------------
-- | Close, all network connections.
closeRemoteEndPoint :: LocalEndPoint 
                    -> RemoteEndPoint 
                    -> RemoteEndPointState 
                    -> IO ()
closeRemoteEndPoint lep rep state = do
  step1 
  step2 state
  where
   step1 = uninterruptibleMask_ $ do
       -- NOTE: This step1 function is different
       -- from Tweag's one, which used "modifyMVar_", but
       -- on this implementation it seems to always deadlock
       -- on the "SendException" test.
       mbSt <- tryReadMVar (localState lep)
       case mbSt of
         Nothing -> return ()
         Just vst -> do
             newState <- case vst of
               LocalEndPointValid v -> return $
                 LocalEndPointValid (over localRemotes (Map.delete (remoteAddress rep)) v)
               c -> return c
             void $ tryPutMVar (localState lep) newState
   step2 (RemoteEndPointValid v) = do
       AMQP.closeChannel (v ^. remoteChannel)
   step2 (RemoteEndPointClosing (ClosingRemoteEndPoint (AMQPExchange exg) ch rd)) = do
     _ <- readMVar rd
     AMQP.deleteExchange ch exg
     AMQP.closeChannel ch
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
apiCloseEndPoint AMQPInternalState{..} lep@LocalEndPoint{..} =
  mask_ $ either errorLog return <=< tryAMQP $ do
    -- we don't close endpoint here because other threads,
    -- should be able to access local endpoint state
    old <- readMVar localState
    mbCh <- case old of
      LocalEndPointValid ValidLocalEndPointState{..} -> do
        -- close channel, no events will be received
        atomically $ do
          writeTMChan _localChan EndPointClosed
          closeTMChan _localChan
        publish _localChannel localExchange PoisonPill
        return (Just _localChannel)
      LocalEndPointClosed -> return Nothing

    takeMVar localDone
    void $ swapMVar localState LocalEndPointClosed
    modifyMVar_ istate_tstate $ \case
      TransportClosed  -> return TransportClosed
      TransportValid v -> return
        $ TransportValid (over tstateEndPoints (Map.delete localAddress) v)
    -- Try to cleanup queue and channel
    case mbCh of
      Just ch -> finaliseQueueAndChannel lep ch
      Nothing -> do
          tmpCh <- AMQP.openChannel (transportConnection istate_params)
          finaliseQueueAndChannel lep tmpCh

--------------------------------------------------------------------------------
finaliseQueueAndChannel :: LocalEndPoint -> AMQP.Channel -> IO ()
finaliseQueueAndChannel LocalEndPoint{..} ch = do
  let queue = fromAddress localAddress
  let (AMQPExchange e) = localExchange
  AMQP.deleteExchange ch e
  _ <- AMQP.deleteQueue ch queue
  AMQP.closeChannel ch

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
apiConnect is@AMQPInternalState{..} lep@LocalEndPoint{..} theirAddress reliability _ = fmap (either Left id) $ try $ do
  mapAMQPException (TransportError ConnectFailed . show) $ do
    eRep <- createOrGetRemoteEndPoint is lep theirAddress
    case eRep of
      Left _ -> return $ Left $ TransportError ConnectFailed "LocalEndPoint is closed."
      Right rep -> do
        conn <- AMQPConnection <$> pure lep
                               <*> pure rep
                               <*> pure reliability
                               <*> newMVar AMQPConnectionInit
                               <*> newEmptyMVar
        let apiConn = Connection
              { send = apiSend conn
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
            newState <- handshake conn w
            return (newState, waitReady conn apiConn)
          RemoteEndPointPending z -> do
            modifyIORef z (\zs -> handshake conn : zs)
            return ( RemoteEndPointPending z, waitReady conn apiConn)
          RemoteEndPointFailed ->
            return ( RemoteEndPointFailed
                   , return $ Left $ TransportError ConnectFailed "RemoteEndPoint failed.")
  where
    waitReady conn apiConn = join $ withMVar (_connectionState conn) $ \case
      AMQPConnectionInit{}   -> return $ yield >> waitReady conn apiConn
      AMQPConnectionValid{}  -> afterP $ Right apiConn
      AMQPConnectionFailed{} -> afterP $ Left $ TransportError ConnectFailed "Connection failed."
      AMQPConnectionClosed{} -> afterP $ Left $ TransportError ConnectFailed "Connection closed"
    handshake _ RemoteEndPointClosed      = return RemoteEndPointClosed
    handshake _ RemoteEndPointPending{}   = 
      throwM $ TransportError ConnectFailed "Connection pending."
    handshake _ (RemoteEndPointClosing x) = return $ RemoteEndPointClosing x
    handshake _ RemoteEndPointFailed      = return RemoteEndPointFailed
    handshake conn (RemoteEndPointValid (ValidRemoteEndPointState exg ch (Counter i m) s z)) = do
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
                          -> IO (Either AMQPError RemoteEndPoint)
createOrGetRemoteEndPoint AMQPInternalState{..} ourEp theirAddr = do
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
                  create v
              _ -> return (LocalEndPointValid v, return $ Right rep)
      else return (LocalEndPointValid v, return $ Left $ IncorrectState "EndPointClosing")
    LocalEndPointClosed ->
      return  ( LocalEndPointClosed
              , return $ Left $ IncorrectState "EndPoint is closed"
              )
  where
    create v = do
      newChannel <- AMQP.openChannel (transportConnection istate_params)
      newExchange <- toExchangeName theirAddr
      AMQP.declareExchange newChannel $ AMQP.newExchange {
          AMQP.exchangeName = newExchange
          , AMQP.exchangeType = "direct"
          , AMQP.exchangePassive = False
          , AMQP.exchangeDurable = False
          , AMQP.exchangeAutoDelete = True
          }

      AMQP.bindQueue newChannel (fromAddress theirAddr) newExchange mempty

      state <- newMVar . RemoteEndPointPending =<< newIORef []
      opened <- newIORef False
      let rep = RemoteEndPoint theirAddr state opened
      return ( LocalEndPointValid 
             $ over localRemotes (Map.insert theirAddr rep) v
             , initialize newChannel (AMQPExchange newExchange) rep >> return (Right rep))

    ourAddr = localAddress ourEp

    initialize ch exg rep = do
      publish ch exg $ MessageConnect ourAddr
      let v = ValidRemoteEndPointState exg ch newCounter Set.empty 0
      modifyMVar_ (remoteState rep) $ \case
        RemoteEndPointPending p -> do
            z <- foldM (\y f -> f y) (RemoteEndPointValid v) . Prelude.reverse =<< readIORef p
            modifyIORef (remoteOpened rep) (const True)
            return z
        RemoteEndPointValid _   -> 
          throwM $ InvariantViolated (RemoteEndPointMustBeValid theirAddr)
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
apiSend :: AMQPConnection
        -> [ByteString] 
        -> IO (Either (TransportError SendErrorCode) ())
apiSend (AMQPConnection us them _ st _) msg = do
  msgs <- try $ mapM_ evaluate msg
  case msgs of
    Left ex ->  do cleanup
                   return $ Left $ TransportError SendFailed (show (ex::SomeException))
    -- TODO: Check that, in case of AMQP-raised exceptions, we are still
    -- doing the appropriate cleanup.
    Right _ -> (fmap Right send_) `catches`
                 [ Handler $ \ex ->    -- TransportError - return, as all require
                                       -- actions were performed
                     return $ Left (ex :: TransportError SendErrorCode)
                 , Handler $ \ex -> do -- AMQPError appeared exception
                     cleanup
                     return $ Left $ TransportError SendFailed (show (ex::AMQPError))
                 , Handler $ \ex -> do -- AMQPException appeared exception
                     cleanup
                     return $ Left $ TransportError SendFailed (show (ex::AMQP.AMQPException))
                 ]

  where
   send_ :: IO ()
   send_ = join $ withMVar (remoteState them) $ \x -> case x of
     RemoteEndPointPending{} -> 
       return $ yield >> send_
     RemoteEndPointFailed -> 
       throwIO $ TransportError SendFailed "Remote end point is failed."
     RemoteEndPointClosed -> 
       throwIO $ TransportError SendFailed "Remote end point is closed."
     RemoteEndPointClosing{} -> 
       throwIO $ TransportError SendFailed "Remote end point is closing."
     RemoteEndPointValid _  -> withMVar st $ \case
       AMQPConnectionInit   -> return $ yield >> send_
       AMQPConnectionClosed -> 
         throwIO $ TransportError SendClosed "Connection is closed"
       AMQPConnectionFailed -> 
         throwIO $ TransportError SendFailed "Connection is failed"
       AMQPConnectionValid (ValidAMQPConnection (Just exg) (Just ch) idx) -> do
         res <- try $ publish ch exg (MessageData idx msg)
         case res of
           Right _ -> afterP ()
           Left (_ :: SomeException) -> do
             rep <- cleanupRemoteEndPoint us them Nothing
             traverse_ (\z -> do
                 onValidEndPoint us $ \w -> atomically $ writeTMChan (_localChan w) $
                    ErrorEvent $ TransportError (EventConnectionLost (remoteAddress them)) "apiSend: Exception on remote side"
                 closeRemoteEndPoint us them z) rep
             throwIO $ TransportError SendFailed "Connection broken."

   cleanup = do
     void $ cleanupRemoteEndPoint us them
       (Just $ \v -> publish (_remoteChannel v) (_remoteExchange v) $ 
                     MessageEndPointClose (localAddress us) False)
     onValidEndPoint us $ \v -> do
       atomically $ writeTMChan (_localChan v) $ ErrorEvent $ TransportError
                 (EventConnectionLost (remoteAddress them)) "Exception on send."

--------------------------------------------------------------------------------
apiClose :: AMQPConnection -> IO ()
apiClose (AMQPConnection _ them _ st _) = 
  either errorLog return <=< tryAMQP $ uninterruptibleMask_ $ do
    join $ modifyMVar st $ \case
      AMQPConnectionValid (ValidAMQPConnection _ _ idx) -> do
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
      throwM $ InvariantViolated (RemoteEndPointMustBeValid (remoteAddress them))
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
cleanupRemoteEndPoint lep rep actions = do
  modifyMVar (localState lep) $ \case
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
       LocalEndPointValid ValidLocalEndPointState{..} -> do
         -- notify about all connections close (?) do we really want it?
         traverse_ (atomically . writeTMChan _localChan . ConnectionClosed) (Set.toList s)
         -- if we have outgoing connections, then we have connection error
         when (i > 0) $ do
           let msg = "Remote end point closed, but " <> show i <> " outgoing connection(s)"
           atomically $ writeTMChan _localChan $ ErrorEvent $ TransportError (EventConnectionLost (remoteAddress rep)) msg

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
-- Do not close the externally-passed AMQP connection,
-- or it will compromise users sharing it!
apiCloseTransport :: AMQPInternalState -> IO ()
apiCloseTransport is = do
  old <- swapMVar (istate_tstate is) TransportClosed
  case old of
    TransportClosed -> return ()
    TransportValid (ValidTransportState _ mp) -> either errorLog return <=< tryAMQP $ do
      traverse_ (apiCloseEndPoint is) mp

--------------------------------------------------------------------------------
afterP :: a -> IO (IO a)
afterP = return . return

--------------------------------------------------------------------------------
-- | Print error to standart output, this function should be used for
-- errors that could not be handled in a normal way.
errorLog :: Show a => a -> IO ()
errorLog s = hPutStrLn stderr (printf "[network-transport-amqp] Unhandled error: %s" $ show s)

--------------------------------------------------------------------------------
promoteAMQPException :: a -> a
promoteAMQPException = mapException DriverError

--------------------------------------------------------------------------------
tryAMQP :: (MonadCatch m) => m a -> m (Either AMQPError a)
tryAMQP = try . promoteAMQPException

--------------------------------------------------------------------------------
mapAMQPException :: (Exception e) => (AMQPError -> e) -> a -> a
mapAMQPException = mapException

#if ! MIN_VERSION_base(4,7,0)
--------------------------------------------------------------------------------
tryReadMVar :: MVar a -> IO (Maybe a)
tryReadMVar (MVar m) = IO $ \ s ->
    case tryReadMVar# m s of
        (# s', 0#, _ #) -> (# s', Nothing #)      -- MVar is empty
        (# s', _,  a #) -> (# s', Just a  #)      -- MVar is full
#endif
