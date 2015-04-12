{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
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
import Network.Transport.Internal (asyncWhenCancelled)
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
apiNewEndPoint :: AMQPInternalState
               -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint is@AMQPInternalState{..} = do
  try . asyncWhenCancelled closeEndPoint $ do
    let AMQPParameters{..} = istate_params
    modifyMVar istate_tstate $ \tst -> case tst of
      TransportClosed -> throwIO $ TransportError NewEndPointFailed "Transport is closed."
      TransportValid vst@(ValidTransportState cnn oldMap) -> do
        -- If a valid transport was found, create a new LocalEndPoint
        (ep@LocalEndPoint{..}, chan) <- endPointCreate is
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
endPointCreate :: AMQPInternalState -> IO (LocalEndPoint, Chan Event)
endPointCreate is@AMQPInternalState{..} = do
  newChannel <- AMQP.openChannel (transportConnection istate_params)
  uuid <- toS . toString <$> nextRandom
  -- Each new `EndPoint` has a new Rabbit queue. Think of it as a
  -- heavyweight connection. Subsequent connections are multiplexed
  -- using Rabbit's exchanges.

  -- TODO: In case we do not randomise the endpoint name, there is the
  -- risk that creating 2 endpoints for a Transport with fixed address
  -- will cause the former to share the same queue.
  (ourEndPoint,_,_) <- AMQP.declareQueue newChannel $ AMQP.newQueue {
      AMQP.queueName = maybe uuid toS (transportEndpoint istate_params)
      , AMQP.queuePassive = False
      , AMQP.queueDurable = False
      , AMQP.queueExclusive = True
      }
{- Do not create an exchange now.
      let ourExchange = ourEndPoint

      AMQP.declareExchange newChannel $ AMQP.newExchange {
          AMQP.exchangeName = ourExchange
          , AMQP.exchangeType = "direct"
          , AMQP.exchangePassive = False
          , AMQP.exchangeDurable = False
          , AMQP.exchangeAutoDelete = True
          }
      AMQP.bindQueue newChannel ourEndPoint ourExchange mempty
-}
  ch <- newChan
  opened <- newIORef True
  let newState = ValidLocalEndPointState ch newChannel opened Map.empty
  st <- newMVar (LocalEndPointValid newState)
  let ep = LocalEndPoint (toAddress ourEndPoint) st
  startReceiver is ep
  return (ep, ch)


--------------------------------------------------------------------------------
toExchangeName :: EndPointAddress -> ConnectionId -> T.Text
toExchangeName eA cId = fromAddress eA <> ":" <> T.pack (show cId)


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
              void $ getOrCreateRemoteEndPoint theirAddress
          Right v@(MessageInitConnection theirAddr theirId rel) -> do
              print v
              modifyMVar localState $ \case
                LocalEndPointValid vst@ValidLocalEndPointState -> do
                    case Map.lookup theirAddress localConnections of
                      Nothing -> return (LocalEndPointValid v
                                        , thowIO $ InvariantViolation (RemoteEndPointLookupFailed theirAddress))
                      Just rep -> modifyMVar (remoteState rep) $ \case
                        RemoteEndPointFailed ->
                          throwIO $ InvariantViolation (RemoteEndPointMustBeValid theirAddress)
                        RemoteEndPointClosed ->
                          throwIO $ InvariantViolation (RemoteEndPointMustBeValid theirAddress)
                        RemoteEndPointClosing
                          throwIO $ InvariantViolation (RemoteEndPointMustBeValid theirAddress)
                        w@RemoteEndPointValid{} -> do
                          conn <- AMQPConnection <$> pure ourEp
                                                 <*> pure rep
                                                 <*> pure rel
                                                 <*> newMVar (AMQPConnectionValid $ ValidAMQPConnection Nothing i)
                                                 <*> newEmptyMVar
                          w' <- register (succ i) w
                          return ( w'
                                 , ( LocalEndPointValid (localConnections ^= Counter (succ i) (Map.insert (succ i) conn m) $ v)
                                   , return ())
                                 )
                        z@(RemoteEndPointPending w) -> do
                          conn <- AMQPConnection <$> pure ourEp
                                                 <*> pure rep
                                                 <*> pure rel
                                                 <*> newMVar (AMQPConnectionValid $ ValidAMQPConnection Nothing i)
                                                 <*> newEmptyMVar
                          modifyIORef w (\xs -> (register (succ i))  : xs)
                          return ( z
                                 , ( LocalEndPointValid (localConnections ^= Counter (succ i) (Map.insert (succ i) conn m) $ v)
                                   , return ())
                                 )

                  where
                    (Counter i m) = v ^. localConnections
                _ -> throwIO $ InvariantViolated (LocalEndPointMustBeValid localAddress)
          where
            registerRemoteEndPoint :: ConnectionId -> RemoteEndPointState -> IO RemoteEndPointState
            registerRemoteEndPoint i RemoteEndPointFailed = do
              writeChan _localChan (ConnectionOpened i rel theirAddress)
              writeChan _localChan (ConnectionClosed i)
              return RemotEndPointFailed
            registerRemoteEndPoint i RemoteEndPointClosed = do
              writeChan _localChan (ConnectionOpened i rel theirAddress)
              writeChan _localChan (ConnectionClosed i)
              return RemoteEndPointClosed
            registerRemoteEndPoint i (RemoteEndPointClosing x) = do
              writeChan _localChan (ConnectionOpened i rel theirAddress)
              writeChan _localChan (ConnectionClosed i)
              return $ RemoteEndPointClosing x
            registerRemoteEndPoint _ RemoteEndPointPending{} = 
              throwIO $ InvariantViolation $ RemoteEndPointCannotBePending (theirAddress)
            registerRemoteEndPoint i (RemoteEndPointValid v@(ValidRemoteEndPoint exg _ s _)) = do
              publish _localChannel exg (MessageInitConnectionOk ourAddr theirId i)
              writeChan _localChan (ConnectionOpened i rel theirAddress)
              return $ RemoteEndPointValid v { _remoteIncomingConnections = Set.insert i s }

          --
          -- MessageCloseConnection
          --
          Right v@(MessageCloseConnection idx) -> do
            print v
            modifyMVar localState $ \case
              LocalEndPointValid v ->
                  case v ^. localConnectionAt idx of
                    Nothing  -> return (LocalEndPointValid v, return ())
                    Just conn -> do
                      old <- swapMVar (connectionState conn) AMQPConnectionFailed
                      let setter = over localEndPointConnections (Map.delete idx)
                      return ( LocalEndPointValid $ setter v
                             , case old of
                                 AMQPConnectionFailed -> return ()
                                 AMQPConnectionInit -> return  () -- throwIO InvariantViolation
                                 AMQPConnectionClosed -> return ()
                                 AMQPConnectionValid (ValidZMQConnection _ _) -> do
                                    writeChan _localChan (ConnectionClosed idx)
                                    connectionCleanup (_connectionRemoteEndPoint conn) idx)
              LocalEndPointClosed -> return (LocalEndPointClosed, return ())

          --
          -- MessageInitConnectionOk
          --
          Right v@(MessageInitConnectionOk theirAddress ourId theirId) -> do
            withMVar localState $ \case
              LocalEndPointValid v -> 
                case v ^. localRemoteAt theirAddress of
                  Nothing  -> return (return ()) -- TODO: send message to the host
                  Just rep -> modifyMVar (remoteState rep) $ \case
                    RemoteEndPointFailed -> return (RemoteEndPointFailed, return ())
                    RemoteEndPointClosed -> 
                      throwIO $ InvariantViolation $ RemoteEndPointShouldBeValidOrClosed theirAddress
                    RemoteEndPointClosing{} -> 
                      throwM $ InvariantViolation  $ RemoteEndPointShouldBeValidOrClosed theirAddress
                    t@(RemoteEndPointValid (ValidRemoteEndPoint exg (Counter x m) s z)) -> return $
                      case ourId `Map.lookup` m of
                          Nothing -> (t, return ()) -- TODO: send message to the hostv
                          Just c  ->
                            (RemoteEndPointValid (ValidRemoteEndPoint exg (Counter x (ourId `Map.delete` m)) s (z+1))
                            , do modifyMVar_ (connectionState c) $ \case
                                   AMQPConnectionFailed -> return ZMQConnectionFailed
                                   AMQPConnectionInit -> return $ ZMQConnectionValid (ValidZMQConnection (Just sock) theirId)
                                   AMQPConnectionClosed -> do
                                       publish exg (MessageCloseConnection theirId)
                                       return ZMQConnectionClosed
                                   ZMQConnectionValid _ -> throwM $ InvariantViolation "RemoteEndPoint should be closed"
                                 void $ tryPutMVar (connectionReady c) ()
                            )
                    RemoteEndPointPending p -> return (RemoteEndPointPending p, throwM $ InvariantViolation "RemoteEndPoint should be closed")
              LocalEndPointClosed -> return $ return ()

        MessageEndPointClose theirAddress True -> getRemoteEndPoint ourEp theirAddress >>=
          traverse_ (\rep -> do
            onValidRemote rep $ \v ->
              ZMQ.send (remoteEndPointSocket v) [] (encode' $ MessageEndPointCloseOk $ localEndPointAddress ourEp)
            remoteEndPointClose True ourEp rep)

        MessageEndPointClose theirAddress False -> getRemoteEndPoint ourEp theirAddress >>=
          traverse_ (\rep -> do
            mst <- cleanupRemoteEndPoint ourEp rep Nothing
            case mst of
              Nothing -> return ()
              Just st -> do
                onValidEndPoint ourEp $ \v -> atomically $ writeTMChan (localEndPointChan v) $
                   ErrorEvent $ TransportError (EventConnectionLost theirAddress) "Exception on remote side"
                closeRemoteEndPoint ourEp rep st)

        MessageEndPointCloseOk theirAddress -> getRemoteEndPoint ourEp theirAddress >>=
          traverse_ (\rep -> do
            state <- swapMVar (remoteEndPointState rep) RemoteEndPointClosed
            closeRemoteEndPoint ourEp rep state)

{--
              -- TODO: Do I need to persist this RemoteEndPoint with the id given to me?
              (rep, isNew) <- findRemoteEndPoint lep theirAddr
              print $ "Inside MessageInitConn: isNew " ++ show isNew
              when isNew $ do
                  let ourId = remoteId rep
                  publish _localChannel theirAddr (MessageInitConnectionOk localAddress ourId theirId)
              -- TODO: This is a bug?. I need to issue a ConnectionOpened with the
              -- internal counter I am keeping, not the one coming from the remote
              -- endpoint.
              writeChan _localChan $ ConnectionOpened theirId rel theirAddr
          Right v@(MessageInitConnectionOk theirAddr theirId ourId) -> do
              print v
              writeChan _localChan $ ConnectionOpened theirId ReliableOrdered theirAddr
          Right v@(MessageCloseConnection theirAddr theirId) -> do
              print v
              dead <- closeRemoteConnection tr lep theirAddr
              print $ "Message Close connection - dead: " ++ show dead
              writeChan _localChan $ ConnectionClosed theirId
              print $ "MessageCloseConnection: after writeChan"
          Right v@(MessageEndPointClose theirAddr theirId) -> do
              print v
              unless (localAddress == theirAddr) $ do
                  void $ closeRemoteConnection tr lep theirAddr
                  writeChan _localChan $ ConnectionClosed theirId
--}
    _ -> throwIO $ TransportError NewEndPointFailed "startReceiver: Endpoint was closed"


--------------------------------------------------------------------------------
connectionCleanup :: RemoteEndPoint -> ConnectionId -> IO ()
connectionCleanup rep cid = modifyMVar_ (remoteState rep) $ \case
   RemoteEndPointValid v -> return $
     RemoteEndPointValid $ over remoteIncomingConnections (Set.delete cid) v
   c -> return c

--------------------------------------------------------------------------------
withValidLocalState_ :: LocalEndPoint
                     -> (ValidLocalEndPointState -> IO ())
                     -> IO ()
withValidLocalState_ LocalEndPoint{..} f = withMVar localState $ \st ->
  case st of
    LocalEndPointClosed -> return ()
    LocalEndPointValid v -> f v

--------------------------------------------------------------------------------
apiCloseEndPoint :: AMQPInternalState
                 -> LocalEndPoint
                 -> IO ()
apiCloseEndPoint AMQPInternalState{..} LocalEndPoint{..} = do
  print "Inside apiCloseEndPoint"
  let ourAddress = localAddress

  -- Notify all the remoters this EndPoint is dying.
  old <- readMVar localState
  case old of
    LocalEndPointClosed -> return ()
    LocalEndPointValid vst@ValidLocalEndPointState{..} -> do
      -- Close the given connection
      writeChan _localChan EndPointClosed
      print $ "apiCloseEndPoint: connections :" ++ show (Map.keys $ vst ^. localConnections)
      print $ "apiCloseEndPoint: my address :" ++ show localAddress
      forM_ (Map.toList $ vst ^. localConnections) $ \(theirAddress, rep) -> do
        print $ "Same address? " ++ show (theirAddress == localAddress)
        withMVar (remoteState rep) $ \rst -> case rst of
          RemoteEndPointClosed -> return ()
          _ -> publish _localChannel theirAddress (MessageEndPointClose ourAddress (remoteId rep))

      let queue = fromAddress localAddress
      _ <- AMQP.deleteQueue _localChannel queue
      AMQP.closeChannel _localChannel

  void $ swapMVar localState LocalEndPointClosed

  modifyMVar_ istate_tstate $ \tst -> case tst of
    TransportClosed  -> return TransportClosed
    TransportValid v@ValidTransportState{..} -> do
       return (TransportValid $ over tstateEndPoints (localAddress `Map.delete`) v)

--------------------------------------------------------------------------------
-- | Returns whether or not the `RemoteEndPoint` is now closed
closeRemoteConnection :: AMQPInternalState
                      -> LocalEndPoint
                      -> EndPointAddress
                      -> IO Bool
closeRemoteConnection AMQPInternalState{..} LocalEndPoint{..} theirAddress = do
  print "Inside closeRemoteConnection"
  modifyMVar localState $ \lst -> case lst of
    (LocalEndPointValid vst@ValidLocalEndPointState{..}) -> do
      case Map.lookup theirAddress (vst ^. localConnections) of
        Nothing -> throwIO $ InvariantViolated (EndPointNotInRemoteMap theirAddress)
        Just rep -> do
          let rep' = decreaseConnections rep
          print $ "Outgoing counter: " ++ show (remoteOutgoingConnections rep')
          clsd <- modifyMVar (remoteState rep) $ \rst -> case rst of
            RemoteEndPointValid -> if remoteOutgoingConnections rep' <= 0 
              then return (RemoteEndPointClosed, True)
              else return (RemoteEndPointValid, False)
            RemoteEndPointClosed -> return (RemoteEndPointClosed, True)
          return (LocalEndPointValid $ over localConnections (Map.insert theirAddress rep') vst, clsd)
    v -> return (v, True)
  where
    decreaseConnections :: RemoteEndPoint -> RemoteEndPoint
    decreaseConnections v@RemoteEndPoint{..} = v { remoteOutgoingConnections = remoteOutgoingConnections - 1 }

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
apiConnect tr@AMQPInternalState{..} lep@LocalEndPoint{..} theirAddress reliability _ = do
  let ourAddress = localAddress
  try . asyncWhenCancelled close $ do
    lst <- takeMVar localState
    putMVar localState lst
    case lst of
      LocalEndPointClosed ->
        throwIO $ TransportError ConnectFailed "apiConnect: LocalEndPointClosed"
      LocalEndPointValid ValidLocalEndPointState{..} -> do
          (rep, isNew) <- findRemoteEndPoint lep theirAddress
          let cId = remoteId rep
          print $ "apiConnect cId: " ++ show cId
          print $ "apiConnect new: " ++ show isNew
          when isNew $ do
              let msg = MessageInitConnection ourAddress cId reliability
              publish _localChannel theirAddress msg
          connAlive <- newIORef True
          return Connection {
                    send = apiSend tr lep theirAddress cId connAlive
                  , close = apiClose tr lep theirAddress cId connAlive
                 }

--------------------------------------------------------------------------------
-- | Find a remote endpoint. If the remote endpoint does not yet exist we
-- create it. Returns if the endpoint was new.
findRemoteEndPoint :: LocalEndPoint
                   -> EndPointAddress
                   -> IO (RemoteEndPoint, Bool)
findRemoteEndPoint lep@LocalEndPoint{..} theirAddr = 
  modifyMVar localState $ \lst -> case lst of
    LocalEndPointClosed -> 
      throwIO $ TransportError ConnectFailed "findRemoteEndPoint: LocalEndPointClosed"
    LocalEndPointValid v ->
      case Map.lookup theirAddr (v ^. localConnections) of
          -- TODO: Check if the RemoteEndPoint is closed.
          Just r -> do
            let newMap = Map.adjust increaseConnections theirAddr (v ^. localConnections)
            return (LocalEndPointValid $ v { _localConnections = newMap }, (r, False))
          Nothing -> do
            newRem <- newValidRemoteEndpoint lep theirAddr
            let newMap = Map.insert theirAddr newRem
            return (LocalEndPointValid $ over localConnections newMap v, (newRem, True))
  where
    increaseConnections :: RemoteEndPoint -> RemoteEndPoint
    increaseConnections v@RemoteEndPoint{..} = v { remoteOutgoingConnections = remoteOutgoingConnections + 1 }

--------------------------------------------------------------------------------
newValidRemoteEndpoint :: LocalEndPoint 
                       -> EndPointAddress 
                       -> IO RemoteEndPoint
newValidRemoteEndpoint LocalEndPoint{..} ep = do
  -- TODO: Experimental: do a bitwise operation on the UUID to generate
  -- a random ConnectionId. Is this safe?
  let queueAsWord64 = foldl1' (+) (map fromIntegral $ B.unpack . endPointAddressToByteString $ localAddress)
  (a,b,c,d) <- toWords <$> nextRandom
  let cId = fromIntegral (a .|. b .|. c .|. d) + queueAsWord64
  var <- newMVar RemoteEndPointValid
  return $ RemoteEndPoint ep cId var 0

--------------------------------------------------------------------------------
connectToSelf :: LocalEndPoint -> IO Connection
connectToSelf lep@LocalEndPoint{..} = do
    let ourEndPoint = localAddress
    (rep, _) <- findRemoteEndPoint lep ourEndPoint
    let cId = remoteId rep
    withValidLocalState_ lep $ \ValidLocalEndPointState{..} ->
      writeChan _localChan $ ConnectionOpened cId ReliableOrdered ourEndPoint
    connAlive <- newIORef True
    return Connection { 
        send  = selfSend cId connAlive
      , close = selfClose cId connAlive
    }
  where
    selfSend :: ConnectionId
             -> IORef Bool
             -> [ByteString]
             -> IO (Either (TransportError SendErrorCode) ())
    selfSend connId connAlive msg = try $ do
      isAlive <- readIORef connAlive
      case isAlive of
        False -> throwIO $ TransportError SendClosed "selfSend: Connections no more."
        True  -> withMVar localState $ \st -> case st of
          LocalEndPointValid ValidLocalEndPointState{..} -> do
            writeChan _localChan (Received connId msg)
          LocalEndPointClosed ->
            throwIO $ TransportError SendFailed "selfSend: Connection closed"

    selfClose :: ConnectionId -> IORef Bool -> IO ()
    selfClose connId connAlive = do
      isAlive <- readIORef connAlive
      case isAlive of
        False -> throwIO $ TransportError SendClosed "selfSend: Connections no more."
        True -> do
          writeIORef connAlive False
          withMVar localState $ \st -> case st of
            LocalEndPointValid ValidLocalEndPointState{..} -> do
                writeChan _localChan (ConnectionClosed connId)
            LocalEndPointClosed -> 
                throwIO $ TransportError SendClosed "selfClose: Connection closed"

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
apiSend :: AMQPInternalState
        -> LocalEndPoint
        -> EndPointAddress
        -> ConnectionId
        -> IORef Bool
        -> [ByteString] 
        -> IO (Either (TransportError SendErrorCode) ())
apiSend is LocalEndPoint{..} their connId connAlive msgs = try $ do
  isAlive <- readIORef connAlive
  case isAlive of
    False -> throwIO $ TransportError SendClosed "apiSend: connAlive = False"
    True  -> withMVar (istate_tstate is) $ \tst -> case tst of
      TransportClosed -> 
        throwIO $ TransportError SendFailed "apiSend: TransportClosed"
      TransportValid _ -> withMVar localState $ \lst -> case lst of
        (LocalEndPointValid vst@ValidLocalEndPointState{..}) -> case Map.lookup their (vst ^. localConnections) of
            Nothing  -> throwIO $ TransportError SendFailed "apiSend: address not in local connections"
            Just rep -> withMVar (remoteState rep) $ \rst -> case rst of
              RemoteEndPointClosed -> throwIO $ TransportError SendFailed "apiSend: Connection closed"
              RemoteEndPointValid ->  publish _localChannel their (MessageData connId msgs)
        _ -> throwIO $ TransportError SendFailed "apiSend: LocalEndPointClosed"

--------------------------------------------------------------------------------
-- | Change the status of the `RemoteEndPoint` to be closed. If the
-- `EndPoint` is already closed, we simply return.
apiClose :: AMQPInternalState
         -> LocalEndPoint
         -> EndPointAddress
         -> ConnectionId
         -> IORef Bool
         -> IO ()
apiClose tr@AMQPInternalState{..} lep@LocalEndPoint{..} ep connId connAlive = do
  print "Inside API Close"
  isAlive <- readIORef connAlive
  case isAlive of
    False -> throwIO $ TransportError SendClosed "apiClose: connAlive = False"
    True  -> do
      let ourAddress = localAddress
      print "apiClose: after closeRemote"
      withValidLocalState_ lep $ \ValidLocalEndPointState{..} ->
          publish _localChannel ep (MessageCloseConnection ourAddress connId)
      print "apiClose: after publishing"
      writeIORef connAlive False

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
