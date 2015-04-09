{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-unused-binds #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{--|
  A Network Transport Layer for `distributed-process`
  based on AMQP and single-owner queues
--}

module Network.Transport.AMQP (
    createTransport
  , AMQPParameters(..)
  ) where

import qualified Network.AMQP as AMQP
import qualified Data.Text as T
import Data.UUID.V4
import Data.List (foldl1')
import Data.UUID (toString, toWords)
import Data.Bits
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import GHC.Generics (Generic)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.String.Conv
import Data.Serialize
import Data.Monoid
import Network.Transport
import Network.Transport.Internal (asyncWhenCancelled)
import Control.Concurrent.MVar
import Control.Applicative
import Control.Monad
import Control.Exception
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)

import Lens.Family2
import Lens.Family2.TH

--------------------------------------------------------------------------------
data AMQPParameters = AMQPParameters {
    transportConnection :: AMQP.Connection
  , transportMultiCastEndpoint :: !T.Text
  , transportEndpoint :: !(Maybe T.Text)
    -- ^ The queue and exchange name. If not specified, will be randomised.
  }

--------------------------------------------------------------------------------
data TransportState
  = TransportValid AMQP.Connection
  | TransportClosed


--------------------------------------------------------------------------------
data AMQPInternalState = AMQPInternalState {
    _istate_params :: !AMQPParameters
  , _istate_tstate :: !(MVar TransportState)
  }

makeLenses ''AMQPInternalState

--------------------------------------------------------------------------------
-- | Largely inspired to: `network-transport-zeromq` by Tweag I/O and
-- `network-transport-tcp` by Well Typed et al.
data AMQPMessage
  = MessageConnect !EndPointAddress -- ^ Connection greeting
  | MessageInitConnection !EndPointAddress !ConnectionId !Reliability
  | MessageInitConnectionOk !EndPointAddress !ConnectionId !ConnectionId
  | MessageCloseConnection !EndPointAddress !ConnectionId
  | MessageData !ConnectionId ![ByteString]
  | MessageEndPointClose   !EndPointAddress !ConnectionId
  | MessageEndPointCloseOk !EndPointAddress
  deriving (Show, Generic)

deriving instance Generic EndPointAddress
instance Serialize EndPointAddress
deriving instance Generic Reliability
instance Serialize Reliability
instance Serialize AMQPMessage

data InvariantViolated = 
  InvariantViolated InvariantViolation
  deriving Show

data InvariantViolation =
  EndPointNotInRemoteMap EndPointAddress
  deriving Show

instance Exception InvariantViolated

--------------------------------------------------------------------------------
-- | Data created by the `Transport` during bootstrap. The rationale is that
-- we need to emulate point-to-point communication in RabbitMQ via a "direct"
-- exchange and a single ownership queue. We also want to ensure the exchange
-- gets deleted every time an `Endpoint` disconnects.
data AMQPContext = AMQPContext {
    ctx_evtChan :: !(Chan Event)
  , ctx_channel  :: AMQP.Channel
  -- ^ TODO: This might need to become an MVar to be reopened/swapped
  -- upon exceptions.
  , ctx_endpoint :: !T.Text
  , ctx_state :: !(MVar LocalEndPointState)
  }

data LocalEndPointState =
    LocalEndPointValid !ValidLocalEndPointState
  | LocalEndPointNoAcceptConections
  | LocalEndPointClosed

data RemoteEndPoint = RemoteEndPoint
  { remoteAddress :: !EndPointAddress
  , remoteId      :: !ConnectionId
  , remoteState   :: !(MVar RemoteEndPointState)
  }

data RemoteEndPointState
  = RemoteEndPointValid
  | RemoteEndPointClosed


data ValidLocalEndPointState = ValidLocalEndPointState
  {
    _localConnections  :: !(Map EndPointAddress RemoteEndPoint)
  }

makeLenses ''ValidLocalEndPointState

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
    let AMQPParameters{..} = is ^. istate_params
    newChannel <- AMQP.openChannel transportConnection
    uuid <- toS . toString <$> nextRandom
    (ourEndPoint,_,_) <- AMQP.declareQueue newChannel $ AMQP.newQueue {
                           AMQP.queueName = maybe uuid toS transportEndpoint
                         , AMQP.queuePassive = False
                         , AMQP.queueDurable = False
                         , AMQP.queueExclusive = True
                         }
    -- TODO: Is this a bad idea? Reuse as exchange name the random queue
    -- generated by RabbitMQ
    let ourExchange = ourEndPoint
    ctx@AMQPContext{..} <- newAMQPCtx newChannel ourExchange
    AMQP.declareExchange newChannel $ AMQP.newExchange {
        AMQP.exchangeName = ourExchange
      , AMQP.exchangeType = "direct"
      , AMQP.exchangePassive = False
      , AMQP.exchangeDurable = False
      , AMQP.exchangeAutoDelete = True
    }

    AMQP.bindQueue newChannel ourEndPoint ourExchange mempty

    startReceiver is ctx

    return EndPoint
      { receive       = readChan ctx_evtChan
      , address       = EndPointAddress $ toS ourEndPoint
      , connect       = apiConnect is ctx
      , closeEndPoint = let evs = [ EndPointClosed
                                  , throw $ userError "Endpoint closed"
                                  ] in
                        apiCloseEndPoint is ctx evs ourEndPoint
      , newMulticastGroup     = return . Left $ newMulticastGroupError
      , resolveMulticastGroup = return . Left . const resolveMulticastGroupError
      }
  where
    newMulticastGroupError =
      TransportError NewMulticastGroupUnsupported "Multicast not supported"
    resolveMulticastGroupError =
      TransportError ResolveMulticastGroupUnsupported "Multicast not supported"

--------------------------------------------------------------------------------
startReceiver :: AMQPInternalState -> AMQPContext -> IO ()
startReceiver tr@AMQPInternalState{..} ctx@AMQPContext{..} = do
  void $ AMQP.consumeMsgs ctx_channel ctx_endpoint AMQP.NoAck $ \(msg,_) -> do
    case decode' msg of
      Left _ -> return ()
      Right v@(MessageInitConnection theirAddr theirId rel) -> do
        print v
        -- TODO: Do I need to persist this RemoteEndPoint with the id given to me?
        (rep, isNew) <- findRemoteEndPoint ctx theirAddr
        when isNew $ do
          let ourId = remoteId rep
          publish ctx_channel theirAddr (MessageInitConnectionOk (toAddress ctx_endpoint) ourId theirId)
          -- TODO: This is a bug?. I need to issue a ConnectionOpened with the
          -- internal counter I am keeping, not the one coming from the remote
          -- endpoint.
          writeChan ctx_evtChan $ ConnectionOpened theirId rel theirAddr
      Right (MessageData cId rawMsg) -> do
        writeChan ctx_evtChan $ Received cId rawMsg
      Right v@(MessageInitConnectionOk theirAddr theirId ourId) -> do
        -- TODO: This smells
        print v
        writeChan ctx_evtChan $ ConnectionOpened theirId ReliableOrdered theirAddr
      Right (MessageCloseConnection theirAddr theirId) -> do
        print "MessageCloseConnection"
        ourId <- cleanupRemoteConnection tr ctx theirAddr
        writeChan ctx_evtChan $ ConnectionClosed theirId
      Right (MessageEndPointClose theirAddr theirId) -> do
        ourId <- cleanupRemoteConnection tr ctx theirAddr
        print "MessageEndPointClose"
        writeChan ctx_evtChan $ ConnectionClosed theirId
      rst -> print rst

--------------------------------------------------------------------------------
withValidLocalState_ :: AMQPContext
                     -> (ValidLocalEndPointState -> IO ())
                     -> IO ()
withValidLocalState_ AMQPContext{..} f = withMVar ctx_state $ \st ->
  case st of
    LocalEndPointClosed -> return ()
    LocalEndPointNoAcceptConections -> return ()
    LocalEndPointValid v -> f v

--------------------------------------------------------------------------------
modifyValidLocalState :: AMQPContext
                       -> (ValidLocalEndPointState -> IO (LocalEndPointState, b))
                       -> IO b
modifyValidLocalState AMQPContext{..} f = modifyMVar ctx_state $ \st ->
  case st of
    LocalEndPointClosed -> 
      throw $ userError "withValidLocalState: LocalEndPointClosed"
    LocalEndPointNoAcceptConections ->
      throw $ userError "withValidLocalState: LocalEndPointNoAcceptConnections"
    LocalEndPointValid v -> f v

--------------------------------------------------------------------------------
newAMQPCtx :: AMQP.Channel -> T.Text -> IO AMQPContext
newAMQPCtx amqpCh ep = do
  ch <- newChan
  st <- newMVar emptyState
  return $ AMQPContext ch amqpCh ep st
  where
    emptyState :: LocalEndPointState
    emptyState = LocalEndPointValid $ ValidLocalEndPointState Map.empty

--------------------------------------------------------------------------------
apiCloseEndPoint :: AMQPInternalState -> AMQPContext -> [Event] -> T.Text -> IO ()
apiCloseEndPoint AMQPInternalState{..} ctx@AMQPContext{..} evts ourEp = do
  let ourAddress = toAddress ourEp

  -- Notify all the remoters this EndPoint is dying.
  withValidLocalState_ ctx $ \vst -> do
    print (Map.keys $ vst ^. localConnections)
    forM_ (Map.toList $ vst ^. localConnections) $ \(theirAddress, rep) ->
      publish ctx_channel theirAddress (MessageEndPointClose ourAddress (remoteId rep))

  -- Close the given connection
  forM_ evts (writeChan ctx_evtChan)
  _ <- AMQP.deleteQueue ctx_channel ctx_endpoint
  AMQP.deleteExchange ctx_channel ctx_endpoint
  modifyMVar_ ctx_state $ return . const LocalEndPointClosed

--------------------------------------------------------------------------------
cleanupRemoteConnection :: AMQPInternalState
                        -> AMQPContext
                        -> EndPointAddress
                        -> IO ConnectionId
cleanupRemoteConnection AMQPInternalState{..} ctx@AMQPContext{..} theirAddress = do
  let ourAddress = toAddress ctx_endpoint
  modifyValidLocalState ctx $ \vst -> case Map.lookup theirAddress (vst ^. localConnections) of
    Nothing -> throwIO $ InvariantViolated (EndPointNotInRemoteMap theirAddress)
    Just rep -> do
      let ourId = remoteId rep
      -- When we first asked to cleanup a remote connection, we do not delete it
      -- immediately; conversely, be set its state to close and if the state was already
      -- closed we delete it. This allows a RemoteEndPoint to be marked in closing state,
      -- but to still be listed so that can receive subsequent notifications, like for
      -- example the ConnectionClosed ones.
      wasAlreadyClosed <- modifyMVar (remoteState rep) $ \rst -> case rst  of
        RemoteEndPointValid  -> return (RemoteEndPointClosed, False)
        RemoteEndPointClosed -> return (RemoteEndPointClosed, True)
      let newStateSetter mp = case wasAlreadyClosed of
                                True -> Map.delete theirAddress mp
                                False -> mp
      return (LocalEndPointValid $ over localConnections newStateSetter vst, ourId)

--------------------------------------------------------------------------------
toAddress :: T.Text -> EndPointAddress
toAddress = EndPointAddress . toS

--------------------------------------------------------------------------------
-- | Connnect to a remote `Endpoint`. What this means in the context of RabbitMQ
-- is that we are given the Exchange we need to publish to. To be sure the
-- Exchange do exist, we force its creation. The `declareExchange` command is
-- idempontent in Network.AMQP.
-- We certainly want to store the information that this `Endpoint` wanted to
-- communicate with the remote one.
apiConnect :: AMQPInternalState
           -> AMQPContext
           -> EndPointAddress  -- ^ Remote address
           -> Reliability      -- ^ Reliability (ignored)
           -> ConnectHints     -- ^ Hints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect tr@AMQPInternalState{..} ctx@AMQPContext{..} theirAddress reliability _ = do
  let ourAddress = toAddress ctx_endpoint
  try . asyncWhenCancelled close $
    if ourAddress == theirAddress
    then connectToSelf ctx
    else do
    (rep, isNew) <- findRemoteEndPoint ctx theirAddress
    let cId = remoteId rep
    print $ "apiConnect cId: " ++ show cId
    print $ "apiConnect new: " ++ show isNew
    when isNew $ do
        let msg = MessageInitConnection ourAddress cId reliability
        publish ctx_channel theirAddress msg
    return Connection {
                  send = apiSend tr ctx theirAddress cId
                , close = apiClose tr ctx theirAddress cId
                }

-- | Find a remote endpoint. If the remote endpoint does not yet exist we
-- create it. Returns if the endpoint was new.
findRemoteEndPoint :: AMQPContext
                   -> EndPointAddress
                   -> IO (RemoteEndPoint, Bool)
findRemoteEndPoint ctx@AMQPContext{..} theirAddr = modifyMVar ctx_state $ \st -> do
  case st of
    LocalEndPointClosed -> 
      throwIO $ TransportError ConnectFailed "findRemoteEndPoint: LocalEndPointClosed"
    LocalEndPointNoAcceptConections ->
      throw $ userError "findRemoteEndpoint: local endpoint doesn't accept connections."
    LocalEndPointValid v ->
      case Map.lookup theirAddr (v ^. localConnections) of
        -- TODO: Check if the RemoteEndPoint is closed.
        Just r -> return (LocalEndPointValid v, (r, False))
        Nothing -> do
          newRem <- newValidRemoteEndpoint ctx theirAddr
          let newMap = Map.insert theirAddr newRem
          return (LocalEndPointValid $ over localConnections newMap v, (newRem, True))

--------------------------------------------------------------------------------
newValidRemoteEndpoint :: AMQPContext -> EndPointAddress -> IO RemoteEndPoint
newValidRemoteEndpoint ctx@AMQPContext{..} ep = do
  -- TODO: Experimental: do a bitwise operation on the UUID to generate
  -- a random ConnectionId. Is this safe?
  let queueAsWord64 = foldl1' (+) (map fromIntegral $ B.unpack . toS $ ctx_endpoint)
  (a,b,c,d) <- toWords <$> nextRandom
  let cId = fromIntegral (a .|. b .|. c .|. d) + queueAsWord64
  var <- newMVar RemoteEndPointValid
  return $ RemoteEndPoint ep cId var

--------------------------------------------------------------------------------
connectFailed :: SomeException -> TransportError ConnectErrorCode
connectFailed = TransportError ConnectFailed . show

--------------------------------------------------------------------------------
-- TODO: Deal with exceptions.
connectToSelf :: AMQPContext -> IO Connection
connectToSelf ctx@AMQPContext{..} = do
    let ourEndPoint = toAddress ctx_endpoint
    (rep, _) <- findRemoteEndPoint ctx ourEndPoint
    let cId = remoteId rep
    print cId
    writeChan ctx_evtChan $ ConnectionOpened cId ReliableOrdered ourEndPoint
    return Connection { 
        send  = selfSend cId
      , close = selfClose cId
    }
  where
    selfSend :: ConnectionId
             -> [ByteString]
             -> IO (Either (TransportError SendErrorCode) ())
    selfSend connId msg =
      try . withMVar ctx_state $ \st -> case st of
        LocalEndPointValid _ -> do
            writeChan ctx_evtChan (Received connId msg)
        LocalEndPointNoAcceptConections -> do
          throwIO $ TransportError SendClosed "selfSend: Connections no more."
        LocalEndPointClosed ->
          throwIO $ TransportError SendFailed "selfSend: Connection closed"

    selfClose :: ConnectionId -> IO ()
    selfClose connId = do
      modifyMVar_ ctx_state $ \st -> case st of
        LocalEndPointValid _ -> do
          writeChan ctx_evtChan (ConnectionClosed connId)
          return LocalEndPointNoAcceptConections
        LocalEndPointNoAcceptConections ->
          throwIO $ TransportError SendFailed "selfClose: No connections accepted"
        LocalEndPointClosed -> 
          throwIO $ TransportError SendClosed "selfClose: Connection closed"

--------------------------------------------------------------------------------
publish :: AMQP.Channel 
        -> EndPointAddress 
        -> AMQPMessage 
        -> IO ()
publish transportChannel address msg = do
    AMQP.publishMsg transportChannel
                    (toS . endPointAddressToByteString $ address)
                    mempty
                    (AMQP.newMsg { AMQP.msgBody = encode' msg
                                 , AMQP.msgDeliveryMode = Just AMQP.NonPersistent
                                 })

--------------------------------------------------------------------------------
-- TODO: Deal with exceptions and error at the broker level.
apiSend :: AMQPInternalState
        -> AMQPContext
        -> EndPointAddress
        -> ConnectionId
        -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend is ctx@AMQPContext{..} their connId msgs = do
  try . withMVar (is ^. istate_tstate) $ \tst -> case tst of
    TransportClosed -> 
      throwIO $ TransportError SendFailed "apiSend: TransportClosed"
    TransportValid _ -> withValidLocalState_ ctx $ \vst -> case Map.lookup their (vst ^. localConnections) of
      Nothing  -> throwIO $ TransportError SendFailed "apiSend: address not in local connections"
      Just rep -> withMVar (remoteState rep) $ \rst -> case rst of
        RemoteEndPointClosed -> throwIO $ TransportError SendFailed "apiSend: Connection closed"
        RemoteEndPointValid ->  publish ctx_channel their (MessageData connId msgs)

--------------------------------------------------------------------------------
-- | Change the status of this `Endpoint` to be closed
-- TODO: If a Remote EndPoint is closed, reopen it afterwards?
apiClose :: AMQPInternalState
         -> AMQPContext
         -> EndPointAddress
         -> ConnectionId
         -> IO ()
apiClose tr@AMQPInternalState{..} ctx@AMQPContext{..} ep connId = do
  let ourAddress = toAddress ctx_endpoint
  _ <- cleanupRemoteConnection tr ctx ep
  publish ctx_channel ep (MessageCloseConnection ourAddress connId)

--------------------------------------------------------------------------------
createTransport :: AMQPParameters -> IO Transport
createTransport params@AMQPParameters{..} = do
  tState <- newMVar (TransportValid transportConnection)
  let iState = AMQPInternalState params tState
  return Transport {
    newEndPoint = apiNewEndPoint iState
  , closeTransport = apiCloseTransport iState
  }

--------------------------------------------------------------------------------
apiCloseTransport :: AMQPInternalState -> IO ()
apiCloseTransport is = do
  modifyMVar_ (is ^. istate_tstate) $ \tst -> case tst of
    TransportClosed -> return TransportClosed
    TransportValid cnn -> do
      -- TODO: Broadcast to die to all the peers.
      AMQP.closeConnection cnn
      return $ TransportClosed
