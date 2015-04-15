{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Network.Transport.AMQP.Internal.Types
  (module Network.Transport.AMQP.Internal.Types) where

import qualified Network.AMQP as AMQP
import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import Data.IORef
import GHC.Generics (Generic)
import Data.ByteString (ByteString)
import Data.Serialize
import Network.Transport
import Control.Concurrent.MVar
import Control.Exception
import Control.Concurrent.Chan (Chan)

import Lens.Family2.TH
import Lens.Family2

--------------------------------------------------------------------------------
-- Data Types
-- Largely inspired to: `network-transport-zeromq` by Tweag I/O and
-- `network-transport-tcp` by Well Typed et al.
--------------------------------------------------------------------------------

data AMQPParameters = AMQPParameters {
    transportConnection :: AMQP.Connection
  , transportMultiCastEndpoint :: !T.Text
  , transportEndpoint :: !(Maybe T.Text)
    -- ^ The queue and exchange name. If not specified, will be randomised.
  }

--------------------------------------------------------------------------------
data TransportState
  = TransportValid ValidTransportState
  | TransportClosed

--------------------------------------------------------------------------------
data ValidTransportState = ValidTransportState {
    _tstateConnection :: AMQP.Connection
  , _tstateEndPoints :: !(Map EndPointAddress LocalEndPoint)
  }

--------------------------------------------------------------------------------
data AMQPInternalState = AMQPInternalState {
    istate_params :: !AMQPParameters
  , istate_tstate :: !(MVar TransportState)
  }

--------------------------------------------------------------------------------
data LocalEndPoint = LocalEndPoint
  { localAddress :: !EndPointAddress
  , localExchange :: !AMQPExchange
  , localDone     :: !(MVar ())
  , localState   :: !(MVar LocalEndPointState)
  }

--------------------------------------------------------------------------------
data LocalEndPointState =
    LocalEndPointValid !ValidLocalEndPointState
  | LocalEndPointClosed

--------------------------------------------------------------------------------
data ValidLocalEndPointState = ValidLocalEndPointState
  {
    _localChan         :: !(Chan Event)
  , _localChannel      :: !AMQP.Channel
  , _localOpened       :: !(IORef Bool)
  , _localConnections  :: !(Counter ConnectionId AMQPConnection)
  , _localRemotes      :: !(Map EndPointAddress RemoteEndPoint)
  }

--------------------------------------------------------------------------------
data Counter a b = Counter 
  { _cntNext :: !a
  , _cntValue :: !(Map a b)
  }

--------------------------------------------------------------------------------
newCounter :: Counter ConnectionId AMQPConnection
newCounter = Counter 0 Map.empty

--------------------------------------------------------------------------------
data AMQPConnection = AMQPConnection 
  { _connectionLocalEndPoint  :: !LocalEndPoint
  , _connectionRemoteEndPoint :: !RemoteEndPoint
  , _connectionReliability    :: !Reliability
  , _connectionState          :: !(MVar AMQPConnectionState)
  , _connectionReady          :: !(MVar ())
  }

newtype AMQPExchange = AMQPExchange T.Text deriving (Show, Eq)

--------------------------------------------------------------------------------
data AMQPConnectionState = 
    AMQPConnectionInit
  | AMQPConnectionValid !ValidAMQPConnection
  | AMQPConnectionClosed
  | AMQPConnectionFailed

--------------------------------------------------------------------------------
data ValidAMQPConnection = ValidAMQPConnection
  { _amqpExchange :: !(Maybe AMQPExchange)
  , _amqpChannel :: !(Maybe AMQP.Channel)
  , _amqpConnectionId :: !ConnectionId
  }

--------------------------------------------------------------------------------
data RemoteEndPoint = RemoteEndPoint
  { remoteAddress :: !EndPointAddress
  , remoteState   :: !(MVar RemoteEndPointState)
  , remoteOpened  :: !(IORef Bool)
  }

--------------------------------------------------------------------------------
data ClosingRemoteEndPoint = ClosingRemoteEndPoint 
  { _closingRemoteExchange :: !AMQPExchange
  , _closingRemoteChannel  :: !AMQP.Channel
  , _closingRemoteDone :: !(MVar ())
  }

--------------------------------------------------------------------------------
data RemoteEndPointState
  = RemoteEndPointValid ValidRemoteEndPointState
  | RemoteEndPointClosed
  | RemoteEndPointFailed
  | RemoteEndPointPending (IORef [RemoteEndPointState -> IO RemoteEndPointState])
  | RemoteEndPointClosing ClosingRemoteEndPoint

--------------------------------------------------------------------------------
data ValidRemoteEndPointState = ValidRemoteEndPointState
  { _remoteExchange :: !AMQPExchange
  , _remoteChannel  :: !AMQP.Channel
  , _remotePendingConnections :: !(Counter ConnectionId AMQPConnection)
  , _remoteIncomingConnections :: !(Set ConnectionId)
  , _remoteOutgoingCount :: !Int
  }

--------------------------------------------------------------------------------
makeLenses ''ValidTransportState
makeLenses ''ValidLocalEndPointState
makeLenses ''ValidRemoteEndPointState
makeLenses ''AMQPConnection
makeLenses ''ValidAMQPConnection
makeLenses ''Counter

--------------------------------------------------------------------------------
-- Lenses
--
--------------------------------------------------------------------------------
localConnectionAt :: Phantom f => ConnectionId -> LensLike' f ValidLocalEndPointState (Maybe AMQPConnection)
localConnectionAt idx = localConnections . cntValue . to (Map.lookup idx)

localRemoteAt :: Phantom f => EndPointAddress -> LensLike' f ValidLocalEndPointState (Maybe RemoteEndPoint)
localRemoteAt eA = localRemotes . to (Map.lookup eA)

--------------------------------------------------------------------------------
data AMQPMessage
  = MessageConnect !EndPointAddress -- ^ Connection greeting
  | MessageInitConnection !EndPointAddress !ConnectionId !Reliability
  | MessageInitConnectionOk !EndPointAddress !ConnectionId !ConnectionId
  | MessageCloseConnection !ConnectionId
  | MessageData !ConnectionId ![ByteString]
  | MessageEndPointClose   !EndPointAddress !Bool
  | MessageEndPointCloseOk !EndPointAddress
  | PoisonPill
  deriving (Show, Generic)

deriving instance Generic EndPointAddress
instance Serialize EndPointAddress
deriving instance Generic Reliability
instance Serialize Reliability
instance Serialize AMQPMessage

data InvariantViolated = 
    InvariantViolated InvariantViolation
  | IncorrectState String
  deriving Show

data InvariantViolation =
    RemoteEndPointLookupFailed EndPointAddress
  | RemoteEndPointCannotBePending EndPointAddress
  | RemoteEndPointShouldBeValidOrClosed EndPointAddress
  | RemoteEndPointMustBeValid EndPointAddress
  | LocalEndPointMustBeValid EndPointAddress
  deriving Show

instance Exception InvariantViolated
