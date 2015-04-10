{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Network.Transport.AMQP.Internal.Types
  (module Network.Transport.AMQP.Internal.Types) where

import qualified Network.AMQP as AMQP
import qualified Data.Text as T
import Data.Map.Strict (Map)
import GHC.Generics (Generic)
import Data.ByteString (ByteString)
import Data.Serialize
import Network.Transport
import Control.Concurrent.MVar
import Control.Exception
import Control.Concurrent.Chan (Chan)

import Lens.Family2.TH

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
    tstateConnection :: AMQP.Connection
  , tstateEndPoints :: !(Map EndPointAddress LocalEndPoint)
  }

--------------------------------------------------------------------------------
data AMQPInternalState = AMQPInternalState {
    istate_params :: !AMQPParameters
  , istate_tstate :: !(MVar TransportState)
  }

--------------------------------------------------------------------------------
data LocalEndPoint = LocalEndPoint
  { localAddress :: !EndPointAddress
  , localState   :: !(MVar LocalEndPointState)
  }

--------------------------------------------------------------------------------
data LocalEndPointState =
    LocalEndPointValid !ValidLocalEndPointState
  | LocalEndPointNoAcceptConections
  | LocalEndPointClosed

--------------------------------------------------------------------------------
data RemoteEndPoint = RemoteEndPoint
  { remoteAddress :: !EndPointAddress
  , remoteId      :: !ConnectionId
  , remoteState   :: !(MVar RemoteEndPointState)
  , remoteOutgoingConnections :: !Int
  }

--------------------------------------------------------------------------------
data RemoteEndPointState
  = RemoteEndPointValid
  | RemoteEndPointClosed

--------------------------------------------------------------------------------
data ValidLocalEndPointState = ValidLocalEndPointState
  {
    _localChan         :: !(Chan Event)
  , _localChannel      :: AMQP.Channel
  , _localConnections  :: !(Map EndPointAddress RemoteEndPoint)
  }

makeLenses ''ValidLocalEndPointState

--------------------------------------------------------------------------------
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
