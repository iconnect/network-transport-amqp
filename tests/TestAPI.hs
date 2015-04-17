{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Monad ( replicateM )

import Network.Transport
import Network.Transport.AMQP
import Network.AMQP
import Test.Tasty
import Test.Tasty.HUnit
import Control.Concurrent

main :: IO ()
main = defaultMain $
  testGroup "API tests"
      [ testCase "simple" test_simple
      -- , testCase "connection break" test_connectionBreak
      -- , testCase "test multicast" test_multicast
      -- , testCase "connect to non existent host" test_nonexists
      ]

newTransport :: IO Transport
newTransport = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  let amqpTransport = AMQPParameters conn "simple-multicast" Nothing
  createTransport amqpTransport

test_simple :: IO ()
test_simple = do
    transport <- newTransport
    Right ep1 <- newEndPoint transport
    Right ep2 <- newEndPoint transport
    Right c1  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
    Right c2  <- connect ep2 (address ep1) ReliableOrdered defaultConnectHints
    Right _   <- send c1 ["123"]
    Right _   <- send c2 ["321"]
    close c1
    close c2
    [ConnectionOpened _ ReliableOrdered _, Received _ ["321"], ConnectionClosed _] <- replicateM 3 $ receive ep1
    [ConnectionOpened _ ReliableOrdered _, Received _ ["123"], ConnectionClosed _] <- replicateM 3 $ receive ep2
    closeTransport transport

test_connectionBreak :: IO ()
test_connectionBreak = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  let amqpTransport = AMQPParameters conn "simple-multicast" Nothing
  (amqp, transport) <- createTransportExposeInternals amqpTransport
  Right ep1 <- newEndPoint transport
  Right ep2 <- newEndPoint transport
  Right ep3 <- newEndPoint transport

  Right c21  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
  Right c22  <- connect ep2 (address ep1) ReliableOrdered defaultConnectHints
  Right c23  <- connect ep3 (address ep1) ReliableOrdered defaultConnectHints

  ConnectionOpened 1 ReliableOrdered _ <- receive ep2
  ConnectionOpened 1 ReliableOrdered _ <- receive ep1
  ConnectionOpened 2 ReliableOrdered _ <- receive ep1

  breakConnection amqp (address ep1) (address ep2)

  ErrorEvent (TransportError (EventConnectionLost _ ) _) <- receive $ ep1
  ErrorEvent (TransportError (EventConnectionLost _ ) _) <- receive $ ep2

  Left (TransportError SendFailed _) <- send c21 ["test"]
  Left (TransportError SendFailed _) <- send c22 ["test"]

  Left (TransportError SendFailed _) <- send c23 ["test"]
  ErrorEvent (TransportError (EventConnectionLost _) _ ) <- receive ep1
  Right c24 <- connect ep3 (address ep1) ReliableOrdered defaultConnectHints
  Right ()  <- send c24 ["final"]
  ConnectionOpened 3 ReliableOrdered _ <- receive ep1
  Received 3 ["final"] <- receive ep1
  closeTransport transport

test_multicast :: IO ()
test_multicast = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  let amqpTransport = AMQPParameters conn "simple-multicast" Nothing
  transport <- createTransport amqpTransport
  Right ep1 <- newEndPoint transport
  Right ep2 <- newEndPoint transport
  Right g1 <- newMulticastGroup ep1
  multicastSubscribe g1
  threadDelay 1000000
  multicastSend g1 ["test"]
  ReceivedMulticast _ ["test"] <- receive ep1
  Right g2 <- resolveMulticastGroup ep2 (multicastAddress g1)
  multicastSubscribe g2
  threadDelay 100000
  multicastSend g2 ["test-2"]
  ReceivedMulticast _ ["test-2"] <- receive ep2
  ReceivedMulticast _ ["test-2"] <- receive ep1
  return ()

test_nonexists :: IO ()
test_nonexists = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  let amqpTransport = AMQPParameters conn "simple-multicast" Nothing
  tr <- createTransport amqpTransport
  Right ep <- newEndPoint tr
  Left (TransportError ConnectFailed _) <- connect ep (EndPointAddress "tcp://129.0.0.1:7684") ReliableOrdered defaultConnectHints
  closeTransport tr
