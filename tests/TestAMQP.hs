{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Applicative
import Network.Transport
import Network.Transport.AMQP
import Network.AMQP
import Network.Transport.Tests
import Network.Transport.Tests.Auxiliary (runTests)

main :: IO ()
main = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  let amqpParameters = AMQPParameters conn "simple-multicast" Nothing
  testTransportCompliance (Right <$> (createTransport amqpParameters))

-- | These tests that our transport layer it's compliant to the 
-- official specification.
testTransportCompliance :: IO (Either String Transport) -> IO ()
testTransportCompliance newTransport = do
  Right transport <- newTransport
  runTests
    [ 
 -- P  ("PingPong",              testPingPong transport numPings)
 -- P    , ("EndPoints",             testEndPoints transport numPings)
 -- P    , ("Connections",           testConnections transport numPings)
 -- P    , ("CloseOneConnection",    testCloseOneConnection transport numPings)
 -- P    , ("CloseOneDirection",     testCloseOneDirection transport numPings)
 -- P    , ("CloseReopen",           testCloseReopen transport numPings)
 -- P    , ("ParallelConnects",      testParallelConnects transport (numPings * 2))
 -- P    , ("SendAfterClose",        testSendAfterClose transport (numPings * 2))
 -- P    , ("Crossing",              testCrossing transport numPings)
 -- P    , ("CloseTwice",            testCloseTwice transport (numPings * 2))
 -- P    , ("ConnectToSelf",         testConnectToSelf transport numPings)
 -- P    , ("ConnectToSelfTwice",    testConnectToSelfTwice transport numPings)
 -- P      ("CloseSelf",             testCloseSelf newTransport)
 -- P     ("CloseEndPoint",         testCloseEndPoint transport numPings)
 -- P      ("CloseTransport",        testCloseTransport newTransport)
-- H    , ("ExceptionOnReceive",    testExceptionOnReceive newTransport)
      ("SendException",         testSendException newTransport)
-- P     , ("Kill",                  testKill newTransport numPings)
    ]
  -- The network-transport-tests testsuite doesn't close any transport,
  -- and in the context of RabbitMQ it leads to dangling queues.
  -- TODO: Fix the aforementioned limitation.
  closeTransport transport
  where
    numPings = 2 :: Int
