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
    [ ("PingPong",              testPingPong transport numPings)
      , ("EndPoints",             testEndPoints transport numPings)
--     , ("Connections",           testConnections transport numPings)
--      , ("CloseOneConnection",    testCloseOneConnection transport numPings)
--     , ("CloseOneDirection",     testCloseOneDirection transport numPings)
--    , ("CloseReopen",           testCloseReopen transport numPings)
-- invariant violated    , ("ParallelConnects",      testParallelConnects transport 10)
--   , ("SendAfterClose",        testSendAfterClose transport 100)
--   , ("Crossing",              testCrossing transport 10)
--   , ("CloseTwice",            testCloseTwice transport 100)
     , ("ConnectToSelf",         testConnectToSelf transport numPings)
-- Invalid: connection closed     , ("ConnectToSelfTwice",    testConnectToSelfTwice transport numPings)
     , ("CloseSelf",             testCloseSelf newTransport)
--   , ("CloseEndPoint",         testCloseEndPoint transport numPings)
--   , ("CloseTransport",        testCloseTransport newTransport)
--   , ("ExceptionOnReceive",    testExceptionOnReceive newTransport)
--   , ("SendException",         testSendException newTransport)
--   , ("Kill",                  testKill newTransport 80)
    ]
  where
    numPings = 1 :: Int
