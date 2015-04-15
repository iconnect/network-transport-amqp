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
      , ("Connections",           testConnections transport numPings)
      , ("CloseOneConnection",    testCloseOneConnection transport numPings)
      , ("CloseOneDirection",     testCloseOneDirection transport numPings)
      , ("CloseReopen",           testCloseReopen transport numPings)
      , ("ParallelConnects",      testParallelConnects transport (numPings * 2))
      , ("SendAfterClose",        testSendAfterClose transport (numPings * 2))
      , ("Crossing",              testCrossing transport numPings)
      , ("CloseTwice",            testCloseTwice transport (numPings * 2))
      , ("ConnectToSelf",         testConnectToSelf transport numPings)
      , ("ConnectToSelfTwice",    testConnectToSelfTwice transport numPings)
-- F      , ("CloseSelf",             testCloseSelf newTransport)
-- F , ("CloseEndPoint",         testCloseEndPoint transport numPings)
-- F      , ("CloseTransport",        testCloseTransport newTransport)
-- H    , ("ExceptionOnReceive",    testExceptionOnReceive newTransport)
-- F     , ("SendException",         testSendException newTransport)
      , ("Kill",                  testKill newTransport numPings)
    ]
  where
    numPings = 2 :: Int
