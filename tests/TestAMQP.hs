{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-} -- Cut noise for post-AMP imports
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
  testTransportCompliance (Right <$> createTransport amqpParameters)

-- | These tests that our transport layer it's compliant to the 
-- official specification.
testTransportCompliance :: IO (Either String Transport) -> IO ()
testTransportCompliance newTransport = do
  Right transport <- newTransport
  runTests
    [
      ("PingPong",              testPingPong transport numPings)
    , ("EndPoints",             testEndPoints transport numPings)
    , ("Connections",           testConnections transport numPings)
    , ("CloseOneConnection",    testCloseOneConnection transport numPings)
    , ("CloseOneDirection",     testCloseOneDirection transport numPings)
    , ("CloseReopen",           testCloseReopen transport numPings)
    , ("ParallelConnects",      testParallelConnects transport 50)
    , ("SendAfterClose",        testSendAfterClose transport 50)
    , ("Crossing",              testCrossing transport numPings)
    , ("CloseTwice",            testCloseTwice transport 50)
    , ("ConnectToSelf",         testConnectToSelf transport numPings)
    , ("ConnectToSelfTwice",    testConnectToSelfTwice transport numPings)
    , ("CloseSelf",             testCloseSelf newTransport)
    , ("CloseEndPoint",         testCloseEndPoint transport numPings)
    , ("CloseTransport",        testCloseTransport newTransport)
    , ("ExceptionOnReceive",    testExceptionOnReceive newTransport)
    , ("SendException",         testSendException newTransport)
    , ("Kill",                  testKill newTransport 20)
                                -- testKill test have a timeconstraint so n-t-amqp
                                -- fails to work with required speed, we need to
                                -- reduce a number of tests here. (Same limitation as n-t-0MQ)
    ]
  where
    numPings = 500 :: Int
