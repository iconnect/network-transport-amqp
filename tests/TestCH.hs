{-# LANGUAGE OverloadedStrings #-}

module Main where

import TEST_SUITE_MODULE (tests)

import Network.Transport.Test (TestTransport(..))
import Network.AMQP
import Network.Transport.AMQP
  ( createTransportExposeInternals
  , breakConnection
  , AMQPParameters(..)
  )
import Test.Framework (defaultMain)

main :: IO ()
main = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  let amqpParameters = AMQPParameters conn "simple-multicast" Nothing
  (amqt, transport) <- createTransportExposeInternals amqpParameters
  defaultMain =<< tests TestTransport
    { testTransport = transport
    , testBreakConnection = breakConnection amqt
    }
