{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
import System.Environment (getArgs)
import Network.Transport
import Network.Transport.AMQP (createTransport, AMQPParameters(..))
import Network.AMQP (openConnection)
import Control.Monad.State (evalStateT, modify, get)
import Control.Monad (forever)
import Control.Exception
import Control.Monad.IO.Class (liftIO)
import qualified Data.Map.Strict as Map (empty, insert, delete, elems)
import qualified Data.ByteString.Char8 as BSC (pack)
import qualified Data.Text as T

import Data.Monoid

main :: IO ()
main = do
  server:_ <- getArgs
  conn <- openConnection "localhost" "/" "guest" "guest"
  let amqpTransport = AMQPParameters conn "multicast" (Just . T.pack $ server)
  transport <- createTransport amqpTransport
  Right endpoint  <- newEndPoint transport

  putStrLn $ "Chat server ready at " ++ (show . endPointAddressToByteString . address $ endpoint)
  
  go endpoint `catch` \(e :: SomeException) -> do
    print e
    closeEndPoint endpoint

  where
   go endpoint = flip evalStateT Map.empty . forever $ do
    event <- liftIO $ receive endpoint
    liftIO $ print $ "Received: " <> show event
    case event of
      ConnectionOpened cid _ addr -> do
        liftIO $ print $ "Connection opened with ID " <> show cid
        get >>= \clients -> liftIO $ do
          Right conn <- connect endpoint addr ReliableOrdered defaultConnectHints
          send conn [BSC.pack . show . Map.elems $ clients]
          close conn
        modify $ Map.insert cid (endPointAddressToByteString addr)
      ConnectionClosed cid -> do
        liftIO $ print $ "Connection closed with ID " <> show cid
        modify $ Map.delete cid
      ErrorEvent (TransportError (EventConnectionLost addr) msg) -> do
        liftIO $ do
          print $ "Connection lost with " <> show addr
          print msg
