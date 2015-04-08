{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
import System.Environment (getArgs)
import Network.Transport
import Network.Transport.AMQP (createTransport, AMQPTransport(..))
import Network.AMQP (openChannel, openConnection)
import Control.Monad.State (evalStateT, modify, get)
import Control.Monad (forever)
import Control.Exception
import Control.Monad.IO.Class (liftIO)
import qualified Data.Map.Strict as Map (empty, insert, delete, elems)
import qualified Data.ByteString.Char8 as BSC (pack)

import Data.Monoid

main :: IO ()
main = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  ch <- openChannel conn
  let amqpTransport = AMQPTransport conn ch (Just "chat-server-example")
  let transport = createTransport amqpTransport
  Right endpoint  <- newEndPoint transport

  putStrLn $ "Chat server ready at " ++ (show . endPointAddressToByteString . address $ endpoint)
  
  go endpoint `catch` \(_ :: SomeException) -> closeEndPoint endpoint

  where
   go endpoint = flip evalStateT Map.empty . forever $ do
    event <- liftIO $ receive endpoint
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
