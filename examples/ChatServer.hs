{-# LANGUAGE OverloadedStrings #-}
import System.Environment (getArgs)
import Network.Transport
import Network.Transport.AMQP (createTransport, AMQPTransport(..))
import Network.AMQP (openChannel, openConnection)
import Control.Monad.State (evalStateT, modify, get)
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import qualified Data.Map.Strict as Map (empty, insert, delete, elems)
import qualified Data.ByteString.Char8 as BSC (pack)

main :: IO ()
main = do
  conn <- openConnection "localhost" "/" "guest" "guest"
  ch <- openChannel conn
  let amqpTransport = AMQPTransport conn ch (Just "chat-server-example")
  let transport = createTransport amqpTransport
  Right endpoint  <- newEndPoint transport

  putStrLn $ "Chat server ready at " ++ (show . endPointAddressToByteString . address $ endpoint)

  flip evalStateT Map.empty . forever $ do
    liftIO $ print "read next event"
    event <- liftIO $ receive endpoint
    liftIO $ print "event read"
    case event of
      ConnectionOpened cid _ addr -> do
        get >>= \clients -> liftIO $ do
          Right conn <- connect endpoint addr ReliableOrdered defaultConnectHints
          send conn [BSC.pack . show . Map.elems $ clients]
          close conn
        modify $ Map.insert cid (endPointAddressToByteString addr)
      ConnectionClosed cid ->
        modify $ Map.delete cid
