-- |
-- kvsimple - simple key-value message monad for example applications



module Kvsimple
  (
      KVMsg(..)
    , store
    , send
    , recv
    , dump
    -- for test only
    , test_kvmsg
  ) where

import qualified Data.HashMap.Lazy as H
import Data.Hashable(Hashable)
import qualified System.ZMQ3.Monadic as Z
import System.ZMQ3.Monadic (Socket(..),ZMQ(..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Control.Monad.IO.Class(liftIO)
-- for binary serialization 
import Data.Binary

-- |
--  Message is not formatted on wire as 3 frames:
--  using haskell binary instead.
--  frame 0: key (0MQ string)
--  frame 1: sequence (8 bytes, network order)
--  frame 2: body (blob)
data KVMsg k v = KVMsg 
  { key :: Maybe k
  , sequence :: Int
  , body :: Maybe v
  }
  deriving (Show)



instance (Binary k, Binary v) => Binary (KVMsg k v) where
    put (KVMsg k s b) = do put k
                           put s
                           put b
    get = do k <- get
             s <- get
             b <- get
             return $ KVMsg k s b
-- Store me in a dict if I have anything to store
-- TODO see by running python sample if need a println
store :: (Eq k, Hashable k) => KVMsg k v -> H.HashMap k v -> H.HashMap k v
store (KVMsg (Just k) s (Just v)) map = H.insert k v map
store _ m = m


-- Send key-value message to socket; any empty frames are sent as such.
send :: (Z.Sender a, Binary k, Binary v) => KVMsg k v -> Socket z a -> ZMQ z ()
send kvm sock = Z.send' sock [] $ encode kvm


-- Reads key-value message from socket, returns new kvmsg instance.
recv :: (Z.Receiver a, Binary k, Binary v) => Socket z a -> ZMQ z (KVMsg k v)
recv sock = Z.receive sock >>= return . decode . LBS.pack . BS.unpack

-- using haskell show.
dump :: (Show k, Show v) => KVMsg k v -> String
dump = show

-- It's good practice to have a self-test method that tests the class; this
test_kvmsg :: Bool -> IO ()
test_kvmsg verbose = do 
    putStrLn " * kvmsg: "
    -- Prepare our context and sockets
    Z.runZMQ $ do
      output <- Z.socket Z.Dealer
      Z.bind output "ipc://kvmsg_selftest.ipc"
      input <- Z.socket Z.Dealer
      Z.connect input "ipc://kvmsg_selftest.ipc"
      
      let kvmap = H.empty
      -- Test send and receive of simple message
      let kvmsg = KVMsg (Just "key") 1 (Just "body")
      ifverbose $ dump kvmsg
      send kvmsg output
      let kvmap2 = store kvmsg kvmap
      
      kvmsg2 <- recv input
      
      ifverbose $ dump kvmsg2

      assert (key kvmsg2 == (Just "key"))

      let kvmap3 = store kvmsg2 kvmap2

      assert (H.size kvmap3 == 1) -- shouldn't be different
    putStrLn "OK"
  where ifverbose s = if verbose == True then (liftIO (putStrLn s)) else (return ())
        assert False      = liftIO $ putStrLn "Failed assertion"
        assert _          = return ()

