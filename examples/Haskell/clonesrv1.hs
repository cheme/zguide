-- |
-- Clone server Model One


module Main
  where

import Kvsimple
import Kvsimple(KVMsg(..))
import qualified System.ZMQ3.Monadic as Z
import System.ZMQ3.Monadic (Socket(..),ZMQ(..))

import Control.Monad.IO.Class(liftIO)
import Control.Monad.CatchIO(MonadCatchIO, catch)
import Control.Exception.Base(AsyncException(UserInterrupt))

import qualified Data.HashMap.Lazy as H
import Control.Concurrent (threadDelay)
import System.Random(randomRIO)


main :: IO()
main = do
  -- Prepare our context and publisher socket
  Z.runZMQ $ do
    publisher <- Z.socket Z.Pub
    Z.bind publisher "tcp://*:5556"
    liftIO $ threadDelay (200 * 1000) -- 200 ms

    let sequence = 0
    let kvmap = H.empty

   -- try:
    --    while True:
    sendKV sequence publisher kvmap
  where
    sendKV seq pub kvm = do
      -- Distribute as key-value message
      let s = seq + 1
      k <- liftIO $ randomRIO (1 :: Integer, 10000)
      b <- liftIO $ randomRIO (1 :: Integer, 1000000)
      let kvmsg = KVMsg (Just k) s (Just b)
      send kvmsg pub
      let map = store kvmsg kvm
      sendKV s pub map
     `catchAsync` \_ -> liftIO $ putStrLn (" Interrupted\n" ++ show (seq +1) ++ " messages out")



catchAsync ::  (MonadCatchIO m) => m a -> (AsyncException -> m a) -> m a
catchAsync = catch
