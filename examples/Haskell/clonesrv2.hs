{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
-- |
-- Clone server Model Two


module Main
  where

import Kvsimple
import Kvsimple(KVMsg(..))
import qualified System.ZMQ3.Monadic as Z
import System.ZMQ3.Monadic (Socket(..),ZMQ(..))

import Control.Monad.IO.Class(liftIO)
import Control.Monad.CatchIO(MonadCatchIO, catch)
import Control.Exception.Base(AsyncException(UserInterrupt))

import qualified Data.ByteString as BS
import Data.Binary
import Data.Hashable(Hashable)

import qualified Data.HashMap.Lazy as H
import Control.Concurrent (threadDelay)
import System.Random(randomRIO)

import Data.Unique(hashUnique,newUnique)
import Control.Monad((=<<))
import qualified Data.Foldable as F
import Control.Monad(foldM)

nothing :: Maybe ()
nothing = Nothing

main :: IO()
main = do
  -- Prepare our context and publisher socket
  Z.runZMQ $ do
    publisher <- Z.socket Z.Pub
    Z.bind publisher "tcp://*:5557"
    let sequence = 0

    -- Start state manager and wait for synchronization signal
    let kvmap = H.empty :: H.HashMap Integer Integer -- Type def for map
    updates <- z_fork $ (\pipe -> (state_manager pipe kvmap))
    sendKV sequence publisher updates
  where
    sendKV seq pub upd = do
      -- Distribute as key-value message
      let s = seq + 1
      k <- liftIO $ randomRIO (1 :: Integer, 10000)
      b <- liftIO $ randomRIO (1 :: Integer, 1000000)
      let kvmsg = KVMsg (Just k) s (Just b)
      send kvmsg pub
      send kvmsg upd
      sendKV s pub upd
      `catchAsync` \_ -> liftIO $ putStrLn (" Interrupted\n" ++ show (seq + 1) ++ " messages out")

-- Send one state snapshot key-value pair to a socket
-- Hash item data is our kvmsg object, ready to send
send_single :: (Binary v, Binary k, Z.Sender a) => BS.ByteString -> Socket z a -> Int -> (k, v) -> ZMQ z Int
send_single id soc s (k, v) = do
  Z.send soc [Z.SendMore] id
  send (KVMsg (Just k) (s + 1) (Just v)) soc
  return $ s + 1


-- The state manager task maintains the state and handles requests from
-- clients for snapshots:
state_manager :: (Hashable a, Binary a, Eq a, Binary b) =>  Socket z Z.Pair -> H.HashMap a b -> ZMQ z (Int, H.HashMap a b)
state_manager pipe kvmap = do
  Z.send pipe [] "Ready"
  snapshot <- Z.socket Z.Router
  Z.bind snapshot "tcp://*:5556"
  handleIn snapshot (0, kvmap) -- Current snapshot version number is 0
  where
    handleIn snapshot req@(seq, map) = do
      evts <- Z.poll (-1) [Z.Sock pipe [Z.In] Nothing, Z.Sock snapshot [Z.In] Nothing]
      handleIn snapshot =<< case evts of
        [Z.In]:[Z.In]:_ -> recPipe req >>= recSnap snapshot
        _:[Z.In]:_      -> recSnap snapshot req
        [Z.In]:_        -> recPipe req
        _               -> return req
    -- Apply state update from main thread
    recPipe (seq, map) = recv pipe >>= (\msg@(KVMsg k s v) -> return (seq, store msg map))
      
    -- Execute state snapshot request
    recSnap snapshot req@(seq, map) = do
      (mframe ::  [BS.ByteString]) <- Z.receiveMulti snapshot
      case mframe of
        -- Request is in second frame of message
        id:"ICANHAZ?":_ -> do
          -- Send state snapshot to client
          -- For each entry in kvmap, send kvmsg to client
          seq <- foldM (send_single id snapshot) 0 $ H.toList map
          
          -- Now send END message with sequence number
          liftIO $ putStrLn ("Sending state shapshot=\n" ++ show seq)
          Z.send snapshot [Z.SendMore] id
          -- Send of string "KTHXBAI" is not used, instead we use an abnormal message for snapshot with (due to the fact our KVmsg are typed Integer and not ByteString)
          send (KVMsg nothing seq nothing) snapshot
        _               -> liftIO $ putStrLn "E: bad request, aborting\n" -- ignore
      return req

catchAsync ::  (MonadCatchIO m) => m a -> (AsyncException -> m a) -> m a
catchAsync = catch


-- utility function to add a pair to monadic zmq monad asynq (similar to zthread_fork)
-- taken from python zpipe in example zhelpers
z_fork :: (Socket z Z.Pair -> ZMQ z a) -> ZMQ z (Socket z Z.Pair)
z_fork run = do
  a <- Z.socket Z.Pair
  Z.setLinger (Z.restrict 0) a
  b <- Z.socket Z.Pair
  Z.setLinger (Z.restrict 0) b
  uni <- liftIO newUnique
  Z.setReceiveHighWM (Z.restrict 1) a
  Z.setReceiveHighWM (Z.restrict 1) b
  let iface = "inproc://" ++ show (hashUnique uni)
  Z.bind a iface
  Z.connect b iface
  Z.async $ run a
  return b


