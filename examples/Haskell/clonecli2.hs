{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
-- |
-- Clone client Model Two
-- 
-- Translated to Haskell

module Main
  where

import Kvsimple
import qualified System.ZMQ3.Monadic as Z
import System.ZMQ3.Monadic (Socket(..),ZMQ(..))

import Control.Monad.IO.Class(liftIO)
import Data.Binary
import Data.Hashable(Hashable)

import qualified Data.HashMap.Lazy as H
import qualified Data.ByteString as BS



main :: IO()
main = do
  -- Prepare our context and publisher socket
  Z.runZMQ $ do
    snapshot <- Z.socket Z.Dealer
    Z.setLinger (Z.restrict 0) snapshot
    Z.connect snapshot "tcp://localhost:5556"
    subscriber <- Z.socket Z.Sub
    Z.subscribe subscriber ""
    Z.setLinger (Z.restrict 0) subscriber
    Z.connect subscriber "tcp://localhost:5557"

    
    -- Get state snapshot
    Z.send snapshot [] "ICANHAZ?"
    (sequence, kvmap) <- recvSna snapshot (0 :: Int, H.empty :: H.HashMap Integer Integer)
    -- Now apply pending updates, discard out-of-sequence messages
    recvSub sequence subscriber kvmap
  where
    recvSna upd inp@(_, kvm) = do
      msg <- recv upd
      case msg of
        (KVMsg Nothing s Nothing) -> liftIO (putStrLn ("Received snapshot=" ++ (show s))) >> return (s, kvm)
        m                         -> recvSna upd (0, store m kvm)

    recvSub seq upd kvm = do
      kvmsg@(KVMsg k s v) :: (KVMsg Integer Integer) <- recv upd
      if (s > seq) then
        recvSub s upd (store kvmsg kvm)
      else
        recvSub seq upd kvm

