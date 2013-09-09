{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
-- |
-- Clone client Model One
-- 
-- Translated to Haskell by

module Main
  where

import Kvsimple
import qualified System.ZMQ3.Monadic as Z
import System.ZMQ3.Monadic (Socket(..),ZMQ(..))

import Control.Monad.IO.Class(liftIO)
import Data.Binary
import Data.Hashable(Hashable)

import qualified Data.HashMap.Lazy as H
import Control.Concurrent (threadDelay)

nothing :: Maybe Integer
nothing = Nothing

moninproc = "inproc://endsrv"

main :: IO()
main = do
  -- Prepare our context and publisher socket
  Z.runZMQ $ do
    updates <- Z.socket Z.Sub
    Z.setLinger (Z.restrict 0) updates
    Z.subscribe updates ""
    -- Usage of a monitor thread to handle end of connection
    -- This is not in guide. 
    monitor <- Z.monitor [Z.DisconnectedEvent] updates
    Z.async $ do
      spubend <- Z.socket Z.Pub
      Z.bind spubend moninproc
      m <- liftIO $ monitor True
      liftIO (monitor False)
      send (KVMsg nothing (-1) nothing) spubend


    Z.connect updates "tcp://localhost:5556"
    Z.connect updates  moninproc

    let kvmap = H.empty
    let sequence = 0
    recvKV sequence updates kvmap
  where
    recvKV seq upd kvm = do
      kvmsg :: (KVMsg Integer Integer) <- recv upd
      let map = store kvmsg kvm
      let s = seq + 1
      -- could have used poll instead of -1 or at least use a data type for KVMSG for End
      case kvmsg of
        (KVMsg _ (-1) _) -> liftIO (putStrLn ("Interrupted\n" ++ (show seq) ++ " messages in\n"))
        _ -> recvKV s upd map


