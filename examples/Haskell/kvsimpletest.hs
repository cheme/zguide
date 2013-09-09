{-# LANGUAGE DeriveGeneric #-}
module Main
  where

import Kvsimple
import System.Environment (getArgs)

main :: IO ()
main = do
   args <- getArgs
   test_kvmsg $ elem "-v" args
