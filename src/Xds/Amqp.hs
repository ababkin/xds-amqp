{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Xds.Amqp where

import           Control.Applicative        ((<$>), (<*>))
import           Control.Concurrent         (threadDelay)
import           Control.Monad              (forever)
import qualified Data.ByteString.Lazy.Char8 as BL
import           Data.Text                  (Text)
import qualified Data.Text                  as T
import           Network.AMQP               (Ack (..), Channel, Connection,
                                             Envelope, Message, QueueOpts (..),
                                             ackEnv, bindQueue, consumeMsgs,
                                             declareQueue, getMsg, msgBody,
                                             newQueue, openChannel,
                                             openConnection, rejectEnv)
import           System.Environment         (getEnv, lookupEnv)

data Status = Success | Failure

data Amqp = Amqp {
    amqpIp       :: String
  , amqpPort     :: Int
  , amqpUsername :: String
  , amqpPassword :: String
  }

getAmqp :: IO Amqp
getAmqp = Amqp
  <$> (getEnv "RMQ_IP")
  <*> (read <$> getEnv "RMQ_PORT")
  <*> (getEnv "RMQ_USERNAME")
  <*> (getEnv "RMQ_PASSWORD")

withAmqpChannel
  :: Amqp
  -> (Channel -> IO a)
  -> IO a
withAmqpChannel Amqp{..} action =
  action =<< openChannel =<< openConnection amqpIp "/" (T.pack amqpUsername) (T.pack amqpPassword)

pollQueue
  :: Text
  -> Channel
  -> Int
  -> (BL.ByteString -> IO Status)
  -> IO ()
pollQueue q chan delay handler = forever $ do
  threadDelay delay
  maybeMsg <- getMsg chan Ack q
  case maybeMsg of
    Nothing -> return ()
    Just (msg, env) -> do
      status <- handler $ msgBody msg
      case status of
        Success -> ackEnv env
        Failure -> rejectEnv env True


