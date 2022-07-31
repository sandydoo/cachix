-- Implement ppong on the client side for WS
-- TODO: upstream to https://github.com/jaspervdj/websockets/issues/159
module Cachix.Deploy.WebsocketPong where

import Data.Time.Clock (UTCTime, diffUTCTime, getCurrentTime, nominalDiffTimeToSeconds)
import qualified Network.WebSockets as WS
import Protolude hiding (throwTo)
import UnliftIO

type LastPongState = IORef UTCTime

data WebsocketPongTimeout
  = WebsocketPongTimeout
  deriving (Show)

instance Exception WebsocketPongTimeout

newState :: (MonadIO m) => m LastPongState
newState = newIORef =<< liftIO getCurrentTime

-- everytime we send a ping we check if we also got a pong back
pingHandler :: (MonadIO m) => LastPongState -> ThreadId -> Int -> m ()
pingHandler state threadID maxLastPing = do
  last <- secondsSinceLastPong state
  when (last > maxLastPing) $ throwTo threadID WebsocketPongTimeout

secondsSinceLastPong :: (MonadIO m) => LastPongState -> m Int
secondsSinceLastPong state = do
  now <- liftIO getCurrentTime
  last <- readIORef state
  return . ceiling . nominalDiffTimeToSeconds $ diffUTCTime now last

pongHandler :: (MonadIO m) => LastPongState -> m ()
pongHandler state =
  writeIORef state =<< liftIO getCurrentTime

installPongHandler :: LastPongState -> WS.ConnectionOptions -> WS.ConnectionOptions
installPongHandler state opts = opts {WS.connectionOnPong = pongHandler state}
