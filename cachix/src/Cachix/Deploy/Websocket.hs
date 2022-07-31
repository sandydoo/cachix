-- high level interface for websocket clients
module Cachix.Deploy.Websocket where

import Cachix.API.WebSocketSubprotocol (AgentInformation)
import qualified Cachix.API.WebSocketSubprotocol as WSS
import Cachix.Client.Retry
import Cachix.Client.Version (versionNumber)
import qualified Cachix.Deploy.WebsocketPong as WebsocketPong
import Control.Exception.Safe (MonadMask)
import Control.Retry (RetryStatus (..))
import Data.Aeson (FromJSON, ToJSON)
import Data.String (String)
import qualified Katip as K
import Network.HTTP.Types (Header)
import qualified Network.WebSockets as WS
import Protolude hiding (bracket, myThreadId, toS)
import Protolude.Conv
import UnliftIO
import UnliftIO.Concurrent (myThreadId)
import UnliftIO.Environment (getEnv)
import UnliftIO.IORef
import qualified Wuss

type AgentState = IORef (Maybe WSS.AgentInformation)

data Options = Options
  { host :: Text,
    path :: Text,
    name :: Text,
    isVerbose :: Bool,
    profile :: Text
  }
  deriving (Show, Generic, ToJSON, FromJSON)

data Input = Input
  { deploymentDetails :: WSS.DeploymentDetails,
    websocketOptions :: Options
  }
  deriving (Show, Generic, ToJSON, FromJSON)

runForever ::
  (MonadMask m, MonadUnliftIO m, K.KatipContext m) =>
  Options ->
  (WS.Connection -> AgentState -> ByteString -> ByteString -> m ()) ->
  m ()
runForever options cmd = do
  runInIO <- askRunInIO

  -- TODO: error if token is missing
  agentToken <- getEnv "CACHIX_AGENT_TOKEN"
  agentState <- newIORef Nothing
  pongState <- WebsocketPong.newState
  mainThreadID <- myThreadId

  let pingHandler :: (MonadIO m, K.KatipContext m) => m ()
      pingHandler = do
        last <- WebsocketPong.secondsSinceLastPong pongState
        K.logLocM K.DebugS $ K.ls $ "Sending WebSocket keep-alive ping, last pong was " <> (show last :: Text) <> " seconds ago"
        WebsocketPong.pingHandler pongState mainThreadID pongTimeout

  let connectionOptions = WebsocketPong.installPongHandler pongState WS.defaultConnectionOptions

  -- TODO: use exponential retry with reset: https://github.com/Soostone/retry/issues/25
  retryAllWithLogging endlessConstantRetryPolicy logger $ do
    K.logLocM K.InfoS $ K.ls ("Agent " <> agentIdentifier <> " connecting to " <> toS (host options) <> toS (path options))
    -- refresh pong state in case we're reconnecting
    WebsocketPong.pongHandler pongState
    -- TODO: https://github.com/jaspervdj/websockets/issues/229
    liftIO $
      Wuss.runSecureClientWith (toS $ host options) 443 (toS $ path options) connectionOptions (headers options (toS agentToken)) $ \connection -> do
        runInIO $ K.logLocM K.InfoS "Connected to Cachix Deploy service"
        WS.withPingThread connection pingEvery (runInIO pingHandler) $
          WSS.receiveDataConcurrently connection $
            runInIO . cmd connection agentState (toS agentToken)
  where
    agentIdentifier = name options <> " " <> toS versionNumber
    pingEvery = 30
    pongTimeout = pingEvery * 2

headers :: Options -> ByteString -> [Header]
headers options agentToken =
  [ ("Authorization", "Bearer " <> toS agentToken),
    ("name", toS (name options)),
    ("version", toS versionNumber)
  ]

-- TODO: log the exception
logger :: (K.KatipContext m) => Bool -> SomeException -> RetryStatus -> m ()
logger _ exception retryStatus =
  K.logLocM K.ErrorS $ K.ls $ "Retrying in " <> delay (rsPreviousDelay retryStatus) <> " due to an exception: " <> displayException exception
  where
    delay :: Maybe Int -> String
    delay Nothing = "0 seconds"
    delay (Just s) = show (floor (fromIntegral s / 1000 / 1000)) <> " seconds"

withKatip :: Bool -> (K.LogEnv -> IO a) -> IO a
withKatip isVerbose =
  bracket createLogEnv K.closeScribes
  where
    permit = if isVerbose then K.DebugS else K.InfoS
    createLogEnv = do
      logEnv <- K.initLogEnv "agent" "production"
      stdoutScribe <- K.mkHandleScribe K.ColorIfTerminal stdout (K.permitItem permit) K.V2
      K.registerScribe "stdout" stdoutScribe K.defaultScribeSettings logEnv

parseMessage :: FromJSON cmd => ByteString -> (WSS.Message cmd -> K.KatipContextT IO ()) -> K.KatipContextT IO ()
parseMessage payload m =
  case WSS.parseMessage payload of
    Left err ->
      -- TODO: show the bytestring?
      K.logLocM K.ErrorS $ K.ls $ "Failed to parse websocket payload: " <> err
    Right message ->
      m message

-- commands

registerAgent :: AgentState -> AgentInformation -> K.KatipContextT IO ()
registerAgent agentState agentInformation = do
  K.logLocM K.InfoS "Agent registered."
  atomicWriteIORef agentState (Just agentInformation)
