{-# LANGUAGE DuplicateRecordFields #-}

module Main
  ( main,
  )
where

import Cachix.API.Error (escalateAs)
import qualified Cachix.API.WebSocketSubprotocol as WSS
import qualified Cachix.Deploy.Activate as Activate
import qualified Cachix.Deploy.Agent as Agent
import qualified Cachix.Deploy.Lock as Lock
import qualified Cachix.Deploy.Log as Log
import qualified Cachix.Deploy.Websocket as WebSocket
import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.STM.TMQueue as TMQueue
import qualified Control.Exception.Safe as Safe
import qualified Data.Aeson as Aeson
import Data.Conduit ((.|))
import qualified Data.Conduit as Conduit
import qualified Data.Conduit.Combinators as Conduit
import qualified Data.Conduit.TQueue as Conduit
import Data.Time.Clock (getCurrentTime)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import GHC.IO.Encoding
import qualified Katip as K
import qualified Network.HTTP.Simple as HTTP
import qualified Network.WebSockets as WS
import Protolude hiding (toS)
import Protolude.Conv
import System.IO (BufferMode (..), hSetBuffering)
import qualified System.Timeout as Timeout
import qualified Wuss

-- | Activate the new deployment.
--
-- If the target profile is already locked by another deployment, exit
-- immediately and rely on the backend to reschedule.
--
-- TODO: what if websocket gets closed while deploying?
main :: IO ()
main = do
  setLocaleEncoding utf8
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

  deployment@Agent.Deployment
    { agentName,
      agentToken,
      profileName,
      host,
      logOptions,
      agentInformation,
      deploymentDetails
    } <-
    escalateAs (FatalError . toS) . Aeson.eitherDecode . toS =<< getContents

  let deploymentID = WSS.id (deploymentDetails :: WSS.DeploymentDetails)
  let logPath = "/api/v1/deploy/log/" <> UUID.toText deploymentID
  let headers = WebSocket.createHeaders agentName agentToken
  let websocketOptions =
        WebSocket.Options
          { WebSocket.host = host,
            WebSocket.path = "/ws-deployment",
            WebSocket.headers = headers,
            WebSocket.agentIdentifier = Agent.agentIdentifier agentName
          }

  Log.withLog logOptions $ \withLog -> do
    void . Lock.withTryLock profileName $ do
      backendQueue <- atomically TMQueue.newTMQueue
      logQueue <- atomically TMQueue.newTMQueue

      Async.withAsync (streamLog withLog host logPath (WebSocket.headers websocketOptions) logQueue) $ \logThread ->
        Async.withAsync (connectToBackend withLog websocketOptions agentInformation backendQueue) $ \backendThread ->
          deploy withLog deployment websocketOptions backendQueue (Conduit.sinkTMQueue logQueue)
            `finally` do
              withLog $ K.logLocM K.DebugS $ K.ls ("Cleaning up websocket connections" :: Text)
              atomically $ do
                TMQueue.closeTMQueue logQueue
                TMQueue.closeTMQueue backendQueue
              Async.waitBoth logThread backendThread

-- | Maintain a websocket connection to the backend for sending deployment
-- progress updates.
connectToBackend ::
  Log.WithLog ->
  WebSocket.Options ->
  WSS.AgentInformation ->
  TMQueue.TMQueue WSS.AgentCommand ->
  IO ()
connectToBackend withLog websocketOptions agentInformation backendQueue =
  WebSocket.withConnection withLog websocketOptions $ \WebSocket.WebSocket {connection} -> do
    Conduit.runConduit $
      Conduit.sourceTMQueue backendQueue
        .| Conduit.mapM_ (sendMessage connection)
  where
    sendMessage :: WS.Connection -> WSS.AgentCommand -> IO ()
    sendMessage connection cmd = do
      command <- createMessage cmd
      WSS.sendMessage connection command

    createMessage :: WSS.AgentCommand -> IO (WSS.Message WSS.AgentCommand)
    createMessage command = do
      uuid <- UUID.nextRandom
      return $
        WSS.Message
          { WSS.method = method,
            WSS.command = command,
            WSS.id = uuid,
            WSS.agent = Just $ WSS.id (agentInformation :: WSS.AgentInformation)
          }
      where
        -- TODO: move to WSS
        method = case command of
          WSS.DeploymentStarted {} -> "DeploymentStarted"
          WSS.DeploymentFinished {} -> "DeploymentFinished"

-- | Run the deployment commands
deploy ::
  -- | Logging context
  Log.WithLog ->
  -- | Deployment information passed from the agent
  Agent.Deployment ->
  -- | Websocket options
  WebSocket.Options ->
  -- | Message queue for the backend websocket connection
  TMQueue.TMQueue WSS.AgentCommand ->
  -- | Logging Websocket connection
  Log.LogStream ->
  IO ()
deploy withLog deployment websocketOptions backendQueue logStream = do
  withLog $ K.logLocM K.InfoS $ K.ls $ "Deploying #" <> deploymentIndex <> ": " <> storePath

  deploymentStatus <- Safe.tryIO $
    Activate.withCacheArgs host agentInformation agentToken $ \cacheArgs -> do
      startDeployment Nothing

      Activate.downloadStorePaths logStream deploymentDetails cacheArgs

      -- Try to get the closure size now that everything is downloaded
      --
      -- TODO: query the remote store to get the size before downloading (and
      -- possibly running out of disk space)
      closureSize <- fromRight Nothing <$> Activate.getClosureSize cacheArgs storePath
      when (isJust closureSize) $ startDeployment closureSize

      rollbackAction <- Activate.activate logStream profileName (toS storePath)

      -- Run network test

      case WSS.rollbackScript deploymentDetails of
        Nothing -> pure ()
        Just rollbackScript -> do
          Log.streamLine logStream "Running rollback script."
          rollbackScriptResult <- Activate.runShellWithExitCode logStream (toS rollbackScript) []

          case rollbackScriptResult of
            ExitSuccess -> return ()
            ExitFailure _ ->
              case rollbackAction of
                Just rollback -> do
                  Log.streamLine logStream "Deployment failed, rolling back ..."
                  rollback
                Nothing ->
                  Log.streamLine logStream "Skipping rollback as this is the first deployment."

  -- TODO: Test network, run rollback script, and optionally trigger rollback
  -- Send ping and wait for reply concurrently. Timeout.
  -- Async.withAsync (forever $ WS.sendPing connection >> threadDelay (1 * 1000 * 1000)) $ \_ -> do
  --   response <- WS.receive connection
  --   case response of
  --     WS.Pin
  -- let waitForPing = Nothing
  -- case waitForPing of
  --   Nothing -> Activate.rollback logStream profileName deploymentDetails
  --   Just () -> do
  --     case WSS.rollbackScript deploymentDetails of
  --       Just script -> return ()
  --       Nothing -> return ()

  case deploymentStatus of
    Left _ -> do
      Log.streamLine logStream "Failed to activate the deployment."
      withLog $ K.logLocM K.InfoS $ K.ls $ "Deploying #" <> deploymentIndex <> " failed."
    Right _ -> do
      Log.streamLine logStream "Successfully activated the deployment."
      withLog $ K.logLocM K.InfoS $ K.ls $ "Deployment #" <> deploymentIndex <> " finished"

  endDeployment (isRight deploymentStatus)
  where
    -- TODO: cut down record access boilerplate

    -- Deployment details

    storePath = WSS.storePath deploymentDetails
    deploymentDetails = Agent.deploymentDetails deployment
    deploymentID = WSS.id (deploymentDetails :: WSS.DeploymentDetails)
    deploymentIndex = show $ WSS.index deploymentDetails
    profileName = Agent.profileName deployment
    agentToken = Agent.agentToken deployment
    agentInformation = Agent.agentInformation deployment

    -- WebSocket options

    host = WebSocket.host websocketOptions

    startDeployment :: Maybe Int64 -> IO ()
    startDeployment closureSize = do
      now <- getCurrentTime
      atomically $
        TMQueue.writeTMQueue backendQueue $
          WSS.DeploymentStarted
            { WSS.id = deploymentID,
              WSS.time = now,
              WSS.closureSize = closureSize
            }

    endDeployment :: Bool -> IO ()
    endDeployment hasSucceeded = do
      now <- getCurrentTime
      atomically $
        TMQueue.writeTMQueue backendQueue $
          WSS.DeploymentFinished
            { WSS.id = deploymentID,
              WSS.time = now,
              WSS.hasSucceeded = hasSucceeded
            }

-- Log

-- TODO: prepend katip-like format to each line
streamLog ::
  -- | Logging context
  Log.WithLog ->
  -- | Host
  Text ->
  -- | Path
  Text ->
  -- | HTTP headers
  HTTP.RequestHeaders ->
  -- | Queue of messages to stream
  TMQueue.TMQueue ByteString ->
  IO ()
streamLog withLog host path headers queue = do
  WebSocket.reconnectWithLog withLog $
    Wuss.runSecureClientWith (toS host) 443 (toS path) WS.defaultConnectionOptions headers $ \connection ->
      Log.streamLog withLog connection queue
        `finally` WebSocket.waitForGracefulShutdown connection
