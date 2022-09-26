{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeApplications #-}

module Cachix.Deploy.Agent where

import qualified Cachix.API.WebSocketSubprotocol as WSS
import qualified Cachix.Client.Config as Config
import Cachix.Client.URI (getBaseUrl)
import Cachix.Client.Version (versionNumber)
import qualified Cachix.Deploy.Log as Log
import qualified Cachix.Deploy.OptionsParser as AgentOptions
import qualified Cachix.Deploy.StdinProcess as StdinProcess
import qualified Cachix.Deploy.Websocket as WebSocket
import Control.Exception.Safe (handleAny, onException)
import qualified Data.Aeson as Aeson
import Data.IORef
import Data.String (String)
import qualified Katip as K
import Paths_cachix (getBinDir)
import Protolude hiding (onException, toS)
import Protolude.Conv
import qualified Servant.Client as Servant
import qualified System.Directory as Directory
import System.Environment (getEnv, lookupEnv)
import qualified System.Posix.Files as Posix.Files
import qualified System.Posix.User as Posix.User

type AgentState = IORef (Maybe WSS.AgentInformation)

type ServiceWebSocket = WebSocket.WebSocket (WSS.Message WSS.AgentCommand) (WSS.Message WSS.BackendCommand)

-- | Everything required for the standalone deployment binary to complete a
-- deployment.
data Deployment = Deployment
  { agentName :: Text,
    agentToken :: Text,
    profileName :: Text,
    host :: Text,
    logOptions :: Log.Options,
    deploymentDetails :: WSS.DeploymentDetails,
    agentInformation :: WSS.AgentInformation
  }
  deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

agentIdentifier :: Text -> Text
agentIdentifier agentName = agentName <> " " <> toS versionNumber

registerAgent :: AgentState -> WSS.AgentInformation -> K.KatipContextT IO ()
registerAgent agentState agentInformation = do
  K.logLocM K.InfoS "Agent registered."
  liftIO $ atomicWriteIORef agentState (Just agentInformation)

run :: Config.CachixOptions -> AgentOptions.AgentOptions -> IO ()
run cachixOptions agentOpts =
  Log.withLog logOptions $ \withLog ->
    handleAny (logAndExit withLog) $ do
      checkUserOwnsHome

      -- TODO: error if token is missing
      agentToken <- toS <$> getEnv "CACHIX_AGENT_TOKEN"
      agentState <- newIORef Nothing

      let agentName = AgentOptions.name agentOpts
      let websocketOptions =
            WebSocket.Options
              { WebSocket.host = host,
                WebSocket.path = "/ws",
                WebSocket.headers = WebSocket.createHeaders agentName agentToken,
                WebSocket.agentIdentifier = agentIdentifier agentName
              }

      WebSocket.withConnection withLog websocketOptions $ \websocket ->
        WebSocket.handleJSONMessages @(WSS.Message WSS.AgentCommand) @(WSS.Message WSS.BackendCommand) websocket $
          WebSocket.withOpenChannel websocket $ \channel ->
            forever $
              WebSocket.read channel >>= \case
                Just (WebSocket.DataMessage message) -> do
                  handleMessage withLog agentState agentName agentToken message
                _ -> pure ()
  where
    host = toS $ Servant.baseUrlHost $ getBaseUrl $ Config.host cachixOptions
    profileName = fromMaybe "system" (AgentOptions.profile agentOpts)

    logAndExit withLog e = do
      void $ withLog $ K.logLocM K.ErrorS $ K.ls (displayException e)
      exitFailure

    verbosity =
      if Config.verbose cachixOptions
        then Log.Verbose
        else Log.Normal

    logOptions =
      Log.Options
        { verbosity = verbosity,
          namespace = "agent",
          environment = "production"
        }

    handleMessage :: Log.WithLog -> AgentState -> Text -> Text -> WSS.Message WSS.BackendCommand -> IO ()
    handleMessage withLog agentState agentName agentToken payload =
      handleCommand (WSS.command payload)
      where
        handleCommand :: WSS.BackendCommand -> IO ()
        handleCommand (WSS.AgentRegistered agentInformation) =
          withLog $ registerAgent agentState agentInformation
        handleCommand (WSS.Deployment deploymentDetails) = do
          agentRegistered <- readIORef agentState

          case agentRegistered of
            -- TODO: this is currently not possible, but relies on the backend
            -- to do the right thing. Can we improve the typing here?
            Nothing -> pure ()
            Just agentInformation -> do
              binDir <- toS <$> getBinDir
              StdinProcess.spawnProcess (binDir <> "/.cachix-deployment") [] $
                toS . Aeson.encode $
                  Deployment
                    { agentName = agentName,
                      agentToken = agentToken,
                      profileName = profileName,
                      host = host,
                      deploymentDetails = deploymentDetails,
                      agentInformation = agentInformation,
                      logOptions = logOptions
                    }

-- | Fetch the home directory and verify that the owner matches the current user.
-- Throws either 'NoHomeFound' or 'UserDoesNotOwnHome'.
checkUserOwnsHome :: IO ()
checkUserOwnsHome = do
  home <- Directory.getHomeDirectory `onException` throwIO NoHomeFound
  stat <- Posix.Files.getFileStatus home
  userId <- Posix.User.getEffectiveUserID

  when (userId /= Posix.Files.fileOwner stat) $ do
    userName <- Posix.User.userName <$> Posix.User.getUserEntryForID userId
    sudoUser <- lookupEnv "SUDO_USER"
    throwIO $
      UserDoesNotOwnHome
        { userName = userName,
          sudoUser = sudoUser,
          home = home
        }

data Error
  = -- | No home directory.
    NoHomeFound
  | -- | Safeguard against creating root-owned files in user directories.
    -- This is an issue on macOS, where, by default, sudo does not reset $HOME.
    UserDoesNotOwnHome
      { userName :: String,
        sudoUser :: Maybe String,
        home :: FilePath
      }
  deriving (Show)

instance Exception Error where
  displayException NoHomeFound = "Could not find the user’s home directory. Make sure to set the $HOME variable."
  displayException UserDoesNotOwnHome {userName = userName, sudoUser = sudoUser, home = home} =
    if isJust sudoUser
      then toS $ unlines [warningMessage, suggestSudoFlagH]
      else toS warningMessage
    where
      warningMessage = "The current user (" <> toS userName <> ") does not own the home directory (" <> toS home <> ")"
      suggestSudoFlagH = "Try running the agent with `sudo -H`."
