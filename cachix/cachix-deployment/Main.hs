{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE RecordWildCards #-}

module Main
  ( main,
  )
where

import Cachix.API.Error (escalateAs)
import Cachix.API.WebSocketSubprotocol qualified as WSS
import Cachix.Client.Retry
import Cachix.Deploy.Activate qualified as Activate
import Cachix.Deploy.Websocket qualified as CachixWebsocket
import Conduit ((.|))
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.STM.TMQueue qualified as TMQueue
import Data.Aeson qualified as Aeson
import Data.Conduit qualified as Conduit
import Data.Conduit.Combinators qualified as Conduit
import Data.Conduit.TQueue qualified as Conduit
import Data.String (String)
import Data.Time.Clock (getCurrentTime)
import Data.UUID (UUID)
import Data.UUID qualified as UUID
import GHC.IO.Encoding
import Katip qualified as K
import Network.HTTP.Simple (RequestHeaders)
import Network.WebSockets qualified as WS
import Protolude hiding (toS)
import Protolude.Conv
import System.IO (BufferMode (..), hSetBuffering)
import Wuss qualified

type Logger = K.KatipContextT IO () -> IO ()

data Env = Env
  { input :: CachixWebsocket.Input,
    connection :: WS.Connection,
    logger :: Logger,
    agentToken :: ByteString
  }

newtype CachixDeployer a = CachixDeployer
  { deploy :: ReaderT Env (K.KatipContextT IO) a
  }
  deriving
    ( Functor,
      Applicative,
      Monad,
      MonadReader Env,
      MonadIO
    )

instance K.Katip CachixDeployer where
  getLogEnv = K.getLogEnv

instance K.KatipContext CachixDeployer where
  getKatipContext = K.getKatipContext
  getKatipNamespace = K.getKatipNamespace

main :: IO ()
main = do
  setLocaleEncoding utf8
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  input <- escalateAs (FatalError . toS) . Aeson.eitherDecode . toS =<< getContents
  -- ASK: why is the deployment binary receiving messages here? Could the agent provide all of the information?
  CachixWebsocket.runForever (CachixWebsocket.websocketOptions input) $
    \payload logger connection _agentState agentToken ->
      runReaderT (deploy (handleMessage payload)) $
        Env {input = input, connection = connection, logger = logger, agentToken = agentToken}

handleMessage ::
  ByteString ->
  CachixDeployer ()
handleMessage payload =
  -- CachixWebsocket.parseMessage payload (handleCommand . WSS.command)
  case WSS.parseMessage payload of
    Left err ->
      -- TODO: show the bytestring?
      K.logLocM K.ErrorS $ K.ls $ "Failed to parse websocket payload: " <> err
    Right message ->
      handleCommand (WSS.command message)

handleCommand :: WSS.BackendCommand -> CachixDeployer ()
handleCommand (WSS.Deployment _) =
  -- TODO: Should the deployment binary be doing this? Isn’t the job of the agent?
  K.logLocM K.ErrorS "cachix-deployment should have never gotten a deployment command directly."
handleCommand (WSS.AgentRegistered agentInformation) = do
  queue <- liftIO $ atomically TMQueue.newTMQueue
  env@Env {..} <- ask
  let CachixWebsocket.Input {..} = input
      deploymentID = WSS.id (deploymentDetails :: WSS.DeploymentDetails)
      streamingThread =
        runLogStreaming env (toS $ CachixWebsocket.host websocketOptions) (CachixWebsocket.headers websocketOptions agentToken) queue deploymentID
      activateThread =
        logger (Activate.activate websocketOptions connection (Conduit.sinkTMQueue queue) deploymentDetails agentInformation agentToken)
          `finally` atomically (TMQueue.closeTMQueue queue)

  -- TODO: replace with concurrently_
  -- TODO: close the TQueue from the activation thread
  liftIO $ Async.concurrently_ activateThread streamingThread
  throwIO ExitSuccess

-- Logging

runLogStreaming :: Env -> String -> RequestHeaders -> TMQueue.TMQueue ByteString -> UUID -> IO ()
runLogStreaming Env {..} host headers queue deploymentID = do
  let path = "/api/v1/deploy/log/" <> UUID.toText deploymentID
  retryAllWithLogging endlessRetryPolicy (CachixWebsocket.logger logger) $ do
    liftIO $
      Wuss.runSecureClientWith host 443 (toS path) WS.defaultConnectionOptions headers $
        \conn ->
          -- ASK: why is the logging thread closing the original connection?
          -- bracket_ (return ()) (WS.sendClose connection ("Closing." :: ByteString)) $
          Conduit.runConduit $
            Conduit.sourceTMQueue queue
              .| Conduit.linesUnboundedAscii
              -- TODO: prepend katip-like format to each line
              -- .| (if CachixWebsocket.isVerbose options then Conduit.print else mempty)
              .| sendLog conn

sendLog :: WS.Connection -> Conduit.ConduitT ByteString Conduit.Void IO ()
sendLog connection = Conduit.mapM_ $ \bs -> do
  now <- getCurrentTime
  WS.sendTextData connection $
    Aeson.encode $
      WSS.Log
        { WSS.line = toS bs,
          WSS.time = now
        }
