{-# LANGUAGE DuplicateRecordFields #-}

module Main
  ( main,
  )
where

import Cachix.API.Error (escalateAs)
import qualified Cachix.API.WebSocketSubprotocol as WSS
import Cachix.Client.Retry
import qualified Cachix.Deploy.Activate as Activate
import qualified Cachix.Deploy.Websocket as CachixWebsocket
import Conduit ((.|))
import qualified Control.Concurrent.STM.TQueue as TQueue
import Control.Exception.Safe (MonadMask)
import qualified Data.Aeson as Aeson
import qualified Data.Conduit as Conduit
import qualified Data.Conduit.Combinators as Conduit
import qualified Data.Conduit.TQueue as Conduit
import Data.String (String)
import Data.Time.Clock (getCurrentTime)
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import GHC.IO.Encoding
import qualified Katip as K
import Network.HTTP.Simple (RequestHeaders)
import qualified Network.WebSockets as WS
import Protolude hiding (atomically, toS)
import Protolude.Conv
import System.IO (BufferMode (..), hSetBuffering)
import UnliftIO (MonadUnliftIO, withRunInIO)
import qualified UnliftIO.Async as Async
import UnliftIO.STM (atomically)
import qualified Wuss

main :: IO ()
main = do
  setLocaleEncoding utf8
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  input <- escalateAs (FatalError . toS) . Aeson.eitherDecode . toS =<< getContents
  let isVerbose = CachixWebsocket.isVerbose . CachixWebsocket.websocketOptions $ input
  CachixWebsocket.withKatip isVerbose $ \logEnv ->
    K.runKatipContextT logEnv () "agent" $
      CachixWebsocket.runForever
        (CachixWebsocket.websocketOptions input)
        (handleMessage input)

handleMessage ::
  CachixWebsocket.Input ->
  WS.Connection ->
  CachixWebsocket.AgentState ->
  ByteString ->
  ByteString ->
  K.KatipContextT IO ()
handleMessage input connection _ agentToken payload =
  CachixWebsocket.parseMessage payload (handleCommand . WSS.command)
  where
    deploymentDetails = CachixWebsocket.deploymentDetails input
    options = CachixWebsocket.websocketOptions input

    handleCommand :: WSS.BackendCommand -> K.KatipContextT IO ()
    handleCommand (WSS.Deployment _) =
      K.logLocM K.ErrorS "cachix-deployment should have never gotten a deployment command directly."
    handleCommand (WSS.AgentRegistered agentInformation) = withRunInIO $ \runInIO -> do
      queue <- atomically TQueue.newTQueue
      let deploymentID = WSS.id (deploymentDetails :: WSS.DeploymentDetails)
          streamingThread =
            runInIO $
              runLogStreaming (toS $ CachixWebsocket.host options) (CachixWebsocket.headers options agentToken) queue deploymentID
          activateThread =
            runInIO $
              Activate.activate options connection (Conduit.sinkTQueue queue) deploymentDetails agentInformation agentToken
      Async.race_ streamingThread activateThread
      throwIO ExitSuccess

    runLogStreaming :: (MonadUnliftIO m, K.KatipContext m, MonadMask m) => String -> RequestHeaders -> Conduit.TQueue ByteString -> UUID -> m ()
    runLogStreaming host headers queue deploymentID = do
      let path = "/api/v1/deploy/log/" <> UUID.toText deploymentID
      retryAllWithLogging endlessRetryPolicy CachixWebsocket.logger $
        liftIO $
          Wuss.runSecureClientWith host 443 (toS path) WS.defaultConnectionOptions headers $
            \conn ->
              bracket_ (pure ()) (WS.sendClose connection ("Closing." :: ByteString)) $
                Conduit.runConduit $
                  Conduit.sourceTQueue queue
                    .| Conduit.linesUnboundedAscii
                    -- TODO: prepend katip-like format to each line
                    -- .| (if CachixWebsocket.isVerbose options then Conduit.print else mempty)
                    .| sendLog conn

sendLog :: WS.Connection -> Conduit.ConduitT ByteString Conduit.Void IO ()
sendLog connection = Conduit.mapM_ f
  where
    f = \bs -> do
      now <- getCurrentTime
      WS.sendTextData connection $ Aeson.encode $ WSS.Log {WSS.line = toS bs, WSS.time = now}
