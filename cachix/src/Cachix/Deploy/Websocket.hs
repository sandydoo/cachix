{-# LANGUAGE QuasiQuotes #-}

-- | A high-level, multiple reader, single writer interface for Websocket clients.
module Cachix.Deploy.Websocket where

import qualified Cachix.Client.Retry as Retry
import qualified Cachix.Client.URI as URI
import Cachix.Client.Version (versionNumber)
import qualified Cachix.Deploy.Log as Log
import qualified Cachix.Deploy.WebsocketPong as WebsocketPong
import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.MVar as MVar
import qualified Control.Concurrent.STM.TBMQueue as TBMQueue
import qualified Control.Concurrent.STM.TMChan as TMChan
import Control.Exception.Safe (Handler (..), MonadMask, isSyncException)
import qualified Control.Exception.Safe as Safe
import qualified Control.Retry as Retry
import qualified Data.Aeson as Aeson
import qualified Data.ByteString as BS
import Data.Conduit ((.|))
import qualified Data.Conduit as Conduit
import qualified Data.Conduit.Combinators as Conduit
import qualified Data.Conduit.TQueue as Conduit
import qualified Data.IORef as IORef
import Data.String (String)
import Data.String.Here (iTrim)
import qualified Data.Text as Text
import qualified Data.Time.Clock as Time
import qualified Katip as K
import qualified Network.HTTP.Simple as HTTP
import qualified Network.WebSockets as WS
import qualified Network.WebSockets.Connection as WS.Connection
import Protolude hiding (Handler, toS)
import Protolude.Conv
import qualified System.Info
import qualified System.Timeout as Timeout
import qualified Wuss

-- | A reliable WebSocket connection that can be run ergonomically in a
-- separate thread.
--
-- Maintains the connection by periodically sending pings.
data WebSocket tx rx = WebSocket
  { -- | The active WebSocket connection, if available
    connection :: MVar.MVar WS.Connection,
    -- | The connection options
    options :: Options,
    -- | A timestamp of the last pong message received
    lastPong :: WebsocketPong.LastPongState,
    -- | See 'Transmit'
    tx :: Transmit tx,
    -- | See 'Receive'
    rx :: Receive rx,
    withLog :: Log.WithLog
  }

data Options = Options
  { host :: URI.Host,
    port :: URI.Port,
    path :: Text,
    useSSL :: Bool,
    headers :: HTTP.RequestHeaders,
    -- | The identifier used when logging. Usually a combination of the agent
    -- name and the CLI version.
    identifier :: Text
  }
  deriving (Show)

-- | A more ergonomic version of the Websocket 'Message' data type
data Message msg
  = ControlMessage WS.ControlMessage
  | DataMessage msg

-- | A bounded queue of outbound messages.
type Transmit msg = TBMQueue.TBMQueue (Message msg)

-- | A broadcast channel for incoming messages.
type Receive msg = TMChan.TMChan (Message msg)

-- | Send messages over the socket.
send :: WebSocket tx rx -> Message tx -> IO ()
send WebSocket {tx} = atomically . TBMQueue.writeTBMQueue tx

-- | Open a new receiving channel.
receive :: WebSocket tx rx -> IO (Receive rx)
receive WebSocket {rx} = atomically $ TMChan.dupTMChan rx

-- | Read incoming messages on a channel opened with 'receive'.
read :: Receive rx -> IO (Maybe (Message rx))
read = atomically . TMChan.readTMChan

-- | Read incoming data messages, ignoring everything else.
readDataMessages :: Receive rx -> (rx -> IO ()) -> IO ()
readDataMessages channel action = loop
  where
    loop =
      read channel >>= \case
        Just (DataMessage message) -> action message *> loop
        Just (ControlMessage _) -> loop
        Nothing -> pure ()

-- | Close the outgoing queue.
drainQueue :: WebSocket tx rx -> Async () -> IO ()
drainQueue WebSocket {tx} outgoingThread = do
  atomically $ TBMQueue.closeTBMQueue tx
  Async.wait outgoingThread

closeGracefully :: WebSocket tx rx -> Async () -> IO ()
closeGracefully websocket incomingThread = do
  repsonseToCloseRequest <- startGracePeriod $ do
    MVar.tryReadMVar (connection websocket) >>= \case
      Just activeConnection -> do
        WS.sendClose activeConnection ("Peer initiated a close request" :: ByteString)
        Async.wait incomingThread
      Nothing -> pure ()

  when (isNothing repsonseToCloseRequest) throwNoResponseToCloseRequest

-- | Run an app inside a new WebSocket connection.
withConnection :: Log.WithLog -> Options -> (WebSocket tx rx -> IO ()) -> IO ()
withConnection withLog options app = do
  websocket <- new withLog options
  runConnection websocket (app websocket)

-- | Set up state for a new WebSocket connection. Use 'runConnection' to then
-- open the connection.
--
-- This is useful for setting up message processing using the tx/rx channels
-- before the connection is established. For example, you might want to use
-- 'receive' to open a receiving channel and capture incoming messages.
new :: Log.WithLog -> Options -> IO (WebSocket tx rx)
new withLog options = do
  connection <- MVar.newEmptyMVar
  tx <- TBMQueue.newTBMQueueIO 100
  rx <- TMChan.newBroadcastTMChanIO
  lastPong <- WebsocketPong.newState
  pure $ WebSocket {connection, options, tx, rx, lastPong, withLog}

runConnection :: WebSocket tx rx -> IO () -> IO ()
runConnection websocket@WebSocket {connection, options, tx, rx, withLog, lastPong} app = do
  threadId <- myThreadId

  -- TODO: store this in the WebSocket record
  let pingEvery = 30
  let pongTimeout = pingEvery * 2
  let onPing = do
        last <- WebsocketPong.secondsSinceLastPong lastPong
        withLog $ K.logLocM K.DebugS $ K.ls $ "Sending WebSocket keep-alive ping, last pong was " <> (show last :: Text) <> " seconds ago"
        WebsocketPong.pingHandler lastPong threadId pongTimeout
  let connectionOptions = WebsocketPong.installPongHandler lastPong WS.defaultConnectionOptions

  let dropConnection = void $ MVar.tryTakeMVar connection
  let closeChannels = atomically $ do
        TBMQueue.closeTBMQueue tx
        TMChan.closeTMChan rx

  flip Safe.finally closeChannels $
    reconnectWithLog withLog $ do
      withLog $ K.logLocM K.InfoS $ K.ls (logOnMessage options)

      -- TODO: https://github.com/jaspervdj/websockets/issues/229
      runClientWith options connectionOptions $
        \newConnection -> flip Safe.finally dropConnection $ do
          withLog $ K.logLocM K.InfoS "Connected to Cachix Deploy service"

          -- Reset the pong state in case we're reconnecting
          WebsocketPong.pongHandler lastPong

          -- Update the connection
          MVar.putMVar connection newConnection

          Async.concurrently_ (sendPingEvery pingEvery onPing websocket) app

runClientWith :: Options -> WS.Connection.ConnectionOptions -> WS.ClientApp a -> IO a
runClientWith Options {host, port, path, headers, useSSL} connectionOptions app =
  if useSSL
    then Wuss.runSecureClientWith hostS (fromIntegral (URI.portNumber port)) (toS path) connectionOptions headers app
    else WS.runClientWith hostS (URI.portNumber port) (toS path) connectionOptions headers app
  where
    hostS = toS (URI.hostBS host)

-- Handle JSON messages

-- | Start processing incoming and outgoing JSON messages.
--
-- Make sure to open an incoming channel with [receive] beforehand to avoid
-- dropping messages.
handleJSONMessages :: (Aeson.ToJSON tx, Aeson.FromJSON rx) => WebSocket tx rx -> IO () -> IO ()
handleJSONMessages websocket app =
  handleJust unwrapThreadExceptions throwIO $
    mask $ \restore -> do
      incomingThread <- Async.async (handleIncomingJSON websocket)
      outgoingThread <- Async.async (handleOutgoingJSON websocket)

      let threads = [incomingThread, outgoingThread]
          cancelThreads = mapM_ Async.uninterruptibleCancel threads
      mapM_ Async.link threads

      let runApp = do
            app
            drainQueue websocket outgoingThread
            closeGracefully websocket incomingThread
      restore (runApp `Safe.finally` cancelThreads)
  where
    unwrapThreadExceptions :: SomeException -> Maybe SomeException
    unwrapThreadExceptions e
      | Just (Async.ExceptionInLinkedThread _ e') <- fromException e = Just e'
      | otherwise = Nothing

handleIncomingJSON :: (Aeson.FromJSON rx) => WebSocket tx rx -> IO ()
handleIncomingJSON websocket@WebSocket {connection, rx, withLog} = do
  activeConnection <- MVar.readMVar connection
  let broadcast = atomically . TMChan.writeTMChan rx

  forever $ do
    msg <- WS.receive activeConnection
    case msg of
      WS.DataMessage _ _ _ am ->
        case Aeson.eitherDecodeStrict' (WS.fromDataMessage am :: ByteString) of
          Left e -> withLog $ K.logLocM K.DebugS . K.ls $ "Cannot parse websocket payload: " <> e
          Right pMsg -> broadcast (DataMessage pMsg)
      WS.ControlMessage controlMsg -> do
        case controlMsg of
          WS.Ping pl ->
            send websocket (ControlMessage (WS.Pong pl))
          WS.Pong _ ->
            WS.connectionOnPong (WS.Connection.connectionOptions activeConnection)
          WS.Close code closeMsg -> do
            hasSentClose <- IORef.readIORef $ WS.Connection.connectionSentClose activeConnection
            unless hasSentClose $ WS.send activeConnection msg
            throwIO $ WS.CloseRequest code closeMsg

        broadcast (ControlMessage controlMsg)

handleOutgoingJSON :: forall tx rx. Aeson.ToJSON tx => WebSocket tx rx -> IO ()
handleOutgoingJSON WebSocket {connection, tx} = do
  activeConnection <- MVar.readMVar connection
  Conduit.runConduit $
    Conduit.sourceTBMQueue tx
      .| Conduit.mapM_ (sendJSONMessage activeConnection)
  where
    sendJSONMessage :: WS.Connection -> Message tx -> IO ()
    sendJSONMessage conn (ControlMessage msg) = WS.send conn (WS.ControlMessage msg)
    sendJSONMessage conn (DataMessage msg) = WS.sendTextData conn (Aeson.encode msg)

-- | Log exceptions and retry, specialized for reconnecting WebSockets.
--
-- Close requests should be retried unless the status code is 1000, which
-- indicates that both the client and server have acknowledged the close
-- request and are ready to terminate the connection.
--
-- Other status codes typically indicate some sort of error. For example,
-- Cloudflare periodically restarts WebSockets and sends a pre-defined
-- status code in the 1xxx range.
--
-- Defined status codes:
-- https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4.1
--
-- TODO: use exponential retry with reset: https://github.com/Soostone/retry/issues/25
-- TODO: clients should be able to decide which errors to recover from.
reconnectWithLog :: (MonadMask m, MonadIO m) => Log.WithLog -> m () -> m ()
reconnectWithLog withLog app =
  Safe.handle closeRequest $
    Retry.recovering Retry.endlessConstantRetryPolicy handlers (const app)
  where
    handlers = Retry.skipAsyncExceptions ++ [exitOnSuccess, exitOnCloseRequest, logSyncExceptions]

    closeRequest (WS.CloseRequest 1000 _) = return ()
    closeRequest e = Safe.throwM e

    exitOnSuccess _ = Handler $ \(_ :: ExitCode) -> return False

    exitOnCloseRequest _ = Handler $ \(e :: WS.ConnectionException) ->
      case e of
        WS.CloseRequest code msg -> do
          liftIO . withLog $
            K.logLocM K.DebugS . K.ls $
              "Received close request from peer (code: " <> show code <> ", message: " <> msg <> ")"

          -- Retry on any code other than 1000
          pure (code /= 1000)
        _ -> return True

    logSyncExceptions = Retry.logRetries (return . isSyncException) logRetries

    logRetries :: (MonadIO m) => Bool -> SomeException -> Retry.RetryStatus -> m ()
    logRetries _ exception retryStatus =
      liftIO . withLog $
        K.logLocM K.ErrorS . K.ls $
          "Retrying in " <> delay (Retry.rsPreviousDelay retryStatus) <> " due to an exception: " <> displayException exception

    delay :: Maybe Int -> String
    delay Nothing = "0 seconds"
    delay (Just t) = show (toSeconds t) <> " seconds"

    toSeconds :: Int -> Int
    toSeconds t =
      floor $ (fromIntegral t :: Double) / 1000 / 1000

waitForPong :: Int -> WebSocket tx rx -> IO (Maybe Time.UTCTime)
waitForPong seconds websocket = do
  channel <- receive websocket
  Async.withAsync (sendPingEvery 1 pass websocket) $ \_ ->
    Timeout.timeout (seconds * 1000 * 1000) $
      fix $ \waitForNextMsg -> do
        read channel >>= \case
          Just (ControlMessage (WS.Pong _)) -> Time.getCurrentTime
          _ -> waitForNextMsg

sendPingEvery :: Int -> IO () -> WebSocket tx rx -> IO ()
sendPingEvery seconds onPing WebSocket {connection} = forever $ do
  onPing
  activeConnection <- MVar.readMVar connection
  WS.sendPing activeConnection BS.empty
  threadDelay (seconds * 1000 * 1000)

startGracePeriod :: IO a -> IO (Maybe a)
startGracePeriod = Timeout.timeout (3 * 1000 * 1000)

-- | Try to gracefully close the WebSocket.
--
-- Do not run with asynchronous exceptions masked, ie. Control.Exception.Safe.finally.
--
-- We send a close request to the peer and continue processing
-- any incoming messages until the server replies with its own
-- close control message.
waitForGracefulShutdown :: WS.Connection -> IO ()
waitForGracefulShutdown connection = do
  WS.sendClose connection ("Closing." :: ByteString)

  -- Grace period
  response <- startGracePeriod $ forever (WS.receiveDataMessage connection)

  when (isNothing response) throwNoResponseToCloseRequest

throwNoResponseToCloseRequest :: IO a
throwNoResponseToCloseRequest = throwIO $ WS.CloseRequest 1000 "No response to close request"

-- Authorization headers for Cachix Deploy

system :: String
system = System.Info.arch <> "-" <> System.Info.os

createHeaders ::
  -- | Agent name
  Text ->
  -- | Agent Token
  Text ->
  HTTP.RequestHeaders
createHeaders agentName agentToken =
  [ ("Authorization", "Bearer " <> toS agentToken),
    ("name", toS agentName),
    ("version", toS versionNumber),
    ("system", toS system)
  ]

logOnMessage :: Options -> Text
logOnMessage Options {host, identifier, path, useSSL} =
  [iTrim|
    ${Text.toTitle identifier} connecting to ${uri} over ${protocol}
  |]
  where
    uri = decodeUtf8 (URI.hostBS host) <> path

    protocol :: Text
    protocol = if useSSL then "HTTPS" else "HTTP"
