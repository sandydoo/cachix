module Cachix.Deploy.StdinProcess where

import Protolude hiding (stdin)
import System.IO (hClose)
import System.Process
import Prelude (String, userError)

-- | Spawn a process with only stdin as an input
spawnProcess :: FilePath -> [String] -> String -> IO ()
spawnProcess cmd args input = do
  (Just stdin, _, _, handle) <-
    createProcess
      (proc cmd args)
        { std_in = CreatePipe,
          -- When launching cachix-deployment we need to make sure it's not killed when the main process is killed
          create_group = True,
          -- Same, but with posix api
          new_session = True
        }
  hPutStr stdin input
  hClose stdin
  exitcode <- waitForProcess handle
  case exitcode of
    ExitSuccess -> return ()
    ExitFailure code -> ioError $ userError $ "Process " <> show cmd <> " failed with exit code " <> show code
