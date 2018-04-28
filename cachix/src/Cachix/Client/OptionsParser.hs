module Cachix.Client.OptionsParser
  ( CachixCommand(..)
  , BinaryCacheName
  , getOpts
  ) where

import Protolude
import Options.Applicative

{-
data CachixOptions = CachixOptions
  {
  } deriving (Show)

parserCachixOptions :: Parser CachixOptions
parserCachixOptions = return $ CachixOptions
-}
type BinaryCacheName = Text

data CachixCommand
  = AuthToken Text
  | Create BinaryCacheName
  | Sync (Maybe BinaryCacheName)
  | Use BinaryCacheName
  deriving (Show)

parserCachixCommand :: Parser CachixCommand
parserCachixCommand = subparser $
  command "authtoken" (infoH authtoken (progDesc "Print greeting")) <>
  command "create" (infoH create (progDesc "Say goodbye")) <>
  command "sync" (infoH sync (progDesc "Say goodbye")) <>
  command "use" (infoH use (progDesc "Say goodbye"))
  where
    authtoken = AuthToken <$> strArgument (metavar "TOKEN")
    create = Create <$> strArgument (metavar "NAME")
    sync = Sync <$> optional (strArgument (metavar "NAME"))
    use = Use <$> strArgument (metavar "NAME")

getOpts :: IO CachixCommand
getOpts = customExecParser (prefs showHelpOnEmpty) opts

opts :: ParserInfo CachixCommand
opts = infoH parserCachixCommand desc

desc :: InfoMod a
desc = fullDesc
    <> progDesc "TODO"
    <> header "cachix.org command interface"
    -- TODO: usage footer

infoH :: Parser a -> InfoMod a -> ParserInfo a
infoH a = info (helper <*> a)