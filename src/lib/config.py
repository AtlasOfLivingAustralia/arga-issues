import toml
from pathlib import Path

class PathCollection:
    def __init__(self, pathDict, rootDirPath):
        for pathKey, pathValue in pathDict.items():
            pathValue.rstrip('/')

            if pathValue.startswith('./'):
                path = rootDirPath / Path(pathValue[2:])
            else:
                path = Path(pathValue)

            setattr(self, pathKey, path)

configFile = "config.toml"

rootDirPath = Path(__file__).parents[2] # Parent of absolute path to src folder

with open(rootDirPath / Path(configFile)) as fp:
    rawConfig = toml.load(fp)

defaultFiles = rawConfig["default"]["files"]
defaultFolders = rawConfig["default"]["folders"]

customFiles = rawConfig["custom"]["files"]
customFolders = rawConfig["custom"]["folders"]

files = {key: customFiles.get(key, value) for key, value in defaultFiles.items()}
paths = {key: customFolders.get(key, value) for key, value in defaultFolders.items()}

filePaths = PathCollection(files, rootDirPath)
folderPaths = PathCollection(paths, rootDirPath)
