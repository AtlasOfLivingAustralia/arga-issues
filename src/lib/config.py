import toml
from pathlib import Path
from dataclasses import make_dataclass, dataclass

rootDir = Path(__file__).parents[2] # Parent of absolute path to src folder
configFile = "config.toml"

configPath = rootDir / configFile
if not configPath.exists():
    raise Exception("No config file found!") from FileNotFoundError

with open(rootDir / configFile) as fp:
    config: dict[str, dict[str, str]] = toml.load(fp)

files = []
for file, path in config["files"].items():
    path = rootDir / path if path.startswith("./") else Path(path)
    files.append((file, Path, path))

files = make_dataclass("Files", files)

folders = []
for folder, path in config["folders"].items():
    path = path.rstrip("/")
    path = rootDir / path if path.startswith("./") else Path(path)
    folders.append((folder, Path, path))

folders = make_dataclass("Folders", folders)
