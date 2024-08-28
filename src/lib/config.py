from pathlib import Path
import toml
from enum import Enum

class ConfigType(Enum):
    FILES = "files"
    FOLDERS = "folders"
        
class Config:
    def __init__(self, configItems: ConfigType):
        rootDir = Path(__file__).parents[2]
        with open(rootDir / "config.toml") as fp:
            data = toml.load(fp)

        items: dict | None = data.get(configItems.value, None)
        if items is None:
            raise AttributeError from Exception(f"Invalid config item: {configItems.value}")
        
        for k, v in items.items():
            path = rootDir / Path(v) if v.startswith("./") else Path(v)
            setattr(self, k, path)

class Files(Config):
    def __init__(self):
        super().__init__(ConfigType.FILES)

class Folders(Config):
    def __init__(self):
        super().__init__(ConfigType.FOLDERS)
