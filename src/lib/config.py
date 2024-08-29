from pathlib import Path
import toml
from enum import Enum

class ConfigType(Enum):
    FILES = "files"
    FOLDERS = "folders"
        
class ConfigMeta(type):
    def __new__(cls: type, name: str, bases: tuple[str], attrs: dict):
        rootDir = Path(__file__).parents[2]
        with open(rootDir / "config.toml") as fp:
            data = toml.load(fp)

        cfgType = attrs.get("cfg", None)
        if cfgType is None:
            raise AttributeError from Exception(f"No parameter `cfg` defined on object as required")

        cfgValue = cfgType.value
        items: dict | None = data.get(cfgValue, None)
        if items is None:
            raise AttributeError from Exception(f"Invalid config item: {cfgValue}")
        
        for k, v in items.items():
            path = rootDir / Path(v) if v.startswith("./") else Path(v)
            attrs[k] = path

        return super().__new__(cls, name, bases, attrs)

class Files(metaclass=ConfigMeta):
    cfg = ConfigType.FILES

class Folders(metaclass=ConfigMeta):
    cfg = ConfigType.FOLDERS
