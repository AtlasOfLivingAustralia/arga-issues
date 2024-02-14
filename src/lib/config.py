from pathlib import Path

class Meta(type):
    def __new__(cls, name, bases, attrs):
        rootDir = Path(__file__).parents[2]
        for k, v in attrs.items():
            if k.startswith("__"):
                continue

            attrs[k] = rootDir / Path(v)

        return super().__new__(cls, name, bases, attrs)

class Files(metaclass=Meta):
    pass

class Folders(metaclass=Meta):
    src: Path = "src" # Source folder for all python code
    dataSources: Path = "dataSources" # Location of all source related files
    mapping: Path = "mapping" # Location for map files
    logs: Path = "logs" # Location of all logging files
