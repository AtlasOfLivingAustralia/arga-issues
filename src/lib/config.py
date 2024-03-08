from pathlib import Path

class Meta(type):
    def __new__(cls, name, bases, attrs):
        rootDir = Path(__file__).parents[2]
        for k, v in attrs.items():
            if k.startswith("__"):
                continue

            if v.startswith("./"):
                path = rootDir / Path(v)
            else:
                path = Path(v)

            attrs[k] = path

        return super().__new__(cls, name, bases, attrs)

# Edit these classes, "./"" indicates root directory for project
class Files(metaclass=Meta):
    pass

class Folders(metaclass=Meta):
    src: Path = "./src" # Source folder for all python code
    dataSources: Path = "./dataSources" # Location of all source related files
    logs: Path = "./logs" # Location of all logging files
