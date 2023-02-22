from pathlib import Path
import importlib

def importModule(path: str):
    scriptPath = Path(path)
    pathString = str(scriptPath.parent / scriptPath.stem)
    pathString = pathString.replace("\\", ".")
    return importlib.import_module(pathString)

def importFunction(path: str, func: str):
    module = importModule(path)
    return getattr(module, func)
