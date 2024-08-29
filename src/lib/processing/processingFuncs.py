from pathlib import Path
import importlib

def importModule(path: str):
    scriptPath = Path(path)
    pathParts = scriptPath.parts[:-1] + (scriptPath.stem,) # Parts is tuple, concat with tuple only
    importPath = ".".join(pathParts)
    return importlib.import_module(importPath)

def importFunction(path: str, func: str):
    module = importModule(path)
    for obj in func.split("."):
        module = getattr(module, obj)
    return module
