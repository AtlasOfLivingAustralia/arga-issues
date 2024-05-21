from pathlib import Path
from lib.processing.stages import File
from lib.tools.logger import Logger
import lib.processing.processingFuncs as pFuncs
from enum import Enum
from functools import wraps

class Script:
    def __init__(self, baseDir: Path, outputDir: Path, scriptInfo: dict):
        self.baseDir = baseDir
        self.outputDir = outputDir
        self.parser = Parser(baseDir, outputDir)

        self._readInfo(scriptInfo)

        for parameter in scriptInfo:
            Logger.debug(f"Unknown step parameter: {parameter}")

    def _readInfo(self, info: dict) -> None:
        # Input information
        self.path = info.pop("path", None)
        self.function = info.pop("function", None)
        self.args = info.pop("args", [])
        self.kwargs = info.pop("kwargs", {})
        self.variable = info.pop("variable", [])

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        # Output information
        self.output = info.pop("output", "")
        self.outputProperties = info.pop("outputProperties", {})
        
    def getOutputs(self, inputs: list[File] = []) -> list[File]:
        return [File(self.outputDir / name, self.outputProperties) for name in self.parser.parseArg(self.output, self.variable, inputs)]

    def _runScript(self, outputFile: Path | None, verbose: bool, *args: list, **kwargs: dict) -> any:
        processFunction = pFuncs.importFunction(self.path, self.function)

        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if args:
                msg += f" with args {args}"
            if kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {kwargs}"
            Logger.info(msg)

        output = processFunction(*args, **kwargs)

        if outputFile is not None:
            if not outputFile.exists():
                Logger.warning(f"Output {outputFile} was not created")
            
        return output

    def run(self, overwrite: bool = False, verbose: bool = False, inputs: list[File] = [], **runtimeKwargs: dict) -> any:
        outputs = self.getOutputs(inputs)
        for output in outputs:
            if output.exists():
                if not overwrite:
                    Logger.info(f"Output {output} exist and not overwriting, skipping '{self.function}'")
                    return
                
                output.delete()

        if self.variable: # Parallelise
            for var in self.variable:
                
                
        args = [self.parser.parseArg(arg) for arg in self.args]
        kwargs = {key: self.parser.parseArg(value) for key, value in self.kwargs} | {key: self.parser.parseArg(value) for key, value in runtimeKwargs}

        return self._runScript(output, verbose, *args, **kwargs)

class Parser:
    class Key(Enum):
        INPUT_STEM = "S"
        INPUT_PARENT = "P"
        VARIABLE = "V"

    def __init__(self, baseDir: Path, outputDir: Path, inputs: list[File], variable: list[str], outputs: list[File]) -> None:
        self.baseDir = baseDir
        self.outputDir = outputDir

    def parseArg(self, arg: str) -> Path | str:
        pass

    def parseArgList(self, args: list[str]) -> list[Path | str]:
        return [self.parseArg(arg) for arg in args]

    def parseKwargs(self, args: dict[any, str]) -> dict[any, Path | str]:
        return {key: self.parseArg(value) for key, value in args.items()}

    def _checkForPaths(self, arg: str) -> Path | str:
        if arg.startswith("./"):
            workingDir = self.baseDir
            return workingDir / arg[2:]
         
        if arg.startswith("../"):
            workingDir = self.baseDir.parent
            newStructure = arg[3:]
            while newStructure.startswith("../"):
                workingDir = workingDir.parent
                newStructure = newStructure[3:]

            return workingDir / newStructure
        
        return arg
