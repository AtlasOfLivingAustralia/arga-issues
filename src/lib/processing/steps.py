from lib.processing.parser import SelectorParser
import lib.processing.processingFuncs as pFuncs
from pathlib import Path
import subprocess
import platform

class ScriptStep:
    def __init__(self, stepParamters: dict):
        self.path = stepParamters.pop("path", None)
        self.function = stepParamters.pop("function", None)
        self.args = stepParamters.pop("args", [])
        self.kwargs = stepParamters.pop("kwargs", {})
        self.outputs = stepParamters.pop("outputs", [])

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError

        for parameter in stepParamters:
            print(f"Unknown step parameter: {parameter}")

    def getOutputs(self):
        return self.outputs

    def process(self, verbose=True):
        processFunction = pFuncs.importFunction(self.path, self.function)

        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {self.args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {self.kwargs}"
            print(msg)
        
        return processFunction(*self.args, **self.kwargs)

class FileStep(ScriptStep):
    def __init__(self, stepParamters: dict, parser: SelectorParser):
        self.parser = parser

        super().__init__(stepParamters)
        
        self.outputs = self.parser.parseMultipleArgs(self.outputs)
        self.args = self.parser.parseMultipleArgs(self.args)
        self.kwargs = {key: self.parser.parseArg(value) for key, value in self.kwargs.items()}

    def process(self, overwrite=False, verbose=True):
        if self.outputs and not overwrite and self.checkOutputsExist():
            print(f"All outputs {self.outputs} exist, not overwriting")
            return
        
        return super().process(verbose)

    def checkOutputsExist(self):
        for output in self.outputs:
            if isinstance(output, Path) and not output.exists():
                return False
        return True

class DownloadStep:
    def __init__(self, stepParameters: dict, parser: SelectorParser):
        self.parser = parser

        self.url = stepParameters.pop("download", None)
        self.filePath = stepParameters.pop("filePath", None)
        self.user = stepParameters.pop("user", "")
        self.password = stepParameters.pop("pass", "")

        if self.url is None:
            raise Exception("No url specified") from AttributeError
        
        if self.filePath is None:
            raise Exception("No file path specified") from AttributeError
        
        self.filePath = self.parser.parseArg(self.filePath)
        
    def getOutputs(self):
        return [self.filePath]
    
    def process(self, overwrite=False):
        if self.filePath.exists() and not overwrite:
            return
        
        print(f"Downloading from {self.url} to file {self.filePath}")

        curl = "curl"
        if platform.system() == 'Windows':
            curl = "curl.exe"

        args = [curl, self.url, "-o", self.filePath]
        if self.user:
            args.extend(["--user", f"{self.user}:{self.password}"])

        subprocess.run(args)

class AugmentStep(ScriptStep):

    def process(self, df, verbose=False):
        processFunction = pFuncs.importFunction(self.path, self.function)
        return processFunction(df, *self.args, **self.kwargs)
