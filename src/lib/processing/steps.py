from lib.processing.parser import SelectorParser
import lib.processing.processingFuncs as pFuncs
from pathlib import Path

class ScriptStep:
    def __init__(self, stepParamters: dict):
        self.script = stepParamters.pop("script", None)
        self.function = stepParamters.pop("function", None)
        self.args = stepParamters.pop("args", [])
        self.kwargs = stepParamters.pop("kwargs", {})
        self.outputs = stepParamters.pop("outputs", [])

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
        if self.function is None:
            raise Exception("No function specified") from AttributeError

        for parameter in stepParamters:
            print(f"Unknown step parameter: {parameter}")

    def getOutputs(self):
        return self.outputs

    def process(self, verbose=True):
        processFunction = pFuncs.importFunction(self.script, self.function)

        if verbose:
            msg = f"Running {self.script} function '{self.function}'"
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
            print("Outputs already exist, not overwriting")
            return
        
        super().process(verbose)

    def checkOutputsExist(self):
        for output in self.outputs:
            if isinstance(output, Path) and not output.exists():
                return False
        return True
