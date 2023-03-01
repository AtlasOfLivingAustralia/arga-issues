from lib.processing.parser import SelectorParser
import lib.processing.processingFuncs as pFuncs

class Step:
    def __init__(self, stepInfo: dict, parser: SelectorParser):
        self.stepInfo = stepInfo
        self.parser = parser

        self.script = stepInfo.pop("script", None)
        self.func = stepInfo.pop("function", None)
        self.args = stepInfo.pop("args", [])
        self.kwargs = stepInfo.pop("kwargs", {})
        self.outputs = stepInfo.pop("outputs", [])

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
        if self.func is None:
            raise Exception("No function specified") from AttributeError
        
        self.outputs = self.parser.parseMultipleArgs(self.outputs)
        self.args = self.parser.parseMultipleArgs(self.args)
        self.kwargs = {key: self.parser.parseArg(value) for key, value in self.kwargs.items()}

        for info in stepInfo:
            print(f"Unknown step property: {info}")

    def getOutputs(self):
        return self.outputs

    def process(self, overwrite=False):
        if self.outputFiles and not overwrite and all(output.exists() for output in self.outputFiles):
            print("Outputs already exist, not overwriting")
            return

        processFunction = pFuncs.importFunction(self.script, self.func)

        msg = f"Running {self.script} function '{self.func}'"
        if self.args:
            msg += f" with args {self.args}"
        if self.kwargs:
            if self.args:
                msg += " and"
            msg += f" with kwargs {self.kwargs}"
        print(msg)
        
        return processFunction(*self.args, **self.kwargs)