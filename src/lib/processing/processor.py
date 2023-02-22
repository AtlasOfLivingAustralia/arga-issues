from pathlib import Path
import lib.processing.processingFuncs as pFuncs

class SelectorParser:
    def __init__(self, inputPaths):
        self.inputPaths = inputPaths

    def parseArg(self, arg: str) -> Path|str:
        if self.validSelector(arg):
            return self.parseSelector(arg, self.inputPaths)
        return arg

    def parseMultipleArgs(self, args: list[str]) -> list[Path|str]:
        return [self.parseArg(arg) for arg in args]

    def validSelector(self, string: str) -> bool:
        return isinstance(string, str) and string[0] == "{" and string[-1] == "}" # Output hasn't got a complete selector
    
    def parseSelector(self, arg: str, inputs: list[Path]) -> str:
        selector = arg[1:-1] # Strip off braces

        attrs = [attr.strip() for attr in selector.split(',')]
        selected = attrs[0]

        if not selected.isdigit():
            raise Exception(f"Invalid input value for input selection: {selected}")

        selected = int(selected)

        if selected < 0 or selected >= len(inputs):
            raise Exception(f"Invalid input selection: {selected}")
        
        selected = inputs[selected]

        if len(attrs) == 1: # Selector only
            return selected

        modifier = attrs[1]

        # Apply modifier
        if modifier == "STEM":
            selected = selected.stem
        elif modifier == "PARENT":
            selected = selected.parent
        elif modifier == "PARENT_STEM":
            selected = selected.parent.stem

        # No suffix addition if only 2 attributes
        if len(attrs) == 2:
            return Path(selected)

        # Apply suffix from last attribute
        return Path(selected + attrs[2])

class Step:
    def __init__(self, stepInfo: dict, inputPaths: list):
        self.stepInfo = stepInfo
        self.inputPaths = inputPaths

        self.script = stepInfo.pop("script", None)
        self.func = stepInfo.pop("function", None)
        self.args = stepInfo.pop("args", [])
        self.kwargs = stepInfo.pop("kwargs", {})
        self.outputFiles = stepInfo.pop("outputs", [])

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
        if self.func is None:
            raise Exception("No function specified") from AttributeError

        self.selectorParser = SelectorParser(self.inputPaths)
        
        self.outputFiles = self.selectorParser.parseMultipleArgs(self.outputFiles)
        self.args = self.selectorParser.parseMultipleArgs(self.args)
        self.kwargs = {key: self.selectorParser.parseArg(value) for key, value in self.kwargs.items()}

        for info in stepInfo:
            print(f"Unknown step property: {info}")

    def process(self, overwrite=False):
        if self.outputs and not overwrite and all(output.exists() for output in self.outputFiles):
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
        
        processFunction(*self.args, **self.kwargs)

class Processor:
    def __init__(self, directoryPath: Path, inputFiles: list[Path], processingSteps: list[dict]):
        self.directoryPath = directoryPath
        self.inputFiles = inputFiles
        self.inputPaths = [directoryPath / file for file in inputFiles]
        self.outputFiles = []
        self.outputPaths = []
        self.steps = []

        if not processingSteps:
            self.outputFiles = self.inputFiles
            self.outputPaths = self.inputPaths
            return

        inputs = self.inputPaths
        for stepInfo in processingSteps:
            step = Step(stepInfo.copy(), inputs)
            self.steps.append(step)
            self.outputFiles.extend(step.outputFiles)
            inputs = [directoryPath / file for file in step.outputFiles]
            self.outputPaths.extend(inputs)

    def process(self):
        for step in self.steps:
            step.process()

    def getOutputFilePaths(self) -> list[Path]:
        return self.outputPaths

    def getOutputFiles(self) -> list[str]:
        return self.outputFiles
