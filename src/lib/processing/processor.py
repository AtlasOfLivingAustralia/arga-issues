from pathlib import Path
import lib.processing.processingFuncs as pFuncs

class SelectorParser:
    def __init__(self, baseDirectory: Path, inputPaths: list[Path]):
        self.baseDirectory = baseDirectory
        self.inputPaths = inputPaths

    def parseArg(self, arg: str) -> Path|str:
        if self.validSelector(arg):
            return self.parseSelector(arg)
        return arg

    def parseMultipleArgs(self, args: list[str]) -> list[Path|str]:
        return [self.parseArg(arg) for arg in args]

    def validSelector(self, string: str) -> bool:
        return isinstance(string, str) and string[0] == "{" and string[-1] == "}" # Output hasn't got a complete selector
    
    def parseSelector(self, arg: str) -> str:
        selector = arg[1:-1] # Strip off braces

        attrs = [attr.strip() for attr in selector.split(',')]
        selectType = attrs.pop(0)

        if selectType == "INPUT": # Input selector
            return self.inputSelector(*attrs)
        
        if selectType == "PATH": # Path creator
            return self.pathSelector(*attrs)
    
    def inputSelector(self, selected=None, modifier=None, suffix=None):
        if selected is None or not selected.isdigit():
            raise Exception(f"Invalid input value for input selection: {selected}")

        selectInt = int(selected)

        if selectInt < 0 or selectInt >= len(self.inputPaths):
            raise Exception(f"Invalid input selection: {selected}")
        
        selectedPath = self.inputPaths[selectInt]

        if modifier is None: # Selector only
            return selectedPath

        # Apply modifier
        if modifier == "STEM":
            selectedPathStr = selected.stem
        elif modifier == "PARENT":
            selectedPathStr = str(selected.parent)
        elif modifier == "PARENT_STEM":
            selectedPathStr = selected.parent.stem
        else:
            raise Exception(f"Invalid modifer: {modifier}") from AttributeError
        
        if suffix is None: # No suffix addition
            return Path(selectedPathStr)

        return Path(selected + suffix) # Apply suffix
    
    def pathSelector(self, fileName=None):
        if fileName is None:
            raise Exception(f"FileName for path not provided") from AttributeError
        
        return self.baseDirectory / fileName

class Step:
    def __init__(self, stepInfo: dict, parser: SelectorParser):
        self.stepInfo = stepInfo
        self.parser = parser

        self.script = stepInfo.pop("script", None)
        self.func = stepInfo.pop("function", None)
        self.args = stepInfo.pop("args", [])
        self.kwargs = stepInfo.pop("kwargs", {})
        self.outputFiles = stepInfo.pop("outputs", [])

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
        if self.func is None:
            raise Exception("No function specified") from AttributeError
        
        self.outputFiles = self.parser.parseMultipleArgs(self.outputFiles)
        self.args = self.parser.parseMultipleArgs(self.args)
        self.kwargs = {key: self.parser.parseArg(value) for key, value in self.kwargs.items()}

        for info in stepInfo:
            print(f"Unknown step property: {info}")

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
            step = Step(stepInfo.copy(), SelectorParser(self.directoryPath, inputs))
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
