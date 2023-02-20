import importlib
from pathlib import Path

def importModuleFromPath(path: str):
    scriptPath = Path(path)
    pathString = str(scriptPath.parent / scriptPath.stem)
    pathString = pathString.replace("\\", ".")
    return importlib.import_module(pathString)

class Step:
    def __init__(self, script, outputs, args=[], kwargs={}):
        self.script = script
        self.outputs = outputs
        self.args = args
        self.kwargs = kwargs

    def process(self, overwrite=False):
        if not overwrite and self.allOutputsExist():
            print("Outputs already exist, not overwriting")
            return

        module = importModuleFromPath(self.script)

        msg = f"Running script {self.script}"

        if self.args:
            msg += f" with args {self.args}"

        if self.kwargs:
            if self.args:
                msg += " and"
            msg += f" with kwargs {self.kwargs}"

        print(msg)
        result = module.process(*self.args, **self.kwargs)

    def allOutputsExist(self):
        return all(output.exists() for output in self.outputs)

class Processor:
    def __init__(self, directoryPath: Path, inputFiles: list[Path], processingSteps: list[dict]):
        self.directoryPath = directoryPath
        self.inputFiles = inputFiles
        self.inputPaths = [self.directoryPath / file for file in inputFiles]
        self.steps = []

        if not processingSteps:
            self.outputFiles = inputFiles
            return

        for step in processingSteps:
            if self.steps:
                inputs = self.steps[-1].outputs
            else:
                inputs = self.inputPaths

            script, outputs, args, kwargs = self.parseProcessing(step)

            args = self.parseArgs(args, inputs)
            outputs = self.parseArgs(outputs, inputs)

            outputPaths = [self.directoryPath / output for output in outputs]

            self.steps.append(Step(script, outputPaths, args, kwargs))
        
        self.outputFiles = outputs

    def parseProcessing(self, processingStep):
        script = processingStep.get("script", None)
        output = processingStep.get("outputs", None)
        args = processingStep.get("args", [])
        kwargs = processingStep.get("kwargs", {})

        if script is None:
            raise Exception("No script specified") from AttributeError

        if output is None:
            raise Exception("No output specified") from AttributeError

        return (script, output, args, kwargs)

    def isSelector(self, arg):
        return arg[0] == "{" and arg[-1] == "}" # Output hasn't got a complete selector

    def getSelector(self, arg, inputs):
        selector = arg[1:-1] # Strip off braces

        attrs = [attr.strip() for attr in selector.split(',')]

        rng = attrs[0].find(">")
        if rng >= 0: # Range of inputs
            first = attrs[0][:rng]
            last = attrs[0][rng+1:]

            if not first or not last:
                raise Exception(f"Invalid range selection '{attrs[0]}'") from AttributeError

            selected = inputs[int(first):int(last) + 1]
        else: # Single input
            selected = [inputs[int(attrs[0])]]

        if len(attrs) == 1: # Selector only
            return selected

        modifier = attrs[1]

        # Apply modifier
        if modifier == "STEM":
            selected = [s.stem for s in selected]
        elif modifier == "PARENT":
            selected = [s.parent for s in selected]
        elif modifier == "PARENT_STEM":
            selected = [s.parent.stem for s in selected]

        # No suffix addition if only 2 attributes
        if len(attrs) == 2:
            return [Path(s) for s in selected]

        # Apply suffix from last attribute
        return [Path(s + attrs[2]) for s in selected]
    
    def parseArgs(self, args, inputPaths):
        output = []

        for arg in args:
            if not self.isSelector(arg):
                output.append(Path(arg))
                continue

            output.extend(self.getSelector(arg, inputPaths))

        return output

    def process(self):
        for step in self.steps:
            step.process()

    def getOutputFilePaths(self) -> list[Path]:
        if self.steps:
            return self.steps[-1].outputs
        return self.inputPaths

    def getOutputFiles(self) -> list[Path]:
        return self.outputFiles
