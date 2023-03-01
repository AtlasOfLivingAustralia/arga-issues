from pathlib import Path
from lib.processing.parser import SelectorParser
from lib.processing.steps import Step

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
            self.outputFiles.extend(step.getOutputs())
            inputs = [directoryPath / file for file in step.getOutputs()]
            self.outputPaths.extend(inputs)

    def process(self):
        for step in self.steps:
            step.process()

    def getOutputFilePaths(self) -> list[Path]:
        return self.outputPaths

    def getOutputFiles(self) -> list[str]:
        return self.outputFiles
