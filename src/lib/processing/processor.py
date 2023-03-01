from pathlib import Path
from lib.processing.parser import SelectorParser
from lib.processing.steps import FileStep

class FileProcessor:
    def __init__(self, inputPaths: list[Path], processingSteps: list[dict], processingDirectory: Path, outputDirectory: Path = None):
        self.inputPaths = inputPaths
        self.steps = []

        print("SETTING UP PROCESSOR")

        if not processingSteps:
            self.outputPaths = self.inputPaths
            print("BREAKING EARLY")
            return
        
        if outputDirectory is None:
            outputDirectory = processingDirectory

        directory = processingDirectory
        nextInputs = inputPaths
        for idx, step in enumerate(processingSteps):
            if idx == len(processingSteps):
                directory = outputDirectory

            parser = SelectorParser(directory, nextInputs)
            fileStep = FileStep(step, parser)
            self.steps.append(fileStep)
            nextInputs = fileStep.getOutputs()

        self.outputPaths = nextInputs

    def process(self, overwrite=False):
        for step in self.steps:
            step.process(overwrite)

    def getOutputs(self) -> list[Path]:
        return self.outputPaths
