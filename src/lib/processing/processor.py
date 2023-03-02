from pathlib import Path
from lib.processing.parser import SelectorParser
from lib.processing.steps import FileStep, DownloadStep

class FileProcessor:
    def __init__(self, inputPaths: list[Path], processingSteps: list[dict], processingDirectory: Path, outputDirectory: Path = None):
        self.inputPaths = inputPaths
        self.steps = []

        if not processingSteps:
            self.outputPaths = self.inputPaths
            return
        
        if outputDirectory is None:
            outputDirectory = processingDirectory

        directory = processingDirectory
        nextInputs = inputPaths
        for idx, step in enumerate(processingSteps):
            if idx == len(processingSteps):
                directory = outputDirectory

            parser = SelectorParser(directory, nextInputs)

            if "download" in step:
                stepObject = DownloadStep(step, parser)
            else:
                stepObject = FileStep(step, parser)
            self.steps.append(stepObject)
            nextInputs = stepObject.getOutputs()

        self.outputPaths = nextInputs

    @classmethod
    def fromSteps(cls, inputPaths, steps, processingDirectory, outputDirectory=None):
        obj = cls(inputPaths, {}, processingDirectory, outputDirectory)
        obj.steps = steps
        obj.outputPaths = steps[-1].getOutputs()
        return obj

    def process(self, overwrite=False):
        for step in self.steps:
            step.process(overwrite)

    def getOutputs(self) -> list[Path]:
        return self.outputPaths
