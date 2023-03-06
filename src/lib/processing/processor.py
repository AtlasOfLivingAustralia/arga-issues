from pathlib import Path
from lib.processing.parser import SelectorParser
from lib.processing.steps import FileStep, DownloadStep

class FileProcessor:
    def __init__(self, inputPaths: list[Path], processingSteps: list[dict], rootDir: Path, downloadDir: Path, processingDir: Path):
        self.inputPaths = inputPaths
        self.steps = []

        if not processingSteps:
            self.outputPaths = self.inputPaths
            return

        nextInputs = inputPaths
        for step in processingSteps:
            parser = SelectorParser(rootDir, downloadDir, processingDir, nextInputs)

            if "download" in step:
                stepObject = DownloadStep(step, parser)
            else:
                stepObject = FileStep(step, parser)
            self.steps.append(stepObject)
            nextInputs = stepObject.getOutputs()

        self.outputPaths = nextInputs

    @classmethod
    def fromSteps(cls, inputPaths, steps, rootDir, downloadDir, processingDir):
        obj = cls(inputPaths, {}, rootDir, downloadDir, processingDir)
        obj.steps = steps
        obj.outputPaths = steps[-1].getOutputs()
        return obj

    def process(self, overwrite=False):
        for step in self.steps:
            step.process(overwrite)

    def getOutputs(self) -> list[Path]:
        return self.outputPaths
