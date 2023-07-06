import lib.dataframeFuncs as dff
from pathlib import Path
from lib.subfileWriter import Writer
from lib.remapper import Remapper
import lib.processing.processingFuncs as pFuncs

class DWCProcessor:

    def __init__(self, location: str, dwcProperties: dict, outputDir: Path):
        self.location = location
        self.dwcProperties = dwcProperties
        self.outputDir = outputDir

        self.augments = dwcProperties.pop("augment", [])
        self.chunkSize = dwcProperties.pop("chunkSize", 100000)

        self.augmentSteps = [Augment(augProperties) for augProperties in self.augments]

        self.writer = Writer(outputDir, "dwcConversion", "dwcChunk")
        self.remapper = Remapper(location)

    def process(self, inputPath: Path, outputFilePath: Path, sep: str, header: int, encoding: str, overwrite: bool = False):
        if outputFilePath.exists() and not overwrite:
            print(f"{outputFilePath} already exists, exiting...")
            return

        for idx, chunk in enumerate(dff.chunkGenerator(inputPath, self.chunkSize, sep, header, encoding)):
            if idx == 0:
                self.remapper.createMappings(chunk.columns)
             
            print(f"At chunk: {idx}", end='\r')
            df = self.remapper.applyMap(chunk, False)
            # df = dff.applyExclusions(df, self.exclude)
            df = self.applyAugments(df)
            # df = dff.dropEmptyColumns(df)

            self.writer.writeDF(df)

        self.writer.oneFile(outputFilePath)

    def applyAugments(self, df):
        for augment in self.augmentSteps:
            df = augment.process(df)
        return df

class Augment:
    def __init__(self, augmentProperties: list[dict]):
        self.augmentProperties = augmentProperties.copy()

        self.path = self.augmentProperties.pop("path", None)
        self.function = self.augmentProperties.pop("function", None)
        self.args = self.augmentProperties.pop("args", [])
        self.kwargs = self.augmentProperties.pop("kwargs", {})

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError

    def process(self, df):
        processFunction = pFuncs.importFunction(self.path, self.function)
        return processFunction(df, *self.args, **self.kwargs)
