import lib.commonFuncs as cmn
import lib.config as cfg
import lib.dataframeFuncs as dff
from pathlib import Path
from lib.subfileWriter import Writer
import lib.processing.processingFuncs as pFuncs

class DWCProcessor:
        self.prefix = prefix

    def __init__(self, prefix: str, dwcProperties: dict, outputDir: Path):
        self.prefix = prefix
        self.dwcProperties = dwcProperties
        self.outputDir = outputDir

        mapPath = cfg.folderPaths.mapping / location
        if mapPath.exists():
            self.map = cmn.loadFromJson(mapPath)
        else:
            raise Exception(f"No DWC map found for location: {self.location}") from FileNotFoundError

        self.augments = dwcProperties.pop("augment", [])
        self.chunkSize = dwcProperties.pop("chunkSize", 100000)

        self.augmentSteps = [Augment(augProperties) for augProperties in self.augments]

        self.writer = Writer(outputDir, "dwcConversion", "dwcChunk")

    def process(self, inputPath: Path, outputFilePath: Path, sep: str, header: int, encoding: str, overwrite: bool = False):
        if outputFilePath.exists() and not overwrite:
            print(f"{outputFilePath} already exists, exiting...")
            return

        for idx, df in enumerate(dff.chunkGenerator(inputPath, self.chunkSize, sep, header, encoding)):
            if idx == 0:
                newColMap, copyColMap = dff.createMappings(df.columns, self.dwcLookup, self.location)
             
            print(f"At chunk: {idx}", end='\r')
            df = dff.applyColumnMap(df, newColMap, copyColMap)
            df = dff.applyExclusions(df, self.exclude)
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
