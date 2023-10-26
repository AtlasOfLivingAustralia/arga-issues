import pandas as pd
from pathlib import Path
import lib.dataframeFuncs as dff
import lib.processing.processingFuncs as pFuncs
from lib.tools.bigFileWriter import BigFileWriter
from lib.processing.dwcMapping import Remapper
from lib.processing.parser import SelectorParser

class DWCProcessor:
    def __init__(self, location: str, dwcProperties: dict, parser: SelectorParser):
        self.location = location
        self.dwcProperties = dwcProperties
        self.parser = parser
        self.outputDir = self.parser.dwcDir

        self.augments = dwcProperties.pop("augment", [])
        self.chunkSize = dwcProperties.pop("chunkSize", 100000)
        self.customMapPath = self.parser.parseArg(dwcProperties.pop("customMap", None), [])

        self.augmentSteps = [DWCAugment(augProperties) for augProperties in self.augments]
        self.remapper = Remapper(location, self.customMapPath)

    def process(self, inputPath: Path, outputFolderName: str, sep: str = ",", header: int = 0, encoding: str = "utf-8", overwrite: bool = False) -> Path:
        outputFolderPath = self.outputDir / outputFolderName
        if outputFolderPath.exists() and not overwrite:
            print(f"{outputFolderPath} already exists, exiting...")
            return
        
        # Get columns and create mappings
        preGenerator = dff.chunkGenerator(inputPath, 1, sep, header, encoding)
        headerChunk = next(preGenerator)
        self.remapper.createMappings(headerChunk.columns)
        
        if not self.remapper.verifyUnique():
            return
        
        events = self.remapper.getEvents()

        writers: dict[str, BigFileWriter] = {}
        for event in events:
            cleanedName = event.lower().replace(" ", "_")
            writers[event] = BigFileWriter(outputFolderPath / f"{cleanedName}.csv", f"{cleanedName}_chunks")

        for idx, chunk in enumerate(dff.chunkGenerator(inputPath, self.chunkSize, sep, header, encoding)):
            print(f"At chunk: {idx}", end='\r')

            df = self.remapper.applyMap(chunk)
            df = self.applyAugments(df)

            for eventColumn in df.columns.levels[0]:
                writers[eventColumn].writeDF(df[eventColumn])

        for writer in writers.values():
            writer.oneFile()

        return outputFolderPath

    def applyAugments(self, df: pd.DataFrame) -> pd.DataFrame:
        for augment in self.augmentSteps:
            df = augment.process(df)
        return df

class DWCAugment:
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

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        processFunction = pFuncs.importFunction(self.path, self.function)
        return processFunction(df, *self.args, **self.kwargs)
