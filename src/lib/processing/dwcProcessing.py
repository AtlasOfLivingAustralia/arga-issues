import pandas as pd
from pathlib import Path
import lib.dataframeFuncs as dff
import lib.processing.processingFuncs as pFuncs
from lib.tools.bigFileWriter import BigFileWriter
from lib.processing.dwcMapping import Remapper

class DWCProcessor:
    def __init__(self, location: str, dwcProperties: dict, outputDir: Path):
        self.location = location
        self.dwcProperties = dwcProperties
        self.outputDir = outputDir

        self.augments = dwcProperties.pop("augment", [])
        self.chunkSize = dwcProperties.pop("chunkSize", 100000)

        self.augmentSteps = [DWCAugment(augProperties) for augProperties in self.augments]
        self.remapper = Remapper(location)

    def process(self, inputPath: Path, outputFolderName: str, sep: str = ",", header: int = 0, encoding: str = "utf-8", overwrite: bool = False) -> Path:
        outputFolderPath = self.outputDir / outputFolderName
        if outputFolderPath.exists() and not overwrite:
            print(f"{outputFolderPath} already exists, exiting...")
            return
        
        # Get columns and create mappings
        preGenerator = dff.chunkGenerator(inputPath, 1, sep, header, encoding)
        headerChunk = next(preGenerator)
        print(headerChunk.columns)
        self.remapper.createMappings(headerChunk.columns)
        
        if not self.remapper.verifyUnique():
            return
        
        events = self.remapper.getEvents()

        writers = {event: BigFileWriter(outputFolderPath / f"{event.lower().replace(' ', '_')}.csv") for event in events}
        for idx, chunk in enumerate(dff.chunkGenerator(inputPath, self.chunkSize, sep, header, encoding)):
            print(f"At chunk: {idx}", end='\r')

            df = self.remapper.applyMap(chunk)
            df = self.applyAugments(df)

            for eventColumn in df.columns.levels[0]:
                writers[eventColumn].writeDF(df[eventColumn])

        for writer in writers.values():
            writer.oneCSV()

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

    def process(self, df: pd.DataFrame) -> None:
        processFunction = pFuncs.importFunction(self.path, self.function)
        return processFunction(df, *self.args, **self.kwargs)
