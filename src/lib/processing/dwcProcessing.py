import pandas as pd
import numpy as np
from pathlib import Path
import lib.dataframeFuncs as dff
import lib.processing.processingFuncs as pFuncs
from lib.tools.bigFileWriter import BigFileWriter
from lib.processing.dwcMapping import Remapper, Events
from lib.processing.parser import SelectorParser
from lib.tools.logger import logger

class DWCProcessor:
    def __init__(self, location: str, dwcProperties: dict, parser: SelectorParser):
        self.location = location
        self.dwcProperties = dwcProperties
        self.parser = parser
        self.outputDir = self.parser.dwcDir

        self.augments = dwcProperties.pop("augment", [])
        self.chunkSize = dwcProperties.pop("chunkSize", 100000)
        self.setNA = dwcProperties.pop("setNA", [])
        self.fillNA = ColumnFiller(dwcProperties.pop("fillNA", {}))
        self.customMapPath = self.parser.parseArg(dwcProperties.pop("customMap", None), [])

        self.augmentSteps = [DWCAugment(augProperties) for augProperties in self.augments]
        self.remapper = Remapper(location, self.customMapPath)

    def process(self, inputPath: Path, outputFolderName: str, sep: str = ",", header: int = 0, encoding: str = "utf-8", overwrite: bool = False) -> Path:
        outputFolderPath = self.outputDir / outputFolderName
        if outputFolderPath.exists() and not overwrite:
            logger.info(f"{outputFolderPath} already exists, exiting...")
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

            df = self.remapper.applyMap(chunk) # Returns a multi-index dataframe
            for na in self.setNA:
                df.replace(na, np.NaN, inplace=True)

            df = self.fillNA.apply(df)
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
    
class ColumnFiller:
    def __init__(self, fillProperties: dict[str, dict]):
        self.fillProperties = fillProperties

        for event, columns in self.fillProperties.items():
            if not self._validEvent(event):
                raise Exception(f"Unknown event: {event}") from AttributeError
            
            for mapToDict in columns.values():
                for mapToEvent in mapToDict:
                    if not self._validEvent(mapToEvent):
                        raise Exception(f"Unknown mapTo event: {event}") from AttributeError

    def _validEvent(self, event: str) -> bool:
        return event in Events._value2member_map_
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        for event, columns in self.fillProperties.items():
            for columnName, mapTo in columns.items():
                for mapToEvent, mapToColumnList in mapTo.items():
                    for mapToColumn in mapToColumnList:
                        df[(mapToEvent, mapToColumn)].fillna(df[(event, columnName)], inplace=True)

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
