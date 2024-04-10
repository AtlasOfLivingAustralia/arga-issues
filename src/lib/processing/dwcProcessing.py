import pandas as pd
import numpy as np
from pathlib import Path
import lib.commonFuncs as cmn
import lib.processing.processingFuncs as pFuncs
import lib.tools.zipping as zp
from lib.tools.bigFileWriter import BigFileWriter
from lib.processing.mapping import Remapper, Event, MapManager
from lib.processing.parser import SelectorParser
from lib.tools.logger import Logger
import gc

class DWCProcessor:
    def __init__(self, location: str, dwcProperties: dict, parser: SelectorParser):
        self.location = location
        self.dwcProperties = dwcProperties
        self.parser = parser
        self.outputDir = self.parser.dwcDir
        self.recordsFile = "records.txt"

        self.mapID = dwcProperties.pop("mapID", -1)

        self.augments = dwcProperties.pop("augment", [])
        self.chunkSize = dwcProperties.pop("chunkSize", 1024)
        self.setNA = dwcProperties.pop("setNA", [])
        self.fillNA = ColumnFiller(dwcProperties.pop("fillNA", {}))
        self.skipRemap = dwcProperties.pop("skipRemap", [])
        self.preserveDwC = dwcProperties.pop("preserveDwC", False)
        self.prefixUnmapped = dwcProperties.pop("prefixUnmapped", True)

        self.customMapID = dwcProperties.pop("customMapID", -1)
        self.customMapPath = self.parser.parseArg(dwcProperties.pop("customMapPath", None), [])

        self.augmentSteps = [DWCAugment(augProperties) for augProperties in self.augments]

        self.mapManager = MapManager(self.parser.rootDir)

    def getMappingProperties(self) -> tuple[int, int, Path]:
        return self.mapID, self.customMapID, self.customMapPath

    def process(self, inputPath: Path, outputFolderName: str, sep: str = ",", header: int = 0, encoding: str = "utf-8", overwrite: bool = False, ignoreRemapErrors: bool = False, forceRetrieve: bool = False, zip: bool = False) -> Path:
        outputFolderPath = self.outputDir / outputFolderName
        if outputFolderPath.exists() and not overwrite:
            Logger.info(f"{outputFolderPath} already exists, exiting...")
            return
        
        # Get columns and create mappings
        Logger.info("Getting column mappings")
        columns = cmn.getColumns(inputPath, sep, header)

        maps = self.mapManager.loadMaps(self.mapID, self.customMapID, self.customMapPath, forceRetrieve)
        if not maps:
            Logger.error("Unable to retrieve any maps")
            raise Exception("No mapping")

        remapper = Remapper(maps, self.location, self.preserveDwC, self.prefixUnmapped)
        translationTable = remapper.buildTable(columns, self.skipRemap)
        
        if not translationTable.allUniqueColumns(): # If there are non unique columns
            if not ignoreRemapErrors:
                for event, firstCol, matchingCols in translationTable.getNonUnique():
                    for col in matchingCols:
                        Logger.info(f"Found mapping for column '{col}' that matches initial mapping '{firstCol}' under event '{event.value}'")
                return
            
            translationTable.forceUnique()
        
        Logger.info("Resolving events")
        writers: dict[str, BigFileWriter] = {}
        for event in translationTable.getEventCategories():
            cleanedName = event.value.lower().replace(" ", "_")
            writers[event] = BigFileWriter(outputFolderPath / f"{cleanedName}.csv", f"{cleanedName}_chunks")

        totalRows = 0
        Logger.info("Processing chunks for DwC conversion")
        for idx, df in enumerate(cmn.chunkGenerator(inputPath, self.chunkSize, sep, header, encoding), start=1):
            print(f"At chunk: {idx}", end='\r')

            df = remapper.applyTranslation(df, translationTable) # Returns a multi-index dataframe
            for na in self.setNA:
                df = df.replace(na, np.NaN)

            df = self.fillNA.apply(df)
            df = self.applyAugments(df)

            for eventColumn in df.columns.levels[0]:
                writers[eventColumn].writeDF(df[eventColumn])

            totalRows += len(df)
            del df
            gc.collect()

        for writer in writers.values():
            writer.oneFile()

        with open(outputFolderPath / self.recordsFile, "w") as fp:
            fp.write(str(totalRows))

        if zip:
            Logger.info(f"Zipping {outputFolderPath}")
            outputFolderPath = zp.compress(outputFolderPath)

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
        return event in Event._value2member_map_
    
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
