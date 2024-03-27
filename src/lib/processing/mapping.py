from __future__ import annotations
import numpy as np
import pandas as pd
import urllib.error
import json
import lib.config as cfg
from pathlib import Path
from enum import Enum
from lib.tools.logger import Logger
from dataclasses import dataclass

class Event(Enum):
    COLLECTION = "collections"
    ACCESSION = "accessions"
    SUBSAMPLE = "subsamples"
    EXTRACTION = "dna_extractions"
    SEQUENCE = "sequences"
    ASSEMBLIE = "assemblies"
    ANNOTATION = "annotations"
    DEPOSITION = "depositions"
    UNMAPPED = "unmapped"
    PRESERVED = "preserved"

@dataclass(frozen=True, eq=True)
class MappedColumn:
    event: Event
    colName: str

class Map:
    def __init__(self, mappings: dict = {}) -> Map:
        self._mappings = mappings

        if mappings:
            self._lookup = self._reverseLookup(mappings)
        else:
            self._lookup = {}

    @classmethod
    def fromFile(cls, filePath: Path) -> Map:
        if not filePath.exists():
            Logger.warning(f"No DWC map found at path: {filePath}")
            return cls()
        
        with open(filePath) as fp:
            rawMap = json.load(fp)

        mappings = {}
        for event, dwcMap in rawMap.items():
            if event not in Event._value2member_map_:
                Logger.warning(f"Unknown event: {event}, skipping")
                continue

            mappings[Event(event)] = dwcMap

        return cls(mappings)

    @classmethod
    def fromSheets(cls, sheetID: int) -> Map:
        documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
        retrieveURL = f"https://docs.google.com/spreadsheets/d/{documentID}/export?format=csv&gid={sheetID}"

        try:
            df = pd.read_csv(retrieveURL, keep_default_na=False)
        except urllib.error.HTTPError:
            Logger.warning(f"Unable to read sheet with id: {sheetID}")
            return cls()

        fields = "Field Name"
        eventColumns = [col for col in df.columns if col[0] == "T" and col[1].isdigit()]
        mappings = {event: {} for event in Event}

        for column, event in zip(eventColumns, mappings.keys()):
            subDF = df[[fields, column]] # Select only the dwc name and event columns
            for _, row in subDF.iterrows():
                dwcName = row[fields]
                oldName = row[column]

                # Clean the old name cell
                if not oldName:
                    continue

                if not isinstance(oldName, str): # Ignore float/int
                    continue

                if oldName in ("", "0", "1", "nan", "NaN", np.nan, np.NaN, np.NAN): # Ignore these values
                    continue

                if any(oldName.startswith(prefix) for prefix in ("ARGA", '"', "/")): # Ignore values with these prefixes
                    continue

                # Remove sections in braces
                openBrace = oldName.find("(")
                closeBrace = oldName.rfind(")", openBrace)
                if openBrace >= 0 and closeBrace >= 0:
                    oldName = oldName[:openBrace] + oldName[closeBrace+1:]

                oldName = [subname.split("::")[-1].strip(" :") for subname in oldName.split(",")] # Overwrite old name with list of subnames
                mappings[Event(event)][dwcName] = oldName

        return cls(mappings)
    
    def hasMappings(self) -> bool:
        return len(self._mappings) > 0

    def saveToFile(self, filePath: Path) -> None:
        output = {}
        for event, dwcMap in self._mappings.items():
            output[event.value] = dwcMap

        with open(filePath, "w") as fp:
            json.dump(output, fp, indent=4)

    def getValues(self, fieldName: str) -> list[MappedColumn]:
        return self._lookup.get(fieldName, [])
    
    def existsInMap(self, fieldName: str) -> bool:
        return fieldName in self._mappings

    @staticmethod
    def _reverseLookup(map: dict[Event, dict[str, list[str]]]) -> dict[str, list[MappedColumn]]:
        lookup: dict[str, list[MappedColumn]] = {}

        for event, columnMap in map.items():
            for newName, oldNameList in columnMap.items():
                for oldName in oldNameList:
                    if oldName not in lookup:
                        lookup[oldName] = []
  
                    lookup[oldName].append(MappedColumn(event, newName))

        return lookup
    
class TranslationTable:
    def __init__(self):
        self._translationTable: dict[str, list[MappedColumn]] = {}
        self._uniqueEntries: dict[MappedColumn, list[str]] = {}

        self._eventsUsed = set()

    def clear(self) -> None:
        self._translationTable.clear()
        self._uniqueEntries.clear()

    def addTranslation(self, column: str, columnMapping: MappedColumn) -> None:
        if column not in self._translationTable:
            self._translationTable[column] = []

        self._translationTable[column].append(columnMapping)
        self._eventsUsed.add(columnMapping.event)

        if columnMapping not in self._uniqueEntries:
            self._uniqueEntries[columnMapping] = [column]
            return
        
        self._uniqueEntries[columnMapping].append(column)

    def getTranslation(self, column: str) -> list[MappedColumn]:
        return self._translationTable.get(column, [])

    def getEventCategories(self) -> list[Event]:
        return list(self._eventsUsed)

    def hasColumn(self, column: str) -> bool:
        return column in self._translationTable
    
    def allUniqueColumns(self) -> bool:
        return all(len(originalColumns) == 1 for originalColumns in self._uniqueEntries.values())
    
    def getNonUnique(self) -> list[tuple[Event, str, list[str]]]:
        return [(mapping.event, oldCols[0], oldCols[1:]) for mapping, oldCols in self._uniqueEntries.items()]
    
    def forceUnique(self) -> None:
        for mapping, oldColumns in self._uniqueEntries.items():
            for column in oldColumns[1:]:
                self._translationTable[column].remove(mapping)

class Remapper:
    def __init__(self, maps: list[Map], prefix: str, preserveDwC: bool = False, prefixUnmapped: bool = True):
        self.maps = maps
        self.prefix = prefix
        self.preserveDwc = preserveDwC
        self.prefixUnmapped = prefixUnmapped

    def buildTable(self, columns: list[str], skipRemap: list[str] = []) -> TranslationTable:
        table = TranslationTable()

        for column in columns:
            if column in skipRemap:
                mapping = MappedColumn(Event.UNMAPPED, f"{self.location}_{column}" if self.prefixUnmapped else column)
                table.addTranslation(mapping)
                continue
 
            # Apply mapping
            for map in self.maps:
                for value in map.getValues(column):
                    if not value:
                        continue

                    table.addTranslation(column, value)

                # If column matches an output column name
                if map.existsInMap(column) and self.preserveDwCMatch:
                    value = MappedColumn(Event.PRESERVED, f"{self.prefixs}_{column}")
                    table.addTranslation(column, value)

            # If no mapped value has been found yet
            if not table.hasColumn(column):
                value = MappedColumn(Event.UNMAPPED, f"{self.prefix}_{column}" if self.prefixUnmapped else column)
                table.addTranslation(column, value)

        return table

    def applyTranslation(self, df: pd.DataFrame, translationTable: TranslationTable) -> pd.DataFrame:
        eventColumns = {}

        for column in df.columns:
            for mappedColumn in translationTable.getTranslation(column):
                if mappedColumn.event not in eventColumns:
                    eventColumns[mappedColumn.event] = {}

                eventColumns[mappedColumn.event][column] = mappedColumn.colName

        for eventName, colMap in eventColumns.items():
            subDF: pd.DataFrame = df[colMap.keys()].copy() # Select only relevant columns
            eventColumns[eventName] = subDF.rename(colMap, axis=1)

        return pd.concat(eventColumns.values(), keys=eventColumns.keys(), axis=1)

class MapManager:
    def __init__(self, baseDir: Path):
        self.baseDir = baseDir
        self.localMapPath = baseDir / "map.json"

    def loadMaps(self, mapID: int = None, customMapID: int = None, customMapPath: Path = None, forceRetrieve: bool = False) -> list[Map]:
        maps = []

        dwcMap = Map.fromFile(self.localMapPath)
        if not dwcMap.hasMappings() or forceRetrieve:
            dwcMap = Map.fromSheets(mapID)

            if dwcMap.hasMappings():
                Logger.info("Added sheets map")
                maps.append(dwcMap)
                dwcMap.saveToFile(self.localMapPath)
        else:
            Logger.info("Added local map")
            maps.append(dwcMap)
        
        if customMapPath is not None:
            customMap = Map.fromFile(customMapPath)

            if not customMap.hasMappings():
                if customMapID is not None:
                    customMap = Map.fromSheets(customMapID)

                    if customMap.hasMappings():
                        Logger.info("Added sheets custom map")
                        maps.append(customMap)

                        if customMapPath is not None:
                            customMap.saveToFile(customMapPath)
            else:
                Logger.info("Added local custom map")
                maps.append(customMap)

        return maps
