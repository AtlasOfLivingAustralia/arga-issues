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
    def __init__(self, filePath: Path) -> (Map | None):
        if not filePath.exists():
            Logger.warning(f"No DWC map found at path: {filePath}")
            return None
        
        self._mappings = self.loadFromFile(filePath)
        self._lookup = self._reverseLookup(self._mappings)
        
    @classmethod
    def fromSheets(cls, sheetID: int) -> (Map | None):
        def init(self, mappings, lookup):
            self._mappings = mappings
            self._lookup = lookup

        documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
        retrieveURL = f"https://docs.google.com/spreadsheets/d/{documentID}/export?format=csv&gid={sheetID}"

        try:
            df = pd.read_csv(retrieveURL, keep_default_na=False)
        except urllib.error.HTTPError:
            Logger.warning(f"Unable to read sheet with id: {sheetID}")
            return None

        fields = "Field Name"
        eventColumns = [col for col in df.columns if col[0] == "T" and col[1].isdigit()]
        _mappings: dict[Event, dict[str: list[str]]] = {event: {} for event in Event}

        for column, event in zip(eventColumns, _mappings.keys()):
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

                oldName = [subname.strip() for subname in oldName.split(",")] # Overwrite old name with list of subnames
                _mappings[event][dwcName] = oldName

        cls.__init__ = init
        return cls(_mappings, cls._reverseLookup(cls, _mappings))
    
    def loadFromFile(self, filePath: Path) -> dict[Event, dict[str, list[str]]]:
        with open(filePath) as fp:
            rawMap = json.load(fp)

        map = {}
        for event, dwcMap in rawMap.items():
            if event not in Event._value2member_map_:
                Logger.warning(f"Unknown event: {event}, skipping")
                continue

            map[Event(event)] = dwcMap

        return map

    def saveToFile(self, filePath: Path) -> None:
        output = {}
        for event, dwcMap in self._mappings.items():
            output[event.value] = dwcMap

        with open(filePath, "w") as fp:
            json.dump(output, fp, indent=4)

    def getValue(self, fieldName: str) -> list[MappedColumn]:
        return self._lookup.get(fieldName, [])
    
    def existsInMap(self, fieldName: str) -> bool:
        return fieldName in self._mappings

    def _reverseLookup(self, map: dict[Event, dict[str, list[str]]]) -> dict[str, list[MappedColumn]]:
        lookup: dict[str, list[MappedColumn]] = {}

        for event, columnMap in map.items():
            for newName, oldNameList in columnMap.items():
                for oldName in oldNameList:
                    if oldName not in lookup:
                        lookup[oldName] = []
  
                    lookup[oldName].append(MappedColumn(event.value, newName))

        return lookup
    
class TranslationTable:
    def __init__(self):
        self._translationTable: dict[str, list[MappedColumn]] = {}
        self._uniqueEntries: dict[MappedColumn, list[str]] = {}

        self._eventsUsed = set()

    def clear(self) -> None:
        self._translationTable.clear()
        self._uniqueEntries.clear()

    def addColumn(self, column: str) -> None:
        self._translationTable[column] = []

    def addTranslation(self, column, columnMapping: MappedColumn) -> None:
        self._translationTable[column].append(columnMapping)
        self._eventsUsed.add(columnMapping.event)

        if columnMapping not in self._uniqueEntries:
            self._uniqueEntries[columnMapping] = [column]
            return
        
        self._uniqueEntries[columnMapping].append(column)

    def addMultipleTranslations(self, column, columnMappings: list[MappedColumn]) -> None:
        for columnMapping in columnMappings:
            self.addTranslation(column, columnMapping)

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
            table.addColumn(column)

            if column in skipRemap:
                mapping = MappedColumn(Event.UNMAPPED, f"{self.location}_{column}" if self.prefixUnmapped else column)
                table.addTranslation(mapping)
                continue
 
            # Apply mapping
            for map in self.maps:
                values = map.getValue(column)
                table.addMultipleTranslations(column, values)

                # If column matches an output column name
                if map.existsInMap(column) and self.preserveDwCMatch:
                    value = MappedColumn(Event.PRESERVED, f"{self.location}_{column}")
                    table.addTranslation(value)

        # If no mapped value has been found yet
        if not table.hasColumn(column):
            value = MappedColumn(Event.UNMAPPED, f"{self.location}_{column}" if self.prefixUnmapped else column)
            table.addTranslation(value)

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
            subDF.rename(colMap, axis=1, inplace=True)
            eventColumns[eventName] = subDF

        return pd.concat(eventColumns.values(), keys=eventColumns.keys(), axis=1)

class MapManager:
    def __init__(self, baseDir: Path):
        self.baseDir = baseDir
        self.localMapPath = baseDir / "map.json"

    def loadMaps(self, mapID: int = None, customMapID: int = None, customMapPath: Path = None, forceRetrieve: bool = False) -> list[Map]:
        maps = []

        dwcMap = Map(self.localMapPath)
        if dwcMap is None or forceRetrieve:
            dwcMap = Map.fromSheets(mapID)

            if dwcMap is not None:
                maps.append(dwcMap)
                dwcMap.saveToFile(self.localMapPath)
        
        if customMapPath is not None:
            customMap = Map(customMapPath)

            if customMap is None and customMapID is not None:
                customMap = Map.fromSheets(customMapID)

                if customMap is not None:
                    maps.append(customMap)

                    if customMapPath is not None:
                        customMap.saveToFile(customMapPath)

        return maps
