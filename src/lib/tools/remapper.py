import lib.config as cfg
import lib.commonFuncs as cmn
from pathlib import Path
import pandas as pd

class Remapper:
    def __init__(self, location: str, customMapPath: Path = None, preserveDwCMatch: bool = False, prefixMissing: bool = True) -> 'Remapper':
        self.location = location
        self.customMapPath = customMapPath
        self.preserveDwCMatch = preserveDwCMatch
        self.prefixMissing = prefixMissing

        self.mappedColumns = {}
        
        self.loadMaps(customMapPath)

    def getMappings(self) -> dict:
        return self.mappedColumns

    def loadMaps(self, customMapPath: Path = None) -> None:
        # DWC map
        mapPath = cfg.folderPaths.mapping / f"{self.location}.json"
        if mapPath.exists():
            self.map = cmn.loadFromJson(mapPath)
        else:
            print(f"WARNING: No DWC map found for location {self.location}")
            self.map = {}
        
        self.reverseLookup = self.buildReverseLookup(self.map)

        # Exit early if no custom path specified
        if customMapPath is None:
            self.customReverseLookup = {}
            return

        # Custom map
        if customMapPath.exists():
            self.customMap = cmn.loadFromJson(customMapPath)
        else:
            raise Exception(f"No custom map found for location: {customMapPath}") from FileNotFoundError

        self.customReverseLookup = self.buildReverseLookup(self.customLookup)

    def buildReverseLookup(self, lookup: dict) -> dict:
        reverse = {}

        for newName, oldNameList in lookup.items():
            for name in oldNameList:
                if name not in reverse:
                    reverse[name] = [newName]
                else:
                    reverse[name].append(newName)

        return reverse

    def createMappings(self, columns: list, skipRemap: list = []) -> dict:
        # self.mappedColumns = {column: (self.mapColumn[column] if column not in skipRemap else []) for column in columns}
        self.mappedColumns = {} # Clear mapped columns

        for column in columns:
            if column in skipRemap:
                self.mappedColumns[column] = []
                continue

            self.mappedColumns[column] = self.mapColumn(column)

        return self.mappedColumns

    def mapColumn(self, column: str) -> list:
        mapValues = [] 
        mapValues.extend(self.reverseLookup.get(column, [])) # Apply DWC mapping
        mapValues.extend(self.customReverseLookup.get(column, [])) # Apply custom mapping

        if column in self.map and self.preserveDwCMatch: # If column matches mapped value and preserve
            mapValues.append(f"{self.location}_preserved_{column}")

        if not mapValues: # If no mapped value has been found yet
            mapValues.append(f"{self.location}_unmapped_{column}" if self.prefixMissing else column)
        
        return mapValues

    def applyMap(self, df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
        for column, newColumnNames in self.mappedColumns.items():
            if not newColumnNames:
                continue

            for colName in newColumnNames[1:]: # Create copies of all column names beyond first
                df[colName] = df[column]

                if verbose:
                    print(f"Copied column '{column}' to '{colName}'")

            df.rename({column: newColumnNames[0]}, axis=1, inplace=True) # Rename column to first new name in list

            if verbose:
                print(f"Renamed column '{column}' to '{newColumnNames[0]}'")

        return df
