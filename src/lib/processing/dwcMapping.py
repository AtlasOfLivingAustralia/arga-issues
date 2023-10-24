import numpy as np
import pandas as pd
import urllib.error
import json
import lib.config as cfg
import lib.commonFuncs as cmn
from pathlib import Path

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
        mapPath = cfg.folders.mapping / f"{self.location}.json"
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
            mapValues.append(f"preserved_{self.location}_{column}")

        if not mapValues: # If no mapped value has been found yet
            mapValues.append(f"unmapped_{self.location}_{column}" if self.prefixMissing else column)
        
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

class MapRetriever:
    def __init__(self):
        self.documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
        self.retrieveURL = f"https://docs.google.com/spreadsheets/d/{self.documentID}/export?format=csv&gid="
        self.sheetIDs = {
            "42bp-genomeArk": 84855374,
            "ala-avh": 404635334,
            "anemone-db": 286004534,
            "bold-datapackage": 1154592624,
            "bold-tsv": 78385490,
            "bold-xml": 984983691,
            "bpa-portal": 1982878906,
            "bvbrc-db": 685936034,
            "csiro-dap": 16336602,
            "csiro-genomes": 215504073,
            "dnazoo-db": 570069681,
            "ena-genomes": 1058330275,
            "ncbi-biosample": 109194600,
            "ncbi-nucleotide": 1759032197,
            "ncbi-refseq": 2003682060,
            "ncbi-taxonomy": 240630744,
            "ncbi-genbank": 1632006425,
            "tern-portal": 1651969444,
            "tsi-koala": 975794491
        }

    def run(self) -> None:
        written = []
        for database, sheetID in self.sheetIDs.items():
            location, _ = database.split("-")
            print(f"Reading {database}")

            try:
                df = pd.read_csv(self.retrieveURL + sheetID, keep_default_na=False)
            except urllib.error.HTTPError:
                print(f"Unable to read sheet for {database}")
                continue

            mappings = self.getMappings(df)
            mapFile = cfg.folders.mapping / f"{location}.json"

            if mapFile.exists() and location not in written: # Old map file
                mapFile.unlink()

            if not mapFile.exists(): # First data source from location
                with open(mapFile, "w") as fp:
                    json.dump(mappings, fp, indent=4)
                written.append(location)
                print(f"Created new {location} map")
                continue

            with open(mapFile) as fp:
                columnMap = json.load(fp)

            for keyword, names in mappings.items():
                if keyword not in columnMap:
                    columnMap[keyword] = names
                else:
                    columnMap[keyword].extend(name for name in names if name not in columnMap[keyword])

            with open(mapFile, "w") as fp:
                json.dump(columnMap, fp, indent=4)

            print(f"Added new values to {location} map")
            if location not in written:
                written.append(location)

    def getMappings(self, df: pd.DataFrame) -> dict:
        fields = "Field Name"
        eventColumns = [col for col in df.columns if col[0] == "T" and col[1].isdigit()]

        mappings = {}
        for column in eventColumns:
            subDF = df[[fields, column]]
            for _, row in subDF.iterrows():
                filteredValue = self.filterEntry(row[column])
                if filteredValue:
                    mappings[f"{column[1]}:{row[fields]}"] = filteredValue

        return mappings
    
    def filterEntry(self, value: any) -> list:
        if not isinstance(value, str): # Ignore float/int
            return []
        
        if value in ("", "0", "1", "nan", "NaN", np.nan, np.NaN, np.NAN): # Ignore these values
            return []
        
        if any(value.startswith(val) for val in ("ARGA", '"', "/")): # Ignore values with these prefixes
            return []

        return [elem.strip() for elem in value.split(",")]
