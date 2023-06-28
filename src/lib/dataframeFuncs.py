import pandas as pd
import numpy as np
import lib.commonFuncs as cmn
import pandas as pd

def getColumnEmptyCount(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return -1

    return len(df[df[column].isna()])

def getColumnCount(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return -1

    return len(df[df[column].notna()])

def cleanColName(colName: str) -> str:
    stripChars = "# _"
    for c in stripChars:
        colName = colName.strip(c)
    return colName

def createMappings(columns: list, dwcLookup: dict, prefix: str, customLookup: dict = {}, preserveDwCMatch: bool = False, keepMapFields: list = [], skipRemap: list = [], prefixMissing: bool = True) -> tuple[dict, dict]:
    newColumns = {colName: [] for colName in columns}
    preserveColumns = {}

    reverseDwCLookup = cmn.reverseLookup(dwcLookup)
    reverseCustomLookup = cmn.reverseLookup(customLookup)

    for currentCol in columns:
        if currentCol in skipRemap: # Don't remap column
            newColumns[currentCol].append(currentCol)
            continue

        if currentCol in reverseCustomLookup: # Apply custom mappings
            newColumns[currentCol].extend(reverseCustomLookup[currentCol])

        if currentCol in reverseDwCLookup: # Apply dwc map
            newColumns[currentCol].extend(reverseDwCLookup[currentCol])

        if not newColumns[currentCol]: # No mapping was done from dwc/custom lookups
            if currentCol in dwcLookup and preserveDwCMatch: # If column is already a valid dwc field and keep matches
                preserveColumns[f"{prefix}_{currentCol}"] = currentCol

            elif currentCol in dwcLookup or currentCol in customLookup: # If it exists in either set of keys and not preservingDwC
                newColumns[currentCol].append(currentCol) # Don't remap, it's a field we want

            elif prefixMissing: # Column isn't in either map and we want to add the prefix
                newColumns[currentCol].append(f"{prefix}_{cleanColName(currentCol)}") # Prefix columns that have no available mapping

            else: # Column isn't in either map and we don't want to add a prefix prefix
                newColumns[currentCol] = None

        if any(key in keepMapFields for key in newColumns[currentCol]): # If the original column of a mapped field should be kept
            preserveColumns[currentCol] = f"{prefix}_{cleanColName(currentCol)}"

    return (newColumns, preserveColumns)

def applyColumnMap(df: pd.DataFrame, newColumns: dict, copyColumns: dict, verbose: bool = False) -> pd.DataFrame:
    # Copy columns as per column map
    for oldCol, newCol in copyColumns.items():
        df[newCol] = df[oldCol]

        if verbose:
            print(f"Created duplicate of {oldCol} named {newCol}")

    # Rename columns with the new columns map
    df = df.rename(newColumns, axis=1)

    if verbose:
        for oldCol, newCol in newColumns.items():
            print(f"Mapped {oldCol} to {newCol}")

    return df

def splitField(df: pd.DataFrame, column: str, func: callable, newColumns: dict) -> pd.DataFrame:
    seriesList = zip(*df[column].apply(lambda x: func(x)))
    for colname, series in zip(newColumns, seriesList):
        if column is None:
            continue
        
        df[colname] = pd.Series(series, dtype=object).replace('', np.nan)

    df = df.drop(column, axis=1)

    return df

def dropEmptyColumns(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    remainingColumns = set(df.columns)
    df.dropna(how='all', axis=1, inplace=True) # Drop all completely empty columns

    if verbose:
        for column in remainingColumns.symmetric_difference(df.columns): # Iterate over removed column names
            print(f"Dropped empty column {column}")

    return df

def applyExclusions(df: pd.DataFrame, exclusionMap: dict) -> pd.DataFrame:
    for excludeType, properties in exclusionMap.items():
        dwcName = properties["dwc"]
        exclusions = properties["data"]
        if dwcName in df.columns: # Make sure dwc field exists in file
            df = df.drop(df[df[dwcName].isin(exclusions)].index)
    
    return df

def chunkGenerator(filePath: str, chunkSize: int, sep: str = ",", header: int = 0, encoding: str = "utf-8") -> pd.DataFrame:
    with pd.read_csv(filePath, on_bad_lines="skip", chunksize=chunkSize, sep=sep, header=header, encoding=encoding, dtype=object) as reader:
        for chunk in reader:
            yield chunk
