import logging
import pandas as pd
import numpy as np
from lib.commonFuncs import reverseLookup

def getColumnEmptyCount(df, column):
    if column not in df.columns:
        # return len(df.index)
        return -1

    return len(df[df[column].isna()])

def getColumnCount(df, column):
    if column not in df.columns:
        return -1

    return len(df[df[column].notna()])

def cleanColName(colName):
    stripChars = "# _"
    for c in stripChars:
        colName = colName.strip(c)
    return colName

def createMappings(columns, dwcLookup, customLookup, prefix, preserveDwCMatch=False, keepMapFields=[], skipRemap=[], prefixMissing=True):
    newColumns = {}
    copyColumns = {}

    reverseDwCLookup = reverseLookup(dwcLookup)
    reverseCustomLookup = reverseLookup(customLookup)

    for currentCol in columns:
        if currentCol in skipRemap:
            newColumns[currentCol] = currentCol
            continue

        prefixedCleanColName = f"{prefix}_{cleanColName(currentCol)}"

        if currentCol in reverseCustomLookup: # Apply custom mappings
            newColumns[currentCol] = reverseCustomLookup[currentCol]

        if currentCol in reverseDwCLookup: # Apply dwc map
            newColumns[currentCol] = reverseDwCLookup[currentCol]

        if currentCol not in newColumns: # Not mapped yet from previous maps
            if currentCol in dwcLookup and preserveDwCMatch: # If column is already a valid dwc field and keep matches
                copyColumns[f"{prefix}_{currentCol}"] = currentCol

            elif currentCol in dwcLookup or currentCol in customLookup: # If it exists in either set of keys
                newColumns[currentCol] = currentCol # Don't remap, it's a field we want

            elif prefixMissing: # Column isn't in either map and we want to prefix
                newColumns[currentCol] = prefixedCleanColName # Prefix columns that have no available mapping

            else: # Column isn't in either map and we don't want to prefix
                newColumns[currentCol] = None

        if currentCol in newColumns and newColumns[currentCol] in keepMapFields: # If the original column of a mapped field should be kept
            copyColumns[currentCol] = prefixedCleanColName

    return newColumns, copyColumns

def applyColumnMap(df, newColumns, copyColumns, verbose=False):
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

def mapAndApply(df, dwcLookup, customLookup, prefix, preserveDwCMatch=False, keepMapFields=[]):
    newColumns, copyColumns = createMappings(df.columns, dwcLookup, customLookup, prefix, preserveDwCMatch, keepMapFields)
    return applyColumnMap(df, newColumns, copyColumns)

def splitField(df, column, func, newColumns):
    seriesList = zip(*df[column].apply(lambda x: func(x)))
    for colname, series in zip(newColumns, seriesList):
        if column is None:
            continue
        
        df[colname] = pd.Series(series, dtype=object).replace('', np.nan)

    df = df.drop(column, axis=1)

    return df

def dropEmptyColumns(df, verbose=False):
    remainingColumns = set(df.columns)
    df.dropna(how='all', axis=1, inplace=True) # Drop all completely empty columns

    if verbose:
        for column in remainingColumns.symmetric_difference(df.columns): # Iterate over removed column names
            print(f"Dropped empty column {column}")

    return df

def applyExclusions(df, exclusionMap):
    for excludeType, properties in exclusionMap.items():
        dwcName = properties["dwc"]
        exclusions = properties["data"]
        if dwcName in df.columns: # Make sure dwc field exists in file
            df = df.drop(df[df[dwcName].isin(exclusions)].index)
    
    return df

def chunkGenerator(filePath, chunkSize, sep=",", header=0, encoding="utf-8"):
    with pd.read_csv(filePath, on_bad_lines="skip", chunksize=chunkSize, sep=sep, header=header, encoding=encoding, dtype=object) as reader:
        for chunk in reader:
            yield chunk
