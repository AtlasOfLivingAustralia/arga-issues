import pandas as pd
import json
import os

def getValueExamples(dataframe, examples = 10, outpath = '.', outfilename = "examples.json"):
    """
    Paramters:
        dataframe: pandas dataframe to extract examples from
        examples: number of unique examples from each column
        outpath: path to output folder
        outfilename: name for the output file, should end with .json

    Creates up (examples) unique examples of each column to determine what the column name should be mapped to.
    """
    exampleDict = {}

    for col in dataframe.columns:
        values = []
        pos = 0
        while len(values) < examples:
            if pos >= len(dataframe):
                break

            if pd.notna(dataframe[col][pos]) and dataframe[col][pos] not in values:
                values.append(dataframe[col][pos])
            pos +=1
            
        exampleDict[col] = values

    with open(os.path.join(outpath, outfilename), "w") as fp:
        json.dump(exampleDict, fp, indent=4, default=str)

def convertToDwC(inpath, outprefix, outpath = '.'):
    """
    Parmaters:
        inpath: full path to file to convert to dwc
        outprefix: prefix for both the generated file, and column names that could not be found in dwc mapping
        outpath: location to put the output file

    Converts a csv/tsv file to a darwin core csv.
    Prints the conversions that were made to column names.
    """
    mappingFile = "../mapping.json"
    with open(mappingFile) as fp:
        lookup = json.load(fp)

    reverseLookup = {oldName: dwcName for dwcName, oldNameList in lookup.items() for oldName in oldNameList}

    df = pd.read_table(inpath, dtype='object', parse_dates=['last_updated'])

    newNames = {}
    for col in df.columns:
        if col in reverseLookup:
            newNames[col] = reverseLookup[col]
        elif col in lookup:
            print(f"Skipping conversion of {col}")
            continue
        else:
            newNames[col] = f"{outprefix}_{col}"

        print(f"Converted {col} to {newNames[col]}")

    df = df.rename(newNames, axis=1)
    df.to_csv(os.path.join(outpath, f"{outprefix}_dwc.csv"))

def latlongToDecimal(latlong):
    """
    Paramters:
        latlong: string representation of both latitude and longitude

    Converts the latitude and longitude string to separate lat and long strings, returned as a tuple.
    """
    split = latlong.split(' ')
    if len(split) == 4:
        latlong = f"{split[1]}{split[0]} {split[3]}{split[2]}"
    elif len(split) == 2:
        latlong = f"{split[0][-1]}{split[0][:-1]} {split[1][-1]}{split[1][:-1]}"
    else:
        return ('', '')

    latlong = latlong.replace('S', '-').replace('W', '-').replace('N', '').replace('E', '')
    return latlong.split(' ')
