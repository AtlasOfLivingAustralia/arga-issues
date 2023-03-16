from pathlib import Path
import lib.dataframeFuncs as dff
import ast
import numpy as np
import pandas as pd
from lib.subfileWriter import Writer

def extractAttr(listOfDicts, attrTuples):
    if listOfDicts is np.NaN:
        return [np.NaN] * len(attrTuples)

    listOfDicts = ast.literal_eval(listOfDicts)

    res = []
    for prop, value in attrTuples:
        for dct in listOfDicts:
            if dct.get(prop, None) == value:
                dct.pop(prop)
                res.append(dct.get("Attribute_text", ""))
                break
        else:
            res.append("")

    return res

def process(filePath: Path, outputFilePath: Path, splitInfo: dict, chunkSize: int = 1024*1024):
    writer = Writer(outputFilePath.parent, "columnSplit", "split")

    for df in dff.chunkGenerator(filePath, chunkSize):
        for column, conversions in splitInfo.items():
            df[list(conversions.keys())] = df[column].apply(lambda x: pd.Series(extractAttr(x, conversions.values())))
        
        writer.writeDF(df)

    writer.oneFile(outputFilePath)
