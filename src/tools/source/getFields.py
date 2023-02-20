import pandas as pd
from lib.sourceManager import SourceManager
import argparse
import json
import lib.commonFuncs as cmn
import lib.config as cfg
import lib.dataframeFuncs as dff
import numpy as np

if __name__ == '__main__':
    sourceManager = SourceManager()

    parser = argparse.ArgumentParser(description="Get column names of preDwc files")
    parser.add_argument('source', choices=sourceManager.choices())
    parser.add_argument('-f', '--filenum', type=int, default=0, help="Pre-DwC file index to get columns of")
    parser.add_argument('-e', '--entries', type=int, default=10, help="Number of unique entries to get")
    args = parser.parse_args()

    source = sourceManager.getDB(args.source)
    entryLimit = args.entries

    preDwCFile = source.getPreDWCFile(args.filenum)

    dwcLookup = cmn.loadFromJson(cfg.filePaths.dwcMapping)
    customLookup = cmn.loadFromJson(cfg.filePaths.otherMapping)

    data = {}
    with pd.read_csv(preDwCFile.filePath, on_bad_lines="skip", chunksize=1024, delimiter=preDwCFile.separator, header=preDwCFile.firstRow, dtype=object) as reader:
        for chunk in reader:

            if not data: # Empty data dict, initial pass
                newColMap, _ = dff.createMappings(chunk.columns, dwcLookup, customLookup, source.location)
                for column in chunk.columns:
                    values = chunk[column].tolist()
                    values = [v for idx, v in enumerate(values, start=1) if v not in values[idx:] and v != 'nan']

                    data[column] = {"maps to": newColMap[column], "values": values[:entryLimit]}

            else: # Second pass onwards
                for column in chunk.columns:
                    if len(data[column]["values"]) >= entryLimit:
                        continue

                    values = chunk[column].tolist()
                    lst = data[column]["values"]
                    for v in values:
                        if v in lst or v == 'nan':
                            continue

                        lst.append(v)
                        if len(lst) >= entryLimit:
                            break

            if all(len(info["values"]) >= entryLimit for _, info in data.items()):
                break

    with open(preDwCFile.directoryPath / "fieldExamples.json", 'w') as fp:
        json.dump(data, fp, indent=4)