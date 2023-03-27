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
    parser.add_argument('-t', '--tsv', action="store_true", help="Output as tsv instead")
    args = parser.parse_args()

    source = sourceManager.getDB(args.source, False)
    entryLimit = args.entries

    preDwCFiles = source.getPreDWCFiles()
    if args.filenum < 0 or args.filenum >= len(preDwCFiles):
        print(f"Invalid preDWC file selected. Valid value is between 0 and {len(preDwCFiles) - 1} inclusive")
        exit()

    stageFile = preDwCFiles[args.filenum]
    outputDir = source.getBaseDir()

    dwcLookup = cmn.loadFromJson(cfg.filePaths.dwcMapping)
    customLookup = cmn.loadFromJson(cfg.filePaths.otherMapping)

    if not stageFile.filePath.exists():
        print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
        exit()

    data = {}
    with pd.read_csv(stageFile.filePath, encoding=stageFile.encoding, on_bad_lines="skip", chunksize=1024, delimiter=stageFile.separator, header=stageFile.firstRow, dtype=object) as reader:
        for idx, chunk in enumerate(reader):
            print(f"Scanning chunk: {idx}", end='\r')
            if not data: # Empty data dict, initial pass
                newColMap, _ = dff.createMappings(chunk.columns, dwcLookup, customLookup, source.location, prefixMissing=False)
                for column in chunk.columns:
                    values = chunk[column].tolist()
                    values = [v for index, v in enumerate(values, start=1) if v not in values[:index] and v != np.NaN]

                    data[column] = {"maps to": newColMap[column], "values": values[:entryLimit]}

            else: # Second pass onwards
                for column in chunk.columns:
                    if len(data[column]["values"]) >= entryLimit:
                        continue

                    values = chunk[column].tolist()
                    lst = data[column]["values"]
                    for v in values:
                        if v in lst or v == np.NaN:
                            continue

                        lst.append(v)
                        if len(lst) >= entryLimit:
                            break

            if all(len(info["values"]) >= entryLimit for info in data.values()):
                break
    
    if args.tsv:
        dfData = {k: v["values"] + ["" for _ in range(entryLimit - len(v["values"]))] for k, v in data.items()}
        df = pd.DataFrame.from_dict(dfData)
        df.index += 1
        df.to_csv(outputDir / "fieldExamples.tsv", sep="\t", index_label="Example #")
    else:
        with open(outputDir / "fieldExamples.json", 'w') as fp:
            json.dump(data, fp, indent=4)
