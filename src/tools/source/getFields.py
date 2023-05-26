import pandas as pd
import json
import lib.commonFuncs as cmn
import lib.config as cfg
import lib.dataframeFuncs as dff
import numpy as np
from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFile
from pathlib import Path

def collectFields(stageFile: StageFile, prefix: str, outputFile: Path, entryLimit: int, overwrite: bool = False):
    dwcLookup = cmn.loadFromJson(cfg.filePaths.dwcMapping)
    customLookup = cmn.loadFromJson(cfg.filePaths.otherMapping)

    data = {}
    with pd.read_csv(stageFile.filePath, encoding=stageFile.encoding, on_bad_lines="skip", chunksize=1024, delimiter=stageFile.separator, header=stageFile.firstRow, dtype=object) as reader:
        for idx, chunk in enumerate(reader):
            print(f"Scanning chunk: {idx}", end='\r')
            if not data: # Empty data dict, initial pass
                newColMap, _ = dff.createMappings(chunk.columns, dwcLookup, prefix, customLookup, prefixMissing=False)
                for column in chunk.columns:
                    values = chunk[column].tolist()
                    values = [v for index, v in enumerate(values) if v not in values[:index] and v not in [np.NaN, np.nan]]

                    data[column] = {"maps to": newColMap[column], "values": values[:entryLimit]}

            else: # Second pass onwards
                for column in chunk.columns:
                    if len(data[column]["values"]) >= entryLimit:
                        continue

                    values = chunk[column].tolist()
                    lst = data[column]["values"]
                    for v in values:
                        if v in lst or v in [np.NaN, np.nan]:
                            continue

                        lst.append(v)
                        if len(lst) >= entryLimit:
                            break

            if all(len(info["values"]) >= entryLimit for info in data.values()):
                break
    
    print() # Add line break after counter
    return data

if __name__ == '__main__':
    parser = SourceArgParser(description="Get column names of preDwc files")
    parser.add_argument('-e', '--entries', type=int, default=10, help="Number of unique entries to get")
    parser.add_argument('-t', '--tsv', action="store_true", help="Output as tsv instead")
    
    sources, args = parser.parse_args()
    entryLimit = args.entries

    for source in sources:
        outputDir = source.getBaseDir()
        extension = "tsv" if args.tsv else "json"
        output = outputDir / f"fieldExamples.{extension}"

        if output.exists() and not args.overwrite:
            print(f"Output file {output} already exists, please run with overwrite flag (-o) to overwrite")
            continue

        stageFiles = source.getPreDWCFiles(args.filenums)
        for stageFile in stageFiles:
            if not stageFile.filePath.exists():
                print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
                continue

            data = collectFields(stageFile, source.getBaseDir(), output, args.entries, args.overwrite > 0)
                
            print(f"Writing to file {output}")
            if args.tsv:
                dfData = {k: v["values"] + ["" for _ in range(entryLimit - len(v["values"]))] for k, v in data.items()}
                df = pd.DataFrame.from_dict(dfData)
                df.index += 1 # Increment index so output is 1-indexed numbers
                df.to_csv(output, sep="\t", index_label="Example #")
            else:
                with open(output, 'w') as fp:
                    json.dump(data, fp, indent=4)
