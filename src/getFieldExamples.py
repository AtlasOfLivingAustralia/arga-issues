import argparse
import json
import os
import pandas as pd
import config
from helperFunctions import loadDataSources, loadSourceFile

if __name__ == '__main__':
    sources = loadDataSources()

    parser = argparse.ArgumentParser(description="Generate examples of column values")
    parser.add_argument('source', choices=sources.keys())
    parser.add_argument('-i', '--inpath', default=config.dataFolder, help="Location of downloaded file to convert.")
    parser.add_argument('-e', '--examples', type=int, default=10, help="Maximum examples to generate.")
    parser.add_argument('-d', '--dir', default=config.examplesFolder, help="Location to put output examples.")
    args = parser.parse_args()

    source = sources[args.source]

    loadPath = source.getLoadPath(args.inpath)
    df = loadSourceFile(loadPath, kwargs=source.parseKwargs)
    exampleDict = {}

    for ref, colname in enumerate(df.columns):
        print(f"Completed %: {100*ref/len(df.columns):3.02f}", end='\r')

        values = []
        pos = 0
        col = df[colname]
        while len(values) < args.examples:
            if pos >= len(df):
                break

            if pd.notna(col[pos]) and col[pos] not in values:
                values.append(col[pos])
            pos +=1
            
        exampleDict[colname] = values

    print("Completed %: 100.00")
    path = os.path.join(args.dir, f"{args.source}_examples.json")
    with open(path, "w") as fp:
        json.dump(exampleDict, fp, indent=4, default=str)
    print(f"Written to file at {path}")
