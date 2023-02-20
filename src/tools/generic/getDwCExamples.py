import json
import argparse
import lib.config
import os
import pandas as pd
from lib.commonFuncs import loadSourceFile

# Generate examples from all DwC files for each field found

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate examples of column values")
    parser.add_argument('-e', '--examples', type=int, default=10, help="Maximum examples to generate per file.")
    args = parser.parse_args()

    exampleDict = {}

    for file in os.listdir(config.dwcFolder):
        df = loadSourceFile(os.path.join(config.dwcFolder, file))
        print(f"Getting examples from file: {file}")
        for ref, colname in enumerate(df.columns):
            print(f"Completed: {100*ref/len(df.columns):3.02f}%", end='\r')
            values = []
            pos = 0
            col = df[colname]

            while len(values) < args.examples:
                if pos >= len(df):
                    break

                if pd.notna(col[pos]) and col[pos] not in values:
                    values.append(col[pos])
                pos +=1
                
            if colname not in exampleDict:
                exampleDict[colname] = []

            exampleDict[colname].extend(values)
        print("Completed: 100.00%")

    path = os.path.join(config.examplesFolder, "processed_DwC_examples.json")
    with open(path, "w") as fp:
        json.dump(exampleDict, fp, indent=4, default=str)
    print(f"Written to file at {path}")