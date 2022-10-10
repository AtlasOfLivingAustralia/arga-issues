import argparse
import json
import os
import pandas as pd
import config
from helperFunctions import loadDataSources

# Generate examples from a table before conversion to DwC to help match column names to DwC fields

if __name__ == '__main__':
    sources = loadDataSources()

    parser = argparse.ArgumentParser(description="Generate examples of column values")
    parser.add_argument('source', choices=sources.keys())
    parser.add_argument('-f', '--format', choices=['json', 'csv'], default='json', help="Output file format")
    parser.add_argument('-e', '--examples', type=int, default=10, help="Maximum examples to generate.")
    args = parser.parse_args()

    source = sources[args.source]
    df = source.loadDataFrame()
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

    path = os.path.join(config.examplesFolder, f"{args.source}_examples.{args.format}")
    if args.format == 'json':
        with open(path, "w") as fp:
            json.dump(exampleDict, fp, indent=4, default=str)
        

    elif args.format == 'csv':
        df = pd.DataFrame.from_dict(exampleDict, orient='index').transpose()
        df.to_csv(path, index=False)
    
    print(f"Written to file at {path}")