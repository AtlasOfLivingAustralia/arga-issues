import argparse
import json
import pandas as pd
from lib.config import folderPaths
from lib.sourceManager import SourceManager

# Generate examples from a table before conversion to DwC to help match column names to DwC fields

if __name__ == '__main__':
    sources = SourceManager()

    parser = argparse.ArgumentParser(description="Generate examples of column values")
    parser.add_argument('source', choices=sources.choices())
    parser.add_argument('-f', '--format', choices=['json', 'csv'], default='json', help="Output file format")
    parser.add_argument('-e', '--examples', type=int, default=10, help="Maximum examples to generate.")
    args = parser.parse_args()

    source = sources.getSource(args.source)
    df = source.loadDataFrame()
    exampleDict = {}

    print(df.head())

    for ref, colname in enumerate(df.columns, start=1):
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

        print(f"Completed %: {100*ref/len(df.columns):3.02f}", end='\r')

    path = folderPaths.genFiles / f"{args.source}_examples.{args.format}"
    if args.format == 'json':
        with open(path, "w") as fp:
            json.dump(exampleDict, fp, indent=4, default=str)

    elif args.format == 'csv':
        df = pd.DataFrame.from_dict(exampleDict, orient='index').transpose()
        df.to_csv(path, index=False)
    
    print(f"\nWritten to file at {path}")