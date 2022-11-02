import pandas as pd
import os
import argparse
import config

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Combine multiple CSVs into one large CSV")
    parser.add_argument('-i', '--inpath', default=config.resultsFolder, help="Folder containing all input csvs.")
    parser.add_argument('-o', '--output', default=os.path.join(config.dataFolder, "combined.csv"))
    args = parser.parse_args()

    out = None
    dirList = os.listdir(args.inpath)
    for ref, file in enumerate(dirList):
        print(f"Completed %: {100*ref/len(dirList):3.02f}", end='\r')
        fullPath = os.path.join(args.inpath, file)
        df = pd.read_csv(fullPath)

        if out is None:
            out = df
        else:
            out = pd.concat([out, df], ignore_index=True)

    print("Completed %: 100.00")
    out.to_csv(args.output, index=False)
    print(f"Written to file at {args.output}")
