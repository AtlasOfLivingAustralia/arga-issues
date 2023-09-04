import lib.dataframeFuncs as dff
import argparse
import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove all space characters from csv")
    parser.add_argument("filePath", help="FilePath to file to clean up")
    parser.add_argument("-n", "--nooverwrite", action="store_true", help="Don't overwrite existing file")
    args = parser.parse_args()

    filePath = Path(args.filePath)
    if filePath.suffix != ".csv":
        print("Invalid file type passed")
        exit()

    df = pd.read_csv(filePath)
    df = dff.removeSpaces(df)

    if not args.nooverwrite:
        df.to_csv(filePath, index=False)
        exit()

    df.to_csv(filePath.parent / f"{filePath.name}_cleaned.csv", index=False)
