import pandas as pd
from argparse import ArgumentParser
from pathlib import Path

if __name__ == "__main__":
    parser = ArgumentParser(description="Convert parquet file to csv")
    parser.add_argument("filepath", type=Path, help="Filepath of file to convert")
    args = parser.parse_args()

    if not args.filepath.exists():
        print(f"No file found at path: {args.filepath}")
        exit()

    pd.read_parquet(args.filepath).to_csv(args.filepath.parent / f"{args.filepath.stem}.csv", index=False)