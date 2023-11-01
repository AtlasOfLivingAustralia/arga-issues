from pathlib import Path
import pandas as pd
from lib.tools.bigFileWriter import BigFileWriter, Format

def compile(inputFolder: Path, outputFilePath: Path) -> None:
    writer = BigFileWriter(outputFilePath, subfileType=Format.CSV)

    dfs = []
    for idx, filePath in enumerate(inputFolder.iterdir(), start=1):
        print(f"Reading file #: {idx}", end="\r")

        try:
            df = pd.read_csv(filePath, sep="\t")
        except pd.errors.EmptyDataError:
            continue

        dfs.append(df)

        if len(dfs) >= 10:
            df = pd.concat(dfs)
            writer.writeDF(df)
            dfs.clear()
            return

    print()
    if dfs:
        df = pd.concat(dfs, axis=1)
        writer.writeDF(df)

    writer.oneFile()
