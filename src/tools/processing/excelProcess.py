import argparse
import pandas as pd
from pathlib import Path
from lib.sourceManager import SourceManager

def process(filePath):
    if not isinstance(filePath, Path):
        filePath = Path(filePath)

    outputDir = filePath.parent

    xls = pd.ExcelFile(filePath)
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet)
        df.to_csv(outputDir / f"{sheet}.csv", index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("filepath")
    args = parser.parse_args()

    process(args.filepath)
