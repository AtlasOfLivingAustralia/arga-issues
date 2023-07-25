from pathlib import Path
import pandas as pd
from lib.tools.subfileWriter import Writer
import concurrent.futures

def parseStats(filePath: Path) -> pd.DataFrame | None:
    with open(filePath, encoding="utf-8") as fp:
        data = fp.read()

    splitData = data.rsplit("#", 1)
    if len(splitData) == 1: # No # found, error reading file
        return None
    
    info, table = splitData

    # Info parsing
    infoData = {}
    splitAssemblyInfo = info.split("##", 1)
    assemblyStats = splitAssemblyInfo[0]

    for stat in assemblyStats.split("\n"):
        if ":" not in stat: # Not a key-value pair
            continue

        key, value = stat.split(":", 1)
        infoData[key.strip("# ")] = value.strip("\t").strip(" ")

    # Table parsing
    prevInfo = [None, None, None, None]
    tableData = []

    for row in table.split("\n")[1:]: # Skip header row
        rowValues = row.split("\t")
        if len(rowValues) != 6: # Expect 6 columns
            continue

        groupInfo = rowValues[:4]

        if groupInfo != prevInfo: # New row
            tableData.append({"unit-name": rowValues[0], "molecule-name": rowValues[1], "molecule-type/loc": rowValues[2], "sequence-type": rowValues[3]})
            prevInfo = groupInfo

        tableData[-1][rowValues[4]] = rowValues[5]

    df = pd.DataFrame.from_records(tableData)
    for key, value in infoData.items(): # Add info to tabular data
        df[key] = value

    return df

def combine(inputFolder: Path, outputFilePath: Path):
    writer = Writer(outputFilePath.parent, "assemblySections", "section")

    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        futures = (executor.submit(parseStats, filePath) for filePath in inputFolder.iterdir())
        for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            print(f"At entry: {idx}", end="\r")

            df = future.result()
            if df is not None:
                writer.writeDF(df)

    print()
    writer.oneFile(outputFilePath)
