from pathlib import Path
import pandas as pd
from lib.subfileWriter import Writer

def combine(inputFolder: Path, outputFilePath: Path):
    writer = Writer(outputFilePath.parent, "assemblySections", "section")

    for idx, filePath in enumerate(inputFolder.iterdir(), start=1):
        print(f"At entry: {idx}", end="\r")

        with open(filePath, encoding="utf-8") as fp:
            data = fp.read()

        info, table = data.rsplit("#", 1)

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

        writer.writeDF(df)

    print()
    writer.oneFile(outputFilePath)
