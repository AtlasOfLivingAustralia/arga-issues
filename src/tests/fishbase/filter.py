from pathlib import Path
import pandas as pd
import lib.dataframeFuncs as dff

with open("keep.txt") as fp:
    files = [file for file in fp.read().rstrip("\n").split("\n") if not file.startswith("_")]

folder = Path("./fishbase_csvs")
colMap = {}

for file in folder.iterdir():
    if file.stem not in files:
        continue

    chunks = dff.chunkGenerator(file, 1)
    df = next(chunks)
    colMap[file] = list(df.columns)

matchWeights = {key: {} for key in list(colMap)}
for idx, (filePath, columns) in enumerate(colMap.items(), start=1):
    for compareKey in list(colMap)[idx:]:
        compareColumns = [c.lower() for c in colMap[compareKey]]

        for cidx, col in enumerate(columns):
            if col.lower() == "ts":
                continue

            try:
                matchPos = compareColumns.index(col.lower())
            except ValueError:
                continue

            weight = (cidx + matchPos) ** 2
            matchWeights[filePath][compareKey] = (weight, col.lower())
            matchWeights[compareKey][filePath] = (weight, col.lower())

for file, matches in matchWeights.items():
    sortedMatches = {key: value for key, value in sorted(matches.items(), key=lambda x: x[1])}
    bestMatch = list(sortedMatches)[0]
    print(file, bestMatch, sortedMatches[bestMatch])