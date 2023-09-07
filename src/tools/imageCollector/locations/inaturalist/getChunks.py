import pandas as pd
import lib.dataframeFuncs as dff
from pathlib import Path

sourceFolder = Path("./inaturalist-open-data-20230827")
outputFolder = Path("./chunks")

chunkSize = 1000

if not outputFolder.exists():
    outputFolder.mkdir()

for file in sourceFolder.iterdir():
    chunkGen = dff.chunkGenerator(file, chunkSize, sep="\t")
    chunk = next(chunkGen)

    chunk.to_csv(outputFolder / f"{file.stem}_chunk.csv", index=False)
