import pandas as pd
import lib.dataframeFuncs as dff
from pathlib import Path

folder = Path("./inaturalist-open-data-20230827")

for file in folder.iterdir():
    chunkGen = dff.chunkGenerator(file, 1000, sep="\t")
    chunk = next(chunkGen)

    chunk.to_csv(f"{file.stem}_chunk.csv", ",", index=False)
