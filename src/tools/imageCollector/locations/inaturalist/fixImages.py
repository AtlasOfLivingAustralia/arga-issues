from pathlib import Path
import pandas as pd
import numpy as np
import lib.dataframeFuncs as dff
from lib.tools.bigFileWriter import BigFileWriter

folder = Path("./subfiles")
photos = Path("./inaturalist-open-data-20230827/photos.csv")
writer = BigFileWriter(Path("inaturalist.csv"), subDirName="fixed_chunks")

for idx, file in enumerate(folder.iterdir(), start=1):
    df = pd.read_csv(file, dtype=object)

    df["format"] = np.NaN
    df["identifier"] = np.NaN

    df.drop_duplicates("datasetID", inplace=True)
    df.set_index("datasetID", inplace=True)

    chunkGen = dff.chunkGenerator(photos, 1024*1024, sep="\t")
    for subIdx, chunk in enumerate(chunkGen, start=1):
        print(f"At: file {idx} | chunk {subIdx}", end="\r")

        chunk.drop_duplicates("photo_uuid", inplace=True)
        chunk.set_index("photo_uuid", inplace=True)

        df["format"].fillna(chunk["extension"], inplace=True)
        df["identifier"].fillna(chunk["photo_id"], inplace=True)

        if all(dff.getColumnEmptyCount(df, column) <= 0 for column in ("format", "identifier")):
            break

    # df.reset_index(drop=True, inplace=True)
    df["identifier"] = "https://inaturalist-open-data.s3.amazonaws.com/photos/" + df["identifier"].astype(str) + "/original." + df["format"]
    
    writer.writeDF(df)

# writer.oneFile()