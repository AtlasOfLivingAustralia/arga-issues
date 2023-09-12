import pandas as pd
from pathlib import Path
import lib.dataframeFuncs as dff
from lib.tools.bigFileWriter import BigFileWriter
import dask.dataframe as dd

observations = Path("./inaturalist-open-data-20230827/observations.csv")
photos = Path("./inaturalist-open-data-20230827/photos.csv")
taxa = Path("./inaturalist-open-data-20230827/taxa.csv")

taxonFile = Path("./taxonCount.csv")
if not taxonFile.exists():
    values = pd.read_csv(observations, sep="\t", usecols=["taxon_id"])
    counts = values.value_counts()
    df = counts.to_frame("counts")
    df = df.reset_index()

    del values

    df["taxon_id"] = df["taxon_id"].astype(int)

    taxonomy = pd.read_csv(taxa, sep="\t", dtype=object)
    taxonomy["taxon_id"] = taxonomy["taxon_id"].astype(int)
    df = df.merge(taxonomy, "left", on="taxon_id")
    df.to_csv(taxonFile, index=False)
else:
    df = pd.read_csv(taxonFile, dtype=object)

speciesFile = Path("./speciesCount.csv")
if not speciesFile.exists():
    df = df[df["rank"].isin(["species", "subspecies"])]
    df.to_csv(speciesFile, index=False)
else:
    df = pd.read_csv(speciesFile)

taxonIDFile = Path("./taxonIDs.txt")
if not taxonIDFile.exists():
    ids = df["taxon_id"].astype(str).tolist()
    with open(taxonIDFile, "w") as fp:
        fp.write("\n".join(ids))
else:
    with open(taxonIDFile) as fp:
        ids = fp.read().split("\n")

obsvIDs = Path("./observationIDs.txt")
if not obsvIDs.exists():
    chunkGen = dff.chunkGenerator(observations, 1024*1024*4, "\t", usecols=["taxon_id", "observation_uuid"])
    # df = dd.read_csv(observations, sep="\t", usecols=["taxon_id", "observation_uuid"], dtype={"taxon_id": 'float64'})
    # observationIDs = pd.DataFrame(columns=["observation_uuid"])

    # writer = BigFileWriter(obsvIDs, "obsv")
    for idx, df in enumerate(chunkGen, start=1):
        print(f"At chunk: {idx}", end="\r")
    # df = df[df["taxon_id"].compute().isin(ids)]
    # df.to_csv(obsvIDs, index=False)

        # writer.writeDF(df["observation_uuid"].to_frame())
        with open(obsvIDs, "a") as fp:
            fp.write("\n".join(df["observation_uuid"].tolist()))
    
    # writer.oneFile()
    print()
# else:
#     df = dd.read_csv(obsvIDs)

# photoIDs = []
photoIDs = Path("./photoIDs.txt")

lengthBefore = 0
lengthAfter = 0
# photosDF = dd.read_csv(photos, sep="\t", usecols=["photo_uuid", "observation_uuid"])
chunkGen = dff.chunkGenerator(photos, 1024*1024*4, sep="\t", usecols=["photo_uuid", "observation_uuid"])
readBytes = 1024*1024*256

for idx, df in enumerate(chunkGen, start=1):
    # print(f"At chunk: {idx}", end="\r")
    lengthBefore += len(df)

    with open(obsvIDs) as fp:
        observationIDs = fp.readlines(readBytes)

        subIdx = 1
        print(" "*50, end="\r")
        while observationIDs:
            print(f"At chunk {idx}, subchunk {subIdx}", end="\r")
            observationIDs = [x.rstrip("\n") for x in observationIDs] # Clean off trailing newline

            df = df[df["observation_uuid"].isin(observationIDs)].copy()
            photoUUIDs = df["photo_uuid"].tolist()
            lengthAfter += len(photoUUIDs)

            with open(photoIDs, "a") as fp2:
                fp2.write("\n".join(photoUUIDs))

            observationIDs = fp.readlines(readBytes)
            subIdx += 1
#     photoIDs.extend(df["photo_uuid"].to_list())

# photoIDs = photosDF[photosDF["observation_uuid"].isin(df["observation_uuid"])]
# photoIDs.to_csv("photoIDs.csv", index=False)
print()
print(lengthBefore, lengthAfter)
