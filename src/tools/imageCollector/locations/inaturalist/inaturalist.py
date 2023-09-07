from pathlib import Path
import pandas as pd
import lib.dataframeFuncs as dff
from lib.tools.subfileWriter import Writer

def run():
    dataFolder = Path("./inaturalist-open-data-20230827")

    observations = dataFolder / "observations.csv" # Large
    observers = dataFolder / "observers.csv"
    photos = dataFolder / "photos.csv" # Large
    taxa = dataFolder / "taxa.csv"

    # Prepare taxonomy for getting species name
    taxonomy = pd.read_csv(taxa, dtype=object, sep="\t")
    taxonomy.drop(taxonomy[taxonomy["rank"] != "species"].index, inplace=True)
    taxonomy.drop(["ancestry", "rank_level", "rank", "active"], axis=1, inplace=True)

    # Prepare observers for getting creator name
    observers = pd.read_csv(observers, dtype=object, sep="\t")
    observers["creator"] = observers["name"].fillna(observers["login"])
    observers.drop(["name", "login"], axis=1, inplace=True)

    writer = Writer(Path("."), "chunks", "chunk")

    photosGen = dff.chunkGenerator(photos, 100, "\t")
    for idx, df in enumerate(photosGen, start=1):
        df.drop(df[df["license"] == "CC-BY-NC-ND"].index, inplace=True)

        df["taxon_id"] = "" # Add empty taxon_id column for filling with observations
        obsvGen = dff.chunkGenerator(observations, 1024*1024, "\t")

        print(" "*50, end="\r")
        for subIdx, obsv in enumerate(obsvGen, start=1):
            print(f"At: chunk {idx} | sub chunk {subIdx}", end="\r")
            obsv.drop(obsv[obsv["quality_grade"] != "research"].index, inplace=True)
            obsv = obsv[["observation_uuid", "taxon_id"]] # Only columns needed
            # obsv.drop(["latitude" , "longitude", "positional_accuracy", "quality_grade", "observer_id", "observed_on"], axis=1, inplace=True)
            df["taxon_id"] = (df.set_index("observation_uuid")["taxon_id"].fillna(obsv.set_index("observation_uuid")["taxon_id"]).reset_index(drop=True))
            break

        df.to_csv("before.csv", index=False)
        df = pd.merge(df, taxonomy, "left", on="taxon_id")
        df = pd.merge(df, observers, "left", on="observer_id")
        df.to_csv("after.csv", index=False)
        break
        # df.drop(["position", "taxon_id", "observer_id", "observation_uuid"], axis=1, inplace=True)
        
        # # Renaming fields
        # df.rename({
        #     "extension": "format",
        #     "photo_uuid": "datasetID"
        #     "taxon_"
        # })
        # df["type"] = "image"
        
        # # Missing fields:
        
        # writer.writeDF(df)
    
    print()

if __name__ == "__main__":
    # downloadURL = "https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz"

    run()