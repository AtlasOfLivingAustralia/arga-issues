from pathlib import Path
import pandas as pd
import lib.dataframeFuncs as dff
import numpy as np

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

    photosGen = dff.chunkGenerator(photos, 100, "\t")
    for idx, df in enumerate(photosGen, start=1):
        print(f"At chunk: {idx}", end="\r")
        df.drop(df[df["license"] == "CC-BY-NC-ND"].index, inplace=True)
    
        obsvGen = dff.chunkGenerator(observations, 1024*1024, "\t")
        for subIdx, obsv in enumerate(obsvGen):
            obsv.drop(obsv[obsv["quality_grade"] != "research"].index, inplace=True)
            obsv.drop(["latitude" , "longitude", "positional_accuracy", "quality_grade", "observer_id", "observed_on"], axis=1, inplace=True)
            df = pd.merge(df, obsv, "left", on="observation_uuid")
            # df = df.combine_first(obsv[["observation_uuid"]])

            if subIdx >= 5:
                break

        try:
            df = pd.merge(df, taxonomy, "left", on="taxon_id")
        except KeyError:
            print("TaxonID error")

        try:
            df = pd.merge(df, observers, "left", on="observer_id")
        except KeyError:
            print("ObserverID error")

        df.drop(["position", "taxon_id", "observer_id"], axis=1, inplace=True)
        df.to_csv("testing.csv", index=False)
        break

if __name__ == "__main__":
    # downloadURL = "https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz"

    run()