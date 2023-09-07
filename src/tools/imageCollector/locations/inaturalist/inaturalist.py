from pathlib import Path
import pandas as pd

def run():
    dataFolder = Path("./inaturalist-open-data-20230827")

    observations = dataFolder / "observations" # Large
    observers = dataFolder / "observers"
    photos = dataFolder / "photos" # Large
    taxa = dataFolder / "taxa"



if __name__ == "__main__":
    # downloadURL = "https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz"

    run()