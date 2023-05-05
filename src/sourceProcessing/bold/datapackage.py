import requests
from bs4 import BeautifulSoup

import pandas as pd
from pathlib import Path

def retrieve(outputFilePath: Path):
    # datapackageURL = "https://www.boldsystems.org/index.php/datapackages"
    # request = requests.get(datapackageURL)

    # tables = pd.read_html(request.text)
    # recentData = tables[0]
    # mostRecent = recentData["Snapshot Date"][0]
    mostRecent = "28-APR-2023"

def cleanUp(folderPath: Path, outputFilePath: Path):
    for file in folderPath.iterdir():
        if file.suffix == ".tsv":
            file.rename(outputFilePath)
            continue

        file.unlink()

    folderPath.rmdir() # Cleanup remaining folder
