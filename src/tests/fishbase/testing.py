import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import lib.commonFuncs as cmn
from pathlib import Path
import pyarrow

# dataset = "fishbase"
dataset = "sealifebase"
baseURL = f"https://fishbase.ropensci.org/{dataset}/"
response = requests.get(baseURL)

soup = BeautifulSoup(response.text, "xml")

total = len(soup.find_all("Contents"))
for idx, file in enumerate(soup.find_all("Contents"), start=1):
    print(f"At file: {idx} / {total}", end="\r")
    fileName = file.find("Key").text
    url = baseURL + fileName

    filePath = Path(f"./{dataset}_downloads/{fileName}")
    if not filePath.parent.exists():
        filePath.parent.mkdir()

    if not filePath.exists():
        cmn.downloadFile(url, filePath, verbose=False)

    convertedPath = Path(f"./{dataset}_csvs/{filePath.stem}.csv")
    if not convertedPath.parent.exists():
        convertedPath.parent.mkdir()

    if not convertedPath.exists():
        try:
            df = pd.read_parquet(filePath)
            df.to_csv(convertedPath, index=False)
        except:
            pass
print()