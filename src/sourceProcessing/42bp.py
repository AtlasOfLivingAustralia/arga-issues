import requests
from pathlib import Path
import lib.commonFuncs as cmn
import yaml
from yaml.scanner import ScannerError
import json
from lib.tools.downloader import Downloader
import pandas as pd

def build(outputFilePath: Path, savedFilePath: Path) -> None:
    location = "https://42basepairs.com/api/v1/files/s3/genomeark/species/"
    baseDLUrl = "https://42basepairs.com/download/s3/genomeark/species/"

    if not savedFilePath.exists():
        rawHTML = requests.get(location)
        rawJSON = rawHTML.json()
        speciesList = rawJSON.get("files", [])

        with open(savedFilePath, 'w') as fp:
            json.dump(speciesList, fp, indent=4)

    else:
        with open(savedFilePath) as fp:
            speciesList = json.load(fp)

    records = []
    downloader = Downloader()
    for species in speciesList:
        name = species.get("name", "")

        if not name or name == "..":
            continue

        downloadURL = baseDLUrl + name + "metadata.yaml"
        filePath = outputFilePath.parent / f"{name[:-1]}_metadata.yaml"
        if not filePath.exists():
            success = downloader.download(downloadURL, filePath)
            if not success:
                continue

        try:
            with open(filePath) as fp:
                data = yaml.load(fp, Loader=yaml.Loader)
        except ScannerError:
            continue

        if "<Error>" in data: # Invalid request
            continue

        data = cmn.flatten(data)
        records.append(data)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputFilePath, index=False)
