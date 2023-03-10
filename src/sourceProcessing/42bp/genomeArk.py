import requests
from pathlib import Path
import lib.commonFuncs as cmn
import yaml
import csv
from yaml.scanner import ScannerError
import json

def build(savedFilePath: Path, overwrite: bool = False) -> list[str]:
    location = "https://42basepairs.com/api/v1/files/s3/genomeark/species/"
    downloadURL = "https://42basepairs.com/download/s3/genomeark/species/"

    if not savedFilePath.exists() or overwrite:
        rawHTML = requests.get(location)
        rawJSON = rawHTML.json()
        speciesList = rawJSON.get("files", [])

        with open(savedFilePath, 'w') as fp:
            json.dump(speciesList, fp)

    else:
        with open(savedFilePath) as fp:
            speciesList = json.load(fp)

    output = []
    for species in speciesList:
        name = species.get("name", "")

        if not name or name == "..":
            continue

        output.append(downloadURL + name + "metadata.yaml")

    return output

def combine(folderPath: Path, outputPath: Path):
    allData = []
    columns = []

    for filePath in folderPath.iterdir():
        if filePath.suffix != '.yaml':
            continue

        try:
            with open(filePath) as fp:
                data = yaml.load(fp, Loader=yaml.Loader)
        except ScannerError:
            continue

        if not isinstance(data, dict):
            continue

        data = cmn.flatten(data)
        allData.append(data)
        columns = cmn.extendUnique(columns, data.keys())
    
    with open(outputPath, 'w', newline='') as fp:
        writer = csv.DictWriter(fp, columns)
        writer.writeheader()
        
        for row in allData:
            writer.writerow(row)
