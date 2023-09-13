import json
import pandas as pd
import requests

def convert(stageFile, outputFilePath):
    with open(stageFile.filePath) as fp:
        data = json.load(fp)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputFilePath, index=False)

def speciesDownload(outputFilePath):
    url = "https://rest.ensembl.org/info/species?"
    request = requests.get(url, headers={ "Content-Type" : "application/json"})
 
    if not request.ok:
        request.raise_for_status()
        return
    
    data = request.json()
    df = pd.DataFrame.from_records(data["species"])
    df.to_csv(outputFilePath, index=False)
