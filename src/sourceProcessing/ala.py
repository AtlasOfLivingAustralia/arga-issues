import pandas as pd
import requests
import json
import math
from pathlib import Path
import lib.dataframeFuncs as dff

def build(outputFilePath: Path) -> None:
    baseURL = "https://biocache-ws.ala.org.au/ws/occurrences/search?q=*%3A*&disableAllQualityFilters=true&qualityProfile=AVH&fq=type_status%3A*&fq=country%3A%22Australia%22&qc=data_hub_uid%3Adh9"
    firstCall = baseURL + "&pageSize=0"
    readSize = 1000

    rawData = requests.get(firstCall)
    jsData = rawData.json()

    records = jsData["totalRecords"]
    totalCalls = math.ceil(records / readSize)

    occurrences = []
    for call in range(totalCalls):
        callURL = baseURL + f"&pageSize={readSize}" + f"&startIndex={call*readSize}"
        print(f"At call: {call+1} / {totalCalls}", end="\r")
        rawData = requests.get(callURL)
        jsData = rawData.json()
        occurrences.extend(jsData["occurrences"])

    df = pd.DataFrame.from_records(occurrences)
    df.to_csv(outputFilePath, index=False)

def collect(outputDir: Path, profileList: list[str], tokenFilePath: Path) -> None:
    with open(tokenFilePath) as fp:
        token = json.load(fp)

    bearerToken = token["access_token"]
    baseURL = "https://api.ala.org.au/profiles"

    for profile in profileList:
        endpoint = f"/api/opus/{profile}/profile?pageSize=1000"
        response = requests.get(baseURL + endpoint, headers={"Authorization": f"Bearer {bearerToken}"})
        data = response.json()

        if "message" in data and "not authorized" in data["message"]:
            print("Failed to authorize, please make sure bearer token is valid.")
            return
        
        print(f"Accessing profile: {profile}")

        records = []
        for idx, entry in enumerate(data, start=1):
            uuid = entry["uuid"]
            print(f"At record: {idx}", end="\r")

            response = requests.get(baseURL + f"/api/opus/{profile}/profile/{uuid}", headers={"Authorization": f"Bearer {bearerToken}"})
            records.append(response.json())
        print()

        df = pd.DataFrame.from_records(records)
        df = dff.removeSpaces(df)
        df.to_csv(outputDir / f"{profile}.csv", index=False)
