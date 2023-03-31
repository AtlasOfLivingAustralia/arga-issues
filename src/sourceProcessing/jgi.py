import requests
import pandas as pd
from pathlib import Path

def _get(project: str, outputFilePath: Path):
    def getResponse(project: str, page: int, entriesPerPage: int) -> dict:
        url = f"https://files.jgi.doe.gov/{project}/?q=*&p={page}&x={entriesPerPage}"
        response = requests.get(url)
        return response.json()
    
    def cleanOrganism(record: dict) -> dict:
        record.pop("agg")
        record.pop("agg_id")
        record.pop("top_hit")
        record.pop("grouped_by")
        record.pop("score")
        record.pop("grouped_count")
        record.pop("files")

        pi = record.get("pi", None)
        if pi is not None:
            record["pi"] = pi.get("name", "")

        return record

    currentPage = 1
    entriesPerPage = 100

    records = []
    while currentPage > 0:
        print(f"Getting page: {currentPage}", end="\r")
        response = getResponse(project, currentPage, entriesPerPage)
        records.extend([cleanOrganism(organism) for organism in response["organisms"]])
        currentPage = int(response["next_page"]) # Next page at end returns False, cast to int to break loop

    print(f"\nCompiling records to file {outputFilePath}")
    pd.DataFrame.from_records(records).to_csv(outputFilePath, index=False)

def getPhytozome(outputFilePath: Path) -> None:
    _get("phytozome_file_list", outputFilePath)

def getMycocosm(outputFilePath: Path) -> None:
    _get("mycocosm_file_list", outputFilePath)

def getPhycocosm(outputFilePath: Path) -> None:
    _get("phycocosm_file_list", outputFilePath)

def getIMG(outputFilePath: Path) -> None:
    _get("img_file_list", outputFilePath)
