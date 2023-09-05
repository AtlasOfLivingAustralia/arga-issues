import requests
import json
import pandas as pd

def buildURL(keyword: str, perPage: int, page: int = 1) -> str:
    return f"https://collections.museumsvictoria.com.au/api/{keyword}?perpage={perPage}&page={page}&hasimages=yes"

def processEntry(entry: dict) -> list[dict]:
    media = entry["media"]
    taxonomy = entry["taxonomy"]
    speciesID = entry["id"]
    taxonName = taxonomy["taxonName"] if taxonomy is not None else ""

    images = []
    for mediaObject in media:
        if not mediaObject["type"] == "image":
            continue

        
        for size in ("large", "medium", "small", "thumbnail"):
            image = mediaObject.get("large", {})
            if image:
                break
        else:
            continue

        imageFormat = image["uri"].rsplit(".", 1)[-1]

        info = {
            "type": "image",
            "format": imageFormat,
            "identifier": image.get("uri", ""),
            "references": f"https://collections.museumsvictoria.com.au/{speciesID}",
            "title": mediaObject.get("alternativeText", ""),
            "description": mediaObject.get("caption", "").replace("<em>", "").replace("</em>", ""),
            "created": mediaObject.get("dateModiied", ""),
            "creator": mediaObject.get("creators", ""),
            "contributor": "",
            "publisher": "Museums Victoria",
            "audience": "",
            "source": mediaObject.get("sources", ""),
            "license": mediaObject.get("licence", {}).get("name", ""),
            "rightsHolder": mediaObject.get("rightsStatement", ""),
            "datasetID": mediaObject.get("id", ""),
            "taxonName": taxonName,
            "width": image.get("width", 0),
            "height": image.get("height", 0)
        }

        images.append(info)

    return images

def run():
    keywords = ["species", "specimens"]
    entriesPerPage = 1000

    headers = {
        "User-Agent": ""
    }

    response = requests.get(buildURL("species", 100), headers=headers)
    data = response.json()

    totalResults = int(response.headers.get("Total-Results", 0))
    print(totalResults)

    entries = []
    for entry in data:
        entries.extend(processEntry(entry))

    df = pd.DataFrame.from_records(entries)
    df.to_csv("images.csv", index=False)

if __name__ == "__main__":
    run()
