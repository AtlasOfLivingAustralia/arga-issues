import requests
import json

def buildURL(keyword: str, perPage: int, page: int = 1) -> str:
    return f"https://collections.museumsvictoria.com.au/api/{keyword}?perpage={perPage}&page={page}&hasimages=yes"

def processEntry(entry: dict) -> list[dict]:
    media = entry["media"]
    taxonName = entry["taxonomy"]["taxonName"]

    images = []
    for mediaObject in media:
        if not mediaObject["type"] == "image":
            continue

        image = mediaObject["large"]
        imageFormat = image["uri"].rsplit(".", 1)[-1]

        info = {
            "type": "image",
            "format": imageFormat,
            "identifier": image["uri"],
            "references": "",
            "title": mediaObject["alternativeText"],
            "description": mediaObject["caption"],
            "created": mediaObject["dateModified"],
            "creator": mediaObject["creators"],
            "contributor": "",
            "publisher": "Museums Victoria",
            "audience": "",
            "source": mediaObject["sources"],
            "license": mediaObject["licence"]["name"],
            "rightsHolder": mediaObject["rightsStatement"],
            "datasetID": mediaObject["id"],
            "taxonName": taxonName,
            "width": image["width"],
            "height": image["height"]
        }

        images.append(info)

    return images

def run():
    keywords = ["species", "specimens"]
    entriesPerPage = 1000

    headers = {
        "User-Agent": ""
    }

    response = requests.get(buildURL("species", 1), headers=headers)
    data = response.json()

    totalResults = int(response.headers.get("Total-Results", 0))
    print(totalResults)

    entries = []
    for entry in data:
        entries.extend(processEntry(entry))

if __name__ == "__main__":
    run()
