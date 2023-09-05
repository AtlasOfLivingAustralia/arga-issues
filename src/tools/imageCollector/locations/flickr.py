import requests
import json
import math
from datetime import datetime

def buildURL(apiKey, method, **kwargs):
    baseURL = "https://api.flickr.com/services/rest/?method="
    url = f"{baseURL}{method}&api_key={apiKey}"
    suffix = "&format=json&nojsoncallback=1"

    parameters = "&".join(f"{key}={value}" for key, value in kwargs.items())
    if parameters:
        url = f"{url}&{parameters}"

    return f"{url}{suffix}"

def run():
    with open("flickrkey.txt") as fp:
        apiKey = fp.read()

    with open("flickrusers.txt") as fp:
        users = fp.read()

    key, secret = apiKey.rstrip("\n").split("\n")
    userList = users.rstrip("\n").split("\n")

    photosPerCall = 500

    # Get licenses
    response = requests.get(buildURL(key, "flickr.photos.licenses.getInfo"))
    licenseData = response.json()
    licenses = {licenseInfo["id"]: licenseInfo["name"] for licenseInfo in licenseData["licenses"]["license"]}

    for user in userList:
        response = requests.get(buildURL(key, "flickr.people.getPhotos", user_id=user, per_page=1))
        data = response.json()

        totalPhotos = data["photos"]["total"]
        totalCalls = math.ceil(totalPhotos / photosPerCall)

        photos = []
        for call in range(1, totalCalls + 1):
            response = requests.get(buildURL(key, "flickr.people.getPhotos", user_id=user, per_page=photosPerCall, page=call))
            photoData = response.json()["photos"]

            for photo in photoData.get("photo", []):
                photoID = photo["id"]

                response = requests.get(buildURL(key, "flickr.photos.getInfo", photo_id=photoID))
                photoInfo = response.json()["photo"]

                response = requests.get(buildURL(key, "flickr.photos.getSizes", photo_id=photoID))
                photoSizes = response.json()["sizes"]

                photo = sorted(photoSizes["size"], key=lambda x: x["width"] * x["height"], reverse=True)[0]

                info = {
                    "type": "image",
                    "format": photo["source"].rsplit(".", 1)[-1],
                    "identifier": photo["source"],
                    "references": photo["url"].rsplit("/", 3)[0],
                    "title": photoInfo["title"]["_content"],
                    "description": photoInfo["description"]["_content"],
                    "created": photoInfo["dates"]["taken"],
                    "creator": photoInfo["owner"]["username"],
                    "contributor": "",
                    "publisher": photoInfo["owner"]["username"],
                    "audience": "",
                    "source": "flickr.com",
                    "license": licenses[int(photoInfo["license"])],
                    "rightsHolder": "",
                    "datasetID": photoID,
                    "taxonName": "",
                    "width": photo["width"],
                    "height": photo["height"]
                }

                photos.append(info)

        

if __name__ == "__main__":
    run()

    a = datetime.fromtimestamp()