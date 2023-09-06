import requests
import json
import math
import pandas as pd
import concurrent.futures

def buildURL(apiKey, method, **kwargs):
    baseURL = "https://api.flickr.com/services/rest/?method="
    url = f"{baseURL}{method}&api_key={apiKey}"
    suffix = "&format=json&nojsoncallback=1"

    parameters = "&".join(f"{key}={value}" for key, value in kwargs.items())
    if parameters:
        url = f"{url}&{parameters}"

    return f"{url}{suffix}"

def processPhoto(apiKey: str, licenses: dict, photo: dict) -> dict:
    photoID = photo["id"]

    response = requests.get(buildURL(apiKey, "flickr.photos.getInfo", photo_id=photoID))
    if response.status_code != 200:
        print(f"Info error with id: {photoID}")
        return {}
    
    photoInfo = response.json()["photo"]

    response = requests.get(buildURL(apiKey, "flickr.photos.getSizes", photo_id=photoID))
    if response.status_code != 200:
        print(f"Size error with id: {photoID}")
        return {}
    
    photoSizes = response.json()["sizes"]
    image = sorted(photoSizes["size"], key=lambda x: int(x["width"]) * int(x["height"]), reverse=True)[0]
    
    tags = [tag["raw"] for tag in photoInfo["tags"]["tag"]]

    return {
        "type": "image",
        "format": image["source"].rsplit(".", 1)[-1],
        "identifier": image["source"],
        "references": image["url"].rsplit("/", 3)[0],
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
        "width": image["width"],
        "height": image["height"],
        "tags": tags
    }

def run():
    with open("flickrkey.txt") as fp:
        apiKeyData = fp.read()

    with open("flickrusers.txt") as fp:
        users = fp.read()

    apiKey, secret = apiKeyData.rstrip("\n").split("\n")
    userList = users.rstrip("\n").split("\n")

    photosPerCall = 500

    # Get licenses
    print("Getting license information")
    response = requests.get(buildURL(apiKey, "flickr.photos.licenses.getInfo"))
    licenseData = response.json()
    licenses = {licenseInfo["id"]: licenseInfo["name"] for licenseInfo in licenseData["licenses"]["license"]}

    for user in userList:
        if user.startswith("_"):
            continue

        print(f"Getting photos for {user}")
        response = requests.get(buildURL(apiKey, "flickr.people.getPhotos", user_id=user, per_page=1))
        data = response.json()

        totalPhotos = data["photos"]["total"]
        totalCalls = math.ceil(totalPhotos / photosPerCall)

        photos = []
        for call in range(1, totalCalls + 1):
            print(f"Processing page {call} / {totalCalls}")
            response = requests.get(buildURL(apiKey, "flickr.people.getPhotos", user_id=user, per_page=photosPerCall, page=call))
            photoData = response.json()["photos"]
            photoList = photoData.get("photo", [])

            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                futures = (executor.submit(processPhoto, apiKey, licenses, photo) for photo in photoList)
            
                try:
                    for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                        result = future.result()
                        print(f"At photo {idx} / {len(photoList)}", end="\r")
                        if result:
                            photos.append(result)

                except (KeyboardInterrupt, ValueError):
                    print("\nExiting...")
                    executor.shutdown(cancel_futures=True)
                    exit()

            print()

        df = pd.DataFrame.from_records(photos)
        df.to_csv(f"{user}_flickrImages.csv", index=False)

if __name__ == "__main__":
    run()