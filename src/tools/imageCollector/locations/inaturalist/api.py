import requests
import json

def buildURL(endpoint: str, **kwargs) -> str:
    baseURL = "https://api.inaturalist.org/v1/"

    url = f"{baseURL}{endpoint}"

    parameters = "&".join(f"{key}={value}" for key, value in kwargs.items())
    if parameters:
        url = f"{url}?{parameters}"

    return url

def run():
    response = requests.get(buildURL("observations", photos="true", quality_grade="research", rank="species", per_page=200, order_by="id"))
    
    with open("response.json", "w") as fp:
        json.dump(response.json(), fp, indent=4)

if __name__ == "__main__":
    run()
