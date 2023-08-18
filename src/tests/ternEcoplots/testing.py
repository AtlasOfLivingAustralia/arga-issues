import requests
import json

def buildURL(endpoint):
    baseURL = "https://ecoplots-test.tern.org.au/api/v1.0/"
    return f"{baseURL}{endpoint}?dformat=ndjson"

with open("apikey.txt") as fp:
    apiKey = fp.read()

headers = {
  "X-Api-Key": apiKey,
  "Content-Type": "application/json"
}

payload = {
    "query": {
        "feature_type": [
            "http://linked.data.gov.au/def/tern-cv/6d40d71e-58cd-4f75-8304-40c01fe5f74c"
        ]
    }
}

response = requests.post(buildURL("datasets"), headers=headers, data=json.dumps(payload))
print(response)
print(response.text)
