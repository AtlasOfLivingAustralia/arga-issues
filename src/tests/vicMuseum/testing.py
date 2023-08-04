import requests
import json
import pandas as pd

def buildURL(keyword: str, perPage: int, page: int = 1) -> str:
    return f"https://collections.museumsvictoria.com.au/api/{keyword}?perpage={perPage}&page={page}"

response = requests.get(buildURL("species", 0), headers={"User-Agent": ""})
print(response.headers)
data = response.json()

for item in data:
    item |= item.pop("taxonomy", {}) # Expand taxonomy field into separate pieces of data

with open("response.json", "w") as fp:
    json.dump(data, fp, indent=4)

df = pd.DataFrame.from_dict(data)
df.to_csv("output.csv", index=False)