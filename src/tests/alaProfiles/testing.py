import requests
import json
import pandas as pd

with open("token.json") as fp:
    token = json.load(fp)

bearerToken = token["access_token"]
profile = "kamilaroi"

baseURL = "https://api.ala.org.au/profiles"
endpoint = f"/api/opus/{profile}/profile?pageSize=1000"
# endpoint = f"/api/opus/{profile}"
# endpoint = f"/api/opus/{profile}/profile/{profileID}"

response = requests.get(baseURL + endpoint, headers={"Authorization": f"Bearer {bearerToken}"})
data = response.json()
print(f"Found {len(data)} records")

records = []
for idx, entry in enumerate(data, start=1):
    uuid = entry["uuid"]
    print(f"At record: {idx}", end="\r")

    response = requests.get(baseURL + f"/api/opus/{profile}/profile/{uuid}", headers={"Authorization": f"Bearer {bearerToken}"})
    records.append(response.json())

print()
# with open(f"{profile}_response.json", "w") as fp:
#     json.dump(data, fp, indent=4)

df = pd.DataFrame.from_records(records)
df.to_csv(f"{profile}_profiles.csv", index=False)