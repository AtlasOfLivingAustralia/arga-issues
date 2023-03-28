import requests
import json
from urllib.parse import quote
import math
import concurrent.futures
import pandas as pd
import subprocess

def buildCall(size, offset, query, tidy):
    baseURL = "https://goat.genomehubs.org/api/v2/"
    fullURL = f"{baseURL}search?query={quote(query)}&result=taxon&includeEstimates=true&size={size}&offset={offset}&tidyData={'true' if tidy else 'false'}"
    return fullURL

hitsPerPage = 10
query = "tax_name(*) AND tax_rank(species)"

response = requests.get(buildCall(0, 0, query, False))
output = response.json()
status = output.get("status", {})
hits = status.get("hits", 0)
print(f"Hits: {hits}")

# totalCalls = math.ceil(hits / hitsPerPage)
# totalCalls = 1

# response = requests.get(buildCall(hits, 0, query, True))
proc = subprocess.Popen(["curl.exe", "-X", "GET", buildCall(20, 0, query, True), "-H", "accept: text/csv"], stdout=subprocess.PIPE)
out, err = proc.communicate()
out = out.decode("utf-8").split("\n")
columns = [s.replace('"', '').split(",") for s in out[0]]

data = [row.split(",") for row in out[1:]]
df = pd.DataFrame.from_records([row.split(",") for row in out[1:]], columns=columns)
df.to_csv("result.csv", index=False)

# data = []
# with concurrent.futures.ThreadPoolExecutor() as executor:
#     futures = [executor.submit(getResults, hitsPerPage, hitsPerPage * call, query) for call in range(totalCalls)]
#     for future in concurrent.futures.as_completed(futures):
#         output = future.result()[0]
#         result = output.pop("result", {})
#         output |= result
#         data.append(output)
# print(data)

# df = pd.DataFrame.from_records(data)
# df.to_csv("goat_response.csv", index=False)
