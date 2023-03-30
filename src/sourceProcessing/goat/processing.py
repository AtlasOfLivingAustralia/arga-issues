import requests
from urllib.parse import quote
import pandas as pd
import subprocess

def buildCall(size, query, tidy):
    baseURL = "https://goat.genomehubs.org/api/v2/"
    fullURL = f"{baseURL}search?query={quote(query)}&result=taxon&includeEstimates=true&size={size}&tidyData={'true' if tidy else 'false'}"
    return fullURL

def build(outputFilePath):
    query = "tax_name(*) AND tax_rank(species)"

    response = requests.get(buildCall(0, query, False))
    output = response.json()
    status = output.get("status", {})
    hits = status.get("hits", 0)

    subprocess.run(["curl.exe", "-X", "GET", buildCall(hits, query, True), "-H", "accept: text/csv", "-o", outputFilePath])

def clean(filePath, outputFilePath):
    df = pd.read_csv(filePath)

    df["aggregation_source"] = df["aggregation_source"].apply(lambda x: x.replace('"', ''))
    combineFields = ["field", "value", "aggregation_source", "aggregation_method"]
    df["data"] = df[combineFields].apply(lambda x: {col: item for col, item in zip(combineFields, x)}, axis=1)
    data = df.groupby("taxon_id")[["data"]].agg(lambda x: [v for v in x])
    df.drop(combineFields + ["data"], axis=1, inplace=True)
    df = df.merge(data, "left", on="taxon_id")
    df = df.drop_duplicates(["taxon_id"])
    df.to_csv(outputFilePath, index=False)