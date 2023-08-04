from pathlib import Path
import requests
import math
import pandas as pd

def buildURL(keyword: str, perPage: int, page: int = 1) -> str:
    return f"https://collections.museumsvictoria.com.au/api/{keyword}?perpage={perPage}&page={page}"

def retrieve(dataset: str, outputFolder: Path):
    response = requests.get(buildURL(dataset, 1), headers={"User-Agent": ""})
    data = response.json()

    totalResults = int(response.headers.get("Total-Results", 0))
    recordsPerPage = 50
    totalCalls = math.ceil(totalResults / recordsPerPage)

    records = []
    for call in range(totalCalls):
        response = requests.get(buildURL(dataset, recordsPerPage, call+1), headers={"User-Agent": ""})
        data = response.json()
        records.extend(data)

    for record in data:
        record |= record.pop("taxonomy", {}) # Expand taxonomy field into separate pieces of data

    df = pd.DataFrame.from_records(records)
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    df.to_csv(outputFolder / f"{dataset}.csv", index=False)
