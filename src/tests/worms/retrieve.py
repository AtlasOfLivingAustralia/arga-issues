import requests
import json
import pandas as pd
import concurrent.futures
from lib.tools.subfileWriter import Writer
from pathlib import Path

def getResponse(endpoint: str, id: int, offset: int = 0) -> list[dict] | None:
    baseURL = "https://www.marinespecies.org/rest/"

    url = f"{baseURL}{endpoint}/{id}?offset={offset}"
    headers = {
        "accept": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    try:
        return response.json()
    except json.JSONDecodeError:
        print(response)
        return None

endpoint = "AphiaRecordsByTaxonRankID"
taxonRankID = 220 # Species level ID
entriesPerFile = 1000

writer = Writer(Path("./species"), "sections", "species")
with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
    completed = False
    iteration = 0

    while not completed:
        records = []
        startOffset = iteration * entriesPerFile
        endOffset = (iteration + 1) * entriesPerFile
        futures = (executor.submit(getResponse, endpoint, taxonRankID, offset) for offset in range(startOffset, endOffset))

        try:
            for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                print(f"Offset: {startOffset + idx}", end="\r")

                result = future.result()

                if result is None:
                    continue

                records.extend(result)
                if len(result) < 50:
                    print(f"\nFinal result: {result}")
                    completed = True
                    executor.shutdown(cancel_futures=True)
                    break

        except KeyboardInterrupt:
            print("\nExiting...")
            executor.shutdown(cancel_futures=True)
            exit()

        df = pd.DataFrame.from_records(records)
        writer.writeDF(df)

    writer.oneFile(Path("./worms.csv"))

# offset = 1
# while len(data) == 50:
#     print(f"Offset: {offset}", end="\r")
#     data = getResponse(endpoint, taxonRankID, offset)
#     records.extend(data)
#     offset += 1

# if data:
#     records.extend(data)

# df = pd.DataFrame.from_records(records)
# df.to_csv("worms.csv", index=False)
# print()
