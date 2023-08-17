import requests
from bs4 import BeautifulSoup
from pathlib import Path
# from extract import parse
import pandas as pd

requestURL = "https://f1000research.com/extapi/search?q=R_TY:GENOME_NOTE"
response = requests.get(requestURL)

with open("response.xml", "w") as fp:
    fp.write(response.text)

records = []
soup = BeautifulSoup(response.text, "xml")
output = soup.find_all("doi")
# print(output)
with open("dois.txt", "w") as fp:
    fp.write("\n".join(doi.text for doi in output))

# for idx, doi in enumerate(soup.find_all("doi"), start=1):
#     print(f"At doi: {idx}", end="\r")
#     outputFile = Path("output.xml")
#     # print(f"https://doi.org/{doi.text}")
#     doiData = f"https://f1000research.com/extapi/article/xml?doi={doi.text}"
#     response = requests.get(doiData)

#     record = parse(response.text)
#     record["doi"] = doi.text
#     records.append(record)

# print("\nSaving to output.csv...")
# df = pd.DataFrame.from_records(records)
# df.to_csv("output.csv", index=False)
