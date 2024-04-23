from pathlib import Path
import requests
from bs4 import BeautifulSoup, ResultSet
import pandas as pd

baseURL = "https://i5k.nal.usda.gov"

def _getSoup(suffix: str) -> BeautifulSoup:
    response = requests.get(baseURL + suffix)
    return BeautifulSoup(response.text, "html.parser")

def _parseAnalysisRow(tableRow: ResultSet[any]) -> dict:
    columns = tableRow.find_all("td")

    subHref = columns[0].find("a").get("href")
    program = columns[1].get_text()
    constructed = columns[2].get_text()

    analysis = {"program": program, "date": constructed}

    subSoup = _getSoup(subHref)
    subTable = subSoup.find("table")
    for subRow in subTable.find_all("tr"):
        subKey = subRow.find("th").get_text()
        subValue = subRow.find("td")

        if subKey == "Organism":
            continue

        if subKey == "Data Source":
            source = subValue.find_all("dd")
            sourceName = source[0].get_text()
            sourceURI = "" if len(source) == 1 else source[1].find("a").get("href")

            analysis["source"] = {
                    "source name": sourceName,
                    "source uri": sourceURI
            }
            continue

        analysis[subKey] = subValue.get_text().replace("\n", " ")

def retrieve(outputFilePath: Path) -> None:
    organisms = []

    for i in range(6):
        print(f"Scraping page: {i+1}/6", end="\r")

        soup = _getSoup(f"/organisms?page={i}")
        table = soup.find("tbody")
        for link in table.find_all("a"):
            name = link.get_text()
            href = link.get("href")

            organism = {"name": name, "analysis": []}

            soup = _getSoup(href)
            for idx, table in enumerate(soup.find_all("tbody")): # Summary, Analysis, Assembly stats, Other information
                for row in table.find_all("tr"):
                    if idx == 1: # Analysis handling
                        organism["analysis"].append(_parseAnalysisRow(row))
                        continue

                    key = row.find("th")
                    value = row.find("td")

                    if idx == 3: # Other information
                        if key == "Links":
                            organism["Links"] = [link.get("href") for link in value.find_all("a")]
                            continue

                    if key.get_text() in ("Resource Type", "Description", "Organism Image", "Image Credit"):
                        continue

                    organism[key.get_text()] = value.get_text().replace("\n", " ")

            organisms.append(organism)
    
    pd.DataFrame.from_records(organisms).to_csv(outputFilePath, index=False)
