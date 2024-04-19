from pathlib import Path
import requests
from bs4 import BeautifulSoup
import pandas as pd

def retrieve(outputFilePath: Path) -> None:
    baseURL = "https://i5k.nal.usda.gov"
    organisms = []

    for i in range(6):
        print(f"Scraping page: {i+1}/6", end="\r")
        response = requests.get(baseURL + f"/organisms?page={i}")
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("tbody")
        for link in table.find_all("a"):
            name = link.get_text()
            href = link.get("href")

            organism = {"name": name}

            response = requests.get(baseURL + href)
            soup = BeautifulSoup(response.text, "html.parser")
            
            for idx, table in enumerate(soup.find_all("tbody")): # Summary, Analysis, Assembly stats, Other information
                if idx == 1:
                    continue # Skip analysis table

                for row in table.find_all("tr"):
                    key = row.find("th").get_text()
                    value = row.find("td").get_text().replace("\n", " ")

                    if key in ("Resource Type", "Description"):
                        continue

                    organism[key] = value

            organisms.append(organism)
    
    pd.DataFrame.from_records(organisms).to_csv(outputFilePath, index=False)
