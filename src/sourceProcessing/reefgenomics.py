import requests
from bs4 import BeautifulSoup
import lib.commonFuncs as cmn
from pathlib import Path

def build(folderPath: Path, outputFilenames: list[str]) -> None:
    url = "http://reefgenomics.org/sitemap.html"
    rawHTML = requests.get(url)
    soup = BeautifulSoup(rawHTML.text, 'html.parser')
    
    writingFile = 0
    useHeaders = []
    output = []
    for row in soup.find_all('tr'):
        headers = [h.text for h in row.find_all('th')]
        if headers:
            if useHeaders: # If headers existed previously, write to file
                cmn.dictListToCSV(output, useHeaders + ["link"], folderPath / f"{outputFilenames[writingFile]}.csv")
                writingFile += 1
                output.clear()

            useHeaders = headers.copy()
            continue

        data = {}
        for idx, value in enumerate(row.find_all('td')):
            data[useHeaders[idx]] = value.text
            if idx == 1:
                data["link"] = value.find('a').get("href")
        output.append(data)

    cmn.dictListToCSV(output, useHeaders + ["link"], folderPath / f"{outputFilenames[writingFile]}.csv")