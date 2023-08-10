import requests
from xml.dom import minidom
from bs4 import BeautifulSoup
from pathlib import Path

# requestURL = "https://f1000research.com/extapi/search?q=*"
requestURL = "https://f1000research.com/extapi/search?q=R_TY:\"GENOME_NOTE\""
outputFolder = Path("./output")
response = requests.get(requestURL)

outputFiles = []
soup = BeautifulSoup(response.text, "xml")
for idx, doi in enumerate(soup.find_all("doi"), start=1):
    outputFile = Path("output.xml")
    # print(f"https://doi.org/{doi.text}")
    doiData = f"https://f1000research.com/extapi/article/xml?doi={doi.text}"
    response = requests.get(doiData)

    outputPath = outputFolder / f"output_{idx}.xml"
    with open(outputPath, "w") as fp:
        fp.write(response.text)
    outputFiles.append(outputPath)

    if idx >= 5:
        break
