from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime

def getPropertyValue(node, value, default=""):
    result = node.find(value)
    if result is not None:
        return result.text
    return default

directory = Path("./output")
fronts = []

for file in directory.iterdir():
    with open(file) as fp:
        data = fp.read()

    record = {}
    soup = BeautifulSoup(data, "xml")

    articleData = soup.find("article-meta")

    title = articleData.find("article-title")
    record["title"] = title.contents[0].strip()

    species = title.find("italic")
    record["species"] = species.contents[0].strip()

    authors = []
    contributorData = articleData.find("contrib-group")
    for contributor in contributorData.find_all("contrib"):
        if "contrib-type" not in contributor.attrs or contributor.attrs["contrib-type"] != "author":
            continue

        nameInfo = contributor.find("name")
        names = nameInfo.find("given-names").text
        surname = nameInfo.find("surname").text

        author = {"name": names, "surname": surname}
        authors.append(author)

    record["authors"] = authors

    publications = []
    for pubDate in articleData.find_all("pub-date"):
        pubType = pubDate.attrs.get("pub-type", "")

        year = int(getPropertyValue(pubDate, "year", -1))
        month = int(getPropertyValue(pubDate, "month", -1))
        day = int(getPropertyValue(pubDate, "day", -1))

        date = "" # Needs to be improved
        if year >= 0:
            date += f"{year}"
            if month >= 0:
                date += f"-{month}"
                if day >= 0:
                    date += f"-{day}"
            
        publications.append({"pubType": pubType, "pubDate": date})

    record["publication"] = publications
    print(record)
    break

# for idx, front in enumerate(front, start=1):
#     for idx2, front2 in enumerate(fronts[idx:], start=1):
#         print(idx, idx+idx2, front == front2)