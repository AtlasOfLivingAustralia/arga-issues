from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime

def getPropertyValue(node, value, default=""):
    result = node.find(value)
    if result is not None:
        return result.text
    return default

def parse(data: str) -> dict:
    record = {}
    soup = BeautifulSoup(data, "xml")

    articleData = soup.find("article-meta")

    title = articleData.find("article-title")
    if title is not None:
        record["title"] = title.contents[0].strip()

    species = title.find("italic")
    if species is not None:
        record["species"] = species.contents[0].strip()

    authors = []
    contributorData = articleData.find("contrib-group")

    affiliations = {}
    for affiliation in contributorData.find_all("aff"):
        affiliations[affiliation.attrs["id"]] = affiliation.contents[-1]

    for contributor in contributorData.find_all("contrib"):
        if "contrib-type" not in contributor.attrs or contributor.attrs["contrib-type"] != "author":
            continue

        nameInfo = contributor.find("name")
        names = nameInfo.find("given-names").text
        surname = nameInfo.find("surname").text

        affils = []
        for ref in contributor.find_all("xref"):
            if ref.attrs["ref-type"] != "aff" or "rid" not in ref.attrs:
                continue

            affils.append(affiliations.get(ref.attrs["rid"], ""))

        author = {"name": names, "surname": surname, "affiliations": affils}
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

    refs = []
    references = soup.find("ref-list")
    for reference in references.find_all("ref"):
        ref = {}

        citation = reference.find("mixed-citation")
        ref["type"] = citation.attrs["publication-type"]

        pubID = reference.find("pub-id")
        if pubID is not None and "pub-id-type" in pubID.attrs:
            reference[pubID.attrs["pub-id-type"]] = pubID.text

        authors = []
        authorList = reference.find_all("name")
        for auth in authorList:
            name = auth.find("given-names")
            surname = auth.find("surname")

            name = name.text if name is not None else ""
            surname = surname.text if surname is not None else ""
            
            authors.append(f"{name} {surname}".strip())
        ref["authors"] = authors

        refs.append(ref)

    record["references"] = refs
    return record
