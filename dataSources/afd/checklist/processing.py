import requests
import json
from pathlib import Path
import pandas as pd
from io import BytesIO
from lib.tools.bigFileWriter import BigFileWriter

class EntryData:
    def __init__(self, rawData: dict):
        data = rawData.get("data", {})
        self.title = data["title"]

        metadata = rawData.get("metadata", {})
        self.key = metadata["nameKey"]
        self.ident = metadata["rank-key"]
        self.description = metadata["rank-desc"]
        self.prefix = metadata["rank-prefix"]
        self.rank = metadata["rank-with-prefix"]
        self.higherTaxon = metadata["is-higher-taxon"]
        self.specialTaxon = metadata["is-special-taxon"]
        
        self.state = rawData.get("state", "")
        self.children = [EntryData(child) for child in rawData.get("children", [])]

def retrieve(outputFilePath: Path):
    writer = BigFileWriter(outputFilePath, "sections", "section")

    checklist = "https://biodiversity.org.au/afd/mainchecklist"
    response = requests.get(checklist).text

    start = response.find("[", response.find("var data ="))
    end = response.rfind("]", start, response.rfind("var checklist;")) + 1

    kingdomData = [EntryData(kingdom) for kingdom in json.loads(response[start:end])]
    downloadChildCSVs(kingdomData, writer)

    writer.oneFile(False)

def downloadChildCSVs(entryData: list[EntryData], writer: BigFileWriter) -> None:
    for entry in entryData:
        content = getCSVData(entry.key)
        if content is not None:
            writer.writeDF(buildDF(content))
            print(f"Wrote file #{len(writer.writtenFiles)}", end="\r")
            continue

        # Content was too large to download
        children = entry.children if entry.children else findChildren(entry.key)
        downloadChildCSVs(children, writer)

def getCSVData(taxonKey: str) -> str | None:
    url = f"https://biodiversity.org.au/afd/taxa/{taxonKey}/names/csv/{taxonKey}.csv"
    response = requests.get(url)
    if not response.headers["Content-Type"].startswith("application/csv"):
        return None
    
    return response.content

def buildDF(content: bytes) -> pd.DataFrame:
    return pd.read_csv(BytesIO(content), encoding="iso-8859-1")

def findChildren(taxonKey: str) -> list[EntryData]:
    response = requests.get(f"https://biodiversity.org.au/afd/taxa/{taxonKey}/checklist-subtaxa.json")
    try:
        children = response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"Unable to get children for item: {taxonKey}")
        return []

    return [EntryData(child) for child in children]

def cleanup(filePath: Path, outputFilePath: Path) -> None:
    df = pd.read_csv(filePath, dtype=object)

    df = df.drop([
        "CAVS_CODE",
        "CAAB_CODE",
        "PUB_PUB_AUTHOR",
        "PUB_PUB_YEAR",
        "PUB_PUB_TITLE",
        "PUB_PUB_PAGES",
        "PUB_PUB_PARENT_BOOK_TITLE",
        "PUB_PUB_PARENT_JOURNAL_TITLE",
        "PUB_PUB_PARENT_ARTICLE_TITLE",
        "PUB_PUB_PUBLICATION_DATE",
        "PUB_PUB_PUBLISHER",
        "PUB_PUB_FORMATTED",
        "PUB_PUB_QUALIFICATION",
        "PUB_PUB_TYPE",
        "PUBLICATION_GUID",
        "PUBLICATION_LAST_UPDATE",
        "PARENT_PUBLICATION_GUID"
    ], axis=1)

    df = df.rename(columns={
        "NAME_TYPE": "taxonomic_status",
        "RANK": "taxon_rank",
        "NAME_GUID": "name_id",
        "TAXON_GUID": "taxon_id",
        "TAXON_LAST_UPDATE": "updated_at",
        "PARENT_TAXON_GUID": "parent_taxon_id"
    })

    df = df.rename(columns={column: column.lower() for column in df.columns})

    datasetID = "ARGA:TL:0001000"
    df["dataset_id"] = datasetID
    df["entity_id"] = f"{datasetID};" + df["scientific_name"]
    df["nomenclatural_code"] = "ICZN"
    df["created_at"] = ""

    df = df.fillna("")
    df["year"] = df["year"].apply(lambda x: x.split(".")[0])
    df["canonical_genus"] = df.apply(lambda row: f"{row['genus']} ({row['subgenus']})" if row["subgenus"] else row["genus"], axis=1)
    df["canonical_name"] = df.apply(lambda row: f"{row['canonical_genus']} {row['species']}" if row["taxon_rank"] == "Species" else f"{row['canonical_genus']} {row['species']} {row['subspecies']}" if row["taxon_rank"] == "subspecies" else row["names_various"], axis=1)
    df["authorship"] = df.apply(lambda row: f"{row['author']}, {row['year']}" if row["author"] not in ("", "NaN", "nan") else "", axis=1)
    df["scientific_name_authorship"] = df.apply(lambda row: f"({row['authorship']})" if row['orig_combination'] == 'N' and row["authorship"] not in ("", "NaN", "nan") else row["authorship"], axis=1)
    
    df.to_csv(outputFilePath, index=False)

def addParents(filePath: Path, outputFilePath: Path) -> None:
    df = pd.read_csv(filePath, dtype=object)
    parentDF = df[df["taxonomic_status"] == "Valid Name"]

    remap = {
        "taxon_id": "parent_taxon_id",
        "scientific_name": "parent_taxon",
        "taxon_rank": "parent_rank"
    }

    parentDF = parentDF[remap.keys()]
    parentDF = parentDF.rename(columns=remap)
    parentDF = parentDF[parentDF["parent_taxon_id"].notna()]

    df = df.merge(parentDF, "left", "parent_taxon_id")

    parentRemap = {
        "parent_taxon_id": "accepted_usage_taxon_id",
        "parent_taxon": "accepted_usage_taxon",
        "parent_rank": "accepted_usage_taxon_rank"
    }

    parentDF = parentDF.rename(columns=parentRemap)

    df = df.merge(parentDF, "left", left_on="taxon_id", right_on="accepted_usage_taxon_id")
    df = df[df["taxonomic_status"].isin(("Valid Name", "Synonym"))]
    df.to_csv(outputFilePath, index=False)
