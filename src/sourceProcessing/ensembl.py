import json
import pandas as pd
import requests
from pathlib import Path
from lib.tools.downloader import Downloader
import lib.tools.zipping as zp

def convert(filePath: Path, outputFilePath: Path) -> None:
    with open(filePath) as fp:
        data = json.load(fp)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputFilePath, index=False)

def download(url: str, outputDir: Path, overwrite: bool = False) -> Path:
    localFile = Path(outputDir / f"{url.rsplit('/', 1)[-1]}")

    if localFile.exists() and overwrite:
        localFile.unlink()

        dl = Downloader()
        success = dl.download(url, localFile, verbose=True)

        if not success:
            return None

    if zp.canBeExtracted(localFile) and overwrite:
        return zp.extract(localFile)
    
    return localFile

def speciesDownload(outputFilePath: Path) -> None:
    url = "https://rest.ensembl.org/info/species?"
    request = requests.get(url, headers={ "Content-Type" : "application/json"})
 
    if not request.ok:
        request.raise_for_status()
        return
    
    data = request.json()
    df = pd.DataFrame.from_records(data["species"])
    df.to_csv(outputFilePath, index=False)

def flatten(filePath: Path, outputFilePath: Path) -> None:
    with open(filePath) as fp:
        data = json.load(fp)

    records = []
    for record in data:
        organism = record.pop("organism")
        record["organism_name"] = organism.pop("name")

        assembly = record.pop("assembly")
        assembly.pop("sequences")

        release = record.pop("data_release")

        records.append(organism | assembly | release | record)

    pd.DataFrame.from_records(records).to_csv(outputFilePath, index=False)

def enrich(filePath: Path, outputFilePath: Path) -> None:
    df = pd.read_csv(filePath, sep="\t", dtype=object, index_col=False)

    baseURL = "http://ftp.ensemblgenomes.org/pub/protists/current/mysql/"
    outputFolder = Path(outputFilePath.parent / "enrichFiles")
    outputFolder.mkdir(exist_ok=True)

    records = []
    for _, row in df.iterrows():
        db = row["core_db"]
        id = row["species_id"]

        tempBase = baseURL + db + "/"
        metaURL = tempBase + "meta.txt.gz"
        statsURL = tempBase + "genome_statistics.txt.gz"

        meta = download(metaURL, outputFolder)
        metaDF = pd.read_csv(meta, header=None, sep="\t", index_col=0, names=["id", "column", "value"])
        
        relevant = metaDF[metaDF["id"] == id]
        relevantData = dict(zip(relevant.column, relevant.value))
        record = {"name": row["#name"]} | {key.replace(".", "_"): value for key, value in relevantData.items()}

        stats = download(statsURL, outputFolder)
        statsDF = pd.read_csv(stats, header=None, sep="\t", index_col=0, names=["column", "value", "id", "n", "timestamp"])
        relevant = statsDF[statsDF["id"] == id]
        relevantData = dict(zip(relevant.column, relevant.value))
        record |= relevantData

        records.append(record)

    enrichDF = pd.DataFrame.from_records(records)
    df = df.merge(enrichDF, how="outer", left_on="#name", right_on="name")
    df.to_csv(outputFilePath, index=False)

def combine(metadataPath: Path, statsPath: Path, outputFilePath: Path) -> None:
    metadata = pd.read_csv(metadataPath)
    stats = pd.read_csv(statsPath)

    uniqueColumns = stats.columns.difference(metadata.columns)
    pd.merge(metadata, stats[uniqueColumns], how="outer", left_on="display_name", right_on="#name").to_csv(outputFilePath, index=False)
