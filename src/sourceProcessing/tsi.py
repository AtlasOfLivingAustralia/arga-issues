import pandas as pd
from pathlib import Path

def koalaAugment(df: pd.DataFrame) -> pd.DataFrame:
    df["collections", "scientificName"] = "Phascolarctos cinereus"
    return df

def mouseAugment(df: pd.DataFrame) -> pd.DataFrame:
    df["collections", "scientificName"] = "Mastacomys fuscus"
    df["collections", "bibliographicCitation"] = "This data was produced by Museums Victoria as part of the Genomic Analysis of Broad-toothed Rats project with funding from the Victorian and Australian Government’s Bushfire Biodiversity Response and Recovery program, further support was provided by the University of Sydney, Amazon Web Services Open Data Sets, and the Australian Genome Research Facility." 
    return df

def manualCollect(sheetsID: str, specificSheetID: str, outputFilePath: Path) -> None:
    sheetURL = f"https://docs.google.com/spreadsheets/d/{sheetsID}/export?format=csv&gid={specificSheetID}"
    df = pd.read_csv(sheetURL, keep_default_na=False, skiprows=[x for x in range(23) if x != 4])
    df = df.drop(["ARGA_DwC"], axis=1)
    df.to_csv(outputFilePath, index=False)
