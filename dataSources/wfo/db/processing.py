from pathlib import Path
import pandas as pd

def cleanup(inputFolder: Path, outputFilePath: Path):
    df = pd.read_csv(inputFolder / "classification.csv", sep="\t", encoding="iso-8859-1", low_memory=False)
    df["scientific_name"] = df["scientificName"] + df["scientificNameAuthorship"]
    df.to_csv(outputFilePath, index=False)
