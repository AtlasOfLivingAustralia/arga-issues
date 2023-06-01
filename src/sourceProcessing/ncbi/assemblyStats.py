from pathlib import Path
import json
import pandas as pd

def combine(inputFolder: Path, outputFilePath: Path):
    entries = []

    for filePath in inputFolder.iterdir():
        with open(filePath) as fp:
            data = fp.read()

        data = data.text.rsplit("# ", 1)[-1] # Clean up everything before table
        data = data.split("\nPrimary Assembly", 1)[0] # Clean up everything after relevant rows
        data = {row[-2]: row[-1] for row in [line.split("\t") for line in data.split("\n")[1:]]}
        data["ftp_path"] = ""
        entries.append(data)

    df = pd.DataFrame.from_records(entries)
    df.to_csv(outputFilePath, index=False)