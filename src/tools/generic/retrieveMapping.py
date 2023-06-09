import pandas as pd
import json
import lib.config as cfg
import numpy as np

def buildURL(docID: str, sheetID: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{docID}/export?format=csv&gid={sheetID}"

def getMappings(df: pd.DataFrame) -> dict:
    relevantColumns = ["Field Name"] + [col for col in df.columns if col.startswith("Event")]
    df = df[relevantColumns]
    
    mappings = {}
    for _, row in df.iterrows():
        options = []
        for colName, value in row.items():
            if colName == "Field Name":
                continue

            if not isinstance(value, str): # Ignore float/int
                continue

            if value in ("0", "1", "nan", "NaN", np.nan, np.NaN, np.NAN) or value.startswith("ARGA"):
                continue
            
            elements = [elem.strip() for elem in value.split(",")]
            options.extend([elem for elem in elements if elem not in options])
        
        mappings[row["Field Name"]] = options

    return mappings

if __name__ == "__main__":
    documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"

    sheetIDs = {
        "ncbi-refseq": 1264640954,
        "42bp-genomeArk": 84855374,
        "anemone-db": 286004534,
        "bold-tsv": 1099164815
    }

    for database, sheetID in sheetIDs.items():
        location, db = database.split("-")
        print(f"Creating map for {database}")

        df = pd.read_csv(buildURL(documentID, sheetID), keep_default_na=False)
        mappings = getMappings(df)

        mapFile = cfg.folderPaths.mapping / f"{location}.json"

        if not mapFile.exists():
            with open(mapFile, "w") as fp:
                json.dump(mappings, fp, indent=4)
            continue

        with open(mapFile) as fp:
            columnMap = json.load(fp)

        for keyword, names in mappings.items():
            if keyword not in columnMap:
                columnMap[keyword] = names
            else:
                columnMap[keyword].extend(names)

        with open(mapFile, "w") as fp:
            json.dump(columnMap, fp, indent=4)
