from pathlib import Path
import pandas as pd

folderPath = Path("./sealifebase_csvs")

records = []
for idx, file in enumerate(folderPath.iterdir(), start=1):
    print(f"At file: {idx}", end="\r")
    df = pd.read_csv(file, dtype=object)
    numberedColumns = {f"Column #{idx}": column for idx, column in enumerate(df.columns, start=1)}
    record = {"filename": file.stem} | numberedColumns
    records.append(record)

df = pd.DataFrame.from_records(records)
df.to_csv(f"{folderPath.stem.split('_', 1)[0]}.csv", index=False)
print()