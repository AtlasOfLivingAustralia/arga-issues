from lib.processing.stageFile import StageFile
from pathlib import Path
import pandas as pd
import requests
from io import StringIO

def enrich(stageFile: StageFile, outputFilePath: Path):
    df = stageFile.loadDataFrame()
    ftpPath = "ftp_path"

    records = []
    for idx, row in df.iterrows():
        print(f"At row: {idx}", end="\r")
        link = row[ftpPath]
        assemblyStats = f"{link}/{link.rsplit('/', 1)[-1]}_assembly_stats.txt"
        data = requests.get(assemblyStats)
        table = data.text.rsplit("# ", 1)[-1]
        df2 = pd.read_csv(StringIO(table), sep="\t")
        df2 = df2.drop(df2[df2["unit-name"] != "all"].index)

        stats = pd.Series(df2.value.values, index=df2.statistic.values).to_dict()
        stats[ftpPath] = link
        records.append(stats)

    statsDF = pd.DataFrame.from_records(records)
    for column in statsDF.columns:
        if '-' in column:
            statsDF[column.replace("-", "_")] = statsDF[column].astype(int)
            statsDF.drop(column, axis=1, inplace=True)

    outDF = df.merge(statsDF, "left", on=ftpPath)
    print(f"\nWriting to file {outputFilePath}")
    outDF.to_csv(outputFilePath, index=False)
