from lib.processing.stageFile import StageFile
from pathlib import Path
import pandas as pd
import requests
from io import StringIO
import concurrent.futures

def getTxT(ftpPath: str) -> dict:
    assemblyStats = f"{ftpPath}/{ftpPath.rsplit('/', 1)[-1]}_assembly_stats.txt"
    data = requests.get(assemblyStats)
    data = data.text.rsplit("# ", 1)[-1] # Clean up everything before table

    table = data.split("\nPrimary Assembly", 1)[0] # Clean up everything after relevant rows
    stats = {row[-2]: row[-1] for row in [line.split("\t") for line in table.split("\n")[1:]]}

    # df2 = pd.read_csv(StringIO(data), sep="\t")
    # df2 = df2.drop(df2[df2["unit-name"] != "all"].index)
    # stats = pd.Series(df2.value.values, index=df2.statistic.values).to_dict()

    stats["ftp_path"] = ftpPath
    return stats

def enrich(stageFile: StageFile, outputFilePath: Path):
    df = stageFile.loadDataFrame()

    records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(getTxT, row["ftp_path"]) for _, row in df.iterrows()]
        for idx, future in enumerate(concurrent.futures.as_completed(futures)):
            print(f"At row: {idx}", end="\r")
            stats = future.result()
            records.append(stats)

    statsDF = pd.DataFrame.from_records(records)
    for column in statsDF.columns:
        if '-' in column:
            statsDF[column.replace("-", "_")] = statsDF[column].astype(int)
            statsDF.drop(column, axis=1, inplace=True)

    outDF = df.merge(statsDF, "left", on="ftp_path")
    print(f"\nWriting to file {outputFilePath}")
    outDF.to_csv(outputFilePath, index=False)
