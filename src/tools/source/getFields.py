import pandas as pd
import numpy as np
import json
from pathlib import Path
from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFile, StageFileStep
from lib.processing.mapping import Remapper, MapManager
import random
from lib.tools.logger import Logger

def _collectFields(stageFile: StageFile, dataStructure: dict, outputDir: Path, entryLimit: int, chunkSize: int) -> dict:
    subFolder = outputDir / "values"
    subFolder.mkdir(exist_ok=True)

    for idx, chunk in enumerate(stageFile.loadDataFrameIterator(chunkSize), start=1):
        print(f"Scanning chunk: {idx}", end='\r')

        for column in chunk.columns:
            seriesValues = [item.replace("\n", "").strip() for item in chunk[column].dropna().unique().tolist()] # Convert column to list of unique values
            with open(subFolder / f"{column}.txt", "+a") as fp:
                fp.write("\n".join(list(seriesValues)) + "\n")

    print("\nPicking values")
    for column in dataStructure:
        file = subFolder / f"{column}.txt"
        with open(file) as fp:
            values = fp.read().rstrip("\n").split("\n")

        values = set(values)
        values.discard("")
        values = list(values)
        
        if len(values) <= entryLimit or entryLimit <= 0:
            dataStructure[column]["Values"] = values
        else:
            dataStructure[column]["Values"] = random.sample(values, entryLimit)

        file.unlink()

    return dataStructure

def _collectRecords(stageFile: StageFile, dataStructure: dict, entryLimit: int, chunkSize: int, seed: int) -> dict:
    for idx, chunk in enumerate(stageFile.loadDataFrameIterator(chunkSize), start=1):
        print(f"Scanning chunk: {idx}", end='\r')
        sample = chunk.sample(n=entryLimit, random_state=seed)

        if idx == 1:
            df = sample
            continue

        df = pd.concat([df, chunk])
        emptyDF = df.isna().sum(axis=1)
        indexes = [idx for idx, _ in sorted(emptyDF.items(), key=lambda x: x[1])]
        df = df.loc[indexes[:entryLimit]]

    for column in dataStructure:
        dataStructure[column]["Values"] = df[column].tolist()
    
    return dataStructure

if __name__ == '__main__':
    parser = SourceArgParser(description="Get column names of preDwc files")
    parser.add_argument('-e', '--entries', type=int, default=50, help="Number of unique entries to get")
    parser.add_argument('-t', '--tsv', action="store_true", help="Output as tsv instead")
    parser.add_argument('-u', '--uniques', action="store_true", help="Find unique values only, ignoring record")
    parser.add_argument('-c', '--chunksize', type=int, default=128, help="File chunk size to read at a time")
    parser.add_argument('-s', '--seed', type=int, default=-1, help="Specify seed to run")

    sources, selectedFiles, overwrite, args = parser.parse_args()
    entryLimit = args.entries

    for source in sources:
        outputDir = source.getBaseDir() / "examples"
        if not outputDir.exists():
            outputDir.mkdir()

        extension = "tsv" if args.tsv else "json"
        source.prepareStage(StageFileStep.PRE_DWC)
        stageFile = source.getPreDWCFiles(selectedFiles)[0] # Should be singular stage file before DwC

        if not stageFile.filePath.exists():
            print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
            continue

        seed = args.seed if args.seed >= 0 else random.randrange(2**32 - 1) # Max value for pandas seed
        random.seed(seed)

        mapID, customMapID, customMapPath = source.systemManager.dwcProcessor.getMappingProperties()
        mapManager = MapManager(source.getBaseDir())
        maps = mapManager.loadMaps(mapID, customMapID, customMapPath, True)
        if not maps:
            Logger.error("No valid map files found")
            exit()

        remapper = Remapper(maps, source.location)

        columns = stageFile.getColumns()
        translationTable = remapper.buildTable(columns)
        dataStructure = {column: {"Maps to": [{"Event": mappedColumn.event, "Column": mappedColumn.colName} for mappedColumn in translationTable.getTranslation(column)], "Values": []} for column in columns}

        if args.uniques:
            Logger.info("Collecting fields...")
            data = _collectFields(stageFile, dataStructure, outputDir, args.entries, args.chunksize)
            output = outputDir / f"fieldExamples_{args.chunksize}_{seed}.{extension}"
        else:
            Logger.info("Collecting records...")
            data = _collectRecords(stageFile, dataStructure, args.entries, args.chunksize, seed)
            output = outputDir / f"recordExamples_{args.chunksize}_{seed}.{extension}"

        Logger.info(f"Writing to file {output}")
        if args.tsv:
            dfData = {k: v["Values"] + ["" for _ in range(entryLimit - len(v["Values"]))] for k, v in data.items()}
            df = pd.DataFrame.from_dict(dfData)
            df.index += 1 # Increment index so output is 1-indexed numbers
            df.to_csv(output, sep="\t", index_label="Example #")
        else:
            with open(output, 'w') as fp:
                json.dump(data, fp, indent=4)
