import pandas as pd
import json
import lib.dataframeFuncs as dff
from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFile, StageFileStep
from lib.processing.dwcMapping import Remapper
import random

def _collectFields(stageFile: StageFile, remapper: Remapper, entryLimit: int, chunkSize: int, samples: int) -> dict:
    chunkGen = dff.chunkGenerator(stageFile.filePath, chunkSize=chunkSize, sep=stageFile.separator, header=stageFile.firstRow, encoding=stageFile.encoding)

    data = {}
    for idx, chunk in enumerate(chunkGen):
        print(f"Scanning chunk: {idx}", end='\r')

        if idx == 0:
            remapper.createMappings(chunk.columns)
            data = {column: {"Maps to": [{"Event": mapEvent, "Column": mapColumn} for mapEvent, mapColumn in mapping], "Values": []} for column, mapping in remapper.mappedColumns.items()}

        for column in chunk.columns:
            columnValues = data[column]["Values"] # Reference pointer to values
            seriesValues = list(set(chunk[column].dropna().tolist())) # Convert column to list of unique values
        
            if len(seriesValues) <= samples:
                columnValues.extend(seriesValues)
                continue

            columnValues.extend(random.sample(seriesValues, samples))

    # Shuffle entries and take only up to entry limit
    for column, properties in data.items():
        random.shuffle(list(set(properties["Values"]))) # Shuffle items inplace
        properties["Values"] = properties["Values"][:entryLimit] # Only keep up to entry limit

    return data

def _collectRecords(stageFile: StageFile, remapper: Remapper, entryLimit: int, chunkSize: int, samples: int, seed: int) -> dict:
    chunkGen = dff.chunkGenerator(stageFile.filePath, chunkSize=chunkSize, sep=stageFile.separator, header=stageFile.firstRow, encoding=stageFile.encoding)

    data = {}
    dfSamples = []
    for idx, chunk in enumerate(chunkGen):
        print(f"Scanning chunk: {idx}", end='\r')
        dfSamples.append(chunk.sample(n=samples, random_state=seed))

    df = pd.concat(dfSamples)
    emptyDF = df.isna().sum(axis=1)
    indexes = [idx for idx, _ in sorted(emptyDF.items(), key=lambda x: x[1])]
    
    reducedDF = df.loc[indexes[:entryLimit]]
    remapper.createMappings(reducedDF.columns)

    data = {column: {"Maps to": [{"Event": mapEvent, "Column": mapColumn} for mapEvent, mapColumn in mapping], "Values": reducedDF[column].tolist()} for column, mapping in remapper.mappedColumns.items()}
    return data

if __name__ == '__main__':
    parser = SourceArgParser(description="Get column names of preDwc files")
    parser.add_argument('-e', '--entries', type=int, default=50, help="Number of unique entries to get")
    parser.add_argument('-t', '--tsv', action="store_true", help="Output as tsv instead")
    parser.add_argument('-u', '--uniques', action="store_true", help="Find unique values only, ignoring record")
    parser.add_argument('-c', '--chunksize', type=int, default=1024*1024, help="File chunk size to read at a time")
    parser.add_argument('-r', '--samples', type=int, default=0, help="Amount of random samples to take per chunk")
    parser.add_argument('-s', '--seed', type=int, default=-1, help="Specify seed to run")

    sources, selectedFiles, args = parser.parse_args()
    entryLimit = args.entries

    for source in sources:
        outputDir = source.getBaseDir() / "examples"
        if not outputDir.exists():
            outputDir.mkdir()

        extension = "tsv" if args.tsv else "json"
        source.prepareStage(StageFileStep.PRE_DWC)
        stageFiles = source.getPreDWCFiles(selectedFiles)
        remapper = source.systemManager.dwcProcessor.remapper

        for stageFile in stageFiles:
            if not stageFile.filePath.exists():
                print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
                continue

        if not stageFile.filePath.exists():
            print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
            continue

        samples = args.samples if args.samples > args.entries else args.entries # Number of samples must be greater than the number of entries
        seed = args.seed if args.seed >= 0 else random.randrange(2**32 - 1) # Max value for pandas seed
        random.seed(seed)

        if args.uniques:
            data = _collectFields(stageFile, remapper, args.entries, args.chunksize, samples)
            output = outputDir / f"fieldExamples_{args.chunksize}_{seed}.{extension}"
        else:
            data = _collectRecords(stageFile, remapper, args.entries, args.chunksize, samples, seed)
            output = outputDir / f"recordExamples_{args.chunksize}_{seed}.{extension}"

        print(f"Writing to file {output}")
        if args.tsv:
            dfData = {k: v["Values"] + ["" for _ in range(entryLimit - len(v["Values"]))] for k, v in data.items()}
            df = pd.DataFrame.from_dict(dfData)
            df.index += 1 # Increment index so output is 1-indexed numbers
            df.to_csv(output, sep="\t", index_label="Example #")
        else:
            with open(output, 'w') as fp:
                json.dump(data, fp, indent=4)
