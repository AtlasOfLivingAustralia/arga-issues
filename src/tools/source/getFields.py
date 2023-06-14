import pandas as pd
import json
import lib.dataframeFuncs as dff
from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFile
from lib.remapper import Remapper
import random

def collectFields(stageFile: StageFile, location: str, entryLimit: int, chunkSize: int = 1024 * 1024) -> dict:
    random.seed()

    remapper = Remapper(location)
    chunkGen = dff.chunkGenerator(stageFile.filePath, chunkSize, stageFile.separator, stageFile.firstRow)

    data = {}
    for idx, chunk in enumerate(chunkGen):
        print(f"Scanning chunk: {idx}", end='\r')

        if idx == 0:
            mappings = remapper.createMappings(chunk.columns)
            data = {column: {"Maps to": mappings[column], "values": []} for column in chunk.columns}

        for column in chunk.columns:
            columnValues = data[column]["values"] # Reference pointer to values
            seriesValues = list(set(chunk[column].dropna().tolist())) # Convert column to list of unique values
        
            if len(seriesValues) <= entryLimit:
                columnValues.extend(seriesValues)
                continue

            columnValues.extend(random.sample(seriesValues, entryLimit))

    # Shuffle entries and take only up to entry limit if required
    for column, properties in data.items():
        if len(properties["values"]) > entryLimit:
            properties["values"] = random.shuffle(list(set(properties["values"])))[:entryLimit]

    return data

if __name__ == '__main__':
    parser = SourceArgParser(description="Get column names of preDwc files")
    parser.add_argument('-e', '--entries', type=int, default=10, help="Number of unique entries to get")
    parser.add_argument('-t', '--tsv', action="store_true", help="Output as tsv instead")
    
    sources, args = parser.parse_args()
    entryLimit = args.entries

    for source in sources:
        outputDir = source.getBaseDir()
        extension = "tsv" if args.tsv else "json"
        output = outputDir / f"fieldExamples.{extension}"

        if output.exists() and args.overwrite <= 0:
            print(f"Output file {output} already exists, please run with overwrite flag (-o) to overwrite")
            continue

        stageFiles = source.getPreDWCFiles(args.filenums)
        for stageFile in stageFiles:
            if not stageFile.filePath.exists():
                print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
                continue

        if not stageFile.filePath.exists():
            print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
            continue

        data = collectFields(stageFile, source.location, args.entries)
            
        print(f"Writing to file {output}")
        if args.tsv:
            dfData = {k: v["values"] + ["" for _ in range(entryLimit - len(v["values"]))] for k, v in data.items()}
            df = pd.DataFrame.from_dict(dfData)
            df.index += 1 # Increment index so output is 1-indexed numbers
            df.to_csv(output, sep="\t", index_label="Example #")
        else:
            with open(output, 'w') as fp:
                json.dump(data, fp, indent=4)
