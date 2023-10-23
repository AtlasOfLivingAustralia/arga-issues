import pandas as pd
import lib.commonFuncs as cmn
import lib.dataframeFuncs as dff
from pathlib import Path
from lib.tools.subfileWriter import Writer
import concurrent.futures
from sourceProcessing.ncbi.flatfileParser import FlatFileParser
from lib.tools.subfileWriter import Writer
from lib.tools.extractor import Extractor

def splitLine(line, endingDivider=True):
    cleanLine = line.rstrip('\n').rstrip()
    if endingDivider:
        cleanLine = cleanLine.rstrip('|')
    return [element.strip() for element in cleanLine.split('|')]

def compileBiocollections(fileDir, outputFilePath):
    collCodes = fileDir / "Collection_codes.txt"
    instCodes = fileDir /"Institution_codes.txt"
    uInstCodes = fileDir / "Unique_institution_codes.txt"

    for ref, filePath in enumerate((collCodes, instCodes, uInstCodes)):
        data = []

        with open(filePath, encoding='utf-8') as fp:
            line = fp.readline()
            headers = splitLine(line)
            line = fp.readline() # Blank line 2 in every file, call again
            line = fp.readline()
            while line:
                data.append(splitLine(line, True))
                line = fp.readline()

        # cull extra data that doesn't map to a header
        df = pd.DataFrame([line[:len(headers)] for line in data], columns=headers) 

        if ref == 0:
            output = df.copy()
        else:
            output = pd.merge(output, df, 'left')

    output.dropna(how='all', axis=1, inplace=True)
    output.to_csv(outputFilePath, index=False)

def augmentBiosample(df):
    return dff.splitField(df, "ncbi_lat long", cmn.latlongToDecimal, ["decimalLatitude", "decimalLongitude"])

def parseStats(filePath: Path) -> pd.DataFrame | None:
    with open(filePath, encoding="utf-8") as fp:
        data = fp.read()

    splitData = data.rsplit("#", 1)
    if len(splitData) == 1: # No # found, error reading file
        return None
    
    info, table = splitData

    # Info parsing
    infoData = {}
    splitAssemblyInfo = info.split("##", 1)
    assemblyStats = splitAssemblyInfo[0]

    for stat in assemblyStats.split("\n"):
        if ":" not in stat: # Not a key-value pair
            continue

        key, value = stat.split(":", 1)
        infoData[key.strip("# ")] = value.strip("\t").strip(" ")

    # Table parsing
    prevInfo = [None, None, None, None]
    tableData = []

    for row in table.split("\n")[1:]: # Skip header row
        rowValues = row.split("\t")
        if len(rowValues) != 6: # Expect 6 columns
            continue

        groupInfo = rowValues[:4]

        if groupInfo != prevInfo: # New row
            tableData.append({"unit-name": rowValues[0], "molecule-name": rowValues[1], "molecule-type/loc": rowValues[2], "sequence-type": rowValues[3]})
            prevInfo = groupInfo

        tableData[-1][rowValues[4]] = rowValues[5]

    df = pd.DataFrame.from_records(tableData)
    for key, value in infoData.items(): # Add info to tabular data
        df[key] = value

    return df

def compileAssemblyStats(inputFolder: Path, outputFilePath: Path):
    writer = Writer(outputFilePath.parent, "assemblySections", "section")

    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        futures = (executor.submit(parseStats, filePath) for filePath in inputFolder.iterdir())
        for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            print(f"At entry: {idx}", end="\r")

            df = future.result()
            if df is not None:
                writer.writeDF(df)

    print()
    writer.oneFile(outputFilePath)

def compileNucleotide(folderPath: Path, outputFilePath: Path):
    writer = Writer(outputFilePath.parent, "seqChunks", "chunk")
    extractor = Extractor(outputFilePath.parent)

    flatfileParser = FlatFileParser()
    records = []
    verbose = True

    for filePath in folderPath.iterdir():
        if verbose:
            print(f"Extracting file {filePath}")
            
        extractedFile = extractor.run(filePath)
        if extractedFile is None:
            print("Failed to extract file, skipping")
            continue

        if verbose:
            print(f"Parsing file {extractedFile}")

        records = flatfileParser.parse(extractedFile, verbose)
        df = pd.DataFrame.from_records(records)
        writer.writeDF(df)

        # Delete extracted file after processing to save space
        extractedFile.unlink()

    writer.oneFile(outputFilePath)
