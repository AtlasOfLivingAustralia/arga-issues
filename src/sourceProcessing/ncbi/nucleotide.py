from pathlib import Path
from sourceProcessing.ncbi.flatfileParser import FlatFileParser
from lib.subfileWriter import Writer
from tools.processing import extract
import pandas as pd

def combine(folderPath: Path, outputFilePath: Path):
    writer = Writer(folderPath, "seqChunks", "chunk")
    flatfileParser = FlatFileParser()
    records = []
    verbose = True

    for filePath in folderPath.iterdir():
        if verbose:
            print(f"Extracting file {filePath}")
            
        extractedFile = extract.process(filePath, outputFilePath.parent)

        if verbose:
            print(f"Parsing file {extractedFile}")

        records = flatfileParser.parse(extractedFile, verbose)
        df = pd.DataFrame.from_records(records)
        writer.writeDF(df)

        # Delete extracted file after processing to save space
        extractedFile.unlink()

    writer.oneFile(outputFilePath)
