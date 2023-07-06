from pathlib import Path
from sourceProcessing.ncbi.flatfileParser import FlatFileParser
from lib.tools.subfileWriter import Writer
from lib.tools.extractor import Extractor
import pandas as pd

def combine(folderPath: Path, outputFilePath: Path):
    writer = Writer(outputFilePath.parent, "seqChunks", "chunk")
    extractor = Extractor(outputFilePath.parent)

    flatfileParser = FlatFileParser()
    records = []
    verbose = True

    for filePath in folderPath.iterdir():
        if verbose:
            print(f"Extracting file {filePath}")
            
        extractedFile = extractor.run(filePath)

        if verbose:
            print(f"Parsing file {extractedFile}")

        records = flatfileParser.parse(extractedFile, verbose)
        df = pd.DataFrame.from_records(records)
        writer.writeDF(df)

        # Delete extracted file after processing to save space
        extractedFile.unlink()

    writer.oneFile(outputFilePath)
