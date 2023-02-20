import zipfile
import gzip
import shutil
import lzma
import argparse
from pathlib import Path
from enum import Enum

class ExtractTypes(Enum):
    ZIP = ".zip"
    GZIP = ".gz"
    XZ = ".xz"

def process(filePath):
    if not isinstance(filePath, Path):
        filePath = Path(filePath)

    extensions = [e.value for e in ExtractTypes]
    while filePath.suffix in extensions:
        extractType = ExtractTypes(filePath.suffix)
        outputFile = filePath.parent / filePath.stem
        
        if extractType == ExtractTypes.ZIP:
            with zipfile.ZipFile(filePath, 'r') as zip:
                zip.extractall()

        if extractType == ExtractTypes.GZIP:
            with gzip.open(filePath, 'rb') as gz, open(outputFile, 'wb') as fp:
                shutil.copyfileobj(gz, fp)

        if extractType == ExtractTypes.XZ:
            print(filePath.stem)
            with lzma.open(filePath) as xz, open(outputFile, 'wb') as fp:
                shutil.copyfileobj(xz, fp)

        filePath = outputFile

    return filePath

    # # while filePath.suffix in 
    # if filePath.suffix == '.gz':
    #     if Path(filePath.stem).suffix: # If there is a suffix on file after un-gzipping
    #         newFilename = filePath.stem
    #     else:
    #         newFilename = f"{filePath.stem}.csv"

    #     newfilePath = filePath.parent / newFilename

    #     with gzip.open(filePath, 'rb') as gz, open(newfilePath, 'wb') as fp:
    #         shutil.copyfileobj(gz, fp)

    #     filePath = newfilePath

    # if filePath.suffix == '.zip':
    #     with zipfile.ZipFile(filePath, 'r') as zip:
    #         zip.extractall(extractLocation)

    # return filePath

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract file or folder")
    parser.add_argument("filePath", help="Path to zipped file or folder to extract")
    args = parser.parse_args()

    path = Path(args.filePath)
    process(path)
