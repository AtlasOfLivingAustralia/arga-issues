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

def process(filePath, outputDir=None):
    if not isinstance(filePath, Path):
        filePath = Path(filePath)

    if outputDir is None:
        outputDir = filePath.parent

    extensions = [e.value for e in ExtractTypes]
    while filePath.suffix in extensions:
        extractType = ExtractTypes(filePath.suffix)
        outputFile = outputDir / filePath.stem
        
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract file or folder")
    parser.add_argument("filePath", help="Path to zipped file or folder to extract")
    args = parser.parse_args()

    path = Path(args.filePath)
    process(path)
