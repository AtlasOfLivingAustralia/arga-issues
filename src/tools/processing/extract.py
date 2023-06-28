import zipfile
import gzip
import shutil
import lzma
import argparse
import tarfile
from pathlib import Path
from enum import Enum

class ExtractTypes(Enum):
    ZIP = ".zip"
    GZIP = ".gz"
    XZ = ".xz"
    TAR = ".tar"

def process(filePath: Path, outputDir: Path = None, addSuffix: str = "", overwrite: bool = False) -> Path:
    if not isinstance(filePath, Path):
        filePath = Path(filePath)

    if outputDir is None:
        outputDir = filePath.parent

    extensions = [e.value for e in ExtractTypes]
    while filePath.suffix in extensions:
        extractType = ExtractTypes(filePath.suffix)

        if not Path(filePath.stem).suffix and addSuffix: # No suffix leftover after next extract stage
            outputFile = outputDir / f"{filePath.stem}.{addSuffix}"
        else:
            outputFile = outputDir / filePath.stem

        if outputFile.exists() and not overwrite:
            print("Output file already exists.... Skipping extraction stage")
            break
        
        if extractType == ExtractTypes.ZIP:
            with zipfile.ZipFile(filePath, 'r') as zip:
                zip.extractall(outputFile)

        if extractType == ExtractTypes.GZIP:
            with gzip.open(filePath, 'rb') as gz, open(outputFile, 'wb') as fp:
                shutil.copyfileobj(gz, fp)

        if extractType == ExtractTypes.XZ:
            with lzma.open(filePath) as xz, open(outputFile, 'wb') as fp:
                shutil.copyfileobj(xz, fp)

        if extractType == ExtractTypes.TAR:
            with tarfile.open(filePath, 'r') as tar:
                tar.extractall(outputFile)

        filePath = outputFile

    return filePath

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract file or folder")
    parser.add_argument("filePath", help="Path to zipped file or folder to extract")
    args = parser.parse_args()

    path = Path(args.filePath)
    process(path)
