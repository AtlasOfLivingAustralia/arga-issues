import zipfile
import gzip
import shutil
import lzma
import tarfile
from pathlib import Path
from enum import Enum

class ExtractTypes(Enum):
    ZIP = ".zip"
    GZIP = ".gz"
    XZ = ".xz"
    TAR = ".tar"

class Extractor:
    extensions = [ext.value for ext in ExtractTypes]

    @staticmethod
    def extract(filePath: str, outputDir: str = "", addSuffix: str = "", overwrite: bool = False) -> Path | None:
        filePath: Path = Path(filePath)

        if not filePath.exists():
            print(f"No file exists at path: {filePath}.")
            return None
        
        if not outputDir:
            outputDir: Path = filePath.parent
        
        while filePath.suffix in Extractor.extensions:
            outputFile = outputDir / Path(filePath.stem)
            if not outputFile.suffix and addSuffix: # No suffix leftover after next extract stage and provided suffix
                outputFile = outputFile.with_suffix(addSuffix)

            if outputFile.exists() and not overwrite:
                print("Output file already exists.... Skipping extraction stage")
                filePath = outputFile
                continue

            extractType = ExtractTypes(filePath.suffix)

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
