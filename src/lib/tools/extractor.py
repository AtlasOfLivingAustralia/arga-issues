import zipfile
import gzip
import shutil
import lzma
import tarfile
from pathlib import Path
from enum import Enum
from lib.tools.logger import Logger

class ExtractTypes(Enum):
    ZIP = ".zip"
    GZIP = ".gz"
    XZ = ".xz"
    TAR = ".tar"

class Extractor:
    extensions = [ext.value for ext in ExtractTypes]

    def __init__(self, outputDir: str = "", addSuffix: str = "", overwrite: bool = False):
        self.outputDir = outputDir
        self.addSuffix = addSuffix
        self.overwrite = overwrite

    def run(self, filePath: str):
        return self.extract(filePath, self.outputDir, self.addSuffix, self.overwrite)

    @staticmethod
    def extract(filePath: str, outputDir: str = "", addSuffix: str = "", overwrite: bool = False) -> Path | None:
        filePath: Path = Path(filePath)

        if not filePath.exists():
            Logger.warning(f"No file exists at path: {filePath}.")
            return None
        
        if not outputDir:
            outputDir: Path = filePath.parent
        
        while filePath.suffix in Extractor.extensions:
            outputFile = outputDir / Path(filePath.stem)
            if not outputFile.suffix and addSuffix: # No suffix leftover after next extract stage and provided suffix
                outputFile = outputFile.with_suffix(addSuffix)

            if outputFile.exists() and not overwrite:
                Logger.info(f"Output {outputFile.name} exists, Skipping extraction stage")
                filePath = outputFile
                continue

            extractType = ExtractTypes(filePath.suffix)

            try:
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
                        tar.extractall(outputFile, filter="data")
            except EOFError:
                outputFile.unlink()
                return None

            filePath = outputFile

        return filePath
