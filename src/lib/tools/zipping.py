import zipfile
import gzip
import shutil
import lzma
import tarfile
from pathlib import Path
from lib.tools.logger import Logger
from enum import Enum
import os

class Extension(Enum):
    ZIP = ".zip"
    GZIP = ".gz"
    XZ = ".xz"
    TAR = ".tar"

class RepeatExtractor:
    def __init__(self, outputDir: str = "", addSuffix: str = "", overwrite: bool = False):
        self.outputDir = outputDir
        self.addSuffix = addSuffix
        self.overwrite = overwrite

    def extract(self, filePath: str) -> Path | None:
        return extract(filePath, self.outputDir, self.addSuffix, self.overwrite)

def extract(filePath: Path, outputDir: Path = None, addSuffix: str = "", overwrite: bool = False) -> Path | None:
    if not filePath.exists():
        Logger.warning(f"No file exists at path: {filePath}.")
        return None
    
    if outputDir is None:
        outputDir = filePath.parent
    
    while filePath.suffix in Extension._value2member_map_:
        outputFile = outputDir / Path(filePath.stem)
        if not outputFile.suffix and addSuffix: # No suffix leftover after next extract stage and provided suffix
            outputFile = outputFile.with_suffix(addSuffix)

        if outputFile.exists() and not overwrite:
            Logger.info(f"Output {outputFile.name} exists, Skipping extraction stage")
            filePath = outputFile
            continue

        extractType = Extension(filePath.suffix)

        try:
            if extractType == Extension.ZIP:
                with zipfile.ZipFile(filePath, 'r') as zip:
                    zip.extractall(outputFile)

            if extractType == Extension.GZIP:
                with gzip.open(filePath, 'rb') as gz, open(outputFile, 'wb') as fp:
                    shutil.copyfileobj(gz, fp)

            if extractType == Extension.XZ:
                with lzma.open(filePath) as xz, open(outputFile, 'wb') as fp:
                    shutil.copyfileobj(xz, fp)

            if extractType == Extension.TAR:
                with tarfile.open(filePath, 'r') as tar:
                    tar.extractall(outputFile, filter="data")
        except EOFError:
            outputFile.unlink()
            return None

        filePath = outputFile

    return filePath

def compress(filePath: Path, outputDir: Path = None, zipName: str = None) -> Path | None:

    def compressFolder(folderPath: Path, parentFolder: Path, fp: zipfile.ZipFile):
        for item in folderPath.iterdir():
            itemPath = Path(parentFolder, item.name)
            if item.is_file():
                fp.write(item, itemPath)
            else:
                compressFolder(item, itemPath, fp)

    if zipName is None:
        zipName = filePath.stem
    if outputDir is None:
        outputDir = filePath.absolute().parent

    outputFile = outputDir / f"{zipName}.zip"

    with zipfile.ZipFile(outputFile, "w", zipfile.ZIP_DEFLATED) as zipfp:
        if filePath.is_file:
            zipfp.write(filePath, outputFile.stem)
        else:
            compressFolder(filePath, outputFile.stem, zipfp)
    
    return outputFile

def canBeExtracted(filePath: Path) -> bool:
    return filePath.suffix in Extension._value2member_map_