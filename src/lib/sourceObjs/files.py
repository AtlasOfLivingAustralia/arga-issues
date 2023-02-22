from lib.processing.processor import Processor
from lib.processing.augmentor import Augmentor
from lib.processing.dwcConverter import DWCConverter
import lib.processing.processingFuncs as pFuncs
import subprocess
from pathlib import Path

class File:
    def __init__(self, directoryPath: Path, fileName: Path):
        self.directoryPath = directoryPath
        self.fileName = fileName

        self.filePath = directoryPath / fileName

class DBFile(File):
    def __init__(self, url: str, directoryPath: Path, fileName: Path, processor: Processor):
        super().__init__(directoryPath, fileName)
        
        self.url = url
        self.processor = processor

    def __repr__(self):
        return self.fileName

    def download(self, overwrite: bool = False, user="", password=""):
        if self.filePath.exists() and not overwrite:
            print(f"Downloaded file {self.filePath.name} already exists")
            return

        if not self.filePath.parent.exists():
            self.filePath.parent.mkdir(parents=True, exist_ok=True)

        print(f"Attempting to download file: {self.filePath.name} from {self.url}")

        if user:
            subprocess.run(f"curl.exe {self.url} -o {self.filePath} --user {user}:{password}")
        else:
            subprocess.run(f"curl.exe {self.url} -o {self.filePath}")

    def process(self):
        self.processor.process()

    def getOutputs(self) -> list[Path]:
        return self.processor.getOutputFilePaths()

class PreDWCFile(File):
    def __init__(self, directoryPath: Path, fileName: Path, location: str, fileProperties: dict = {}, dwcProperties: dict = {}, enrichDBs: dict= []):
        super().__init__(directoryPath, fileName)

        self.location = location
        self.properties = fileProperties
        self.dwcProperties = dwcProperties
        self.enrichDBs = enrichDBs

        self.dwcFileName = f"{fileName.stem}-dwc.csv"

        self.separator = fileProperties.get("separator", ",")
        self.firstRow = fileProperties.get("firstrow", 0)

        self.converter = DWCConverter(directoryPath, self.dwcFileName, self.separator, self.firstRow)

        augmentSteps = dwcProperties.get("augment", None)
        self.augmentor = None if augmentSteps is None else Augmentor(augmentSteps)

    def convert(self):
        self.converter.applyTo(self.filePath, self.location, enrichDBs=self.enrichDBs, augmentor=self.augmentor)

    def getOutput(self):
        return self.dwcFileName

class OutputFile(File):
    def __init__(self, directoryPath, fileName):
        super().__init__(directoryPath, fileName)
