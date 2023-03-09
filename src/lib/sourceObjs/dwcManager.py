from lib.sourceObjs.fileManager import StageFile
from lib.processing.processor import DWCProcessor
from pathlib import Path

class DWCStageFile:
    def __init__(self, preDwCFile: StageFile, processor: DWCProcessor, outputDir: Path):
        self.parent = preDwCFile
        self.processor = processor

        self.filePath = outputDir / f"{self.parent.filePath.stem}-dwc.csv"

    def create(self):
        if self.filePath.exists():
            print(f"{str(self.filePath)} already exists, skipping creation")
            return
        
        self.parent.create()
        self.filePath.parent.mkdir(parents=True, exist_ok=True)

        self.processor.process(self.parent.filePath, self.filePath, self.parent.separator, self.parent.firstRow, self.parent.encoding)

class DWCManager:
    def __init__(self, prefix: str, dwcProperties: dict, enrichDBs: dict, outputDir: Path):
        self.prefix = prefix
        self.dwcProperties = dwcProperties
        self.enrichDBs = enrichDBs
        self.files = []

        self.processor = DWCProcessor(prefix, dwcProperties, enrichDBs, outputDir)

    def getFiles(self):
        return self.files

    def addFiles(self, preDwCFiles: list[StageFile]):
        for file in preDwCFiles:
            stageFile = DWCStageFile(file, self.processor)
            self.files.append(stageFile)

    def createAll(self):
        for file in self.files:
            file.create()
