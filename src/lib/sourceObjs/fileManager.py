from pathlib import Path
from enum import Enum
from lib.processing.processor import FileProcessor, DWCProcessor

class FileStage(Enum):
    RAW       = 0
    PROCESSED = 1
    COMBINED  = 2
    PRE_DWC   = 3
    DWC       = 4

class StageFile:
    def __init__(self, filePath: Path, fileProperties: dict, processor: FileProcessor, parents: list['StageFile'] = []):
        self.filePath = filePath
        self.fileProperties = fileProperties
        self.processor = processor
        self.parents = parents

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

    def getFilePath(self):
        return self.filePath
    
    def create(self, overwrite: bool = False) -> None:
        if self.filePath.exists() and not overwrite:
            print(f"{self.filePath} already exists and not overwriting, skipping creation")
            return
        
        for parent in self.parents:
            parent.create()

        self.filePath.parent.mkdir(parents=True, exist_ok=True)
        self.processor.process(overwrite)

class DWCStageFile:
    def __init__(self, preDwCFile: StageFile, processor: DWCProcessor):
        self.parent = preDwCFile
        self.separator = preDwCFile.separator
        self.firstRow = preDwCFile.firstRow
        self.encoding = preDwCFile.encoding

        self.processor = processor
        self.filePath = processor.outputDir / f"{self.parent.filePath.stem}-dwc.csv"

    def getFilePath(self):
        return self.filePath

    def create(self, overwrite: bool = False) -> None:
        if self.filePath.exists() and not overwrite:
            print(f"{self.filePath} already exists and not overwriting, skipping creation")
            return
        
        self.parent.create()
        self.filePath.parent.mkdir(parents=True, exist_ok=True)

        self.processor.process(self.parent.filePath, self.filePath, self.parent.separator, self.parent.firstRow, self.parent.encoding)

class FileManager:
    def __init__(self, sourceDirectories: tuple, authFile: str, dwcProcessor: DWCProcessor):
        self.sourceDirectories = sourceDirectories
        self.authFile = authFile
        self.dwcProcessor = dwcProcessor

        self.user = ""
        self.password = ""

        if self.authFile is not None:
            with open(self.sourceDirectories[0] / self.authFile) as fp:
                data = fp.read().splitlines()

            self.user = data[0].split('=')[1]
            self.password = data[1].split('=')[1]

        self.stages = {stage: [] for stage in FileStage}

    def getFiles(self, stage: FileStage):
        return self.stages[stage]
    
    def create(self, stage: FileStage, fileNumbers: list[int] = [], overwrite: bool = False):
        if not fileNumbers: # Create all files:
            for file in self.stages[stage]:
                file.create(overwrite)
            return
        
        for number in fileNumbers:
            if number >= 0 and number <= len(self.stages[stage]):
                self.stages[stage][number].create()
            else:
                print(f"Invalid number provided: {number}")

    def addDownloadURLStage(self, url: str, fileName: str, processing: list[dict], fileProperties: dict = {}):
        downloadedFile = self.sourceDirectories[1] / fileName # downloaded files go into download directory
        downloadProcessor = FileProcessor([], [{"download": url, "filePath": downloadedFile, "user": self.user, "pass": self.password}], self.sourceDirectories)
        
        rawFile = StageFile(downloadedFile, {} if processing else fileProperties, downloadProcessor)
        self.stages[FileStage.RAW].append(rawFile)

        if not processing:
            return
        
        processor = FileProcessor([rawFile.getFilePath()], processing, self.sourceDirectories)
        for outputPath in processor.getOutputs():
            processedFile = StageFile(outputPath, fileProperties, processor, [rawFile])
            self.stages[FileStage.PROCESSED].append(processedFile)

    def addRetrieveScriptStage(self, script, processing, fileProperties):
        downloadProcessor = FileProcessor([], [script], self.sourceDirectories)
        for filePath in downloadProcessor.getOutputs():
            processedFile = StageFile(filePath, fileProperties, downloadProcessor)
            self.stages[FileStage.RAW].append(processedFile)

        if not processing:
            return
        
        for file in self.stages[FileStage.RAW]:
            processor = FileProcessor([file.filePath], processing, self.sourceDirectories)
            for filePath in processor.getOutputs():
                processedFile = StageFile(filePath, {}, processor, [file])
                self.stages[FileStage.PROCESSED].append(processedFile)
    
    def addCombineStage(self, processing):
        parentFiles = self.stages[FileStage.PROCESSED]
        combineProcessor = FileProcessor(parentFiles, processing, self.sourceDirectories)
        for outputPath in combineProcessor.getOutputs():
            combinedFile = StageFile(outputPath, {}, combineProcessor, parentFiles)
            self.stages[FileStage.COMBINED].append(combinedFile)
    
    def pushPreDwC(self):
        fileStages = (FileStage.RAW, FileStage.PROCESSED, FileStage.COMBINED, FileStage.PRE_DWC)
        for idx, stage in enumerate(fileStages[:-1], start=1):
            nextStage = fileStages[idx]
            if self.stages[stage] and not self.stages[nextStage]: # If this stage has files and next doesn't
                self.stages[nextStage] = self.stages[stage].copy()

        for file in self.stages[FileStage.PRE_DWC]:
            stageFile = DWCStageFile(file, self.dwcProcessor)
            self.stages[FileStage.DWC].append(stageFile)
