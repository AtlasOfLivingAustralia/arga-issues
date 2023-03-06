from pathlib import Path
from enum import Enum
from lib.processing.processor import FileProcessor

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

    def getFilePath(self):
        return self.filePath
    
    def create(self):
        if self.filePath.exists():
            print(f"{str(self.filePath)} already exists, skipping creation")
            return
        
        for parent in self.parents:
            parent.create()

        self.filePath.parent.mkdir(parents=True, exist_ok=True)

        self.processor.process()

class FileManager:
    def __init__(self, rootDir: Path, downloadDir: Path, processingDir: Path):
        self.rootDir = rootDir
        self.downloadDir = downloadDir
        self.processingDir = processingDir

        self.stages = {stage: [] for stage in FileStage}

    def getFile(self, stage: FileStage, idx: int):
        return self.stages[stage][idx]
    
    def createAll(self, stage: FileStage):
        for file in self.stages[stage]:
            file.create()

    def addDownloadURLStage(self, url, fileName, processing, fileProperties={}):
        downloadedFile = self.downloadDir / fileName
        downloadProcessor = FileProcessor([], [{"download": url, "filePath": downloadedFile}], Path(), self.downloadDir)
        rawFile = StageFile(downloadedFile, fileProperties, downloadProcessor)
        self.stages[FileStage.RAW].append(rawFile)

        if not processing:
            self.stages[FileStage.PROCESSED].append(rawFile)
            return
        
        processor = FileProcessor([rawFile.getFilePath()], processing, self.processingDir)
        for outputPath in processor.getOutputs():
            processedFile = StageFile(outputPath, {}, processor, [rawFile])
            self.stages[FileStage.PROCESSED].append(processedFile)

    def addRetrieveScriptStage(self, scriptStep, fileProperties):
        downloadProcessor = FileProcessor.fromSteps([], [scriptStep], self.downloadDir)
        for filePath in downloadProcessor.getOutputs():
            processedFile = StageFile(filePath, fileProperties, downloadProcessor)
            self.stages[FileStage.PROCESSED].append(processedFile)
    
    def addCombineStage(self, processing):
        parentFiles = self.stages[FileStage.PROCESSED]
        combineProcessor = FileProcessor(parentFiles, processing, self.processingDir)
        for outputPath in combineProcessor.getOutputs():
            combinedFile = StageFile(outputPath, {}, combineProcessor, parentFiles)
            self.stages[FileStage.COMBINED].append(combinedFile)
    
    def pushPreDwC(self):
        for stage in (FileStage.COMBINED, FileStage.PROCESSED, FileStage.RAW):
            if self.stages[stage]: # If the stage has files
                self.stages[FileStage.PRE_DWC] = self.stages[stage].copy()
                return True
        return False
