from pathlib import Path
from enum import Enum
from lib.processing.fileProcessor import FileProcessor
from lib.processing.stages import StageFile, DWCStageFile, StageDownloadScript, StageScript
from lib.processing.parser import SelectorParser

class FileStage(Enum):
    RAW       = 0
    PROCESSED = 1
    COMBINED  = 2
    PRE_DWC   = 3
    DWC       = 4

class SystemManager:
    def __init__(self, location: str, rootDir: Path, dwcProperties: dict, enrichDBs: dict, authFileName: str = ""):
        self.location = location
        self.rootDir = rootDir
        self.authFileName = authFileName
        self.dwcProperties = dwcProperties
        self.enrichDBs = enrichDBs

        self.user = ""
        self.password = ""

        if self.authFileName:
            with open(self.rootDir / self.authFileName) as fp:
                data = fp.read().splitlines()

            self.user = data[0].split('=')[1]
            self.password = data[1].split('=')[1]

        self.downloadDir = self.rootDir / "raw"
        self.processingDir = self.rootDir / "processing"
        self.preConversionDir = self.rootDir / "preConversion"
        self.dwcDir = self.rootDir / "dwc"
        
        self.parser = SelectorParser(self.rootDir, self.downloadDir, self.processingDir, self.preConversionDir, self.dwcDir)

        self.stages = {stage: [] for stage in FileStage}

    def getFiles(self, stage: FileStage):
        return self.stages[stage]
    
    def createAll(self, stage: FileStage, overwrite: bool = False):
        for file in self.stages[stage]:
            file.create(overwrite)

    def addDownloadURLStage(self, url: str, fileName: str, processing: list[dict], fileProperties: dict = {}):
        downloadedFile = self.downloadDir / fileName # downloaded files go into download directory
        downloadScript = StageDownloadScript(url, downloadedFile, self.parser, self.user, self.password)
        
        rawFile = StageFile(downloadedFile, {} if processing else fileProperties, downloadScript)
        self.stages[FileStage.RAW].append(rawFile)

        nextInputs = [rawFile]
        for step in processing:
            scriptStep = StageScript(step, nextInputs, self.parser)
            nextInputs = [StageFile(filePath, {}, scriptStep) for filePath in scriptStep.getOutputs()]

        self.stages[FileStage.PROCESSED].extend(nextInputs) # Next inputs are the outputs after final step

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

