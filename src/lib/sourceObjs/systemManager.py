from pathlib import Path
from enum import Enum
from lib.processing.stages import StageFile, StageDownloadScript, StageScript, StageDWCConversion
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessor import DWCProcessor

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
        self.dwcProcessor = DWCProcessor(self.location, self.dwcProperties, self.enrichDBs, self.dwcDir)

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

        outputs = [rawFile]
        for step in processing:
            scriptStep = StageScript(step, outputs, self.parser)
            outputs = [StageFile(filePath, {}, scriptStep) for filePath in scriptStep.getOutputs()]

        self.stages[FileStage.PROCESSED].extend(outputs)

    def addRetrieveScriptStage(self, script, processing, fileProperties):
        scriptStep = StageScript(script, [], self.parser)
        outputs = [StageFile(filePath, fileProperties, scriptStep) for filePath in scriptStep.getOutputs()]
        self.stages[FileStage.RAW].extend(outputs)

        for step in processing:
            scriptStep = StageScript(step, outputs, self.parser)
            outputs = [StageFile(filePath, {}, scriptStep) for filePath in scriptStep.getOutputs()]

        self.stages[FileStage.PROCESSED].extend(outputs)
    
    def addCombineStage(self, processing):
        outputs = self.stages[FileStage.PROCESSED]
        for step in processing:
            combineScript = StageScript(step, outputs, self.parser)
            outputs = combineScript.getOutputs()

        self.stages[FileStage.COMBINED].extend(outputs)
    
    def pushPreDwC(self):
        fileStages = (FileStage.RAW, FileStage.PROCESSED, FileStage.COMBINED, FileStage.PRE_DWC)
        for idx, stage in enumerate(fileStages[:-1], start=1):
            nextStage = fileStages[idx]
            if self.stages[stage] and not self.stages[nextStage]: # If this stage has files and next doesn't
                self.stages[nextStage] = self.stages[stage].copy()

        for file in self.stages[FileStage.PRE_DWC]:
            dwcConversionScript = StageDWCConversion(file, self.dwcProcessor)
            dwcOutput = dwcConversionScript.getOutput()
            convertedFile = StageFile(dwcOutput, {}, dwcConversionScript)
            self.stages[FileStage.DWC].append(convertedFile)
