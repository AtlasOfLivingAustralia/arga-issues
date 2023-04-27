from pathlib import Path
from lib.processing.stages import FileStage, StageFile, StageDownloadScript, StageScript, StageDWCConversion
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessor import DWCProcessor

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
    
    def create(self, stage: FileStage, fileNumbers: list[int] = [], overwrite: int = 0):
        if not fileNumbers: # Create all files:
            for file in self.stages[stage]:
                file.create(stage, overwrite)
            return
        
        for number in fileNumbers:
            if number >= 0 and number <= len(self.stages[stage]):
                self.stages[stage][number].create(stage, overwrite)
            else:
                print(f"Invalid number provided: {number}")

    def buildProcessingChain(self, processingSteps: list[dict], initialInputs: list[StageFile], finalStage: FileStage) -> None:
        inputs = initialInputs.copy()
        for idx, step in enumerate(processingSteps):
            scriptStep = StageScript(step, inputs, self.parser)
            stage = FileStage.INTERMEDIATE if idx < len(processingSteps) else finalStage
            inputs = [StageFile(filePath, {}, scriptStep, stage) for filePath in scriptStep.getOutputs()]

        self.stages[finalStage].extend(inputs)

    def addDownloadURLStage(self, url: str, fileName: str, processing: list[dict], fileProperties: dict = {}):
        downloadedFile = self.downloadDir / fileName # downloaded files go into download directory
        downloadScript = StageDownloadScript(url, downloadedFile, self.parser, self.user, self.password)
        
        rawFile = StageFile(downloadedFile, {} if processing else fileProperties, downloadScript, FileStage.RAW)
        self.stages[FileStage.RAW].append(rawFile)

        self.buildProcessingChain(processing, [rawFile], FileStage.PROCESSED)

    def addRetrieveScriptStage(self, script, processing, fileProperties):
        scriptStep = StageScript(script, [], self.parser)
        outputs = [StageFile(filePath, fileProperties, scriptStep, FileStage.RAW) for filePath in scriptStep.getOutputs()]
        self.stages[FileStage.RAW].extend(outputs)

        self.buildProcessingChain(processing, outputs, FileStage.PROCESSED)
    
    def addCombineStage(self, processing):
        self.buildProcessingChain(processing, self.stages[FileStage.PROCESSED], FileStage.COMBINED)
    
    def pushPreDwC(self):
        fileStages = (FileStage.RAW, FileStage.PROCESSED, FileStage.COMBINED, FileStage.PRE_DWC)
        for idx, stage in enumerate(fileStages[:-1], start=1):
            nextStage = fileStages[idx]
            if self.stages[stage] and not self.stages[nextStage]: # If this stage has files and next doesn't
                self.stages[nextStage] = self.stages[stage].copy()

        for file in self.stages[FileStage.PRE_DWC]:
            conversionScript = StageDWCConversion(file, self.dwcProcessor)
            dwcOutput = conversionScript.getOutput()
            convertedFile = StageFile(dwcOutput, {}, conversionScript, FileStage.DWC)
            self.stages[FileStage.DWC].append(convertedFile)
