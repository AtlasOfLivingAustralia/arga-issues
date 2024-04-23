from pathlib import Path
from lib.processing.stageFile import StageFileStep, StageFile, StageDwCFile
from lib.processing.stageScript import StageDownloadScript, StageScript, StageDWCConversion
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessing import DWCProcessor
import concurrent.futures
from lib.tools.logger import Logger

class SystemManager:
    def __init__(self, location: str, rootDir: Path, dwcProperties: dict, authFileName: str = "", maxWorkers: int = 100):
        self.location = location
        self.rootDir = rootDir
        self.dwcProperties = dwcProperties
        self.authFileName = authFileName
        self.maxWorkers = maxWorkers
        self.inlineLimit = 10

        self.user = ""
        self.password = ""

        if self.authFileName:
            with open(self.rootDir / self.authFileName) as fp:
                data = fp.read().splitlines()

            self.user = data[0].split('=')[1]
            self.password = data[1].split('=')[1]
        
        self.parser = SelectorParser(self.rootDir)
        self.dwcProcessor = DWCProcessor(self.location, self.dwcProperties, self.parser)

        self.stageFiles: dict[StageFileStep, list[StageFile]] = {stage: [] for stage in StageFileStep}

    def getFiles(self, stage: StageFileStep) -> list[StageFile]:
        return self.stageFiles[stage]
    
    def create(self, stage: StageFileStep, fileNumbers: list[int] = [], overwrite: int = 0, verbose: bool = False, **kwargs: dict) -> bool:
        files: list[StageFile] = []

        if not fileNumbers: # Create all files:
            files = self.stageFiles[stage] # All stage files
        else:
            for number in fileNumbers:
                if number >= 0 and number <= len(self.stageFiles[stage]):
                    files.append(self.stageFiles[stage][number])
                else:
                    Logger.error(f"Invalid number provided: {number}")

        if len(files) >= self.inlineLimit: # Parallel process large number of tasks
            Logger.info("Running tasks in parallel")
            return self._createParallel(files, stage, overwrite, verbose, **kwargs) > 0
        else:
            Logger.info("Running one task at a time")
            return self._createInline(files, stage, overwrite, verbose, **kwargs) > 0

    def _createInline(self, files: list[StageFile], stage: StageFileStep, overwrite: int, verbose: bool, **kwargs: dict) -> int:
        successCount = 0
        fileCount = len(files)

        for idx, file in enumerate(files, start=1):
            success = file.create(stage, overwrite, verbose, **kwargs)

            if success:
                Logger.info(f"Created file {idx}/{fileCount}: {file.filePath}")
                successCount += 1
            else:
                Logger.warning(f"Failed to create file {idx}/{fileCount}: {file.filePath}")

        return successCount

    def _createParallel(self, files: list[StageFile], stage: StageFileStep, overwrite: int, verbose: bool, **kwargs: dict) -> int:
        successCount = 0
        fileCount = len(files)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.maxWorkers) as executor:
            futures = (executor.submit(file.create, stage, overwrite, verbose, **kwargs) for file in files)

            try:
                for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                    success = future.result()
                    print(f"Completion: {idx/fileCount:0.2f}%", end="\r")

                    if success:
                        successCount += 1
                    
            except KeyboardInterrupt:
                Logger.info("\nExiting...")
                executor.shutdown(cancel_futures=True)
                return 0
            
            print()

        Logger.info(f"Successfully created {successCount}/{fileCount} files")
        return successCount
    
    def buildProcessingChain(self, processingSteps: list[dict], initialInputs: list[StageFile], finalStage: StageFileStep) -> None:
        inputs = initialInputs.copy()
        for idx, step in enumerate(processingSteps, start=1):
            scriptStep = StageScript(step, inputs, self.parser)
            stage = StageFileStep.INTERMEDIATE if idx < len(processingSteps) else finalStage
            inputs = [StageFile(filePath, properties, scriptStep, stage) for filePath, properties in scriptStep.getOutputs()]

        self.stageFiles[finalStage].extend(inputs)

    def addDownloadURLStage(self, url: str, fileName: str, processing: list[dict], fileProperties: dict = {}, buildProcessing: bool =True):
        downloadedFile = self.parser.downloadDir / fileName # downloaded files go into download directory
        downloadScript = StageDownloadScript(url, downloadedFile, self.parser, self.user, self.password)
        
        rawFile = StageFile(downloadedFile, fileProperties.copy(), downloadScript, StageFileStep.DOWNLOADED)
        self.stageFiles[StageFileStep.DOWNLOADED].append(rawFile)

        if buildProcessing:
            self.buildProcessingChain(processing, [rawFile], StageFileStep.PROCESSED)

    def addRetrieveScriptStage(self, script: dict, processing: list[dict], buildProcessing: bool = True):
        scriptStep = StageScript(script, [], self.parser)
        outputs = [StageFile(filePath, properties, scriptStep, StageFileStep.DOWNLOADED) for filePath, properties in scriptStep.getOutputs()]
        self.stageFiles[StageFileStep.DOWNLOADED].extend(outputs)

        if buildProcessing:
            self.buildProcessingChain(processing, outputs, StageFileStep.PROCESSED)
    
    def addFinalStage(self, processing):
        self.buildProcessingChain(processing, self.stageFiles[StageFileStep.PROCESSED], StageFileStep.PRE_DWC)
    
    def prepareDwC(self):
        for file in self.stageFiles[StageFileStep.PRE_DWC]:
            conversionScript = StageDWCConversion(file, self.dwcProcessor)
            dwcOutput = conversionScript.getOutput()
            convertedFile = StageDwCFile(dwcOutput, conversionScript)
            self.stageFiles[StageFileStep.DWC].append(convertedFile)
