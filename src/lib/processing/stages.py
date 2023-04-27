from pathlib import Path
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessor import DWCProcessor
import lib.commonFuncs as cmn
import lib.processing.processingFuncs as pFuncs
from enum import Enum

class FileStage(Enum):
    RAW          = 0
    INTERMEDIATE = 1
    PROCESSED    = 2
    COMBINED     = 3
    PRE_DWC      = 4
    DWC          = 5

class StageFile:
    def __init__(self, filePath: Path, fileProperties: dict, parentScript: 'StageScript', stage: 'FileStage'):
        self.filePath = filePath
        self.fileProperties = fileProperties
        self.parentScript = parentScript
        self.stage = stage

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

    def getFilePath(self) -> Path:
        return self.filePath
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def create(self, overwriteStage: FileStage, overwriteAmount: int = 0) -> None:
        if self.filePath.exists():
            if self.stage not in (overwriteStage, FileStage.INTERMEDIATE):
                return
            
            elif overwriteAmount <= 0:
                print(f"{self.filePath} already exists")
                return
        
        self.filePath.parent.mkdir(parents=True, exist_ok=True)
        self.parentScript.run(overwriteStage, overwriteAmount)

class StageScript:
    def __init__(self, processingStep: dict, inputs: list[StageFile], parser: SelectorParser):
        self.processingStep = processingStep.copy()
        self.inputs = inputs
        self.parser = parser
        self.scriptRun = False

        self.path = self.processingStep.pop("path", None)
        self.function = self.processingStep.pop("function", None)
        self.args = self.processingStep.pop("args", [])
        self.kwargs = self.processingStep.pop("kwargs", {})
        self.outputs = self.processingStep.pop("outputs", [])

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        self.args = self.parser.parseMultipleArgs(self.args, self.inputs)
        self.kwargs = {key: self.parser.parseArg(value, self.inputs) for key, value in self.kwargs.items()}
        self.outputs = self.parser.parseMultipleArgs(self.outputs, self.inputs)

        for parameter in self.processingStep:
            print(f"Unknown step parameter: {parameter}")

    def getOutputs(self) -> list[Path]:
        return self.outputs

    def run(self, overwriteStage: FileStage, overwrite: int = 0, verbose: bool = True):
        if all(output.exists() for output in self.outputs) and overwrite <= 0:
            if verbose:
                print(f"All outputs {self.outputs} exist and not overwriting, skipping '{self.function}'")
            return
        
        for input in self.inputs:
            input.create(overwriteStage, overwrite - 1)
            if not input.exists():
                if verbose:
                    print(f"Missing input file: {input.filePath}")
                return
        
        if self.scriptRun:
            return
        
        processFunction = pFuncs.importFunction(self.path, self.function)

        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {self.args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {self.kwargs}"
            print(msg)
        
        output = processFunction(*self.args, **self.kwargs)
        self.scriptRun = True
        return output

class StageDownloadScript:
    def __init__(self, url: str, downloadedFile: Path, parser: SelectorParser, user: str, password: str):
        self.url = url
        self.downloadedFile = downloadedFile
        self.parser = parser
        self.user = user
        self.password = password

        self.downloadedFile = parser.parseArg(downloadedFile, [])

    def getOutputs(self) -> list[Path]:
        return [self.downloadedFile]

    def run(self, overwriteStage: FileStage, overwriteAmount: int = 0, verbose: bool = False):
        if self.downloadedFile.exists() and overwriteAmount <= 0:
            if verbose:
                print(f"File already downloaded at {self.downloadedFile}, skipping redownload")
            return
        
        cmn.downloadFile(self.url, self.downloadedFile, self.user, self.password)

class StageDWCConversion:
    def __init__(self, input: StageFile, dwcProcessor: DWCProcessor):
        self.input = input
        self.dwcProcessor = dwcProcessor
        self.outputFileName = f"{self.input.filePath.stem}-dwc.csv"

    def getOutput(self) -> Path:
        return self.dwcProcessor.outputDir / self.outputFileName

    def run(self, overwriteStage: FileStage, overwriteAmount: int = 0, verbose: bool = True):
        outputPath = self.getOutput()

        if outputPath.exists() and overwriteAmount <= 0:
            print(f"DWC file {outputPath} exists and not overwriting, skipping creation")
            return
        
        print(f"Creating DWC from preDWC file {self.input.filePath}")

        self.dwcProcessor.process(
            self.input.filePath,
            self.getOutput(),
            self.input.separator,
            self.input.firstRow,
            self.input.encoding,
            overwriteAmount > 0
        )
