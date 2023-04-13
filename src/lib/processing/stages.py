from __future__ import annotations
from pathlib import Path
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessor import DWCProcessor
import platform
import subprocess
import lib.processing.processingFuncs as pFuncs

class StageFile:
    def __init__(self, filePath: Path, fileProperties: dict, parentScript: StageScript):
        self.filePath = filePath
        self.fileProperties = fileProperties
        # self.processor = processor
        self.parentScript = parentScript

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

    def getFilePath(self) -> Path:
        return self.filePath
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def create(self, overwrite: bool = False) -> None:
        if self.filePath.exists() and not overwrite:
            print(f"{self.filePath} already exists and not overwriting, skipping creation")
            return
        
        self.filePath.parent.mkdir(parents=True, exist_ok=True)
        self.parentScript.run()

class StageScript:
    def __init__(self, processingStep: dict, inputs: list[StageFile], parser: SelectorParser):
        self.processingStep = processingStep.copy()
        self.inputs = inputs
        self.parser = parser

        self.path = self.processingStep.pop("path", None)
        self.function = self.processingStep.pop("function", None)
        self.args = self.processingStep.pop("args", [])
        self.kwargs = self.processingStep.pop("kwargs", {})
        self.outputs = self.processingStep.pop("outputs", [])

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        self.args = self.parser.parseMultipleArgs(self.args)
        self.kwargs = {key: self.parser.parseArg(value) for key, value in self.kwargs.items()}
        self.outputs = self.parser.parseMultipleArgs(self.outputs)

        for parameter in processingStep:
            print(f"Unknown step parameter: {parameter}")

    def getOutputs(self) -> list[Path]:
        return self.outputs

    def run(self, overwrite=False, verbose=True):
        if self.outputs and not overwrite:
            if verbose:
                print(f"All outputs {self.outputs} exist and not overwriting, skipping '{self.function}'")
            return
        
        if not all([input.exists() for input in self.inputs]):
            print(f"Not all inputs exist, unable to run script")
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
        
        return processFunction(*self.args, **self.kwargs)

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

    def run(self, overwrite=False):
        if self.downloadedFile.exists() and not overwrite:
            return
        
        print(f"Downloading from {self.url} to file {self.filePath}")

        curl = "curl"
        if platform.system() == 'Windows':
            curl = "curl.exe"

        args = [curl, self.url, "-o", self.filePath]
        if self.user:
            args.extend(["--user", f"{self.user}:{self.password}"])

        subprocess.run(args)

class StageDWCConversion:
    def __init__(self, input: StageFile, dwcProcessor: DWCProcessor):
        self.input = input
        self.dwcProcessor = dwcProcessor
        self.outputFileName = f"{self.input.filePath.stem}-dwc.csv"

    def getOutput(self) -> Path:
        return self.dwcProcessor.outputDir / self.outputFileName

    def run(self, overwrite=False):
        outputPath = self.getOutput()

        if outputPath.exists() and not overwrite:
            print(f"DWC file {outputPath} exists and not overwriting, skipping creation")
            return
        
        print(f"Creating DWC from preDWC file {self.input.filePath}")
        
        self.dwcProcessor.process(
            self.input.filePath,
            self.getOutput(),
            self.input.separator,
            self.input.firstRow,
            self.input.encoding,
            overwrite
        )
