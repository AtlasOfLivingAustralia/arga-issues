import pandas as pd
import lib.commonFuncs as cmn
import lib.processing.processingFuncs as pFuncs
from pathlib import Path
from enum import Enum
from collections.abc import Iterator
from lib.tools.logger import Logger
from lib.processing.parser import Parser

class Step(Enum):
    DOWNLOAD   = 0
    PROCESSING = 1
    CONVERSION = 2

class File:
    def __init__(self, filePath: Path, fileProperties: dict):
        self.filePath = filePath
        self.fileProperties = fileProperties.copy()

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

    def __repr__(self) -> str:
        return str(self.filePath)
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def delete(self) -> None:
        self.filePath.unlink(True)
    
    def loadDataFrame(self, offset: int = 0, rows: int = None) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep=self.separator, header=self.firstRow + offset, encoding=self.encoding, nrows=rows)
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = -1) -> Iterator[pd.DataFrame]:
        return cmn.chunkGenerator(self.filePath, chunkSize, self.separator, self.firstRow + offset, self.encoding, nrows=rows)

    def getColumns(self) -> list[str]:
        return cmn.getColumns(self.filePath, self.separator, self.firstRow)

class StackedFile(File):
    def __init__(self, filePath: Path, fileProperties: dict):
        super().__init__(filePath, fileProperties)

    def delete(self) -> None:
        cmn.clearFolder(self.filePath, True)
        
    def _getFiles(self) -> list[Path]:
        return [file for file in self.filePath.iterdir() if file.suffix == ".csv"]

    def loadDataFrame(self, offset: int = 0, rows: int = None) -> pd.DataFrame:
        dfs = {file.stem: pd.read_csv(file, sep=self.separator, header=offset, nrows=rows, encoding=self.encoding) for file in self._getFiles()}
        return pd.concat(dfs.values(), axis=1, keys=dfs.keys())
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = None) -> Iterator[pd.DataFrame]:
        sections = {file.stem: pd.read_csv(file, sep=self.separator, chunksize=chunkSize, header=offset, nrows=rows, encoding=self.encoding) for file in self._getFiles()}
        while True:
            try:
                yield pd.concat([next(chunk) for chunk in sections.values()], axis=1, keys=sections.keys())
            except StopIteration:
                return

class Script:
    def __init__(self, scriptInfo: dict, outputDir: Path):
        self.outputDir = outputDir
        scriptInfo = scriptInfo.copy()
        
        self.path = scriptInfo.pop("path", None)
        self.function = scriptInfo.pop("function", None)
        self.args = scriptInfo.pop("args", [])
        self.kwargs = scriptInfo.pop("kwargs", {})
        self.outputs = scriptInfo.pop("outputs", [])
        self.outputStructure = scriptInfo.pop("outputStructure", "")
        self.outputProperties = scriptInfo.pop("outputProperties", {})

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError

        self.scriptRun = False

        for parameter in scriptInfo:
            Logger.debug(f"Unknown step parameter: {parameter}")

    def getOutputs(self, inputs: list[File] = []) -> list[File]:
        if self.outputs:
            return [File(self.outputDir / output, {}) for output in self.outputs]
        
        if self.outputStructure:
            return [File(self.outputDir / output, {}) for output in Parser().parse(self.outputStructure, inputs)]
        
        return []

    def run(self, overwrite: bool = False, verbose: bool = False, **kwargs: dict):
        if self.scriptRun:
            return
        
        if not overwrite and any(output.exists() for output in self.outputs):
            Logger.info(f"All outputs {self.outputs} exist and not overwriting, skipping '{self.function}'")
            return
        
        for output in self.getOutputs():
            output.delete()
        
        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {self.args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {self.kwargs}"
            print(msg)

        processFunction = pFuncs.importFunction(self.path, self.function)
        output = processFunction(*self.args, **self.kwargs)

        for outputFile in self.getOutputs():
            if not outputFile.exists():
                Logger.warning(f"Output {outputFile} was not created")

        return output
