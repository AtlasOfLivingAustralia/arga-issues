from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path
from enum import Enum
import pandas as pd
import lib.dataframeFuncs as dff
from collections.abc import Iterator

if TYPE_CHECKING:
    from lib.processing.stageScript import StageScript

class StageFileStep(Enum):
    RAW          = 0
    INTERMEDIATE = 1
    PROCESSED    = 2
    COMBINED     = 3
    PRE_DWC      = 4
    DWC          = 5

class StageFile:
    def __init__(self, filePath: Path, fileProperties: dict, parentScript: StageScript, stage: StageFileStep):
        self.filePath = filePath
        self.fileProperties = fileProperties
        self.parentScript = parentScript
        self.stage = stage
        self.directory = filePath.parent

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

    def getFilePath(self) -> Path:
        return self.filePath
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def updateStage(self, stage: StageFileStep) -> None:
        self.stage = stage
    
    def loadDataFrame(self) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep=self.separator, header=self.firstRow, encoding=self.encoding)
    
    def loadDataFrameIterator(self, chunkSize: int = 1024 * 1024) ->  Iterator[pd.DataFrame]:
        return dff.chunkGenerator(self.filePath, chunkSize, self.separator, self.firstRow, self.encoding)

    def create(self, overwriteStage: StageFileStep, overwriteAmount: int = 0) -> None:
        if self.filePath.exists():
            if self.stage not in (overwriteStage, StageFileStep.INTERMEDIATE):
                return
            
            elif overwriteAmount <= 0:
                print(f"{self.filePath} already exists")
                return
        
        self.filePath.parent.mkdir(parents=True, exist_ok=True)
        self.parentScript.run(overwriteStage, overwriteAmount)
