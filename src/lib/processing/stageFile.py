from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path
from enum import Enum
import pandas as pd
import lib.commonFuncs as cmn
from collections.abc import Iterator
from lib.tools.logger import Logger

if TYPE_CHECKING:
    from lib.processing.stageScript import StageScript

class StageFileStep(Enum):
    DOWNLOADED   = 0
    PROCESSED    = 1
    PRE_DWC      = 2
    DWC          = 3
    INTERMEDIATE = 4

class StageFile:
    def __init__(self, filePath: Path, fileProperties: dict, parentScript: StageScript, stage: StageFileStep):
        self.filePath = filePath
        self.fileProperties = fileProperties.copy()
        self.parentScript = parentScript
        self.stage = stage
        self.directory = filePath.parent

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

    def __repr__(self) -> str:
        return str(self.filePath)

    def getFilePath(self) -> Path:
        return self.filePath
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def updateStage(self, stage: StageFileStep) -> None:
        self.stage = stage
    
    def loadDataFrame(self) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep=self.separator, header=self.firstRow, encoding=self.encoding)
    
    def loadDataFrameIterator(self, chunkSize: int = 1024 * 1024) ->  Iterator[pd.DataFrame]:
        return cmn.chunkGenerator(self.filePath, chunkSize, self.separator, self.firstRow, self.encoding)

    def getColumns(self) -> list[str]:
        return cmn.getColumns(self.filePath, self.separator, self.firstRow)

    def create(self, overwriteStage: StageFileStep, overwriteAmount: int = 0, verbose: bool = False, **kwargs: dict) -> bool:
        if self.filePath.exists():
            if self.stage not in (overwriteStage, StageFileStep.INTERMEDIATE) or overwriteAmount <= 0:
                Logger.info(f"{self.filePath} already exists")
                return False
        
        self.filePath.parent.mkdir(parents=True, exist_ok=True)
        self.parentScript.run(overwriteStage, overwriteAmount, verbose, **kwargs)
        return True
