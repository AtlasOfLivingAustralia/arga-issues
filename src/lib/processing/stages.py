import pandas as pd
import lib.commonFuncs as cmn
from pathlib import Path
from enum import Enum
from collections.abc import Iterator
from lib.tools.logger import Logger

class Step(Enum):
    DOWNLOAD   = 0
    PROCESSING = 1
    CONVERSION = 2

class File:
    def __init__(self, filePath: Path, fileProperties: dict = {}):
        self.filePath = filePath
        self.fileProperties = fileProperties.copy()

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

        self._backup = None

    def __repr__(self) -> str:
        return str(self.filePath)
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def backUp(self, overwrite: bool = False) -> None:
        backupPath = self.filePath.parent / f"{self.filePath.stem}_backup{self.filePath.suffix}"
        if backupPath.exists():
            if not overwrite:
                Logger.info("Unable to create new backup as it already exists")
                return
        
            backupPath.unlink()
        
        self.filePath.rename(backupPath)
        self._backup = backupPath

    def restoreBackUp(self) -> None:
        if self._backup is None:
            Logger.warning("Unable to restore backup as no previous backup made")
            return
        
        self.delete()
        self.filePath = self._backup
        self._backup = None
    
    def deleteBackup(self) -> None:
        if self._backup is None:
            return
        
        self._backup.unlink()
        self._backup = None
    
    def delete(self) -> None:
        self.filePath.unlink(True)
    
    def loadDataFrame(self, offset: int = 0, rows: int = None, **kwargs: dict) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep=self.separator, header=self.firstRow + offset, encoding=self.encoding, nrows=rows, **kwargs)
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = -1) -> Iterator[pd.DataFrame]:
        return cmn.chunkGenerator(self.filePath, chunkSize, self.separator, self.firstRow + offset, self.encoding, nrows=rows)

    def getColumns(self) -> list[str]:
        return cmn.getColumns(self.filePath, self.separator, self.firstRow)

class StackedFile(File):
    def __init__(self, filePath: Path):
        super().__init__(filePath, {})

    def delete(self) -> None:
        cmn.clearFolder(self.filePath, True)
        
    def _getFiles(self) -> list[Path]:
        return [file for file in self.filePath.iterdir() if file.suffix == ".csv"]

    def loadDataFrame(self, offset: int = 0, rows: int = None, **kwargs: dict) -> pd.DataFrame:
        dfs = {file.stem: pd.read_csv(file, sep=self.separator, header=offset, nrows=rows, encoding=self.encoding, **kwargs) for file in self._getFiles()}
        return pd.concat(dfs.values(), axis=1, keys=dfs.keys())
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = None) -> Iterator[pd.DataFrame]:
        sections = {file.stem: pd.read_csv(file, sep=self.separator, chunksize=chunkSize, header=offset, nrows=rows, encoding=self.encoding) for file in self._getFiles()}
        while True:
            try:
                yield pd.concat([next(chunk) for chunk in sections.values()], axis=1, keys=sections.keys())
            except StopIteration:
                return
