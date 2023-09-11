import csv
from pathlib import Path
import lib.commonFuncs as cmn
import pandas as pd
import sys
from enum import Enum
import pyarrow

class Format(Enum):
    CSV = "csv"
    PARQUET = "parquet"

class Subfile:
    def __init__(self, folderPath: Path, fileName: str, fileFormat: Format):
        self.folderPath = folderPath
        self.fileName = fileName
        self.fileFormat = fileFormat

        self.fullPath = folderPath / f"{fileName}.{Format(fileFormat).value}"

    def __repr__(self) -> str:
        return self.fullPath

    def write(self, df: pd.DataFrame) -> None:
        if self.fileFormat == Format.CSV:
            df.to_csv(self.fullPath, index=False)
            return
        
        if self.fileFormat == Format.PARQUET:
            df.to_parquet(self.fullPath, "pyarrow", index=False)
            return

    def read(self) -> pd.DataFrame:
        if self.fileFormat == Format.CSV:
            return pd.read_csv(self.fullPath)
        
        if self.fileFormat == Format.PARQUET:
            return pd.read_parquet(self.fullPath, "pyarrow")
        
    def rename(self, newFilePath: Path) -> None:
        if self.fileFormat == Format.CSV:
            self.fullPath.rename(newFilePath)
            return
        
        if self.fileFormat == Format.PARQUET:
            self.read().to_csv(newFilePath, index=False)
            self.remove()
            return
        
    def remove(self) -> None:
        self.fullPath.unlink()

class BigFileWriter:
    def __init__(self, outputFile: Path, subDirName: str = "chunks", sectionPrefix: str = "chunk", subfileType: Format = Format.PARQUET) -> 'BigFileWriter':
        self.outputFile = outputFile
        self.subfileDir = outputFile.parent / subDirName
        self.sectionPrefix = sectionPrefix
        self.subfileType = subfileType

        self.writtenFiles: list[Subfile] = []
        self.globalColumns: list[str] = []

        maxInt = sys.maxsize
        while True:
            try:
                csv.field_size_limit(maxInt)
                return
            except OverflowError:
                maxInt = int(maxInt/10)

    def writeCSV(self, cols: list[str], rows: list[list[str]]) -> None:
        df = pd.DataFrame(columns=cols, data=rows)
        self.writeDF(df)

    def writeDF(self, df: pd.DataFrame, format: Format = None) -> None:
        if not self.subfileDir.exists():
            self.subfileDir.mkdir(parents=True)

        if format is None:
            format = self.subfileType

        subfile = Subfile(self.subfileDir, f"{self.sectionPrefix}_{len(self.writtenFiles)}", format)
        subfile.write(df)

        self.writtenFiles.append(subfile)
        self.globalColumns = cmn.extendUnique(self.globalColumns, df.columns)

    def oneFile(self) -> None:
        if self.outputFile.exists():
            print(f"Removing old file {self.outputFile}")
            self.outputFile.unlink()

        if len(self.writtenFiles) == 1:
            print(f"Only single subfile, moving {self.writtenFiles[0]} to {self.outputFile}")

            self.writtenFiles[0].rename(self.outputFile)
            self.subfileDir.rmdir()
            return

        print("Combining into one file")
        with open(self.outputFile, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, self.globalColumns)
            writer.writeheader()

            fileCount = len(self.writtenFiles)
            for idx, file in enumerate(self.writtenFiles, start=1):
                print(f"At file: {idx} / {fileCount}", end='\r')

                df = file.read()
                for row in df.to_dict(orient="records"):
                    writer.writerow(row)

                file.remove()

        print(f"\nCreated a single file at {self.outputFile}")
        self.subfileDir.rmdir()
        self.writtenFiles.clear()
