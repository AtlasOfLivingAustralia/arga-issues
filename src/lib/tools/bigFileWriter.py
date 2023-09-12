import csv
from pathlib import Path
import lib.commonFuncs as cmn
import pandas as pd
import sys
from enum import Enum
import pyarrow as pa
import pyarrow.parquet as pq

class Format(Enum):
    CSV = "csv"
    TSV = "tsv"
    PARQUET = "parquet"

class Subfile:

    fileFormat = Format.CSV

    def __new__(cls, *args):
        subclassMap = {subclass.fileFormat: subclass for subclass in cls.__subclasses__()}
        subclass = subclassMap.get(args[-1], cls)
        return super().__new__(subclass)

    def __init__(self, location: Path, fileName: str, format: Format) -> 'Subfile':
        self.filePath = location / f"{fileName}.{Format(format).value}"

    def __repr__(self) -> str:
        return str(self.filePath)
    
    @classmethod
    def fromFilePath(cls, filePath: Path) -> 'Subfile':
        fileFormat = Format(filePath.suffix)
        instance = cls(Path(), "", fileFormat)
        instance.filePath = filePath
        return instance
    
    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(self.fullPath, index=False)
    
    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.fullPath)

    def rename(self, newFilePath: Path, newFileFormat: Format) -> None:
        if newFileFormat == self.fileFormat:
            self.filePath.rename(newFilePath)
            return
        
        df = self.read()
        
        if newFileFormat == Format.CSV:
            df.to_csv(newFilePath, index=False)

        elif newFileFormat == Format.TSV:
            df.to_csv(newFilePath, sep="\t", index=False)
            
        elif newFileFormat == Format.PARQUET:
            df.to_parquet(newFilePath, "pyarrow", index=False)

        self.remove()
        
    def remove(self) -> None:
        self.filePath.unlink()   

class TSVSubfile(Subfile):

    fileFormat = Format.TSV

    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(self.filePath, sep="\t", index=False)

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep="\t")
    
class PARQUETSubfile(Subfile):

    fileFormat = Format.PARQUET

    def write(self, df: pd.DataFrame) -> None:
        df.to_parquet(self.filePath, "pyarrow", index=False)

    def read(self) -> pd.DataFrame:
        return pd.read_parquet(self.filePath, "pyarrow")

class BigFileWriter:
    def __init__(self, outputFile: Path, subDirName: str = "chunks", sectionPrefix: str = "chunk", subfileType: Format = Format.PARQUET) -> 'BigFileWriter':
        self.outputFile = outputFile
        self.outputFileType = Format(outputFile.suffix[1:])
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

    def oneFile(self, removeOld: bool = True) -> None:
        if self.outputFile.exists():
            print(f"Removing old file {self.outputFile}")
            self.outputFile.unlink()

        if len(self.writtenFiles) == 1:
            print(f"Only single subfile, moving {self.writtenFiles[0]} to {self.outputFile}")

            self.writtenFiles[0].rename(self.outputFile, self.outputFileType)
            self.subfileDir.rmdir()
            return

        print("Combining into one file")
        if self.outputFileType in (Format.CSV, Format.TSV):
            self.oneCSV(removeOld)
        elif self.outputFileType == Format.PARQUET:
            self.oneParquet(removeOld)

        print(f"\nCreated a single file at {self.outputFile}")
        self.subfileDir.rmdir()
        self.writtenFiles.clear()

    def oneCSV(self, removeOld: bool = True):
        delim = "\t" if self.outputFileType == Format.TSV else ","

        with open(self.outputFile, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, self.globalColumns, delimiter=delim)
            writer.writeheader()

            fileCount = len(self.writtenFiles)
            for idx, file in enumerate(self.writtenFiles, start=1):
                print(f"At file: {idx} / {fileCount}", end='\r')

                df = file.read()
                for row in df.to_dict(orient="records"):
                    writer.writerow(row)

                if removeOld:
                    file.remove()
        
    def oneParquet(self, removeOld: bool = True):
        schema = pa.schema([(column, pa.string()) for column in self.globalColumns])
        with pq.ParquetWriter(self.outputFile, schema=schema) as writer:
            for file in self.writtenFiles:
                writer.write_table(pq.read_table(str(file)))

                if removeOld:
                    file.remove()
