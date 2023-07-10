import csv
from pathlib import Path
import lib.commonFuncs as cmn
import pandas as pd
import sys

class Writer:
    def __init__(self, outputDir: Path, subDirName: str, sectionPrefix: str) -> 'Writer':
        self.outputDir = outputDir
        self.subfileDir = outputDir / subDirName
        self.sectionPrefix = sectionPrefix

        self.writtenFiles = []
        self.globalColumns = []

        maxInt = sys.maxsize

        while True:
            try:
                csv.field_size_limit(maxInt)
                return
            except OverflowError:
                maxInt = int(maxInt/10)

    def writing(func):
        def wrapper(self, *args):
            if not self.subfileDir.exists():
                self.subfileDir.mkdir(parents=True, exist_ok=True)

            func(self, *args)
            
        return wrapper

    @writing
    def writeCSV(self, columns: list, entryData: list) -> None:
        filePath = self.subfileDir / f"{self.sectionPrefix}_{len(self.writtenFiles)}.csv"

        with open(filePath, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, columns)
            writer.writeheader()

            for line in entryData:
                writer.writerow(line)
        
        self.writtenFiles.append(filePath)
        self.globalColumns = cmn.extendUnique(self.globalColumns, columns)

    @writing
    def writeDF(self, df: pd.DataFrame) -> None:
        filePath = self.subfileDir / f"{self.sectionPrefix}_{len(self.writtenFiles)}.csv"
        df.to_csv(filePath, index=False)
        self.writtenFiles.append(filePath)
        self.globalColumns = cmn.extendUnique(self.globalColumns, df.columns)

    def oneFile(self, outputFilePath: Path, keepWrittenFile: bool = False) -> None:
        if outputFilePath.exists():
            print(f"Removing old file {outputFilePath}")
            outputFilePath.unlink()

        if len(self.writtenFiles) == 1:
            print(f"Only single subfile, moving {self.writtenFiles[0]} to {outputFilePath}")
            self.writtenFiles[0].rename(outputFilePath)
            self.subfileDir.rmdir()
            if not keepWrittenFile:
                self.writtenFiles = []
            return

        print("Combining into one file")
        with open(outputFilePath, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, self.globalColumns)
            writer.writeheader()

            fileCount = len(self.writtenFiles)
            for idx, file in enumerate(self.writtenFiles, start=1):
                print(f"At file: {idx} / {fileCount}", end='\r')

                with open(file, encoding='utf-8') as fp:
                    reader = csv.DictReader(fp)
                    for row in reader:
                        writer.writerow(row)

                file.unlink()
        print(f"\nCreated a single file at {outputFilePath}")
        self.subfileDir.rmdir()
        if keepWrittenFile:
            self.writtenFiles = [outputFilePath]
        else:
            self.writtenFiles = []
