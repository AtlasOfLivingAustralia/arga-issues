import csv
from pathlib import Path
import lib.commonFuncs as cmn
import pandas as pd

class Writer:
    def __init__(self, outputDir: Path, subDirName: str, sectionPrefix: str) -> 'Writer':
        self.outputDir = outputDir
        self.subfileDir = outputDir / subDirName
        self.sectionPrefix = sectionPrefix

        self.subfileDir.mkdir(parents=True, exist_ok=True)

        self.writtenFiles = []
        self.globalColumns = []

    def writeCSV(self, columns: list, entryData: list) -> None:
        filePath = self.subfileDir / f"{self.sectionPrefix}_{len(self.writtenFiles)}.csv"

        with open(filePath, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, columns)
            writer.writeheader()

            for line in entryData:
                writer.writerow(line)
        
        self.writtenFiles.append(filePath)
        self.globalColumns = cmn.extendUnique(self.globalColumns, columns)

    def writeDF(self, df: pd.DataFrame) -> None:
        filePath = self.subfileDir / f"{self.sectionPrefix}_{len(self.writtenFiles)}.csv"
        df.to_csv(filePath, index=False)
        self.writtenFiles.append(filePath)
        self.globalColumns = cmn.extendUnique(self.globalColumns, df.columns)

    def oneFile(self, outputFilePath: Path) -> None:
        if outputFilePath.exists():
            print(f"Removing old file {outputFilePath}")
            outputFilePath.unlink()

        if len(self.writtenFiles) == 1:
            print(f"Only single subfire, moving {self.writtenFiles[0]} to {outputFilePath}")
            self.writtenFiles[0].rename(outputFilePath)
            self.subfileDir.rmdir()
            return

        print(f"Combining into one file at {outputFilePath}")
        with open(outputFilePath, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, self.globalColumns)
            writer.writeheader()

            for file in self.writtenFiles:
                with open(file, encoding='utf-8') as fp:
                    reader = csv.DictReader(fp)
                    for row in reader:
                        writer.writerow(row)

                file.unlink()
        self.writtenFiles = [self.outputFilePath]
