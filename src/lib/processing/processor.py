from pathlib import Path
import lib.commonFuncs as cmn
import lib.config as cfg
from lib.processing.parser import SelectorParser
from lib.processing.steps import FileStep, DownloadStep, AugmentStep
import lib.dataframeFuncs as dff
import pandas as pd
from lib.processing.subfileWriter import Writer

class FileProcessor:
    def __init__(self, inputPaths: list[Path], processingSteps: list[dict], sourceDirectories: tuple):
        self.inputPaths = inputPaths
        self.steps = []

        if not processingSteps:
            self.outputPaths = self.inputPaths
            return

        nextInputs = inputPaths
        for step in processingSteps:
            parser = SelectorParser(sourceDirectories, nextInputs)

            if "download" in step:
                stepObject = DownloadStep(step.copy(), parser)
            else:
                stepObject = FileStep(step.copy(), parser)
            self.steps.append(stepObject)
            nextInputs = stepObject.getOutputs()

        self.outputPaths = nextInputs

    @classmethod
    def fromSteps(cls, inputPaths, steps, sourceDirectories):
        obj = cls(inputPaths, {}, sourceDirectories)
        obj.steps = steps
        obj.outputPaths = steps[-1].getOutputs()
        return obj

    def process(self, overwrite=False):
        for step in self.steps:
            step.process(overwrite)

    def getOutputs(self) -> list[Path]:
        return self.outputPaths

class DWCProcessor:
    dwcLookup = cmn.loadFromJson(cfg.filePaths.dwcMapping)
    customLookup = cmn.loadFromJson(cfg.filePaths.otherMapping)
    exclude = cmn.loadFromJson(cfg.filePaths.excludedEntries)

    def __init__(self, prefix: str, dwcProperties: dict, enrichDBs: dict, outputDir: Path):
        self.prefix = prefix
        self.dwcProperties = dwcProperties
        self.enrichDBs = enrichDBs
        self.outputDir = outputDir

        self.augments = dwcProperties.pop("augment", [])
        self.chunkSize = dwcProperties.pop("chunkSize", 1024*1024)

        self.augmentSteps = [AugmentStep(augProperties) for augProperties in self.augments]

        self.writer = Writer(outputDir, "dwcConversion", "dwcChunk")

    def chunkGen(self, filePath, sep, header, encoding):
        with pd.read_csv(filePath, on_bad_lines="skip", chunksize=self.chunkSize, sep=sep, header=header, encoding=encoding, dtype=object) as reader:
            for chunk in reader:
                yield chunk

    def process(self, inputPath, outputFilePath, sep, header, encoding):
        if not self.checkPreparedEnrichment():
            return

        for idx, df in enumerate(self.chunkGen(inputPath, sep, header, encoding)):
            if idx == 0:
                newColMap, copyColMap = dff.createMappings(df.columns, self.dwcLookup, self.customLookup, self.prefix)
             
            df = dff.applyColumnMap(df, newColMap, copyColMap)
            df = dff.applyExclusions(df, self.exclude)
            df = self.applyAugments(df)
            df = self.applyEnrichment(df)
            df = dff.dropEmptyColumns(df)

            self.writer.writeDF(df)

        self.writer.oneFile(outputFilePath)

    def checkPreparedEnrichment(self):
        for database in self.enrichDBs.values():
            for enrichFile in database.getDWCFiles():
                if not enrichFile.filePath.exists():
                    print(f"Database {database.database} file {enrichFile.filePath} not prepared for enrichment, cancelling DWC conversion")
                    return False
        return True

    def applyAugments(self, df):
        for augment in self.augmentSteps:
            df = augment.process(df)
        return df
    
    def applyEnrichment(self, df):
        for keyword, database in self.enrichDBs.items():
            for enrichFile in database.getDWCFiles():
                for enrichChunk in self.chunkGen(enrichFile.filePath, enrichFile.separator, enrichFile.firstRow, enrichFile.encoding):
                    if keyword not in df or keyword not in enrichChunk:
                        continue

                    df = df.merge(enrichChunk, 'left', on=keyword, suffixes=('', f'_{database.database}'))
        return df