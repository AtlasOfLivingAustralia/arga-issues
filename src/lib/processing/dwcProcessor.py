import lib.commonFuncs as cmn
import lib.commonFuncs as cfg
import lib.dataframeFuncs as dff
from pathlib import Path
from lib.subfileWriter import Writer
from lib.processing.steps import AugmentStep

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
        self.chunkSize = dwcProperties.pop("chunkSize", 100000)

        self.augmentSteps = [AugmentStep(augProperties) for augProperties in self.augments]

        self.writer = Writer(outputDir, "dwcConversion", "dwcChunk")

    def process(self, inputPath: Path, outputFilePath: Path, sep: str, header: int, encoding: str, overwrite: bool = False):
        if not self.checkPreparedEnrichment():
            return

        for idx, df in enumerate(dff.chunkGenerator(inputPath, self.chunkSize, sep, header, encoding)):
            if idx == 0:
                newColMap, copyColMap = dff.createMappings(df.columns, self.dwcLookup, self.customLookup, self.prefix)
             
            print(f"At chunk: {idx}", end='\r')
            df = dff.applyColumnMap(df, newColMap, copyColMap)
            df = dff.applyExclusions(df, self.exclude)
            df = self.applyAugments(df)
            df = self.applyEnrichment(df)
            # df = dff.dropEmptyColumns(df)

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
                for enrichChunk in dff.chunkGenerator(enrichFile.filePath, self.chunkSize, enrichFile.separator, enrichFile.firstRow, enrichFile.encoding):
                    if keyword not in df or keyword not in enrichChunk:
                        continue

                    columnDifferences = list(enrichChunk.columns.difference(df.columns)) + [keyword]
                    df = df.merge(enrichChunk[columnDifferences], 'left', on=keyword)
        return df
