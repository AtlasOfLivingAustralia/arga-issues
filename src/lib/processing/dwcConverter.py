import lib.config as cfg
import lib.commonFuncs as cmn
import lib.dataframeFuncs as dff
import numpy as np
import pandas as pd
import csv

class DWCConverter:
    dwcLookup = cmn.loadFromJson(cfg.filePaths.dwcMapping)
    customLookup = cmn.loadFromJson(cfg.filePaths.otherMapping)
    exclude = cmn.loadFromJson(cfg.filePaths.excludedEntries)
    chunkSize = 1024 * 1024

    def __init__(self, directoryPath, fileName, sep=',', header=0):
        self.directoryPath = directoryPath
        self.fileName = fileName
        self.sep = sep
        self.header = header

        self.filePath = directoryPath / fileName

    def chunkGenerator(self, filePath, chunkSize, sep=',', header=0):
        with pd.read_csv(filePath, on_bad_lines="skip", chunksize=chunkSize, sep=sep, header=header, dtype=object) as reader:
            for chunk in reader:
                yield chunk

    def applyTo(self, filePath, prefix, preserve=False, keep=[], splitFields={}, enrichDBs=[], augmentor=None, noRemap=[]):
        outputCols = []
        dwcPaths = []

        chunks = self.chunkGenerator(filePath, self.chunkSize, self.sep, self.header)
        for idx, df in enumerate(chunks):
            if idx == 0:
                columns = df.columns
                newColMap, copyColMap = dff.createMappings(columns, self.dwcLookup, self.customLookup, prefix, preserve, keep, noRemap)

            df = dff.applyColumnMap(df, newColMap, copyColMap)
            df = dff.applyExclusions(df, self.exclude)

            for column, (func, newColumns) in splitFields.items():
                df = dff.splitField(df, column, func, newColumns)

            for database, keyword in enrichDBs.items():
                print(f"Merging database {database.database} for chunk {idx}")
                for output in database.outputs:
                    enrichChunks = self.chunkGenerator(output, self.chunkSize)
                    for enrichChunk in enrichChunks:
                        if keyword not in df or keyword not in enrichChunk:
                            continue
                        
                        df = df.merge(enrichChunk, 'left', on=keyword, suffixes=('', f'_{database.database}'))

            if augmentor is not None:
                df = augmentor.augment(df)

            df = dff.dropEmptyColumns(df)
            
            outputPath = filePath.parent / f"dwc_chunk_{idx}.csv"
            df.to_csv(outputPath, index=False)

            outputCols = cmn.extendUnique(outputCols, df.columns)
            dwcPaths.append(outputPath)

        with open(self.filePath, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, outputCols)
            writer.writeheader()

            for dwcFile in dwcPaths:
                with open(dwcFile, encoding='utf-8') as dwcfp:
                    reader = csv.DictReader(dwcfp)
                    for row in reader:
                        writer.writerow(row)

                dwcFile.unlink()

    def getOutput(self):
        return self.filePath
