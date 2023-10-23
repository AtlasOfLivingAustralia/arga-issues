import pandas as pd

def clean(stageFile, outputFilePath):
    # Load to csv and resave to remove quotation marks around data
    df = pd.read_csv(stageFile.filePath)
    df.to_csv(outputFilePath, index=False)
