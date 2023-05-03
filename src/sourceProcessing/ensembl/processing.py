import json
import pandas as pd

def convert(stageFile, outputFilePath):
    with open(stageFile.filePath) as fp:
        data = json.load(fp)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputFilePath, index=False)