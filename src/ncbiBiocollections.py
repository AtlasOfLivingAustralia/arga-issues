import config
import os
import pandas as pd
from helperFunctions import splitLine

if __name__ == '__main__':
    biocolls = os.path.join(config.dataFolder, "biocollections")
    collCodes = os.path.join(biocolls, "Collection_codes.txt")
    instCodes = os.path.join(biocolls, "Institution_codes.txt")
    uInstCodes = os.path.join(biocolls, "Unique_institution_codes.txt")

    for ref, file in enumerate((collCodes, instCodes, uInstCodes)):
        data = []

        with open(file) as fp:
            line = fp.readline()
            headers = splitLine(line)
            line = fp.readline()
            while line:
                data.append(splitLine(line, True))
                line = fp.readline()

        # cull extra data that doesn't map to a header
        df = pd.DataFrame([line[:len(headers)] for line in data], columns=headers) 

        if ref == 0:
            output = df.copy()
        else:
            output = pd.merge(output, df, 'left')

    output.dropna(how='all', axis=1, inplace=True)
    output.to_csv(os.path.join(config.dataFolder, "biocollections.csv"), index=False)