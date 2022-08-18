import pandas as pd
import os
from helperFunctions import getValueExamples

output = "ena_assembly_all_fields.tsv"
dataOutLoc = "../data"
generatedOutLoc = "../generatedFiles"

outPath = os.path.join(dataOutLoc, output)
if not os.path.exists(outPath):
    print("Downloading data")
    df = pd.read_table("https://www.ebi.ac.uk/ena/portal/api/search?result=assembly&fields=all&limit=0&format=tsv")
    df.to_csv(outPath, sep='\t', index=False)
else:
    df = pd.read_table(outPath, dtype='object', parse_dates=['last_updated']) 

getValueExamples(df, outpath=dataOutLoc, outfilename="ENAexamples.json")
