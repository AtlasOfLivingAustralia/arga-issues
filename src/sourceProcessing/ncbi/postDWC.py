import pandas as pd
import os
from lib.config import folderPaths
from lib.commonFuncs import latlongToDecimal
from lib.dataframeFuncs import splitField, mapAndApply

source = "ncbi"

def process(df, dwcLookup, customLookup, enrichDBs):
    for db, onTerm in enrichDBs.items():

        for idx, _ in enumerate(db.outputFiles):
            df = db.loadDF(idx)
            

        if database == 'ncbi-biosample': # Biosample
            print("Merging biosample")
            for ref, file in enumerate(os.listdir("../../data/ncbi/biosample/biosample")):
                print(f"At biosample file: {ref}", end='\r')
                biosample = pd.read_csv(os.path.join("../../data/ncbi/biosample/biosample", file), dtype=object)

                # Remap lat_lon to 2 fields
                biosample = splitField(biosample, "Attribute_attribute_name_lat_lon", latlongToDecimal, ["decimalLatitude", "decimalLongitude"])

                # Convert column names
                biosample = mapAndApply(biosample, dwcLookup, customLookup, source)

                df = df.merge(biosample, 'left', on=onTerm, suffixes=('', '_biosample'))
            print()

        elif database == 'ncbi-bioproject': # Bioproject
            print('Merging bioproject')
            bioproject = pd.read_csv("../../data/ncbi/bioproject/bioproject.csv", dtype=object)
            bioproject = mapAndApply(bioproject, dwcLookup, customLookup, source)
            df = df.merge(bioproject, 'left', on=onTerm, suffixes=('', '_bioproject'))

        elif database == 'ncbi-taxonomy': # Taxon data
            print('Merging taxon data')
            taxonomy = pd.read_csv("../../data/ncbi/taxonomy/ncbiTaxonomy.csv", dtype=object)
            df = df.merge(taxonomy, 'left', on=onTerm)

    return df
