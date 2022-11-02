import argparse
import json
import os
import logging
import config
import pandas as pd
from helperFunctions import reverseLookup, loadDataSources

if __name__ == '__main__':
    sources = loadDataSources()

    parser = argparse.ArgumentParser(description="Generate DwC file")
    parser.add_argument('source', choices=sources.keys())
    parser.add_argument('-u', '--uri', action='store_true', help="Generate DwC from source URI instead.")
    parser.add_argument('-p', '--preserve', action='store_true', help="Preserve input column names that match dwc values.")
    args = parser.parse_args()

    logging.basicConfig(
        filename=os.path.join(config.logsFolder, f"{args.source}_dwc_creation.log"),
        encoding="utf-8",
        level=logging.DEBUG,
        filemode='w',
        format="%(message)s"
    )

    # Load the mapping file and generate a reverse lookup for terms
    with open(config.dwcMappingPath) as fp:
        dwcLookup = json.load(fp)
    dwcReverseLookup = reverseLookup(dwcLookup)

    # Load the custom mapping file and generate a reverse lookup
    with open(config.customMappingPath) as fp:
        customReverseLookup = reverseLookup(json.load(fp))

    # Load exclusion file for entries to exclude
    with open(config.excludePath) as fp:
        exclude = json.load(fp)

    source = sources[args.source]
    prefix = args.source.split('-')[0].lower()

    df = source.loadDataFrame(fromuri=args.uri)

    newNames = {}
    copyColumns = {}

    for col in df.columns:
        if col in dwcReverseLookup: # Map to new name
            newNames[col] = dwcReverseLookup[col]
        elif col in customReverseLookup: # Map from custom name to new name
            newNames[col] = customReverseLookup[col]
        elif col in dwcLookup: # Name matches a dwc field name
            if args.preserve: # Preseve the matching field name with a prefix
                copyColumns[col] = f"{prefix}_{col}"
            else:
                logging.info(f"Skipping conversion of {col}")
            continue
        else:
            newNames[col] = f"{prefix}_{col}"

        logging.info(f"Converted {col} to {newNames[col]}")

    df = df.rename(newNames, axis=1)

    # Preserve columns that have matching names to dwc fields
    for col, newCol in copyColumns.items():
        df[newCol] = df[col]
        logging.info(f"Created duplicate of {col} named {newCol}")

    # Use exclude file to exclude entries
    for excludeType, properties in exclude.items():
        dwcName = properties["dwc"]
        exclusions = properties["data"]
        if dwcName in df.columns:
            df.drop(df[df[dwcName].isin(exclusions)].index, inplace=True)

    # Enrich ncbi sources with taxonData
    if source.source == "ncbi":
        taxonomy = pd.read_csv("../data/results/ncbiTaxonomy/ncbiTaxonomy.csv", dtype=object)
        df = pd.merge(df, taxonomy, 'left')

    # # Set species name as bin number if missing for bold entries:
    if source.source == "bold":
        print("HERE")

    df.to_csv(os.path.join(config.dwcFolder, f"{args.source}_dwc.csv"), index=False)
