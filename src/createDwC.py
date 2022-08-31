import argparse
import json
import os
import logging
import config
from helperFunctions import loadDataSources, loadSourceFile

if __name__ == '__main__': 
    sources = loadDataSources()

    parser = argparse.ArgumentParser(description="Generate DwC file")
    parser.add_argument('source', choices=sources.keys())
    parser.add_argument('-i', '--inpath', default=config.dataFolder, help="Location of downloaded file to convert.")
    parser.add_argument('-m', '--mapfile', default=config.mappingPath, help="Path of mapping file.")
    parser.add_argument('-d', '--dir', default=config.dwcFolder, help="Directory to output dwc file.")
    parser.add_argument('-u', '--fromuri', action='store_true', help="Read data from uri instead of file, only works if source doesn't have a listed processed file.")
    parser.add_argument('-p', '--preserve', action='store_true', help="Preserve input column names that match dwc values.")
    args = parser.parse_args()

    logging.basicConfig(
        filename=os.path.join(config.logsFolder, f"{args.source}_dwc_creation.log"),
        encoding='utf-8',
        level=logging.DEBUG
    )

    with open(args.mapfile) as fp:
        lookup = json.load(fp)

    reverseLookup = {oldName: dwcName for dwcName, oldNameList in lookup.items() for oldName in oldNameList}

    source = sources[args.source]
    prefix = args.source.split('-')[0].lower()

    loadPath = source.getLoadPath(args.inpath, args.fromuri)
    df = loadSourceFile(loadPath, kwargs=source.parseKwargs)

    newNames = {}
    copyColumns = {}

    for col in df.columns:
        if col in reverseLookup:
            newNames[col] = reverseLookup[col]
        elif col in lookup:
            if args.preserve:
                copyColumns[col] = f"{prefix}_{col}"
            else:
                logging.info(f"Skipping conversion of {col}")
            continue
        else:
            newNames[col] = f"{prefix}_{col}"

        logging.info(f"Converted {col} to {newNames[col]}")

    df = df.rename(newNames, axis=1)

    for col, newCol in copyColumns.items():
        df[newCol] = df[col]
        logging.info(f"Created duplicate of {col} named {newCol}")

    df.to_csv(os.path.join(args.dir, f"{args.source}_dwc.csv"))
