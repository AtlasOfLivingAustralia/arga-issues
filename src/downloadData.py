import os
import sys
import subprocess
import argparse
import config
from helperFunctions import loadDataSources

if __name__ == '__main__':
    sources = loadDataSources()

    parser = argparse.ArgumentParser(description="Download source uri to file")
    parser.add_argument('source', choices=sources.keys())
    parser.add_argument('-o', '--overwrite', action='store_true', help="Overwrite currently downloaded file if it exists.")
    parser.add_argument('-d', '--dir', default=config.dataFolder, help="Output directory for downloaded file.")
    args = parser.parse_args()

    source = sources[args.source]
    if os.path.exists(source.downloadedFile) and not args.overwrite:
        print("File already exists. Try using -o to overwrite.")
        sys.exit()

    outpath = os.path.join(args.dir, source.downloadedFile)
    print(f"Downloading to {outpath}")
    subprocess.run(f"curl {source.uri} -o {outpath}")
