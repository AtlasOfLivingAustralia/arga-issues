import argparse
from lib.sourceManager import SourceManager

if __name__ == '__main__':
    sources = SourceManager()

    parser = argparse.ArgumentParser(description="Download source uri to file")
    parser.add_argument('source', choices=sources.choices())
    parser.add_argument('-o', '--overwrite', action='store_true', help="Overwrite currently downloaded file if it exists.")
    parser.add_argument('-n', '--filenumbers', type=int, default=None, nargs='+', help="Choose which files to download by number")
    args = parser.parse_args()

    source = sources.getDB(args.source, False)
    source.download()
