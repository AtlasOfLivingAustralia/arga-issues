import argparse
from lib.sourceManager import SourceManager
from lib.sourceObjs.dbTypes import DBType

# Prepare a source before being used in dwc creation

if __name__ == '__main__':
    sources = SourceManager()

    parser = argparse.ArgumentParser(description="Prepare for DwC conversion")
    parser.add_argument("source", choices=sources.choices())
    parser.add_argument("-o", "--overwrite", action="store_true", help="Force overwrite if files already exist")
    parser.add_argument('-n', '--filenumbers', type=int, default=None, nargs='+', help="Choose which files to download by number")
    
    args = parser.parse_args()

    db = sources.getDB(args.source, False)
    db.createPreDwC(args.filenumbers, args.overwrite)
