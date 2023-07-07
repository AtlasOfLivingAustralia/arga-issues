from lib.sourceObjs.argParseWrapper import SourceArgParser
import pandas as pd

if __name__ == '__main__':
    parser = SourceArgParser(description="View portion of DWC file")
    parser.add_argument("-e", "--entries", type=int, default=100, help="Amount of entries to view")
    columnGroup = parser.add_mutually_exclusive_group()
    columnGroup.add_argument("-m", "--mapped", action="store_true", help="Get only mapped fields")
    columnGroup.add_argument("-u", "--unmapped", action="store_true", help="Get only unmapped fields")
    
    sources, args = parser.parse_args()
    for source in sources:
        dwcFiles = source.getDWCFiles(args.filenums)
        for file in dwcFiles:
            if not file.filePath.exists():
                print(f"DwC file {file.filePath} does not exist, have you run preDwCCreate.py?")
                continue

            df = next(file.loadDataFrameIterator(args.entries))
            
            if args.mapped:
                unmapped = [col for col in df.columns if not col.startswith(source.location)]
                df = df[unmapped]
            elif args.unmapped:
                mapped = [col for col in df.columns if col.startswith(source.location)]
                df = df[mapped]

            df.to_csv(file.directory.parent / f"{file.filePath.name}_example.csv", index=False)
