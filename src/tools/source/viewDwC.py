from lib.sourceObjs.argParseWrapper import SourceArgParser
import pandas as pd

if __name__ == '__main__':
    parser = SourceArgParser(description="View portion of DWC file")
    parser.add_argument("-e", "--entries", type=int, default=100, help="Amount of entries to view")
    parser.add_argument("-x", "--excludeUnmapped", action="store_true", help="Exclude unmapped fields")
    
    sources, args = parser.parse_args()
    for source in sources:
        dwcFiles = source.getDWCFiles(args.filenums)
        for idx, file in enumerate(dwcFiles, start=1):
            df = next(file.loadDataFrameIterator(args.entries))

            if args.excludeUnmapped:
                keepColls = [col for col in df.columns if not col.startswith(source.location)]
                df = df[keepColls]

            df.to_csv(file.directory.parent / f"dwc_example_{idx}.csv", index=False)
