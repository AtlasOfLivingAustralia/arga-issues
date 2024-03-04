from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="View portion of DWC file")
    parser.add_argument("-e", "--entries", type=int, default=100, help="Amount of entries to view")
    columnGroup = parser.add_mutually_exclusive_group()
    columnGroup.add_argument("-m", "--mapped", action="store_true", help="Get only mapped fields")
    columnGroup.add_argument("-u", "--unmapped", action="store_true", help="Get only unmapped fields")
    
    sources, selectedFiles, overwrite, args = parser.parse_args()
    for source in sources:
        source.prepareStage(StageFileStep.DWC)
        dwcFiles = source.getDWCFiles(selectedFiles)
        for file in dwcFiles:
            if not file.filePath.exists():
                print(f"DwC file {file.filePath} does not exist, have you run preDwCCreate.py?")
                continue

            df = next(file.loadDataFrameIterator(args.entries))

            fileName = f"{file.filePath.name}_example.csv"
            if args.mapped:
                unmapped = [col for col in df.columns if not col.startswith(source.location)]
                df = df[unmapped]
                fileName = f"{file.filePath.name}_mapped_example.csv"
            elif args.unmapped:
                mapped = [col for col in df.columns if col.startswith(source.location)]
                df = df[mapped]
                fileName = f"{file.filePath.name}_unmapped_example.csv"

            df.to_csv(file.directory.parent / fileName, index=False)
