from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Prepare for DwC conversion")
    
    sources, selectedFiles, args = parser.parse_args()
    for source in sources:
        source.createPreDwC(selectedFiles, args.overwrite)
