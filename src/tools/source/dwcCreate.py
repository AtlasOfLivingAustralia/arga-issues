from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Convert preDWC file to DWC")
    
    sources, args = parser.parse_args()
    for source in sources:
        source.createDwC(args.filenumbers, args.overwrite)
