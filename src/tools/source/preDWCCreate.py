from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Prepare for DwC conversion")
    
    sources, args = parser.parse_args()
    for source in sources:
        source.createPreDwC(args.filenums, args.overwrite)
