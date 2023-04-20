from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Prepare for DwC conversion")
    
    source, args = parser.parse_args()
    source.createPreDwC(args.filenumbers, args.overwrite)
