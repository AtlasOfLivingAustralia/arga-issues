from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Convert preDWC file to DWC")
    
    source, args = parser.parse_args()
    source.createDwC(args.filenumbers, args.overwrite)
