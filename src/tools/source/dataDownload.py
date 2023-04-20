from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Download source data")

    source, args = parser.parse_args()
    source.download(args.filenumbers, args.overwrite)
