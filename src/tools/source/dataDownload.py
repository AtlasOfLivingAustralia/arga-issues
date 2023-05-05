from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Download source data")

    sources, args = parser.parse_args()
    for source in sources:
        source.download(args.filenum, args.overwrite)
