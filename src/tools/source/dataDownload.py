from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Download source data")

    sources, selectedFiles, args = parser.parse_args()
    for source in sources:
        source.prepareStage(StageFileStep.DOWNLOADED)
        source.download(selectedFiles, args.overwrite)
