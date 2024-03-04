from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Download source data")

    sources, selectedFiles, overwrite, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.prepareStage(StageFileStep.DOWNLOADED)
        source.createStage(StageFileStep.DOWNLOADED, selectedFiles, overwrite, **vars(kwargs))
