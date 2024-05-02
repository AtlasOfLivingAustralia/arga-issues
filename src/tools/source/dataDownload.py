from lib.data.argParser import SourceArgParser
from lib.processing.stages import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Download source data")

    sources, selectedFiles, overwrite, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.prepareStage(StageFileStep.DOWNLOADED)
        source.createStage(StageFileStep.DOWNLOADED, selectedFiles, overwrite, **kwargs)
