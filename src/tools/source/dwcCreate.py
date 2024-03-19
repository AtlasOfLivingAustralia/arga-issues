from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Convert preDWC file to DWC")
    parser.add_argument("-i", "--ignoreRemapErrors", action="store_true", help="Ignore remapping errors from matching columns")
    parser.add_argument("-f", "--forceRetrieve", action="store_true", help="Force retrieve maps from google sheets")

    sources, selectedFiles, overwrite, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.prepareStage(StageFileStep.DWC)
        source.createStage(StageFileStep.DWC, selectedFiles, overwrite, **kwargs)
