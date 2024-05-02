from lib.data.argParser import SourceArgParser
from lib.processing.stages import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Convert preDWC file to DWC")
    parser.add_argument("-i", "--ignoreRemapErrors", action="store_true", help="Ignore remapping errors from matching columns")
    parser.add_argument("-f", "--forceRetrieve", action="store_true", help="Force retrieve maps from google sheets")
    parser.add_argument("-z", "--zip", action="store_true", help="Zip output file")

    sources, selectedFiles, overwrite, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.prepareStage(StageFileStep.DWC)
        source.createStage(StageFileStep.DWC, selectedFiles, overwrite, **kwargs)
