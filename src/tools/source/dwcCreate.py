from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Convert preDWC file to DWC")
    parser.add_argument("-s", "--singlefile", action="store_true", help="Create single DwC file")
    parser.add_argument("-i", "--ignoreRemapErrors", action="store_true", help="Ignore remapping errors from matching columns")
    
    sources, selectedFiles, overwrite, kwargs = parser.parse_args()
    for source in sources:
        source.prepareStage(StageFileStep.DWC)
        source.createStage(StageFileStep.DWC, selectedFiles, overwrite, **kwargs)
