from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Convert preDWC file to DWC")
    parser.add_argument("-s", "--singlefile", action="store_true", help="Create single DwC file")
    
    sources, selectedFiles, args = parser.parse_args()
    for source in sources:
        source.prepareStage(StageFileStep.DWC)
        source.createDwC(selectedFiles, args.overwrite)
