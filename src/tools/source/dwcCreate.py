from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Convert preDWC file to DWC")
    
    sources, selectedFiles, args = parser.parse_args()
    for source in sources:
        source.prepareStage(StageFileStep.DWC)
        source.createDwC(selectedFiles, args.overwrite)
