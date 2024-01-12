from lib.sourceObjs.argParseWrapper import SourceArgParser
from lib.processing.stageFile import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Prepare for DwC conversion")
    
    sources, selectedFiles, overwrite, kwargs = parser.parse_args()
    for source in sources:
        source.prepareStage(StageFileStep.PRE_DWC)
        source.createStage(StageFileStep.PRE_DWC, selectedFiles, overwrite, **kwargs)
