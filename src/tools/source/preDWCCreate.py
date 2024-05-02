from lib.data.argParser import SourceArgParser
from lib.processing.stages import StageFileStep

if __name__ == '__main__':
    parser = SourceArgParser(description="Prepare for DwC conversion")
    
    sources, selectedFiles, overwrite, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.prepareStage(StageFileStep.PRE_DWC)
        source.createStage(StageFileStep.PRE_DWC, selectedFiles, overwrite, **kwargs)
