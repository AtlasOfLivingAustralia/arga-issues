from lib.data.argParser import ArgParser
from lib.tools.logger import Logger
from lib.processing.stages import Step

if __name__ == '__main__':
    parser = ArgParser(description="Run update on data source")
    
    sources, overwrite, verbose, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        if not source.checkUpdateReady():
            Logger.info(f"Data source '{source}' is not ready for update.")
            continue

        for step in (Step.DOWNLOAD, Step.PROCESSING, Step.CONVERSION):
            source.create(step, (True, True), True)

        outputFile = source.package()
