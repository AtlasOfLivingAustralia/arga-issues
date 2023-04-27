from argparse import ArgumentParser, Namespace
from lib.sourceManager import SourceManager
from lib.sourceObjs.sourceDatabase import Database

class SourceArgParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        self.manager = SourceManager()

        super().__init__(*args, **kwargs)

        self.add_argument("sources", choices=self.manager.choices(), nargs="+", help="")
        self.add_argument("-o", "--overwrite", type=int, default=0, help="Amount of steps to force overwrite on")
        self.add_argument('-n', '--filenumbers', type=int, default=None, nargs='+', help="Choose which files to download by number")

    def parse_args(self, *args, **kwargs) -> tuple[list[Database], Namespace]:
        parsedArgs = super().parse_args(*args, **kwargs)
        sources = self.manager.getDB(parsedArgs.sources)
        del parsedArgs.sources # Remove source name stored in original parsed args

        return (sources, parsedArgs)
    