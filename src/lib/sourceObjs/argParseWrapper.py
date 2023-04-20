from argparse import ArgumentParser, Namespace
from lib.sourceManager import SourceManager
from lib.sourceObjs.sourceDatabase import Database

class SourceArgParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        self.manager = SourceManager()

        super().__init__(*args, **kwargs)

        self.add_argument("source", choices=self.manager.choices(), nargs="+", help="")
        self.add_argument("-o", "--overwrite", action="store_true", help="Force recreation of file at this stage")
        self.add_argument('-n', '--filenumbers', type=int, default=None, nargs='+', help="Choose which files to download by number")

    def parse_args(self, *args, **kwargs) -> tuple[Database, Namespace]:
        parsedArgs = super().parse_args(*args, **kwargs)
        source = self.manager.getDB(parsedArgs.source)
        del parsedArgs.source # Remove source name stored in original parsed args

        return (source, parsedArgs)
    