import argparse
from lib.data.sources import SourceManager
from lib.data.database import Database

class ArgParser:
    def __init__(self, description=""):
        self.parser = argparse.ArgumentParser(description=description)
        self.manager = SourceManager()

        self.parser.add_argument("sources", choices=self.manager.choices(), nargs="+", help="Database to interact with", metavar="SOURCE")
        self.parser.add_argument("-p", "--prepare", action="store_true", help="Force redoing preparation")
        self.parser.add_argument("-o", "--overwrite", action="store_true", help="Force overwriting files")
        self.parser.add_argument("-q", "--quiet", action="store_false", help="Suppress output")

    def add_argument(self, *args, **kwargs) -> None:
        self.parser.add_argument(*args, **kwargs)

    def parse_args(self, *args, **kwargs) -> tuple[list[Database], tuple[bool, bool], bool, argparse.Namespace]:
        parsedArgs = self.parser.parse_args(*args, **kwargs)

        sources = self.manager.getDB(self._extract(parsedArgs, "sources"))
        prepare = self._extract(parsedArgs, "prepare")
        overwrite = self._extract(parsedArgs, "overwrite")
        verbose = self._extract(parsedArgs, "quiet")

        return sources, (prepare, overwrite), verbose, parsedArgs
    
    def namespaceKwargs(self, namespace: argparse.Namespace) -> dict:
        return vars(namespace)

    def add_mutually_exclusive_group(self, *args, **kwargs) -> argparse._MutuallyExclusiveGroup:
        return self.parser.add_mutually_exclusive_group(*args, **kwargs)
    
    def _extract(self, namespace: argparse.Namespace, attribute: str) -> any:
        attr = getattr(namespace, attribute)
        delattr(namespace, attribute)
        return attr
