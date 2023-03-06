from pathlib import Path

class SelectorParser:
    def __init__(self, rootDir: Path, downloadDir: Path, processingDir: Path, inputPaths: list[Path]):
        self.rootDir = rootDir
        self.downloadDir = downloadDir
        self.processingDir = processingDir
        self.inputPaths = inputPaths

    def parseArg(self, arg: str) -> Path|str:
        if self.validSelector(arg):
            return self.parseSelector(arg)
        return arg

    def parseMultipleArgs(self, args: list[str]) -> list[Path|str]:
        return [self.parseArg(arg) for arg in args]

    def validSelector(self, string: str) -> bool:
        return isinstance(string, str) and string[0] == "{" and string[-1] == "}" # Output hasn't got a complete selector
    
    def parseSelector(self, arg: str) -> str:
        selector = arg[1:-1] # Strip off braces

        attrs = [attr.strip() for attr in selector.split(',')]
        selectType = attrs.pop(0)

        if selectType == "INPUT": # Input selector
            return self.inputSelector(*attrs)
        
        if selectType == "PATH": # Path creator
            return self.pathSelector(*attrs)
    
    def inputSelector(self, selected=None, modifier=None, suffix=None):
        if selected is None or not selected.isdigit():
            raise Exception(f"Invalid input value for input selection: {selected}")

        selectInt = int(selected)

        if selectInt < 0 or selectInt >= len(self.inputPaths):
            raise Exception(f"Invalid input selection: {selected}")
        
        selectedPath = self.inputPaths[selectInt]

        if modifier is None: # Selector only
            return selectedPath

        # Apply modifier
        if modifier == "STEM":
            selectedPathStr = selected.stem
        elif modifier == "PARENT":
            selectedPathStr = str(selected.parent)
        elif modifier == "PARENT_STEM":
            selectedPathStr = selected.parent.stem
        else:
            raise Exception(f"Invalid modifer: {modifier}") from AttributeError
        
        if suffix is None: # No suffix addition
            return Path(selectedPathStr)

        return Path(selected + suffix) # Apply suffix
    
    def pathSelector(self, directory=None, fileName=None):
        if directory is None:
            raise Exception("No directory specified") from AttributeError
        
        selectedDir = None
        if directory == "ROOT":
            selectedDir = self.rootDir
        elif directory == "DOWNLOAD":
            selectedDir = self.downloadDir
        elif directory == "PROCESSING":
            selectedDir = self.processingDir
        else:
            raise Exception(f"Invalid directory selected: {directory}") from AttributeError

        if fileName is None:
            return selectedDir
        return selectedDir / fileName
