from pathlib import Path

class SelectorParser:
    def __init__(self, rootDir: Path, downloadDir: Path, processingDir: Path, predwcDir: Path, dwcDir: Path):
        self.rootDir = rootDir
        self.downloadDir = downloadDir
        self.processingDir = processingDir
        self.predwcDir = predwcDir
        self.dwcDir = dwcDir

        self.dirKeywords = ("ROOT", "DOWNLOAD", "PROCESSING", "PREDWC", "DWC")
        self.mapping = {keyword: directory for keyword, directory in zip(self.dirKeywords, (rootDir, downloadDir, processingDir, predwcDir, dwcDir))}

    def parseArg(self, arg: str, inputs: list) -> Path|str:
        if self.validSelector(arg):
            return self.parseSelector(arg, inputs)
        return arg

    def parseMultipleArgs(self, args: list[str], inputs: list) -> list[Path|str]:
        return [self.parseArg(arg, inputs) for arg in args]

    def validSelector(self, string: str) -> bool:
        return isinstance(string, str) and string[0] == "{" and string[-1] == "}" # Output hasn't got a complete selector
    
    def parseSelector(self, arg: str, inputs: list) -> str:
        selector = arg[1:-1] # Strip off braces

        attrs = [attr.strip() for attr in selector.split(',')]
        selectType = attrs.pop(0)

        if selectType == "INPUT": # Input selector
            return self.inputSelector(*attrs, inputs=inputs)
        
        if selectType == "PATH": # Path creator
            return self.pathSelector(*attrs)
        
        if selectType == "INPUTPATH":
            return self.inputPathSelector(*attrs, inputs=inputs)
    
    def inputSelector(self, selected: str = None, modifier: str = None, suffix: str = None, inputs: list = []):
        if selected is None or not selected.isdigit():
            raise Exception(f"Invalid input value for input selection: {selected}")

        selectInt = int(selected)

        if selectInt < 0 or selectInt >= len(inputs):
            raise Exception(f"Invalid input selection: {selected}")
        
        selectedPath = inputs[selectInt]

        if modifier is None: # Selector only
            return selectedPath

        # Apply modifier
        if modifier == "STEM":
            selectedPathStr = selectedPath.stem
        elif modifier == "PARENT":
            selectedPathStr = str(selectedPath.parent)
        elif modifier == "PARENT_STEM":
            selectedPathStr = selectedPath.parent.stem
        else:
            raise Exception(f"Invalid modifer: {modifier}") from AttributeError
        
        if suffix is None: # No suffix addition
            return Path(selectedPathStr)

        return Path(selectedPathStr + suffix) # Apply suffix
    
    def pathSelector(self, directory: str = None, fileName: str = None) -> Path:
        if directory is None:
            raise Exception("No directory specified") from AttributeError
        
        if directory not in self.dirKeywords:
            raise Exception(f"Invalid directory selected: {directory}") from AttributeError
        
        selectedDir = self.mapping[directory]

        if fileName is None:
            return selectedDir
        return selectedDir / fileName

    def inputPathSelector(self, directory: str = None, selected: str = None, modifier: str = None, suffix: str = None, inputs: list = []):
        return self.pathSelector(directory) / self.inputSelector(selected, modifier, suffix, inputs=inputs)
