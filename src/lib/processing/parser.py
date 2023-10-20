from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from lib.processing.stageFile import StageFile

class SelectorParser:
    def __init__(self, rootDir: Path, dataDir: Path, downloadDir: Path, processingDir: Path, predwcDir: Path, dwcDir: Path):
        self.rootDir = rootDir
        self.dataDir = dataDir
        self.downloadDir = downloadDir
        self.processingDir = processingDir
        self.predwcDir = predwcDir
        self.dwcDir = dwcDir

        self.dirKeywords = ("ROOT", "DATA", "DOWNLOAD", "PROCESSING", "PREDWC", "DWC")
        self.mapping = {keyword: directory for keyword, directory in zip(self.dirKeywords, (rootDir, downloadDir, processingDir, predwcDir, dwcDir))}

    def parseArg(self, arg: str, inputs: list) -> Path|str:
        if self.validSelector(arg):
            return self.parseSelector(arg, inputs)
        return arg

    def parseMultipleArgs(self, args: list[str], inputs: list[StageFile]) -> list:
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
    
    def inputSelector(self, selected: str = None, property: str = None, suffix: str = None, inputs: list[StageFile] = []):
        if selected is None or not selected.isdigit():
            raise Exception(f"Invalid input value for input selection: {selected}")

        selectInt = int(selected)

        if selectInt < 0 or selectInt >= len(inputs):
            raise Exception(f"Invalid input selection: {selected}")
        
        selectedStageFile = inputs[selectInt]

        if property is None: # No specific property, return StageFile
            return selectedStageFile

        # Apply property
        propertyMap = {
            "FILEPATH": selectedStageFile.filePath,
            "FILEPATH_STEM": selectedStageFile.filePath.stem,
            "DIRECTORY_PATH": selectedStageFile.directory,
            "DIRECTORY_NAME": selectedStageFile.directory.stem
        }

        if property not in propertyMap:
            raise Exception(f"Invalid property: {property}") from AttributeError
        
        selectedPath = propertyMap[property]
        
        if suffix is None: # No suffix addition
            return selectedPath

        return Path(str(selectedPath) + suffix) # Apply suffix
    
    def pathSelector(self, directory: str = None, fileName: str = None) -> Path:
        if directory is None:
            raise Exception("No directory specified") from AttributeError
        
        if directory not in self.dirKeywords:
            raise Exception(f"Invalid directory selected: {directory}") from AttributeError
        
        selectedDir = self.mapping[directory]

        if fileName is None:
            return selectedDir
        return selectedDir / fileName

    def inputPathSelector(self, directory: str = None, selected: str = None, property: str = None, suffix: str = None, inputs: list[StageFile] = []):
        return self.pathSelector(directory) / self.inputSelector(selected, property, suffix, inputs=inputs)
