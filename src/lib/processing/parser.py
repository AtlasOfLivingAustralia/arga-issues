from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path
from enum import Enum

if TYPE_CHECKING:
    from lib.processing.stageFile import StageFile

class StageFileProperty(Enum):
    DIRECTORY = "DIRECTORY"
    FILEPATH  = "FILEPATH"

class PathProperty(Enum):
    PARENT = "PARENT"
    STEM   = "STEM"

class SelectorParser:
    def __init__(self, rootDir: Path):
        self.rootDir = rootDir
        self.dataDir = self.rootDir / "data"
        self.downloadDir = self.dataDir / "raw"
        self.processingDir = self.dataDir / "processing"
        self.preDwcDir = self.dataDir / "preConversion"
        self.dwcDir = self.dataDir / "dwc"

        self.mapping = {
            "ROOT": self.rootDir,
            "DATA": self.dataDir,
            "DOWNLOAD": self.downloadDir,
            "PROCESSING": self.processingDir,
            "PREDWC": self.preDwcDir,
            "DWC": self.dwcDir
        }

    def parseArg(self, arg: str, inputs: list[StageFile]) -> [StageFile | Path]:
        if self._validSelector(arg):
            return self._parseSelector(arg, inputs)
        return arg

    def parseMultipleArgs(self, args: list[str], inputs: list[StageFile]) -> list[StageFile | Path]:
        return [self.parseArg(arg, inputs) for arg in args]

    def _validSelector(self, string: str) -> bool:
        return isinstance(string, str) and string[0] == "{" and string[-1] == "}" # Output hasn't got a complete selector
    
    def _parseSelector(self, arg: str, inputs: list) -> str:
        selector = arg[1:-1] # Strip off braces

        attrs = [attr.strip() for attr in selector.split(',')]
        selectType = attrs.pop(0)

        if selectType == "INPUT":
            if len(attrs) < 2 or len(attrs) > 4:
                raise Exception("Invalid quantity of arguments provided, expected 2 to 4")
            
            return self._selectInput(inputs, *attrs)
        
        if selectType == "PATH": # Path creator
            if len(attrs) < 1 or len(attrs) > 2:
                raise Exception("Invalid quantity of arguments provided, expected 1 or 2")
            
            return self._selectPath(*attrs)
        
        if selectType == "INPUTPATH":
            if len(attrs) < 3 or len(attrs) > 5:
                raise Exception("Invalid quantity of arguments provided, expected 3 to 5")
            
            selectedInput = self._selectInput(inputs, *attrs[1:])
            return self._selectPath(attrs[0], selectedInput.name)
        
    def _selectInput(self, inputs: list[StageFile], selected: str, properties: str = None, suffix: str = None) -> StageFile | Path:
        if selected is None or not selected.isdigit():
            raise Exception(f"Invalid input value for input selection: {selected}")

        selectInt = int(selected)

        if selectInt < 0 or selectInt >= len(inputs):
            raise Exception(f"Invalid input selection: {selected}")
        
        selectedStageFile = inputs[selectInt]

        if properties is None:
            return selectedStageFile
    
        propertyChain = properties.split("_")

        stageFileProperty = StageFileProperty._value2member_map_.get(propertyChain[0], None)
        if stageFileProperty == StageFileProperty.DIRECTORY:
            selectedPath = selectedStageFile.directory
        elif stageFileProperty == StageFileProperty.FILEPATH:
            selectedPath = selectedStageFile.filePath
        else:
            raise Exception(f"Invalid stage file property: {stageFileProperty}")

        for prop in propertyChain[1:]:
            pathProperty = PathProperty._value2member_map_.get(prop, None)
            if pathProperty == PathProperty.STEM:
                selectedPath = selectedPath.parent / selectedPath.stem
            elif pathProperty == PathProperty.PARENT:
                selectedPath = selectedPath.parent
            else:
                raise Exception(f"Invalid path property: {prop}")

        if suffix is None:
            return selectedPath
        
        return selectedPath.with_suffix(suffix)
    
    def _selectPath(self, directory: str, fileName: str = None) -> Path:        
        if directory not in self.mapping:
            raise Exception(f"Invalid directory selected: {directory}") from AttributeError
        
        selectedDir = self.mapping[directory]

        if fileName is None:
            return selectedDir
        return selectedDir / fileName
