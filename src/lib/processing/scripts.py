from pathlib import Path
from lib.processing.stages import File
from lib.tools.logger import Logger
import lib.processing.processingFuncs as pFuncs

class Script:
    def __init__(self, scriptInfo: dict, outputDir: Path):
        self.outputDir = outputDir
        scriptInfo = scriptInfo.copy()
        
        self.path = scriptInfo.pop("path", None)
        self.function = scriptInfo.pop("function", None)
        self.args = scriptInfo.pop("args", [])
        self.kwargs = scriptInfo.pop("kwargs", {})
        self.outputs = scriptInfo.pop("outputs", [])
        self.outputStructure = scriptInfo.pop("outputStructure", "")
        self.outputProperties = scriptInfo.pop("outputProperties", {})

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError

        self.scriptRun = False

        for parameter in scriptInfo:
            Logger.debug(f"Unknown step parameter: {parameter}")

    def getOutputs(self, inputs: list[File] = []) -> list[File]:
        if self.outputs:
            return [File(self.outputDir / output, {}) for output in self.outputs]
        
        if self.outputStructure:
            parser = self.Parser()
            return [File(self.outputDir / output, {}) for output in parser.parse(self.outputStructure, inputs)]
        
        return []

    def run(self, overwrite: bool = False, verbose: bool = False, **kwargs: dict):
        if self.scriptRun:
            return
        
        if not overwrite and any(output.exists() for output in self.outputs):
            Logger.info(f"All outputs {self.outputs} exist and not overwriting, skipping '{self.function}'")
            return
        
        for output in self.getOutputs():
            output.delete()
        
        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {self.args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {self.kwargs}"
            print(msg)

        processFunction = pFuncs.importFunction(self.path, self.function)
        output = processFunction(*self.args, **self.kwargs)

        for outputFile in self.getOutputs():
            if not outputFile.exists():
                Logger.warning(f"Output {outputFile} was not created")

        return output

class Parser:
    def __init__(self):
        self.functionMap = {
            "STEM": self._stem
        }

    def _stem(self, filePath: Path) -> str:
        return str(filePath.stem)
    
    def parse(self, structure: str, inputs: list[File]) -> list[str]:
        start = structure.find("{")
        end = structure.find("}")

        prefix = structure[:start]
        suffix = structure[end+1:]
        body = structure[start+1:end]
        bodyFunction = self.functionMap[body]

        outputs = []
        for file in inputs:
            outputs.append(f"{prefix}{bodyFunction(file.filePath)}{suffix}")

        return outputs
