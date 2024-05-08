from lib.processing.stages import File
from pathlib import Path

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
