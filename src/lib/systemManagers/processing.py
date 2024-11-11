from pathlib import Path
from lib.processing.stages import File
from lib.processing.scripts import Script
from lib.tools.logger import Logger
import time
from datetime import datetime

class _Node:
    def __init__(self, script: Script, parents: list['_Node']):
        self.script = script
        self.parents = parents
        self.executed = False

    def getOutput(self) -> File:
        return self.script.output
    
    def getFunction(self) -> str:
        return self.script.function

    def execute(self, overwrite: bool, verbose: bool) -> tuple[bool, list[dict]]:
        metadata = []

        if self.executed:
            return True, metadata
        
        parentSuccess = True
        for parent in self.parents:
            success, metadata = parent.execute(overwrite, verbose)
            parentSuccess = parentSuccess and success
        
        if not parentSuccess:
            return False, metadata
        
        stattTime = time.perf_counter()
        success = self.script.run(overwrite, verbose)

        metadata.append({
            "function": self.getFunction(),
            "output": self.getOutput().filePath.name,
            "success": success,
            "duration": time.perf_counter() - stattTime,
            "timestamp": datetime.now().isoformat()
        })

        self.executed = success
        return success, metadata

class _Root(_Node):
    def __init__(self, file: File):
        self.file = file

    def getOutput(self) -> File:
        return self.file
    
    def execute(self, *args) -> tuple[bool, list]:
        return True, []

class ProcessingManager:
    def __init__(self, baseDir: Path, processingDir: Path):
        self.baseDir = baseDir
        self.processingDir = processingDir
        self.nodes: list[_Node] = []

    def _createNode(self, step: dict, parents: list[_Node]) -> _Node | None:
        inputs = [node.getOutput() for node in parents]
        try:
            script = Script(self.baseDir, self.processingDir, dict(step), inputs)
        except AttributeError as e:
            Logger.error(f"Invalid processing script configuration: {e}")
            return None
        
        return _Node(script, parents)
    
    def _addProcessing(self, node: _Node, processingSteps: list[dict]) -> _Node:
        for step in processingSteps:
            subNode = self._createNode(step, [node])
            node = subNode
        return node
    
    def getLatestNodeFiles(self) -> list[File]:
        return [node.getOutput() for node in self.nodes]

    def process(self, overwrite: bool = False, verbose: bool = False) -> tuple[bool, dict]:
        if all(isinstance(node, _Root) for node in self.nodes): # All root nodes, no processing required
            Logger.info("No processing required for any nodes")
            return True, {}

        if not self.processingDir.exists():
            self.processingDir.mkdir()

        metadata = {"steps": []}
        allSucceeded = True

        startTime = time.perf_counter()
        for node in self.nodes:
            success, stepMetadata = node.execute(overwrite, verbose)

            metadata["steps"].extend(stepMetadata)
            allSucceeded = allSucceeded and success

        metadata["totalTime"] = time.perf_counter() - startTime

        return allSucceeded, metadata

    def registerFile(self, file: File, processingSteps: list[dict]) -> bool:
        node = _Root(file)
        node = self._addProcessing(node, processingSteps)
        self.nodes.append(node)

    def addAllProcessing(self, processingSteps: list[dict]) -> bool:
        if not processingSteps:
            return
        
        for idx, node in enumerate(self.nodes):
            self.nodes[idx] = self._addProcessing(node, processingSteps)

    def addFinalProcessing(self, processingSteps: list[dict]) -> bool:
        if not processingSteps:
            return
        
        # First step of final processing should combine all chains to a single file
        finalNode = self._createNode(processingSteps[0], self.nodes)
        self.nodes = [self._addProcessing(finalNode, processingSteps[1:])]
