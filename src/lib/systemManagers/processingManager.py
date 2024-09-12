from pathlib import Path
from lib.processing.stages import File
from lib.processing.scripts import Script
from lib.tools.logger import Logger

class _Node:
    def __init__(self, script: Script, parents: list['_Node']):
        self.script = script
        self.parents = parents
        self.executed = False

    def getOutput(self) -> File:
        return self.script.output

    def execute(self, overwrite: bool, verbose: bool) -> bool:
        if self.executed:
            return True
        
        parentSuccesses = []
        for parent in self.parents:
            parentSuccesses.append(parent.execute(overwrite, verbose))
        
        if not all(parentSuccesses):
            return False
        
        success = self.script.run(overwrite, verbose)
        self.executed = success
        return success

class _Root(_Node):
    def __init__(self, file: File):
        self.file = file

    def getOutput(self) -> File:
        return self.file
    
    def execute(self, *args) -> bool:
        return True

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

    def process(self, overwrite: bool = False, verbose: bool = False) -> bool:
        if all(isinstance(node, _Root) for node in self.nodes): # All root nodes, no processing required
            Logger.info("No processing required for any nodes")
            return True

        if not self.processingDir.exists():
            self.processingDir.mkdir()

        successes = []
        for node in self.nodes:
            successes.append(node.execute(overwrite, verbose))

        return all(successes)

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
