from pathlib import Path
from lib.processing.stages import File
from lib.processing.scripts import Script

class _Node:
    def __init__(self, file: File, upperBranch: '_Branch'):
        self.file = file
        self.upperBranch = upperBranch
        self.lowerBranch = None

    def attachBranch(self, branch: '_Branch'):
        self.lowerBranch = branch

    def runBranch(self, overwrite: bool = False):
        if self.lowerBranch is not None:
            self.lowerBranch.execute(overwrite)

class _Branch:
    def __init__(self, script: Script, parents: list[_Node]):
        self.script = script
        self.parentNodes = parents
        self.childrenNodes = [_Node(file, self) for file in script.getOutputs([node.file for node in self.parentNodes])]

    def execute(self, overwrite: bool = False, chain: bool = True):
        self.script.run(overwrite)
        if not chain:
            return
        
        for node in self.childrenNodes:
            node.runBranch(overwrite)

class ProcessingTree:
    def __init__(self, rootNode: _Node):
        self.root = rootNode
        self.lowestNodes = [rootNode]

    def extend(self, script: Script):
        branch = _Branch(script, self.lowestNodes)

        for node in self.lowestNodes:
            node.attachBranch(branch)

        self.lowestNodes = branch.childrenNodes

class ProcessingManager():
    def __init__(self, processingDir: Path):
        self.processingDir = processingDir
        self.trees: list[ProcessingTree] = []

    def process(self, overwrite: bool = False, verbose: bool = False) -> None:
        for tree in self.trees:
            tree.root.runBranch(overwrite)

    def getLatestNodes(self) -> list[File]:
        nodes: list[_Node] = []
        for tree in self.trees:
            for node in tree.lowestNodes:
                if node not in nodes:
                    nodes.append(node)

        return [node.file for node in nodes]

    def registerFile(self, file: File) -> ProcessingTree:
        node = _Node(file, None)
        tree = ProcessingTree(node)
        self.trees.append(tree)
        return tree

    def addProcessing(self, tree: ProcessingTree, processingSteps: list[dict]):
        for step in processingSteps:
            script = Script(step, self.processingDir)
            tree.extend(script)

    def addAllProcessing(self, processingSteps: list[dict]):
        for tree in self.trees:
            self.addProcessing(tree, processingSteps)
