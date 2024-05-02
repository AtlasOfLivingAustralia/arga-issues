from pathlib import Path
from lib.processing.stages import File, Script

class _Node:
    def __init__(self, file: File, upperBranch: '_Branch'):
        self.file = file
        self.upperBranch = upperBranch
        self.lowerBranch = None

    def attachBranch(self, branch: '_Branch'):
        self.lowerBranch = branch

class _Branch:
    def __init__(self, script: Script, parents: list[_Node]):
        self.script = script
        self.parentNodes = parents
        self.childrenNodes = [_Node(file, self) for file in script.outputs]

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
        self.trees = []

    def registerFile(self, file: File) -> ProcessingTree:
        node = _Node(file)
        tree = ProcessingTree(node)
        self.trees.append(tree)
        return tree

    def addProcessing(self, tree: ProcessingTree, processingSteps: list[dict]):
        for step in processingSteps:
            script = Script(step, self.processingDir)
            tree.extend(script)

    def addPerFileProcessing(self, processingSteps: list[dict]):
        for tree in self.trees:
            self.addProcessing(tree, processingSteps)

    def addFinalProcessing(self, processingSteps):
        for step in processingSteps:
            pass