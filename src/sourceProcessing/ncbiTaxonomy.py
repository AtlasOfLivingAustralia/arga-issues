import pickle
import pandas as pd
import argparse
from pathlib import Path
from typing import Self

class Tree:
    class Node:
        def __init__(self, id: int):
            self.id = id
            self.parent = None
            self.rank = "Unknown"

            self.name = ""
            self.taxInfo = {}
        
        def setParent(self, parent: Self) -> None:
            if self.id != parent.id:
                self.parent = parent

        def setRank(self, rank: str) -> None:
            self.rank = rank

        def setName(self, name: str) -> None:
            self.name = name

        def getTaxInfo(self, includeOwn: bool = False) -> dict[str, str]:
            info = {}
            if self.parent is not None:
                info = self.parent.getTaxInfo(True)

            if includeOwn:
                # info.append({"ID": self.id, "Rank": self.rank, "Name": self.name})
                # info.append(f"{self.rank}: {self.name} ({self.id})")
                info[self.rank] = self.name

            return info.copy()

    def __init__(self):
        self.nodes = {}

    def exists(self, taxID: int) -> bool:
        return taxID in self.nodes

    def addNode(self, taxID: int, parentTaxID: int, rank: str) -> None:
        if parentTaxID not in self.nodes:
            self.nodes[parentTaxID] = self.Node(parentTaxID)

        if taxID not in self.nodes:
            self.nodes[taxID] = self.Node(taxID)

        self.nodes[taxID].setParent(self.nodes[parentTaxID])
        self.nodes[taxID].setRank(rank)

    def getNode(self, taxID: int) -> Node:
        return self.nodes[taxID]

    def getTaxonomy(self, taxID: int) -> dict[str, str]:
        return self.nodes[taxID].getTaxInfo()

    def toDataframe(self, rankColumns: str) -> pd.DataFrame:
        dataDict = {}
        for taxID, node in self.nodes.items():
            taxInfo = node.getTaxInfo()
            dataDict[taxID] = {key: value for key, value in taxInfo.items() if key in rankColumns}
            dataDict[taxID]["taxonRank"] = node.rank
            dataDict[taxID]["higherClassification"] = ';'.join(taxInfo.values())

        df =  pd.DataFrame.from_dict(dataDict, orient='index')
        df.index.name = "taxonID"
        return df

def splitLine(line: str) -> list[str]:
    return [element.strip('\t') for element in line.rstrip('|\n').split('|')]

def process(folderPath: Path, outputFilePath: Path) -> None:
    tree = Tree()
    
    print("Adding nodes")
    nodeData = {}
    with open(folderPath / "nodes.dmp") as fp:
        line = fp.readline()

        while line:
            taxID, parentTaxID, rank, embl, divID, iDivFlag, gcID, iGCFlag, mgcID, iMGCFlag, gHidden, sHidden, comments = splitLine(line)
            tree.addNode(int(taxID), int(parentTaxID), rank)
            line = fp.readline()

    print("Adding names")
    # Adding names
    with open(folderPath / "names.dmp") as fp:
        line = fp.readline()
        while line:
            taxID, name, uniqueName, nameClass = splitLine(line)
            if nameClass == 'scientific name':
                tree.getNode(int(taxID)).setName(name)

            line = fp.readline()

    # print("Dumping tree")
    # with open("../generatedFiles/taxonTree", "wb") as fp:
    #     pickle.dump(tree, fp)

    print("Creating Dataframe")
    rankColumns = ["kingdom", "phylum", "class", "order", "family", "subfamily", "genus", "subgenus", "species"]
    df = tree.toDataframe(rankColumns)
    
    print(f"Writing to file {outputFilePath}")
    outputFilePath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outputFilePath)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Get taxonomic tree for ncbi taxon ID's")
    parser.add_argument('id', type=int)
    args = parser.parse_args()

    with open("../generatedFiles/taxonTree", "rb") as fp:
        tree = pickle.load(fp)

    for line in tree.getTaxonomy(args.id):
        print(line)
