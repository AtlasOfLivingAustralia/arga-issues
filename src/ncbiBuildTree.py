import pickle
from xml.etree.ElementInclude import include
import pandas as pd

class Tree:
    class Node:
        def __init__(self, id):
            self.id = id
            self.parent = None
            self.rank = "Unknown"

            self.name = ""
            self.taxInfo = {}
        
        def setParent(self, parent):
            if self.id != parent.id:
                self.parent = parent

        def setRank(self, rank):
            self.rank = rank

        def setName(self, name):
            self.name = name

        def getTaxInfo(self, includeOwn=False):
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

    def exists(self, taxID):
        return taxID in self.nodes

    def addNode(self, taxID, parentTaxID, rank):
        if parentTaxID not in self.nodes:
            self.nodes[parentTaxID] = self.Node(parentTaxID)

        if taxID not in self.nodes:
            self.nodes[taxID] = self.Node(taxID)

        self.nodes[taxID].setParent(self.nodes[parentTaxID])
        self.nodes[taxID].setRank(rank)

    def getNode(self, taxID):
        return self.nodes[taxID]

    def getTaxonomy(self, taxID):
        return self.nodes[taxID].getTaxInfo()

    def toDataframe(self, rankColumns):
        dataDict = {}
        for taxID, node in self.nodes.items():
            taxInfo = node.getTaxInfo()
            dataDict[taxID] = {key: value for key, value in taxInfo.items() if key in rankColumns}
            dataDict[taxID]["taxonRank"] = node.rank
            dataDict[taxID]["higherClassification"] = ';'.join(taxInfo.values())

        df = pd.DataFrame.from_dict(dataDict, orient='index')
        df.index.name = "taxonID"
        df.to_csv("../data/ncbiTaxonomy.csv")

def splitLine(line):
    return [element.strip('\t') for element in line.rstrip('|\n').split('|')]

if __name__ == '__main__':
    # Loading node data
    tree = Tree()

    print("Adding nodes")
    nodeData = {}
    with open("../data/taxdump/nodes.dmp") as fp:
        line = fp.readline()

        while line:
            taxID, parentTaxID, rank, embl, divID, iDivFlag, gcID, iGCFlag, mgcID, iMGCFlag, gHidden, sHidden, comments = splitLine(line)
            tree.addNode(int(taxID), int(parentTaxID), rank)
            line = fp.readline()

    print("Adding names")
    # Adding names
    with open("../data/taxdump/names.dmp") as fp:
        line = fp.readline()
        while line:
            taxID, name, uniqueName, nameClass = splitLine(line)
            if nameClass == 'scientific name':
                tree.getNode(int(taxID)).setName(name)

            line = fp.readline()

    print("Dumping tree")
    with open("../generatedFiles/taxonTree", "wb") as fp:
        pickle.dump(tree, fp)

    print("Creating Dataframe")
    rankColumns = ["kingdom", "phylum", "class", "order", "family", "subfamily", "genus", "subgenus", "species"]
    tree.toDataframe(rankColumns)
