from pathlib import Path
import pandas as pd
from enum import Enum
from lib.tools.logger import Logger

class DumpFiles(Enum):
    NODES = "nodes.dmp"
    NAMES = "names.dmp"
    DIVISIONS = "divisions.dmp"
    GENETIC_CODES = "gencode.dmp"
    DELETED_NODES = "delnodes.dmp"
    MERGED_NODES = "merged.dmp"
    CITATIONS = "citations.dmp"

headings = {
    DumpFiles.NODES: [
        "tax_id",
        "parent_tax_id",
        "rank",
        "embl_code",
        "division_id",
        "inherited_div_flag",
        "genetic_code_id",
        "inherited_GC_flag",
        "mitochondrial_genetic_code_id",
        "inherited_MGC_flag",
        "GenBank_hidden_flag",
        "hidden_subtree_root_flag",
        "comments"
    ],
    DumpFiles.NAMES: [
        "tax_id",
        "name_txt",
        "unique_name",
        "name_class"
    ],
    DumpFiles.DIVISIONS: [
        "division_id",
        "division_cde",
        "division_name",
        "comments"
    ],
    DumpFiles.GENETIC_CODES: [
        "genetic_code_id",
        "abbreviation",
        "name",
        "cde",
        "starts"
    ],
    DumpFiles.DELETED_NODES: [
        "tax_id"
    ],
    DumpFiles.MERGED_NODES: [
        "old_tax_id",
        "new_tax_id",
    ],
    DumpFiles.CITATIONS: [
        "cit_id",
        "cit_key",
        "pubmed_id",
        "medline_id",
        "url",
        "text",
        "taxid_list"
    ]
}

class TaxonNode:
    def __init__(self, id: int):
        self.id = id
        self.parent = None
        self.rank = "Unknown"

        self.name = ""
        self.taxInfo = {}
    
    def setParent(self, parent: 'TaxonNode') -> None:
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
            info[self.rank] = self.name

        return info.copy()

class TaxonTree:
    def __init__(self):
        self.nodes: dict[int, TaxonNode] = {}
        self.taxonRanks = [
            "kingdom",
            "phylum",
            "class",
            "order",
            "family",
            "subfamily",
            "genus",
            "subgenus",
            "species"
        ]

    def exists(self, taxID: int) -> bool:
        return taxID in self.nodes

    def addNode(self, taxID: int, parentTaxID: int, rank: str) -> None:
        if parentTaxID not in self.nodes:
            self.nodes[parentTaxID] = TaxonNode(parentTaxID)

        if taxID not in self.nodes:
            self.nodes[taxID] = TaxonNode(taxID)

        self.nodes[taxID].setParent(self.nodes[parentTaxID])
        self.nodes[taxID].setRank(rank)

    def getNode(self, taxID: int) -> TaxonNode:
        return self.nodes[taxID]

    def getTaxonomy(self, taxID: int) -> dict[str, str]:
        return self.nodes[taxID].getTaxInfo()

    def toDataframe(self) -> pd.DataFrame:
        dataDict = {}
        for taxID, node in self.nodes.items():
            taxInfo = node.getTaxInfo(True)
            dataDict[taxID] = {"name": node.name, "parentTaxonID": int(node.parent.id) if node.parent is not None else None}
            dataDict[taxID] |= {key: value for key, value in taxInfo.items() if key in self.taxonRanks}

        df = pd.DataFrame.from_dict(dataDict, orient='index')
        df["parentTaxonID"] = df["parentTaxonID"].apply(lambda x: str(x).replace(".0", "")) # Ints still saving with 0, need this hack
        df.index.name = "taxonID"
        return df
    
def splitLine(line: str) -> list[str]:
    return [element.strip('\t') for element in line.rstrip('|\n').split('|')]

def parse(dumpFolder: Path, outputFile: Path) -> None:
    # Build taxon tree
    Logger.info("Building taxonomy tree from nodes")
    tree = TaxonTree()

    with open(dumpFolder / DumpFiles.NODES.value) as fp:
        line = fp.readline()
        while "|" in line:
            taxID, parentTaxID, rank, *_ = splitLine(line) # Ignore values beyond first 3
            tree.addNode(int(taxID), int(parentTaxID), rank)
            
            line = fp.readline()

    # Adding names to nodes
    Logger.info("Adding names to nodes")
    with open(dumpFolder / DumpFiles.NAMES.value) as fp:
        line = fp.readline()
        while "|" in line:
            taxID, name, _, nameClass = splitLine(line)
            if nameClass == "scientific name":
                node = tree.getNode(int(taxID))
                node.setName(name)

            line = fp.readline()

    Logger.info("Compiling tree to dataframe and writing to file")
    df = tree.toDataframe()
    df.to_csv(outputFile)
