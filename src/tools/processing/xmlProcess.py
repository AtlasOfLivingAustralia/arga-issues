import argparse
import json
from pathlib import Path
from xml.etree import cElementTree as ET
from lib.subfileWriter import Writer
import lib.commonFuncs as cmn
import gc
import pandas as pd

class ElementContainer:
    def __init__(self, element: ET.Element):
        self.element = element
        self.tag = element.tag
        self.children = {}

        self.text = self.cleanText(element.text) if element.text else ""
        self.attributes = element.attrib.copy()

    def cleanText(self, text: str) -> str:
        if text is None:
            return

        for c in ("\n", "\r", "\t"): # Characters to remove
            text = text.replace(c, "")

        for x in ("B", "I", "P", "b", "i", "p"): # XML tags to remove
            text = text.replace(f"<{x}>", "").replace(f"</{x}>", "")
        
        return text.strip()
    
    def addChild(self, element: 'ElementContainer'):
        if element.tag not in self.children:
            self.children[element.tag] = [element]
            return
        
        self.children[element.tag].append(element)

    def extractAttributes(self, splitAttrib: dict, delete: bool = False) -> dict:
        extracted = {}

        for attribute, valueMap in splitAttrib.items():
            for attributeValue, newColumn in valueMap.items():
                if self.attributes.get(attribute, None) == attributeValue:
                    if delete: # Remove old attribute
                        self.attributes.pop(attribute) 

                    extracted |= {newColumn: self.text}
                    break

        return extracted

    def flatten(self, compressChildren: list = [], collectionExtract: dict = {}) -> dict:
        flat = {}

        if self.text:
            flat[f"{self.tag}_text"] = self.text

        for attr, value in self.attributes.items():
            flat[f"{self.tag}_{attr}"] = value

        for tag, children in self.children.items():
            if len(children) > 1 or tag in compressChildren:
                if tag in collectionExtract:
                    for child in children:
                        flat |= child.extractAttributes(collectionExtract[tag])

                flat |= {self.tag: [child.flatten(compressChildren) for child in children]}
                
            else:
                flat |= children[0].flatten(compressChildren, collectionExtract)

        return flat

def process(filePath: Path, outputFilePath: Path, encoding="utf-8", entryCount: int = 0, firstEntry: int = 0, subfileRows: int = 0, onlyIncludeTags: list = [], compressChild: list = [], collectionExtract: dict = {}):
    writer = Writer(outputFilePath.parent, "xmlProcessing", "xmlSection")

    if entryCount < 0:
        raise Exception(f"Invalid entry count {entryCount}, must be >= 0") from AttributeError

    if firstEntry < 0:
        raise Exception(f"Invalid first entry {firstEntry}, must be >= 0") from AttributeError
    
    if subfileRows < 0:
        raise Exception(f"Invalid subfile rows {subfileRows}, must be >= 0") from AttributeError

    lastEntry = (firstEntry + entryCount - 1) if entryCount > 0 else -1 # Ignore last entry if all entries requested

    parser = ET.XMLParser(encoding=encoding)
    iterator = ET.iterparse(filePath, events=('start', 'end'), parser=parser)
    _, root = next(iterator)

    event, element = next(iterator)
    topLevelTag = element.tag

    path = [ElementContainer(element)]
    data = []
    columns = []
    currentEntry = 0

    print(f'At entry: {currentEntry+1:,}', end='\r')
    for event, element in iterator:
        if event == 'start':
            path.append(ElementContainer(element))

            if element.tag == topLevelTag and len(path) == 1:
                currentEntry += 1
                print(f'At entry: {currentEntry+1:,}', end='\r')

        elif event == 'end':
            if element == root:
                break

            elementContainer = path.pop()

            if onlyIncludeTags and element.tag not in onlyIncludeTags:
                element.clear()
                root.clear()
                continue

            # Data writing only happens at end of top level tag
            if element.tag != topLevelTag or len(path) > 1: # Make sure path length 1 incase sub element has same name
                path[-1].addChild(elementContainer)
                element.clear()
                continue

            if len(data) >= firstEntry: # Only add data if it is within data entry range
                flatContainer = elementContainer.flatten(compressChild, collectionExtract)
                data.append(flatContainer)
                columns = cmn.extendUnique(columns, flatContainer.keys())
                if len(data) == lastEntry: # Exit if last entry reached
                    break

            if len(data) == subfileRows:
                df = pd.DataFrame.from_records(data, columns=columns)
                writer.writeDF(df)
                data.clear()
                columns.clear()
                del df
                gc.collect()

            element.clear()
            root.clear()

    # Write remaining data to file
    if data:
        df = pd.DataFrame.from_records(data, columns=columns)
        writer.writeDF(df)

    print()
    writer.oneFile(outputFilePath) # Compress to one file

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert xml to csv")
    parser.add_argument('inputFilePath', help="Path to xml file to parse")
    parser.add_argument('outputFilePath', help="Path to output csv file")
    parser.add_argument('-e', '--entries', type=int, default=0, help="Amount of entries to parse.")
    parser.add_argument('-s', '--subfile', type=int, default=0, help="Maximum entries per csv generated.")
    parser.add_argument('-f', '--firstEntry', type=int, default=0, help="First entry to parse from.")
    parser.add_argument('-t', '--tagProperties', help="Path to json file with tag properties. Properties file should have `onlyIncludeTags` to only use specific tags, `compressChild` for tags to compress children of, and `collectionExtract` for extracting fields from compressed collections.")
    args = parser.parse_args()

    inputPath = Path(args.inputFilePath)
    if not inputPath.exists():
        print(f"No file found at path: {inputPath}")
        exit()

    outputPath = Path(args.outputFilePath)

    tagProperties = Path(args.tagProperties)
    if not tagProperties.exists():
        print(f"No tagfile found at path: {tagProperties}")
        exit()

    with open(args.tagProperties) as fp:
        properties = json.load(fp)

    onlyIncludeTags = properties.get("onlyIncludeTags", [])
    compressChild = properties.get("compressChild", [])
    collectionExtract = properties.get("collectionExtract", {})

    process(inputPath, outputPath, args.entries, args.firstEntry, args.subfile, onlyIncludeTags, compressChild, collectionExtract)
