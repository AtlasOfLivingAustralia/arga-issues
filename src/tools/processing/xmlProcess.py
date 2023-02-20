import argparse
import time
import gc
import json
import csv
import sys
import os
from pathlib import Path
from xml.etree import cElementTree as ET
from lib.commonFuncs import getColumns, extendUnique, addUniqueEntry

class Writer:
    def __init__(self, rootDir, subfileDir, filePrefix):
        self.rootDir = rootDir
        self.outputDir = rootDir if not subfileDir else rootDir / subfileDir
        self.outputDir.mkdir(parents=True, exist_ok=True)
        self.filePrefix = filePrefix

        self.writtenFiles = []
        self.globalColumns = []

    def write(self, columns, entryData):
        filePath = self.outputDir / f"{self.filePrefix}_{len(self.writtenFiles)}.csv"

        with open(filePath, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, columns)
            writer.writeheader()

            for line in entryData:
                writer.writerow(line)
        
        self.writtenFiles.append(filePath)
        self.globalColumns = extendUnique(self.globalColumns, columns)

    def oneFile(self, fileName, deleteFolder=True):
        outputFile = self.rootDir / f"{fileName}.csv"

        if len(self.writtenFiles) == 1:
            self.writtenFiles[0].rename(outputFile)
            if deleteFolder and self.outputDir != self.rootDir:
                self.outputDir.rmdir()
            return

        print(f"Combining into one file at {outputFile}")
        with open(outputFile, 'w', newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, self.globalColumns)
            writer.writeheader()

            for file in self.writtenFiles:
                with open(file, encoding='utf-8') as fp:
                    reader = csv.DictReader(fp)
                    for row in reader:
                        writer.writerow(row)

                file.unlink()
        self.writtenFiles = [outputFile]

def cleanText(text):
    if text is None:
        return

    for c in ("\n", "\r", "\t"): # Characters to remove
        text = text.replace(c, "")

    for x in ("B", "I", "P", "b", "i", "p"): # XML tags to remove
        text = text.replace(f"<{x}>", "").replace(f"</{x}>", "")
    
    return text.strip()

def addElementsFromTags(element, path, properties, tags):
    if element.tag not in tags:
        return

    attributes = element.attrib
    tagAttributes = tags[element.tag]

    if tagAttributes and not attributes:
        return

    for attr in tagAttributes:
        splitAttr = attr.split('=')

        attr = splitAttr.pop(0)
        value = "" if len(splitAttr) == 0 else splitAttr[0] # Specific attr value required
            
        if attr not in attributes:
            continue

        text = cleanText(element.text) if element.text else ""
        attrValue = attributes[attr]
        prefix = element.tag

        if value: # Required value
            if value == attrValue: # Value matches attr value
                addUniqueEntry(properties, f"{prefix}_{value}", text) # Add text of tag
        else: # No value required
            addUniqueEntry(properties, f"{prefix}_{attr}", attrValue) # Add attribute value

def addElementProperties(element, path, properties, limitDuplicates=0, onlyTags={}):
    if onlyTags:
        if element.tag not in onlyTags:
            return
        
        onlyAttributes = {}
        for attr in onlyTags[element.tag]:
            s = attr.split('=', 1)
            onlyAttributes[s[0]] = s[1] if len(s) > 1 else ""
    else:
        onlyAttributes = {}

    prefix = element.tag
    attributes = element.attrib
    text = cleanText(element.text) if element.text else ""
    
    if not attributes: # No attributes
        if onlyAttributes: # Relevant tag requires attributes
            return

        if text:
            addUniqueEntry(properties, prefix, text, limitDuplicates)
        return

    if not text:
        for attr, value in attributes.items():
            if not onlyAttributes or (attr in onlyAttributes and not onlyAttributes[attr]):
                addUniqueEntry(properties, f"{prefix}_{attr}", value, limitDuplicates)
        return

    primaryValue = list(attributes.values())[0]
    matchingTags = len(path[-2].findall(element.tag))

    matchingAttrs = [attr for attr in onlyAttributes if attr in attributes]
    matchingValueAttrs = [attr for attr in matchingAttrs if attributes[attr] == onlyAttributes[attr] or not onlyAttributes[attr]]

    if onlyAttributes:
        if not len(matchingAttrs): # If we only care about certain tags, make sure the tag attributes exist
            return
        
        for attr in matchingValueAttrs:
            attributes.pop(attr) # Clean off attributes that are already targeted

    if matchingTags == 1: # This is only tag, treat all attributes as a propertties
        addUniqueEntry(properties, prefix, text, limitDuplicates)
        addUniqueEntry(properties, f"{prefix}_properties", attributes, limitDuplicates)
        return
    
    # Multiple tags, differentiate by first attribute and save rest as properties
    addUniqueEntry(properties, f"{prefix}_{primaryValue}", text, limitDuplicates)
    if len(attributes) > 1:
        addUniqueEntry(properties, f"{prefix}_{primaryValue}_properties", {attr: value for idx, (attr, value) in enumerate(attributes.items()) if idx > 0}, limitDuplicates)

def process(filePath, subfileRows=0, limitEntries=0, limitDuplicates=0, firstEntry=0, filePrefix="xmlsection", onefile="", skipTags=[], onlyTags={}, subDir=""):
    subfileRows = max(subfileRows, 0) # Make sure subfiles have positive rows, 0 means no subfiling
    writer = Writer(filePath.parent, subDir, filePrefix)

    entryData = []
    outputColumns = []

    iterator = ET.iterparse(filePath, events=('start', 'end'))
    _, root = next(iterator)

    event, element = next(iterator)
    tag = element.tag

    toplevelTag = tag
    path = []
    entryNumber = 0

    while tag is not root.tag:
        if event == 'start':
            path.append(element)
            if tag == toplevelTag:
                entryData.append({})
                entryNumber += 1
                print(f'At entry: {entryNumber:,}', end='\r')

        else: # end event
            skip = False
            for skipTag in skipTags:
                if skipTag in path:
                    skip = True
                    break
            
            if not skip:
                addElementProperties(element, path, entryData[-1], limitDuplicates, onlyTags)

            if tag == toplevelTag and len(path) == 1:
                outputColumns = extendUnique(outputColumns, entryData[-1].keys())

                # Break out if set amount of entries is valid and offset by first entry
                brk = limitEntries > 0 and entryNumber >= firstEntry + limitEntries

                if brk or subfileRows > 0 and entryNumber % subfileRows == 0:
                    writer.write(outputColumns, entryData)

                    entryData.clear()
                    outputColumns.clear()
                    gc.collect()

                element.clear()
                root.clear()

                if brk:
                    break

            path.pop()

        event, element = next(iterator)
        tag = element.tag
    else: # Got to the end of the root tag
        if entryData: # Write remaining data to file after loop exits if there is entry data leftover
            writer.write(outputColumns, entryData)

    print()

    if onefile:
        writer.oneFile(onefile)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert xml to csv")
    parser.add_argument('filePath', help="Path to xml file to parse")
    parser.add_argument('-p', '--prefix', default="xmlsection", help="File prefix for output files")
    parser.add_argument('-e', '--entries', type=int, default=0, help="Amount of entries to parse.")
    parser.add_argument('-s', '--subfile', type=int, default=0, help="Maximum entries per CSV generated.")
    parser.add_argument('-f', '--firstentry', type=int, default=0, help="First entry to parse from.")
    parser.add_argument('-l', '--limitduplicates', type=int, default=0, help="Limit for the amount of duplicate columns. A value of 0 means no limit.")
    parser.add_argument('-t', '--time', action='store_true', help="Keep time of processing.")
    parser.add_argument('-o', '--onefile', default="", nargs="?", const="output", help="One file output, can give filename to save to specific file.")
    args = parser.parse_args()

    path = Path(args.filePath)
    if not path.exists():
        print(f"No file found at path: {path}")
        sys.exit()

    if args.time:
        tic = time.perf_counter()

    process(
        path,
        args.subfile,
        limitEntries=args.entries,
        firstEntry=args.firstentry,
        filePrefix=args.prefix,
        onefile=args.onefile
    )

    if args.time:
        seconds = time.perf_counter() - tic
        minutes, seconds = divmod(seconds, 60)
        out = "Completed parsing in "
        if minutes > 0:
            out += f"{minutes:.0f} minutes, "
        out += f"{seconds:.02f} seconds"
        print(out)
