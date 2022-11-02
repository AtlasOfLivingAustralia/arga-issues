from xml.etree import cElementTree as ET
import argparse
import pandas as pd
import os
import config
import time
import gc

class DataHandler:
    def __init__(self, subfileRows, duplicateLimit, outputDir, filePrefix="result"):
        self.subfileRows = max(subfileRows, 0)
        self.duplicateLimit = max(duplicateLimit, 0)
        self.outputDir = outputDir
        self.filePrefix = filePrefix

        self.index = 0
        self.data = {}
        self.subfileIndex = 0

        self.charReplacements = ("\n", "\r", "\t")
        self.xmlReplacements = ("B", "I", "P", "b", "i", "p")

    def handleElement(self, element):
        self.data[self.index] = self.getElementProps(element, {})
        element.clear()
        self.index += 1

        if self.index == self.subfileRows:
            self.writeToFile()
            self.index = 0
            self.subfileIndex += 1
            del self.data
            gc.collect()
            self.data = {}

    def addUniqueEntry(self, dictionary, key, value):
        if key not in dictionary:
            dictionary[key] = value
            return

        suffixNum = 1
        while suffixNum != self.duplicateLimit:
            newKey = f"{key}_{suffixNum}"
            if newKey not in dictionary:
                dictionary[key] = value
                return
            n += 1

    def getElementProps(self, element, rows):
        children = list(element)
        for child in children:
            rows = self.getElementProps(child, rows)

        if not children and element.text:
            value = self.handleText(element.text)

            # Attribute data
            for attr, property in element.attrib.items():
                colname = f"{element.tag}_{attr}_{property}"
                self.addUniqueEntry(rows, colname, value)

        else: 
            # Attribute data
            for attr, value in element.attrib.items():
                colname = f"{element.tag}_{attr}"
                self.addUniqueEntry(rows, colname, value)

            # Text data
            text = self.handleText(element.text)
            if text:
                self.addUniqueEntry(rows, element.tag, text)

        return rows

    def handleText(self, text):
        if text is None:
            return

        for c in self.charReplacements:
            text = text.replace(c, "")

        for x in self.xmlReplacements:
            text = text.replace(f"<{x}>", "").replace(f"</{x}>", "")
        
        return text.strip()

    def writeToFile(self):
        df = pd.DataFrame.from_dict(self.data, "index")
        df.to_csv(os.path.join(self.outputDir, f"{self.filePrefix}_{self.subfileIndex}.csv"), index=False)
        del df

    def exit(self):
        if self.index != 0:
            self.writeToFile()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert xml to csv")
    parser.add_argument('filepath', help="Path to xml file to parse")
    parser.add_argument('toplevel', help="Outer most element name for entries")
    parser.add_argument('-o', '--outputdir', default=config.resultsFolder, help="Location to put results")
    parser.add_argument('-e', '--entries', type=int, default=0, help="Amount of entries to parse.")
    parser.add_argument('-s', '--subfile', type=int, default=0, help="Maximum entries per CSV generated.")
    parser.add_argument('-f', '--firstentry', type=int, default=0, help="First entry to parse from.")
    parser.add_argument('-l', '--limitduplicates', type=int, default=0, help="Limit for the amount of duplicate columns. A value of 0 means no limit.")
    parser.add_argument('-t', '--time', action='store_true', help="Keep time of processing.")
    parser.add_argument('-c', '--count', action='store_true', help="Only count entries, don't parse.")
    args = parser.parse_args()

    # Bioproject top level: 'Package'
    # Biosameple top level: 'BioSample'
    
    depth = 0
    within = False
    entryNumber = 0
    maxDepth = 0

    handler = DataHandler(args.subfile, args.limitduplicates, args.outputdir)

    if args.time:
        tic = time.time()

    for event, elem in ET.iterparse(args.filepath, events=("start", "end")):
        if elem.tag == args.toplevel:
            if depth == 0: # Entering top level element
                within = True
                entryNumber += 1
                print(f'At entry: {entryNumber:,}', end='\r')
                if not args.count and entryNumber >= args.firstentry:
                    handler.handleElement(elem)

            elif depth == 1: # Exiting top level element
                within = False
                depth = 0
                if args.entries > 0 and entryNumber >= args.firstentry + args.entries:
                    break

        if within:
            depth += 1 if event == "start" else -1
            if depth > maxDepth:
                maxDepth = depth

    print()

    if args.count:
        print(f"Maximum depth: {maxDepth}")

    if args.time:
        seconds = time.time() - tic
        minutes, seconds = divmod(seconds, 60)
        out = "Completed parsing in "
        if minutes > 0:
            out += f"{minutes:.0f} minutes, "
        out += f"{seconds:.02f} seconds"
        print(out)

    handler.exit()
    