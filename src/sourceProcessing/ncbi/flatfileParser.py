import argparse
import numpy as np
import sys
from pathlib import Path
import csv

def rawBlockToSections(block, headings, skipJoins=[]):
    headingPositions = getHeadingPositions(block, headings)
    return getHeadingBlocks(block, headingPositions, skipJoins)

def getHeadingPositions(block, headings):
    headingPositions = {}

    for heading in headings:
        position = block.find(heading)
        if position >= 0:
            headingPositions[heading] = position

    return headingPositions

def getHeadingBlocks(block, headingPositions, skipJoins=[]):
    headings = headingPositions.keys()
    positions = list(headingPositions.values()) + [len(block)]

    headingBlocks = {}
    for idx, (heading, position) in enumerate(zip(headings, positions)):
        blockText = block[position:positions[idx+1]]
        blockText = [line.strip() for line in blockText.split('\n')]
        if heading not in skipJoins:
            blockText = ''.join(blockText).replace(heading, '').strip()
        else:
            blockText[0] = blockText[0].replace(heading, '').strip()

        headingBlocks[heading.lower()] = blockText

    return headingBlocks

def parseInfo(infoBlock):
    headings = ["LOCUS", "DEFINITION", "ACCESSION", "VERSION", "KEYWORDS", "SOURCE", "ORGANISM"]
    locusHeadings = ["locus", "basepairs", "", "type", "shape", "val", "date"]

    info = rawBlockToSections(infoBlock, headings, ["ORGANISM"])
    organism = info.pop("organism")
    info["species"] = organism[0]
    info["higherClassification"] = ''.join(organism[1:])

    # Split the compacted locus line of values into separate values
    splitValues = [value.strip() for value in info["locus"].split()]
    for name, value in zip(locusHeadings, splitValues):
        if name: # If the new name exists, otherwise skip that value
            info[name] = value

    return info

def parseReferences(referencesBlock):
    headings = ["REFERENCE", "AUTHORS", "TITLE", "JOURNAL", "PUBMED"]

    references = []
    for reference in referencesBlock.split(headings[0]):
        reference = rawBlockToSections(headings[0] + reference, headings)
        splitRef = reference.pop("reference").split(' ', 1)
        if len(splitRef) == 1: # no bases
            base = np.NAN
        else:
            base = splitRef[1].strip().lstrip('(bases').rstrip(')')
        
        if splitRef[0]:
            references.append({"reference": splitRef[0], "bases": base} | reference)

    return references

def parseFeatures(featureBlock):
    featureList = [] # A list of all features
    lastKey = None # Last key updated for data over multiple lines
    featureIndent = 21 # Amount of spaces that features information is indented

    for line in featureBlock.split('\n')[1:]: # Split by newline and ignore first line
        data = line[featureIndent:]
        data = data.replace('"', '')

        if not data:
            continue

        if not line.startswith(' ' * featureIndent): # Line contains a heading
            heading = line[:featureIndent].strip()
            featureList.append({"feature": heading, "range": data})

        else: # Data line with no heading
            if data[0] == '/': # New data point
                data = data[1:]
                if '=' in data:
                    key, value = data.split('=', 1)
                else:
                    key = "property"
                    value = data

                featureList[-1][key] = value
                lastKey = key
                
            else: # Data value is part of value from previous line
                lastData = featureList[-1].setdefault(lastKey, "default")
                featureList[-1][lastKey] = lastData + data

    features = []
    for featureDict in featureList:
        feature = featureDict.pop("feature")
        gene = featureDict.pop("gene", "default")

        # geneInfo = features.setdefault(gene, {})
        geneInfo = {}

        if feature != "gene":
            featureDict = {f"{feature}_{key}": value for key, value in featureDict.items()}

        features.append({"gene": gene} | geneInfo | featureDict)

    return features

def parseOrigin(originBlock):
    origin = []
    for line in originBlock.split('\n')[1:]:
        if not line:
            continue
        
        basepairs, seq = line.strip().split(' ', 1)
        origin.append({"basepairs": basepairs, "seq": seq})

    return origin

def process(filePath):
    if not isinstance(filePath, Path):
        filePath = Path(filePath)

    outputFilePath = filePath.parent / f"{filePath.stem}.csv"
    writeHeader = True

    with open(filePath) as fp:
        data = fp.read()

    firstLocus = data.find("LOCUS")
    header = data[:firstLocus]

    data = data[firstLocus:]
    data = data.split("//\n")

    for entry in data[:-1]: # Exclude empty item after // at end of file
        referencesRef = entry.find("REFERENCE")
        featuresRef = entry.find("FEATURES")
        originRef = entry.rfind("ORIGIN")

        infoBlock = entry[:referencesRef]
        referencesBlock = entry[referencesRef:featuresRef]
        featureBlock = entry[featuresRef:originRef]
        originBlock = entry[originRef:]

        info = parseInfo(infoBlock)
        references = parseReferences(referencesBlock)
        features = parseFeatures(featureBlock)
        origin = parseOrigin(originBlock)

        data = info | {"genes": features} | {"references": references} | {"origin": origin}
        with open(outputFilePath, "a", newline='', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, data.keys())

            if writeHeader:
                writer.writeheader()
                writeHeader = False

            writer.writerow(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parser for NCBI sequence flatfiles")
    parser.add_argument('file', help="Sequence file to parse.")
    parser.add_argument('-p', '--path', default="../data/raw/sequenceFiles", help="Path to sequence file folder.")
    args = parser.parse_args()

    filePath = Path(args.path, args.file)
    if not filePath.exists():
        print(f"No file found at  {filePath}")
        sys.exit()

    process(filePath)
