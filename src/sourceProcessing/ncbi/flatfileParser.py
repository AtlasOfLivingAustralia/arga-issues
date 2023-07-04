from pathlib import Path
from enum import Enum

class Section(Enum):
    INFO = "INFO"
    REFERENCE = "REFERENCE"
    COMMENT = "COMMENT"
    FEATURES = "FEATURES"
    ORIGIN = "ORIGIN"

class FlatFileParser:
    def __init__(self):
        self.seqBaseURL = "https://ftp.ncbi.nlm.nih.gov/genbank/"
        self.genbankBaseURL = "https://www.ncbi.nlm.nih.gov/nuccore/"
        self.fastaSuffix = "?report=fasta&format=text"

        self.sectionParserCallbacks = {
            Section.INFO: self.parseInfo,
            Section.REFERENCE: self.parseReferences,
            Section.COMMENT: self.parseComment,
            Section.FEATURES: self.parseFeatures,
            Section.ORIGIN: self.parseOrigin
        }

    def parse(self, filePath: Path, verbose: bool = False) -> list[dict]:
        with open(filePath) as fp:
            try:
                data = fp.read()
            except UnicodeDecodeError:
                print(f"Failed to read file: {filePath}")
                return []

        # Cut off header of file
        firstLocusPos = data.find("LOCUS")
        header = data[:firstLocusPos]
        data = data[firstLocusPos:]

        records = []
        for idx, entry in enumerate(data.split("//\n")[:-1], start=1): # Split into separate loci, exclude empty entry after last
            if verbose:
                print(f"Parsing entry: {idx}", end="\r")
            splitData = self.splitEntryBlocks(entry)
            output = {}
            for section, textBlock in splitData.items():
                if section in [Section.ORIGIN]: # Skip origin section
                    continue

                callBack = self.sectionParserCallbacks[section]
                output |= callBack(textBlock)

            # Attach seq file path and fasta file
            # output["seq_file"] = f"{self.seqBaseURL}{filePath.name}.gz"
            version = output.get("version", "")
            if version:
                output["genbank_url"] = f"{self.genbankBaseURL}{output['version']}"
                output["fasta_url"] = f"{self.genbankBaseURL}{output['version']}{self.fastaSuffix}"
            records.append(output)

        if verbose:
            print()

        return records

    def splitEntryBlocks(self, entryText: str) -> dict[Section, str]:
        headers = [Section.REFERENCE.value, Section.COMMENT.value, Section.FEATURES.value, Section.ORIGIN.value]
        referencesPos, commentPos, featuresPos, originPos = [entryText.find(header) for header in headers]

        infoBlock = entryText[:referencesPos]
        if commentPos > 0:
            referencesBlock = entryText[referencesPos:commentPos]
            commentBlock = entryText[commentPos:featuresPos]
        else:
            referencesBlock = entryText[referencesPos:featuresPos]
            commentBlock = ""
        featureBlock = entryText[featuresPos:originPos]
        originBlock = entryText[originPos:]

        blockData = {
            Section.INFO: infoBlock,
            Section.REFERENCE: referencesBlock,
            Section.COMMENT: commentBlock,
            Section.FEATURES: featureBlock,
            Section.ORIGIN: originBlock   
        }

        return blockData

    def getHeadingBlocks(self, textBlock: str, headings: list[str], skipJoining: list[str] = []) -> dict:
        # Get heading positions in text block
        foundHeadings = []
        headingPositions = []
        for heading in headings:
            position = textBlock.find(heading)
            if position >= 0:
                foundHeadings.append(heading)
                headingPositions.append(position)

        # Split block text at heading positions
        headingPositions.append(len(textBlock)) # Add length of block to end of positions to do string slicing
        headingBlocks = {}
        for idx, (head, startPos) in enumerate(zip(foundHeadings, headingPositions), start=1):
            text = textBlock[startPos:headingPositions[idx]]
            text = [line.strip() for line in text.split("\n")]
            text[0] = text[0].replace(head, "").strip() # Clean off heading in block of text

            if head not in skipJoining: # Create 1 line of text when joining
                text = " ".join(text).strip()

            headingBlocks[head.lower()] = text

        return headingBlocks
    
    def parseInfo(self, infoBlock: str) -> dict:
        headings = ["LOCUS", "DEFINITION", "ACCESSION", "VERSION", "KEYWORDS", "SOURCE", "ORGANISM"]

        # Manually split ORGANISM so don't join automatically
        info = self.getHeadingBlocks(infoBlock, headings, ["ORGANISM"])
        try:
            organism = info.pop("organism")
            info["species"] = organism[0]
            info["higherClassification"] = ''.join(organism[1:])
        except:
            print(infoBlock)
            print(info)
        
        # Split the compacted locus line of values into separate values
        locusHeadings = ["locus", "basepairs", "", "type", "shape", "val", "date"]
        splitValues = [value.strip() for value in info["locus"].split()]
        for name, value in zip(locusHeadings, splitValues):
            if name: # If the new name exists, otherwise skip that value
                info[name] = value

        return info

    def parseReferences(self, referencesBlock: str) -> dict:
        headings = ["REFERENCE", "AUTHORS", "TITLE", "JOURNAL", "PUBMED"]

        references = []
        for reference in referencesBlock.split(headings[0]):
            reference = self.getHeadingBlocks(headings[0] + reference, headings)
            splitRef = reference.pop("reference").split(' ', 1)
            if len(splitRef) == 1: # no bases
                base = ""
            else:
                base = splitRef[1].strip().lstrip('(bases').rstrip(')')
            
            if splitRef[0]:
                references.append({"reference": splitRef[0], "bases": base} | reference)

        return {"referenes": references}
    
    def parseComment(self, commentBlock: str) -> dict:
        return {"comment": commentBlock.replace("\n", " ")}

    def parseFeatures(self, featureBlock: str) -> dict:
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

        return {"genes": features}

    def parseOrigin(self, originBlock: str) -> dict:
        # origin = ""
        # origin = []
        # for line in originBlock.split('\n')[1:]:
        #     if not line:
        #         continue
            
        #     basepairs, seq = line.strip().split(" ", 1)
        #     origin.append({"basepairs": basepairs, "seq": seq})

        origin = "".join(line.split(" ", 1)[1].replace(" ", "") for line in originBlock.split("\n")[1:-1])
        return {"origin": origin}
