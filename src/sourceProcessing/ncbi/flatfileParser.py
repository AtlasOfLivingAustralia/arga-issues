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

            entryData = self.parseEntry(entry)

            # Attach seq file path and fasta file
            # output["seq_file"] = f"{self.seqBaseURL}{filePath.name}.gz"
            version = entryData.get("version", "")
            if version:
                entryData["genbank_url"] = f"{self.genbankBaseURL}{version}"
                entryData["fasta_url"] = f"{self.genbankBaseURL}{version}{self.fastaSuffix}"

            # Add specimen field
            for value in (entryData.get("specimen_voucher", None), entryData.get("isolate", None), f"NCBI_{entryData['accession']}_specimen"):
                if value is not None:
                    entryData["specimen"] = value
                    break

            records.append(entryData)

        if verbose:
            print()

        return records
    
    def parseEntry(self, entryBlock: str) -> dict:
        sections = self.getSections(entryBlock)

        entryData = {}
        for section in sections:
            heading, data = section.split(" ", 1)
            # headings = ["LOCUS", "DEFINITION", "ACCESSION", "VERSION", "DBLINK", "KEYWORDS", "SOURCE", "ORGANISM"]

            if heading == "LOCUS":
                parameters = ["locus", "basepairs", "", "type", "shape", "seq_type", "date"]
                entryData |= {param: value.strip() for param, value in zip(parameters, data.split()) if param}

            elif heading == "DEFINITION":
                entryData["definition"] = self.flattenBlock(data)

            elif heading in ("ACCESSION", "VERSION", "COMMENT"):
                entryData[heading.lower()] = data.strip()

            elif heading == "DBLINK":
                dbs = {}
                for line in data.split("\n"):
                    if not line:
                        continue

                    key, value = line.split(":")
                    dbs[key.strip().lower()] = value.strip()

                entryData |= dbs

            elif heading == "KEYWORDS":
                if data.strip() == ".":
                    entryData["keywords"] = ""
                else:
                    entryData["keywords"] = self.flattenBlock(data)

            elif heading == "SOURCE":
                source, leftover = self.getSections(data, 2)
                organism, higherClassification = leftover.split("\n", 1)

                entryData["source"] = source.strip()
                entryData["organism"] = organism.split()[1].strip()
                entryData["higher_classification"] = self.flattenBlock(higherClassification)

            elif heading == "REFERENCE":
                reference = {}
                refInfo, properties = data.split("\n", 1)
                refInfo = refInfo.strip().split(" ", 1)
                if len(refInfo) == 1: # No bases specified
                    reference["basesFrom"] = ""
                    reference["basesTo"] = ""
                else:
                    baseRanges = refInfo[1].strip().lstrip("(bases").rstrip(")")
                    bases = []
                    for baseRange in baseRanges.split(";"):
                        basesFrom, basesTo = baseRange.split("to")
                        bases.append(f"({basesFrom.strip()}) to {basesTo.strip()}")

                    reference["bases"] = bases

                referenceSections = self.getSections(properties, 2)
                for section in referenceSections:
                    sectionName, sectionData = section.strip().split(" ", 1)
                    reference[sectionName.lower()] = self.flattenBlock(sectionData)

                if "references" not in entryData:
                    entryData["references"] = []

                entryData["references"].append(reference)

            elif heading == "FEATURES":
                featureBlocks = self.getSections(data, 5)
                genes = {}
                otherProperties = {}

                for block in featureBlocks[1:]:
                    sectionHeader, sectionData = block.lstrip().split(" ", 1)

                    propertyList = self.flattenBlock(sectionData, "//").split("///")
                    properties = {"bp_range": propertyList[0]}
                    for property in propertyList[1:]: # base pair range is first property in list
                        splitProperty = property.split("=")
                        if len(splitProperty) == 2:
                            key, value = splitProperty
                        else:
                            key = splitProperty[0]
                            value = key

                        properties[key] = value.strip('"')

                    if sectionHeader == "source":
                        value = properties.pop("organism", None)
                        if value is not None:
                            properties["features_organism"] = value

                        entryData |= properties

                    else:
                        gene = properties.get("gene", None)
                        if gene is not None:  # If section is associated with a gene
                            properties.pop("gene")
                            if gene not in genes:
                                genes[gene] = {}

                            if sectionHeader not in genes[gene]:
                                genes[gene][sectionHeader] = []

                            genes[gene][sectionHeader].append(properties)

                        else: # Not associated with a gene
                            if sectionHeader not in otherProperties:
                                otherProperties[sectionHeader] = []

                            otherProperties[sectionHeader].append(properties)
                
                entryData["genes"] = genes
                entryData["other_properties"] = otherProperties

            elif heading == "ORIGIN":
                pass # Skip adding of origin to output

            else:
                print(f"Unhandled heading: {heading}")

        return entryData

    def getSections(self, textBlock: str, whitespace: int = 0) -> list[str]:
        sections = []
        sectionStart = 0
        searchPos = 0
        nextNewlinePos = textBlock.find("\n", searchPos)
        while nextNewlinePos >= 0:
            nextPlus1 = nextNewlinePos + 1
            followingLineStart = textBlock[nextPlus1:nextPlus1+whitespace+1]
            if nextPlus1+whitespace >= len(textBlock) or (not len(followingLineStart[:-1].strip()) and followingLineStart[-1].isalpha()):
                sections.append(textBlock[sectionStart:nextPlus1])
                sectionStart = nextPlus1

            searchPos = nextPlus1
            nextNewlinePos = textBlock.find("\n", searchPos)

        return sections
    
    def flattenBlock(self, textBlock: str, joiner: str = " ") -> str:
        return joiner.join(line.strip() for line in textBlock.split("\n") if line)
