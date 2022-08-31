import os

mappingFile = "mapping.json"
sourcesFile = "arga-sources.json"

rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
srcFolder = os.path.join(rootDir, "src")
logsFolder = os.path.join(rootDir, "logs")
dataFolder = os.path.join(rootDir, "data")
resultsFolder = os.path.join(dataFolder, "results")
genFolder = os.path.join(rootDir, "generatedFiles")
examplesFolder = os.path.join(genFolder, "examples")
dwcFolder = os.path.join(genFolder, "dwc")

mappingPath = os.path.join(rootDir, mappingFile)
sourcesPath = os.path.join(rootDir, sourcesFile)
