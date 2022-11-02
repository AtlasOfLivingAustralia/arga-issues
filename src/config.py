import os
import yaml

configFile = "config.yaml"
rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(rootDir, configFile)) as fp:
    cfg = yaml.load(fp, Loader=yaml.Loader)

files = {name: path.replace('/', '\\') for name, path in cfg["Files"].items()}
dirs = {name: path.replace('/', '\\') for name, path in cfg["Directories"].items()}

srcFolder = os.path.join(rootDir, dirs["Source"])
logsFolder = os.path.join(rootDir, dirs["Logging"])
dataFolder = os.path.join(rootDir, dirs["Data"])
resultsFolder = os.path.join(rootDir, dirs["Results"])
genFolder = os.path.join(rootDir, dirs["Generated files"])
examplesFolder = os.path.join(rootDir, dirs["Generated examples"])
dwcFolder = os.path.join(rootDir, dirs["DwC files"])

dwcMappingPath = os.path.join(rootDir, files["DwC mapping"])
customMappingPath = os.path.join(rootDir, files["Other mapping"])
sourcesPath = os.path.join(rootDir, files["Sources"])
excludePath = os.path.join(rootDir, files["Excluded entries"])
