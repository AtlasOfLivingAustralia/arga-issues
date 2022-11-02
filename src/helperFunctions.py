import pandas as pd
import json
import os
import config

def loadDataSources(filepath=config.sourcesPath):
    class Source:
        def __init__(self, **kwargs):
            for kwarg, value in kwargs.items():
                setattr(self, kwarg, value)

        def loadDataFrame(self, fromuri=False):
            parseKwargs = getattr(self, 'parseKwargs', {})
            if fromuri:
                return loadSourceFile(self.uri, parseKwargs)

            if hasattr(self, 'processedFile'):
                return loadSourceFile(os.path.join(config.dataFolder, self.processedFile), parseKwargs)

            return loadSourceFile(os.path.join(config.dataFolder, self.downloadedFile), parseKwargs)

    with open(filepath) as fp:
        sources = json.load(fp)

    return {name: Source(**data) for name, data in sources.items()}

def loadSourceFile(filepath, kwargs={}, skipBadLines=True):
    """
    Paramters:
        filepath: Path to file to load from
        kwargs: Optional kwargs to use when parsing
        skipBadLines: Skips lines with errors if set, otherwise raises error.

    Populates a pandas dataframe from a filepath.
    """
    df = pd.read_csv(
        filepath,
        dtype=object,
        on_bad_lines='skip' if skipBadLines else 'error',
        **kwargs
    )

    return df

def reverseLookup(lookupDict):
    return {oldName: newName for newName, oldNameList in lookupDict.items() for oldName in oldNameList}

def splitLine(line, endingDivider=True):
    cleanLine = line.rstrip('\n').rstrip()
    if endingDivider:
        cleanLine = cleanLine.rstrip('|')
    return [element.strip() for element in cleanLine.split('|')]
    
def latlongToDecimal(latlong):
    """
    Paramters:
        latlong: string representation of both latitude and longitude

    Converts the latitude and longitude string to separate lat and long strings, returned as a tuple.
    """
    split = latlong.split(' ')
    if len(split) == 4:
        latlong = f"{split[1]}{split[0]} {split[3]}{split[2]}"
    elif len(split) == 2:
        latlong = f"{split[0][-1]}{split[0][:-1]} {split[1][-1]}{split[1][:-1]}"
    else:
        return ('', '')

    latlong = latlong.replace('S', '-').replace('W', '-').replace('N', '').replace('E', '')
    return latlong.split(' ')
