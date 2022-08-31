import pandas as pd
import json
import os
import config

def loadDataSources(filepath=config.sourcesPath):
    """
    Parameters:
        filepath: Path to the json containing the sources information.

    Creates a dictionary of Source objects mapped to the name of the source. 
    """
    class Source:
        def __init__(self, params):
            self.downloadedFile = params.get("downloadedFile", "") # Filename for URI download to save to
            self.processedFile = params.get("processedFile", "") # Some files need to be processed before conversion to dwc
            self.uri = params.get("uri", "") # URI for the source
            self.parseKwargs = params.get("parseKwargs", {}) # Kwargs for parsing into pandas dataframe

        def getLoadPath(self, dir, forceuri=False):
            if self.processedFile: # Processed file has priority
                return os.path.join(dir, self.processedFile)

            if forceuri:
                return self.uri

            return os.path.join(dir, self.downloadedFile)

    with open(filepath) as fp:
        sourceDict = json.load(fp)

    return {name: Source(params) for name, params in sourceDict.items()}

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
        dtype=str,
        on_bad_lines='skip' if skipBadLines else 'error',
        **kwargs
    )

    return df

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
