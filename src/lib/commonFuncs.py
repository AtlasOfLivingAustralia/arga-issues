import logging
import csv
import lib.config as cfg
import json
import platform
import subprocess

def reverseLookup(lookup: dict) -> dict:
    nameMap = {}

    for newName, oldNameList in lookup.items():
        for name in oldNameList:
            if name not in nameMap:
                nameMap[name]  = []
            
            nameMap[name].append(newName)

    return nameMap
    
def latlongToDecimal(latlong: str) -> str:
    """
    Paramters:
        latlong: string representation of both latitude and longitude

    Converts the latitude and longitude string to separate lat and long strings, returned as a tuple.
    """
    latlong = str(latlong)
    split = latlong.split(' ')
    if len(split) == 4:
        latlong = f"{split[1]}{split[0]} {split[3]}{split[2]}"
    elif len(split) == 2:
        latlong = f"{split[0][-1]}{split[0][:-1]} {split[1][-1]}{split[1][:-1]}"
    else:
        return latlong

    latlong = latlong.replace('S', '-').replace('W', '-').replace('N', '').replace('E', '')
    return latlong.split(' ')

def loadLogger(filename):
    logging.basicConfig(
        filename=cfg.folderPaths.logs / f"{filename}.log",
        encoding="utf-8",
        level=logging.DEBUG,
        filemode='w',
        format="%(message)s"
    )

    return logging.getLogger(__name__)

def flatten(inputDict: dict, parent: str = "") -> dict:
    res = {}
    
    for key, value in inputDict.items():
        addKey = key if not parent else f"{parent}_{key}"

        if isinstance(value, dict):
            res.update(flatten(value, parent=key))
        elif isinstance(value, list) or isinstance(value, tuple):
            for item in value:
                if isinstance(item, dict):
                    res.update(flatten(item, parent=key))
                else:
                    res[addKey] = item
        else:
            res[addKey] = value

    return res

def getColumns(filePath: str, separator: str = ',', headerRow: int = 0) -> str:
    with open(filePath, encoding='utf-8') as fp:
        reader = csv.reader(fp, delimiter=separator)
        for idx, row in enumerate(reader):
            if idx == headerRow:
                return row

def extendUnique(lst: list, values: list) -> None:
    lst.extend([item for item in values if item not in lst])
    return lst

def addUniqueEntry(dictionary: dict, key: any, value: any, duplicateLimit: int = 0) -> None:
    if key not in dictionary:
        dictionary[key] = value
        return

    suffixNum = 1
    while suffixNum != duplicateLimit:
        newKey = f"{key}_{suffixNum}"
        if newKey not in dictionary:
            dictionary[newKey] = value
            return
        suffixNum += 1

def loadFromJson(path: str) -> dict:
    with open(path) as fp:
        return json.load(fp)

def dictListToCSV(dictList: list, columns: list, filePath: str) -> None:
    with open(filePath, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.DictWriter(fp, columns)
        writer.writeheader()

        for d in dictList:
            writer.writerow(d)

def downloadFile(url: str, filePath: str, user: str = "", password: str = "", verbose: bool = True):
    curl = "curl.exe" if platform.system() == 'Windows' else "curl"
    args = [curl, url, "-o", filePath]
    if user:
        args.extend(["--user", f"{user}:{password}"])

    if verbose:
        print(f"Downloading from {url} to file {filePath}")

    subprocess.run(args)
