import json
from ...lib.config import folderPaths, filePaths

if __name__ == '__main__':
    exclude = {
        'taxonID': {
            "dwc": "taxonID",
            "data": []
        },
        'speciesID': {
            "dwc": "specific_host",
            "data": []
        }
    }

    with open(folderPaths.rawData / "categories.dmp") as fp:
        data = fp.readline()
        while data:
            category, speciesID, taxonID = data.split()
            if category not in ('A', 'B', 'E'):
                exclude["taxonID"]["data"].append(taxonID)
                exclude["speciesID"]["data"].append(speciesID)
            data = fp.readline()

    with open(filePaths.excludedEntries, 'w') as fp:
        json.dump(exclude, fp, indent=4)
