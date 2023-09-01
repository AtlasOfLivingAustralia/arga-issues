import requests
import json
import pandas as pd

class AphiaObject:
    def __init__(self, data: dict) -> 'AphiaObject':
        self.data = data.copy()
        self.id = data["AphiaID"]
        self.rank = data["rank"]

        # Convert flag fields from integers to boolean
        for field in ("isMarine", "isBrackish", "isFreshwater", "isTerrestrial", "isExtinct"):
            self.data[field] = self.data[field] == 1

        self.parentRank = None
        self.parentID = None

    def __repr__(self) -> str:
        return f"({self.data['scientificname']} | {self.data['rank']} | {self.id} | {self.data['taxonRankID']})"
    
    def setParent(self, parent: 'AphiaObject') -> None:
        self.parentRank = parent.rank
        self.parentID = parent.id

    def export(self) -> dict:
        return self.data | {"parentID": self.parentID, "parentRank": self.parentRank}

def getResponse(endpoint: str, id: int) -> dict:
    baseURL = "https://www.marinespecies.org/rest/"

    url = f"{baseURL}{endpoint}/{id}"
    headers = {
        "accept": "application/json"
    }
    
    response = requests.get(url, headers=headers)

    try:
        return response.json()
    except json.JSONDecodeError:
        return {}

def getIDInfo(id: int) -> dict:
    endpoint = "AphiaRecordByAphiaID"
    return getResponse(endpoint, id)

def getChildrenInfo(id: int) -> dict:
    endpoint = "AphiaChildrenByAphiaID"
    return getResponse(endpoint, id)

if __name__ == "__main__":
    toplevel = AphiaObject(getIDInfo(1))

    unchecked = [toplevel]
    depth = 0
    while unchecked:
        newChildren = []
        print(f"At depth: {depth}")
        for parent in unchecked:
            childrenInfo = getChildrenInfo(parent.id)

            if not childrenInfo:
                continue

            for child in childrenInfo:
                childObj = AphiaObject(child)

                if childObj.id == parent.id: # Parent appears when getting children for some reason
                    continue 

                childObj.setParent(parent)
                newChildren.append(childObj)

        savedChildren = newChildren[:3]
        if not savedChildren:
            print("No children")
            break

        print(f"Saving to {savedChildren[0].rank}.json")
        with open(f"./taxonomy/{savedChildren[0].rank}.json", "w") as fp:
            json.dump(savedChildren[0].export(), fp, indent=4)

        unchecked.clear()
        unchecked = savedChildren.copy()
        depth += 1