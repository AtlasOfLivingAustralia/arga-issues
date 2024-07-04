import requests
import json
from lib.tools.progressBar import ProgressBar
import pandas as pd
import lib.dataframeFuncs as dff
from pathlib import Path

class APIHandler:
    def __init__(self, baseURL: str, headers: dict, perCall: str, offset: str, entriesPerCall: int):
        self.baseURL = baseURL
        self.headers = headers
        self.perCall = perCall
        self.offset = offset
        self.entriesPerCall = entriesPerCall

        self.key = None

    def _buildURL(self, entries: int, offset: int) -> str:
        return f"{self.baseURL}{self._perCall}={entries}&{self._offset}={offset}"
    
    def loadKey(self, filePath):
        with open(filePath) as fp:
            self.key = fp.read().rstrip("\n")

    def retrieveData(self, record: list[str], totalEntries: list[str]) -> list[dict]:
        session = requests.Session()
        firstCall = session.get(self._buildURL(1, 0), headers=self.headers)
        data = firstCall.json()

        for subsectionName in totalEntries:
            data = data.get(subsectionName, None)

            if data is None:
                print("Unable to find total entries number")
                return

        totalCalls = (data / self.entriesPerCall).__ceil__()
        progress = ProgressBar(50, "Retrieved")
        records = []
        for call in range(totalCalls):
            response = session.get(self._buildURL(self.entriesPerCall, call * self.entriesPerCall))
            progress.render((call + 1) // totalCalls)

            try:
                data = response.json()
            except json.JSONDecodeError:
                continue

            for subsectionName in record:
                data = data[subsectionName]

            records.extend(data)

        print()
        return records
