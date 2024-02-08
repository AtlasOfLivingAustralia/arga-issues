import re
import requests
import urllib.parse
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import concurrent.futures
from pathlib import Path
import json
from lib.tools.logger import Logger

class Crawler:
    def __init__(self, workingDir: Path, reString: str, downloadLink: str = "", maxDepth: int = -1, maxWorkers: int = 200, retries: int = 5, user: str = "", password: str = ""):
        self.workingDir = workingDir
        self.reString = reString
        self.downloadLink = downloadLink
        self.maxDepth = maxDepth
        self.maxWorkers = maxWorkers
        self.retries = retries

        self.subdir = self.workingDir / "crawlerProgress"

        self.regex = re.compile(reString)
        self.auth = HTTPBasicAuth(user, password) if user else None

    def crawl(self, url: str):
        folderURLs, subDirDepth = self.loadProgress() # Load urls from progress

        if subDirDepth < 0: # No previous crawler progress
            folderURLs.append(url)
            subDirDepth = 0
        elif not folderURLs: # Found progress but no more folders left to search
            Logger.info("Nothing left to crawl, exiting...")
            return
        
        Logger.info("Crawling...")
        while len(folderURLs):
            newFolders = []
            errorFolders = []
            matchingFiles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.maxWorkers) as executor:
                futures = [executor.submit(self.getMatches, folderURL) for folderURL in folderURLs]
                totalFolders = len(folderURLs)
                try:
                    for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                        print(f"At depth: {subDirDepth}, folder: {idx} / {totalFolders}", end="\r")
                        try:
                            success, usedFolderURL, newSubFolders, newFiles = future.result(timeout=10)
                        except concurrent.futures.TimeoutError:
                            errorFolders.append(usedFolderURL)
                            continue

                        if not success:
                            errorFolders.append(usedFolderURL)
                            continue
                        
                        folderURLs.remove(usedFolderURL)
                        matchingFiles.extend(newFiles)

                        if subDirDepth < self.maxDepth or self.maxDepth < 0:
                            newFolders.extend(newSubFolders)

                except KeyboardInterrupt:
                    return

            folderURLs = newFolders.copy()
            subDirDepth += 1
            self.writeProgress(subDirDepth, folderURLs, matchingFiles, errorFolders)
            print()
                    
            # for idx, folderURL in enumerate(folderURLs):
            #     print(f"At depth: {subDirDepth}, folder: {idx+1} / {len(folderURLs)}", end="\r")
            #     success, newSubFolders, newFiles = self.getMatches(folderURL)
            #     if not success:
            #         print(f"\nFailed at folder {idx}, exiting...")
            #         return (matchingFiles, folderURLs[idx:])

            #     matchingFiles.extend(newFiles)

            #     if subDirDepth < self.maxDepth or self.maxDepth <= 0:
            #         newFolders.extend(newSubFolders)

            # folderURLs = newFolders.copy()
            # subDirDepth += 1
            # print()

    # def getFolder(self, url: str) -> str:
    #     for _ in range(self.retries):
    #         try:
    #             rawHTML = requests.get(url, auth=self.auth)
    #             return rawHTML.text
    #         except (ConnectionError, requests.exceptions.ConnectionError):
    #             pass
            
    #     return ""

    def getURLList(self) -> list[str]:
        matches = []
        for idx, file in enumerate(self.subdir.iterdir()):
            print(f"Collecting url from file: {idx}", end="\r")
            with open(file) as fp:
                data = json.load(fp)

            matches.extend(data.get("Files", []))
        print()
        return matches
                
    def getMatches(self, location: str) -> tuple[bool, str, list[str], list[str]]:
        for attempt in range(self.retries):
            try:
                rawHTML = requests.get(location, auth=self.auth)
                break
            except (ConnectionError, requests.exceptions.ConnectionError):
                if attempt == self.retries:
                    return (False, location, [], [])

        soup = BeautifulSoup(rawHTML.text, 'html.parser')

        folders = []
        matches = []
        for link in soup.find_all('a'):
            link = link.get('href')

            if link is None:
                continue
            
            fullLink = urllib.parse.urljoin(location, link)
            if fullLink.startswith(location) and fullLink != location and fullLink.endswith('/'): # Folder classification
                folders.append(fullLink)

            if self.regex.match(link):
                if self.downloadLink:
                    matches.append(urllib.parse.urljoin(self.downloadLink, link))
                else:
                    matches.append(fullLink)

        return (True, location, folders, matches)
    
    def writeProgress(self, depth: int, foundFolders: list, foundFiles: list, errorFolders: list):
        self.subdir.mkdir(parents=True, exist_ok=True)

        with open(self.subdir / f"crawler_depth_{depth}.json", "w") as fp:
            json.dump({"Folders": foundFolders, "Files": foundFiles, "Error Folders": errorFolders}, fp, indent=4)

    def loadProgress(self) -> tuple[list[Path], int]:
        files = list(self.subdir.glob("crawler_depth_*.json"))
        sortedFiles = sorted(files, key=lambda x: int(x.stem.split("_")[-1]), reverse=True)
        for file in sortedFiles:
            with open(file) as fp:
                data = json.load(fp)

            if "Folders" in data:
                return (data["Folders"], len(files))
        return ([], -1)
