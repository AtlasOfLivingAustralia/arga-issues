import re
import requests
import urllib.parse
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import concurrent.futures

class Crawler:
    def __init__(self, url, reString, maxDepth=-1, retries=5, user="", password=""):
        self.url = url
        self.reString = reString
        self.maxDepth = maxDepth
        self.retries = retries

        self.regex = re.compile(reString)
        self.auth = HTTPBasicAuth(user, password) if user else None

    def crawl(self):
        subDirDepth = 0
        folderURLs = [self.url]
        matchingFiles = []

        while len(folderURLs):
            newFolders = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(self.getMatches, folderURL) for folderURL in folderURLs]
                for idx, future in enumerate(concurrent.futures.as_completed(futures)):
                    print(f"At depth: {subDirDepth}, folder: {idx+1} / {len(folderURLs)}", end="\r")
                    success, newSubFolders, newFiles = future.result()
                    if not success:
                        print(f"\nFailed at folder {idx}, exiting...")
                        return (matchingFiles, folderURLs[idx:])
                    
                    matchingFiles.extend(newFiles)

                    if subDirDepth < self.maxDepth or self.maxDepth <= 0:
                        newFolders.extend(newSubFolders)

            folderURLs = newFolders.copy()
            subDirDepth += 1
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

        return (matchingFiles, [])
    
    def getMatches(self, location):
        for attempt in range(self.retries):
            try:
                rawHTML = requests.get(location, auth=self.auth)
                break
            except (ConnectionError, requests.exceptions.ConnectionError):
                if attempt == self.retries:
                    return (False, [], [])

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
                matches.append(fullLink)

        return (True, folders, matches)
