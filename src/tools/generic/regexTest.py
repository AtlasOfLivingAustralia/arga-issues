import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

def getMatches(location, regex):
    rawHTML = requests.get(location)
    soup = BeautifulSoup(rawHTML.text, 'html.parser')
    exp = re.compile(regex)

    folders = []
    matches = []
    for link in soup.find_all('a'):
        link = link.get('href')
        fullLink = urljoin(location, link)

        print(fullLink)

        if link.endswith('/') and link.count('/') == 1: # Folder classification
            folders.append(fullLink)

        if exp.match(link):
            matches.append(fullLink)

    return folders, matches

location = "http://reefgenomics.org/sitemap.html"

regexMatch = "metadata\\.yaml"

locations = [location]
matches = []

maxDepth = 0

depth = 0
while len(locations):
    newLocations = []

    for location in locations:
        newFolders, newFiles = getMatches(location, regexMatch)

        if depth < maxDepth:
            newLocations.extend(newFolders)

        matches.extend(newFiles)

    locations = newLocations.copy()
    depth += 1

print(matches)
