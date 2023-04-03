import smtplib
from email.message import EmailMessage
import datetime
import time
import json
from pathlib import Path
import requests
from urllib.parse import urljoin
import logging
import traceback
import sys

class Author:
    def __init__(self, data):
        self.firstName = data["firstName"]
        self.lastName = data["lastName"]
        self.affiliation = data.get("affiliation", "")
        self.ror = data.get("affiliationROR", "")

class Funder:
    def __init__(self, data):
        self.organisation = data.get("organization", "")
        self.identifier = data["identifier"]
        self.awardNumber = data.get("awardNumber", "")
        self.identifierType = data.get("identifierType", "")

class DataEntry:
    def __init__(self, data):
        self.links = data["_links"]
        self.href = self.links["self"]["href"]
        self.url = "https://datadryad.org/stash/dataset/" + self.href.split('/')[-1]
        self.identifier = data["identifier"]
        self.id = data["id"]
        self.storageSize = data["storageSize"]
        self.issn = data.get("relatedPublicationISSN", "")
        self.title = data["title"]
        self.abstract = data["abstract"]
        self.keywords = data.get("keywords", [])
        self.locations = [location["place"] for location in data.get("locations", [])]
        self.version = data["versionNumber"]
        self.versionStatus = data["versionStatus"]
        self.curationStatus = data["curationStatus"]
        self.versionChanges = data["versionChanges"]
        self.publicationDate = data["publicationDate"]
        self.lastModificationDate = data["lastModificationDate"]
        self.visibility = data["visibility"]
        self.sharingLink = data["sharingLink"]
        self.userID = data["userId"]
        self.license = data["license"]

        self.authors = [Author(author) for author in data["authors"]]
        self.funders = [Funder(funder) for funder in data.get("funders", [])]

    def __repr__(self):
        return str(self)

    def __str__(self):
        return str(self.data)

    def summarise(self):
        keywordList = '\t\t- ' + '\n\t\t- '.join(word for word in self.keywords)
        return f"{self.title}\n\t- {self.sharingLink}\n\t- Published {self.publicationDate}\n\t- Keywords:\n{keywordList}"

class DryadRequest:
    serverCall = "https://datadryad.org/api/v2/datasets"

    def __init__(self, pageNumber=1):
        call = urljoin(self.serverCall, f"?page={pageNumber}")
        response = requests.get(call)
        raw = response.json()

        self.page = pageNumber
        self.embedded = raw["_embedded"]
        self.links = raw["_links"]
        self.lastPage = int(self.links["last"]["href"].split('=')[-1])
        self.count = int(raw["count"])
        self.total = int(raw["total"])
        self.entries = [DataEntry(entry) for entry in self.embedded["stash:datasets"]]
        self.entryCount = len(self.entries)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{40*'-'}\nLinks: {self.links}\nCount: {self.count}\nTotal: {self.total}\n{40*'-'}"

class LastUpdate:
    def __init__(self, filename):
        self.filename = filename
        self.path = Path(filename)

        self.savedAttrs = ["page", "entryCount", "year", "month", "day"]

    def __repr__(self):
        return str(self.dataDict())

    def fileExists(self):
        return self.path.exists()

    def dataDict(self):
        return {attr: getattr(self, attr) for attr in self.savedAttrs}

    def setParameters(self, page, entryCount, year, month, day):
        self.page = page
        self.entryCount = entryCount
        self.year = year
        self.month = month
        self.day = day

        with open(self.path, 'w') as fp:
            json.dump(self.dataDict(), fp, indent=4)

    def loadFromFile(self):
        with open(self.path) as fp:
            data = json.load(fp)

        for attr, value in data.items():
            setattr(self, attr, int(value))

    def checkSentToday(self, datetime):
        if self.year == datetime.year and self.month == datetime.month and self.day == datetime.day:
            return True
        return False

    def getDate(self):
        return f"{self.day}/{self.month}/{self.year}"

if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(
        filename="updater.LOG",
        format="[%(asctime)s] %(levelname)s - %(message)s",
        datefmt="%d/%m/%Y %I:%M:%S%p",
        encoding="utf-8",
        level=logging.DEBUG
    )

    # logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    # Email messaging setup
    with open("sendFrom.txt") as fp:
        data = fp.read().splitlines()

    # Email address on first line, password key on second line
    sendingFrom = data[0]
    sendingPw = data[1]

    # Put addresses to email to one per line
    with open("sendTo.txt") as fp:
        sendingTo = fp.read().splitlines()

    sendTime = datetime.time(10, 0, 0) # 10 AM
    sendDay = 0 # Monday

    # Dryad API setup
    lastEntryDataFile = "lastUpdate.json"
    entriesPerPage = 20
    lastUpdate = LastUpdate(lastEntryDataFile)

    if lastUpdate.fileExists():
        lastUpdate.loadFromFile()
        logging.info("Loading previous update information from file")
    else:
        logging.info("No update file found")
        logging.info("Requesting first page")
        firstPageRequest = DryadRequest()
        logging.info("Requesting last page")
        finalPageRequest = DryadRequest(firstPageRequest.lastPage)

        now = datetime.datetime.now()
        lastUpdate.setParameters(finalPageRequest.page, finalPageRequest.entryCount, now.year, now.month, now.day)

        logging.info(f"Creating new update file with page {lastUpdate.page} and entry {lastUpdate.entryCount}")

    try:
        while True:
            now = datetime.datetime.now()
            if sendDay == now.weekday() and now.time() > sendTime and not lastUpdate.checkSentToday(now):
                logging.info("No update sent today as required, sending email now")
            else:
                dayDifference = (6 - now.weekday()) + sendDay
                hourDifference = (23 - now.hour) + sendTime.hour
                minuteDifference = (59 - now.minute) + sendTime.minute
                secondDifference = (59 - now.second) + sendTime.second

                diff = datetime.timedelta(days=dayDifference, hours=hourDifference, minutes=minuteDifference, seconds=secondDifference)
                
                if diff.days >= 7:
                    diff = diff - datetime.timedelta(7)

                logging.info(f"Sleeping {diff.total_seconds()} seconds until next update in {diff}")
                time.sleep(diff.total_seconds()) # Sleep until signal time

            previousLastPage = DryadRequest(lastUpdate.page)
            currentLastPage = DryadRequest(previousLastPage.lastPage)

            entriesToEmail = []

            # Entries that are new on the last page from the previous update
            previousLastPageNewEntries = previousLastPage.entryCount - lastUpdate.entryCount
            logging.info(f"Summarising info from last {previousLastPageNewEntries} entries on previous last page")
            for entry in previousLastPage.entries[entriesPerPage - previousLastPageNewEntries:]:
                entriesToEmail.append(entry.summarise())

            # Entries that are on new pages
            newPagesFullOfEntries = currentLastPage.page - previousLastPage.page
            for page in range(lastUpdate.page, newPagesFullOfEntries):
                logging.info(f"Summarising info on page {page}")
                pageData = DryadRequest(page)
                for entry in pageData.entries:
                    entriesToEmail.append(entry.summarise())

            smptServer = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            smptServer.ehlo()
            smptServer.login(sendingFrom, sendingPw)

            msg = EmailMessage()
            msg['From'] = sendingFrom
            msg['To'] = sendingTo

            msg['Subject'] = f"Dryad update: {lastUpdate.getDate()} - {now.strftime('%d/%m/%y')}"

            if len(entriesToEmail):
                content = '\n\n'.join(entry for entry in entriesToEmail)
            else:
                content = "There are no new projects this week unfortunately."

            msg.set_content(f"Here are your new dryad projects:\n\n{content}\n\nRegards,\nARGA BOT")

            smptServer.sendmail(sendingFrom, sendingTo, msg.as_string())
            logging.info("Sent email with info summary")

            lastUpdate.setParameters(currentLastPage.page, currentLastPage.entryCount, now.year, now.month, now.day)
            logging.debug(f"Updating update file with page {currentLastPage.page} and entry {currentLastPage.entryCount}")

            smptServer.quit()

            print("UPDATE FILE:", lastUpdate)

    except KeyboardInterrupt:
        logging.info("Caught CTRL + C, exiting...")

    except Exception as e:
        logging.error(traceback.format_exc())

    finally:
        logging.shutdown()
