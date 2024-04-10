import json
import smtplib
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage
from .dryad import DryadAPI

class UpdateKeeper:
    class Update:
        def __init__(self, lastPage: int = -1, lastEntry: int = -1):
            self.lastPage = lastPage
            self.lastEntry = lastEntry

        def __repr__(self) -> str:
            return str(self)
        
        def __str__(self) -> str:
            return f"\tPage: {self.lastPage}\n\tEntry: {self.lastEntry}"
        

    def __init__(self, lastUpdateFile: Path, apis: list[str]) -> 'UpdateKeeper':
        self.lastUpdateFile = lastUpdateFile
        self.apis = apis

        self.lastDate = None
        self.apiInfo = {api: self.Update() for api in apis}

    def __repr__(self) -> str:
        return str(self)
    
    def __str__(self) -> str:
        lastUpdate = self.lastDate.date() if self.lastDate is not None else None
        apiInfo = "\n\n".join(f"{api}\n{update}" for api, update in self.apis.items())
        return f"Last Update: {lastUpdate}\n\n{apiInfo}"

    def load(self) -> None:
        if not self.lastUpdateFile.exists():
            print("No last update file found, creating...")
            self.save()
            return

        with open(self.lastUpdateFile) as fp:
            data = json.load(fp)

        self.lastDate = data.get("date", None)
        if self.lastDate is not None:
            self.lastDate = datetime.fromisoformat(self.lastDate)

        apiInfo = data.get("apis", {})
        for api, properties in apiInfo.items():
            if api not in self.apis:
                continue



    def save(self) -> None:
        data = {
            "page": self.lastPage,
            "entry": self.lastEntry,
            "date": self.lastDate.isoformat()
        }

        with open(self.lastUpdateFile, "w") as fp:
            json.dump(data, fp, indent=4)

    def update(self, api: str, page: int, entry: int):
        self.lastPage = page
        self.lastEntry = entry
        self.lastDate = datetime.now()

class Bot:
    def __init__(self, sendDay: int = 0, sendHour: int = 9, sendMinute: int = 0):
        self.sendDay = sendDay
        self.sendTime = datetime.time(sendHour, sendMinute, 0)

        self.apis = {
            "Dryad": DryadAPI
        }

        self.lastUpdateFile = Path("lastUpdate.json")
        self.sendFromFile = Path("sendFrom.txt")
        self.sendToFile = Path("sendTo.txt")

        self.updateKeeper = UpdateKeeper(self.lastUpdateFile)
        self.updateKeeper.load()

        # Sending from
        with open(self.sendFromFile) as fp:
            data = fp.read().splitlines()

        self.sendUser = data[0]
        self.sendPass = data[1]

        # Sending to
        with open(self.sendToFile) as fp:
            self.sendToList = [email for email in fp.read().splitlines() if email]

    def sendEmail(self, subject: str, content: str):
        smptServer = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        smptServer.ehlo()
        smptServer.login(self.sendUser, self.sendPass)

        msg = EmailMessage()
        msg["From"] = self.sendUser
        msg["To"] = self.sendToList

        msg["Subject"] = subject
        msg.set_content(content)

        smptServer.sendmail(self.sendUser, self.sendToList, msg.as_string())
        smptServer.quit()
