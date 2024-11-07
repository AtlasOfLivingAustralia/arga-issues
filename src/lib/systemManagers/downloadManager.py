from pathlib import Path
import lib.commonFuncs as cmn
from lib.processing.stages import File
from lib.processing.scripts import Script
from lib.tools.logger import Logger
import lib.tools.downloading as dl

class _Download:
    def retrieve(self, overwrite: bool) -> bool:
        raise NotImplementedError

class _URLDownload(_Download):
    def __init__(self, url: str, filePath: Path, properties: dict, username: str, password: str):
        self.url = url
        self.file = File(filePath, properties)
        self.auth = dl.buildAuth(username, password) if username else None

    def retrieve(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            Logger.info(f"Output file {self.file.filePath} already exists")
            return self.file.filePath
        
        self.file.filePath.unlink(True)
        return dl.download(self.url, self.file.filePath, verbose=True, auth=self.auth)

class _ScriptDownload(_Download):
    def __init__(self, baseDir: Path, downloadDir: Path, scriptInfo: dict):
        self.script = Script(baseDir, downloadDir, scriptInfo, [])        
        self.file = self.script.output

    def retrieve(self, overwrite: bool, verbose: bool) -> bool:
        return self.script.run(overwrite, verbose)

class DownloadManager:
    def __init__(self, baseDir: Path, downloadDir: Path, authFile: str):
        self.baseDir = baseDir
        self.downloadDir = downloadDir
        self.authFile = authFile

        authPath = self.baseDir / self.authFile
        if authFile and authPath.exists():
            with open(authPath) as fp:
                data = fp.read().rstrip("\n").split()

            self.username = data[0]
            self.password = data[1]

        else:
            self.username = ""
            self.password = ""

        self.downloads: list[_Download] = []

    def getFiles(self) -> list[File]:
        return [download.file for download in self.downloads]

    def getLatestFile(self) -> File:
        return self.files[-1].file

    def download(self, overwrite: bool = False, verbose: bool = False) -> bool:
        if not self.downloadDir.exists():
            self.downloadDir.mkdir(parents=True)

        successes = []
        for download in self.downloads:
            successes.append(download.retrieve(overwrite, verbose))

        return all(successes)

    def registerFromURL(self, url: str, fileName: str, fileProperties: dict = {}) -> bool:
        download = _URLDownload(url, self.downloadDir / fileName, fileProperties, self.username, self.password)
        self.downloads.append(download)
        return True

    def registerFromScript(self, scriptInfo: dict) -> bool:
        try:
            download = _ScriptDownload(self.baseDir, self.downloadDir, scriptInfo)
        except AttributeError as e:
            Logger.error(f"Invalid download script configuration: {e}")
            return False
        
        self.downloads.append(download)
        return True
