from pathlib import Path
import lib.commonFuncs as cmn
from lib.processing.stages import File
from lib.processing.scripts import Script
from lib.tools.logger import Logger
from lib.tools.downloader import Downloader

class _Download:
    def retrieve(self, overwrite: bool) -> Path:
        raise NotImplementedError

class _URLDownload(_Download):
    def __init__(self, url: str, filePath: Path, properties: dict, username: str, password: str):
        self.url = url
        self.file = File(filePath, properties)
        self.username = username
        self.password = password

    def retrieve(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            Logger.info(f"Output file {self.file.filePath} already exists")
            return self.file.filePath
        
        self.file.filePath.unlink(True)
        downloader = Downloader(username=self.username, password=self.password)
        return downloader.download(self.url, self.file.filePath, verbose=True)

class _ScriptDownload(_Download):
    def __init__(self, baseDir: Path, downloadDir: Path, scriptInfo: dict):
        self.script = Script(baseDir, downloadDir, scriptInfo, [])

    def retrieve(self, overwrite: bool, verbose: bool) -> Path:
        self.script.run(overwrite, verbose)

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

    def download(self, overwrite: bool = False, verbose: bool = False) -> None:
        if not self.downloadDir.exists():
            self.downloadDir.mkdir()

        for download in self.downloads:
            download.retrieve(overwrite, verbose)

    def registerFromURL(self, url: str, fileName: str, fileProperties: dict = {}) -> None:
        download = _URLDownload(url, self.downloadDir / fileName, fileProperties, self.username, self.password)
        self.downloads.append(download)

    def registerFromScript(self, scriptInfo: dict) -> None:
        download = _ScriptDownload(self.baseDir, self.downloadDir, scriptInfo)
        self.downloads.append(download)
