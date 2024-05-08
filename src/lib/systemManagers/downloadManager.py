from pathlib import Path
import lib.commonFuncs as cmn
from lib.processing.parser import SelectorParser
from lib.processing.stages import File, Script
from lib.tools.logger import Logger

class _Download:
    def retrieve(self, overwrite: bool) -> Path:
        raise NotImplementedError

class _URLDownload(_Download):
    def __init__(self, url: str, filePath: Path, properties: dict, username: str, password: str):
        self.url = url
        self.file = File(filePath, properties)
        self.username = username
        self.password = password

    def retrieve(self, overwrite: bool, verbose: bool) -> Path:
        if not overwrite and self.file.exists():
            return self.file.filePath
        
        self.file.filePath.unlink(True)
        return cmn.downloadFile(self.url, self.filePath, user=self.user, password=self.password)

class _ScriptDownload(_Download):
    def __init__(self, scriptInfo: dict, dir: Path):
        self.script = Script(scriptInfo, dir)

    def retrieve(self, overwrite: bool, verbose: bool) -> Path:
        self.script.run(overwrite, verbose)

class DownloadManager:
    def __init__(self, downloadDir: Path, authFile: Path):
        self.downloadDir = downloadDir
        self.authFile = authFile

        self.downloads: list[_Download] = []

    def getFiles(self) -> list[File]:
        return [download.file for download in self.downloads]

    def getLatestFile(self) -> File:
        return self.files[-1].file

    def download(self, overwrite: bool = False, verbose: bool = False) -> None:
        for download in self.downloads:
            download.retrieve(overwrite, verbose)

    def registerFromURL(self, url: str, fileName: str, fileProperties: dict = {}, username = "", password = "") -> None:
        download = _URLDownload(url, self.downloadDir / fileName, fileProperties, username, password)
        self.downloads.append(download)

    def registerFromScript(self, scriptInfo: dict) -> None:
        download = _ScriptDownload(scriptInfo, self.downloadDir)
        self.downloads.append(download)
