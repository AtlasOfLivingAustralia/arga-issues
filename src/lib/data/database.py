import lib.config as cfg
from enum import Enum
from pathlib import Path

from lib.systemManagers.timeManager import TimeManager
from lib.systemManagers.downloadManager import DownloadManager
from lib.systemManagers.processingManager import ProcessingManager
from lib.systemManagers.conversionManager import ConversionManager

from lib.processing.stages import Step

from lib.tools.crawler import Crawler
from lib.tools.logger import Logger

class Retrieve(Enum):
    URL     = 0
    CRAWL   = 1
    SCRIPT  = 2

class Database:

    retrieveType = Retrieve.URL

    def __init__(self, location: str, database: str, subsection: str, config: dict):
        self.location = location
        self.database = database

        # Auth
        self.authFile: str = config.pop("auth", "")

        # Config stages
        self.downloadConfig: dict = config.pop("download", None)
        self.processingConfig: dict = config.pop("processing", {})
        self.conversionConfig: dict = config.pop("conversion", {})

        if self.downloadConfig is None:
            raise Exception("No download config specified as required") from AttributeError

        # Relative folders
        self.locationDir = cfg.Folders.dataSources / location
        self.databaseDir = self.locationDir / database
        self.subsectionDir = self.databaseDir / subsection # If no subsection, does nothing
        self.dataDir = self.subsectionDir / "data"
        self.downloadDir = self.dataDir / "download"
        self.processingDir = self.dataDir / "processing"
        self.convertedDir = self.dataDir / "converted"

        # System Managers
        self.downloadManager = DownloadManager(self.databaseDir, self.downloadDir, self.authFile)
        self.processingManager = ProcessingManager(self.databaseDir, self.processingDir)
        self.conversionManager = ConversionManager(self.convertedDir, location)
        self.timeManager = TimeManager(self.databaseDir)

        # Report extra config options
        self._reportLeftovers(config)

    def __str__(self):
        return f"{self.location}-{self.database}"

    def __repr__(self):
        return str(self)
    
    def _reportLeftovers(self, properties: dict) -> None:
        for property in properties:
            Logger.debug(f"{self.location}-{self.database} unknown config item: {property}")

    def _prepareDownload(self, overwrite: bool, verbose: bool) -> None:
        files: list[dict] = self.downloadConfig.pop("files", [])

        for file in files:
            url = file.get("url", None)
            name = file.get("name", None)
            properties = file.get("properties", {})

            if url is None:
                raise Exception("No url provided for source") from AttributeError

            if name is None:
                raise Exception("No filename provided to download to") from AttributeError
            
            self.downloadManager.registerFromURL(url, name, properties)
    
    def _prepareProcessing(self, overwrite: bool, verbose: bool) -> None:
        specificProcessing: dict[int, list[dict]] = self.processingConfig.pop("specific", {})
        perFileProcessing: list[dict] = self.processingConfig.pop("perFile", [])
        finalProcessing: list[dict] = self.processingConfig.pop("final", [])

        for idx, file in enumerate(self.downloadManager.getFiles()):
            processing = specificProcessing.get(idx, [])
            self.processingManager.registerFile(file, processing)

        self.processingManager.addAllProcessing(perFileProcessing)
        self.processingManager.addFinalProcessing(finalProcessing)
    
    def _prepareConversion(self, overwrite: bool, verbose: bool) -> None:
        for file in self.processingManager.getLatestNodes():
            self.conversionManager.addFile(file, self.conversionConfig, self.databaseDir)

    def _prepare(self, step: Step, overwrite: bool, verbose: bool) -> None:
        callbacks = {
            Step.DOWNLOAD: self._prepareDownload,
            Step.PROCESSING: self._prepareProcessing,
            Step.CONVERSION: self._prepareConversion
        }

        if step not in callbacks:
            raise Exception(f"Uknown step to prepare: {step}")

        for stepType, callback in callbacks.items():
            callback(overwrite if step == stepType else False, verbose)
            if step == stepType:
                return

    def _execute(self, step: Step, overwrite: bool, verbose: bool, **kwargs: dict) -> None:
        if step == Step.DOWNLOAD:
            self.downloadManager.download(overwrite, verbose, **kwargs)
            return
        
        if step == Step.PROCESSING:
            self.processingManager.process(overwrite, verbose, **kwargs)
            return
        
        if step == Step.CONVERSION:
            self.conversionManager.convert(overwrite, verbose, **kwargs)
            return
        
        raise Exception(f"Unknown step to execute: {step}")
    
    def create(self, step: Step, overwrite: tuple[bool, bool], verbose: bool, **kwargs: dict) -> None:
        prepare, reprocess = overwrite

        self._prepare(step, prepare, verbose)
        self._execute(step, reprocess, verbose, **kwargs)

class CrawlDB(Database):

    retrieveType = Retrieve.CRAWL

    def _prepareDownload(self, overwrite: bool, verbose: bool) -> None:
        properties = self.downloadConfig.pop("properties", {})
        folderPrefix = self.downloadConfig.pop("prefix", False)
        saveFile = self.downloadConfig.pop("saveFile", "crawl.txt")
        saveFilePath: Path = self.subsectionDir / saveFile

        if not overwrite and saveFilePath.exists():
            Logger.info("Local file found, skipping crawling")
            with open(saveFilePath) as fp:
                urls = fp.read().splitlines()
        else:
            saveFilePath.unlink(True)

            urls = self._crawl(saveFilePath.parent)
            saveFilePath.parent.mkdir(parents=True, exist_ok=True) # Create base directory if it doesn't exist to put the file
            with open(saveFilePath, 'w') as fp:
                fp.write("\n".join(urls))

        for url in urls:
            fileName = self._getFileNameFromURL(url, folderPrefix)
            self.downloadManager.registerFromURL(url, fileName, properties)

    def _crawl(self, crawlerDirectory: Path) -> None:
        url = self.downloadConfig.pop("url", None)
        regex = self.downloadConfig.pop("regex", ".*")
        link = self.downloadConfig.pop("link", "")
        maxDepth = self.downloadConfig.pop("maxDepth", -1)

        crawler = Crawler(crawlerDirectory, regex, link, maxDepth, user=self.downloadManager.username, password=self.downloadManager.password)

        if url is None:
            raise Exception("No file location for source") from AttributeError
        
        crawler.crawl(url, True)
        return crawler.getURLList()
    
    def _getFileNameFromURL(self, url: str, folderPrefix: bool) -> str:
        urlParts = url.split('/')
        fileName = urlParts[-1]

        if not folderPrefix:
            return fileName

        folderName = urlParts[-2]
        return f"{folderName}_{fileName}"

class ScriptDB(Database):

    retrieveType = Retrieve.SCRIPT

    def _prepareDownload(self, overwrite: bool, verbose: bool) -> None:
        self.downloadManager.registerFromScript(self.downloadConfig)
