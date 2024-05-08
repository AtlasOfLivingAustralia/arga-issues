import lib.config as cfg
from enum import Enum

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

    def __init__(self, location: str, database: str, config: dict = {}):
        self.location = location
        self.database = database

        # Auth
        self.authFile: str = config.pop("auth", "")

        # Config stages
        self.downloadConfig: dict = config.pop("download", None)
        self.processingConfig: dict = config.pop("processing", {})
        self.conversionConfig: dict = config.pop("conversion", {})

        if self.download is None:
            raise Exception("No download config specified as required") from AttributeError

        # Relative folders
        self.locationDir = cfg.Folders.dataSources / location
        self.databaseDir = self.locationDir / database
        self.dataDir = self.database / "data"
        self.downloadDir = self.database / "download"
        self.processingDir = self.database / "processing"
        self.convertedDir = self.database / "converted"

        # System Managers
        self.downloadManager = DownloadManager(self.downloadDir, self.authFile)
        self.processingManager = ProcessingManager(self.processingDir)
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

    def _prepareDownload(self, overwrite: bool = False) -> None:
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
    
    def _prepareProcessing(self, overwrite: bool = False) -> None:
        specificProcessing: dict[int, list[dict]] = self.processingConfig.pop("specific")
        perFileProcessing: list[dict] = self.processingConfig.pop("perFile")
        finalProcessing: list[dict] = self.processingConfig.pop("final")

        for idx, file in enumerate(self.downloadManager.getFiles()):
            tree = self.processingManager.registerFile(file)
            if idx in specificProcessing:
                self.processingManager.addProcessing(tree, specificProcessing[idx])

        self.processingManager.addAllProcessing(perFileProcessing)
        self.processingManager.addAllProcessing(finalProcessing)
    
    def _prepareConversion(self, overwrite: bool = False) -> None:
        for file in self.processingManager.getLatestNodes():
            self.conversionManager.addFile(file, self.conversionConfig, self.databaseDir)

    def prepare(self, step: Step, overwrite: bool = False) -> None:
        self._prepareDownload(overwrite)

        if step == Step.DOWNLOAD:
            return
        
        self._prepareProcessing(overwrite)

        if step == Step.PROCESSING:
            return
        
        self._prepareConversion(overwrite)

    def execute(self, step: Step, overwrite: bool = False) -> None:
        self.downloadManager.download(overwrite)

        if step == Step.DOWNLOAD:
            return
        
        self.processingManager.process(overwrite)
        
        if step == Step.PROCESSING:
            return
        
        self.conversionManager.convert(overwrite)

class CrawlDB(Database):

    retrieveType = Retrieve.CRAWL

    def _prepareDownload(self, overwrite: bool = False) -> None:
        properties = self.downloadConfig.pop("properties", {})
        saveFile = self.downloadConfig.pop("saveFile", "crawl.txt")
        saveFilePath = self.databaseDir / saveFile

        if not overwrite and saveFilePath.exists():
            Logger.info("Local file found, skipping crawling")
            with open(saveFilePath) as fp:
                urls = fp.read().splitlines()
        else:
            saveFilePath.unlink(True)

            urls = self._crawl()
            self.databaseDir.mkdir(parents=True, exist_ok=True) # Create base directory if it doesn't exist to put the file
            with open(saveFilePath, 'w') as fp:
                fp.write("\n".join(urls))

        for url in urls:
            fileName = self._getFileNameFromURL(url)
            self.downloadManager.registerFromURL(url, fileName, properties)

    def _crawl(self) -> None:
        url = self.downloadConfig.pop("url", None)
        regex = self.downloadConfig.pop("regex", ".*")
        link = self.downloadConfig.pop("link", "")
        maxDepth = self.downloadConfig.pop("maxDepth", -1)

        crawler = Crawler(self.databaseDir, regex, link, maxDepth, self.downloadManager.username, self.downloadManager.password)

        if url is None:
            raise Exception("No file location for source") from AttributeError
        
        Logger.info("Crawling...")
        crawler.crawl(url)
        return crawler.getURLList()
    
    def _getFileNameFromURL(self, url: str) -> str:
        urlParts = url.split('/')
        fileName = urlParts[-1]

        if not self.folderPrefix:
            return fileName

        folderName = urlParts[-2]
        return f"{folderName}_{fileName}"

class ScriptDB(Database):

    retrieveType = Retrieve.SCRIPT

    def _prepareDownload(self, overwrite: bool = False) -> None:
        script = self.downloadConfig.pop("script", None)

        if self.script is None:
            raise Exception("No script specified") from AttributeError

        self.downloadManager.registerFromScript(script)
