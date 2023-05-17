import lib.config as cfg
from pathlib import Path
from lib.sourceObjs.dbTypes import DBType
from lib.sourceObjs.systemManager import SystemManager
from lib.processing.stageFile import StageFile, StageFileStep
from lib.crawler import Crawler

class Database:
    def __init__(self, dataType: str, location: str, database: str, properties: dict = {}, enrichDBs: dict = {}):
        self.dataType = dataType
        self.location = location
        self.database = database
        self.enrichDBs = enrichDBs
        self.dbType = DBType.UNKNOWN

        # Standard properties
        self.folderPrefix = properties.pop("folderPrefix", False)
        self.authFile = properties.pop("auth", "")
        self.globalProcessing = properties.pop("globalProcessing", [])
        self.combineProcessing = properties.pop("combineProcessing", [])
        self.dwcProperties = properties.pop("dwcProperties", {})

        self.locationDir = cfg.folderPaths.data / location
        self.databaseDir = self.locationDir / database
        self.systemManager = SystemManager(self.location, self.databaseDir, self.dwcProperties, self.enrichDBs, self.authFile)

        self.postInit(properties)
        self.checkLeftovers(properties)

    def __str__(self):
        return f"{self.location}-{self.database}, {self.outputs}"

    def __repr__(self):
        return str(self)

    def postInit(self, properties: dict) -> None:
        raise NotImplementedError

    def prepare(self) -> None:
        raise NotImplementedError
    
    def getFileNameFromURL(self, url: str) -> str:
        urlParts = url.split('/')
        fileName = urlParts[-1]

        if not self.folderPrefix:
            return fileName

        folderName = urlParts[-2]
        return f"{folderName}_{fileName}"
    
    def download(self, fileNumbers: list[int] = -1, overwrite: int = 0) -> None:
        self.systemManager.create(StageFileStep.RAW, fileNumbers, overwrite)

    def createPreDwC(self, fileNumbers: list[int] = -1, overwrite: int = 0) -> None:
        self.systemManager.create(StageFileStep.PRE_DWC, fileNumbers, overwrite)

    def createDwC(self, fileNumbers: list[int] = -1, overwrite: int = 0) -> None:
        self.systemManager.create(StageFileStep.DWC, fileNumbers, overwrite)

    def createDirectory(self) -> None:
        print(f"Creating directory for data: {str(self.databaseDir)}")
        self.databaseDir.mkdir(parents=True, exist_ok=True)

    def checkLeftovers(self, properties: dict) -> None:
        for property in properties:
            print(f"{self.location}-{self.database} unknown property: {property}")
    
    def getDataType(self) -> str:
        return self.dataType

    def getDBType(self) -> str:
        return self.dbType
    
    def getBaseDir(self) -> Path:
        return self.databaseDir
    
    def getDownloadedFiles(self) -> list[StageFile]:
        return self.fileManager.getFiles(StageFileStep.RAW)
    
    def getPreDWCFiles(self) -> list[StageFile]:
        return self.systemManager.getFiles(StageFileStep.PRE_DWC)
    
    def getDWCFiles(self) -> list[StageFile]:
        return self.systemManager.getFiles(StageFileStep.DWC)

class SpecificDB(Database):

    def postInit(self, properties: dict) -> None:
        self.dbType = DBType.SPECIFIC
        self.files = properties.pop("files", None)

        if self.files is None:
            raise Exception("No provided files for source") from AttributeError

    def prepare(self) -> None:
        for file in self.files:
            url = file.get("url", None)
            fileName = file.get("downloadedFile", None)
            processingSteps = file.get("processing", [])
            fileProperties = file.get("fileProperties", {})

            if url is None:
                raise Exception("No url provided for source") from AttributeError

            if fileName is None:
                raise Exception("No filename provided to download to") from AttributeError
            
            self.systemManager.addDownloadURLStage(url, fileName, processingSteps, fileProperties)

        if self.combineProcessing:
            self.systemManager.addCombineStage(self.combineProcessing)
        
        self.systemManager.pushPreDwC()

class LocationDB(Database):

    def postInit(self, properties: dict) -> None:
        self.dbType = DBType.LOCATION
        self.localFile = "files.txt"

        self.fileProperties = properties.pop("fileProperties", {})
        self.fileLocation = properties.pop("dataLocation", None)
        self.downloadLink = properties.pop("downloadLink", "")
        self.regexMatch = properties.pop("regexMatch", ".*")
        self.maxSubDirDepth = properties.pop("subDirectoryDepth", -1)

        if self.fileLocation is None:
            raise Exception("No file location for source") from AttributeError
        
        self.crawler = Crawler(self.fileLocation, self.regexMatch, self.downloadLink, self.maxSubDirDepth, user=self.systemManager.user, password=self.systemManager.password)

    def prepare(self, recrawl: bool = False) -> None:
        localFilePath = self.databaseDir / self.localFile

        if not recrawl and localFilePath.exists():
            with open(localFilePath) as fp:
                urls = fp.read().splitlines()
        else:
            print("Crawling...")
            localFilePath.unlink(True)
            
            urls, _ = self.crawler.crawl()

            self.databaseDir.mkdir(parents=True, exist_ok=True) # Create base directory if it doesn't exist to put the file
            with open(localFilePath, 'w') as fp:
                fp.write("\n".join(urls))

        for url in urls:
            fileName = self.getFileNameFromURL(url)
            self.systemManager.addDownloadURLStage(url, fileName, self.globalProcessing, self.fileProperties)

        if self.combineProcessing:
            self.systemManager.addCombineStage(self.combineProcessing)

        self.systemManager.pushPreDwC()

class ScriptDB(Database):

    def postInit(self, properties: dict) -> None:
        self.dbType = DBType.SCRIPTDATA
        self.script = properties.pop("script", None)

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
    def prepare(self) -> None:
        self.systemManager.addRetrieveScriptStage(self.script, self.globalProcessing)

        if self.combineProcessing:
            self.systemManager.addCombineStage(self.combineProcessing)

        self.systemManager.pushPreDwC()
