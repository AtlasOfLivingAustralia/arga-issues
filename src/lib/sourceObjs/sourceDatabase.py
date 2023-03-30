import lib.config as cfg
import lib.commonFuncs as cmn
from pathlib import Path
from lib.sourceObjs.dbTypes import DBType
from lib.processing.processor import DWCProcessor
from lib.processing.steps import FileStep
from lib.processing.parser import SelectorParser
from lib.sourceObjs.fileManager import FileManager, FileStage, StageFile

class Database:

    def __init__(self, dataType: str, location: str, database: str, properties: dict = {}, enrichDBs: dict = {}):
        self.dataType = dataType
        self.location = location
        self.database = database
        self.enrichDBs = enrichDBs
        self.dbType = DBType.UNKNOWN

        # Standard properties
        self.folderPrefix = properties.pop("folderPrefix", False)
        self.authFile = properties.pop("auth", None)
        self.globalProcessing = properties.pop("globalProcessing", [])
        self.combineProcessing = properties.pop("combineProcessing", [])
        self.fileProperties = properties.pop("fileProperties", {})
        self.dwcProperties = properties.pop("dwcProperties", {})

        self.locationDir = cfg.folderPaths.data / location
        self.databaseDir = self.locationDir / database
        self.downloadDir = self.databaseDir / "raw"
        self.processingDir = self.databaseDir / "processing"
        self.preConversionDir = self.databaseDir / "preConversion"
        self.dwcDir = self.databaseDir / "dwc"

        self.sourceDirectories = (self.databaseDir, self.downloadDir, self.processingDir, self.preConversionDir)
        dwcProcessor = DWCProcessor(self.location, self.dwcProperties, self.enrichDBs, self.dwcDir)
        self.fileManager = FileManager(self.sourceDirectories, self.authFile, dwcProcessor)

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
    
    def download(self, overwrite: bool = False) -> None:
        self.fileManager.createAll(FileStage.RAW, overwrite)

    def createPreDwC(self, overwrite: bool = False) -> None:
        self.fileManager.createAll(FileStage.PRE_DWC, overwrite)

    def createDwC(self, overwrite: bool = False) -> None:
        self.fileManager.createAll(FileStage.DWC, overwrite)

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
    
    def getPreDWCFiles(self) -> list[StageFile]:
        return self.fileManager.getFiles(FileStage.PRE_DWC)
    
    def getDWCFiles(self) -> list[StageFile]:
        return self.fileManager.getFiles(FileStage.DWC)

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
            
            self.fileManager.addDownloadURLStage(url, fileName, processingSteps, fileProperties)

        if self.combineProcessing:
            self.fileManager.addCombineStage(self.combineProcessing)
        
        self.fileManager.pushPreDwC()

class LocationDB(Database):

    def postInit(self, properties: dict) -> None:
        self.dbType = DBType.LOCATION
        self.localFile = "files.txt"
        self.subDirDepthLimit = 20

        self.fileLocation = properties.pop("dataLocation", None)
        self.regexMatch = properties.pop("regexMatch", ".*")
        self.maxSubDirDepth = properties.pop("subDirectoryDepth", self.subDirDepthLimit)

        # Never travel to depth greater than sub directory depth limit
        if self.maxSubDirDepth < 0:
            self.maxSubDirDepth = self.subDirDepthLimit
        else:
            self.maxSubDirDepth = min(self.maxSubDirDepth, self.subDirDepthLimit)

        if self.fileLocation is None:
            raise Exception("No file location for source") from AttributeError

    def prepare(self, recrawl: bool = False) -> None:
        localFilePath = self.databaseDir / self.localFile

        if not recrawl and localFilePath.exists():
            with open(localFilePath) as fp:
                urls = fp.read().splitlines()
        else:
            print("Crawling...")
            localFilePath.unlink(True)

            urls = cmn.crawl(self.fileLocation, self.regexMatch, self.maxSubDirDepth, user=self.user, password=self.password)
            
            with open(localFilePath, 'w') as fp:
                fp.writelines(urls)

        for url in urls:
            fileName = self.getFileNameFromURL(url)
            self.fileManager.addDownloadURLStage(url, fileName, self.globalProcessing, self.fileProperties)

        if self.combineProcessing:
            self.fileManager.addCombineStage(self.combineProcessing)

        self.fileManager.pushPreDwC()

class ScriptUrlDB(Database):
    
    def postInit(self, properties: dict) -> None:
        self.dbType = DBType.SCRIPTURL
        self.folderPrefix = properties.pop("folderPrefix", False)
        self.script = properties.pop("script", None)
        
        if self.script is None:
            raise Exception("No script specified") from AttributeError

        self.scriptStep = FileStep(self.script, SelectorParser(self.sourceDirectories, []))
        
    def prepare(self) -> None:
        urls = self.scriptStep.process()

        for url in urls:
            fileName = self.getFileNameFromURL(url)
            self.fileManager.addDownloadURLStage(url, fileName, self.globalProcessing)

        if self.combineProcessing:
            self.fileManager.addCombineStage(self.combineProcessing)
        
        self.fileManager.pushPreDwC()

class ScriptDataDB(Database):

    def postInit(self, properties: dict) -> None:
        self.dbType = DBType.SCRIPTDATA
        self.script = properties.pop("script", None)

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
    def prepare(self) -> None:
        self.fileManager.addRetrieveScriptStage(self.script, self.globalProcessing, self.fileProperties)

        if self.combineProcessing:
            self.fileManager.addCombineStage(self.combineProcessing)

        self.fileManager.pushPreDwC()
