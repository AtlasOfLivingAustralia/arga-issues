import lib.config as cfg
import lib.commonFuncs as cmn
from pathlib import Path
from lib.sourceObjs.dbTypes import DBType
from lib.processing.processor import FileProcessor
from lib.processing.steps import ScriptStep, FileStep
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
        self.enrichDict = properties.pop("enrich", {}) # Dict to store enrich info

        self.locationDir = cfg.folderPaths.data / location
        self.databaseDir = self.locationDir / database
        self.downloadDir = self.databaseDir / "raw"
        self.processingDir = self.databaseDir / "processing"
        self.preConversionDir = self.databaseDir / "preConversion"
        self.dwcDir = self.databaseDir / "dwc"

        self.sourceDirectories = (self.databaseDir, self.downloadDir, self.processingDir, self.preConversionDir, self.dwcDir)

        self.fileManager = FileManager(self.sourceDirectories, self.authFile)
        self.enrichDBs = {} # Dict to store references to enrich dbs

        self.postInit(properties)
        self.checkLeftovers(properties)

    def __str__(self):
        return f"{self.location}-{self.database}, {self.outputs}"

    def __repr__(self):
        return str(self)

    def postInit(self):
        raise NotImplementedError

    def prepare(self):
        raise NotImplementedError
    
    def getFileNameFromURL(self, url: str) -> str:
        urlParts = url.split('/')
        fileName = urlParts[-1]

        if not self.folderPrefix:
            return fileName

        folderName = urlParts[-2]
        return f"{folderName}_{fileName}"
    
    def download(self):
        self.fileManager.createAll(FileStage.RAW)

    def createPreDwC(self):
        self.fileManager.createAll(FileStage.PRE_DWC)

    def createDwC(self):
        self.fileManager.createAll(FileStage.DWC)

    def createDirectory(self):
        print(f"Creating directory for data: {str(self.databaseDir)}")
        self.databaseDir.mkdir(parents=True, exist_ok=True)

    def checkLeftovers(self, properties: dict):
        for property in properties:
            print(f"{self.location}-{self.database} unknown property: {property}")
    
    def getDataType(self):
        return self.dataType

    def getDBType(self):
        return self.dbType
    
    def getBaseDir(self):
        return self.databaseDir
    
    def getPreDWCFile(self, idx) -> StageFile:
        return self.fileManager.getFile(FileStage.PRE_DWC, idx)

class SpecificDB(Database):

    def postInit(self, properties):
        self.dbType = DBType.SPECIFIC
        self.files = properties.pop("files", None)

        if self.files is None:
            raise Exception("No provided files for source") from AttributeError

    def prepare(self):
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

    def postInit(self, properties):
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

    def prepare(self, recrawl=False):
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
    
    def postInit(self, properties):
        self.dbType = DBType.SCRIPTURL
        self.folderPrefix = properties.pop("folderPrefix", False)
        self.script = properties.pop("script", None)
        
        if self.script is None:
            raise Exception("No script specified") from AttributeError

        self.scriptStep = FileStep(self.script, SelectorParser(self.sourceDirectories, []))
        
    def prepare(self):
        urls = self.scriptStep.process()

        for url in urls:
            fileName = self.getFileNameFromURL(url)
            self.fileManager.addDownloadURLStage(url, fileName, self.globalProcessing)

        if self.combineProcessing:
            self.fileManager.addCombineStage(self.combineProcessing)
        
        self.fileManager.pushPreDwC()

class ScriptDataDB(Database):

    def postInit(self, properties):
        self.dbType = DBType.SCRIPTDATA
        self.script = properties.pop("script", None)

        if self.script is None:
            raise Exception("No script specified") from AttributeError

        self.scriptStep = FileStep(self.script, SelectorParser(self.downloadDir, []))
        
    def prepare(self):
        self.fileManager.addRetrieveScriptStage(self.scriptStep, self.fileProperties)

        if self.combineProcessing:
            self.fileManager.addCombineStage(self.combineProcessing)

        self.fileManager.pushPreDwC()
