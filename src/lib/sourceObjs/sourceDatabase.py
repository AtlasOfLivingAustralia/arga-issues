import lib.config as cfg
import lib.commonFuncs as cmn
from abc import ABC, abstractclassmethod
from pathlib import Path
from lib.sourceObjs.dbTypes import DBType
from lib.sourceObjs.files import DBFile, PreDWCFile
from lib.processing.processor import Processor, Step, SelectorParser
import lib.processing.processingFuncs as pFuncs

class Database(ABC):

    def __init__(self, dataType: str, location: str, database: str, properties: dict = {}, enrichDBs: dict = {}):
        self.dataType = dataType
        self.location = location
        self.database = database
        self.enrichDBs = enrichDBs
        self.dbType = DBType.UNKNOWN

        # Standard properties
        self.authFile = properties.pop("auth", None)
        self.globalProcessing = properties.pop("globalProcessing", [])
        self.combineProcessing = properties.pop("combineProcessing", [])
        self.fileProperties = properties.pop("fileProperties", {})
        self.dwcProperties = properties.pop("dwcProperties", {})
        self.enrichDict = properties.pop("enrich", {}) # Dict to store enrich info
        
        self.processor = None

        self.dbFiles = []
        self.preDWCFiles = []
        self.outputs = []
        self.enrichDBs = {} # Dict to store references to enrich dbs

        self.locationDir = cfg.folderPaths.data / location
        self.databaseDir = self.locationDir / database

        self.user = ""
        self.password = ""

        if self.authFile is not None:
            with open(self.databaseDir / self.authFile) as fp:
                data = fp.read().splitlines()

            self.user = data[0].split('=')[1]
            self.password = data[1].split('=')[1]

        self.postInit(properties)
        self.checkLeftovers(properties)

    def __str__(self):
        return f"{self.location}-{self.database}, {self.outputs}"

    def __repr__(self):
        return str(self)

    @abstractclassmethod
    def postInit(self):
        pass

    @abstractclassmethod
    def prepare(self):
        pass

    @abstractclassmethod
    def createPreDwC(self):
        pass

    @abstractclassmethod
    def createDwC(self):
        pass

    def createDirectory(self):
        print(f"Creating directory for data: {str(self.databaseDir)}")
        self.databaseDir.mkdir(parents=True, exist_ok=True)

    def checkLeftovers(self, properties: dict):
        for property in properties:
            print(f"{self.location}-{self.database} unknown property: {property}")

    def addDBFile(self, url: str, fileName: Path, processingSteps: list[dict]) -> list[Path]:
        processor = Processor(self.databaseDir, [fileName], processingSteps)
        dbFile = DBFile(url, self.databaseDir, fileName, processor)
        self.dbFiles.append(dbFile)
        return dbFile.getOutputs()

    def addPreDWCFile(self, fileName: Path, properties: dict):
        preDWCFile = PreDWCFile(self.databaseDir, fileName, self.location, properties, self.dwcProperties, self.enrichDBs)
        self.preDWCFiles.append(preDWCFile)
        self.outputs.append(self.databaseDir / preDWCFile.getOutput())
    
    def getDataType(self):
        return self.dataType

    def getDBType(self):
        return self.dbType

    def validIdx(self, fileList: list, idx: int) -> bool:
        return idx >= 0 and idx < len(fileList)

    def getSourceFile(self, idx: int) -> DBFile:
        if not self.validIdx(self.dbFiles, idx):
            raise Exception(f"Invalid database file selected: {idx}") from FileNotFoundError
        return self.dbFiles[idx]

    def getPreDWCFile(self, idx: int) -> PreDWCFile:
        if not self.validIdx(self.preDWCFiles, idx):
            raise Exception(f"Invalid preDWC file selected: {idx}") from FileNotFoundError

        return self.preDWCFiles[idx]

    def getOutputFile(self, idx: int):
        if not self.validIDx(self.preDWCFiles, idx):
            raise Exception(f"Invalid output file selected: {idx}") from FileNotFoundError

        return self.outputs[idx]

    def downloadFile(self, idx):
        file = self.getSourceFile(idx)
        file.download(user=self.user, password=self.password)

    def downloadAllFiles(self):
        for idx, _ in enumerate(self.dbFiles):
            self.downloadFile(idx)

    def processFile(self, idx):
        file = self.getSourceFile(idx)
        file.process()

    def processAllFiles(self):
        for idx, _ in enumerate(self.dbFiles):
            self.processFile(idx)

    def combine(self):
        if not self.combineProcessing:
            return

        self.processor.process()

    def convertDwC(self, idx):
        file = self.getPreDWCFile(idx)
        file.convert()

    def convertAllDwC(self):
        for idx, _ in enumerate(self.preDWCFiles):
            self.convertDwC(idx)

class SpecificDB(Database):

    def postInit(self, properties):
        self.dbType = DBType.SPECIFIC
        self.files = properties.pop("files", None)

        if self.files is None:
            raise Exception("No provided files for source") from AttributeError

    def prepare(self):
        outputs = []
        for file in self.files:
            url = file.get("url", None)
            fileName = file.get("downloadedFile", None)
            processingSteps = file.get("processing", [])
            fileProperties = file.get("fileProperties", {})

            if url is None:
                raise Exception("No url provided for source") from AttributeError

            if fileName is None:
                raise Exception("No filename provided to download to") from AttributeError
            
            out = self.addDBFile(url, Path(fileName), processingSteps)
            outputs.extend(out)
        
         # If no processing for combination is required
        if not self.combineProcessing:
            for file in outputs:
                self.addPreDWCFile(file, fileProperties)
            return

        # Outputs require combining, so preDWC files are a result of combining
        self.processor = Processor(self.databaseDir, outputs, self.combineProcessing)
        for file in self.processor.getOutputFiles():
            self.addPreDWCFile(file, self.fileProperties)

    def createPreDwC(self):
        self.downloadAllFiles()
        self.processAllFiles()
        self.combine()

    def createDwC(self):
        self.createPreDwC()
        self.convertAllDwC()

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
                lines = fp.read().splitlines()

            for url in lines:
                fileName = url.rsplit('/', 1)[1]
                out = self.addDBFile(url, Path(fileName), self.globalProcessing)
                for file in out:
                    self.addPreDWCFile(file, self.fileProperties)

            return

        localFilePath.unlink(True)
        print("Crawling...")

        foundFiles = cmn.crawl(self.fileLocation, self.regexMatch, self.maxSubDirDepth, user=self.user, password=self.password)
        for file in foundFiles:
            with open(localFilePath, 'a') as fp:
                fp.write(file + "\n")

            fileName = file.rsplit('/')[1]
            out = self.addDBFile(file, Path(fileName), self.combineProcessing)
            for outputFile in out:
                self.addPreDWCFile(outputFile, self.fileProperties)

    def createPreDwC(self, firstFile, fileAmount):
        for idx in range(firstFile, firstFile + fileAmount):
            self.downloadFile(idx)
            self.processFile(idx)

    def createDwC(self, firstFile, fileAmount):
        self.createPreDwC(firstFile, fileAmount)
        for idx in range(firstFile, firstFile + fileAmount):
            self.convertDwC(idx)

class ScriptUrlDB(Database):
    
    def postInit(self, properties):
        self.dbType = DBType.SCRIPTURL
        self.step = Step(properties, SelectorParser(self.databaseDir, []))
        self.folderPrefix = properties.pop("folderPrefix", False)
        
    def prepare(self):
        outputs = []
        urls = self.step.process()

        for url in urls:
            urlParts = url.split('/')
            fileName = urlParts[-1]

            if self.folderPrefix:
                folderName = urlParts[-2]
                fileName = f"{folderName}_{fileName}"

            out = self.addDBFile(url, Path(fileName), self.globalProcessing)
            outputs.extend(out)

        if not self.combineProcessing:
            for outputFile in outputs:
                self.addPreDWCFile(outputFile, self.fileProperties)
            return
        
        self.processor = Processor(self.databaseDir, outputs, self.combineProcessing)
        for file in self.processor.getOutputFiles():
            self.addPreDWCFile(file, self.fileProperties)

    def createPreDwC(self, firstFile, fileAmount):
        if self.combineProcessing:
            self.downloadAllFiles()
            self.processAllFiles()
            self.combine()
            return
        
        for idx in range(firstFile, firstFile + fileAmount):
            self.downloadFile(idx)
            self.processFile(idx)

    def createDwC(self, firstFile, fileAmount):
        self.createPreDwC(firstFile, fileAmount)
        for idx in range(firstFile, firstFile + fileAmount):
            self.convertDwC(idx)

class ScriptDataDB(Database):

    def postInit(self, properties):
        self.dbType = DBType.SCRIPTDATA
        self.step = Step(properties, SelectorParser(self.databaseDir, []))
        self.folderPrefix = properties.pop("folderPrefix", False)
        
    def prepare(self):
        outputs = []
        for file in self.step.outputFiles:
            out = self.addDBFile("", Path(file), self.globalProcessing)
            outputs.extend(out)

        if not self.combineProcessing:
            for outputFile in outputs:
                self.addPreDWCFile(outputFile, self.fileProperties)
            return
        
        self.processor = Processor(self.databaseDir, outputs, self.combineProcessing)
        for file in self.processor.getOutputFiles():
            self.addPreDWCFile(file, self.fileProperties)

    def createPreDwC(self, firstFile, fileAmount):
        self.step.process()

    def createDwC(self, firstFile, fileAmount):
        self.createPreDwC(firstFile, fileAmount)
        for idx in range(firstFile, firstFile + fileAmount):
            self.convertDwC(idx)
