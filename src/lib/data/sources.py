import json
import lib.config as cfg
from pathlib import Path
from lib.data.database import Database, CrawlDB, ScriptDB
from lib.tools.logger import Logger

class SourceManager:
    def __init__(self):
        self.locations: dict[str, Location] = {}
        
        for locationPath in cfg.Folders.dataSources.iterdir():
            locationObj = Location(locationPath)
            self.locations[locationObj.locationName] = locationObj

    def _packDB(self, location: str, database: str) -> str:
        return f"{location}-{database}"
    
    def _unpackDB(self, source: str) -> tuple[str, str]:
        location, database = source.split('-')
        return (location, database)

    def choices(self) -> list[str]:
        # return [f"{location}-{db}" for location, source in self.locations.items() for db in source.getDatabaseList()]
        output = []
        for locationName, sourceLocation in self.locations.items():
            for database in sourceLocation.getDatabaseList():
                output.append(self._packDB(locationName, database))

        return output
    
    def getLocations(self) -> dict[str, 'Location']:
        return self.locations
    
    def getDB(self, source: str) -> Database:
        location, database = self._unpackDB(source)
        location = self.locations.get(location, None)

        if location is None:
            raise Exception(f"Invalid location: {location}")
        
        return location.loadDB(database)

    def getMultipleDBs(self, sources: list[str]) -> list[Database]:
        return [self.getDB(source) for source in sources]

class Location:
    def __init__(self, locationPath: Path):
        self.locationPath = locationPath
        self.locationName = locationPath.stem

        self.databases: list[str] = []
        for databaseFolder in locationPath.iterdir():
            if databaseFolder.is_file(): # Skip files
                continue

            self.databases.append(databaseFolder.stem)

        self.configFile = "config.json"
        self.dbMapping = {
            "url": Database,
            "crawl": CrawlDB,
            "script": ScriptDB
        }

    def getDatabaseList(self) -> list:
        return self.databases
    
    def _loadConfig(self, database: str) -> dict:
        if database not in self.databases:
            raise Exception(f"Invalid database '{database}' for location '{self.locationName}'")
        
        configPath = self.locationPath / database / self.configFile
        if not configPath.exists():
            raise Exception(f"No config file found for database '{database}'")
        
        with open(configPath) as fp:
            return json.load(fp)

    def loadDB(self, database: str) -> Database:
        config = self._loadConfig(database)

        retrieveType = config.pop("retrieveType", None)
        if retrieveType is None:
            raise Exception("No retrieve type specified for database")
        
        dbType = self.dbMapping.get(retrieveType, None)

        if dbType is None:
            raise Exception(f"Invalid retrieve type: {retrieveType}. Should be one of: {', '.join(self.dbMapping.keys())}")
        
        return dbType(self.locationName, database, config)
