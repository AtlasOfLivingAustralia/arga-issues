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
            for database in sourceLocation.getDatabases():
                output.append(self._packDB(locationName, database))

        return output
    
    def getLocations(self) -> dict[str, 'Location']:
        return self.locations
    
    def getDBs(self, source: str, subsection: str = "all") -> list[Database]:
        location, database = self._unpackDB(source)
        location = self.locations.get(location, None)

        if location is None:
            raise Exception(f"Invalid location: {location}")
        
        return location.loadDBs(database, subsection)
        
class Location:
    def __init__(self, locationPath: Path):
        self.locationPath = locationPath
        self.locationName = locationPath.stem

        self.configFile = "config.json"
        self.dbMapping = {
            "url": Database,
            "crawl": CrawlDB,
            "script": ScriptDB
        }

        # Setup databases
        self.databases: list = []
        for databaseFolder in locationPath.iterdir():
            if databaseFolder.is_file(): # Skip files
                continue

            self.databases.append(databaseFolder.stem)

    def getDatabases(self) -> list[str]:
        return self.databases
    
    def _loadConfig(self, database: str) -> dict:
        if database not in self.databases:
            raise Exception(f"Invalid database '{database}' for location '{self.locationName}'")
        
        configPath = self.locationPath / database / self.configFile
        if not configPath.exists():
            raise Exception(f"No config file found for database '{database}'")
        
        with open(configPath) as fp:
            return json.load(fp)

    def loadDBs(self, database: str, subsection: str = "all") -> Database:
        config = self._loadConfig(database)

        retrieveType = config.pop("retrieveType", None)
        if retrieveType is None:
            raise Exception("No retrieve type specified for database")
        
        dbType = self.dbMapping.get(retrieveType, None)

        if dbType is None:
            raise Exception(f"Invalid retrieve type: {retrieveType}. Should be one of: {', '.join(self.dbMapping.keys())}")
        
        subsections = config.pop("subsections", [])
        if not subsections:
            return [dbType(self.locationName, database, "", config)]

        if subsection == "all":
            return [dbType(self.locationName, database, section, config) for section in subsections]
        
        if subsection not in subsections:
            raise Exception(f"Invalid subsection: {subsection}")
        
        return [dbType(self.locationName, database, subsection, config)]
