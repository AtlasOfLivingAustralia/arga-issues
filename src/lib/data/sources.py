import json
import lib.config as cfg
from lib.data.database import Database, CrawlDB, ScriptDB
from lib.tools.logger import Logger

class SourceManager:
    def __init__(self):
        self.locations: dict[str, Location] = {}
        
        for locationPath in cfg.Folders.dataSources.iterdir():
            if locationPath.is_file(): # Ignore files in directory
                continue

            location = locationPath.stem

            databases = {}
            for databaseFolder in locationPath.iterdir():
                databaseName = databaseFolder.stem
                databases[databaseName] = databaseFolder / "config.json"

            self.locations[location] = Location(location, databases)

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

    def getDB(self, sources: list[str]) -> list[Database]:
        locations = []

        for source in sources:
            location, database = self._unpackDB(source)
            location = self.locations.get(location, None)

            if location is None:
                raise Exception(f"Invalid location: {location}")

            locations.append(location.loadDB(database))

        return locations

class Location:
    def __init__(self, location: str, databaseItems: dict):
        self.location = location
        self.databaseItems = databaseItems
        self.databases = {}

        self.dbMapping = {
            "url": Database,
            "crawl": CrawlDB,
            "script": ScriptDB
        }

    def getDatabaseList(self) -> list:
        return list(self.databaseItems.keys())

    def createDB(self, database: str, databaseInfo: dict) -> Database:
        retrieveType = databaseInfo.pop("retrieveType", "(Not Provided)")
        db = self.dbMapping.get(retrieveType, None)

        if db is None:
            Logger.error(f"Invalid retrieveType: {retrieveType}")
            raise Exception(f"Invalid retrieveType: {retrieveType}. Should be one of ({self.dbMapping.keys()})")
        
        return db(self.location, database, databaseInfo)

    def loadDB(self, database: str) -> Database:
        databasePath = self.databaseItems.get(database, None)

        if databasePath is None:
            raise Exception(f"Invalid database selected: {database}")
        
        with open(databasePath) as fp:
            databaseInfo = json.load(fp)

        return self.createDB(database, databaseInfo)
