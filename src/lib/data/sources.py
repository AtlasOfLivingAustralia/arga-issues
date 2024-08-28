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
            Logger.error(f"Invalid location: {location}")
            return []
        
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
    
    def _loadConfig(self, database: str) -> dict | None:
        if database not in self.databases:
            Logger.error(f"Invalid database '{database}' for location '{self.locationName}'")
            return None
        
        configPath = self.locationPath / database / self.configFile
        if not configPath.exists():
            Logger.error(f"No config file found for database '{self.locationName}-{database}'")
            return None
        
        with open(configPath) as fp:
            return json.load(fp)
        
    def _translateSubsection(self, config: dict, subsection: str, subvalue: str) -> dict:

        def translate(obj: any) -> any:
            if isinstance(obj, str):
                return obj.replace("{SUBSECTION}", subsection).replace("{SUBSECTION:VALUE}", subvalue)
            
            if isinstance(obj, list):
                return [translate(item) for item in obj]
            
            if isinstance(obj, dict):
                return {key: translate(value) for key, value in obj.items()}
            
            return obj

        return {key: translate(value) for key, value in config.items()}

    def loadDBs(self, database: str, subsection: str = "all") -> list[Database]:
        config = self._loadConfig(database)
        if config is None:
            return []

        retrieveType = config.pop("retrieveType", None)
        if retrieveType is None:
            Logger.error(f"No retrieve type specified for database {database}")
        
        dbType = self.dbMapping.get(retrieveType, None)

        if dbType is None:
            Logger.error(f"Database {database} has invalid retrieve type: {retrieveType}. Should be one of: {', '.join(self.dbMapping.keys())}")
            return []

        subsections: list = config.pop("subsections", [])

        if not subsections: # No subsections in config
            try:
                return [dbType(self.locationName, database, "", config)]
            except AttributeError:
                return []
        
        subsectionLookup = {}
        for section in subsections:
            parts = section.split(":")
            if len(parts) == 1:
                subsectionLookup[parts[0]] = ""
            elif len(parts) == 2:
                subsectionLookup[parts[0]] = parts[1]
            else:
                Logger.warning(f"Bad config subsection: {section}")

        if subsection == "all":
            retVal = []
            for subKey, value in subsectionLookup.items():
                try:
                    retVal.append(dbType(self.locationName, database, subKey, self._translateSubsection(config, subKey, value)))
                except AttributeError:
                    continue
                
            return retVal
        
        if subsection not in subsectionLookup:
            Logger.error(f"Invalid subsection: {subsection}")
            return []
        
        try:
            return [dbType(self.locationName, database, subsection, self._translateSubsection(config, subsection, subsectionLookup[subsection]))]
        except AttributeError:
            return []
