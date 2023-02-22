import lib.config as cfg
from lib.sourceObjs.sourceLocation import SourceLocation
from lib.sourceObjs.sourceDatabase import Database

class SourceManager:
    
    def __init__(self):
        self.locations = {}
        
        for locationPath in cfg.folderPaths.sources.iterdir():
            if locationPath.is_file(): # Ignore files in directory
                continue

            location = locationPath.stem

            databases = {}
            for databaseSettingsPath in locationPath.iterdir():
                databaseName = databaseSettingsPath.stem
                databases[databaseName] = databaseSettingsPath

            self.locations[location] = SourceLocation(location, databases)

    def choices(self) -> list:
        # return [f"{location}-{db}" for location, source in self.locations.items() for db in source.getDatabaseList()]
        output = []
        for locationName, sourceLocation in self.locations.items():
            for database in sourceLocation.getDatabaseList():
                output.append(f"{locationName}-{database}")

        return output

    def getDB(self, source: str, enrich: bool = True) -> Database:
        location, database = source.split('-')
        location = self.locations.get(location, None)

        if location is None:
            raise Exception(f"Invalid location: {location}")

        return location.loadDB(database, enrich)
