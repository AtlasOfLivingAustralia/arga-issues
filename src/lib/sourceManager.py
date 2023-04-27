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

    def packDB(self, location: str, database: str) -> str:
        return f"{location}-{database}"
    
    def unpackDB(self, source: str) -> tuple[str, str]:
        location, database = source.split('-')
        return (location, database)

    def choices(self) -> list:
        # return [f"{location}-{db}" for location, source in self.locations.items() for db in source.getDatabaseList()]
        output = []
        for locationName, sourceLocation in self.locations.items():
            for database in sourceLocation.getDatabaseList():
                output.append(self.packDB(locationName, database))

        return output

    def getDB(self, sources: list[str], enrich: bool = True) -> list[Database]:
        locations = []

        for source in sources:
            location, database = self.unpackDB(source)
            location = self.locations.get(location, None)

            if location is None:
                raise Exception(f"Invalid location: {location}")

            locations.append(location.loadDB(database, enrich))

        return locations
