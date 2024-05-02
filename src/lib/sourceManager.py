import lib.config as cfg
from lib.data.location import SourceLocation
from lib.data.database import Database

class SourceManager:
    
    def __init__(self):
        self.locations: dict[str, SourceLocation] = {}
        
        for locationPath in cfg.Folders.dataSources.iterdir():
            if locationPath.is_file(): # Ignore files in directory
                continue

            location = locationPath.stem

            databases = {}
            for databaseFolder in locationPath.iterdir():
                databaseName = databaseFolder.stem
                databases[databaseName] = databaseFolder / "config.json"

            self.locations[location] = SourceLocation(location, databases)

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
    
    def getLocations(self) -> dict[str, SourceLocation]:
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
