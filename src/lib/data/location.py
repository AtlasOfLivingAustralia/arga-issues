from lib.data.database import Database, SpecificDB, LocationDB, ScriptDB
import json

class SourceLocation:

    def __init__(self, location: str, databaseItems: dict):
        self.location = location
        self.databaseItems = databaseItems
        self.databases = {}

        self.dbMapping = {
            "specific": SpecificDB,
            "location": LocationDB,
            "script": ScriptDB
        }

    def getDatabaseList(self) -> list:
        return list(self.databaseItems.keys())

    def createDB(self, database: str, databaseInfo: dict) -> Database:
        dbType = databaseInfo.pop("dbType", "(Not Provided)")
        db = self.dbMapping.get(dbType, None)

        if db is None:
            raise Exception(f"Invalid dbType: {dbType}. Should be one of ({self.dbMapping.keys()})")
        
        return db(self.location, database, databaseInfo)

    def loadDB(self, database: str) -> Database:
        databasePath = self.databaseItems.get(database, None)

        if databasePath is None:
            raise Exception(f"Invalid database selected: {database}")
        
        with open(databasePath) as fp:
            databaseInfo = json.load(fp)

        db = self.createDB(database, databaseInfo)
        return db
