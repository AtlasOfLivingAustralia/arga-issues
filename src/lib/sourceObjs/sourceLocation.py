from lib.sourceObjs.sourceDatabase import Database, SpecificDB, LocationDB, ScriptDB
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

    def getTopLevelData(self, databaseInfo: dict) -> tuple[str, dict]:
        dbType = databaseInfo.pop("dbType", "Unknown") # Describes how the data is stored in the db (specific, location)
        enrichDBs = databaseInfo.pop("enrich", {}) # Databases needed to enrich this data

        return (dbType, enrichDBs)

    def createDB(self, dbType: str, database: str, databaseInfo: dict, enrichDBs: dict) -> Database:
        db = self.dbMapping.get(dbType, None)

        if db is None:
            raise Exception(f"Invalid dbType: {dbType}. Should be one of ({self.dbMapping.keys()})")
        
        return db(self.location, database, databaseInfo, enrichDBs)

    def loadDB(self, database: str, enrich: bool = False) -> Database:
        databasePath = self.databaseItems.get(database, None)

        if databasePath is None:
            raise Exception(f"Invalid database selected: {database}")
        
        with open(databasePath) as fp:
            databaseInfo = json.load(fp)

        dbType, enrichDBs = self.getTopLevelData(databaseInfo)
        
        enrichment = {}
        if enrich:
            for enrichDatabase, enrichKey in enrichDBs.items():
                enrichDB = self.loadDB(enrichDatabase, False) # Don't load enrich for enrich dbs
                enrichment[enrichKey] = enrichDB

        db = self.createDB(dbType, database, databaseInfo, enrichment)
        db.prepare()
        return db
