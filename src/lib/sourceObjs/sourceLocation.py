from lib.sourceObjs.sourceDatabase import Database, SpecificDB, LocationDB, ScriptDataDB, ScriptUrlDB
import json

class SourceLocation:

    def __init__(self, location: str, databaseItems: dict):
        self.location = location
        self.databaseItems = databaseItems
        self.databases = {}

        self.dbMapping = {
            "specific": SpecificDB,
            "location": LocationDB,
            "scripturl": ScriptUrlDB,
            "scriptdata": ScriptDataDB
        }

    def getDatabaseList(self) -> list:
        return list(self.databaseItems.keys())

    def getTopLevelData(self, databaseInfo: dict) -> tuple[str, str, dict]:
        dbType = databaseInfo.pop("dbType", "Unknown") # Describes how the data is stored in the db (specific, location)
        dataType = databaseInfo.pop("dataType", "Unknown") # Describes what type of data this is (source, enrich)
        enrichDBs = databaseInfo.pop("enrich", {}) # Databases needed to enrich this data

        return (dbType, dataType, enrichDBs)

    def createDB(self, dbType: str, dataType: str, database: str, databaseInfo: dict, enrichDBs: dict) -> Database:
        db = self.dbMapping.get(dbType, None)

        if db is None:
            raise Exception(f"Invalid dbType: {dbType}. Should be one of ({self.dbMapping.keys()})")
        
        return db(dataType, self.location, database, databaseInfo, enrichDBs)

    def loadDB(self, database: str, enrich: bool = False) -> Database:
        databasePath = self.databaseItems.get(database, None)

        if databasePath is None:
            raise Exception(f"Invalid database selected: {database}")
        
        with open(databasePath) as fp:
            databaseInfo = json.load(fp)

        dbType, dataType, enrichDBs = self.getTopLevelData(databaseInfo)
        
        enrichment = {}
        if enrich:
            for enrichDatabase, enrichKey in enrichDBs.items():
                enrichDB = self.loadDB(enrichDatabase, False) # Don't load enrich for enrich dbs
                enrichment[enrichKey] = enrichDB

        db = self.createDB(dbType, dataType, database, databaseInfo, enrichment)
        db.prepare()
        return db
    