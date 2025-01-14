from lib.data.sources import SourceManager
from lib.processing.stages import Step

if __name__ == "__main__":
    manager = SourceManager()
    locations = manager.getLocations()

    databases = []
    for locationName in locations:
        databases.extend(manager.requestDBs(locationName))

    for db in databases:
        print(db.location, db.database, db.checkUpdateReady())
    # print("\n".join(str(item) for item in databases))