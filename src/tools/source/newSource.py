import lib.config as cfg
import argparse
from lib.data.sources import SourceManager
from pathlib import Path
import json

def _specificConfig() -> dict:
    return {
        "dbType": "specific",
        "files": []
    }

def _locationConfig() -> dict:
    return {
        "dbType": "location",
        "dataLocation": "",
        "regexMatch": ""
    }

def _scriptConfig() -> dict:
    return {
        "dbType": "script",
        "script": {
            "path": "",
            "function": "",
            "args": [],
            "output": []
        }
    }

def _defaultConfig() -> dict:
    return {
        "dbType": "",
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate folders for a new data source")
    parser.add_argument("location", help="Location of the data source")
    parser.add_argument("database", help="Database name for location")
    parser.add_argument("-t", "--type", choices=["specific", "location", "script"], help="Extra setup if db type is known")
    args = parser.parse_args()

    sourceManager = SourceManager()
    locationFolder: Path = cfg.Folders.dataSources / args.location
    databaseFolder: Path = locationFolder / args.database

    if databaseFolder.exists():
        print(f"Database {args.location}-{args.database} already exists, exiting...")
        exit()

    if not locationFolder.exists():
        print(f"Creating new location folder for: {args.location}")
        locationFolder.mkdir()
    else:
        print(f"Location '{args.location}' already exists, creating within")

    print(f"Creating database folder: {args.database}")
    databaseFolder.mkdir()

    # Setting up database folder
    dataFolder = databaseFolder / "data"
    dataFolder.mkdir()

    configs = {
        "specific": _specificConfig,
        "location": _locationConfig,
        "script": _scriptConfig
    }

    config = configs.get(args.type, _defaultConfig)

    configFile = databaseFolder / "config.json"
    with open(configFile, "w") as fp:
        json.dump(config(), fp, indent=4)
