import json
from pathlib import Path
from lib.processing.stages import Step
from lib.tools.logger import Logger
from datetime import datetime

class MetadataManager:
    _stepKeys = {
        Step.DOWNLOAD: "downloading",
        Step.PROCESSING: "processing",
        Step.CONVERSION: "converting"
    }

    def __init__(self, databaseDir: Path):
        self.metadataPath = databaseDir / "metadata.json"
        self._load()
        
    def _load(self) -> None:
        if self.metadataPath.exists():       
            try:
                with open(self.metadataPath) as fp:
                    self.data = json.load(fp)
                    return
                
            except json.JSONDecodeError:
                self.metadataPath.unlink()

        self.data = {}

    def _save(self) -> None:
        with open(self.metadataPath, "w") as fp:
            json.dump(self.data, fp, indent=4)

    def update(self, step: Step, metadata: dict) -> None:
        if not metadata:
            Logger.info("No metadata to update")
            return
        
        key = self._stepKeys[step]
        self.data[key] = metadata
        self._save()

        Logger.info(f"Updated {key} metadata and saved to file")

    def getLastDownloadUpdate(self) -> datetime | None:
        subsectionData = self.data.get(self._stepKeys[Step.DOWNLOAD], None)
        if subsectionData is None:
            return None
        
        return min(datetime.fromisoformat(item["timestamp"]) for item in subsectionData["files"])
