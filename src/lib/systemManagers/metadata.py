import json
from pathlib import Path
from lib.processing.stages import Step

class MetadataManager:
    _stepKeys = {
        Step.DOWNLOAD: "downloading",
        Step.PROCESSING: "processing",
        Step.CONVERSION: "converting"
    }

    def __init__(self, metadataDir: Path):
        self.metadataPath = metadataDir / "metadata.json"
        self._load()
        
    def _load(self) -> None:
        if not self.metadataPath.exists():
            self.data = {}
            return
        
        with open(self.metadataPath) as fp:
            self.data = json.load(fp)

    def _save(self) -> None:
        with open(self.metadataPath, "w") as fp:
            json.dump(self.data, fp, indent=4)

    def update(self, step: Step, metadata: dict) -> None:
        if not metadata:
            return
        
        self.data[self._stepKeys[step]] = metadata
        self._save()
