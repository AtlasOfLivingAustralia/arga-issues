from datetime import datetime
from lib.processing.stages import Step
import json
from pathlib import Path

class TimeManager:
    def __init__(self, databaseDir: Path, fileName: str = "lastUpdates"):
        self.timeFile = databaseDir / f"{fileName}.json"

        if not self.timeFile.exists():
            self.timeData = {}
            return

        with open(self.timeFile) as fp:
            self.timeData = json.load(fp)

    def update(self, stageFileStep: Step):
        self.timeData[stageFileStep.name.lower()] = datetime.now().isoformat()
        
        with open(self.timeFile, "w") as fp:
            json.dump(self.timeData, fp, indent=4)
