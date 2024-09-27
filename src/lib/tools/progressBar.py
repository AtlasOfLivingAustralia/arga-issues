class ProgressBar:
    def __init__(self, barLength: int, processName: str = "Progress", decimalPlaces: float = 2):
        self.barLength = barLength
        self.processName = processName
        self.decimalPlaces = decimalPlaces

        self._loading = "-\\|/"
        self._pos = 0

    def _getLoadStage(self) -> str:
        self._pos = (self._pos + 1) % len(self._loading)
        return self._loading[self._pos]
    
    def _getBar(self, completion: int) -> str:
        # completion = atTask / self.taskCount
        length = min(int(self.barLength * completion), self.barLength)
        percentage = f"{completion*100:.02f}%"
        percentageLength = len(percentage)
        percentagePos = (self.barLength - percentageLength + 1) // 2
        secondHalfStart = percentageLength + percentagePos
        return f"[{min(length, percentagePos) * '='}{max(percentagePos - length, 0) * '-'}{percentage}{max(length - secondHalfStart, 0) * '='}{((self.barLength - secondHalfStart) - max(length - secondHalfStart, 0)) * '-'}]"

    def update(self, completion: float, extraInfo: str = "") -> int:
        output = f"> {self.processName}{' - ' if extraInfo else ''}{extraInfo} ({self._getLoadStage()}): {self._getBar(completion)}"
        print(output, end="\r")
        return len(output)

class UpdatableProgressBar(ProgressBar):
    def __init__(self, barLength: int, taskCount: int, processName: str = "Progress", newlineOnComplete: bool = True, decimalPlaces: int = 2):
        self.taskCount = taskCount
        self.newlineOnComplete = newlineOnComplete

        self._completed = False
    
        super().__init__(barLength, processName, decimalPlaces)

    def update(self, atTask: int, extraInfo: str = "") -> int:
        if self._completed:
            return
        
        outputLen = super().update(atTask / self.taskCount, extraInfo)

        if atTask == self.taskCount:
            self._completed = True

            if self.newlineOnComplete:
                print()

        return outputLen

class SteppableProgressBar(UpdatableProgressBar):
    def __init__(self, barLength: int, taskCount: int, processName: str = "Progress", newlineOnComplete: bool = True, decimalPlaces: int = 2, initialRender: bool = False):
        super().__init__(barLength, taskCount, processName, newlineOnComplete, decimalPlaces)

        if initialRender:
            self._atTask = -1
            self.update()
        else:
            self._atTask = 0

    def update(self, extraInfo: str = "") -> int:
        self._atTask += 1
        return super().update(self._atTask, extraInfo)
