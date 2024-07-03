class ProgressBar:
    def __init__(self, barLength: int, processName: str = "Progress"):
        self.barLength = barLength
        self.processName = processName
        self._loading = "-\\|/"
        self._pos = 0

    def _getLength(self, completion: float) -> int:
        return int(completion * self.barLength)
    
    def _updatePos(self):
        self._pos = (self._pos + 1) % len(self._loading)

    def render(self, completion: float) -> None:
        length = self._getLength(completion)
        print(f"> {self.processName} ({self._loading[self._pos]}): [{length * '='}{(self.barLength - length) * '-'}]", end="\r")
        self._updatePos()

class AdvancedProgressBar(ProgressBar):
    def __init__(self, taskCount: int, barLength: int, processName: str = "Progress", newlineOnComplete: bool = True):
        self._taskCount = taskCount
        self._completed = False
        self._newlineOnComplete = newlineOnComplete

        super().__init__(barLength, processName)

    def render(self, atTask: int, extraInfo: str = "") -> int:
        if self._completed:
            return
        
        length = self._getLength(atTask / self._taskCount)
        output = f"> {self.processName}{" " if extraInfo else ""}{extraInfo} ({self._loading[self._pos]}): [{length * '='}{(self.barLength - length) * '-'}]"
        print(output)
        self._updatePos()

        if atTask == self._taskCount:
            self._completed = True

            if self._newlineOnComplete:
                print()

        return len(output)
