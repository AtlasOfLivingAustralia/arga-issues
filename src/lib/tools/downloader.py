import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from lib.tools.logger import Logger

class Downloader:
    def __init__(self, defaultChunksize: int = 1024*1024, username: str = "", password: str = "", barLength: int = 20):
        self.defaultChunksize = defaultChunksize
        self.auth = HTTPBasicAuth(username, password) if username else None
        self.barLength = barLength

        self._loading = "-\\|/"
        self._pos = 0

    def _renderProgress(self, completion: float) -> None:
        length = int(completion * self.barLength)
        print(f"Downloading {self._loading[self._pos]} | [{length * '='}{(self.barLength - length) * '-'}]", end="\r")
        self._pos = (self._pos + 1) % 4

    def download(self, url: str, filePath: Path, customChunksize: int = -1, verbose: bool = False) -> bool:
        if verbose:
            Logger.info(f"Downloading from {url} to file {filePath}")

        with requests.get(url, stream=True, auth=self.auth) as stream:
            try:
                stream.raise_for_status()
            except HTTPError:
                Logger.error("Received HTTP error")
                return False
            
            fileSize = int(stream.headers.get("Content-Length", 0))
            chunksize = customChunksize if customChunksize > 0 else self.defaultChunksize

            with open(filePath, "wb") as fp:
                for idx, chunk in enumerate(stream.iter_content(chunksize)):
                    if fileSize > 0:
                        self._renderProgress((idx * chunksize) / fileSize)
                    else:
                        print(f"Downloading chunk: {idx}", end="\r")

                    fp.write(chunk)

        print()
        return True
