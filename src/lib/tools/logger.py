import logging
import lib.config as cfg
from pathlib import Path
from datetime import datetime
import sys

# Log file information
logFolder: Path = cfg.folders.logs
logFileName = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logFilePath = logFolder / f"{logFileName}.log"

# Get root logger
logger = logging.getLogger()

# Configure logger
formatter = logging.Formatter("[%(asctime)s] %(module)s - %(level)s: %(message)s", "%H:%M:%S")

fileHandler = logging.FileHandler(filename=str(logFilePath))
fileHandler.setFormatter(formatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(formatter)
streamHandler.setLevel(logging.DEBUG)
logger.addHandler(streamHandler)

logger.debug("Logger Initialised")
