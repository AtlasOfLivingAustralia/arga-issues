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
logger.setLevel(logging.DEBUG)

# Setup handlers
# formatter = logging.Formatter("[%(asctime)s] %(module)s - %(levelname)s: %(message)s", "%H:%M:%S")
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S")

fileHandler = logging.FileHandler(filename=logFilePath)
fileHandler.setFormatter(formatter)
fileHandler.setLevel(logging.INFO)
logger.addHandler(fileHandler)

streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

logger.info("Logger initialised")