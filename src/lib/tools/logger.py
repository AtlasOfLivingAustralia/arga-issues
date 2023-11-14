import logging
import lib.config as cfg
from pathlib import Path
from datetime import datetime
import sys

# Log file information
logFolder: Path = cfg.folders.logs
logFileName = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
logFilePath = logFolder / f"{logFileName}.log" 

# Configure root logger
logging.basicConfig(
    filename=logFilePath,
    format="[%(asctime)s] %(module)s - %(level)s: %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG
)

# logger object to import from other modules
logger = logging.getLogger()

# Add logging data to stdout
logger.addHandler(logging.StreamHandler(sys.stdout))
