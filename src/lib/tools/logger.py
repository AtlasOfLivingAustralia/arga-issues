import logging
import lib.config as cfg
from pathlib import Path
from datetime import datetime

# Log file information
logFolder: Path = cfg.folders.logs
logFileName = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

# Configure root logger
logging.basicConfig(
    filename=logFolder / logFileName,
    format="[%(asctime)s] %(module)s - %(level)s: %(message)s",
    datefmt="%H:%M:%S",
    level=logging.WARNING
)

# logger object to import from other modules
logger = logging.getLogger()
