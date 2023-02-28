from enum import Enum, auto

class DBType(Enum):
    UNKNOWN = auto()
    SPECIFIC = auto()
    LOCATION = auto()
    SCRIPTURL = auto()
    SCRIPTDATA = auto()
