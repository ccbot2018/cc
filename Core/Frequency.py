
from enum import Enum
import datetime as dt

class Frequency(Enum):
    hour = "hour"
    min = "minute"
    thirtymin = "thirtymin"
    fivemin = "fivemin"
    day = "day"
    snapshot = "snapshot"


FREQUENCIES_DICT = {Frequency.min : dt.timedelta(0,60),
                        Frequency.fivemin : dt.timedelta(0,360),
                        Frequency.thirtymin : dt.timedelta(0, 1800),
                        Frequency.hour : dt.timedelta(0,3600),
                        Frequency.day : dt.timedelta(1)}






