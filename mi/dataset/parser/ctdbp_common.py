__author__ = 'tgupta'

from mi.core.common import BaseEnum
import re

from mi.dataset.parser.common_regexes import \
    END_OF_LINE_REGEX, \
    ANY_CHARS_REGEX

# regex for identifying start of a header line
START_HEADER = r'\*'

# Time tuple corresponding to January 1st, 2000
JAN_1_2000 = (2000, 1, 1, 0, 0, 0, 0, 0, 0)

# Header data:
HEADER_PATTERN = START_HEADER    # Header data starts with '*'
HEADER_PATTERN += ANY_CHARS_REGEX         # followed by text
HEADER_PATTERN += END_OF_LINE_REGEX         # followed by newline
HEADER_MATCHER = re.compile(HEADER_PATTERN)

# All ctdbp records are ASCII characters separated by a newline.
CTDBP_RECORD_PATTERN = ANY_CHARS_REGEX       # Any number of ASCII characters
CTDBP_RECORD_PATTERN += END_OF_LINE_REGEX       # separated by a new line
CTDBP_RECORD_MATCHER = re.compile(CTDBP_RECORD_PATTERN)


class CtdbpParserDataParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    TEMPERATURE = "temperature"
    CONDUCTIVITY = "conductivity"
    PRESSURE = "pressure"
    PRESSURE_TEMP = "pressure_temp"
    OXYGEN = "oxygen"
    CTD_TIME = "ctd_time"