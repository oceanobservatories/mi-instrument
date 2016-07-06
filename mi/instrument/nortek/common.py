import struct
from datetime import datetime

import binascii
import re

from mi.core.exceptions import SampleException
from ooi.logging import log

# newline.
NEWLINE = '\n\r'

# default timeout.
TIMEOUT = 15
# offset to accurately set instrument clock, in seconds
CLOCK_SYNC_OFFSET = 2.0
# maximum acceptable time difference when verifying clock sync, in seconds
CLOCK_SYNC_MAX_DIFF = 2
# sample collection is ~60 seconds, add padding
SAMPLE_TIMEOUT = 70
# set up the 'structure' lengths (in bytes) and sync/id/size constants
CHECK_SUM_SEED = 0xb58c

HW_CONFIG_LEN = 48
HW_CONFIG_SYNC_BYTES = '\xa5\x05\x18\x00'
HARDWARE_CONFIG_DATA_PATTERN = r'(%s)(.{44})(\x06\x06)' % HW_CONFIG_SYNC_BYTES
HARDWARE_CONFIG_DATA_REGEX = re.compile(HARDWARE_CONFIG_DATA_PATTERN, re.DOTALL)

HEAD_CONFIG_LEN = 224
HEAD_CONFIG_SYNC_BYTES = '\xa5\x04\x70\x00'
HEAD_CONFIG_DATA_PATTERN = r'(%s)(.{220})(\x06\x06)' % HEAD_CONFIG_SYNC_BYTES
HEAD_CONFIG_DATA_REGEX = re.compile(HEAD_CONFIG_DATA_PATTERN, re.DOTALL)

USER_CONFIG_LEN = 512
USER_CONFIG_SYNC_BYTES = '\xa5\x00\x00\x01'
USER_CONFIG_DATA_PATTERN = r'(%s)(.{508})(\x06\x06)' % USER_CONFIG_SYNC_BYTES
USER_CONFIG_DATA_REGEX = re.compile(USER_CONFIG_DATA_PATTERN, re.DOTALL)

# min, sec, day, hour, year, month
CLOCK_DATA_PATTERN = r'([\x00-\x60])([\x00-\x60])([\x01-\x31])([\x00-\x24])([\x00-\x99])([\x01-\x12])\x06\x06'
CLOCK_DATA_REGEX = re.compile(CLOCK_DATA_PATTERN, re.DOTALL)

# Special combined regex to give battery voltage a "unique sync byte" to search for (non-unique regex workaround)
ID_BATTERY_DATA_PATTERN = r'(?:AQD|VEC) ?[0-9]{4,5} {0,6}\x06\x06([\x00-\xFF]-?[\x13-\x46])\x06\x06'
ID_BATTERY_DATA_REGEX = re.compile(ID_BATTERY_DATA_PATTERN, re.DOTALL)

# [\x00, \x01, \x02, \x04, and \x05]
MODE_DATA_PATTERN = r'([\x00-\x02,\x04,\x05]\x00)(\x06\x06)'
MODE_DATA_REGEX = re.compile(MODE_DATA_PATTERN, re.DOTALL)

# ~5000mV (0x1388) minimum to ~18000mv (0x4650) maximum
BATTERY_DATA_PATTERN = r'([\x00-\xFF][\x13-\x46])\x06\x06'
BATTERY_DATA_REGEX = re.compile(BATTERY_DATA_PATTERN, re.DOTALL)

# ["VEC 8181", "AQD 8493      "]
ID_DATA_PATTERN = r'((?:AQD|VEC) ?[0-9]{4,5}) {0,6}\x06\x06'
ID_DATA_REGEX = re.compile(ID_DATA_PATTERN, re.DOTALL)


NORTEK_COMMON_REGEXES = [USER_CONFIG_DATA_REGEX,
                         HARDWARE_CONFIG_DATA_REGEX,
                         HEAD_CONFIG_DATA_REGEX,
                         ID_BATTERY_DATA_REGEX,
                         CLOCK_DATA_REGEX]

INTERVAL_TIME_REGEX = r"([0-9][0-9]:[0-9][0-9]:[0-9][0-9])"


def convert_word_to_int(word):
    """
    Converts a word into an integer field
    """
    try:
        return struct.unpack('<H', word)[0]
    except struct.error:
        raise SampleException("Invalid number of bytes in word input! Found %s with input %s" % (word, len(word)))


def convert_word_to_bit_field(word):
    """
    Convert little-endian short to a bit field
    @param input_bytes
    @retval an list of 1 or 0 in order
    """
    try:
        short = struct.unpack('<H', word)[0]
        return [int(x) for x in format(short, '016b')]
    except struct.error:
        raise SampleException("Invalid number of bytes in word input! Found %s with input %s" % (word, len(word)))


def convert_bcd_bytes_to_ints(input_bytes):
    """
    Convert block of 6 BCD-encoded bytes into a date/time structure for the instrument family
    @param input_bytes 6 bytes
    @retval An array of 6 ints corresponding to the date/time structure
    @raise SampleException If the date/time cannot be found
    """
    if len(input_bytes) != 6:
        raise SampleException("Invalid number of bytes in input! Found %s" % len(input_bytes))

    return [int(binascii.hexlify(c)) for c in input_bytes]


def convert_time(response):
    """
    Converts the timestamp in BCD to a datetime object
    """
    minutes, seconds, day, hour, year, month = convert_bcd_bytes_to_ints(response)
    return datetime(year + 2000, month, day, hour, minutes, seconds)
