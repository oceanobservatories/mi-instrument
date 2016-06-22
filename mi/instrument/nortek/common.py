import struct
from datetime import datetime

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
ID_BATTERY_DATA_PATTERN = r'(?:AQD|VEC) [0-9]{4} {0,6}\x06\x06([\x00-\xFF][\x13-\x46])\x06\x06'
ID_BATTERY_DATA_REGEX = re.compile(ID_BATTERY_DATA_PATTERN, re.DOTALL)

# [\x00, \x01, \x02, \x04, and \x05]
MODE_DATA_PATTERN = r'([\x00-\x02,\x04,\x05]\x00)(\x06\x06)'
MODE_DATA_REGEX = re.compile(MODE_DATA_PATTERN, re.DOTALL)

# ~5000mV (0x1388) minimum to ~18000mv (0x4650) maximum
BATTERY_DATA_PATTERN = r'([\x00-\xFF][\x13-\x46])\x06\x06'
BATTERY_DATA_REGEX = re.compile(BATTERY_DATA_PATTERN, re.DOTALL)

# ["VEC 8181", "AQD 8493      "]
ID_DATA_PATTERN = r'((?:AQD|VEC) [0-9]{4}) {0,6}\x06\x06'
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
    if len(word) != 2:
        raise SampleException("Invalid number of bytes in word input! Found %s with input %s" % (word, len(word)))

    convert, = struct.unpack('<H', word)
    log.trace('word %r, convert %r', word, convert)

    return convert

def convert_bytes_to_bit_field(input_bytes):
    """
    Convert bytes to a bit field, reversing bytes in the process.
    ie ['\x05', '\x01'] becomes [0, 0, 0, 1, 0, 1, 0, 1]
    @param input_bytes an array of string literal bytes.
    @retval an list of 1 or 0 in order
    """
    byte_list = list(input_bytes)
    byte_list.reverse()
    result = []
    for byte in byte_list:
        bin_string = bin(ord(byte))[2:].rjust(8, '0')
        result.extend([int(x) for x in list(bin_string)])
    log.trace("Returning a bitfield of %s for input string: [%s]", result, input_bytes)
    return result


def convert_words_to_datetime(input_bytes):
    """
    Convert block of 6 words into a date/time structure for the
    instrument family
    @param input_bytes 6 bytes
    @retval An array of 6 ints corresponding to the date/time structure
    @raise SampleException If the date/time cannot be found
    """
    if len(input_bytes) != 6:
        raise SampleException("Invalid number of bytes in input! Found %s" % len(input_bytes))

    minutes, seconds, day, hour, year, month, = struct.unpack('<6B', input_bytes)

    minutes = int('%02x' % minutes)
    seconds = int('%02x' % seconds)
    day = int('%02x' % day)
    hour = int('%02x' % hour)
    year = int('%02x' % year)
    month = int('%02x' % month)

    return [minutes, seconds, day, hour, year, month]


def convert_datetime_to_words(int_array):
    """
    Convert array if integers into a block of 6 words that could be fed
    back to the instrument as a timestamp.
    @param int_array An array of 6 hex values corresponding to a vector
    date/time stamp.
    @retval A string of 6 binary characters
    """
    if len(int_array) != 6:
        raise SampleException("Invalid number of bytes in date/time input! Found %s" % len(int_array))

    output = [chr(int(str(n), 16)) for n in int_array]
    return "".join(output)


def convert_time(response):
    """
    Converts the timestamp in BCD to a datetime object
    """
    minutes, seconds, day, hour, year, month = struct.unpack('6B', response)
    year = 2000 + int('%02x' % year)
    month = int('%02x' % month)
    day = int('%02x' % day)
    hour = int('%02x' % hour)
    minutes = int('%02x' % minutes)
    seconds = int('%02x' % seconds)
    dt = datetime(year, month, day, hour, minutes, seconds)
    return dt

