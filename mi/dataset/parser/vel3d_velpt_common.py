
import struct
import ntplib
import calendar
from mi.core.log import get_logger
log = get_logger()

# This marks the first byte in all record types
SYNC_MARKER = b'\xA5'

# This byte follows the Sync Byte and lets the parser know
# what type of record is being parsed.
HARDWARE_CONFIGURATION_ID = b'\x05'
HEAD_CONFIGURATION_ID = b'\x04'
USER_CONFIGURATION_ID = b'\x00'


def _convert_bcd_to_decimal(in_val):
    """
    Converts Binary Coded Decimal to a decimal value
    :param in_val: The value to convert
    :return: The decimal value
    """
    tens = (struct.unpack('B', in_val)[0]) >> 4
    actual = struct.unpack('B', in_val)[0]
    low_byte = tens << 4
    return (tens*10) + (actual-low_byte)


def get_timestamp_tuple(record, start_byte=4):
    """
    Convert the date and time from the record to a tuple of time values
    :param record: The record read from the file which contains the date and time
    :param start_byte: optional input of starting byte, defaults to 4
    :return: the Unix timestamp
    """
    # for records that have a timestamp, they occur in the 4th to 9th bytes, in the order
    # minute, second, day, hour, year, month, where year must have 2000 added
    minute = _convert_bcd_to_decimal(record[start_byte])
    second = _convert_bcd_to_decimal(record[start_byte+1])
    day = _convert_bcd_to_decimal(record[start_byte+2])
    hour = _convert_bcd_to_decimal(record[start_byte+3])
    year = 2000 + _convert_bcd_to_decimal(record[start_byte+4])
    month = _convert_bcd_to_decimal(record[start_byte+5])

    return year, month, day, hour, minute, second


def get_date_time_string(record, start_byte=4):
    """
    Convert the date and time from the record to the standard string YYYY/MM/DD HH:MM:SS
    :param record: The record read from the file which contains the date and time
    :param start_byte: optional input of starting byte, defaults to 4
    :return: The date time string
    """
    time_tuple = get_timestamp_tuple(record, start_byte)
    return "{:d}/{:0>2d}/{:0>2d} {:0>2d}:{:0>2d}:{:0>2d}".format(time_tuple[0], time_tuple[1], time_tuple[2],
                                                                 time_tuple[3], time_tuple[4], time_tuple[5])


def get_timestamp(record, start_byte=4):
    """
    Convert the date and time from the record to a Unix timestamp
    :param record: The record read from the file which contains the date and time
    :param start_byte: optional input of starting byte, defaults to 4
    :return: the Unix timestamp
    """
    time_tuple = get_timestamp_tuple(record, start_byte)
    elapsed_seconds = calendar.timegm(time_tuple)

    return float(ntplib.system_to_ntp_time(elapsed_seconds))


def match_checksum(record):
    """
    Calculate the record checksum and compare it to the checksum stored in the record.
    :param record: the read-in record
    :return: boolean, true if checksums match, false if they do not
    """
    # Check that the checksum of this record is good
    stored_checksum = struct.unpack('<H', record[-2:])[0]

    # 46476 is the base value of the checksum given in the IDD as 0xB58C
    calculated_checksum = 46476

    for x in range(0, len(record)-3, 2):
        calculated_checksum += struct.unpack('<H', record[x:x+2])[0]

    # Modulo 65536 is applied to the checksum to keep it a 16 bit value
    calculated_checksum %= 65536

    if calculated_checksum != stored_checksum:
        log.warning('Invalid checksum: %d, expected %d', stored_checksum, calculated_checksum)
        return False

    return True


def rstrip_non_ascii(in_string):
    return ''.join(c for c in in_string if 32 < ord(c) < 127)
