"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/utilities.py
@author Joe Padula
@brief Utilities that can be used by any parser
Release notes:

initial release
"""

__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

from datetime import datetime
import time
import ntplib
import calendar
import string

from mi.core.log import get_logger
log = get_logger()

# Format of DCL Controller Timestamp in records
# Example: 2014/08/17 00:57:10.648
ZULU_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# Format of DCL Controller Timestamp in records
# Example: 2014/08/17 00:57:10.648
DCL_CONTROLLER_TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S.%f"


def formatted_timestamp_utc_time(timestamp_str, format_str):
    """
    Converts a formatted timestamp timestamp string to UTC time
    NOTE: will not handle seconds >59 correctly due to limitation in
    datetime module.
    :param timestamp_str: a formatted timestamp string
    :param format_str: format string used to decode the timestamp_str
    :return: utc time value
    """

    dt = datetime.strptime(timestamp_str, format_str)

    return calendar.timegm(dt.timetuple()) + (dt.microsecond / 1000000.0)


def zulu_timestamp_to_utc_time(zulu_timestamp_str):
    """
    Converts a zulu formatted timestamp timestamp string to UTC time.
    :param zulu_timestamp_str: a zulu formatted timestamp string
    :return: UTC time in seconds and microseconds precision
    """

    return formatted_timestamp_utc_time(zulu_timestamp_str,
                                        ZULU_TIMESTAMP_FORMAT)


def zulu_timestamp_to_ntp_time(zulu_timestamp_str):
    """
    Converts a zulu formatted timestamp timestamp string to NTP time.
    :param zulu_timestamp_str: a zulu formatted timestamp string
    :return: NTP time in seconds and microseconds precision
    """

    utc_time = zulu_timestamp_to_utc_time(zulu_timestamp_str)

    return float(ntplib.system_to_ntp_time(utc_time))


def time_2000_to_ntp_time(time_2000):
    """
    This function calculates and returns a timestamp in epoch 1900
    based on an integer timestamp in epoch 2000.
    Parameter:
      time_2000 - timestamp in number of seconds since Jan 1, 2000
    Returns:
      timestamp in number of seconds since Jan 1, 1900
    """
    return time_2000 + zulu_timestamp_to_ntp_time("2000-01-01T00:00:00.00Z")


def dcl_controller_timestamp_to_utc_time(dcl_controller_timestamp_str):
    """
    Converts a DCL controller timestamp string to UTC time.
    :param dcl_controller_timestamp_str: a DCL controller timestamp string
    :return: UTC time in seconds and microseconds precision
    """

    no_frac_timestamp_str, frac_timestamp_str = dcl_controller_timestamp_str.split('.')
    no_frac_format_str, frac_format_str = DCL_CONTROLLER_TIMESTAMP_FORMAT.split('.')

    tt = time.strptime(no_frac_timestamp_str, no_frac_format_str)

    frac_of_sec = float('.' + frac_timestamp_str)

    return calendar.timegm(tt) + frac_of_sec


def dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp_str):
    """
    Converts a DCL controller timestamp string to NTP time.
    :param dcl_controller_timestamp_str: a DCL controller timestamp string
    :return: NTP time (float64) in seconds and microseconds precision
    """

    utc_time = dcl_controller_timestamp_to_utc_time(dcl_controller_timestamp_str)

    return float(ntplib.system_to_ntp_time(utc_time))


def mac_timestamp_to_utc_timestamp(mac_timestamp):
    """
    :param mac_timestamp: A mac based timestamp
    :return: The mac timestamp converted to unix time
    """

    unix_minus_mac_secs = (datetime(1970, 1, 1) - datetime(1904, 1, 1)).total_seconds()

    secs_since_1970 = mac_timestamp - unix_minus_mac_secs

    return secs_since_1970


def convert_to_signed_int_32_bit(input):
    """
    Utility function to convert a hex string into a 32 bit signed hex integer value
    :param input: hex String
    :return: signed 32 bit integer
    """
    val = int(input, 16)
    if val > 0x7FFFFFFF:
        val = ((val+0x80000000)&0xFFFFFFFF) - 0x80000000
    return val


def convert_to_signed_int_16_bit(input):
    """
    Utility function to convert a hex string into a 16 bit signed hex integer value
    :param input: hex String
    :return: signed 16 bit integer
    """
    val = int(input, 16)
    if val > 0x7FFF:
        val = ((val+0x8000)&0xFFFF) - 0x8000
    return val


def convert_to_signed_int_8_bit(input):
    """
    Utility function to convert a hex string into a 8 bit signed hex integer value
    :param input: hex String
    :return: signed 8 bit integer
    """
    val = int(input, 16)
    if val > 0x7F:
        val = ((val+0x80)&0xFF) - 0x80
    return val


def sum_hex_digits(ascii_hex_str):
    """
    This method will take an ascii hex string and sum each of the bytes
    returning the result as hex.
    :param ascii_hex_str: The ascii hex string to sum
    :return:
    """

    len_of_ascii_hex = len(ascii_hex_str)

    if len_of_ascii_hex % 2 != 0:
        raise ValueError("The ASCII Hex string is not divisible by 2.")

    x = 0

    # Iterate through each byte of ascii hex
    for index in range(0, len_of_ascii_hex, 2):
        # Convert each byte to an int and add it to the existing summation
        x += int(ascii_hex_str[index:index+2], 16)

    # Return the resultant summation as hex
    return hex(x)