"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/utilities.py
@author Joe Padula, vipul lakhani
@brief Utilities that can be used by any parser
Release notes:

initial release
"""
from datetime import datetime
import time
import ntplib
import calendar


from mi.core.log import get_logger

__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

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


def julian_time_to_ntp(julian_timestamp_str):
    """
    Converts a julian formatted timestamp timestamp string to NTP time.
    :param julian_timestamp_str: a julian formatted timestamp string (julian_timestamp_str = 200412)
    :return: NTP time in seconds
    """

    timestamp = datetime.strptime(julian_timestamp_str, "%Y%j")

    return (timestamp - datetime(1900, 1, 1)).total_seconds()


def time_1904_to_ntp(time_1904):
    """
    :param time_1904: time in 1904 ( example time_1904 = 3601587612.0)
    :return: ntp (timestamp in number of seconds since Jan 1, 1900)
    """
    return time_1904 + (datetime(1904, 1, 1) - datetime(1900, 1, 1)).total_seconds()


def time_2000_to_ntp(time_2000):
    """
    :param time_2000: a timestamp in epoch 2000
    :return: timestamp in epoch 1900
    This function calculates and returns a timestamp in epoch 1900
    based on an integer timestamp in epoch 2000.
    Parameter:
      time_2000 - timestamp in number of seconds since Jan 1, 2000
    Returns:
      timestamp in number of seconds since Jan 1, 1900
    """
    return time_2000 + zulu_timestamp_to_ntp_time("2000-01-01T00:00:00.00Z")


def dcl_time_to_utc(dcl_controller_timestamp_str):
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


def dcl_time_to_ntp(dcl_controller_timestamp_str):
    """
    Converts a DCL controller timestamp string to NTP time.
    :param dcl_controller_timestamp_str: a DCL controller timestamp string
    :return: NTP time (float64) in seconds and microseconds precision
    """

    utc_time = dcl_time_to_utc(dcl_controller_timestamp_str)

    return float(ntplib.system_to_ntp_time(utc_time))


def timestamp_yyyymmddhhmmss_to_ntp(timestamp_str):
    """
    Converts a timestamp string, in the YYYYMMDDHHMMSS format, to NTP time.
    :param timestamp_str: a timestamp string in the format YYYYMMDDHHMMSS
    :return: Time (float64) in seconds from epoch 01-01-1900.
    """

    timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

    return (timestamp - datetime(1900, 1, 1)).total_seconds()


def timestamp_yyyy_mm_dd_hh_mm_ss_to_ntp(timestamp_str):
    """
    Converts a timestamp string, in the YYYY/MM/DD HH:MM:SS format, to NTP time.
    :param timestamp_str: a timestamp string in the format YYYY/MM/DD HH:MM:SS
    :return: Time (float64) in seconds from epoch 01-01-1900.
    """

    timestamp = datetime.strptime(timestamp_str, "%Y/%m/%d %H:%M:%S")

    return (timestamp - datetime(1900, 1, 1)).total_seconds()


def timestamp_yyyy_mm_dd_hh_mm_ss_csv_to_ntp(timestamp_str):
    """
    Converts a timestamp string, in the YYYY,MM,DD,HH,MM,SS format, to NTP time.
    :param timestamp_str: a timestamp string in the format YYYY,MM,DD,HH,MM,SS
    :return: Time (float64) in seconds from epoch 01-01-1900.
    """

    timestamp = datetime.strptime(timestamp_str, "%Y,%m,%d,%H,%M,%S")

    return (timestamp - datetime(1900, 1, 1)).total_seconds()


def timestamp_ddmmyyyyhhmmss_to_ntp(timestamp_str):
    """
    Converts a timestamp string, in the DD Mon YYYY HH:MM:SS format, to NTP time.
    :param timestamp_str: a timestamp string in the format DD Mon YYYY HH:MM:SS
    :return: Time (float64) in seconds from epoch 01-01-1900.
    """
    timestamp = datetime.strptime(timestamp_str, "%d %b %Y %H:%M:%S")
    return (timestamp - datetime(1900, 1, 1)).total_seconds()


def timestamp_mmddyyhhmmss_to_ntp(timestamp_str):
    """
    Converts a timestamp string, in the MMDDYYHHMMSS format, to NTP time.
    :param timestamp_str: a timestamp string in the format MM/DD/YY HH:MM:SS
    :return: Time (float64) in seconds from epoch 01-01-1900.
    """

    timestamp = datetime.strptime(timestamp_str, "%m/%d/%y %H:%M:%S")

    return (timestamp - datetime(1900, 1, 1)).total_seconds()


def timestamp_ddmmyyhhmmss_to_ntp(timestamp_str):
    """
    Converts a timestamp string, in the DDMMYYHHMMSS format, to NTP time.
    :param timestamp_str: a timestamp string in the format DD/MM/YY HH:MM:SS
    :return: Time (float64) in seconds from epoch 01-01-1900.
    """

    timestamp = datetime.strptime(timestamp_str, "%d/%m/%y %H:%M:%S")

    return (timestamp - datetime(1900, 1, 1)).total_seconds()

def mac_timestamp_to_utc_timestamp(mac_timestamp):
    """
    :param mac_timestamp: A mac based timestamp
    :return: The mac timestamp converted to unix time
    """

    unix_minus_mac_secs = (datetime(1970, 1, 1) - datetime(1904, 1, 1)).total_seconds()

    secs_since_1970 = mac_timestamp - unix_minus_mac_secs

    return secs_since_1970


def convert_to_signed_int_32_bit(hex_str):
    """
    Utility function to convert a hex string into a 32 bit signed hex integer value
    :param hex_str: hex String
    :return: signed 32 bit integer
    """
    val = int(hex_str, 16)
    if val > 0x7FFFFFFF:
        val = ((val+0x80000000) & 0xFFFFFFFF) - 0x80000000
    return val


def convert_to_signed_int_16_bit(hex_str):
    """
    Utility function to convert a hex string into a 16 bit signed hex integer value
    :param hex_str: hex String
    :return: signed 16 bit integer
    """
    val = int(hex_str, 16)
    if val > 0x7FFF:
        val = ((val+0x8000) & 0xFFFF) - 0x8000
    return val


def convert_to_signed_int_8_bit(hex_str):
    """
    Utility function to convert a hex string into a 8 bit signed hex integer value
    :param hex_str: hex String
    :return: signed 8 bit integer
    """
    val = int(hex_str, 16)
    if val > 0x7F:
        val = ((val+0x80) & 0xFF) - 0x80
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


def particle_to_yml(particles, filename, mode='w+'):
    """
    This function write particles to .yml file and create .yml file for testing
    """
    # open write append, if you want to start from scratch manually delete this file
    with open(filename, mode) as fid:
        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')
        for index in range(len(particles)):
            particle_dict = particles[index].generate_dict()
            fid.write('  - _index: %d\n' % (index+1))
            fid.write('    particle_object: %s\n' % particles[index].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            if particle_dict.get('internal_timestamp') is not None:
                fid.write('    internal_timestamp: %.7f\n' % particle_dict.get('internal_timestamp'))

            if particle_dict.get('port_timestamp') is not None:
                fid.write('    port_timestamp: %.7f\n' % particle_dict.get('port_timestamp'))

            values_dict = {}
            for value in particle_dict.get('values'):
                values_dict[value.get('value_id')] = value.get('value')

            for key in sorted(values_dict.iterkeys()):
                value = values_dict[key]
                if value is None:
                    fid.write('    %s: %s\n' % (key, 'Null'))
                elif isinstance(value, float):
                    fid.write('    %s: %15.5f\n' % (key, value))
                elif isinstance(value, str):
                    fid.write("    %s: '%s'\n" % (key, value))
                else:
                    fid.write('    %s: %s\n' % (key, value))
