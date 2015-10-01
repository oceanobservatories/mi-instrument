#!/usr/bin/env python

"""
@package mi.core.time_tools
@file mi/core/time_tools.py
@author Bill French
@brief Common time functions for drivers
"""
__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from mi.core.log import get_logger
log = get_logger()

import calendar
from datetime import datetime
import time
import re

DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$'
DATE_MATCHER = re.compile(DATE_PATTERN)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
NTP_DIFF = (datetime(1970, 1, 1) - datetime(1900, 1, 1)).total_seconds()
Y2K = (datetime(2000, 1, 1) - datetime(1900, 1, 1)).total_seconds()


def get_timestamp_delayed(time_format):
    """
    Return a formatted date string of the current utc time,
    but the string return is delayed until the next second
    transition.

    Formatting:
    http://docs.python.org/library/time.html#time.strftime

    @param time_format: strftime() format string
    @return: formatted date string
    @raise ValueError if format is None
    """
    if not time_format:
        raise ValueError

    now = datetime.utcnow()

    # If we are too close to a second transition then sleep for a bit.
    if now.microsecond < 100000:
        time.sleep(0.2)
        now = datetime.utcnow()

    current = datetime.utcnow()
    while current.microsecond > now.microsecond:
        current = datetime.utcnow()

    return time.strftime(time_format, time.gmtime())


def get_timestamp(time_format):
    """
    Return a formatted date string of the current utc time.

    Formatting:
    http://docs.python.org/library/time.html#time.strftime

    @param time_format: strftime() format string
    @return: formatted date string
    @raise ValueError if format is None
    """
    if not time_format:
        raise ValueError

    return time.strftime(time_format, time.gmtime())


def string_to_ntp_date_time(datestr):
    """
    Extract an ntp date from a ISO8601 formatted date string.
    @param datestr an ISO8601 formatted string containing date information
    @retval an ntp date number (seconds since jan 1 1900)
    @throws InstrumentParameterException if datestr cannot be formatted to
    a date.
    """
    if not isinstance(datestr, basestring):
        raise IOError('Value %s is not a string.' % str(datestr))

    if not DATE_MATCHER.match(datestr):
        raise ValueError("date string not in ISO8601 format YYYY-MM-DDTHH:MM:SS.SSSSZ")

    try:
        # This assumes input date string are in UTC (=GMT)

        # if there is no decimal place, add one to match the date format
        if datestr.find('.') == -1:
            if datestr[-1] != 'Z':
                datestr += '.0Z'
            else:
                datestr = datestr[:-1] + '.0Z'

        # if there is no trailing 'Z' on the input string add one
        if datestr[-1:] != 'Z':
            datestr += 'Z'

        dt = datetime.strptime(datestr, DATE_FORMAT)
        timestamp = (dt - datetime(1900, 1, 1)).total_seconds()

    except ValueError as e:
        raise ValueError('Value %s could not be formatted to a date. %s' % (str(datestr), e))

    return timestamp


def time_to_ntp_date_time(unix_time=None):
        """
        return an NTP timestamp.  Currently this is a float, but should be a 64bit fixed point block.
        TODO: Fix return value
        @param unix_time: Unix time as returned from time.time()
        """
        if unix_time is None:
            unix_time = time.time()

        return unix_time + NTP_DIFF


def timegm_to_float(timestamp):
    """
    takes the timestamp, applies time.mktime to capture the fraction.
        then, gets the time "converted" from gmt using calendar.timegm(timegm assumes gmt for param)
        then adds the fraction back on and returns the float value.
    :param timestamp: time in gmt
    :return: float epoch time (gmt)
    """
    gmfloat = float(calendar.timegm(timestamp))
    return gmfloat


def system_to_ntp_time(unix_timestamp):
    return unix_timestamp + NTP_DIFF


def ntp_to_system_time(ntp_timestamp):
    return ntp_timestamp - NTP_DIFF
