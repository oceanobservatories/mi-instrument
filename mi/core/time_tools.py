#!/usr/bin/env python

"""
@package mi.core.time_tools
@file mi/core/time_tools.py
@author Bill French
@brief Common time functions for drivers
"""
from mi.core.log import get_logger ; log = get_logger()

import calendar
import datetime
import ntplib
import time
import re

DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$'
DATE_MATCHER = re.compile(DATE_PATTERN)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

__author__ = 'Bill French'
__license__ = 'Apache 2.0'


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

    result = None
    now = datetime.datetime.utcnow()

    # If we are too close to a second transition then sleep for a bit.
    if now.microsecond < 100000:
        time.sleep(0.2)
        now = datetime.datetime.utcnow()

    current = datetime.datetime.utcnow()
    while current.microsecond > now.microsecond:
        current = datetime.datetime.utcnow()

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


def string_to_ntp_date_time(time_format):
    """
    Extract an ntp date from a ISO8601 formatted date string.
    @param time_format an ISO8601 formatted string containing date information
    @retval an ntp date number (seconds since jan 1 1900)
    @throws InstrumentParameterException if datestr cannot be formatted to
    a date.
    """
    if not isinstance(time_format, basestring):
        raise IOError('Value %s is not a string.' % str(time_format))

    if not DATE_MATCHER.match(time_format):
        raise ValueError("date string not in ISO8601 format YYYY-MM-DDTHH:MM:SS.SSSSZ")

    try:
        # This assumes input date string are in UTC (=GMT)

        # if there is no decimal place, add one to match the date format
        if time_format.find('.') == -1:
            if time_format[-1] != 'Z':
                time_format += '.0Z'
            else:
                time_format = time_format[:-1] + '.0Z'

        # if there is no trailing 'Z' on the input string add one
        if time_format[-1:] != 'Z':
            time_format += 'Z'

        dt = datetime.datetime.strptime(time_format, DATE_FORMAT)

        unix_timestamp = calendar.timegm(dt.timetuple()) + (dt.microsecond / 1000000.0)

        # convert to ntp (seconds since gmt jan 1 1900)
        timestamp = ntplib.system_to_ntp_time(unix_timestamp)
        # log.debug("converted time string '%s', unix_ts: %s ntp: %s", datestr, unix_timestamp, timestamp)

    except ValueError as e:
        raise ValueError('Value %s could not be formatted to a date. %s' % (str(time_format), e))

    return timestamp


def time_to_ntp_date_time(unix_time=None):
        """
        return an NTP timestamp.  Currently this is a float, but should be a 64bit fixed point block.
        TODO: Fix return value
        @param unix_time: Unix time (seconds since 1970/1/1) as returned from time.time()
        """
        if unix_time is None:
            unix_time = time.time()

        timestamp = ntplib.system_to_ntp_time(unix_time)
        return float(timestamp)


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


def ntp_to_string(timestamp, time_format=DATE_FORMAT):
    """
    takes an NTP timestamp (seconds since 1900/1/1) and outputs in provided format
    :param timestamp: ntp timestamp
    :param time_format: datetime compatible time string format
    :return: 
    """
    unix_time = ntplib.ntp_to_system_time(timestamp)
    dt = datetime.datetime.utcfromtimestamp(unix_time)
    return dt.strftime(time_format)
