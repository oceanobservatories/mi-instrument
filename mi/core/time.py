"""
@package mi.core.time
@file mi/core/time.py
@author Bill French
@brief Common time functions for drivers
"""

# Needed because we import the time module below, and our name is time.
# Without this '.' is searched first and we import ourselves.
from __future__ import absolute_import

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import calendar
from datetime import datetime
import ntplib
import time
import re

from mi.core.log import get_logger
log = get_logger()

DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$'
DATE_MATCHER = re.compile(DATE_PATTERN)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def string_to_ntp_date_time(datestr):
    """
    Extract an ntp date from a ISO8601 formatted date string.
    @param datestr an ISO8601 formatted string containing date information
    @retval an ntp date number (seconds since jan 1 1900)
    @throws InstrumentParameterException if datestr cannot be formatted to
    a date.
    """
    if not isinstance(datestr, str):
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

        unix_timestamp = calendar.timegm(dt.timetuple()) + (dt.microsecond / 1000000.0)

        # convert to ntp (seconds since gmt jan 1 1900)
        timestamp = ntplib.system_to_ntp_time(unix_timestamp)
        log.debug("converted time string '%s', unix_ts: %s ntp: %s", datestr, unix_timestamp, timestamp)

    except ValueError as e:
        raise ValueError('Value %s could not be formatted to a date. %s' % (str(datestr), e))

    return timestamp


def time_to_ntp_date_time(unix_time=None):
    """
    return an NTP timestamp.  Currently this is a float, but should be a 64bit fixed point block.
    TODO: Fix return value
    @param unit_time: Unix time as returned from time.time()
    """
    if unix_time is None:
        unix_time = time.time()

    timestamp = ntplib.system_to_ntp_time(unix_time)
    return float(timestamp)
