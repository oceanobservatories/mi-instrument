#!/usr/bin/env python

"""
@package mi.dataset.parser.zplsc_c_dcl
@file /mi/dataset/parser/zplsc_c_dcl.py
@author Ronald Ronquillo & Richard Han
@brief Parser for the zplsc_c_dcl dataset driver

This file contains code for the zplsc_c_dcl parser and code to produce data particles

The ZPLSC sensor, series C, provides acoustic return measurements from the water column.
The data files (*.zplsc.log) are comma-delimited ASCII with a DCL timestamp header.
The file may contain record data for multiple phases and bursts of measurements.
Mal-formed sensor data records produce no particles.

The sensor data record has the following format:
Field   Type    Description
-----   ----    -----------
1       uchar   Header: @Dyyyymmddhhmmss!@P (Contains timestamp)
2       uint    Instrument Serial Number
3       int     Phase
4       int     Burst number (1 - 65535)
5       int     Number of stored frequencies 1 - 4
6       int     N1 = Number of bins for frequency 1
7**     int     N2 = Number of bins for frequency 2
8**     int     N3 = Number of bins for frequency 3
9**     int     N4 = Number of bins for frequency 4
10      uint    Minimum value in the data subtracted out
11**    uint    Minimum value in the data subtracted out
12**    uint    Minimum value in the data subtracted out
13**    uint    Minimum value in the data subtracted out
14      uchar   Date of burst, YYMMDDHHMMSSHH
15      double  Tilt X
16      double  Tilt Y
17      double  Battery voltage
18      double  Temperature
19      double  Pressure (valid value if sensor is available)

20      uchar   Channel 1 board number always 0
21      uint    Channel 1 frequency
N1 val  uint    Channel 1 values minus minimum value

N1+1    uchar   Channel 2 board number always 1
N1+2    uint    Channel 2 frequency
N2 val  uint        Channel 2 values minus minimum value (if available)

N2+1    uchar   Channel 3 board number always 2
N2+2    uint    Channel 3 frequency
N3 val  uint    Channel 3 values minus minimum value (if available)

N3+1    uchar   Channel 4 board number always 3
N3+2    uint    Channel 4 frequency
N4 val  uint    Channel 4 values minus minimum value (if available)

Last    char    Literal !\n

** This field may not be present if fewer than four frequencies are acquired.


Release notes:

Initial Release
"""

__author__ = 'Ronald Ronquillo & Richard Han'
__license__ = 'Apache 2.0'


import ntplib
import re

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
from mi.core.exceptions import RecoverableSampleException
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logging_metaclass
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_ntp_time
from mi.dataset.parser.common_regexes import UNSIGNED_INT_REGEX, END_OF_LINE_REGEX, \
    DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX


# Expected values used for validating data
MAX_NUM_FREQS = 4
# Values below referenced from ZPLSG_Specifications_AZFP-2013_2.pdf
VALID_FREQUENCIES = ('38', '70', '125', '200', '455', '770', None)


class ZplscCParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    TRANS_TIMESTAMP = "zplsc_c_transmission_timestamp"
    SERIAL_NUMBER = "serial_number"
    PHASE = "zplsc_c_phase"
    BURST_NUMBER = "burst_number"
    NUM_FREQ = "zplsc_c_number_of_frequencies"
    NUM_BINS_FREQ_1 = "zplsc_c_number_of_bins_frequency_1"
    NUM_BINS_FREQ_2 = "zplsc_c_number_of_bins_frequency_2"
    NUM_BINS_FREQ_3 = "zplsc_c_number_of_bins_frequency_3"
    NUM_BINS_FREQ_4 = "zplsc_c_number_of_bins_frequency_4"
    MIN_VAL_CHAN_1 = "zplsc_c_min_value_channel_1"
    MIN_VAL_CHAN_2 = "zplsc_c_min_value_channel_2"
    MIN_VAL_CHAN_3 = "zplsc_c_min_value_channel_3"
    MIN_VAL_CHAN_4 = "zplsc_c_min_value_channel_4"
    BURST_DATE = "zplsc_c_date_of_burst"
    TILT_X = "zplsc_c_tilt_x"
    TILT_Y = "zplsc_c_tilt_y"
    BATTERY_VOLTAGE = "zplsc_c_battery_voltage"
    TEMPERATURE = "zplsc_c_temperature"
    PRESSURE = "zplsc_c_pressure"
    BOARD_NUM_CHAN_1 = "zplsc_c_board_number_channel_1"
    FREQ_CHAN_1 = "zplsc_c_frequency_channel_1"
    VALS_CHAN_1 = "zplsc_c_values_channel_1"
    BOARD_NUM_CHAN_2 = "zplsc_c_board_number_channel_2"
    FREQ_CHAN_2 = "zplsc_c_frequency_channel_2"
    VALS_CHAN_2 = "zplsc_c_values_channel_2"
    BOARD_NUM_CHAN_3 = "zplsc_c_board_number_channel_3"
    FREQ_CHAN_3 = "zplsc_c_frequency_channel_3"
    VALS_CHAN_3 = "zplsc_c_values_channel_3"
    BOARD_NUM_CHAN_4 = "zplsc_c_board_number_channel_4"
    FREQ_CHAN_4 = "zplsc_c_frequency_channel_4"
    VALS_CHAN_4 = "zplsc_c_values_channel_4"


# Basic patterns
common_matches = {
    'ANY_NON_BRACKET_CHAR': '[^\[\]]+',
    'DCL_TIMESTAMP': DATE_YYYY_MM_DD_REGEX + '\s' + TIME_HR_MIN_SEC_MSEC_REGEX,
    'END_OF_LINE_REGEX': END_OF_LINE_REGEX,
    'UINT': UNSIGNED_INT_REGEX
}

# DCL Log record:
# Timestamp [Text]MoreText newline
DCL_LOG_MATCHER = re.compile(r"""
    (?P<dcl_timestamp> %(DCL_TIMESTAMP)s)\s\[
    (?P<dcl_tag> .*?)]:
    (?P<dcl_status> .*?)
    %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE)

# Phase Status Record:
# 2015/04/06 00:34:06.498 $5e02300000000001aSTAT- 82857 seconds to Phase 01#07d2
PHASE_STATUS_MATCHER = re.compile(r"""
    (?P<dcl_timestamp> %(DCL_TIMESTAMP)s)\s\$
    (?P<status> .*?)STAT-\s
    (?P<seconds>  %(UINT)s)\sseconds\sto\sPhase\s
    (?P<phase> .*?)
    %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE)

# Sensor Data Record:
# 2015/04/06 23:35:06.057 @D20150406233501!@P,55078,1,1,4,19,19,19,19,31408,34160,25264,20440,15040623350051,21.8,45.7,10.0,7.9,99.0,0,38,28056,6760,9344,4984,3304,5600,1904,0,992,3288,2864,2400,2296,1808,3296,4344,1496,27760,17688,1,125,22696,3472,2840,224,1184,832,1336,2488,872,240,272,600,360,0,736,312,168,22752,15592,2,200,30904,8600,6880,6400,5360,6208,5392,7696,5072,5488,2704,4264,2808,1912,4136,3520,0,26288,20608,3,455,34944,5400,1496,1528,400,664,1768,1496,472,256,88,168,104,152,80,72,0,19648,8832!
SENSOR_DATA_MATCHER = re.compile(r"""
    (?P<dcl_timestamp> %(DCL_TIMESTAMP)s)\s@D
    (?P<transmission_timestamp> %(UINT)s)!@P,
    (?P<condensed_data> %(UINT)s,%(UINT)s,%(UINT)s,(?P<num_of_freqs> %(UINT)s),.*?)!
    %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE)

# The following is used to parse and encode values and is defined as below:
# (parameter name, count (or count reference), encoding function)
ZPLSC_C_DATA_RULES = [
    (ZplscCParticleKey.TRANS_TIMESTAMP,  1,              str),
    (ZplscCParticleKey.SERIAL_NUMBER,    1,              str),
    (ZplscCParticleKey.PHASE,            1,              int),
    (ZplscCParticleKey.BURST_NUMBER,     1,              int),
    (ZplscCParticleKey.NUM_FREQ,         1,              int),
    (ZplscCParticleKey.NUM_BINS_FREQ_1,  1,              int),
    (ZplscCParticleKey.NUM_BINS_FREQ_2,  1,              int),
    (ZplscCParticleKey.NUM_BINS_FREQ_3,  1,              int),
    (ZplscCParticleKey.NUM_BINS_FREQ_4,  1,              int),
    (ZplscCParticleKey.MIN_VAL_CHAN_1,   1,              int),
    (ZplscCParticleKey.MIN_VAL_CHAN_2,   1,              int),
    (ZplscCParticleKey.MIN_VAL_CHAN_3,   1,              int),
    (ZplscCParticleKey.MIN_VAL_CHAN_4,   1,              int),
    (ZplscCParticleKey.BURST_DATE,       1,              str),
    (ZplscCParticleKey.TILT_X,           1,              float),
    (ZplscCParticleKey.TILT_Y,           1,              float),
    (ZplscCParticleKey.BATTERY_VOLTAGE,  1,              float),
    (ZplscCParticleKey.TEMPERATURE,      1,              float),
    (ZplscCParticleKey.PRESSURE,         1,              float),

    (ZplscCParticleKey.BOARD_NUM_CHAN_1, 1,              str),
    (ZplscCParticleKey.FREQ_CHAN_1,      1,              int),
    (ZplscCParticleKey.VALS_CHAN_1,      ZplscCParticleKey.NUM_BINS_FREQ_1, lambda x: map(int, x)),

    (ZplscCParticleKey.BOARD_NUM_CHAN_2, 1,              str),
    (ZplscCParticleKey.FREQ_CHAN_2,      1,              int),
    (ZplscCParticleKey.VALS_CHAN_2,      ZplscCParticleKey.NUM_BINS_FREQ_2, lambda x: map(int, x)),

    (ZplscCParticleKey.BOARD_NUM_CHAN_3, 1,              str),
    (ZplscCParticleKey.FREQ_CHAN_3,      1,              int),
    (ZplscCParticleKey.VALS_CHAN_3,      ZplscCParticleKey.NUM_BINS_FREQ_3, lambda x: map(int, x)),

    (ZplscCParticleKey.BOARD_NUM_CHAN_4, 1,              str),
    (ZplscCParticleKey.FREQ_CHAN_4,      1,              int),
    (ZplscCParticleKey.VALS_CHAN_4,      ZplscCParticleKey.NUM_BINS_FREQ_4, lambda x: map(int, x))
]


class DataParticleType(BaseEnum):
    ZPLSC_C_DCL_SAMPLE = 'zplsc_c_instrument'


class ZplscCInstrumentDataParticle(DataParticle):
    """
    Class for generating the zplsc_c instrument particle.
    """

    _data_particle_type = DataParticleType.ZPLSC_C_DCL_SAMPLE
    __metaclass__ = get_logging_metaclass(log_level='trace')

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name, count(or count reference),
        # and a function to use for data conversion.

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                self._encode_value(name, self.raw_data[name], function)
                for name, counter, function in ZPLSC_C_DATA_RULES]


class ZplscCDclParser(SimpleParser):
    """
    ZPLSC C DCL Parser.
    """

    __metaclass__ = get_logging_metaclass(log_level='trace')

    def parse_file(self):
        """
        Parse the zplsc_c log file (averaged condensed data).
        Read file line by line. Values are extracted from lines containing condensed ASCII data
        @return: dictionary of data values with the particle names as keys or None
        """

        # Loop over all lines in the data file and parse the data to generate particles
        for number, line in enumerate(self._stream_handle, start=1):

            # Check if this is the dcl status log
            match = DCL_LOG_MATCHER.match(line)
            if match is not None:
                log.trace("MATCHED DCL_LOG_MATCHER: %s: %s", number, match.groups())
                # No data to extract, move on to the next line
                continue

            # Check if this is the instrument phase status log
            match = PHASE_STATUS_MATCHER.match(line)
            if match is not None:
                log.trace("MATCHED PHASE_STATUS_MATCHER: %s: %s", number, match.groups())
                # No data to extract, move on to the next line
                continue

            # Check if this is the instrument condensed ASCII data
            match = SENSOR_DATA_MATCHER.match(line)
            if match is not None:
                log.trace("MATCHED SENSOR_DATA_MATCHER: %s: %s", number, match.groups())

                # Extract the condensed ASCII data from this line
                data_dict = self.parse_line(match)
                if data_dict is None:
                    log.error('Erroneous data found in line %s: %s', number, line)
                    continue

                # Convert the DCL timestamp into the particle timestamp
                time_stamp = dcl_controller_timestamp_to_ntp_time(match.group('dcl_timestamp'))

                # Extract a particle and append it to the record buffer
                particle = self._extract_sample(
                    ZplscCInstrumentDataParticle, None, data_dict, time_stamp)
                if particle is not None:
                    log.trace('Parsed particle: %s' % particle.generate_dict())
                    self._record_buffer.append(particle)

                continue

            # Error, line did not match any expected regex
            self._exception_callback(
                RecoverableSampleException('Unknown data found in line %s:%s' % (number, line)))

    @staticmethod
    def parse_line(matches):
        """
        Parse a line from the zplsc_c log file (averaged condensed data).
        If erroneous data is detected return None so the line will be skipped.
        @param matches: MatchObject containing regex matches for ZPLSC_C condensed ASCII data
        @return: dictionary of values with the particle names as keys or None
        """
        data = [matches.group('transmission_timestamp')] + matches.group('condensed_data').split(',')
        num_freqs = matches.group('num_of_freqs')

        # Number of frequencies should be a number from 1 through 4 only
        if not num_freqs.isdigit() or not (1 <= int(num_freqs) <= MAX_NUM_FREQS):
            log.error("Invalid data: Number of frequencies out of range(1-4): %s", num_freqs)
            return None

        data_dict = {}
        index = 0

        # Iterate through the ZPLSC_C data rules to parse out the individual condensed ASCII data
        for key, counter, encoder in ZPLSC_C_DATA_RULES:
            # Skip channels beyond the expected number of frequencies (from 1-4)
            channel = key[-1]
            if channel.isdigit() and (int(channel) > int(num_freqs)):
                data_dict[key] = None
                continue

            # Retrieve the expected length of data for this key
            count = counter
            if type(counter) is str:
                count = data_dict.get(counter)
                if not count.isdigit():
                    log.error("Invalid data: %s value %s %s is not a valid count integer.",
                              counter, type(count), count)
                    return None
                count = int(count)

            # Check if position and length are within array bounds
            if not 0 <= (len(data) - index) >= count:
                log.error("Invalid data: Expected data count(%s) out of bounds for %s"
                          ", length:%s, index: %s",
                          count, key, len(data), index)
                return None

            try:
                if count > 1:
                    data_dict[key] = data[index:index + count]
                elif count == 1:
                    data_dict[key] = data[index]
                else:
                    data_dict[key] = None
            except IndexError, e:
                log.error("IndexError %s: %s: %s >= data length %s, index: %s",
                          e, key, count, (len(data)-index), index)
                return None

            index += count

        # Check for valid board numbers per channel (0-3)
        for value, name in enumerate((ZplscCParticleKey.BOARD_NUM_CHAN_1, ZplscCParticleKey.BOARD_NUM_CHAN_2,
                                      ZplscCParticleKey.BOARD_NUM_CHAN_3, ZplscCParticleKey.BOARD_NUM_CHAN_4)):
            if data_dict[name] not in (str(value), None):
                log.error("Invalid data: %s should always be \'%s\' or None: %s %s",
                          name, value, data_dict[name], type(data_dict[name]))
                return None

        # Check for valid frequency value per channel
        for name in (ZplscCParticleKey.FREQ_CHAN_1, ZplscCParticleKey.FREQ_CHAN_2,
                     ZplscCParticleKey.FREQ_CHAN_3, ZplscCParticleKey.FREQ_CHAN_4):
            if data_dict[name] not in VALID_FREQUENCIES:
                log.error("Invalid data: %s: %s (Valid values %s)",
                          name, data_dict[name], VALID_FREQUENCIES)
                return None

        return data_dict