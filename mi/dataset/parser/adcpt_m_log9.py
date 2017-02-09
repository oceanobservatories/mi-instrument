#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_m_log9
@file marine-integrations/mi/dataset/parser/adcpt_m_log9.py
@author Richard Han
@brief Parser for the adcpt_m_log9 dataset driver

This file contains code for the adcpt_m_log9 parser and code to produce data
particles. This parser is for recovered data only - it produces a single data
particle (adcpt_m_instrument_dspec_recovered)for the data recovered from the
instrument.

LOG9 is in TRDI Waves Parameters Log - Format 9 described in detail in
WavesMon Users Guide Table 10 page 50. Data records are preceded by spaces
and end in a line break. The waves parameters log is comma-delimited ASCII
(with leading spaces). Bursts are preceded by spaces and the burst number.
Data in the bursts are a mix of integer and float data.


Release notes:

Initial Release
"""

__author__ = 'Richard Han'
__license__ = 'Apache 2.0'


import calendar
import re

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.core.instrument.dataset_data_particle import DataParticle

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum

from mi.dataset.parser.common_regexes import \
    INT_REGEX, \
    END_OF_LINE_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX


# Basic patterns

FLOAT_REGEX =  r'[+-]?[0-9]+\.[0-9]+'
FLOAT_OR_INT_GROUP_REGEX = r'(' + FLOAT_REGEX + '|' + INT_REGEX + ')'

# regex for identifying an empty line
EMPTY_LINE_REGEX = ONE_OR_MORE_WHITESPACE_REGEX + END_OF_LINE_REGEX
EMPTY_LINE_MATCHER = re.compile(EMPTY_LINE_REGEX, re.DOTALL)

ZERO_OR_MORE_WHITESPACE_REGEX = r'\s*'


# Regex for identifying a single record of Log9 data
LOG9_DATA_PATTERN = r'(' + ONE_OR_MORE_WHITESPACE_REGEX + '(' + FLOAT_OR_INT_GROUP_REGEX + ',?' \
                    + ZERO_OR_MORE_WHITESPACE_REGEX + ')+' + END_OF_LINE_REGEX + ')'
LOG9_DATA_MATCHER = re.compile(LOG9_DATA_PATTERN, re.DOTALL)

#  Data map used by data particle class to construct the data particle from parsed data
LOG9_DATA_MAP = [
    ('burst_number', 0, int),
    ('burst_start_time', 1, list),
    ('significant_wave_height', 2, float),
    ('peak_wave_period', 3, float),
    ('peak_wave_direction', 4, float),
    ('tp_sea', 5, float),
    ('dp_sea', 6, float),
    ('hs_sea', 7, float),
    ('tp_swell', 8, float),
    ('dp_swell', 9, float),
    ('hs_swell', 10, float),
    ('depth_water_level', 11, float),
    ('h_max', 12, float),
    ('t_max', 13, float),
    ('h_1_3', 14, float),
    ('t_1_3', 15, float),
    ('h_mean', 16, float),
    ('t_mean', 17, float),
    ('h_1_10', 18, float),
    ('t_1_10', 19, float),
    ('d_mean', 20, float),
    ('num_bins', 21, int),
    ('depth_level_magnitude', 22, list),
    ('depth_level_direction', 23, list),]

BURST_START_TIME_IDX = 1

class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the adcpt_m Log 9 recovered data
    """
    # adcpt_m log 9 instrument data particle
    SAMPLE = 'adcpt_m_instrument_log9_recovered'


class AdcptMLog9InstrumentDataParticle(DataParticle):
    """
    Class for generating the adcpt_m_instrument_log9_recovered data particle.
    """

    BURST_YEAR_IDX = 0
    BURST_MONTH_IDX = 1
    BURST_DAY_IDX = 2
    BURST_HOUR_IDX = 3
    BURST_MINUTE_IDX = 4
    BURST_SECOND_IDX = 5
    BURST_CENTI_SECOND_IDX = 6

    _data_particle_type = DataParticleType.SAMPLE

    def __init__(self, raw_data, *args, **kwargs):

        super(AdcptMLog9InstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # set timestamp
        burst_time = self.raw_data[BURST_START_TIME_IDX]
        timestamp = (
            int(burst_time[self.BURST_YEAR_IDX]) + 2000,
            int(burst_time[self.BURST_MONTH_IDX]),
            int(burst_time[self.BURST_DAY_IDX]),
            int(burst_time[self.BURST_HOUR_IDX]),
            int(burst_time[self.BURST_MINUTE_IDX]),
            float(str(burst_time[self.BURST_SECOND_IDX]) + '.' + str(burst_time[self.BURST_CENTI_SECOND_IDX])),
            0, 0, 0)

        elapsed_seconds = calendar.timegm(timestamp)
        self.set_internal_timestamp(unix_time=elapsed_seconds)

        self.instrument_particle_map = LOG9_DATA_MAP


    def _build_parsed_values(self):
        """
        Build parsed values for the Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[group], function)
                for name, group, function in self.instrument_particle_map]


class AdcptMLog9Parser(SimpleParser):
    """
    Parser for adcpt_m Log9*.txt files.
    """

    BURST_NUM_IDX = 0
    BURST_YEAR_IDX = 1
    BURST_MONTH_IDX = 2
    BURST_DAY_IDX = 3
    BURST_HOUR_IDX = 4
    BURST_MINUTE_IDX = 5
    BURST_SECOND_IDX = 6
    BURST_CENTI_SECOND_IDX = 7
    SIGNIFICANT_WAVE_IDX = 8
    PEAK_WAVE_PERIOD_IDX = 9
    PEAK_WAVE_DIRECTION_IDX = 10
    TP_SEA_IDX = 11
    DP_SEA_IDX = 12
    HS_SEA_IDX = 13
    TP_SWELL_IDX = 14
    DP_SWELL_IDX = 15
    HS_SWELL_IDX = 16
    DEPTH_WATER_LEVEL_IDX = 17
    H_MAX_IDX = 18
    T_MAX_IDX = 19
    H_1_3_IDX = 20
    T_1_3_IDX = 21
    H_MEAN_IDX = 22
    T_MEAN_IDX = 23
    H_1_10_IDX = 24
    T_1_10_IDX = 25
    D_MEAN_IDX = 26
    NUM_BINS_IDX = 27
    DEPTH_LEVEL_MAGNITUDE_IDX = 28
    DEPTH_LEVEL_DIRECTION_IDX = 29


    def parse_file(self):
        """
        Parse a Log9 input file. Build a data particle from the parsed data.
        """

        # read the first line in the file
        line = self._stream_handle.readline()

        while line:

            if EMPTY_LINE_MATCHER.match(line):
                # ignore blank lines, do nothing
                pass

            elif LOG9_DATA_MATCHER.match(line):

                depth_level_magnitude = []
                depth_level_direction = []

                # Extract a line of the adcpt_m log 9 data
                sensor_match = LOG9_DATA_MATCHER.match(line)
                data = sensor_match.group(1)
                values = data.split(',')

                try:
                    # Extract individual fields from the data
                    burst_number = int(values[self.BURST_NUM_IDX])
                    year = int(values[self.BURST_YEAR_IDX])
                    month = int(values[self.BURST_MONTH_IDX])
                    day = int(values[self.BURST_DAY_IDX])
                    hour = int(values[self.BURST_HOUR_IDX])
                    minute = int(values[self.BURST_MINUTE_IDX])
                    second = int(values[self.BURST_SECOND_IDX])
                    centi_seconds = int(values[self.BURST_CENTI_SECOND_IDX])

                    burst_start_time = [year, month, day, hour, minute, second, centi_seconds]

                    significant_wave_height  = float(values[self.SIGNIFICANT_WAVE_IDX])
                    peak_wave_period = float(values[self.PEAK_WAVE_PERIOD_IDX])
                    peak_wave_direction = float(values[self.PEAK_WAVE_DIRECTION_IDX])
                    tp_sea = float(values[self.TP_SEA_IDX])
                    dp_sea = float(values[self.DP_SEA_IDX])
                    hs_sea = float(values[self.HS_SEA_IDX])
                    tp_swell = float(values[self.TP_SWELL_IDX])
                    dp_swell = float(values[self.DP_SWELL_IDX])
                    hs_swell = float(values[self.HS_SWELL_IDX])
                    depth_water_level = float(values[self.DEPTH_WATER_LEVEL_IDX])
                    h_max = float(values[self.H_MAX_IDX])
                    t_max = float(values[self.T_MAX_IDX])
                    h_1_3 = float(values[self.H_1_3_IDX])
                    t_1_3 = float(values[self.T_1_3_IDX])
                    h_mean = float(values[self.H_MEAN_IDX])
                    t_mean = float(values[self.T_MEAN_IDX])
                    h_1_10 = float(values[self.H_1_10_IDX])
                    t_1_10 = float(values[self.T_1_10_IDX])
                    d_mean = float(values[self.D_MEAN_IDX])
                    num_bins = int(values[self.NUM_BINS_IDX])

                    # build the depth level magnitude and direction lists
                    for i in range(0, num_bins):
                        depth_level_magnitude.append(float(values[self.DEPTH_LEVEL_MAGNITUDE_IDX + (i*2)]))
                        depth_level_direction.append(int(values[self.DEPTH_LEVEL_DIRECTION_IDX + (i*2)]))

                    # Construct the parsed data list to hand over to the Data Particle class for particle creation
                    parsed_data = [
                        burst_number,
                        burst_start_time,
                        significant_wave_height,
                        peak_wave_period,
                        peak_wave_direction,
                        tp_sea,
                        dp_sea,
                        hs_sea,
                        tp_swell,
                        dp_swell,
                        hs_swell,
                        depth_water_level,
                        h_max,
                        t_max,
                        h_1_3,
                        t_1_3,
                        h_mean,
                        t_mean,
                        h_1_10,
                        t_1_10,
                        d_mean,
                        num_bins,
                        depth_level_magnitude,
                        depth_level_direction
                    ]

                    # Check for inconsistent data and drop bad records
                    if len(depth_level_magnitude) != num_bins:
                        error_message = 'Unexpected Number of Magnitude elements encountered: expected %s, got %s'\
                                        % (num_bins, len(depth_level_magnitude))
                        log.warn(error_message)
                        self._exception_callback(RecoverableSampleException(error_message))

                    elif len(depth_level_direction) != num_bins:
                        error_message = 'Unexpected Number of Direction elements encountered: expected %s, got %s'\
                                        % (num_bins, len(depth_level_direction))
                        log.warn(error_message)
                        self._exception_callback(RecoverableSampleException(error_message))

                    # The length of the data is inconsistent with num_bins
                    elif len(values) != (self.NUM_BINS_IDX + num_bins*2 + 1):
                        error_message = 'Unexpected number of elements encountered in data record, skipping.'
                        log.warn(error_message)
                        self._exception_callback(RecoverableSampleException(error_message))

                    else:
                        # Extract a particle and append it to the record buffer
                        particle = self._extract_sample(AdcptMLog9InstrumentDataParticle, None, parsed_data, None)
                        self._record_buffer.append(particle)

                # Throw an exception if there was an error decoding the data
                except (ValueError, TypeError, IndexError) as ex:
                    error_msg = 'Error extracting values from data: %s' % ex
                    log.warn(error_msg)
                    self._exception_callback(RecoverableSampleException(error_msg))

            else:
                # Generate a warning for unknown data
                error_message = 'Unexpected adcpt_m LOG9 data found in line %s' % line
                log.warn(error_message)
                self._exception_callback(RecoverableSampleException(error_message))

            # read the next line in the file
            line = self._stream_handle.readline()
