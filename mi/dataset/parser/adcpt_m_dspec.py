#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_m_dspec
@file marine-integrations/mi/dataset/parser/adcpt_m_dspec.py
@author Tapana Gupta
@brief Parser for the adcpt_m_dspec dataset driver

This file contains code for the adcpt_m_dspec parser and code to produce data
particles. This parser is for recovered data only - it produces a single data
particle (adcpt_m_instrument_dspec_recovered)for the data recovered from the
instrument.

DSpec data files (Dspec*.txt) are space-delimited ASCII (with leading spaces).
DSpec files contain leader rows containing English readable text. Subsequent
rows in DSpec contain integer data. A single data particle is extracted from
a DSpec input file.

A recoverable sample exception is thrown if the data format does not match the
corresponding header information. An invalid filename will cause an exception-
no data particles are produced in this case, since the timestamp is extracted
from the filename.


Release notes:

Initial Release
"""

__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'


import calendar
import re

from mi.core.exceptions import \
    SampleException, \
    RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.core.instrument.dataset_data_particle import \
    DataParticle

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum

from mi.dataset.parser.common_regexes import \
    UNSIGNED_INT_REGEX, \
    INT_REGEX, \
    FLOAT_REGEX, \
    END_OF_LINE_REGEX, \
    SPACE_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, \
    ANY_CHARS_REGEX


# Basic patterns

# Regex for zero or more whitespaces
ZERO_OR_MORE_WHITESPACE_REGEX = r'\s*'

# regex for identifying an empty line
EMPTY_LINE_REGEX = END_OF_LINE_REGEX
EMPTY_LINE_MATCHER = re.compile(EMPTY_LINE_REGEX, re.DOTALL)

INT_GROUP_REGEX = r'(' + UNSIGNED_INT_REGEX + ')'
FLOAT_GROUP_REGEX = r'(' + FLOAT_REGEX + ')'
INT_OR_FLOAT_GROUP_REGEX = r'(' + INT_REGEX + '|' + FLOAT_REGEX + ')'

# regex for identifying start of a header line
START_METADATA = ZERO_OR_MORE_WHITESPACE_REGEX + '\%'

# Regex to extract the timestamp from the DSpec log file path (path/to/DSpecYYMMDDHHmm.txt)
FILE_NAME_REGEX = r'.+DSpec([0-9]+)\.txt'
FILE_NAME_MATCHER = re.compile(FILE_NAME_REGEX, re.DOTALL)


# A regex used to match a date in the format YYMMDDHHmm
DATE_TIME_REGEX = r"""
(?P<year>       \d{2})
(?P<month>      \d{2})
(?P<day>        \d{2})
(?P<hour>       \d{2})
(?P<minute>     \d{2})"""

DATE_TIME_MATCHER = re.compile(DATE_TIME_REGEX, re.VERBOSE|re.DOTALL)


# Header data:
HEADER_PATTERN = START_METADATA    # Metadata starts with '%' or ' %'
HEADER_PATTERN += ANY_CHARS_REGEX         # followed by text
HEADER_PATTERN += END_OF_LINE_REGEX         # followed by a newline
HEADER_MATCHER = re.compile(HEADER_PATTERN, re.DOTALL)

# Extract num_dir and num_freq from the following header line
# % <num_dir> Directions and <num_freq> Frequencies
DIR_FREQ_PATTERN = START_METADATA + ONE_OR_MORE_WHITESPACE_REGEX + INT_GROUP_REGEX + \
                   ONE_OR_MORE_WHITESPACE_REGEX + 'Directions and' + \
                   ONE_OR_MORE_WHITESPACE_REGEX + INT_GROUP_REGEX + \
                   ONE_OR_MORE_WHITESPACE_REGEX + 'Frequencies' + END_OF_LINE_REGEX

DIR_FREQ_MATCHER = re.compile(DIR_FREQ_PATTERN, re.DOTALL)


# Extract freq_w_band and freq_0 from the following header line
# % Frequency Bands are <freq_w_band> Hz wide(first frequency band is centered at <freq_0>)
FREQ_BAND_PATTERN = START_METADATA + ONE_OR_MORE_WHITESPACE_REGEX + 'Frequency Bands are' + \
                    ONE_OR_MORE_WHITESPACE_REGEX + FLOAT_GROUP_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                    'Hz wide\(first frequency band is centered at' + ONE_OR_MORE_WHITESPACE_REGEX + \
                    FLOAT_GROUP_REGEX + '\)' + END_OF_LINE_REGEX

FREQ_BAND_MATCHER = re.compile(FREQ_BAND_PATTERN, re.DOTALL)


# Extract start_dir from the following header line
# % The first direction slice begins at <start_dir> degrees
START_DIR_PATTERN = START_METADATA + ONE_OR_MORE_WHITESPACE_REGEX + 'The first direction slice begins at' + \
                    ONE_OR_MORE_WHITESPACE_REGEX + INT_OR_FLOAT_GROUP_REGEX + \
                    ONE_OR_MORE_WHITESPACE_REGEX + 'degrees' + END_OF_LINE_REGEX
START_DIR_MATCHER = re.compile(START_DIR_PATTERN, re.DOTALL)


# List of possible matchers for header data
HEADER_MATCHER_LIST = [DIR_FREQ_MATCHER, FREQ_BAND_MATCHER, START_DIR_MATCHER]


# Regex for identifying a single record of DSpec data
DSPEC_DATA_REGEX = r'((' + SPACE_REGEX + UNSIGNED_INT_REGEX + ')+)' + END_OF_LINE_REGEX
DSPEC_DATA_MATCHER = re.compile(DSPEC_DATA_REGEX, re.DOTALL)

#  Data map used by data particle class to construct the data particle from parsed data
DSPEC_DATA_MAP = [
('file_time', 0, str),
('num_dir', 1, int),
('num_freq', 2, int),
('freq_w_band', 3, float),
('freq_0', 4, float),
('start_dir', 5, float),
('directional_surface_spectrum', 6, list)]

# Position of 'file_time' in DSPEC_DATA_MAP
FILE_TIME_POSITION = 0


class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the adcpt_m Dspec recovered data
    """
    SAMPLE = 'adcpt_m_instrument_dspec_recovered'  # instrument data particle


class AdcptMDspecInstrumentDataParticle(DataParticle):
    """
    Class for generating the adcpt_m_instrument_dspec_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def __init__(self, raw_data, *args, **kwargs):

        super(AdcptMDspecInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # construct the timestamp from the file time
        file_time = self.raw_data[FILE_TIME_POSITION]

        match = DATE_TIME_MATCHER.match(file_time)

        if match:
            timestamp = (
                int(match.group('year')) + 2000,
                int(match.group('month')),
                int(match.group('day')),
                int(match.group('hour')),
                int(match.group('minute')),
                0.0, 0, 0, 0)

            elapsed_seconds = calendar.timegm(timestamp)
            self.set_internal_timestamp(unix_time=elapsed_seconds)
        else:
            # timestamp is essential for a data particle - no timestamp, bail out
            raise SampleException("AdcptMDspecInstrumentDataParticle: Unable to construct "
                                  "internal timestamp from file time: %s" % file_time)

        self.instrument_particle_map = DSPEC_DATA_MAP


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


class AdcptMDspecParser(SimpleParser):


    def parse_file(self):
        """
        Parse the DSpec*.txt file. Build a data particle from the parsed data.
        """
        file_time = ''
        num_dir = 0
        num_freq = 0
        freq_w_band = 0.0
        freq_0 = 0.0
        start_dir = 0.0

        dspec_matrix = []

        # Extract the file time from the file name
        input_file_name = self._stream_handle.name

        match = FILE_NAME_MATCHER.match(input_file_name)

        if match:
            file_time = match.group(1)
        else:
            error_message = 'Unable to extract file time from DSpec input file name: %s '\
                                        % input_file_name
            log.warn(error_message)
            self._exception_callback(RecoverableSampleException(error_message))

        # read the first line in the file
        line = self._stream_handle.readline()

        # loop over all lines in the data file
        while line:

            if EMPTY_LINE_MATCHER.match(line):
                # ignore blank lines, do nothing
                pass

            elif HEADER_MATCHER.match(line):

                # we need header records to extract useful information
                for matcher in HEADER_MATCHER_LIST:
                    header_match = matcher.match(line)

                    if header_match is not None:

                        # Look for specific header lines and extract header fields
                        if matcher is DIR_FREQ_MATCHER:
                            num_dir = int(header_match.group(1))
                            num_freq = int(header_match.group(2))

                        elif matcher is FREQ_BAND_MATCHER:
                            freq_w_band = header_match.group(1)
                            freq_0 = header_match.group(2)

                        elif matcher is START_DIR_MATCHER:
                            start_dir = header_match.group(1)

                        else:
                            #ignore
                            pass

            elif DSPEC_DATA_MATCHER.match(line):

                # Extract a row of the Directional Surface Spectrum matrix
                sensor_match = DSPEC_DATA_MATCHER.match(line)
                data = sensor_match.group(1)
                values = [int(x) for x in data.split()]

                num_values = len(values)

                # If the number of values in a line of data doesn't match num_dir,
                # Drop the record, throw a recoverable exception and continue parsing
                if num_values != num_dir:
                    error_message = 'Unexpected Number of directions in line: expected %s, got %s'\
                                    % (num_dir, num_values)
                    log.warn(error_message)
                    self._exception_callback(RecoverableSampleException(error_message))
                else:
                    # Add the row to the dspec matrix
                    dspec_matrix.append(values)

            else:
                # Generate a warning for unknown data
                error_message = 'Unexpected data found in line %s' % line
                log.warn(error_message)
                self._exception_callback(RecoverableSampleException(error_message))

            # read the next line in the file
            line = self._stream_handle.readline()

        # Check to see if the specified number of frequencies were retrieved from the data
        dspec_matrix_length = len(dspec_matrix)
        if dspec_matrix_length != num_freq:
            error_message = 'Unexpected Number of frequencies in DSpec Matrix: expected %s, got %s'\
                            % (num_freq, dspec_matrix_length)
            log.warn(error_message)
            self._exception_callback(RecoverableSampleException(error_message))

        # Construct the parsed data list to hand over to the Data Particle class for particle creation
        parsed_data = [
            file_time,  # ('file_time', 0, str),
            num_dir,  # ('num_dir', 1, int),
            num_freq,  # ('num_freq', 2, int),
            freq_w_band,  # ('freq_w_band', 3, float),
            freq_0,  # ('freq_0', 4, float),
            start_dir,  # ('start_dir', 5, float),
            dspec_matrix  # ('directional_surface_spectrum', 6, list)]
        ]

        # Extract a particle and append it to the record buffer
        particle = self._extract_sample(AdcptMDspecInstrumentDataParticle, None, parsed_data, None)
        self._record_buffer.append(particle)