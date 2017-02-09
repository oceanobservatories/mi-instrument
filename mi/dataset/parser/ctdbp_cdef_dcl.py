#!/usr/bin/env python

"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/ctdbp_cdef_dcl.py
@author Jeff Roy
@brief Parser for the ctdbp_cdef_dcl dataset driver

This file contains code for the ctdbp_cdef_dcl parser and code to produce data particles.

The input file is ASCII.
The record types are separated by a newline.
All lines start with a timestamp.
Metadata records: timestamp [text] more text newline.
Instrument records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

There are two file formats supported, the current config, which is listed as incorrect,
and the subsequent files after the inst config is corrected, labelled here as UNCORR
and CORR.

Release notes:
This is a merger of the previous versions in files
ctdbp_cdef_dcl_ce.py
ctdbp_cdef_dcl_cp.py

Initial Release
"""

import re

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.core.exceptions import \
    RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.utilities import dcl_controller_timestamp_to_utc_time

from mi.dataset.parser.common_regexes import \
    ANY_CHARS_REGEX, \
    FLOAT_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, \
    TIME_HR_MIN_SEC_MSEC_REGEX, \
    DATE_YYYY_MM_DD_REGEX, \
    END_OF_LINE_REGEX

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'
log = get_logger()


# Basic patterns
COMMA = ','                          # simple comma
COLON = ':'                          # simple colon
HASH = '#'                           # hash symbol
START_GROUP = '('                    # match group start
END_GROUP = ')'                      # match group end
ZERO_OR_MORE_WHITESPACE_REGEX = r'\s*'

# DateTimeStr:   DD Mon YYYY HH:MM:SS
DATE_TIME_STR = r'(?P<date_time_string>\d{2} \D{3} \d{4} \d{2}:\d{2}:\d{2})'  # named group = date_time_string

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
TIMESTAMP = START_GROUP + '?P<dcl_controller_timestamp>'                      # named group = dcl_controller_timestamp
TIMESTAMP += DATE_YYYY_MM_DD_REGEX + ONE_OR_MORE_WHITESPACE_REGEX
TIMESTAMP += TIME_HR_MIN_SEC_MSEC_REGEX
TIMESTAMP += END_GROUP

TEMP_REGEX = START_GROUP + '?P<temp>' + FLOAT_REGEX + END_GROUP                  # named group = temp
CONDUCTIVITY_REGEX = START_GROUP + '?P<conductivity>' + FLOAT_REGEX + END_GROUP  # named group = conductivity
PRESSURE_REGEX = START_GROUP + '?P<pressure>' + FLOAT_REGEX + END_GROUP          # named group = pressure

START_METADATA = r'\['                                                   # metadata delimited by []'s
END_METADATA = r'\]'

# LOGGER IDENTIFICATION, such as [ctdbp1:DLOGP3]:
# NOTE: Fall 2016 deployments to Coastal Endurance configured without Logger identification output
LOGGER_ID_PATTERN = START_METADATA                           # Metadata record starts with '['
LOGGER_ID_PATTERN += ANY_CHARS_REGEX                                 # followed by text
LOGGER_ID_PATTERN += END_METADATA                                    # followed by ']'
LOGGER_ID_PATTERN += COLON                                           # an immediate colon
LOGGER_ID_PATTERN += ZERO_OR_MORE_WHITESPACE_REGEX                   # and maybe whitespace after

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX          # dcl controller timestamp
METADATA_PATTERN += LOGGER_ID_PATTERN                                # Metadata record starts with '['
METADATA_PATTERN += '\D+' + ANY_CHARS_REGEX                          # followed by more text
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# match a single line uncorrected instrument record
UNCORR_REGEX = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + HASH              # dcl timestamp, named group = timestamp
UNCORR_REGEX += ONE_OR_MORE_WHITESPACE_REGEX
UNCORR_REGEX += TEMP_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX           # named group = temp
UNCORR_REGEX += CONDUCTIVITY_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX   # named group = conductivity
UNCORR_REGEX += PRESSURE_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX       # named group = pressure
UNCORR_REGEX += FLOAT_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # salinity (omitted)
UNCORR_REGEX += FLOAT_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # sound vel (omitted)
UNCORR_REGEX += DATE_TIME_STR + COMMA + ONE_OR_MORE_WHITESPACE_REGEX        # named group = time_string
UNCORR_REGEX += FLOAT_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # sigma t (omitted)
UNCORR_REGEX += FLOAT_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # sigma v (omitted)
UNCORR_REGEX += FLOAT_REGEX + ZERO_OR_MORE_WHITESPACE_REGEX                 # sigma I (omitted)
UNCORR_REGEX += END_OF_LINE_REGEX
UNCORR_MATCHER = re.compile(UNCORR_REGEX)

# match a single line corrected instrument record from Endurance
ENDURANCE_CORR_REGEX = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX             # dcl timestamp, named group = timestamp
ENDURANCE_CORR_REGEX += '(?:' + HASH + '|' + LOGGER_ID_PATTERN + ')?'        # a logger id or a hash, non-captured group
ENDURANCE_CORR_REGEX += ZERO_OR_MORE_WHITESPACE_REGEX
ENDURANCE_CORR_REGEX += TEMP_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX           # named group = temp
ENDURANCE_CORR_REGEX += CONDUCTIVITY_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX   # named group = conductivity
ENDURANCE_CORR_REGEX += PRESSURE_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX       # named group = pressure
ENDURANCE_CORR_REGEX += FLOAT_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # optode oxygen (omitted)
ENDURANCE_CORR_REGEX += DATE_TIME_STR + ZERO_OR_MORE_WHITESPACE_REGEX               # named group = time_string
ENDURANCE_CORR_REGEX += END_OF_LINE_REGEX
ENDURANCE_CORR_MATCHER = re.compile(ENDURANCE_CORR_REGEX)

# match a single line instrument record from Pioneer
PIONEER_REGEX = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + HASH       # dcl timestamp, named group = timestamp
PIONEER_REGEX += ONE_OR_MORE_WHITESPACE_REGEX
PIONEER_REGEX += TEMP_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX           # named group = temp
PIONEER_REGEX += CONDUCTIVITY_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX   # named group = conductivity
PIONEER_REGEX += PRESSURE_REGEX + COMMA + ONE_OR_MORE_WHITESPACE_REGEX       # named group = pressure
PIONEER_REGEX += DATE_TIME_STR + ZERO_OR_MORE_WHITESPACE_REGEX               # named group = time_string
PIONEER_REGEX += END_OF_LINE_REGEX
PIONEER_MATCHER = re.compile(PIONEER_REGEX)

# This table is used in the generation of the data particle.
# Column 1 - particle parameter name & match group name
# Column 2 - data encoding function (conversion required - int, float, etc)
DATA_PARTICLE_MAP = [
    ('dcl_controller_timestamp', str),
    ('temp', float),
    ('conductivity', float),
    ('pressure', float),
    ('date_time_string', str)
]


class DataParticleType(BaseEnum):
    INSTRUMENT_TELEMETERED = 'ctdbp_cdef_dcl_instrument'
    INSTRUMENT_RECOVERED = 'ctdbp_cdef_dcl_instrument_recovered'


class CtdbpCdefDclDataParticle(DataParticle):
    """
    Class for parsing data from the data set
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CtdbpCdefDclDataParticle, self).__init__(raw_data,
                                                       port_timestamp,
                                                       internal_timestamp,
                                                       preferred_timestamp,
                                                       quality_flag,
                                                       new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        utc_time = dcl_controller_timestamp_to_utc_time(self.raw_data.group('dcl_controller_timestamp'))
        self.set_internal_timestamp(unix_time=utc_time)

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """

        return [self._encode_value(name, self.raw_data.group(name), function)
                for name, function in DATA_PARTICLE_MAP]


class CtdbpCdefDclRecoveredDataParticle(CtdbpCdefDclDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.INSTRUMENT_RECOVERED


class CtdbpCdefDclTelemeteredDataParticle(CtdbpCdefDclDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED


class CtdbpCdefDclParser(SimpleParser):
    """
    """
    def __init__(self,
                 is_telemetered,
                 stream_handle,
                 exception_callback):

        super(CtdbpCdefDclParser, self).__init__({},
                                                 stream_handle,
                                                 exception_callback)

        if is_telemetered:
            self._particle_class = CtdbpCdefDclTelemeteredDataParticle
        else:
            self._particle_class = CtdbpCdefDclRecoveredDataParticle

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        for line in self._stream_handle:
            # first check for a match against the uncorrected pattern
            match = UNCORR_MATCHER.match(line)
            if match is None:
                # check for a match against corrected Endurance pattern
                match = ENDURANCE_CORR_MATCHER.match(line)
            if match is None:
                # check for a match against Pioneer pattern
                match = PIONEER_MATCHER.match(line)

            if match is not None:
                log.debug('record found')
                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     match,
                                                     None)
                self._record_buffer.append(data_particle)

            else:
                # NOTE: Need to check for the metadata line last, since the corrected Endurance
                # record also has the [*] pattern
                test_meta = METADATA_MATCHER.match(line)

                if test_meta is None:
                    # something in the data didn't match a required regex, so raise an exception and press on.
                    message = "Error while decoding parameters in data: [%s]" % line
                    self._exception_callback(RecoverableSampleException(message))

