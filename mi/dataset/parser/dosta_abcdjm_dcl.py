#!/usr/bin/env python

"""
@package mi.dataset.parser.dosta_abcdjm_dcl
@file mi/dataset/parser/dosta_abcdjm_dcl.py
@author Steve Myerson
@brief A dosta_abcdjm_dcl-specific data set agent parser

This file contains code for the dosta_abcdjm_dcl parsers and code to produce data particles.
For telemetered data, there is one parser which produces one type of data particle.
For recovered data, there is one parser which produces one type of data particle.
The input files and the content of the data particles are the same for both
recovered and telemetered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 2 types of records.
Records are separated by a newline.
All records start with a timestamp.
Metadata records: timestamp [text] more text newline.
Sensor Data records: timestamp product_number serial_number sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.
"""

__author__ = 'Steve Myerson'
__license__ = 'Apache 2.0'

import re


from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.common_regexes import \
    DATE_YYYY_MM_DD_REGEX, \
    TIME_HR_MIN_SEC_MSEC_REGEX

from mi.dataset.parser.utilities import dcl_controller_timestamp_to_ntp_time

from mi.core.common import BaseEnum
from mi.core.exceptions import UnexpectedDataException

from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue

# Basic patterns
ANY_CHARS = r'.*'              # any characters excluding a newline
FLOAT = r'(\d+\.\d*)'          # unsigned floating point number
NEW_LINE = r'(?:\r\n|\n)'      # any type of new line
SPACE = ' '
TAB = '\t'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (integers, floats)
# All records end with one of the newlines.
TIMESTAMP = '(' + DATE_YYYY_MM_DD_REGEX + SPACE + TIME_HR_MIN_SEC_MSEC_REGEX + ')'
START_METADATA = r'\['
END_METADATA = r'\]'
PRODUCT = '(4831)'                     # the only valid Product Number

# All dosta records are ASCII characters separated by a newline.
DOSTA_RECORD_REGEX = ANY_CHARS       # Any number of characters
DOSTA_RECORD_REGEX += NEW_LINE       # separated by a new line
DOSTA_RECORD_MATCHER = re.compile(DOSTA_RECORD_REGEX)

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_REGEX = TIMESTAMP + SPACE   # date and time
METADATA_REGEX += START_METADATA     # Metadata record starts with '['
METADATA_REGEX += ANY_CHARS          # followed by text
METADATA_REGEX += END_METADATA       # followed by ']'
METADATA_REGEX += ANY_CHARS          # followed by more text
METADATA_REGEX += NEW_LINE           # Record ends with a newline
METADATA_MATCHER = re.compile(METADATA_REGEX)

# Sensor data record:
#   Timestamp ProductNumber<tab>SerialNumber<tab>SensorData
#   where SensorData are tab-separated unsigned floating point numbers
SENSOR_DATA_REGEX = TIMESTAMP + SPACE    # date and time
SENSOR_DATA_REGEX += PRODUCT + TAB       # Product number must be valid
SENSOR_DATA_REGEX += r'(\d{3,4})' + TAB  # 3 or 4 digit serial number
SENSOR_DATA_REGEX += FLOAT + TAB         # oxygen content
SENSOR_DATA_REGEX += FLOAT + TAB         # relative air saturation
SENSOR_DATA_REGEX += FLOAT + TAB         # ambient temperature
SENSOR_DATA_REGEX += FLOAT + TAB         # calibrated phase
SENSOR_DATA_REGEX += FLOAT + TAB         # temperature compensated phase
SENSOR_DATA_REGEX += FLOAT + TAB         # phase measurement with blue excitation
SENSOR_DATA_REGEX += FLOAT + TAB         # phase measurement with red excitation
SENSOR_DATA_REGEX += FLOAT + TAB         # amplitude measurement with blue excitation
SENSOR_DATA_REGEX += FLOAT + TAB         # amplitude measurement with red excitation
SENSOR_DATA_REGEX += FLOAT               # raw temperature voltage from thermistor
SENSOR_DATA_REGEX += NEW_LINE            # Record ends with a newline
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_REGEX)

# SENSOR_DATA_MATCHER produces the following groups.
# The following are indices into groups() produced by SENSOR_DATA_MATCHER.
# i.e, match.groups()[INDEX]
SENSOR_GROUP_TIMESTAMP = 0
SENSOR_GROUP_YEAR = 1
SENSOR_GROUP_MONTH = 2
SENSOR_GROUP_DAY = 3
SENSOR_GROUP_HOUR = 4
SENSOR_GROUP_MINUTE = 5
SENSOR_GROUP_SECOND = 6
SENSOR_GROUP_MILLI_SECOND = 7
SENSOR_GROUP_PRODUCT = 8
SENSOR_GROUP_SERIAL = 9
SENSOR_GROUP_OXYGEN_CONTENT = 10
SENSOR_GROUP_AIR_SATURATION = 11
SENSOR_GROUP_AMBIENT_TEMPERATURE = 12
SENSOR_GROUP_CALIBRATED_PHASE = 13
SENSOR_GROUP_COMPENSATED_PHASE = 14
SENSOR_GROUP_BLUE_PHASE = 15
SENSOR_GROUP_RED_PHASE = 16
SENSOR_GROUP_BLUE_AMPLITUDE = 17
SENSOR_GROUP_RED_AMPLITUDE = 18
SENSOR_GROUP_RAW_TEMPERATURE = 19

# This table is used in the generation of the instrument data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
INSTRUMENT_PARTICLE_MAP = [
    ('dcl_controller_timestamp',       SENSOR_GROUP_TIMESTAMP,           str),
    ('product_number',                 SENSOR_GROUP_PRODUCT,             int),
    ('serial_number',                  SENSOR_GROUP_SERIAL,              str),
    ('estimated_oxygen_concentration', SENSOR_GROUP_OXYGEN_CONTENT,      float),
    ('estimated_oxygen_saturation',    SENSOR_GROUP_AIR_SATURATION,      float),
    ('optode_temperature',             SENSOR_GROUP_AMBIENT_TEMPERATURE, float),
    ('calibrated_phase',               SENSOR_GROUP_CALIBRATED_PHASE,    float),
    ('temp_compensated_phase',         SENSOR_GROUP_COMPENSATED_PHASE,   float),
    ('blue_phase',                     SENSOR_GROUP_BLUE_PHASE,          float),
    ('red_phase',                      SENSOR_GROUP_RED_PHASE,           float),
    ('blue_amplitude',                 SENSOR_GROUP_BLUE_AMPLITUDE,      float),
    ('red_amplitude',                  SENSOR_GROUP_RED_AMPLITUDE,       float),
    ('raw_temperature',                SENSOR_GROUP_RAW_TEMPERATURE,     float)
]


class DostaStateKey(BaseEnum):
    POSITION = 'position'            # position within the input file


class DataParticleType(BaseEnum):
    REC_INSTRUMENT_PARTICLE = 'dosta_abcdjm_dcl_instrument_recovered'
    TEL_INSTRUMENT_PARTICLE = 'dosta_abcdjm_dcl_instrument'


class DostaAbcdjmDclInstrumentDataParticle(DataParticle):
    """
    Class for generating the Dosta instrument particle.
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(DostaAbcdjmDclInstrumentDataParticle, self).__init__(raw_data,
                                                                   port_timestamp,
                                                                   internal_timestamp,
                                                                   preferred_timestamp,
                                                                   quality_flag,
                                                                   new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # Convert the DCL controller timestamp string to NTP time (in seconds and microseconds).
        dcl_controller_timestamp = self.raw_data[SENSOR_GROUP_TIMESTAMP]
        elapsed_seconds_useconds = dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp)
        self.set_internal_timestamp(elapsed_seconds_useconds)

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[group], function)
                for name, group, function in INSTRUMENT_PARTICLE_MAP]


class DostaAbcdjmDclRecoveredInstrumentDataParticle(DostaAbcdjmDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_INSTRUMENT_PARTICLE


class DostaAbcdjmDclTelemeteredInstrumentDataParticle(DostaAbcdjmDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_INSTRUMENT_PARTICLE


class DostaAbcdjmDclParser(SimpleParser):

    """
    Parser for Dosta_abcdjm_dcl data.
    In addition to the standard constructor parameters,
    this constructor takes an additional parameter particle_class.
    """
    def __init__(self,
                 stream_handle,
                 exception_callback,
                 particle_class):

        # No fancy sieve function needed for this parser.
        # File is ASCII with records separated by newlines.

        super(DostaAbcdjmDclParser, self).__init__({},
                                                   stream_handle,
                                                   exception_callback)

        self._particle_class = particle_class

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        for line in self._stream_handle:
            # check for a match against the sensor data pattern
            match = SENSOR_DATA_MATCHER.match(line)

            if match is not None:
                log.debug('record found')
                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     match.groups(),
                                                     None)
                self._record_buffer.append(data_particle)

            else:
                # check to see if this is any other expected format
                test_meta = METADATA_MATCHER.match(line)

                if test_meta is None or line.find(TAB) != -1:
                    # something in the data didn't match a required regex, so raise an exception and press on.
                    message = "Error while decoding parameters in data: [%s]" % line
                    self._exception_callback(UnexpectedDataException(message))


class DostaAbcdjmDclRecoveredParser(DostaAbcdjmDclParser):
    """
    This is the entry point for the Recovered Dosta_abcdjm_dcl parser.
    """
    def __init__(self,
                 stream_handle,
                 exception_callback):

        super(DostaAbcdjmDclRecoveredParser, self).__init__(
            stream_handle,
            exception_callback,
            DostaAbcdjmDclRecoveredInstrumentDataParticle)


class DostaAbcdjmDclTelemeteredParser(DostaAbcdjmDclParser):
    """
    This is the entry point for the Telemetered Dosta_abcdjm_dcl parser.
    """
    def __init__(self,
                 stream_handle,
                 exception_callback):

        super(DostaAbcdjmDclTelemeteredParser, self).__init__(
            stream_handle,
            exception_callback,
            DostaAbcdjmDclTelemeteredInstrumentDataParticle)
