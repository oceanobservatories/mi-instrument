#!/usr/bin/env python

"""
@package mi.dataset.parser.flort_dj_dcl
@file marine-integrations/mi/dataset/parser/flort_dj_dcl.py
@author Steve Myerson
@brief Parser for the flort_dj_dcl dataset driver

This file contains code for the flort_dj_dcl parsers and code to produce data particles.
For telemetered data, there is one parser which produces one type of data particle.
For recovered data, there is one parser which produces one type of data particle.
The input files and the content of the data particles are the same for both
recovered and telemetered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 2 types of records.
Records are separated by a newline.
All records start with a timestamp.
Metadata records: timestamp [text] more text newline.
Sensor Data records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release
"""

import re

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.exceptions import UnexpectedDataException

from mi.core.instrument.dataset_data_particle import DataParticle

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.common_regexes import \
    DATE_YYYY_MM_DD_REGEX

from mi.dataset.parser.utilities import \
    dcl_controller_timestamp_to_ntp_time

log = get_logger()

__author__ = 'Steve Myerson'
__license__ = 'Apache 2.0'

# Basic patterns
ANY_CHARS = r'.*'          # Any characters excluding a newline
NEW_LINE = r'(?:\r\n|\n)'  # any type of new line
UINT = r'(\d*)'            # unsigned integer as a group
SPACE = ' '
TAB = '\t'
START_GROUP = '('
END_GROUP = ')'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
TIME = r'(\d{2}):(\d{2}):(\d{2})\.\d{3}'  # Time: HH:MM:SS.mmm
SENSOR_DATE = r'(\d{2}/\d{2}/\d{2})'      # Sensor Date: MM/DD/YY
SENSOR_TIME = r'(\d{2}:\d{2}:\d{2})'      # Sensor Time: HH:MM:SS
TIMESTAMP = START_GROUP + DATE_YYYY_MM_DD_REGEX + SPACE + TIME + END_GROUP
START_METADATA = r'\['
END_METADATA = r'\]'

# All flort records are ASCII characters separated by a newline.
FLORT_RECORD_PATTERN = ANY_CHARS       # Any number of ASCII characters
FLORT_RECORD_PATTERN += NEW_LINE       # separated by a new line
FLORT_RECORD_MATCHER = re.compile(FLORT_RECORD_PATTERN)

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE  # dcl controller timestamp
METADATA_PATTERN += START_METADATA    # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS         # followed by text
METADATA_PATTERN += END_METADATA      # followed by ']'
METADATA_PATTERN += ANY_CHARS         # followed by more text
METADATA_PATTERN += NEW_LINE          # metadata record ends with a newline
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# Sensor data record:
#   Timestamp Date<tab>Time<tab>SensorData
#   where SensorData are tab-separated unsigned integer numbers
SENSOR_DATA_PATTERN = TIMESTAMP + SPACE    # dcl controller timestamp
SENSOR_DATA_PATTERN += SENSOR_DATE + TAB   # sensor date
SENSOR_DATA_PATTERN += SENSOR_TIME + TAB   # sensor time
SENSOR_DATA_PATTERN += UINT + TAB          # measurement wavelength beta
SENSOR_DATA_PATTERN += UINT + TAB          # raw signal beta
SENSOR_DATA_PATTERN += UINT + TAB          # measurement wavelength chl
SENSOR_DATA_PATTERN += UINT + TAB          # raw signal chl
SENSOR_DATA_PATTERN += UINT + TAB          # measurement wavelength cdom
SENSOR_DATA_PATTERN += UINT + TAB          # raw signal cdom
SENSOR_DATA_PATTERN += UINT                # raw internal temperature
SENSOR_DATA_PATTERN += NEW_LINE            # sensor data ends with a newline
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

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
SENSOR_GROUP_SENSOR_DATE = 7
SENSOR_GROUP_SENSOR_TIME = 8
SENSOR_GROUP_WAVELENGTH_BETA = 9
SENSOR_GROUP_RAW_SIGNAL_BETA = 10
SENSOR_GROUP_WAVELENGTH_CHL = 11
SENSOR_GROUP_RAW_SIGNAL_CHL = 12
SENSOR_GROUP_WAVELENGTH_CDOM = 13
SENSOR_GROUP_RAW_SIGNAL_CDOM = 14
SENSOR_GROUP_INTERNAL_TEMPERATURE = 15

# This table is used in the generation of the instrument data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
INSTRUMENT_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    SENSOR_GROUP_TIMESTAMP,             str),
    ('date_string',                 SENSOR_GROUP_SENSOR_DATE,           str),
    ('time_string',                 SENSOR_GROUP_SENSOR_TIME,           str),
    ('measurement_wavelength_beta', SENSOR_GROUP_WAVELENGTH_BETA,       int),
    ('raw_signal_beta',             SENSOR_GROUP_RAW_SIGNAL_BETA,       int),
    ('measurement_wavelength_chl',  SENSOR_GROUP_WAVELENGTH_CHL,        int),
    ('raw_signal_chl',              SENSOR_GROUP_RAW_SIGNAL_CHL,        int),
    ('measurement_wavelength_cdom', SENSOR_GROUP_WAVELENGTH_CDOM,       int),
    ('raw_signal_cdom',             SENSOR_GROUP_RAW_SIGNAL_CDOM,       int),
    ('raw_internal_temp',           SENSOR_GROUP_INTERNAL_TEMPERATURE,  int)
]


class DataParticleType(BaseEnum):
    REC_INSTRUMENT_PARTICLE = 'flort_dj_dcl_instrument_recovered'
    TEL_INSTRUMENT_PARTICLE = 'flort_dj_dcl_instrument'


class FlortDjDclInstrumentDataParticle(DataParticle):
    """
    Class for generating the Flort_dj instrument particle.
    """

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        # The particle timestamp is the DCL Controller timestamp.
        # Convert the DCL controller timestamp string to NTP time (in seconds and microseconds).
        dcl_controller_timestamp = self.raw_data[SENSOR_GROUP_TIMESTAMP]
        elapsed_seconds_useconds = dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp)
        self.set_internal_timestamp(elapsed_seconds_useconds)

        return [self._encode_value(name, self.raw_data[group], function)
                for name, group, function in INSTRUMENT_PARTICLE_MAP]


class FlortDjDclRecoveredInstrumentDataParticle(FlortDjDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_INSTRUMENT_PARTICLE


class FlortDjDclTelemeteredInstrumentDataParticle(FlortDjDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_INSTRUMENT_PARTICLE


class FlortDjDclParser(SimpleParser):

    """
    Parser for Flort_dj_dcl data.
    In addition to the standard constructor parameters,
    this constructor takes an additional parameter particle_class.
    """

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        for line in self._stream_handle:

            # If this is a valid sensor data record,
            # use the extracted fields to generate a particle.

            sensor_match = SENSOR_DATA_MATCHER.match(line)
            if sensor_match is not None:
                particle = self._extract_sample(self._particle_class,
                                                None,
                                                sensor_match.groups(),
                                                None)
                # increment state for this chunk even if we don't get a particle
                self._record_buffer.append(particle)

            # It's not a sensor data record, see if it's a metadata record.

            else:

                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.

                meta_match = METADATA_MATCHER.match(line)
                if meta_match is None:
                    error_message = 'Unknown data found in chunk %s' % line
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))
