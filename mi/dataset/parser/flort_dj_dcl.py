#!/usr/bin/env python

"""
@package mi.dataset.parser.flort_dj_dcl
@file marine-integrations/mi/dataset/parser/flort_dj_dcl.py
@author Steve Myerson
@brief Parser for the flort_dj_dcl dataset driver

This file contains code for the flort_dj_dcl parsers and code to produce
data particles.
For telemetered data, there is one parser which produces one type of data
particle.  For recovered data, there is one parser which produces one type
of data particle.  The input files and the content of the data particles are
the same for both recovered and telemetered.  Only the names of the output
particle streams are different.

The input file is ASCII and contains 2 types of records.
Records are separated by a newline.
All records start with a timestamp.
Metadata records: timestamp [text] more text newline.
Sensor Data records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release

Change History:

Date         Ticket#    Engineer     Description
------------ ---------- -----------  --------------------------
5/28/17      #9809      janeenP      Added functionality for combined CTDBP
                                         with FLORT

"""

import ntplib
import re

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.exceptions import \
    UnexpectedDataException, \
    RecoverableSampleException
from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey

from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.common_regexes import \
    DATE_YYYY_MM_DD_REGEX, \
    ANY_CHARS_REGEX, \
    SPACE_REGEX, \
    FLOAT_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, \
    END_OF_LINE_REGEX

from mi.dataset.parser.utilities import \
    dcl_time_to_ntp, \
    formatted_timestamp_utc_time, \
    timestamp_mmddyyhhmmss_to_ntp

log = get_logger()

__author__ = 'Steve Myerson'
__license__ = 'Apache 2.0'

# Basic patterns
UINT = r'(\d*)'  # unsigned integer as a group
TAB = '\t'
START_GROUP = '('
END_GROUP = ')'
COLON = ':'  # simple colon
COMMA = ','  # simple comma
HASH = '#'  # hash symbol
ZERO_OR_MORE_WHITESPACE_REGEX = r'\s*'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
TIME = r'(\d{2}):(\d{2}):(\d{2})\.\d{3}'  # Time: HH:MM:SS.mmm
SENSOR_DATE = r'(\d{2}/\d{2}/\d{2})'  # Sensor Date: MM/DD/YY
SENSOR_TIME = r'(\d{2}:\d{2}:\d{2})'  # Sensor Time: HH:MM:SS

# CTDBP date_time DD MON YYYY HH:MM:SS
CTDBP_FLORT_DATE_TIME = r'(\d{2} \D{3} \d{4} \d{2}:\d{2}:\d{2})'

TIMESTAMP = START_GROUP + DATE_YYYY_MM_DD_REGEX + SPACE_REGEX
TIMESTAMP += TIME + END_GROUP
START_METADATA = r'\['
END_METADATA = r'\]'

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by text
METADATA_PATTERN += END_METADATA  # followed by ']'
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with a newline
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# FLORT Sensor data record:
#   Timestamp Date<tab>Time<tab>SensorData
#   where SensorData are tab-separated unsigned integer numbers
SENSOR_DATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
SENSOR_DATA_PATTERN += SENSOR_DATE + TAB  # sensor date
SENSOR_DATA_PATTERN += SENSOR_TIME + TAB  # sensor time
SENSOR_DATA_PATTERN += UINT + TAB  # measurement wavelength beta
SENSOR_DATA_PATTERN += UINT + TAB  # raw signal beta
SENSOR_DATA_PATTERN += UINT + TAB  # measurement wavelength chl
SENSOR_DATA_PATTERN += UINT + TAB  # raw signal chl
SENSOR_DATA_PATTERN += UINT + TAB  # measurement wavelength cdom
SENSOR_DATA_PATTERN += UINT + TAB  # raw signal cdom
SENSOR_DATA_PATTERN += UINT  # raw internal temperature
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX  # sensor data ends with a newline
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
    ('measurement_wavelength_beta', SENSOR_GROUP_WAVELENGTH_BETA, int),
    ('raw_signal_beta', SENSOR_GROUP_RAW_SIGNAL_BETA, int),
    ('measurement_wavelength_chl', SENSOR_GROUP_WAVELENGTH_CHL, int),
    ('raw_signal_chl', SENSOR_GROUP_RAW_SIGNAL_CHL, int),
    ('measurement_wavelength_cdom', SENSOR_GROUP_WAVELENGTH_CDOM, int),
    ('raw_signal_cdom', SENSOR_GROUP_RAW_SIGNAL_CDOM, int),
    ('raw_internal_temp', SENSOR_GROUP_INTERNAL_TEMPERATURE, int)
]

# Combined CTDBP_FLORT Sensor data record:
#
# match a single line from a platform generated by a combined CTDBP_CDEF_DCL
# with a FLORT_D plugged into it and process the FLORT data only.
#
#   Timestamp <optional>LOGGER_ID <optional># SensorData Date Time
#   where SensorData are comma-separated floats and unsigned integer numbers
#         Date is DD MMM YYYY format
#         Time is HH:MM:SS format
LOGGER_ID = START_METADATA + ANY_CHARS_REGEX + END_METADATA
LOGGER_ID += COLON  # [ id_string ]:

CTDBP_FLORT_PATTERN = TIMESTAMP  # dcl controller timestamp
CTDBP_FLORT_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += '(?:' + HASH + '|' + LOGGER_ID + ')?'  # logger id or #
CTDBP_FLORT_PATTERN += ZERO_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += FLOAT_REGEX + COMMA  # temp (omitted)
CTDBP_FLORT_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += FLOAT_REGEX + COMMA  # pressure (omitted)
CTDBP_FLORT_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += FLOAT_REGEX + COMMA  # conductivity (omitted)
CTDBP_FLORT_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += UINT + COMMA  # raw backscatter
CTDBP_FLORT_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += UINT + COMMA  # raw chlorophyl
CTDBP_FLORT_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += UINT + COMMA  # raw cdom
CTDBP_FLORT_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += CTDBP_FLORT_DATE_TIME  # sensor date_time
CTDBP_FLORT_PATTERN += ZERO_OR_MORE_WHITESPACE_REGEX
CTDBP_FLORT_PATTERN += END_OF_LINE_REGEX  # sensor data ends with a newline
CTDBP_FLORT_MATCHER = re.compile(CTDBP_FLORT_PATTERN)

# Combined CTDBP_FLORT_MATCHER produces the following groups.
# The following are indices into groups() produced by CTDBP_FLORT_MATCHER
# i.e, match.groups()[INDEX]
CTDBP_FLORT_GROUP_RAW_BACKSCATTER = 7
CTDBP_FLORT_GROUP_RAW_CHL = 8
CTDBP_FLORT_GROUP_RAW_CDOM = 9
CTDBP_FLORT_GROUP_DATE_TIME = 10

# This table is used in the generation of the combined FLORT instrument data
# particle from the combined CTDBP FLORT data.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
CTDBP_FLORT_PARTICLE_MAP = [
    ('raw_signal_beta', CTDBP_FLORT_GROUP_RAW_BACKSCATTER, int),
    ('raw_signal_chl', CTDBP_FLORT_GROUP_RAW_CHL, int),
    ('raw_signal_cdom', CTDBP_FLORT_GROUP_RAW_CDOM, int)
]


class DataParticleType(BaseEnum):
    REC_INSTRUMENT_PARTICLE = 'flort_sample'
    TEL_INSTRUMENT_PARTICLE = 'flort_sample'


class FlortDjDclInstrumentDataParticle(DataParticle):
    """
    Class for generating the Flort_dj instrument particle.
    """

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument
        Data Particle.
        @throws RecoverableSampleException If there is a problem with
        sample creation
        """
        # Generate a particle by calling encode_value for each entry
        # in the Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into the match groups (which is what has been stored
        # in raw_data), and a function to use for data conversion.

        try:
            return [self._encode_value(name, self.raw_data[group], function)
                    for name, group, function in self._data_particle_map]

        except (ValueError, TypeError, IndexError) as ex:
            message = ("Error(%s) while decoding parameters in data: [%s]" % (ex, self.raw_data))
            log.warn("Warning: %s", message)
            raise RecoverableSampleException(message)


class FlortDjDclRecoveredInstrumentDataParticle(
                FlortDjDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_INSTRUMENT_PARTICLE


class FlortDjDclTelemeteredInstrumentDataParticle(
                FlortDjDclInstrumentDataParticle):
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
        Parse through the file, pulling single lines and comparing to
        the established patterns, generating particles for data lines
        """
        for line in self._stream_handle:
            message = 'data line \n%s' % line
            log.debug(message)

            # First check for valid FLORT DJ DCL data
            # If this is a valid sensor data record,
            # use the extracted fields to generate a particle.
            sensor_match = SENSOR_DATA_MATCHER.match(line)

            if sensor_match is not None:
                self._particle_class._data_particle_map = INSTRUMENT_PARTICLE_MAP
                log.debug('FLORT DJ match found')
            else:
                log.debug('FLORT DJ match NOT found')
                # check for a match against the FLORT D data in a combined
                # CTDBP FLORT instrument record
                sensor_match = CTDBP_FLORT_MATCHER.match(line)

                if sensor_match is not None:
                    self._particle_class._data_particle_map = CTDBP_FLORT_PARTICLE_MAP
                    log.debug('check for CTDBP/FLORT match')

            if sensor_match is not None:
                # FLORT data matched against one of the patterns
                log.debug('record found')

                # DCL Controller timestamp is the port_timestamp
                dcl_controller_timestamp = sensor_match.groups()[SENSOR_GROUP_TIMESTAMP]
                port_timestamp = dcl_time_to_ntp(dcl_controller_timestamp)

                if self._particle_class._data_particle_map == INSTRUMENT_PARTICLE_MAP:
                    # For valid FLORT DJ data, Instrument timestamp is the internal_timestamp
                    instrument_timestamp = sensor_match.groups()[SENSOR_GROUP_SENSOR_DATE] \
                                           + ' ' + sensor_match.groups()[SENSOR_GROUP_SENSOR_TIME]
                    internal_timestamp = timestamp_mmddyyhhmmss_to_ntp(instrument_timestamp)
                else:
                    # _data_particle_map is CTDBP_FLORT_PARTICLE_MAP
                    utc_time = formatted_timestamp_utc_time(sensor_match.groups()[CTDBP_FLORT_GROUP_DATE_TIME],
                                                            "%d %b %Y %H:%M:%S")
                    instrument_timestamp = ntplib.system_to_ntp_time(utc_time)
                    internal_timestamp = instrument_timestamp

                # using port_timestamp as preferred_ts because internal_timestamp is not accurate
                particle = self._extract_sample(self._particle_class,
                                                None,
                                                sensor_match.groups(),
                                                port_timestamp=port_timestamp,
                                                internal_timestamp=internal_timestamp,
                                                preferred_ts=DataParticleKey.PORT_TIMESTAMP)
                # increment state for this chunk even if we don't
                # get a particle
                self._record_buffer.append(particle)

            # It's not a sensor data record, see if it's a metadata record.
            else:
                log.debug('No data recs found, check for meta record')

                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.

                meta_match = METADATA_MATCHER.match(line)
                if meta_match is None:
                    error_message = 'Unknown data found in chunk %s' % line
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))
