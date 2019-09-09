#!/usr/bin/env python

"""

This file contains code for the pco2a_a_sample parser and code to produce data particles.
For instrument recovered and telemetered data, there is one driver which produces two(air/water) types of data particle.
The input files and the content of the data particles are the same for both
instrument telemetered and instrument recovered.
The names of the output particle streams are the same as well.

The input file is ASCII and contains 2 types of records.
Records are separated by a newline.
All records start with a timestamp.
DCL metadata records: timestamp [text] more text newline.
Sensor metadata records: timestamp text newline.
Sensor data records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release based on mi.dataset.parser.pco2a_a_dcl
"""


import re

from mi.core.log import log

from mi.core.common import BaseEnum

from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, \
    DclFileCommonParser, TIMESTAMP, \
    START_METADATA, END_METADATA, START_GROUP, END_GROUP

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, SPACE_REGEX, \
    FLOAT_REGEX, UNSIGNED_INT_REGEX, ANY_CHARS_REGEX

from mi.dataset.parser.utilities import timestamp_yyyy_mm_dd_hh_mm_ss_csv_to_ntp

from mi.core.instrument.data_particle import DataParticleKey

# Basic patterns
UINT = r'(' + UNSIGNED_INT_REGEX + r')'  # unsigned integer as a group
FLOAT = r'(' + FLOAT_REGEX + r')'  # floating point as a captured group
COMMA = r','
CHAR_A = r'(A)'  # air
CHAR_W = r'(W)'  # water
CHAR_M = r'M'
EITHER = r'|'

# Timestamp at the start of each record: 2019,02,27,01,00,00
SENSOR_DATE = r'\d{4},\d{2},\d{2}'  # Sensor Date: YYYY,MM,DD
SENSOR_TIME = r'\d{2},\d{2},\d{2}'  # Sensor Time: HH,MM,SS

# Metadata record:
#   Timestamp [Text]MoreText crlf
DCL_METADATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
DCL_METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
DCL_METADATA_PATTERN += ANY_CHARS_REGEX  # followed by text
DCL_METADATA_PATTERN += END_METADATA  # followed by ']'
DCL_METADATA_PATTERN += ANY_CHARS_REGEX  # followed by more text
DCL_METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with CRLF

# Instrument metadata:
#   Timestamp Text crlf
SENSOR_METADATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
SENSOR_METADATA_PATTERN += ANY_CHARS_REGEX  # followed by text
SENSOR_METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with CRLF

# Match either format of metadata
EITHER_METADATA_PATTERN = START_GROUP + DCL_METADATA_PATTERN + END_GROUP
EITHER_METADATA_PATTERN += EITHER
EITHER_METADATA_PATTERN += START_GROUP + SENSOR_METADATA_PATTERN + END_GROUP

METADATA_MATCHER = re.compile(EITHER_METADATA_PATTERN)

# Sensor data record:
#   Timestamp Date<space>Time<space>SensorData
#   where SensorData are comma-separated unsigned integer numbers
SENSOR_DATA_PATTERN = CHAR_M + COMMA  # signifies this is a data measurement
SENSOR_DATA_PATTERN += START_GROUP + SENSOR_DATE + COMMA  # sensor date
SENSOR_DATA_PATTERN += SENSOR_TIME + END_GROUP + COMMA  # sensor time
SENSOR_DATA_PATTERN += UINT + COMMA  # zero a/d of most recent auto-zero sequence [counts]
SENSOR_DATA_PATTERN += UINT + COMMA  # current a/d [counts]
SENSOR_DATA_PATTERN += FLOAT + COMMA  # measured co2 [ppm]
SENSOR_DATA_PATTERN += FLOAT + COMMA  # average IRGA temperature [deg C]
SENSOR_DATA_PATTERN += FLOAT + COMMA  # humidity [mbar]
SENSOR_DATA_PATTERN += FLOAT + COMMA  # humidity sensor temperature [deg C]
SENSOR_DATA_PATTERN += UINT + COMMA  # gas stream pressure [mbar]
SENSOR_DATA_PATTERN += FLOAT + COMMA  # IRGA detector temperature [deg C]
SENSOR_DATA_PATTERN += FLOAT + COMMA  # IRGA source temperature [deg C]
SENSOR_DATA_PATTERN += FLOAT  # supply voltage
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX

SENSOR_DATA_PATTERN_AIR = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
SENSOR_DATA_PATTERN_AIR += CHAR_A + SPACE_REGEX
SENSOR_DATA_PATTERN_AIR += SENSOR_DATA_PATTERN
SENSOR_DATA_MATCHER_AIR = re.compile(SENSOR_DATA_PATTERN_AIR)

SENSOR_DATA_PATTERN_WATER = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
SENSOR_DATA_PATTERN_WATER += CHAR_W + SPACE_REGEX
SENSOR_DATA_PATTERN_WATER += SENSOR_DATA_PATTERN
SENSOR_DATA_MATCHER_WATER = re.compile(SENSOR_DATA_PATTERN_WATER)

# SENSOR_DATA_MATCHER produces the following groups.
# The following are indices into groups() produced by SENSOR_DATA_MATCHER.
# i.e, match.groups()[INDEX]
SENSOR_GROUP_SENSOR_DATE_TIME = 9
SENSOR_GROUP_ZERO_A2D = 10
SENSOR_GROUP_CURRENT_A2D = 11
SENSOR_GROUP_CO2 = 12
SENSOR_GROUP_AVG_IRGA_TEMP = 13
SENSOR_GROUP_HUMIDITY = 14
SENSOR_GROUP_HUMIDITY_TEMP = 15
SENSOR_GROUP_STREAM_PRESSURE = 16
SENSOR_GROUP_DETECTOR_TEMP = 17
SENSOR_GROUP_SOURCE_TEMP = 18
SENSOR_GROUP_SUPPLY_VOLTAGE = 19
SENSOR_GROUP_SAMPLE_TYPE = 8

INSTRUMENT_PARTICLE_AIR_MAP = [
    ('zero_a2d', SENSOR_GROUP_ZERO_A2D, int),
    ('current_a2d', SENSOR_GROUP_CURRENT_A2D, int),
    ('measured_air_co2', SENSOR_GROUP_CO2, float),
    ('avg_irga_temperature', SENSOR_GROUP_AVG_IRGA_TEMP, float),
    ('humidity', SENSOR_GROUP_HUMIDITY, float),
    ('humidity_temperature', SENSOR_GROUP_HUMIDITY_TEMP, float),
    ('gas_stream_pressure', SENSOR_GROUP_STREAM_PRESSURE, int),
    ('irga_detector_temperature', SENSOR_GROUP_DETECTOR_TEMP, float),
    ('irga_source_temperature', SENSOR_GROUP_SOURCE_TEMP, float),
    ('supply_voltage', SENSOR_GROUP_SUPPLY_VOLTAGE, float)
]

INSTRUMENT_PARTICLE_WATER_MAP = [
    ('zero_a2d', SENSOR_GROUP_ZERO_A2D, int),
    ('current_a2d', SENSOR_GROUP_CURRENT_A2D, int),
    ('measured_water_co2', SENSOR_GROUP_CO2, float),
    ('avg_irga_temperature', SENSOR_GROUP_AVG_IRGA_TEMP, float),
    ('humidity', SENSOR_GROUP_HUMIDITY, float),
    ('humidity_temperature', SENSOR_GROUP_HUMIDITY_TEMP, float),
    ('gas_stream_pressure', SENSOR_GROUP_STREAM_PRESSURE, int),
    ('irga_detector_temperature', SENSOR_GROUP_DETECTOR_TEMP, float),
    ('irga_source_temperature', SENSOR_GROUP_SOURCE_TEMP, float),
    ('supply_voltage', SENSOR_GROUP_SUPPLY_VOLTAGE, float)
]


# Recovered data will go to these streams now vs "recovered" streams
class DataParticleType(BaseEnum):
    PCO2A_INSTRUMENT_AIR_PARTICLE = 'pco2a_a_dcl_instrument_air'
    PCO2A_INSTRUMENT_WATER_PARTICLE = 'pco2a_a_dcl_instrument_water'


class Pco2aADclParticleClassKey(BaseEnum):
    """
    An enum for the keys application to the pco2a_a_dcl particle classes
    """
    AIR_PARTICLE_CLASS = 'air_particle_class'
    WATER_PARTICLE_CLASS = 'water_particle_class'


class Pco2aADclInstrumentDataParticleAir(DclInstrumentDataParticle):
    """
    Class for generating the Pco2a_a_dcl instrument particles.
    """
    _data_particle_type = DataParticleType.PCO2A_INSTRUMENT_AIR_PARTICLE

    data_matcher = SENSOR_DATA_MATCHER_AIR

    def __init__(self, raw_data, *args, **kwargs):
        super(Pco2aADclInstrumentDataParticleAir, self).__init__(
            raw_data, INSTRUMENT_PARTICLE_AIR_MAP, *args, **kwargs)

        # instrument_timestamp is the internal_timestamp
        instrument_timestamp = self.raw_data[SENSOR_GROUP_SENSOR_DATE_TIME]
        elapsed_seconds_useconds = \
            timestamp_yyyy_mm_dd_hh_mm_ss_csv_to_ntp(instrument_timestamp)
        self.set_internal_timestamp(elapsed_seconds_useconds)

        # instrument clock is not accurate so, use port_timestamp as the preferred_ts
        self.contents[DataParticleKey.PREFERRED_TIMESTAMP] = DataParticleKey.PORT_TIMESTAMP


class Pco2aADclInstrumentDataParticleWater(DclInstrumentDataParticle):
    """
    Class for generating the Pco2a_a_dcl instrument particles.
    """
    _data_particle_type = DataParticleType.PCO2A_INSTRUMENT_WATER_PARTICLE

    data_matcher = SENSOR_DATA_MATCHER_WATER

    def __init__(self, raw_data, *args, **kwargs):
        super(Pco2aADclInstrumentDataParticleWater, self).__init__(
            raw_data, INSTRUMENT_PARTICLE_WATER_MAP, *args, **kwargs)

        # Instrument timestamp is the internal timestamp
        instrument_timestamp = self.raw_data[SENSOR_GROUP_SENSOR_DATE_TIME]
        elapsed_seconds_useconds = \
            timestamp_yyyy_mm_dd_hh_mm_ss_csv_to_ntp(instrument_timestamp)
        self.set_internal_timestamp(elapsed_seconds_useconds)

        # instrument clock is not accurate so, use port_timestamp as the preferred_ts
        self.contents[DataParticleKey.PREFERRED_TIMESTAMP] = DataParticleKey.PORT_TIMESTAMP


class Pco2aADclParser(DclFileCommonParser):
    """
    This is the entry point for the parser.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        super(Pco2aADclParser, self).__init__(config,
                                              stream_handle,
                                              exception_callback,
                                              None,
                                              METADATA_MATCHER)