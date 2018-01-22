#!/usr/bin/env python

"""
@package mi.dataset.parser.metbk_a_dcl
@file marine-integrations/mi/dataset/parser/metbk_a_dcl.py
@author Ronald Ronquillo
@brief Parser for the metbk_a_dcl dataset driver

This file contains code for the metbk_a_dcl parsers and code to produce data particles.
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

from mi.dataset.parser.dcl_file_common import \
    DclInstrumentDataParticle, \
    DclFileCommonParser, SPACES, TIMESTAMP, \
    START_METADATA, END_METADATA

from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import UnexpectedDataException, InstrumentParameterException

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, FLOAT_REGEX, ANY_CHARS_REGEX

log = get_logger()

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'

# Basic patterns
FLOAT = '('+FLOAT_REGEX+')'  # floating point as a captured group

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACES  # dcl controller timestamp
METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by text
METADATA_PATTERN += END_METADATA  # followed by ']'
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# Sensor data record:
#   Timestamp Date<tab>Time<tab>SensorData
#   where SensorData are space-separated floating point numbers
SENSOR_DATA_PATTERN = TIMESTAMP + SPACES  # dcl controller timestamp
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Barometric Pressure
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Relative Humidity
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Air Temperature
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Longwave Irradiance
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Precipitation Level
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Sea Surface Temperature
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Sea Surface Conductivity
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Shortwave Irradiance
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Eastward Wind Velocity relative to Magnetic North
SENSOR_DATA_PATTERN += FLOAT + SPACES  # Northward Wind Velocity relative to Magnetic North
SENSOR_DATA_PATTERN += FLOAT_REGEX + SPACES  # Not Applicable (Don't Care)
SENSOR_DATA_PATTERN += FLOAT_REGEX  # Not Applicable (Don't Care)
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX  # sensor data ends with CR-LF
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

# SENSOR_DATA_MATCHER produces the following groups.
# The following are indices into groups() produced by SENSOR_DATA_MATCHER
# incremented after common timestamp values.
# i.e, match.groups()[INDEX]
SENSOR_GROUP_BAROMETRIC_PRESSURE = 1
SENSOR_GROUP_RELATIVE_HUMIDITY = 2
SENSOR_GROUP_AIR_TEMPERATURE = 3
SENSOR_GROUP_LONGWAVE_IRRADIANCE = 4
SENSOR_GROUP_PRECIPITATION = 5
SENSOR_GROUP_SEA_SURFACE_TEMPERATURE = 6
SENSOR_GROUP_SEA_SURFACE_CONDUCTIVITY = 7
SENSOR_GROUP_SHORTWAVE_IRRADIANCE = 8
SENSOR_GROUP_EASTWARD_WIND_VELOCITY = 9
SENSOR_GROUP_NORTHWARD_WIND_VELOCITY = 11

# This table is used in the generation of the instrument data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
INSTRUMENT_PARTICLE_MAP = []


class DataParticleType(BaseEnum):
    REC_INSTRUMENT_PARTICLE = 'metbk_a_dcl_instrument_recovered'
    TEL_INSTRUMENT_PARTICLE = 'metbk_a_dcl_instrument'


class MetbkADclInstrumentDataParticle(DclInstrumentDataParticle):
    """
    Class for generating the Metbk_a instrument particle.
    """

    def __init__(self, raw_data, *args, **kwargs):
        super(MetbkADclInstrumentDataParticle, self).__init__(
            raw_data,
            INSTRUMENT_PARTICLE_MAP,
            *args, **kwargs)


class MetbkADclRecoveredInstrumentDataParticle(MetbkADclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_INSTRUMENT_PARTICLE


class MetbkADclTelemeteredInstrumentDataParticle(MetbkADclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_INSTRUMENT_PARTICLE


class MetbkADclParser(DclFileCommonParser):
    """
    This is the entry point for the Metbk_a_dcl parser.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        super(MetbkADclParser, self).__init__(config,
                                              stream_handle,
                                              exception_callback,
                                              SENSOR_DATA_MATCHER,
                                              METADATA_MATCHER)
        self.particle_classes = None

    def parse_file(self):

        # If not set from config & no InstrumentParameterException error from constructor
        if self.particle_classes is None:
            self.particle_classes = (self._particle_class,)

        for particle_class in self.particle_classes:

            for line in self._stream_handle:
                raw_list = line.split()
                raw_list[0:2] = [' '.join(raw_list[0:2])]                   # Merge the first and second elements to form a timestamp

                if raw_list is not None:
                    raw_data = tuple(raw_list)
                    self.construct_instrument_particle_map(raw_data)        # Set them to their proper mappings
                    particle = self._extract_sample(particle_class,
                                                    None,
                                                    raw_data,
                                                    preferred_ts=DataParticleKey.PORT_TIMESTAMP)
                    self._record_buffer.append(particle)

    def construct_instrument_particle_map(self, raw_data):
        barometric_pressure             = raw_data[1]
        relative_humidity               = raw_data[2]
        air_temperature                 = raw_data[3]
        longwave_irradiance             = raw_data[4]
        precipitation                   = raw_data[5]
        sea_surface_temperature         = raw_data[6]
        sea_surface_conductivity        = raw_data[7]
        shortwave_irradiance            = raw_data[8]
        eastward_wind_velocity          = raw_data[9]
        northward_wind_velocity         = raw_data[10]

        global INSTRUMENT_PARTICLE_MAP
        INSTRUMENT_PARTICLE_MAP = [
            ('barometric_pressure', SENSOR_GROUP_BAROMETRIC_PRESSURE, self.is_float(barometric_pressure)),
            ('relative_humidity', SENSOR_GROUP_RELATIVE_HUMIDITY, self.is_float(relative_humidity)),
            ('air_temperature', SENSOR_GROUP_AIR_TEMPERATURE, self.is_float(air_temperature)),
            ('longwave_irradiance', SENSOR_GROUP_LONGWAVE_IRRADIANCE, self.is_float(longwave_irradiance)),
            ('precipitation', SENSOR_GROUP_PRECIPITATION, self.is_float(precipitation)),
            ('sea_surface_temperature', SENSOR_GROUP_SEA_SURFACE_TEMPERATURE, self.is_float(sea_surface_temperature)),
            ('sea_surface_conductivity', SENSOR_GROUP_SEA_SURFACE_CONDUCTIVITY, self.is_float(sea_surface_conductivity)),
            ('shortwave_irradiance', SENSOR_GROUP_SHORTWAVE_IRRADIANCE, self.is_float(shortwave_irradiance)),
            ('eastward_wind_velocity', SENSOR_GROUP_EASTWARD_WIND_VELOCITY, self.is_float(eastward_wind_velocity)),
            ('northward_wind_velocity', SENSOR_GROUP_NORTHWARD_WIND_VELOCITY, self.is_float(northward_wind_velocity))
        ]

    def is_float(self, val):
        if isinstance(val, float):
            return float
        elif isinstance(val, str):
            return str
        else:
            return 0