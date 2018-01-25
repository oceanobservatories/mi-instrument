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
    DclFileCommonParser

from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.core.exceptions import UnexpectedDataException

log = get_logger()

__author__ = 'Phillip Tran'
__license__ = 'Apache 2.0'

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
SENSOR_GROUP_NORTHWARD_WIND_VELOCITY = 10

# This table is used in the generation of the instrument data particle.
# This will be a list of tuples with the following columns.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
INSTRUMENT_PARTICLE_MAP = [
    ('barometric_pressure', SENSOR_GROUP_BAROMETRIC_PRESSURE, float),
    ('relative_humidity', SENSOR_GROUP_RELATIVE_HUMIDITY, float),
    ('air_temperature', SENSOR_GROUP_AIR_TEMPERATURE, float),
    ('longwave_irradiance', SENSOR_GROUP_LONGWAVE_IRRADIANCE, float),
    ('precipitation', SENSOR_GROUP_PRECIPITATION, float),
    ('sea_surface_temperature', SENSOR_GROUP_SEA_SURFACE_TEMPERATURE, float),
    ('sea_surface_conductivity', SENSOR_GROUP_SEA_SURFACE_CONDUCTIVITY, float),
    ('shortwave_irradiance', SENSOR_GROUP_SHORTWAVE_IRRADIANCE, float),
    ('eastward_wind_velocity', SENSOR_GROUP_EASTWARD_WIND_VELOCITY, float),
    ('northward_wind_velocity', SENSOR_GROUP_NORTHWARD_WIND_VELOCITY, float)
]


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

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        Will only append float values and ignore strings.
        Returns the list.
        """
        data_list = []
        for name, group, func in INSTRUMENT_PARTICLE_MAP:
            if isinstance(self.raw_data[group], func):
                data_list.append(self._encode_value(name, self.raw_data[group], func))
        return data_list





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
                                              '',
                                              '')
        self.particle_classes = None
        self.instrument_particle_map = INSTRUMENT_PARTICLE_MAP

    def parse_file(self):
        """
        This method reads the file and parses the data within, and at
        the end of this method self._record_buffer will be filled with all the particles in the file.
        """

        # If not set from config & no InstrumentParameterException error from constructor
        if self.particle_classes is None:
            self.particle_classes = (self._particle_class,)

        for particle_class in self.particle_classes:

            for line in self._stream_handle:
                if not re.findall(r'\[.*\]:[a-z|A-Z]+', line):                  # Disregard anything that has a character after [Something]:
                    line = re.sub(r'\[.*\]:', '', line)
                    raw_data = line.split()
                    raw_data[0:2] = [' '.join(raw_data[0:2])]                   # Merge the first and second elements to form a timestamp
                    if raw_data is not None:
                        for i in range(1, len(raw_data)):                       # Ignore 0th element, because that is the timestamp
                            raw_data[i] = self.select_type(raw_data[i])
                    # self.construct_instrument_particle_map(raw_data)
                    particle = self._extract_sample(particle_class,
                                                    None,
                                                    raw_data,
                                                    preferred_ts=DataParticleKey.PORT_TIMESTAMP)
                    self._record_buffer.append(particle)

                # else:
                #     # If it's a valid metadata record, ignore it.
                #     # Otherwise generate warning for unknown data.
                #     error_message = 'Unknown data found in chunk %s' % line
                #     log.warn(error_message)
                #     self._exception_callback(UnexpectedDataException(error_message))

    @staticmethod
    def select_type(raw_list_element):
        """
        This function will return the float or string value of the particle.
        """
        try:
            return float(raw_list_element)
        except ValueError:
            pass

