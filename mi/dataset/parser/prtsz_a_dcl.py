#!/usr/bin/env python

"""
@package mi.dataset.parser.prtsz_a_dcl
@file mi-dataset/mi/dataset/parser/prtsz_a_dcl.py
@author Samuel Dahlberg
@brief Parser for the prtsz_a_dcl dataset driver.

This file contains code for the PRTSZ parser and code to produce data particles
for the instrument recovered data from the PRTSZ instrument.

The input file has ASCII data.
The records are separated by a newline.
Comments: dcl_timestamp [text]: text newline.
Instrument records: dcl_timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.
"""

import re

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()
from mi.core.common import BaseEnum
from mi.dataset.dataset_parser import SimpleParser
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from datetime import datetime
from mi.dataset.parser.utilities import dcl_time_to_ntp, timestamp_yyyymmddhhmmss_to_ntp

# Basic REGEX patterns
DCL_TIMESTAMP_REGEX = r'(\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}.\d{3})'
FLOAT = r'([+-]?\d+.\d+[Ee]?[+-]?\d*)'
INTEGER = r'([+-]?[0-9]+)'
NEWLINE = r'(?:\r\n|\n)?'

# REGEX for the 36 columns of data representing the different volume concentration size classes
size_classes = r'(([+-]?\d+.\d{4})(,([+-]?\d+.\d{4})){35})'

# Full REGEX pattern for data
DATA_PATTERN = (
        DCL_TIMESTAMP_REGEX + r'\s' +  # Time-Stamp
        size_classes + r',' +  # 36 Column data for particle concentration
        FLOAT + r',' +  # Laser transmission Sensor [mW]
        FLOAT + r',' +  # Supply voltage in [V]
        FLOAT + r',' +  # External analog input 1 [V]
        FLOAT + r',' +  # Laser Reference sensor [mW]
        FLOAT + r',' +  # Depth in [m of seawater]
        FLOAT + r',' +  # Temperature [C]
        INTEGER + r',' +  # Year
        INTEGER + r',' +  # Month
        INTEGER + r',' +  # Day
        INTEGER + r',' +  # Hour
        INTEGER + r',' +  # Minute
        INTEGER + r',' +  # Second
        FLOAT + r',' +  # External analog input 2 [V]
        FLOAT + r',' +  # Mean Diameter [um]
        FLOAT + r',' +  # Total Volume Concentration [PPM]
        INTEGER + r',' +  # Relative Humidity [%]
        INTEGER + r',' +  # Accelerometer X
        INTEGER + r',' +  # Accelerometer Y
        INTEGER + r',' +  # Accelerometer Z
        INTEGER + r',' +  # Raw pressure [overflow single bit]
        INTEGER + r',' +  # Raw pressure [least significant 16 bits]
        INTEGER + r',' +  # Ambient Light [counts]
        FLOAT + r',' +  # External analog input 3 [V]
        FLOAT + r',' +  # Computed optical transmission over path [dimensionless]
        FLOAT + NEWLINE  # Beam-attenuation (c) [m-1]
)

DATA_REGEX = re.compile(DATA_PATTERN)

# REGEX pattern for comments
COMMENT_PATTERN = (
        DCL_TIMESTAMP_REGEX +
        r'(\s\[.*?])' +
        r'(.*?)'
)

COMMENT_REGEX = re.compile(COMMENT_PATTERN)


class PrtszAParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted for the data particle.
    """

    VOLUME_CONCENTRATION = 'volume_concentration'
    LASER_TRANSMISSION_SENSOR = 'laser_transmission_sensor'
    SUPPLY_VOLTAGE = 'supply_voltage'
    LASER_REFERENCE_SENSOR = 'laser_reference_sensor'
    DEPTH = 'depth'
    TEMPERATURE = 'temperature'
    MEAN_DIAMETER = 'mean_particle_diameter'
    TOTAL_VOLUME_CONCENTRATION = 'total_volume_concentration'
    RELATIVE_HUMIDITY = 'relative_humidity'
    AMBIENT_LIGHT = 'ambient_light'
    COMPUTED_OPTICAL_TRANSMISSION = 'computed_optical_transmission'
    BEAM_ATTENUATION = 'beam_attenuation'
    NUM_BINS = 'num_bins'
    # MAX_PARTICLE_SIZE = 'max_particle_size'
    # PARTICLE_LOWER_SIZE_BINS = 'particle_lower_size_bins'


class DataParticleType(BaseEnum):
    PRTSZ_A_PARTICLE_TYPE = 'prtsz_a_instrument'
    __metaclass__ = get_logging_metaclass(log_level='trace')


class PrtszADataParticle(DataParticle):
    """
    Class for generating the prtsz_a instrument particle.
    """

    _data_particle_type = DataParticleType.PRTSZ_A_PARTICLE_TYPE

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]


class PrtszADclParser(SimpleParser):
    """
    Prtsz_a dcl parser.
    """

    def parse_record(self, record):
        """
        Parse a matched data record from the prtsz_a log file.
        @param record: MatchObject containing a regex match for prtsz_a ASCII data
        @return: dictionary of values with the particle names as keys
        """

        dcl_time = str(record.group(1))

        # Combine all dates of instrument time into ntp epoch time
        year = int(record.group(12))
        month = int(record.group(13))
        day = int(record.group(14))
        hour = int(record.group(15))
        minute = int(record.group(16))
        second = int(record.group(17))
        instrument_time = datetime(year, month, day, hour, minute, second).strftime("%Y%m%d%H%M%S")

        # Create a list of the 36 columns of volume concentration data and assign to parameter
        vol_conc = (record.group(2)).split(',')
        vol_conc = list(map(float, vol_conc))

        # Assign the remaining prtsz data to named variables
        laser_transmission_sensor = float(record.group(6))
        supply_voltage = float(record.group(7))
        laser_reference_sensor = float(record.group(9))
        depth = float(record.group(10))
        temperature = float(record.group(11))
        mean_diameter = float(record.group(19))
        total_volume_concentration = float(record.group(20))
        relative_humidity = int(record.group(21))
        ambient_light = int(record.group(27))
        computed_optical_transmission = float(record.group(29))
        beam_attenuation = float(record.group(30))

        # Volume concentration bin information
        bin_count = 36
        # max_particle_size = 500
        # particle_lower_size_bins = [1.00, 1.48, 1.74, 2.05, 2.42, 2.86, 3.38, 3.98, 4.70, 5.55, 6.55, 7.72, 9.12, 10.8,
        #                             12.7, 15.0, 17.7, 20.9, 24.6, 29.1, 34.3, 40.5, 47.7, 56.3, 66.5, 78.4, 92.6, 109,
        #                             129, 152, 180, 212, 250, 297, 354, 420]

        prtsz_particle_data = {
            PrtszAParticleKey.VOLUME_CONCENTRATION: vol_conc,
            PrtszAParticleKey.LASER_TRANSMISSION_SENSOR: laser_transmission_sensor,
            PrtszAParticleKey.SUPPLY_VOLTAGE: supply_voltage,
            PrtszAParticleKey.LASER_REFERENCE_SENSOR: laser_reference_sensor,
            PrtszAParticleKey.DEPTH: depth,
            PrtszAParticleKey.TEMPERATURE: temperature,
            PrtszAParticleKey.MEAN_DIAMETER: mean_diameter,
            PrtszAParticleKey.TOTAL_VOLUME_CONCENTRATION: total_volume_concentration,
            PrtszAParticleKey.RELATIVE_HUMIDITY: relative_humidity,
            PrtszAParticleKey.AMBIENT_LIGHT: ambient_light,
            PrtszAParticleKey.COMPUTED_OPTICAL_TRANSMISSION: computed_optical_transmission,
            PrtszAParticleKey.BEAM_ATTENUATION: beam_attenuation,
            PrtszAParticleKey.NUM_BINS: bin_count,
            # PrtszAParticleKey.MAX_PARTICLE_SIZE: max_particle_size,
            # PrtszAParticleKey.PARTICLE_LOWER_SIZE_BINS: particle_lower_size_bins
        }

        return prtsz_particle_data, dcl_time, instrument_time

    def parse_file(self):
        """
        Parse the prtsz_a log file.
        Read file line by line. Values are extracted from lines containing ASCII data
        @return: dictionary of data values with the particle names as keys
        """

        for number, line in enumerate(self._stream_handle, start=1):

            # Is line a comment?
            match = COMMENT_REGEX.match(line)
            if match is not None:
                log.trace("MATCHED INSTRUMENT COMMENT: %s: %s", number, match.groups())
                continue

            # Is line a data output?
            match = DATA_REGEX.match(line)
            if match is not None:
                prtsz_particle_data, dcl_timestamp, instrument_timestamp = self.parse_record(match)
                if prtsz_particle_data is None:
                    log.error('Erroneous data found in line %s: %s', number, line)
                    continue

                port_timestamp = dcl_time_to_ntp(dcl_timestamp)
                internal_timestamp = timestamp_yyyymmddhhmmss_to_ntp(instrument_timestamp)

                particle = self._extract_sample(self._particle_class, None, prtsz_particle_data, port_timestamp,
                                                internal_timestamp, DataParticleKey.PORT_TIMESTAMP)
                if particle is not None:
                    self._record_buffer.append(particle)
                    log.trace('Parsed particle: %s' % particle.generate_dict())
                continue

            # Error, line did not match any expected regex
            self._exception_callback(
                RecoverableSampleException('Unknown data found in line %s:%s' % (number, line)))
