#!/usr/bin/env python

"""
@package mi.dataset.parser.presf_de_dcl
@file mi-dataset/mi/dataset/parser/presf_de_dcl.py
@author Samuel Dahlberg
@brief Parser for the presf_de_dcl dataset driver.

This file contains code for the PRESF_DE parser and code to produce data particles
for the instrument recovered data from the PRESF_DE instruments.

The input file has ASCII data.
The records are separated by a newline.
Comments: dcl_timestamp [text]: text newline.
Instrument records: dcl_timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.
"""

import re

from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()
from mi.core.common import BaseEnum
from mi.dataset.dataset_parser import SimpleParser
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.parser.utilities import dcl_time_to_ntp

# Common REGEX patterns
DCL_TIMESTAMP_REGEX = r'(\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}.\d{3})'
NEWLINE = r'(?:\r\n|\n)?'
FLOAT = r'([+-]?\d+.\d+[Ee]?[+-]?\d*)'

# Regex pattern for a line with the active column list
COLS_PATTERN = (
        r'(\[\w*:\w*\]:)' +  # DCL logger ID
        r'# Active channels: ' +  # Line prefix
        r'(.+)' +
        NEWLINE
)
COLS_REGEX = re.compile(COLS_PATTERN)

# Regex pattern for a line with a time stamp, unix time value and
# up to 7 channel data values.
DATA_PATTERN = (
        r'(\[\w*:\w*\]:)' +  # DCL logger ID
        DCL_TIMESTAMP_REGEX + r',\s*' +  # PRESF Date and Time
        FLOAT + r',' +  # PRESF Unix time (milliseconds since 1/1/1970)
        r'(.+)' +
        NEWLINE
)
DATA_REGEX = re.compile(DATA_PATTERN, re.DOTALL)


class PresfDeDclParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted for the data particle.
    """

    TEMPERATURE_00 = 'temperature_00',
    PRESSURE_00 = 'pressure_00',
    TEMPERATURE_01 = 'temperature_01',
    SEAPRESSURE_00 = 'seapressure_00',
    DEPTH = 'depth',
    PERIOD_00 = 'period_00',
    PERIOD_01 = 'period_01'


class DataParticleType(BaseEnum):
    PRESF_DE_PARTICLE_TYPE = 'presf_de_instrument'
    __metaclass__ = get_logging_metaclass(log_level='trace')


class PresfDeDataParticle(DataParticle):
    """
    Class for generating the presf_de instrument particle.
    """

    _data_particle_type = DataParticleType.PRESF_DE_PARTICLE_TYPE

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]


class PresfDeDclParser(SimpleParser):
    """
    Presf_de DCL Parser.
    """

    def __init__(self, config, stream_handle, exception_callback):
        """
        Init to set the active channels of the data output to the default expected channels.
        """

        super(PresfDeDclParser, self).__init__(config, stream_handle, exception_callback)

        self.num_channels = 7  # Default: all channels active
        self.active_channels = [
            PresfDeDclParticleKey.TEMPERATURE_00,
            PresfDeDclParticleKey.PRESSURE_00,
            PresfDeDclParticleKey.TEMPERATURE_01,
            PresfDeDclParticleKey.SEAPRESSURE_00,
            PresfDeDclParticleKey.DEPTH,
            PresfDeDclParticleKey.PERIOD_00,
            PresfDeDclParticleKey.PERIOD_01
        ]

    def update_active_channels(self, record):
        """
        Updates the active channel list if new data output format has changed.
        """

        channel_names = record.group(2).strip('\n').split('|')
        active_channels_list = []

        for name in channel_names:
            if name == "temperature_00":
                active_channels_list.append(PresfDeDclParticleKey.TEMPERATURE_00)
                continue
            if name == "pressure_00":
                active_channels_list.append(PresfDeDclParticleKey.PRESSURE_00)
                continue
            if name == "temperature_01":
                active_channels_list.append(PresfDeDclParticleKey.TEMPERATURE_01)
                continue
            if name == "seapressure_00":
                active_channels_list.append(PresfDeDclParticleKey.SEAPRESSURE_00)
                continue
            if name == "depth_00":
                active_channels_list.append(PresfDeDclParticleKey.DEPTH)
                continue
            if name == "period_00":
                active_channels_list.append(PresfDeDclParticleKey.PERIOD_00)
                continue
            if name == "period_01":
                active_channels_list.append(PresfDeDclParticleKey.PERIOD_01)
                continue

        self.active_channels = active_channels_list

    def parse_record(self, record):
        """
        Parse a matched data record from the presf_de log file.
        @param record: MatchObject containing a regex match for presf_de data
        @return: dictionary of values with the particle names as keys
        """

        presf_particle_data = {}

        try:
            # Assign the instrument time as dcl-format string
            instrument_time = str(record.group(2))

            # Remaining data correspond to active channels (default or previously read from log)
            channel_data_list = record.group(4).strip('\n').split(',')
            for channel in range(0, len(channel_data_list)):
                presf_particle_data[self.active_channels[channel]] = float(channel_data_list[channel])

            return presf_particle_data, instrument_time
        except ValueError as er:
            return None, None

    def parse_file(self):
        """
        Parse the presf_de log file.
        Read file line by line. Values are extracted from lines containing data
        @return: dictionary of data values with the particle names as keys
        """

        for number, line in enumerate(self._stream_handle, start=1):

            # Is line an output format update?
            match = COLS_REGEX.match(line)
            if match is not None:
                self.update_active_channels(match)
                self.num_channels = len(self.active_channels)
                continue

            # Is line a data output?
            match = DATA_REGEX.match(line)
            if match is not None:
                presf_particle_data, instrument_timestamp = self.parse_record(match)
                if presf_particle_data is None:
                    log.error('Erroneous data found in line %s: %s', number, line)
                    continue

                internal_timestamp = dcl_time_to_ntp(instrument_timestamp)

                particle = self._extract_sample(PresfDeDataParticle, None, presf_particle_data,
                                                internal_timestamp=internal_timestamp,
                                                preferred_ts=DataParticleKey.INTERNAL_TIMESTAMP)

                if particle is not None:
                    self._record_buffer.append(particle)
                    log.trace('Parsed particle: %s' % particle.generate_dict())
