# I think I will use this one as a normal and imm parser, since apparently the only difference is the addition of a dcl timestamp on the non-imm.
# I can make 4 different data particles: telem, recovered, dcl_telem, dcl_recovered.
# !/usr/bin/env python

"""
@package mi.dataset.parser.phsen_gh
@file mi-dataset/mi/dataset/parser/phsen_gh.py
@author Samuel Dahlberg
@brief Parser for the phsen_gh dataset driver.

This file contains code for the phsen_gh parser and code to produce data particles
for the instrument data from the phsen_gh, handling both dcl and non dcl data configurations. Data files are ascii.
"""

import re
from datetime import datetime
from mi.core.common import BaseEnum
from mi.dataset.dataset_parser import SimpleParser
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logger, get_logging_metaclass
from mi.dataset.parser.utilities import dcl_time_to_ntp

log = get_logger()

INTEGER = r'(?:[+-]?[0-9]+)'
NEWLINE = r'(?:\r\n|\n)?'
FLOAT = r'(?:[+-]?\d+.\d+[Ee]?[+-]?\d*)'  # includes scientific notation
DCL_TIMESTAMP = r'(?P<DCLTime>\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}.\d{3})'

# The difference in phsen output from DCL to non-DCL is the lack of a DCL timestamp in the latter's data
DATA_PATTERN = (
        r'(?:' + DCL_TIMESTAMP + r'\s+)?' +  # DCL Time-Stamp (optional)
        r'DSPHOX(?P<Serial>\d{5}),' + r'\s*' +  # Serial number
        r'(?P<InstTime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}),' + r'\s*' +  # Instrument clock
        r'(?P<SampleNumber>' + INTEGER + r'),\s*' +  # Sample Number
        r'(?P<ErrorCode>' + INTEGER + r'),\s*' +  # Error code
        r'(?P<Temp>' + FLOAT + r'),\s*' +  # Temperature (degC)
        r'(?P<PH>' + FLOAT + r'),\s*' +  # pH
        r'(?P<Volts>' + FLOAT + r'),\s*' +  # external reference electrode (volts)
        r'(?P<Pressure>' + FLOAT + r'),\s*' +  # pressure (dbar)
        r'(?P<Salinity>' + FLOAT + r'),\s*' +  # salinity (psu)
        r'(?P<Cond>' + FLOAT + r'),\s*' +  # conductivity (mS/cm)
        r'(?P<Oxy>' + FLOAT + r'),\s*' +  # oxygen (mL/L)
        r'(?P<Humidity>' + FLOAT + r'),\s*' +  # internal relative humidity (%)
        r'(?P<IntTemp>' + FLOAT + r')' + NEWLINE  # internal temperature (degC)
)

DATA_REGEX = re.compile(DATA_PATTERN, re.DOTALL)


class DataParticleType(BaseEnum):
    PHSEN_GH = 'phsen_gh_instrument'
    __metaclass__ = get_logging_metaclass(log_level='trace')


class PhsenGhDataParticle(DataParticle):
    """
    Class for generating the phsen gh instrument particle.
    """

    _data_particle_type = DataParticleType.PHSEN_GH

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]


class PhsenGHParticleKey(BaseEnum):
    """
    Class that defines the fields that need to be extracted for the data particle.
    """

    SERIAL_NUMBER = 'serial_number'
    SAMPLE_NUMBER = 'sample_number'
    ERROR_FLAG = 'error_flag'
    TEMPERATURE = 'temperature'
    SEAWATER_PH = 'seawater_ph'
    EXT_REFERENCE = 'external_reference'
    PRESSURE = 'pressure'
    SALINITY = 'salinity'
    CONDUCTIVITY = 'conductivity'
    OXY_CONC = 'dissolved_oxygen'
    INTERNAL_HUMIDITY = 'internal_humidity'
    INTERNAL_TEMP = 'internal_temperature'


class PhsenGhParser(SimpleParser):
    """
    parser for the phsen_gh, which will cover both phsen_gh and phsen_gh_dcl.
    Will parse both regular phsen data and dcl formatted phsen data.
    """

    def parse_record(self, match):
        """
        Parse a data record from the phsen data file, storing the instrument data
        """

        phsen_particle_data = {
            PhsenGHParticleKey.SERIAL_NUMBER: int(match.group('Serial')),
            PhsenGHParticleKey.SAMPLE_NUMBER: int(match.group('SampleNumber')),
            PhsenGHParticleKey.ERROR_FLAG: int(match.group('ErrorCode')),
            PhsenGHParticleKey.TEMPERATURE: float(match.group('Temp')),
            PhsenGHParticleKey.SEAWATER_PH: float(match.group('PH')),
            PhsenGHParticleKey.EXT_REFERENCE: float(match.group('Volts')),
            PhsenGHParticleKey.PRESSURE: float(match.group('Pressure')),
            PhsenGHParticleKey.SALINITY: float(match.group('Salinity')),
            PhsenGHParticleKey.CONDUCTIVITY: float(match.group('Cond')),
            PhsenGHParticleKey.OXY_CONC: float(match.group('Oxy')),
            PhsenGHParticleKey.INTERNAL_HUMIDITY: float(match.group('Humidity')),
            PhsenGHParticleKey.INTERNAL_TEMP: float(match.group('IntTemp'))
        }

        return phsen_particle_data

    def parse_file(self):
        """
        Parse the phsen file.
        Read file line by line.
        @return: dictionary of data values with the particle names as keys
        """

        for number, line in enumerate(self._stream_handle, start=0):

            # Is line a data output?
            match = DATA_REGEX.match(line)
            if match is not None:
                phsen_particle_data = self.parse_record(match)
                if phsen_particle_data is None:
                    log.error('Erroneous data found in line %s: %s', number, line)
                    continue

                # For port timestamp, we will look to see if the dcl group exists in this data regex
                if match.groupdict().get('DCLTime'):
                    dcl_timestamp = str(match.groupdict()['DCLTime'])
                    port_timestamp = dcl_time_to_ntp(dcl_timestamp)
                    preferred_timestamp = DataParticleKey.PORT_TIMESTAMP
                else:
                    port_timestamp = None
                    preferred_timestamp = DataParticleKey.INTERNAL_TIMESTAMP

                instrument_timestamp = datetime.strptime(match.groupdict()['InstTime'], '%Y-%m-%dT%H:%M:%S')
                internal_timestamp = (instrument_timestamp - datetime(1900, 1, 1)).total_seconds()

                particle = self._extract_sample(self._particle_class, None, phsen_particle_data, port_timestamp,
                                                internal_timestamp, preferred_timestamp)
                if particle is not None:
                    self._record_buffer.append(particle)
                    log.trace('Parsed particle: %s' % particle.generate_dict())
                continue
