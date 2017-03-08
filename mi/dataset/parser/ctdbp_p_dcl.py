#!/usr/bin/env python

"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/ctdbp_p_dcl.py
@author Jeff Roy
@brief Parser for the ctdbp_p_dcl, dosta_abcdjm_ctdbp_p_dcl
 and flord_g_ctdbp_p_dcl dataset drivers

This file contains code for the CTDBP P DCL Common parser and code to produce data particles.

The input file is ASCII.
The record types are separated by a newline.
Metadata records: # [text] more text newline.
Instrument records: sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

import re
from datetime import datetime
import calendar

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.core.exceptions import \
    RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.common_regexes import \
    ANY_CHARS_REGEX, \
    FLOAT_REGEX, \
    INT_REGEX, \
    END_OF_LINE_REGEX

# Basic patterns
COMMA = ','                          # simple comma
HASH = '#'                           # hash symbol
START_GROUP = '('                    # match group start
END_GROUP = ')'                      # match group end

SERIAL_REGEX = START_GROUP + '?P<serial_number>' + INT_REGEX + END_GROUP                  # ctdbp
TEMP_REGEX = START_GROUP + '?P<temp>' + FLOAT_REGEX + END_GROUP                           # ctdbp
CONDUCTIVITY_REGEX = START_GROUP + '?P<conductivity>' + FLOAT_REGEX + END_GROUP           # ctdbp
PRESSURE_REGEX = START_GROUP + '?P<pressure>' + FLOAT_REGEX + END_GROUP                   # ctdbp
CALPHASE_REGEX = START_GROUP + '?P<oxy_calphase_volts>' + FLOAT_REGEX + END_GROUP         # dosta
OXY_TEMP_REGEX = START_GROUP + '?P<oxy_temp_volts>' + FLOAT_REGEX + END_GROUP             # dosta
CHLOR_VOLTS_REGEX = START_GROUP + '?P<raw_signal_chl_volts>' + FLOAT_REGEX + END_GROUP    # flord
BETA_VOLTS_REGEX = START_GROUP + '?P<raw_signal_beta_volts>' + FLOAT_REGEX + END_GROUP    # flord


# DateTimeStr:   DD Mon YYYY HH:MM:SS
DATE_TIME_STR_REGEX = r'(?P<date_time_string>\d{2}\D{3}\d{4}\d{2}:\d{2}:\d{2})'  # named group = date_time_string

DATE_TIME_STR_FORMAT = '%d%b%Y%H:%M:%S'  # used to to get time tuple from string

# Metadata record:
STATUS_REGEX = HASH + ANY_CHARS_REGEX + END_OF_LINE_REGEX
STATUS_MATCHER = re.compile(STATUS_REGEX)

# empty lines exist in all sample files, suppress warning due to empty line
EMPTY_MATCHER = re.compile(END_OF_LINE_REGEX)

# match a single line uncorrected instrument record
DATA_REGEX = SERIAL_REGEX + COMMA          # dcl timestamp, named group = timestamp
DATA_REGEX += TEMP_REGEX + COMMA           # named group = temp
DATA_REGEX += CONDUCTIVITY_REGEX + COMMA   # named group = conductivity
DATA_REGEX += PRESSURE_REGEX + COMMA       # named group = pressure
DATA_REGEX += CALPHASE_REGEX + COMMA       # Volt0 calphase (DOSTA)
DATA_REGEX += OXY_TEMP_REGEX + COMMA       # Volt1 temp (DOSTA)
DATA_REGEX += CHLOR_VOLTS_REGEX + COMMA    # Volt2 Florescence (FLORD)
DATA_REGEX += BETA_VOLTS_REGEX + COMMA     # Volt3 Back Scatter (FLORD)
DATA_REGEX += DATE_TIME_STR_REGEX          # Instrument timestamp
DATA_MATCHER = re.compile(DATA_REGEX)

# This table is used in the generation of the CTDBP data particle.
# Column 1 - particle parameter name & match group name
# Column 2 - data encoding function (conversion required - int, float, etc)
CTDBP_DATA_PARTICLE_MAP = [
    ('serial_number', str),
    ('temp', float),
    ('conductivity', float),
    ('pressure', float),
    ('date_time_string', str)
]

# This table is used in the generation of the DOSTA data particle.
# Column 1 - particle parameter name & match group name
# Column 2 - data encoding function (conversion required - int, float, etc)
DOSTA_DATA_PARTICLE_MAP = [
    ('oxy_calphase_volts', float),
    ('oxy_temp_volts', float),
    ('date_time_string', str)
]

# This table is used in the generation of the FLORD data particle.
# Column 1 - particle parameter name & match group name
# Column 2 - data encoding function (conversion required - int, float, etc)
FLORD_DATA_PARTICLE_MAP = [
    ('raw_signal_chl_volts', float),
    ('raw_signal_beta_volts', float),
    ('date_time_string', str)
]


class DataParticleType(BaseEnum):
    CTDBP_TELEMETERED = 'ctdbp_p_dcl_instrument'
    CTDBP_RECOVERED = 'ctdbp_p_dcl_instrument_recovered'
    DOSTA_TELEMETERED = 'dosta_abcdjm_ctdbp_p_dcl_instrument'
    DOSTA_RECOVERED = 'dosta_abcdjm_ctdbp_p_dcl_instrument_recovered'
    FLORD_TELEMETERED = 'flord_g_ctdbp_p_dcl_instrument'
    FLORD_RECOVERED = 'flord_g_ctdbp_p_dcl_instrument_recovered'


class CtdbpPDclCommonDataParticle(DataParticle):
    """
    Class for parsing data from the CTDBP P data set
    """
    _data_particle_map = None

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CtdbpPDclCommonDataParticle, self).__init__(raw_data,
                                                          port_timestamp,
                                                          internal_timestamp,
                                                          preferred_timestamp,
                                                          quality_flag,
                                                          new_sequence)

        # The particle timestamp is from the date time string.
        timestamp_str = self.raw_data.group('date_time_string')
        dt = datetime.strptime(timestamp_str, DATE_TIME_STR_FORMAT)
        timestamp = calendar.timegm(dt.timetuple())

        self.set_internal_timestamp(unix_time=timestamp)

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """       

        return [self._encode_value(name, self.raw_data.group(name), function)
                for name, function in self._data_particle_map]


class CtdbpPDclRecoveredDataParticle(CtdbpPDclCommonDataParticle):
    """
    Class for generating CTDBP Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.CTDBP_RECOVERED
    _data_particle_map = CTDBP_DATA_PARTICLE_MAP


class CtdbpPDclTelemeteredDataParticle(CtdbpPDclCommonDataParticle):
    """
    Class for generating CTDBP Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.CTDBP_TELEMETERED
    _data_particle_map = CTDBP_DATA_PARTICLE_MAP


class DostaAbcdjmCtdbpPDclRecoveredDataParticle(CtdbpPDclCommonDataParticle):
    """
    Class for generating DOSTA Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.DOSTA_RECOVERED
    _data_particle_map = DOSTA_DATA_PARTICLE_MAP


class DostaAbcdjmCtdbpPDclTelemeteredDataParticle(CtdbpPDclCommonDataParticle):
    """
    Class for generating DOSTA Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.DOSTA_TELEMETERED
    _data_particle_map = DOSTA_DATA_PARTICLE_MAP


class FlordGCtdbpPDclRecoveredDataParticle(CtdbpPDclCommonDataParticle):
    """
    Class for generating DOSTA Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.FLORD_RECOVERED
    _data_particle_map = FLORD_DATA_PARTICLE_MAP


class FlordGCtdbpPDclTelemeteredDataParticle(CtdbpPDclCommonDataParticle):
    """
    Class for generating DOSTA Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.FLORD_TELEMETERED
    _data_particle_map = FLORD_DATA_PARTICLE_MAP


class CtdbpPDclCommonParser(SimpleParser):
    """
    """

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """
        
        for line in self._stream_handle:
            # first check for a match against the uncorrected pattern
            match = DATA_MATCHER.match(line)

            if match is not None:
                log.debug('record found')

                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     match,
                                                     None)
                self._record_buffer.append(data_particle)

            else:
                test_status = STATUS_MATCHER.match(line)
                # just ignore the status messages
                
                if test_status is None:
                    test_empty = EMPTY_MATCHER.match(line)
                    # empty lines exist in all sample files, suppress warning due to empty line
                    if test_empty is None:
                        # something in the data didn't match a required regex, so raise an exception and press on.
                        message = "Error while decoding parameters in data: [%s]" % line
                        self._exception_callback(RecoverableSampleException(message))