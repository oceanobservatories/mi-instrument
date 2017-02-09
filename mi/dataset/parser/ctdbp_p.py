#!/usr/bin/env python

"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/ctdbp_p.py
@author Jeff Roy, Rene Gelinas
@brief Parser for the ctdbp_p, dosta_abcdjm_ctdbp_p and flord_g_ctdbp_p dataset drivers.

This file contains code for the CTDBP-P Common parser and code to produce data particles
for the intrument recovered data from the CTDBP-P and the attached DOSTA and FLORD instruments.

The input file has ASCII hexadecimal data.
The record types are separated by a newline.
Metadata records: * [text] more text newline.
Instrument records: sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release
"""

import re
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser import utilities
from mi.dataset.parser.common_regexes import ANY_CHARS_REGEX, ASCII_HEX_CHAR_REGEX, END_OF_LINE_REGEX

__author__ = 'Rene Gelinas'
__license__ = 'Apache 2.0'

# Basic patterns

# Regex for identifying a header line
START_HEADER = r'\*'
HEADER_PATTERN = START_HEADER               # Header data starts with '*'
HEADER_PATTERN += ANY_CHARS_REGEX           # followed by text
HEADER_PATTERN += END_OF_LINE_REGEX         # followed by newline
HEADER_MATCHER = re.compile(HEADER_PATTERN)

# Empty string
EMPTY_MATCHER = re.compile(END_OF_LINE_REGEX)

# This list of strings corresponds to the particle parameter names
# for each of the instruments, i.e., CTDBP-P, DOSTA, FLORD.
# It is used by build_parsed_values.  Named groups in regex must match.
CTDBP_DATA_PARTICLE_MAP = ['temperature', 'conductivity', 'pressure', 'pressure_temp', 'ctd_time']
DOSTA_DATA_PARTICLE_MAP = ['oxy_calphase', 'oxy_temp', 'ctd_time']
FLORD_DATA_PARTICLE_MAP = ['raw_signal_chl', 'raw_signal_beta', 'ctd_time']

# Regex for instrument recovered data from the CTCBP-P.
# Each data record is in the following format:
# ttttttccccccppppppvvvvvvvvvvvvvvvvvvvvssssssss
# where each character indicates one hex ascii character.
# First 6 chars: tttttt = Temperature A/D counts
# Next 6 chars: cccccc = Conductivity A/D counts
# Next 6 chars: pppppp = pressure A/D counts
# Next 4 chars: vvvv = volt0: Temperature Compensation A/D counts
# Next 4 chars: vvvv = volt1: DOSTA-Calibrated Phase Difference A/D counts
# Next 4 chars: vvvv = volt2: DOSTA-Oxygen Sensor Temperature A/D counts
# Next 4 chars: vvvv = volt3: FLORD-Chlorophyll-a Fluorescence A/D counts
# Next 4 chars: vvvv = volt3: FLORD-Optical Backscatter A/D counts
# Last 8 chars: ssssssss = seconds since January 1, 2000
# Total of 46 hex characters and line terminator

CTDBP_P_DATA_REGEX = r'(?P<temperature>' + ASCII_HEX_CHAR_REGEX + '{6})'
CTDBP_P_DATA_REGEX += r'(?P<conductivity>' + ASCII_HEX_CHAR_REGEX + '{6})'
CTDBP_P_DATA_REGEX += r'(?P<pressure>' + ASCII_HEX_CHAR_REGEX + '{6})'
CTDBP_P_DATA_REGEX += r'(?P<pressure_temp>' + ASCII_HEX_CHAR_REGEX + '{4})'
CTDBP_P_DATA_REGEX += r'(?P<oxy_calphase>' + ASCII_HEX_CHAR_REGEX + '{4})'
CTDBP_P_DATA_REGEX += r'(?P<oxy_temp>' + ASCII_HEX_CHAR_REGEX + '{4})'
CTDBP_P_DATA_REGEX += r'(?P<raw_signal_chl>' + ASCII_HEX_CHAR_REGEX + '{4})'
CTDBP_P_DATA_REGEX += r'(?P<raw_signal_beta>' + ASCII_HEX_CHAR_REGEX + '{4})'
CTDBP_P_DATA_REGEX += r'(?P<ctd_time>' + ASCII_HEX_CHAR_REGEX + '{8})'
CTDBP_P_DATA_REGEX += END_OF_LINE_REGEX
CTDBP_P_DATA_MATCHER = re.compile(CTDBP_P_DATA_REGEX, re.VERBOSE)


class DataParticleType(BaseEnum):
    CTDBP_RECOVERED = 'ctdbp_cdef_instrument_recovered'  # Reusing cdef stream. It should be renamed.
    DOSTA_RECOVERED = 'dosta_abcdjm_ctdbp_p_instrument_recovered'
    FLORD_RECOVERED = 'flord_g_ctdbp_p_instrument_recovered'


class CtdbpPCommonDataParticle(DataParticle):
    """
    Class for generating an instrument recovered data particle for either the CTDBP-P,
    or the attched instruments, the DOSTA and the FLORD.
    """
    _data_particle_map = None

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CtdbpPCommonDataParticle, self).__init__(raw_data,
                                                       port_timestamp,
                                                       internal_timestamp,
                                                       preferred_timestamp,
                                                       quality_flag,
                                                       new_sequence)

        # Calculate the internal timestamp.  CTD Time is sceonds from 1/1/2000.
        # Internal timestamp is seconds from 1/1/1900
        ctd_time = int(self.raw_data.group('ctd_time'), 16)
        ntp_time = utilities.time_2000_to_ntp_time(ctd_time)
        self.set_internal_timestamp(timestamp=ntp_time)

    def _build_parsed_values(self):
        """
        Take recovered Hex raw data and extract different fields, converting Hex to Integer values based on the
        particle map.
        @throws SampleException If there is a problem with sample creation
        """

        return [self._encode_value(name, self.raw_data.group(name), lambda x: int(x, 16))
                for name in self._data_particle_map]


class CtdbpPRecoveredDataParticle(CtdbpPCommonDataParticle):
    """
    Class for generating CTDBP Data Particles from instrument Recovered data.
    """
    _data_particle_type = DataParticleType.CTDBP_RECOVERED
    _data_particle_map = CTDBP_DATA_PARTICLE_MAP


class DostaAbcdjmCtdbpPRecoveredDataParticle(CtdbpPCommonDataParticle):
    """
    Class for generating DOSTA Data Particles from instrument Recovered data.
    """
    _data_particle_type = DataParticleType.DOSTA_RECOVERED
    _data_particle_map = DOSTA_DATA_PARTICLE_MAP


class FlordGCtdbpPRecoveredDataParticle(CtdbpPCommonDataParticle):
    """
    Class for generating DOSTA Data Particles from instrument Recovered data.
    """
    _data_particle_type = DataParticleType.FLORD_RECOVERED
    _data_particle_map = FLORD_DATA_PARTICLE_MAP


class CtdbpPCommonParser(SimpleParser):
    """
    """

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        for line in self._stream_handle:
            # first check for a match against the uncorrected pattern
            header_match = HEADER_MATCHER.match(line)
            empty_match = EMPTY_MATCHER.match(line)
            if header_match is None and empty_match is None:
                data_match = CTDBP_P_DATA_MATCHER.match(line)
                if data_match is not None:
                    data_particle = self._extract_sample(self._particle_class,
                                                         None,
                                                         data_match,
                                                         None)
                    self._record_buffer.append(data_particle)
                else:
                    # something in the data didn't match a required regex, so raise an exception and press on.
                    message = "Error while decoding parameters in data: [%s]" % line
                    self._exception_callback(RecoverableSampleException(message))
