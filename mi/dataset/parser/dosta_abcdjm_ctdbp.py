#!/usr/bin/env python

"""
@package mi.dataset.parser.dosta_abcdjm_ctdbp
@file mi-dataset/mi/dataset/parser/dosta_abcdjm_ctdbp.py
@author Jeff Roy
@brief Parser for the dosta_abcdjm_ctdbp dataset driver

This file contains code for the dosta_abcdjm_ctdbp parser and code to produce data
particles. This parser is for recovered data only - it produces a single
particle for the data recovered from the instrument.

The input file is ASCII. There are two sections of data contained in the
input file.  The first is a set of header information, and the second is a set
of hex ascii data with one data sample per line in the file. Each line in the
header section starts with a '*'. The header lines are simply ignored.
Each line of sample data produces a single data particle.
Malformed sensor data records and all header records produce no particles.

Release notes:

This parser was broken out of the previous dosta_abcdjm_ctdbp_ce

Initial Release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

import calendar
import re

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
from mi.core.exceptions import UnexpectedDataException

from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.common_regexes import \
    END_OF_LINE_REGEX, ANY_CHARS_REGEX, ASCII_HEX_CHAR_REGEX

# Basic patterns

# Time tuple corresponding to January 1st, 2000
JAN_1_2000 = (2000, 1, 1, 0, 0, 0, 0, 0, 0)

# regex for identifying start of a header line
START_HEADER = r'\*'
# Header data:
HEADER_PATTERN = START_HEADER    # Header data starts with '*'
HEADER_PATTERN += ANY_CHARS_REGEX         # followed by text
HEADER_PATTERN += END_OF_LINE_REGEX         # followed by newline
HEADER_MATCHER = re.compile(HEADER_PATTERN)

# this list of strings corresponds to the particle parameter names
# it is used by build_parsed_values.  Named groups in regex must match.
DATA_PARTICLE_MAP = [
    'oxygen',
    'ctd_time'
]

# Regex for data from the Endurance array
# Each data record is in the following format:
# ttttttccccccppppppvvvvoooooossssssss
# where each character indicates one hex ascii character.
# First 6 chars: tttttt = Temperature A/D counts (CTDBP data omitted from output)
# Next 6 chars: cccccc = Conductivity A/D counts (CTDBP data omitted from output)
# Next 6 chars: pppppp = pressure A/D counts (CTDBP data omitted from output)
# Next 4 chars: vvvv = temperature compensation A/D counts (CTDBP data omitted from output)
# Next 6 chars: oooooo = Dissolved Oxygen in counts 
# Last 8 chars: ssssssss = seconds since January 1, 2000
# Total of 36 hex characters and line terminator

ENDURANCE_DATA_REGEX = r'(?:' + ASCII_HEX_CHAR_REGEX + '{6})'
ENDURANCE_DATA_REGEX += r'(?:' + ASCII_HEX_CHAR_REGEX + '{6})'
ENDURANCE_DATA_REGEX += r'(?:' + ASCII_HEX_CHAR_REGEX + '{4})'
ENDURANCE_DATA_REGEX += r'(?:' + ASCII_HEX_CHAR_REGEX + '{6})'
ENDURANCE_DATA_REGEX += r'(?P<oxygen>' + ASCII_HEX_CHAR_REGEX + '{6})'
ENDURANCE_DATA_REGEX += r'(?P<ctd_time>' + ASCII_HEX_CHAR_REGEX + '{8})' + END_OF_LINE_REGEX

ENDURANCE_DATA_MATCHER = re.compile(ENDURANCE_DATA_REGEX, re.VERBOSE)


class DataParticleType(BaseEnum):
    """
    Class that defines the data particle generated from the dosta_abcdjm_ctdbp recovered data
    """
    SAMPLE = 'dosta_abcdjm_ctdbp_instrument_recovered'


class DostaAbcdjmCtdbpInstrumentDataParticle(DataParticle):
    """
    Class for generating the dosta_abcdjm_ctdbp_instrument_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(DostaAbcdjmCtdbpInstrumentDataParticle, self).__init__(raw_data,
                                                                     port_timestamp,
                                                                     internal_timestamp,
                                                                     preferred_timestamp,
                                                                     quality_flag,
                                                                     new_sequence)

        # the data contains seconds since Jan 1, 2000. Need the number of seconds before that
        seconds_till_jan_1_2000 = calendar.timegm(JAN_1_2000)

        # calculate the internal timestamp
        ctd_time = int(self.raw_data.group('ctd_time'), 16)
        elapsed_seconds = seconds_till_jan_1_2000 + ctd_time
        self.set_internal_timestamp(unix_time=elapsed_seconds)

    def _build_parsed_values(self):
        """
        Take recovered Hex raw data and extract different fields, converting Hex to Integer values.
        @throws SampleException If there is a problem with sample creation
        """

        return [self._encode_value(name, self.raw_data.group(name), lambda x: int(x, 16))
                for name in DATA_PARTICLE_MAP]


class DostaAbcdjmCtdbpParser(SimpleParser):
    """
    Parser for dosta_abcdjm_ctdbp data.
    """

    def __init__(self, stream_handle, exception_callback):
        super(DostaAbcdjmCtdbpParser, self).__init__({},
                                                     stream_handle,
                                                     exception_callback)

    def parse_file(self):
        for line in self._stream_handle:
            # If this is a valid sensor data record,
            # use the extracted fields to generate a particle.

            # check for match from Endurance
            match = ENDURANCE_DATA_MATCHER.match(line)

            if match is not None:
                particle = self._extract_sample(DostaAbcdjmCtdbpInstrumentDataParticle, None, match, None)

                if particle is not None:
                    self._record_buffer.append(particle)

            # It's not a sensor data record, see if it's a header record.
            else:

                # If it's a valid header record, ignore it.
                # Otherwise generate warning for unknown data.

                header_match = HEADER_MATCHER.match(line)

                log.debug('Header match: %s', str(header_match))
                if header_match is None:
                    warning_message = 'Unknown data found in chunk %s' % line
                    log.warn(warning_message)
                    self._exception_callback(UnexpectedDataException(warning_message))
