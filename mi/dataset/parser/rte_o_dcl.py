#!/usr/bin/env python

"""
@package mi.dataset.parser.rte_o_stc
@file marine-integrations/mi/dataset/parser/rte_o_stc.py
@author Jeff Roy
@brief Parser for the rte_o_stc dataset driver
Release notes:

Initial Release
"""

import re

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_utc_time
from mi.dataset.parser.common_regexes import \
    UNSIGNED_INT_REGEX, \
    FLOAT_REGEX, \
    TIME_HR_MIN_SEC_MSEC_REGEX, \
    DATE_YYYY_MM_DD_REGEX, \
    SPACE_REGEX


log = get_logger()

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'


# This is an example of the input string
#             2013/11/16 20:46:24.989 Coulombs = 1.1110C,
#             AVG Q_RTE Current = 0.002A, AVG RTE Voltage = 12.02V,
#             AVG Supply Voltage = 12.11V, RTE Hits 0, RTE State = 1

NEW_DATA_REGEX = r'(?P<rte_time>'
NEW_DATA_REGEX += DATE_YYYY_MM_DD_REGEX + SPACE_REGEX + TIME_HR_MIN_SEC_MSEC_REGEX + r') '
NEW_DATA_REGEX += r'Coulombs = (?P<rte_coulombs>' + FLOAT_REGEX + r')C, '
NEW_DATA_REGEX += r'AVG Q_RTE Current = (?P<rte_avg_q_current>' + FLOAT_REGEX + r')A, '
NEW_DATA_REGEX += r'AVG RTE Voltage = (?P<rte_avg_voltage>' + FLOAT_REGEX + r')V, '
NEW_DATA_REGEX += r'AVG Supply Voltage = (?P<rte_avg_supply_voltage>' + FLOAT_REGEX + r')V, '
NEW_DATA_REGEX += r'RTE Hits (?P<rte_hits>' + UNSIGNED_INT_REGEX + r'), '
NEW_DATA_REGEX += r'RTE State = (?P<rte_state>' + UNSIGNED_INT_REGEX + r')'
NEW_DATA_MATCHER = re.compile(NEW_DATA_REGEX)

# This table is used in the generation of the data particle.
# Column 1 - particle parameter name & match group name
# Column 2 - data encoding function (conversion required - int, float, etc)
DATA_PARTICLE_MAP = [
    ('rte_time', str),
    ('rte_coulombs', float),
    ('rte_avg_q_current', float),
    ('rte_avg_voltage', float),
    ('rte_avg_supply_voltage', float),
    ('rte_hits', int),
    ('rte_state', int)
]

METADATA_REGEX = DATE_YYYY_MM_DD_REGEX + SPACE_REGEX + TIME_HR_MIN_SEC_MSEC_REGEX
METADATA_REGEX += r' \[.+DLOGP\d+\].+'
METADATA_MATCHER = re.compile(METADATA_REGEX)


class RteDataParticleType(BaseEnum):
    INSTRUMENT = 'rte_o_dcl_instrument'
    RECOVERED = 'rte_o_dcl_instrument_recovered'


class RteODclParserDataAbstractParticle(DataParticle):
    """
    Abstract Class for parsing data from the rte_o_stc data set
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        # The particle timestamp is the DCL Controller timestamp.
        utc_time = dcl_controller_timestamp_to_utc_time(self.raw_data.group('rte_time'))
        self.set_internal_timestamp(unix_time=utc_time)

        return [self._encode_value(name, self.raw_data.group(name), function)
                for name, function in DATA_PARTICLE_MAP]


class RteODclParserDataParticle(RteODclParserDataAbstractParticle):
    """
    Class for parsing data from the rte_o_stc data set
    """

    _data_particle_type = RteDataParticleType.INSTRUMENT


class RteODclParserRecoveredDataParticle(RteODclParserDataAbstractParticle):
    """
    Class for parsing data from the rte_o_stc data set
    """

    _data_particle_type = RteDataParticleType.RECOVERED


class RteODclParser(SimpleParser):

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        for line in self._stream_handle:

            data_match = NEW_DATA_MATCHER.match(line)
            if data_match:

                # particle-ize the data block received, return the record
                data_particle = self._extract_sample(self._particle_class, None, data_match, None)
                # increment state for this chunk even if we don't get a particle
                self._record_buffer.append(data_particle)

            else:
                # NOTE: Need to check for the metadata line last, since the corrected Endurance
                # record also has the [*] pattern
                test_meta = METADATA_MATCHER.match(line)

                if test_meta is None:
                    # something in the data didn't match a required regex, so raise an exception and press on.
                    message = "Error while decoding parameters in data: [%s]" % line
                    self._exception_callback(RecoverableSampleException(message))





