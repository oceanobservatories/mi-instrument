"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/hyd_o_dcl.py
@author Emily Hahn
@brief Parser for the hydrogen series o instrument through a dcl
"""
__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re

from mi.core.log import get_logger
log = get_logger()
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import SampleException

from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.common_regexes import FLOAT_REGEX
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_utc_time

DCL_TIMESTAMP_REGEX = r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}.\d{3})'

# regex to match the data
DATA_LINE_REGEX = DCL_TIMESTAMP_REGEX + ' \*(' + FLOAT_REGEX + ') (' + FLOAT_REGEX + ') %'
DATA_LINE_MATCHER = re.compile(DATA_LINE_REGEX)

# regex to match the dcl logger line which is ignored
IGNORE_LINE_REGEX = DCL_TIMESTAMP_REGEX + ' \[hyd\d*:DLOGP\d+\]:.*'
IGNORE_LINE_MATCHER = re.compile(IGNORE_LINE_REGEX)

DCL_TIMESTAMP_GROUP = 1
# map for unpacking the particle
PARTICLE_MAP = [
    ('dcl_controller_timestamp', DCL_TIMESTAMP_GROUP, str),
    ('hyd_raw', 2, float),
    ('hyd_percent', 3, float)
]


class HydODclCommonDataParticle(DataParticle):
    def _build_parsed_values(self):
        # the timestamp comes from the DCL logger timestamp, parse the string into a datetime
        utc_time = dcl_controller_timestamp_to_utc_time(self.raw_data.group(DCL_TIMESTAMP_GROUP))
        self.set_internal_timestamp(unix_time=utc_time)

        return [self._encode_value(name, self.raw_data.group(idx), function)
                for name, idx, function in PARTICLE_MAP]


class HydODclTelemeteredDataParticle(HydODclCommonDataParticle):
    _data_particle_type = 'hyd_o_dcl_instrument'


class HydODclRecoveredDataParticle(HydODclCommonDataParticle):
    _data_particle_type = 'hyd_o_dcl_instrument_recovered'


class HydODclParser(SimpleParser):
    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            # this is a telemetered parser
            self.particle_class = HydODclTelemeteredDataParticle

        else:
            # this is a recovered parser
            self.particle_class = HydODclRecoveredDataParticle

        # no config for this parser, pass in empty dict
        super(HydODclParser, self).__init__({},
                                            stream_handle,
                                            exception_callback)

    def parse_file(self):
        """
        The main parsing function which loops over each line in the file and extracts particles if the correct
        format is found.
        """
        # read the first line in the file
        line = self._stream_handle.readline()

        while line:
            # check for a data line or a dcl logger line we specifically ignore
            data_match = DATA_LINE_MATCHER.match(line)
            ignore_match = IGNORE_LINE_MATCHER.match(line)

            if data_match:
                # found a data line, extract this particle
                particle = self._extract_sample(self.particle_class, None, data_match, None)
                self._record_buffer.append(particle)
            elif not ignore_match:
                # we found a line with an unknown format, call an exception
                error_message = 'Found line with unknown format %s' % line
                log.warn(error_message)
                self._exception_callback(SampleException(error_message))

            # read the next line
            line = self._stream_handle.readline()