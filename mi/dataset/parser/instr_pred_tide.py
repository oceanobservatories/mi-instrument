#!/usr/bin/env python

"""
@package mi.dataset.parser.instr_pred_tide
@file mi-dataset/mi/dataset/parser/instr_pred_tide.py
@author Mark Steiner
@brief Parser for predicted tide data files

This file contains code to parse data files and produce data particles
for the instrument_predicted_tide data stream.

The input file is ASCII. Each line in the file contains only a time a corresponding
predicted tide value. Each line produces a single data particle. The data file does
not contain any header or trailer information.

Release notes:

Initial Release
"""

import calendar
import re
import pytz
from datetime import datetime

from mi.dataset.dataset_parser import SimpleParser
from mi.core.exceptions import UnexpectedDataException
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.core.log import get_logger

log = get_logger()

__author__ = 'Mark Steiner'
__license__ = 'Apache 2.0'

# Regex for data in the predicted tide file
# Each data record is in the following format:
# yyyy mm dd hh MM ss -n.nnnn
# where each character indicates one ascii character.
# yyyy = year
# mm = month
# dd = day
# hh = hour
# MM = min
# ss = sec
# -n.nnnn = predicted_tide
# Total of 27 characters and line terminator

DATA_LINE_RE = r'\A(\d{4}) (\d{2}) (\d{2}) (\d{2}) (\d{2}) (\d{2}) ([+\- ]\d\.\d{4,5})\Z'
DATA_LINE_RE_MATCHER = re.compile(DATA_LINE_RE)

# The following are indices into groups() produced by DATA_LINE_RE_MATCHER
MATCH_GRP_TS_YEAR = 1
MATCH_GRP_TS_MONTH = 2
MATCH_GRP_TS_DAY = 3
MATCH_GRP_TS_HOUR = 4
MATCH_GRP_TS_MIN = 5
MATCH_GRP_TS_SEC = 6
MATCH_GRP_PRED_TIDE = 7


DATA_PARTICLE_TYPE = "instrument_predicted_tide"


class InstrPredictedTideDataParticle(DataParticle):
    """
    Class for generating the instrumant_predicted_tide data particle.
    """
    _data_particle_type = DATA_PARTICLE_TYPE

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(InstrPredictedTideDataParticle, self).__init__(raw_data,
                                                             port_timestamp,
                                                             internal_timestamp,
                                                             preferred_timestamp,
                                                             quality_flag,
                                                             new_sequence)

        self.set_internal_timestamp(unix_time=self.parse_internal_timestamp())

    def parse_internal_timestamp(self):
        dt = datetime(int(self.raw_data.group(MATCH_GRP_TS_YEAR)),
                      int(self.raw_data.group(MATCH_GRP_TS_MONTH)),
                      int(self.raw_data.group(MATCH_GRP_TS_DAY)),
                      int(self.raw_data.group(MATCH_GRP_TS_HOUR)),
                      int(self.raw_data.group(MATCH_GRP_TS_MIN)),
                      int(self.raw_data.group(MATCH_GRP_TS_SEC)),
                      tzinfo=pytz.UTC)
        return calendar.timegm(dt.timetuple())

    def _build_parsed_values(self):
        """
        Append the predicted tide value for the particle
        """
        data_list = []
        data_list.append(self._encode_value('predicted_tide',
                                            round(float(self.raw_data.group(MATCH_GRP_PRED_TIDE)), 4), float))
        return data_list


class InstrPredictedTideParser(SimpleParser):
    """
    Parser for instrument_predicted_tide data.
    """
    def parse_file(self):
        line_counter = 0
        errored_lines = []
        for line in self._stream_handle:
            line = line.strip()
            line_counter += 1

            # skip blank lines
            if not line:
                continue

            # If there is a match in the line, generate the data particle object
            match = DATA_LINE_RE_MATCHER.match(line)
            if match is not None:
                particle = self._extract_sample(InstrPredictedTideDataParticle, None, match)
                if particle is not None:
                    self._record_buffer.append(particle)
            else:
                errored_lines.append(line_counter)

        if errored_lines:
            # List only the first 10 errored lines to prevent spamming the logs
            errored_line_str = str(errored_lines[:10]) if len(errored_lines) < 11 \
                else str(errored_lines[:10]).strip(']') + ", ...]"
            error_message = 'Unknown data found in %d lines. Line numbers: %s' \
                            % (len(errored_lines), errored_line_str)
            log.warn(error_message)
            self._exception_callback(UnexpectedDataException(error_message))
