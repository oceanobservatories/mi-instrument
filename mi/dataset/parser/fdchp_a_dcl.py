"""
@package mi.dataset.parser
@file mi/dataset/parser/fdchp_a_dcl.py
@author Emily Hahn
@brief A parser for the fdchp series a instrument through a DCL
"""


import re

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException, UnexpectedDataException
from mi.core.instrument.dataset_data_particle import DataParticle

from mi.dataset.dataset_parser import SimpleParser
log = get_logger()

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

# the common regexes were not used here due to desire to have one group containing the
# full date time rather than each number in a group
DATE_TIME_REGEX = r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})'

INSTRUMENT_STARTED_REGEX = r'Instrument Started'
INSTRUMENT_STARTED_MATCHER = re.compile(INSTRUMENT_STARTED_REGEX)

LOG_START_REGEX = DATE_TIME_REGEX + r' \[fdchp:DLOGP\d+\]:,*(.*)'
LOG_START_MATCHER = re.compile(LOG_START_REGEX)

INSTRUMENT_START_LOG_REGEX = LOG_START_REGEX + INSTRUMENT_STARTED_REGEX
INSTRUMENT_START_LOG_MATCHER = re.compile(INSTRUMENT_START_LOG_REGEX)

# match the start of the data line
DATA_START_REGEX = DATE_TIME_REGEX + r' FLUXDATA '
DATA_START_MATCHER = re.compile(DATA_START_REGEX)

# number of chars starting a data line through the end of 'FLUXDATA '
START_N_CHARS = 33

# the expected number of comma separated values in the data line
N_FIELDS = 66


class DataParticleType(BaseEnum):
    TELEMETERED = 'fdchp_a_dcl_instrument'
    RECOVERED = 'fdchp_a_dcl_instrument_recovered'


class FdchpADclCommonParticle(DataParticle):

    # dictionary for unpacking float fields which map directly to a parameters (string -> float)
    UNPACK_DICT = {
        # start at index 2 since stored and dcl timestamp are first
        'time_datacollection': 2,
        'v_num_datacollection': 3,
        # index 4 is a string
        'wind_u_avg': 5,
        'wind_v_avg': 6,
        'wind_w_avg': 7,
        'speed_of_sound_avg': 8,
        'wind_u_std': 9,
        'wind_v_std': 10,
        'wind_w_std': 11,
        'speed_of_sound_std': 12,
        'wind_u_max': 13,
        'wind_v_max': 14,
        'wind_w_max': 15,
        'speed_of_sound_max': 16,
        'wind_u_min': 17,
        'wind_v_min': 18,
        'wind_w_min': 19,
        'speed_of_sound_min': 20,
        'x_accel': 21,
        'y_accel': 22,
        'z_accel': 23,
        'x_accel_std': 24,
        'y_accel_std': 25,
        'z_accel_std': 26,
        'x_accel_max': 27,
        'y_accel_max': 28,
        'z_accel_max': 29,
        'x_accel_min': 30,
        'y_accel_min': 31,
        'z_accel_min': 32,
        'x_ang_rate_avg': 33,
        'y_ang_rate_avg': 34,
        'z_ang_rate_avg': 35,
        'x_ang_rate_std': 36,
        'y_ang_rate_std': 37,
        'z_ang_rate_std': 38,
        'x_ang_rate_max': 39,
        'y_ang_rate_max': 40,
        'z_ang_rate_max': 41,
        'x_ang_rate_min': 42,
        'y_ang_rate_min': 43,
        'z_ang_rate_min': 44,
        'heading': 45,
        'pitch': 46,
        'roll': 47,
        'heading_std': 48,
        'pitch_std': 49,
        'roll_std': 50,
        'heading_max': 51,
        'pitch_max': 52,
        'roll_max': 53,
        'heading_min': 54,
        'pitch_min': 55,
        'roll_min': 56,
        'u_corr': 57,
        'v_corr': 58,
        'w_corr': 59,
        'u_corr_std': 60,
        'v_corr_std': 61,
        'w_corr_std': 62,
        'wind_speed': 63,
        'uw_momentum_flux': 64,
        'vw_momentum_flux': 65,
        'buoyance_flux': 66,
        'eng_wave_motion': 67
    }

    def _build_parsed_values(self):

        parameters = [
            # start timestamp may not have been provided, allow it to be set to None
            self._encode_value('instrument_start_timestamp', self.raw_data[0], self.str_or_none),
            self._encode_value('dcl_controller_timestamp', self.raw_data[1], str),
            self._encode_value('status_datacollection', self.raw_data[4], str)
        ]

        # loop through unpack dictionary and encode floats
        for name, index in self.UNPACK_DICT.iteritems():
            parameters.append(self._encode_value(name, self.raw_data[index], float))

        # use the floating point unix timestamp in time_datacollection as the internal timestamp
        unix_ts = float(self.raw_data[self.UNPACK_DICT.get('time_datacollection')])
        self.set_internal_timestamp(unix_time=unix_ts)

        return parameters

    @staticmethod
    def str_or_none(str_val):
        if str_val is None:
            return None
        return str(str_val)


class FdchpADclTelemeteredParticle(FdchpADclCommonParticle):
    _data_particle_type = DataParticleType.TELEMETERED


class FdchpADclRecoveredParticle(FdchpADclCommonParticle):
    _data_particle_type = DataParticleType.RECOVERED


class FdchpADclParser(SimpleParser):

    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            # this is a telemetered parser
            self.particle_class = FdchpADclTelemeteredParticle
        else:
            # this is a recovered parser
            self.particle_class = FdchpADclRecoveredParticle

        # no config for this parser, pass in empty dict
        super(FdchpADclParser, self).__init__({},
                                              stream_handle,
                                              exception_callback)

    def parse_file(self):
        """
        Entry point into parsing the file, loop over each line and interpret it until the entire file is parsed
        """
        stored_start_timestamp = None

        # read the first line in the file
        line = self._stream_handle.readline()

        while line:
            # data will be at start of line so use match
            data_match = DATA_START_MATCHER.match(line)
            # instrument started may be in middle so use search
            log_match = LOG_START_MATCHER.match(line)

            if data_match:
                # found a data line
                dcl_timestamp = data_match.group(1)

                # Note Bug #10002 found early deployments created data missing commas
                # between some fields.  Replace commas with space and then split to
                # correctly parse files from deployments with either firmware

                fields_set = line[START_N_CHARS:].replace(',', ' ')
                fields = fields_set.split()

                if len(fields) != N_FIELDS:
                    msg = 'Expected %d fields but received %d' % (N_FIELDS, len(fields))
                    log.warn(msg)
                    self._exception_callback(SampleException(msg))
                else:
                    # create an array of the fields to parse in the particle
                    raw_data = [stored_start_timestamp, dcl_timestamp]
                    raw_data.extend(fields)
                    # extract this particle
                    particle = self._extract_sample(self.particle_class, None, raw_data, None)
                    self._record_buffer.append(particle)
                    stored_start_timestamp = None

            elif log_match:
                # pull out whatever text is within the log
                log_contents = log_match.group(2)

                # there are two cases, a log message simply contains the 'Instrument Started' text, or it contains
                # an entire other log message which may contain 'Instrument Started'
                instr_log_match = INSTRUMENT_STARTED_MATCHER.match(log_contents)
                full_log_instr_match = INSTRUMENT_START_LOG_MATCHER.match(log_contents)

                # text other than instrument started is ignored within log messages
                if instr_log_match:
                    # found a line containing a single log instrument started, hold on to it until we get a data line
                    stored_start_timestamp = log_match.group(1)
                elif full_log_instr_match:
                    # found a log within a log, use the inner timestamp associated with the instrument start
                    stored_start_timestamp = full_log_instr_match.group(1)

            else:
                msg = 'Data with unexpected format received: %s' % line
                log.warn(msg)
                self._exception_callback(UnexpectedDataException(msg))

            line = self._stream_handle.readline()
