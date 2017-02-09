#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/cspp_eng_dcl.py
@author Jeff Roy
@brief Parser for CSPP Engineering data collected by acoustic modem
Release notes:

initial release
"""

import re
import ntplib

from mi.core.log import get_logger
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.common import BaseEnum
from mi.core.exceptions import RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, ANY_CHARS_REGEX, \
    ASCII_HEX_CHAR_REGEX, FLOAT_REGEX, SPACE_REGEX as SPACE
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_ntp_time, formatted_timestamp_utc_time

log = get_logger()

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

MODE_REGEX = r'(?P<mode>(sent|rcvd)):'
TIMESTAMP_REGEX = r'(?P<timestamp>' + DATE_YYYY_MM_DD_REGEX
TIMESTAMP_REGEX += ONE_OR_MORE_WHITESPACE_REGEX
TIMESTAMP_REGEX += TIME_HR_MIN_SEC_MSEC_REGEX + ')'
MESSAGE_REGEX = r'(?P<message>' + ANY_CHARS_REGEX + ')'

RECORD_REGEX = TIMESTAMP_REGEX
RECORD_REGEX += ONE_OR_MORE_WHITESPACE_REGEX
RECORD_REGEX += MODE_REGEX
RECORD_REGEX += r'\s*'
RECORD_REGEX += MESSAGE_REGEX
RECORD_REGEX += END_OF_LINE_REGEX

RECORD_MATCHER = re.compile(RECORD_REGEX)

NMEA_REGEX = r'(?P<sentence>\$PWETA'
NMEA_REGEX += ANY_CHARS_REGEX + ')'
NMEA_REGEX += r'\*(?P<checksum>' + ASCII_HEX_CHAR_REGEX + r'{2})'

NMEA_MATCHER = re.compile(NMEA_REGEX)

RANGE_REGEX = r'Range 1 to 2 :' + ONE_OR_MORE_WHITESPACE_REGEX
RANGE_REGEX += r'(?P<range>' + FLOAT_REGEX
RANGE_REGEX += r')' + ONE_OR_MORE_WHITESPACE_REGEX + 'm'
RANGE_MATCHER = re.compile(RANGE_REGEX)

DSP_REGEX = r'DSP Bat =' + ONE_OR_MORE_WHITESPACE_REGEX
DSP_REGEX += r'(?P<dsp_bat>' + FLOAT_REGEX + r')V'
DSP_MATCHER = re.compile(DSP_REGEX)

XMIT_REGEX = r'Xmit Bat =' + ONE_OR_MORE_WHITESPACE_REGEX
XMIT_REGEX += r'(?P<dsp_bat>' + FLOAT_REGEX + r')V'
XMIT_MATCHER = re.compile(XMIT_REGEX)

DATE_TIME_FORMAT = "%m/%d/%Y %H:%M:%S"


# encoding functions for partially populated particles.
def float_or_none(float_val):
    if float_val is None:
        return None
    return float(float_val)


def int_or_none(int_val):
    if int_val is None:
        return None
    return int(int_val)


def str_or_none(str_val):
    if str_val is None:
        return None
    return str(str_val)


# Define the particle stream names
class CsppEngDclParticleType(BaseEnum):
    MODEM = 'cspp_eng_dcl_modem'
    DATA = 'cspp_eng_dcl_eng_data'
    SUMMARY = 'cspp_eng_dcl_summary'
    PROFILE = 'cspp_eng_dcl_profile'


class CsppEngDclParticle(DataParticle):

    _param_map = None  # Must be defined in subclass

    def _build_parsed_values(self):

        if self._param_map is None:
            raise NotImplemented('self._param_map not defined')

        return [self._encode_value(name, data, function)
                for (name, function), data in zip(self._param_map, self.raw_data)]


class CsppEngDclModemParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.MODEM

    _param_map = [('cspp_modem_distance', float_or_none),
                  ('cspp_dsp_battery_voltage', float_or_none),
                  ('cspp_transmit_battery_voltage', float_or_none)
                  ]


class CsppEngDclEngDataParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.DATA

    _param_map = [('cspp_date_time', int_or_none),
                  ('cspp_current_state', int_or_none),
                  ('cspp_previous_state', int_or_none),
                  ('cspp_profile_start_time', int_or_none),
                  ('cspp_profile_start_delta', int_or_none),
                  ('cspp_enable_device', str_or_none),
                  ('cspp_enable_state', str_or_none),
                  ('cspp_wave_start_time', int_or_none),
                  ('cspp_wave_height', float_or_none),
                  ('cspp_wave_period', float_or_none),
                  ('cspp_wave_mode', int_or_none),
                  ]


class CsppEngDclSummaryParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.SUMMARY

    _param_map = [('cspp_start_depth', float),
                  ('cspp_end_depth', float),
                  ('cspp_travel_time', float),
                  ('cspp_system_voltage', float),
                  ('cspp_winch_end_position', int),
                  ('cspp_filename', str),
                  ('cspp_file_size', int),
                  ('cspp_file_space', int),
                  ('cspp_num_files', int),
                  ('cspp_profile_number', int),
                  ('cspp_low_voltage', float),
                  ('cspp_low_cell_voltage', float),
                  ('cspp_watts_used', float),
                  ('cspp_low_battery_number', int)
                  ]


class CsppEngDclProfileParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.PROFILE

    _param_map = [('cspp_home_depth', float),
                  ('cspp_start_time', int),
                  ('cspp_setup_rate', float),
                  ('cspp_setup_start_depth', float),
                  ('cspp_setup_end_depth', float),
                  ('cspp_setup_ascent_rate', float),
                  ('cspp_setup_descent_rate', float),
                  ('cspp_num_profiles', int),
                  ('cspp_interval_time', int),
                  ('cspp_start_delay', int),
                  ]


# dictionary mapping expected NMEA commands to number of expected fields and corresponding particle class
NMEA_SENTENCE_MAP = {'DATE': (3, CsppEngDclEngDataParticle),
                     'PFS': (2, CsppEngDclEngDataParticle),
                     'PST': (3, CsppEngDclEngDataParticle),
                     'ENA': (2, CsppEngDclEngDataParticle),
                     'WHE': (5, CsppEngDclEngDataParticle),
                     'SUM': (14, CsppEngDclSummaryParticle),
                     'PRO': (10, CsppEngDclProfileParticle)
                     }


class CsppEngDclParser(SimpleParser):

    @staticmethod
    def calc_checksum(sentence):
        """   Calculates NMEA checksum

        :param sentence:
        :return: string of checksum in upper case hex notation
        """
        checksum = 0
        for c in sentence:
            checksum ^= ord(c)

        return checksum

    def process_date(self, fields):

        date_time_str = fields[0] + SPACE + fields[1]  # concatenate date and time
        date_time_utc = formatted_timestamp_utc_time(date_time_str, DATE_TIME_FORMAT)
        date_time_utc += float(fields[2])*3600  # adjust for timezone (%z format is not supported in Python 2.7)
        date_time_ntp = ntplib.system_to_ntp_time(date_time_utc)
        self._eng_data[0] = date_time_ntp
        return

    def process_start(self, fields):
        date_time_str = fields[1] + SPACE + fields[0]  # concatenate date and time
        date_time_utc = formatted_timestamp_utc_time(date_time_str, DATE_TIME_FORMAT)
        date_time_ntp = ntplib.system_to_ntp_time(date_time_utc)
        self._eng_data[3] = date_time_ntp
        self._eng_data[4] = fields[2]
        return

    def process_wave(self, fields):
        date_time_str = fields[1] + SPACE + fields[0]  # concatenate date and time
        date_time_utc = formatted_timestamp_utc_time(date_time_str, DATE_TIME_FORMAT)
        date_time_ntp = ntplib.system_to_ntp_time(date_time_utc)
        self._eng_data[7] = date_time_ntp
        self._eng_data[8:] = fields[2:]
        return

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        #  initialize data for modem particle
        first_timestamp = None
        date_timestamp = None
        distance = None
        dsp_bat = None
        xmit_bat = None

        #  initialize raw_data for CsppEngDclEngDataParticle
        self._eng_data = [None] * 10

        for line in self._stream_handle:

            data_match = RECORD_MATCHER.match(line)

            if data_match is None:
                message = 'got malformed line %s ' % line
                log.warn(message)
                self._exception_callback(RecoverableSampleException(message))
                continue
            if data_match.group('mode') == 'sent':
                continue  # skip sent messages, go to next line

            timestamp_str = data_match.group('timestamp')
            message = data_match.group('message')

            if first_timestamp is None:
                first_timestamp = timestamp_str  # save the first timestamp for the modem particle

            # save off header information for modem particle
            # modem particle created after processing entire file.
            range_match = RANGE_MATCHER.match(message)
            if range_match:
                distance = range_match.group('range')
                continue  # go to next line
            dsp_match = DSP_MATCHER.match(message)
            if dsp_match:
                dsp_bat = dsp_match.group('dsp_bat')
                continue  # go to next line
            xmit_match = XMIT_MATCHER.match(message)
            if xmit_match:
                xmit_bat = xmit_match.group('dsp_bat')
                continue  # go to next line

            # process NMEA sentences
            nmea_match = NMEA_MATCHER.match(message)
            if nmea_match:
                sentence = nmea_match.group('sentence')
                checksum = int(nmea_match.group('checksum'), 16)  # Convert to integer

                # Note: NMEA checksums typically do not include the $ at the
                # beginning of the sentence but it appears Wetlabs implemented
                # it that way.
                comp_checksum = self.calc_checksum(sentence)

                if comp_checksum == checksum:
                    fields = sentence.split(',')
                    command = fields[5]
                    count = fields[6]

                    sentence_params = NMEA_SENTENCE_MAP.get(command)

                    if sentence_params is None:
                        # skip NMEA sentences we are not looking for
                        log.debug('NMEA sentence skipped %s', line)
                        continue  # go to next line

                    expected_count, particle_class = sentence_params
                    if int(count) != expected_count:
                        message = 'did not get expected number of fields on line %s' % line
                        log.warn(message)
                        self._exception_callback(RecoverableSampleException(message))
                        continue  # go to next line

                    if particle_class == CsppEngDclEngDataParticle:
                        if command == 'DATE':
                            date_timestamp = timestamp_str  # save timestamp from the DATE record
                            self.process_date(fields[7:])
                        elif command == 'PFS':
                            self._eng_data[1:3] = fields[7:9]
                        elif command == 'PST':
                            self.process_start(fields[7:])
                        elif command == 'ENA':
                            self._eng_data[5:7] = fields[7:9]
                        elif command == 'WHE':
                            self.process_wave(fields[7:])

                    else:
                        # Create particle and add to buffer
                        timestamp = dcl_controller_timestamp_to_ntp_time(timestamp_str)
                        data_particle = self._extract_sample(particle_class,
                                                             None,
                                                             fields[7:],
                                                             timestamp)
                        self._record_buffer.append(data_particle)
                else:
                    message = 'checksum failed on line %s' % line
                    log.warn(message)
                    self._exception_callback(RecoverableSampleException(message))

        # end for loop

        # only send modem particle if we have a timestamp
        # and at least one parameter
        if first_timestamp and (distance or dsp_bat or xmit_bat):
            timestamp = dcl_controller_timestamp_to_ntp_time(first_timestamp)
            data_particle = self._extract_sample(CsppEngDclModemParticle,
                                                 None,
                                                 [distance, dsp_bat, xmit_bat],
                                                 timestamp)
            self._record_buffer.append(data_particle)

        if any(self._eng_data):  # Publish CsppEngDclEngDataParticle if we have any data
            if date_timestamp:  # preference is DATE timestamp
                timestamp = dcl_controller_timestamp_to_ntp_time(date_timestamp)
            else:
                timestamp = dcl_controller_timestamp_to_ntp_time(first_timestamp)

            data_particle = self._extract_sample(CsppEngDclEngDataParticle,
                                                 None,
                                                 self._eng_data,
                                                 timestamp)
            self._record_buffer.append(data_particle)

