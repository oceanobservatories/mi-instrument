
"""
@package mi.dataset.parser.presf_abc
@file marine-integrations/mi/dataset/parser/presf_abc.py
@author Christopher Fortin, Jeff Roy, Rene Gelinas
@brief Parser for the presf_abc dataset driver

This file contains code for the presf_abc parsers and code to produce data
particles.  This parser only parses recovered data. There is one parser which
produces two types of data particles. The names of the output particle streams
are unique.

The input file is ASCII and contains five types of records.
The first record type is the header record of ASCII text preceded by '*'.
The next three types of the records are 18 digit hexadecimal values with
specific formats for the logging session data, tide data and wave burst
metadata.  The last record type is 12 digit hexadecimal list of wave burst
data values, two per line.

All records end with the newline regular expression.
Header data records: '*', text
Header End record: '*S>DD'
Logging Session start record: 'FFFFFFFFFBFFFFFFFF'
Logging Session data record: 2-18 digit hexadecimal
Logging Session end record: 'FFFFFFFFFCFFFFFFFF'
Tide Data record: 18 digit hexadecimal
Wave Metadata record: 2-18 digit hexadecimal
Wave Burst records: 12 digit pressure measurement (2 measurements per record)
Wave Burst End record: 'FFFFFFFFFFFFFFFFFF'
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all status records produce no particles.

Release notes:

Initial Release
"""

import re

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser import utilities

from mi.dataset.parser.common_regexes import FLOAT_REGEX, \
    SCIENTIFIC_REGEX, END_OF_LINE_REGEX, ASCII_HEX_CHAR_REGEX, \
    ANY_CHARS_REGEX

from mi.core.log import get_logger
log = get_logger()

__author__ = 'Rene Gelinas'
__license__ = 'Apache 2.0'

# Basic patterns
FLOAT = r'(' + FLOAT_REGEX + ')'              # generic float
SCI_NOTATION = r'(' + SCIENTIFIC_REGEX + ')'  # Generic scientific notation
WHITESPACE = r'(\s*)'                         # any whitespace
HEADER_RECORD_START = r'\*' + WHITESPACE

# Header records - text
HEADER_LINE_REGEX = HEADER_RECORD_START + ANY_CHARS_REGEX
HEADER_LINE_REGEX += END_OF_LINE_REGEX
HEADER_LINE_MATCHER = re.compile(HEADER_LINE_REGEX)

# Logging Session records - 18 digit hexadecimal
SESSION_START_REGEX = r'F{9}BF{8}' + END_OF_LINE_REGEX
SESSION_START_MATCHER = re.compile(SESSION_START_REGEX)

SESSION_TIME_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{8})'
SESSION_TIME_REGEX += r'0{10}'
SESSION_TIME_REGEX += END_OF_LINE_REGEX
SESSION_TIME_MATCHER = re.compile(SESSION_TIME_REGEX)

SESSION_STATUS_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{4})'
SESSION_STATUS_REGEX += r'(' + ASCII_HEX_CHAR_REGEX + '{4})'
SESSION_STATUS_REGEX += r'0{10}'
SESSION_STATUS_REGEX += END_OF_LINE_REGEX
SESSION_STATUS_MATCHER = re.compile(SESSION_STATUS_REGEX)

SESSION_END_REGEX = r'F{9}CF{8}' + END_OF_LINE_REGEX
SESSION_END_MATCHER = re.compile(SESSION_END_REGEX)

# Tide data records - 18 digit hexadecimal
TIDE_DATA_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{6})'
TIDE_DATA_REGEX += r'(' + ASCII_HEX_CHAR_REGEX + '{4})'
TIDE_DATA_REGEX += r'(' + ASCII_HEX_CHAR_REGEX + '{8})'
TIDE_DATA_REGEX += END_OF_LINE_REGEX
TIDE_DATA_MATCHER = re.compile(TIDE_DATA_REGEX)

# Wave data records - 18 digit hexadecimal
WAVE_DATA_START_REGEX = r'0{18}' + END_OF_LINE_REGEX
WAVE_DATA_START_MATCHER = re.compile(WAVE_DATA_START_REGEX)

WAVE_DATA_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{8})'
WAVE_DATA_REGEX += r'(' + ASCII_HEX_CHAR_REGEX + '{2})'
WAVE_DATA_REGEX += r'0{8}'
WAVE_DATA_REGEX += END_OF_LINE_REGEX
WAVE_DATA_MATCHER = re.compile(WAVE_DATA_REGEX)

WAVE_BURST_DATA_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{6})'
WAVE_BURST_DATA_REGEX += r'(' + ASCII_HEX_CHAR_REGEX + '{6})'
WAVE_BURST_DATA_REGEX += END_OF_LINE_REGEX
WAVE_BURST_DATA_MATCHER = re.compile(WAVE_BURST_DATA_REGEX)

WAVE_DATA_END_REGEX = r'F{18}' + END_OF_LINE_REGEX
WAVE_DATA_END_MATCHER = re.compile(WAVE_DATA_END_REGEX)

# Data end pattern
FILE_DATA_END_REGEX = 'S>'
FILE_DATA_END_REGEX += END_OF_LINE_REGEX
FILE_DATA_END_MATCHER = re.compile(FILE_DATA_END_REGEX)

# SESSION_TIME_MATCHER produces the following groups:
SESSION_GROUP_SAMPLE_TIME = 1

# SESSION_SAMPLE_DATA_MATCHER produces the following groups:
SESSION_GROUP_TIDE_INTERVAL = 1
SESSION_GROUP_WAVE_PERIOD = 2

# TIDE_DATA_MATCHER produces the following groups:
TIDE_GROUP_PRESSURE_NUM = 1
TIDE_GROUP_TEMPERATURE_NUM = 2
TIDE_GROUP_START_TIME = 3

# WAVE_DATA_MATCHER produces the following groups:
WAVE_GROUP_START_TIME = 1
WAVE_GROUP_NUM_SAMPLES_MSB = 2
WAVE_GROUP_PRESS_TEMP_COMP_NUM = 1
WAVE_GROUP_NUM_SAMPLES_LSB = 2

# WAVE_BURST_DATA_MATCHER produces the following groups:
WAVE_BURST_GROUP_PRESSURE_NUM_1 = 1
WAVE_BURST_GROUP_PRESSURE_NUM_2 = 2


class PresfAbcSessionKey(BaseEnum):
    TIDE_SAMPLE_START_TIME = 'tide_sample_start_timestamp'
    TIDE_SAMPLE_INTERVAL = 'tide_sample_period'
    WAVE_INTEGRATION_PERIOD = 'wave_integration_period'


class PresfAbcTideParticleKey(BaseEnum):
    TM_START_TIME = 'presf_time'
    TM_PRESSURE_NUM = 'presf_tide_pressure_number'
    TM_TEMPERATURE_NUM = 'presf_tide_temperature_number'


class PresfAbcWaveParticleKey(BaseEnum):
    WM_START_TIME = 'presf_time'
    WM_PTCN_NUM = 'presf_wave_press_temp_comp_number'
    WM_BURST_PRESSURE_NUM = 'presf_wave_burst_pressure_number'
    WM_NUM_BURST_SAMPLES = 'wm_num_burst_samples'


class DataParticleType(BaseEnum):
    TIDE_RECOVERED = 'presf_abc_tide_measurement_recovered'
    WAVE_RECOVERED = 'presf_abc_wave_burst_recovered'


class DataSection(BaseEnum):
    SESSION = 0
    TIDE = 1
    WAVE = 2


class PresfAbcTideDataParticle(DataParticle):
    """
    Class for parsing data from the presf_abc_dcl tide data set
    """

    _data_particle_type = DataParticleType.TIDE_RECOVERED

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(PresfAbcTideDataParticle, self).__init__(raw_data,
                                                       port_timestamp,
                                                       internal_timestamp,
                                                       preferred_timestamp,
                                                       quality_flag,
                                                       new_sequence)

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """

        result = list()

        result.append(self._encode_value(
            PresfAbcTideParticleKey.TM_START_TIME,
            self.raw_data[PresfAbcTideParticleKey.TM_START_TIME], int))
        result.append(self._encode_value(
            PresfAbcTideParticleKey.TM_PRESSURE_NUM,
            self.raw_data[PresfAbcTideParticleKey.TM_PRESSURE_NUM], int))
        result.append(self._encode_value(
            PresfAbcTideParticleKey.TM_TEMPERATURE_NUM,
            self.raw_data[PresfAbcTideParticleKey.TM_TEMPERATURE_NUM], int))

        # The particle timestamp is the time of the tide measurement.
        tm_start_time = self.raw_data[PresfAbcTideParticleKey.TM_START_TIME]
        ntp_time = utilities.time_2000_to_ntp_time(tm_start_time)
        self.set_internal_timestamp(timestamp=ntp_time)

        return result


class PresfAbcWaveDataParticle(DataParticle):
    """
    Class for parsing data from the presf_abc_dcl wave data set
    """

    _data_particle_type = DataParticleType.WAVE_RECOVERED

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(PresfAbcWaveDataParticle, self).__init__(raw_data,
                                                       port_timestamp,
                                                       internal_timestamp,
                                                       preferred_timestamp,
                                                       quality_flag,
                                                       new_sequence)

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """

        result = list()

        result.append(self._encode_value(
            PresfAbcWaveParticleKey.WM_START_TIME,
            self.raw_data[PresfAbcWaveParticleKey.WM_START_TIME], int))
        result.append(self._encode_value(
            PresfAbcWaveParticleKey.WM_PTCN_NUM,
            self.raw_data[PresfAbcWaveParticleKey.WM_PTCN_NUM], int))
        result.append(self._encode_value(
            PresfAbcWaveParticleKey.WM_BURST_PRESSURE_NUM,
            self.raw_data[PresfAbcWaveParticleKey.WM_BURST_PRESSURE_NUM],
            lambda x: [int(y) for y in x]))

        # The particle timestamp is the time of the start fo the wave burst.
        wm_start_time = self.raw_data[PresfAbcWaveParticleKey.WM_START_TIME]
        ntp_time = utilities.time_2000_to_ntp_time(wm_start_time)
        self.set_internal_timestamp(timestamp=ntp_time)

        return result


class PresfAbcParser(SimpleParser):
    """
    Class for parsing recovered data from the presf_abc instrument.
    """
    def __init__(self,
                 stream_handle,
                 exception_callback):

        self._wave_particle_class = PresfAbcWaveDataParticle
        self._tide_particle_class = PresfAbcTideDataParticle
        self._current_data_section = DataSection.SESSION

        super(PresfAbcParser, self).__init__({}, stream_handle,
                                             exception_callback)

    @staticmethod
    def empty_session_data():

        session_data = dict.fromkeys(
            [PresfAbcSessionKey.TIDE_SAMPLE_START_TIME,
             PresfAbcSessionKey.TIDE_SAMPLE_INTERVAL,
             PresfAbcSessionKey.WAVE_INTEGRATION_PERIOD])

        return session_data

    @staticmethod
    def empty_tide_data():

        tide_data = dict.fromkeys(
            [PresfAbcTideParticleKey.TM_START_TIME,
             PresfAbcTideParticleKey.TM_PRESSURE_NUM,
             PresfAbcTideParticleKey.TM_TEMPERATURE_NUM])

        return tide_data

    @staticmethod
    def empty_wave_data():

        wave_data = dict.fromkeys(
            [PresfAbcWaveParticleKey.WM_START_TIME,
             PresfAbcWaveParticleKey.WM_PTCN_NUM,
             PresfAbcWaveParticleKey.WM_NUM_BURST_SAMPLES])

        wave_data[PresfAbcWaveParticleKey.WM_BURST_PRESSURE_NUM] = []

        return wave_data

    def parse_session_data(self, line, session_data):
        """
        Description:

            This function parses the logging session data of the presf_abc
            hex file.  There are two logging session data records.  The first
            record contains the time of the beginning of the first tide sample.
            The second record contains the tide sample interval (in seconds)
            and the wave integration period (in the number of 0.25 second
            interval).


        Parameters:
            line: logging session data line to parse
            session_data: tide data structure for particle creation
        """

        session_time = SESSION_TIME_MATCHER.match(line)
        if session_time and \
           session_data[PresfAbcSessionKey.TIDE_SAMPLE_START_TIME] is None:

            time_first_tide_sample =\
                int(session_time.group(SESSION_GROUP_SAMPLE_TIME), 16)
            session_data[PresfAbcSessionKey.TIDE_SAMPLE_START_TIME] =\
                time_first_tide_sample

        else:
            session_status = SESSION_STATUS_MATCHER.match(line)
            if session_status:
                tide_sample_interval =\
                    int(session_status.group(SESSION_GROUP_TIDE_INTERVAL), 16)
                session_data[PresfAbcSessionKey.TIDE_SAMPLE_INTERVAL] =\
                    tide_sample_interval

                wave_intr_period =\
                    int(session_status.group(SESSION_GROUP_WAVE_PERIOD), 16)
                session_data[PresfAbcSessionKey.WAVE_INTEGRATION_PERIOD] =\
                    wave_intr_period

            else:
                # Expected format is incorrect.
                log.debug("Unexpected logging session status data.")
                self._exception_callback(RecoverableSampleException(
                    "Unexpected logging session data: %s" % line))

    def parse_tide_data(self, line, tide_data):
        """
        Description:

            This function parses the tide data of the presf_abc hex file.  The
            tide data contains the pressure number (used to calculate the
            pressure), the temperature number (used the calculate the
            temperature), and the start time of the tide measurement (seconds
            from 1-1-2000).

        Parameters:
            line: tide data line to parse
            tide_data: tide data structure for particle creation
        """

        tide_data_re = TIDE_DATA_MATCHER.match(line)
        if tide_data_re:
            # Parse the tide measurement start time
            tm_start_time =\
                int(tide_data_re.group(TIDE_GROUP_START_TIME), 16)
            tide_data[PresfAbcTideParticleKey.TM_START_TIME] =\
                tm_start_time

            # Parse the tide measurement pressure count.
            p_dec_tide =\
                int(tide_data_re.group(TIDE_GROUP_PRESSURE_NUM), 16)
            tide_data[PresfAbcTideParticleKey.TM_PRESSURE_NUM] =\
                p_dec_tide

            # Parse the timde measurement temperature count
            t_dec_tide =\
                int(tide_data_re.group(TIDE_GROUP_TEMPERATURE_NUM), 16)
            tide_data[PresfAbcTideParticleKey.TM_TEMPERATURE_NUM] =\
                t_dec_tide

            particle = self._extract_sample(self._tide_particle_class,
                                            None,
                                            tide_data,
                                            None)
            self._record_buffer.append(particle)

        else:
            log.debug("Unexpected format for tide data: %s" % line)
            self._exception_callback(RecoverableSampleException(
                "Unexpected format for tide data: %s" % line))

    def parse_wave_data(self, line, wave_data):
        """
        Description:

            This function parses the wave data of the presf_abc hex file.
            The wave data contains two pressure number measurements (used
            to calculate the pressure).

        Parameters:
            line: wave data line to parse
            wave_data: tide data structure for particle creation
        """

        # Get the possible wave date record matches.
        wave_data_re = WAVE_DATA_MATCHER.match(line)
        wave_burst_data_re = WAVE_BURST_DATA_MATCHER.match(line)
        wave_data_end_re = WAVE_DATA_END_MATCHER.match(line)

        # Check if the record is one of the two wave metadata records.
        if wave_data_re:
            if wave_data[PresfAbcWaveParticleKey.WM_START_TIME] is None:
                # Parse the Wave Burst start time
                wb_start_time =\
                    int(wave_data_re.group(WAVE_GROUP_START_TIME), 16)
                wave_data[PresfAbcWaveParticleKey.WM_START_TIME] =\
                    wb_start_time

                # Parse the number of Wave Burst samples (MSB)
                wb_samples_msb =\
                    int(wave_data_re.group(WAVE_GROUP_NUM_SAMPLES_MSB), 16) <<\
                    8
                wave_data[PresfAbcWaveParticleKey.WM_NUM_BURST_SAMPLES] =\
                    wb_samples_msb

            else:
                # Parse the Pressure Temperature Compensation Number
                ptcn =\
                    int(wave_data_re.group(WAVE_GROUP_PRESS_TEMP_COMP_NUM), 16)
                wave_data[PresfAbcWaveParticleKey.WM_PTCN_NUM] = ptcn

                # Parse the number of Wave Burst samples (LSB)
                wb_samples_lsb =\
                    int(wave_data_re.group(WAVE_GROUP_NUM_SAMPLES_LSB), 16)
                wave_data[PresfAbcWaveParticleKey.WM_NUM_BURST_SAMPLES] +=\
                    wb_samples_lsb

        # Check if the record is a wave burst record.
        elif wave_burst_data_re:
            # Parse the first pressure measurement from the record
            p_dec_wave = int(wave_burst_data_re.
                             group(WAVE_BURST_GROUP_PRESSURE_NUM_1), 16)
            wave_data[PresfAbcWaveParticleKey.WM_BURST_PRESSURE_NUM].\
                append(p_dec_wave)

            # Parse the second pressure measurement from the record
            p_dec_wave = int(wave_burst_data_re.
                             group(WAVE_BURST_GROUP_PRESSURE_NUM_2), 16)
            wave_data[PresfAbcWaveParticleKey.WM_BURST_PRESSURE_NUM].\
                append(p_dec_wave)

        # Check if the record is the end wave burst record.
        elif wave_data_end_re:
            # Check we recieved the correct number of wave burst data.
            if len(wave_data[PresfAbcWaveParticleKey.WM_BURST_PRESSURE_NUM])\
                    == wave_data[PresfAbcWaveParticleKey.WM_NUM_BURST_SAMPLES]:

                # Create the data particle and add it to the buffer.
                particle = self._extract_sample(self._wave_particle_class,
                                                None,
                                                wave_data,
                                                None)
                self._record_buffer.append(particle)

            else:
                log.debug("Unexcepted number of wave burst records: %s" % line)
                self._exception_callback(RecoverableSampleException(
                    "Unexcepted number of wave burst records: %s" % line))

        else:
            log.debug("Unexpected format for wave data: %s" % line)
            self._exception_callback(RecoverableSampleException(
                "Unexpected format for wave data: %s" % line))

    def parse_file(self):
        """
        The main parsing function which loops over each line in the file and
        extracts particles if the correct format is found.
        """
        session_data = self.empty_session_data()
        tide_data = self.empty_tide_data()
        wave_data = self.empty_wave_data()
        #
        # # First, parse the header
        # self.parse_header()

        for line in self._stream_handle:
            #####
            # Check for a header line and ignore it.
            #####
            if HEADER_LINE_MATCHER.match(line):
                continue  # read next line

            #####
            # Check for a transition to another data section (logging, tide or
            # wave) and set the current data section appropriately
            #####

            # Check for the start of a logging session data section.
            if SESSION_START_MATCHER.match(line):
                # Start of new logging session, clear the logging session data.
                session_data = self.empty_session_data()

                self._current_data_section = DataSection.SESSION
                continue  # read next line

            # If this is the end of the session data, clear the tide and wave
            # data and set the current data section to the tide data section.
            if SESSION_END_MATCHER.match(line):
                # End of the logging session, clear the tide and wave data.
                tide_data = self.empty_tide_data()
                wave_data = self.empty_wave_data()

                self._current_data_section = DataSection.TIDE
                continue  # read next line

            # Check for the start of a wave data section.
            if WAVE_DATA_START_MATCHER.match(line):
                self._current_data_section = DataSection.WAVE
                continue  # read next line

            # Check for end of the data in the file and get out of the loop.
            if FILE_DATA_END_MATCHER.match(line):
                break

            #####
            # If we got here, the record isn't a flag to transition to another
            # data section, so parse the data appropriately.
            #####
            if self._current_data_section == DataSection.SESSION:
                self.parse_session_data(line, session_data)
                continue  # read next line

            if self._current_data_section == DataSection.TIDE:
                self.parse_tide_data(line, tide_data)
                continue  # read next line

            if self._current_data_section == DataSection.WAVE:
                self.parse_wave_data(line, wave_data)

                # If this is the end of the wave data, clear the tide and wave
                # data and set the current section to the tide data section.
                if WAVE_DATA_END_MATCHER.match(line):
                    tide_data = self.empty_tide_data()
                    wave_data = self.empty_wave_data()

                    self._current_data_section = DataSection.TIDE
