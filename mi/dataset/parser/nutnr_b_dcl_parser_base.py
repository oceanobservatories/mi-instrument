#!/usr/bin/env python

"""
@package mi.dataset.parser.nutnr_b_dcl_parser_base
@file mi/dataset/parser/nutnr_b_dcl_parser_base.py
@author Steve Myerson (Raytheon), Mark Worden
@brief Base nutnr_b_dcl parser code.

This file contains code for the base nutnr_b_dcl parser and regex code used
to produce the particles.
"""

__author__ = 'Steve Myerson (Raytheon), Mark Worden'
__license__ = 'Apache 2.0'

import re

import ntplib

from mi.core.log import get_logger, get_logging_metaclass
log = get_logger()

from mi.core.exceptions import RecoverableSampleException, NotImplementedException, \
    UnexpectedDataException

from mi.dataset.dataset_parser import Parser

from mi.dataset.parser.nutnr_b_particles import NutnrBDataParticleKey
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_utc_time, \
    dcl_controller_timestamp_to_ntp_time

from mi.core.common import BaseEnum

from mi.dataset.parser.common_regexes import SPACE_REGEX, END_OF_LINE_REGEX, \
    ANY_CHARS_REGEX, FLOAT_REGEX, ONE_OR_MORE_WHITESPACE_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX, \
    DATE_YYYY_MM_DD_REGEX, DATE_MM_DD_YYYY_REGEX, TIME_HR_MIN_SEC_REGEX, INT_REGEX

# DCL-Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Inst-Timestamp: MM/DD/YYYY HH:MM:SS (Instrument timestamp)
DCL_TIMESTAMP = '(' + DATE_YYYY_MM_DD_REGEX + SPACE_REGEX + TIME_HR_MIN_SEC_MSEC_REGEX + ')'
INST_TIMESTAMP = DATE_MM_DD_YYYY_REGEX + SPACE_REGEX + TIME_HR_MIN_SEC_REGEX

START_METADATA = r'\['
END_METADATA = r'\]'

# All records end with one of the newlines.

# Metadata records:
#   Record-Timestamp [<text>] <more text> (Some of these records are ignored)
#   Record-Timestamp DCL-Timestamp: Message <text> (Metadata fields)

META_MESSAGE_REGEX = DCL_TIMESTAMP + SPACE_REGEX   # Record Timestamp
META_MESSAGE_REGEX += INST_TIMESTAMP + ': '  # DCL Timestamp
META_MESSAGE_REGEX += 'Message: '            # Indicates useful metadata record
META_MESSAGE_MATCHER = re.compile(META_MESSAGE_REGEX)

IDLE_TIME_REGEX = DCL_TIMESTAMP + SPACE_REGEX   # Record Timestamp
IDLE_TIME_REGEX += START_METADATA         # Start of metadata
IDLE_TIME_REGEX += ANY_CHARS_REGEX              # Any text
IDLE_TIME_REGEX += END_METADATA + ':'     # End of metadata
IDLE_TIME_REGEX += '.*?Idle state, without initialize'
IDLE_TIME_REGEX += END_OF_LINE_REGEX
IDLE_TIME_MATCHER = re.compile(IDLE_TIME_REGEX)

START_TIME_REGEX = META_MESSAGE_REGEX
START_TIME_REGEX += 'ISUS Awakened on Schedule at UTC '
START_TIME_REGEX += DATE_MM_DD_YYYY_REGEX + SPACE_REGEX + TIME_HR_MIN_SEC_REGEX + '.'
START_TIME_REGEX += END_OF_LINE_REGEX
START_TIME_MATCHER = re.compile(START_TIME_REGEX)

SPEC_ON_TIME_REGEX = META_MESSAGE_REGEX
SPEC_ON_TIME_REGEX += 'Turning ON Spectrometer.'
SPEC_ON_TIME_REGEX += END_OF_LINE_REGEX
SPEC_ON_TIME_MATCHER = re.compile(SPEC_ON_TIME_REGEX)

SPEC_POWERED_TIME_REGEX = META_MESSAGE_REGEX
SPEC_POWERED_TIME_REGEX += 'Spectrometer powered up.'
SPEC_POWERED_TIME_REGEX += END_OF_LINE_REGEX
SPEC_POWERED_TIME_MATCHER = re.compile(SPEC_POWERED_TIME_REGEX)

LAMP_ON_TIME_REGEX = META_MESSAGE_REGEX
LAMP_ON_TIME_REGEX += 'Turning ON UV light source.'
LAMP_ON_TIME_REGEX += END_OF_LINE_REGEX
LAMP_ON_TIME_MATCHER = re.compile(LAMP_ON_TIME_REGEX)

LAMP_POWERED_TIME_REGEX = META_MESSAGE_REGEX
LAMP_POWERED_TIME_REGEX += 'UV light source powered up.'
LAMP_POWERED_TIME_REGEX += END_OF_LINE_REGEX
LAMP_POWERED_TIME_MATCHER = re.compile(LAMP_POWERED_TIME_REGEX)

# Filename can be any of the following formats:
#   just_a_filename
#   directory/filename       (unix style)
#   directory\filename       (windows style)
LOG_FILE_REGEX = META_MESSAGE_REGEX
LOG_FILE_REGEX += "Data log file is '"
LOG_FILE_REGEX += r'([\w\.\\/]+)'    # This is the Filename
LOG_FILE_REGEX += "'"
LOG_FILE_REGEX += r'\.'
LOG_FILE_REGEX += END_OF_LINE_REGEX
LOG_FILE_MATCHER = re.compile(LOG_FILE_REGEX)

NEXT_WAKEUP_REGEX = META_MESSAGE_REGEX
NEXT_WAKEUP_REGEX += "ISUS Next Wakeup at UTC "
NEXT_WAKEUP_REGEX += INST_TIMESTAMP
NEXT_WAKEUP_REGEX += r'\.'
NEXT_WAKEUP_REGEX += END_OF_LINE_REGEX
NEXT_WAKEUP_MATCHER = re.compile(NEXT_WAKEUP_REGEX)


# The META_MESSAGE_MATCHER produces the following groups (used in group(xxx)):
class MetaDataMatchGroups(BaseEnum):
    META_GROUP_DCL_TIMESTAMP = 1
    META_GROUP_DCL_YEAR = 2
    META_GROUP_DCL_MONTH = 3
    META_GROUP_DCL_DAY = 4
    META_GROUP_DCL_HOUR = 5
    META_GROUP_DCL_MINUTE = 6
    META_GROUP_DCL_SECOND = 7
    META_GROUP_DCL_MILLISECOND = 8
    META_GROUP_INST_MONTH = 9
    META_GROUP_INST_DAY = 10
    META_GROUP_INST_YEAR = 11
    META_GROUP_INST_HOUR = 12
    META_GROUP_INST_MINUTE = 13
    META_GROUP_INST_SECOND = 14
    META_GROUP_INST_LOGFILE = 15


SEPARATOR = ','

# Instrument data records:
#   Record-Timestamp SAT<NDC or NLC>Serial-Number,<comma separated data>
#2014/06/27 14:46:48.947 SATNDF0260,2014178,14.780263,0.00,0.00,0.00,0.00,0.000000,23.12,23.69,16.93,165422,8.10,12.02,4.97,15.03,240.67,2.22,982.20,977.88,986,973,980,973,999,986,978,963,964,982,
#993,965,985,979,967,977,985,967,982,972,970,973,978,985,992,977,983,977,962,951,963,968,972,977,985,976,973,967,964,977,963,978,986,973,983,987,971,983,967,

INST_COMMON_REGEX = '([0-9a-zA-Z]{4})'                      # serial number
INST_COMMON_REGEX += SEPARATOR
INST_COMMON_REGEX += '(\d{7})' + SEPARATOR                   # julian date sample was recorded
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal time of sample
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal nitrate concentration
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal first fitting result
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal second fitting result
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal third fitting result
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')'                 # decimal RMS error

INST_CONC_DATA_REGEX = DCL_TIMESTAMP + SPACE_REGEX              # Record-Timestamp
INST_CONC_DATA_REGEX += '(SAT)'                                 # frame header
INST_CONC_DATA_REGEX += '(NLC|NDC)'                             # frame type
INST_CONC_DATA_REGEX += INST_COMMON_REGEX
INST_CONC_DATA_REGEX_W_NEWLINE = INST_CONC_DATA_REGEX + END_OF_LINE_REGEX
INST_CONC_DATA_W_NEWLINE_MATCHER = re.compile(INST_CONC_DATA_REGEX_W_NEWLINE)

INST_FULL_DATA_REGEX = DCL_TIMESTAMP + SPACE_REGEX              # Record-Timestamp
INST_FULL_DATA_REGEX += '(SAT)'                                 # frame header
INST_FULL_DATA_REGEX += '(NLF|NDF)'                             # frame type
INST_FULL_DATA_REGEX += INST_COMMON_REGEX + SEPARATOR
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal temp interior
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal temp spectrometer
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal temp lamp
INST_FULL_DATA_REGEX += '(' + INT_REGEX + ')' + SEPARATOR       # decimal lamp time
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal humidity
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal voltage lamp
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal voltage analog
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal voltage main
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal ref channel average
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal ref channel variance
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal sea water dark
INST_FULL_DATA_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal spec channel average
INST_FULL_DATA_REGEX += '((?:' + INT_REGEX + SEPARATOR + '){255}' + \
                        INT_REGEX + ')'     # 256 int spectral values
INST_FULL_DATA_MATCHER = re.compile(INST_FULL_DATA_REGEX)


class InstrumentDataMatchGroups(BaseEnum):
    # The following match groups are applicable to the instrument conc and full
    INST_GROUP_DCL_TIMESTAMP = 1
    INST_GROUP_DCL_YEAR = 2
    INST_GROUP_DCL_MONTH = 3
    INST_GROUP_DCL_DAY = 4
    INST_GROUP_DCL_HOUR = 5
    INST_GROUP_DCL_MINUTE = 6
    INST_GROUP_DCL_SECOND = 7
    INST_GROUP_DCL_MILLISECOND = 8
    INST_GROUP_FRAME_HEADER = 9
    INST_GROUP_FRAME_TYPE = 10
    INST_GROUP_SERIAL_NUMBER = 11
    INST_GROUP_JULIAN_DATE = 12
    INST_GROUP_TIME_OF_DAY = 13
    INST_GROUP_NITRATE = 14
    INST_GROUP_FITTING1 = 15
    INST_GROUP_FITTING2 = 16
    INST_GROUP_FITTING3 = 17
    INST_GROUP_RMS_ERROR = 18

    # The following match groups are only applicable to the instrument full
    INST_GROUP_TEMP_INTERIOR = 19
    INST_GROUP_TEMP_SPECTROMETER = 20
    INST_GROUP_TEMP_LAMP = 21
    INST_GROUP_LAMP_TIME = 22
    INST_GROUP_HUMIDITY = 23
    INST_GROUP_VOLTAGE_LAMP = 24
    INST_GROUP_VOLTAGE_ANALOG = 25
    INST_GROUP_VOLTAGE_MAIN = 26
    INST_GROUP_REF_CHANNEL_AVERAGE = 27
    INST_GROUP_REF_CHANNEL_VARIANCE = 28
    INST_GROUP_SEA_WATER_DARK = 29
    INST_GROUP_SPEC_CHANNEL_AVERAGE = 30
    INST_GROUP_SPECTRAL_CHANNELS = 31


NITRATE_DARK_CONCENTRATE = 'NDC'        # frame type Nitrate Dark Concentrate
NITRATE_LIGHT_CONCENTRATE = 'NLC'       # frame type Nitrate Light Concentrate
NITRATE_DARK_FULL = 'NDF'               # frame type Nitrate Dark Full
NITRATE_LIGHT_FULL = 'NLF'              # frame type Nitrate Light Full

CONCENTRATE_FRAME_TYPES = (NITRATE_DARK_CONCENTRATE, NITRATE_LIGHT_CONCENTRATE)
FULL_FRAME_TYPES = (NITRATE_DARK_FULL, NITRATE_LIGHT_FULL)

FRAME_TYPE_DARK_INDEX = 0
FRAME_TYPE_LIGHT_INDEX = 1

METADATA_STATE_TABLE = [
    [0x01, START_TIME_MATCHER, 0],
    [0x02, SPEC_ON_TIME_MATCHER, 0],
    [0x04, SPEC_POWERED_TIME_MATCHER, 0],
    [0x08, LAMP_ON_TIME_MATCHER, 0],
    [0x10, LAMP_POWERED_TIME_MATCHER, 0],
    [0x20, LOG_FILE_MATCHER, 0]
]
ALL_METADATA_RECEIVED = 0x3F
METADATA_VALUE_INDEX = 2

# nutr_b rex and matchers used to ignore specific lines
DCL_DATE_TIME_ONLY_REGEX = DCL_TIMESTAMP + '\s*' + END_OF_LINE_REGEX

# The following regexes are for lines to ignore
INITIALIZING_ANALOG_OUTPUT_REGEX = \
    META_MESSAGE_REGEX + 'Initializing analog output.' + \
    END_OF_LINE_REGEX

ENTERING_LOW_POWER_SUSPENSION_REGEX = \
    META_MESSAGE_REGEX + \
    'Entering low power suspension, waiting for scheduled event\.' + \
    END_OF_LINE_REGEX

INSTRUMENT_POWERED_OFF_REGEX = \
    DCL_TIMESTAMP + SPACE_REGEX + START_METADATA + ANY_CHARS_REGEX + \
    END_METADATA + ':Instrument Stopped \[(Quit/)?Power Off\]' + \
    END_OF_LINE_REGEX

END_OF_BLOCK_REGEX = \
    DCL_TIMESTAMP + SPACE_REGEX + START_METADATA + ANY_CHARS_REGEX + \
    END_METADATA + ':----- END OF BLOCK -----' + END_OF_LINE_REGEX

# Note that the \xF0 character in the following regex is the eth char
DCL_DATE_TIME_WITH_ETH_CHAR_REGEX = \
    DCL_TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + "\xF0" + ANY_CHARS_REGEX + \
    END_OF_LINE_REGEX + "?"

CHARGING_PPC_CHARGED_REGEX = \
    DCL_TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + \
    "Charging power protection circuit \+\+\+\+\+\+\+\+\+\+ charged" + \
    END_OF_LINE_REGEX

ENTERING_OPERATIONAL_LOOP_REGEX = \
    DCL_TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + \
    "Entering operational loop, press S to exit\." + \
    END_OF_LINE_REGEX

DATA_LOGGER_ENTERING_ACQUISITION_STATE_REGEX = \
    DCL_TIMESTAMP + SPACE_REGEX + START_METADATA + ANY_CHARS_REGEX + \
    END_METADATA + ':Instrument sampling - Data logger entering data acquisition state' + \
    END_OF_LINE_REGEX

LAST_LINE_REGEX = DCL_TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + '\xef'

NUTR_B_DCL_IGNORE_REGEX = \
    '(' + DCL_DATE_TIME_ONLY_REGEX + '|' + \
    INITIALIZING_ANALOG_OUTPUT_REGEX + '|' + \
    ENTERING_LOW_POWER_SUSPENSION_REGEX + '|' + \
    INSTRUMENT_POWERED_OFF_REGEX + '|' + \
    END_OF_BLOCK_REGEX + '|' + \
    DCL_DATE_TIME_WITH_ETH_CHAR_REGEX + '|' + \
    DATA_LOGGER_ENTERING_ACQUISITION_STATE_REGEX + '|' + \
    ENTERING_OPERATIONAL_LOOP_REGEX + '|' + \
    CHARGING_PPC_CHARGED_REGEX + '|' + \
    LAST_LINE_REGEX + ')'
NUTR_B_DCL_IGNORE_MATCHER = re.compile(NUTR_B_DCL_IGNORE_REGEX)


class NutnrBDclParser(Parser):

    __metaclass__ = get_logging_metaclass(log_level='debug')
    """
    Parser for nutnr_b_dcl data.
    In addition to the standard parser constructor parameters,
    this constructor needs the following additional parameters:
      - instrument particle class
      - metadata particle class
      - frame_types tuple
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 instrument_particle_class,
                 dark_instrument_particle_class,
                 metadata_particle_class,
                 frame_types):

        super(NutnrBDclParser, self).__init__(config,
                                              stream_handle,
                                              None,
                                              None,
                                              state_callback,
                                              publish_callback,
                                              exception_callback)

        # Initialize the
        self._file_parsed = False
        self._record_buffer = []
        self._metadata_state = 0
        self._metadata_timestamp = 0.0
        self._metadata_particle_generated_for_block = False

        # Save the names of the particle classes to be generated.
        self._metadata_particle_class = metadata_particle_class
        self._instrument_particle_class = instrument_particle_class
        self._dark_instrument_particle_class = dark_instrument_particle_class

        # Save the input frame types
        self._frame_types = frame_types

    def _extract_metadata_unix_timestamp(self, idle_match):
        """
        This function will create a timestamp to be used as the internal
        timestamp for the metadata particle is generated.
        """

        # calculate the metadata particle internal timestamp
        # from the DCL timestamp.

        utc_time = dcl_controller_timestamp_to_utc_time(idle_match.group(
            MetaDataMatchGroups.META_GROUP_DCL_TIMESTAMP))

        return utc_time

    def _extract_instrument_ntp_timestamp(self, inst_match):
        """
        This function will create a timestamp to be used as the internal
        timestamp for the instrument particle is generated.
        """

        # calculate the instrument particle internal timestamp
        # from the DCL timestamp.

        return dcl_controller_timestamp_to_ntp_time(inst_match.group(
            InstrumentDataMatchGroups.INST_GROUP_DCL_TIMESTAMP))

    def _process_idle_metadata_record(self, idle_match):
        """
        This function processes an Idle State metadata record.
        It will create a timestamp to be used as the internal timestamp for the
        metadata particle is generated.
        """

        self._metadata_timestamp = ntplib.system_to_ntp_time(
            self._extract_metadata_unix_timestamp(idle_match))

    def _create_instrument_particle(self, inst_match):
        raise NotImplementedException(
            "The _create_instrument_particle must be implemented by the inheriting class!")

    def _process_instrument_record_match(self, inst_match):
        """
        This function processes an instrument data match record.
        It will return the list of data particles generated.
        """
        # If the frame type is not DARK or LIGHT,
        # raise a recoverable sample exception.

        frame_type = inst_match.group(InstrumentDataMatchGroups.INST_GROUP_FRAME_TYPE)

        if frame_type != self._frame_types[FRAME_TYPE_DARK_INDEX] \
                and frame_type != self._frame_types[FRAME_TYPE_LIGHT_INDEX]:
            error_message = 'Invalid frame type %s' % frame_type
            log.warn(error_message)
            self._exception_callback(RecoverableSampleException(error_message))

        else:

            #generate one metadata record if it has not already been done
            if self._metadata_state == ALL_METADATA_RECEIVED and self._metadata_particle_generated_for_block is False:

                # Fields for the metadata particle must be
                # in the same order as the RAW_INDEX_META_xxx values.
                # DCL Controller timestamp and serial number
                # are from the instrument data record.
                # Other data comes from the various metadata records
                # which has been accumulated in the Metadata State Table.
                meta_fields = [value
                               for state, matcher, value in METADATA_STATE_TABLE]

                metadata_tuple = [
                    (NutnrBDataParticleKey.DCL_CONTROLLER_TIMESTAMP,
                     inst_match.group(InstrumentDataMatchGroups.INST_GROUP_DCL_TIMESTAMP),
                     str),
                    (NutnrBDataParticleKey.SERIAL_NUMBER,
                     inst_match.group(InstrumentDataMatchGroups.INST_GROUP_SERIAL_NUMBER),
                     str),
                    (NutnrBDataParticleKey.STARTUP_TIME,
                     meta_fields[0],
                     int),
                    (NutnrBDataParticleKey.SPEC_ON_TIME,
                     meta_fields[1],
                     int),
                    (NutnrBDataParticleKey.SPEC_POWERED_TIME,
                     meta_fields[2],
                     int),
                    (NutnrBDataParticleKey.LAMP_ON_TIME,
                     meta_fields[3],
                     int),
                    (NutnrBDataParticleKey.LAMP_POWERED_TIME,
                     meta_fields[4],
                     int),
                    (NutnrBDataParticleKey.DATA_LOG_FILE,
                     meta_fields[5],
                     str)]

                particle = self._extract_sample(self._metadata_particle_class,
                                                None,
                                                metadata_tuple,
                                                self._metadata_timestamp)

                if particle is not None:
                    self._record_buffer.append(particle)
                    self._metadata_particle_generated_for_block = True

            #
            particle = self._create_instrument_particle(inst_match)
            if particle is not None:
                self._record_buffer.append(particle)

    def _process_next_wakeup_match(self):

        # Clear the metadata state
        self._metadata_state = 0

        # Reset the flag to indicate that we have not generated the metadata particle
        self._metadata_particle_generated_for_block = False

    def _process_metadata_record_part(self, line):
        """
        This function checks to see if a metadata record is contained
        in this chunk.
        """

        match_found = False

        for table_data in METADATA_STATE_TABLE:
            state, matcher, value = table_data
            match = matcher.match(line)

            # If we get a match, it's one of the metadata records
            # that we're interested in.

            if match is not None:

                match_found = True

                # Update the state to reflect that we've got
                # this particular metadata record.

                self._metadata_state |= state

                # For all matchers except the LOG_FILE matcher,
                # convert the instrument time to seconds since
                # Jan 1, 1970 (Unix Epoch time).

                if matcher != LOG_FILE_MATCHER:
                    table_data[METADATA_VALUE_INDEX] = self._extract_metadata_unix_timestamp(match)

                # For the LOG_FILE matcher, save the name of the log file.
                else:
                    table_data[METADATA_VALUE_INDEX] = match.group(MetaDataMatchGroups.META_GROUP_INST_LOGFILE)

        if match_found is False:
            error_message = 'Unexpected metadata found: ' + line
            log.warn(error_message)
            self._exception_callback(UnexpectedDataException(error_message))

    def parse_file(self):
        """
        Parse file and collect particles
        """
        raise NotImplementedException("The parse_file must be implemented by the inheriting class!")

    def get_records(self, num_records_requested=1):
        """
        Returns a list of particles that is  equal to the num_records_requested when there are that many particles
        are available or a list of particles less than the num_records_requested when there are fewer than
        num_records_requested available.
        """
        particles_to_return = []

        if num_records_requested > 0:

            # If the file was not read, let's parse it
            if self._file_parsed is False:
                self.parse_file()

            # Iterate through the particles returned, and pop them off from the beginning of the record
            # buffer to the end
            while len(particles_to_return) < num_records_requested and len(self._record_buffer) > 0:
                particles_to_return.append(self._record_buffer.pop(0))

        return particles_to_return
