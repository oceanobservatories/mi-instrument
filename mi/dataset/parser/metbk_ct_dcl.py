#!/usr/bin/env python

"""
@package mi.dataset.parser.metbk_ct_dcl
@file marine-integrations/mi/dataset/parser/metbk_ct_dcl.py
@author Tim Fisher
@brief Parser for the metbk_ct_dcl dataset driver

This file contains code for the metbk_ct_dcl parser and code to produce data particles.
For recovered data, there is one parser which produces one type of data particle.

The input file is hex and contains 1 type of record.
All detail records are 20 bytes separated by a newline.
Each detail record contains temperature, conductivity and time.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release
"""

import binascii
import re

from mi.core.common import BaseEnum
from mi.core.exceptions import \
    SampleException

from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.utilities import zulu_timestamp_to_ntp_time

NEW_LINE = r'[\n\r]+'             # Handle any type of new line

# For Recovered CT files, the serial number is in the Configuration XML section.
REC_CT_SERIAL_REGEX = r'^'        # At the beginning of the record
REC_CT_SERIAL_REGEX += r'\* <HardwareData DeviceType=\'SBE37SM-RS485\' SerialNumber=\''
REC_CT_SERIAL_REGEX += r'(\d+)'   # Serial number is any number of digits
REC_CT_SERIAL_REGEX += r'\'>'     # the rest of the XML syntax
REC_CT_SERIAL_MATCHER = re.compile(REC_CT_SERIAL_REGEX)

# The REC_CT_SERIAL_MATCHER produces the following group:
REC_CT_SERIAL_GROUP_SERIAL_NUMBER = 1

# The end of the Configuration XML section is denoted by a *END* record.
REC_CT_CONFIGURATION_END = r'^'                    # At the beginning of the record
REC_CT_CONFIGURATION_END += r'\*END\*'             # *END*
REC_CT_CONFIGURATION_END += NEW_LINE               # separated by a new line
REC_CT_CONFIGURATION_END_MATCHER = re.compile(REC_CT_CONFIGURATION_END)

# Recovered CT Data record (hex ascii):
REC_CT_SAMPLE_BYTES = 21                # includes record separator

REC_CT_REGEX = b'([0-9a-fA-F]{6})'      # Temperature
REC_CT_REGEX += b'([0-9a-fA-F]{6})'     # Conductivity
REC_CT_REGEX += b'([0-9a-fA-F]{8})'     # Time in seconds since 1/1/2000
REC_CT_REGEX += NEW_LINE                # separated by a new line
REC_CT_MATCHER = re.compile(REC_CT_REGEX)

# The REC_CT_MATCHER produces the following groups:
REC_CT_GROUP_TEMPERATURE = 1
REC_CT_GROUP_CONDUCTIVITY = 2
REC_CT_GROUP_TIME = 3

# Indices into raw_data tuples for CT data
RAW_INDEX_REC_CT_SERIAL = 0
RAW_INDEX_REC_CT_TEMPERATURE = 1
RAW_INDEX_REC_CT_CONDUCTIVITY = 2
RAW_INDEX_REC_CT_TIME = 3


def convert_hex_ascii_to_int(int_val):
    """
    Use to convert from hex-ascii to int when encoding data particle values
    """
    return int(int_val, 16)


def generate_particle_timestamp(time_2000):
    """
    This function calculates and returns a timestamp in epoch 1900
    based on an ASCII hex time in epoch 2000.
    Parameter:
      time_2000 - number of seconds since Jan 1, 2000
    Returns:
      number of seconds since Jan 1, 1900
    """
    return int(time_2000, 16) + zulu_timestamp_to_ntp_time("2000-01-01T00:00:00.00Z")


class DataParticleType(BaseEnum):
    INSTRUMENT_PARTICLE = 'metbk_ct_dcl_instrument'


class DataParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"
    TEMPERATURE = "temperature"
    CONDUCTIVITY = "conductivity"
    CTD_TIME = "ctd_time"


class MetbkCtDclParser(SimpleParser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        #
        # File is ASCII with records separated by newlines.
        #
        super(MetbkCtDclParser, self).__init__(config, stream_handle, exception_callback)

        #
        # set flags to indicate the end of Configuration has not been reached
        # and the serial number has not been found.
        #
        self._serial_number = None
        self._end_config = False

        self.input_file = stream_handle

    def check_for_config_end(self, chunk):
        """
        This function searches the input buffer for the end of Configuration record.
        If found, the read_state and state are updated.
        """
        match = REC_CT_CONFIGURATION_END_MATCHER.match(chunk)
        if match is not None:
            self._end_config = True

    def check_for_serial_number(self, chunk):
        """
        This function searches the input buffer for a serial number.
        """
        #
        # See if this record contains the serial number.
        # If found, convert from decimal ASCII and save.
        #
        match = REC_CT_SERIAL_MATCHER.match(chunk)
        if match is not None:
            self._serial_number = int(match.group(REC_CT_SERIAL_GROUP_SERIAL_NUMBER))

    def parse_file(self):
        """
        Parse the file for the recovered CT parser
        :return: list of result particles
        """

        # Read the first line in the file
        line = self._stream_handle.readline()

        # Process each line of the file
        while line:
            #
            # Search for serial number if not already found.
            #
            if self._serial_number is None:
                self.check_for_serial_number(line)

            #
            # Once the serial number is found,
            # search for the end of the Configuration section.
            #
            elif not self._end_config:
                self.check_for_config_end(line)

            #
            # Once the end of the Configuration section is reached,
            # all remaining records are supposedly CT data records.
            # Parse the record and generate the particle for this chunk.
            # Add it to the return list of particles.
            #
            else:
                particle = self.parse_ct_record(line)
                if particle is not None:
                    self._record_buffer.append(particle)

            # read the next line in the file
            line = self._stream_handle.readline()

    def parse_ct_record(self, ct_record):
        """
        This function parses a Recovered CT record and returns a data particle.
        Parameters:
          ct_record - the input which is being parsed
        """
        ct_match = REC_CT_MATCHER.match(ct_record)
        if ct_match is not None:
            #
            # If this is CT record, generate the data particle.
            # Data stored for each particle is a tuple of the following:
            #   inductive ID (obtained from configuration data)
            #   serial number
            #   temperature
            #   conductivity
            #   time of science data
            #
            sample = self._extract_sample(MetbkCtDclInstrumentDataParticle, None,
                                         (self._serial_number,
                                          ct_match.group(REC_CT_GROUP_TEMPERATURE),
                                          ct_match.group(REC_CT_GROUP_CONDUCTIVITY),
                                          ct_match.group(REC_CT_GROUP_TIME)))

        #
        # If there wasn't a match, the input data is messed up.
        #
        else:
            error_message = 'unknown data found in CT chunk %s, leaving out the rest of chunk' \
                            % binascii.b2a_hex(ct_record)
            log.error(error_message)
            self._exception_callback(SampleException(error_message))
            sample = None

        return sample


class MetbkCtDclInstrumentDataParticle(DataParticle):
    _data_particle_type = DataParticleType.INSTRUMENT_PARTICLE

    def _build_parsed_values(self):
        """
        Build parsed values for Telemetered Data Particle.
        Take something in the hex ASCII data values and turn it into a
        particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        #
        # The particle timestamp is the time contained in the CT instrument data.
        # This time field is number of seconds since 1/1/2000.
        # Convert from epoch in 2000 to epoch in 1900.
        #
        time_stamp = generate_particle_timestamp(self.raw_data[RAW_INDEX_REC_CT_TIME])
        self.set_internal_timestamp(timestamp=time_stamp)

        #
        # Raw data for this particle consists of the following fields (hex ASCII
        # unless noted otherwise):
        #   inductive ID (hex)
        #   serial number (hex)
        #   temperature
        #   conductivity
        #   time of science data
        #
        particle = [
            self._encode_value(DataParticleKey.SERIAL_NUMBER,
                               self.raw_data[RAW_INDEX_REC_CT_SERIAL], str),
            self._encode_value(DataParticleKey.TEMPERATURE,
                               self.raw_data[RAW_INDEX_REC_CT_TEMPERATURE],
                               convert_hex_ascii_to_int),
            self._encode_value(DataParticleKey.CONDUCTIVITY,
                               self.raw_data[RAW_INDEX_REC_CT_CONDUCTIVITY],
                               convert_hex_ascii_to_int),
            self._encode_value(DataParticleKey.CTD_TIME,
                               self.raw_data[RAW_INDEX_REC_CT_TIME],
                               convert_hex_ascii_to_int)
        ]

        return particle
