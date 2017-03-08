#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdmo_ghqr_sio
@file mi/dataset/parser/ctdmo_ghqr_sio.py
@author Emily Hahn (original telemetered), Steve Myerson (recovered)
@brief A CTDMO series ghqr specific data set agent parser

This file contains code for the CTDMO parsers and code to produce data particles.
For telemetered data, there is one parser which produces two data particles.
For recovered data, there are two parsers, with each parser producing one data particle.

There are two types of CTDMO data.
CT, aka instrument, sensor or science data.
CO, aka offset data.

For telemetered data, both types (CT, CO) of data are in SIO Mule files.
For recovered data, the CT data is stored in a separate file.
Additionally, both CT and CO data are stored in another file (SIO Controller file),
but only the CO data in the SIO Controller file is processed here,
with the CT data being ignored.
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import binascii
import re
import struct

from mi.dataset.parser.utilities import zulu_timestamp_to_ntp_time

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.sio_mule_common import \
    SioParser, \
    SIO_HEADER_MATCHER, \
    SIO_HEADER_GROUP_ID, \
    SIO_HEADER_GROUP_TIMESTAMP

from mi.core.common import BaseEnum
from mi.core.exceptions import \
    DatasetParserException, \
    RecoverableSampleException, \
    SampleException, \
    UnexpectedDataException

from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue

ID_INSTRUMENT = 'CT'    # ID for instrument (science) data
ID_OFFSET = 'CO'        # ID for time offset data

# Recovered CT file format (file is ASCII, lines separated by new line):
#   Several lines of unformatted ASCII text and key value pairs (ignored here)
#   Configuration information in XML format (only serial number is of interest)
#   *END* record (IDD says *end* so we'll check for either)
#   Instrument data in HEX ASCII (need to extract these values)

NEW_LINE = r'[\n\r]+'             # Handle any type of new line

REC_CT_RECORD = r'.*'             # Any number of ASCII characters
REC_CT_RECORD += NEW_LINE         # separated by a new line
REC_CT_RECORD_MATCHER = re.compile(REC_CT_RECORD)

# For Recovered CT files, the serial number is in the Configuration XML section.
REC_CT_SERIAL_REGEX = r'^'        # At the beginning of the record
REC_CT_SERIAL_REGEX += r'\* <HardwareData DeviceType=\'SBE37-IM\' SerialNumber=\''
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
REC_CT_SAMPLE_BYTES = 31                # includes record separator

REC_CT_REGEX = b'([0-9a-fA-F]{6})'      # Temperature
REC_CT_REGEX += b'([0-9a-fA-F]{6})'     # Conductivity
REC_CT_REGEX += b'([0-9a-fA-F]{6})'     # Pressure
REC_CT_REGEX += b'([0-9a-fA-F]{4})'     # Pressure Temperature
REC_CT_REGEX += b'([0-9a-fA-F]{8})'     # Time since Jan 1, 2000
REC_CT_REGEX += NEW_LINE                # separated by a new line
REC_CT_MATCHER = re.compile(REC_CT_REGEX)

# The REC_CT_MATCHER produces the following groups:
REC_CT_GROUP_TEMPERATURE = 1
REC_CT_GROUP_CONDUCTIVITY = 2
REC_CT_GROUP_PRESSURE = 3
REC_CT_GROUP_PRESSURE_TEMP = 4
REC_CT_GROUP_TIME = 5

# Telemetered CT Data record (binary):
TEL_CT_RECORD_END = b'\x0D'           # records separated by a new line
TEL_CT_SAMPLE_BYTES = 13              # includes record separator

TEL_CT_REGEX = b'([\x00-\xFF])'       # Inductive ID
TEL_CT_REGEX += b'([\x00-\xFF]{7})'   # Temperature, Conductivity, Pressure (reversed)
TEL_CT_REGEX += b'([\x00-\xFF]{4})'   # Time since Jan 1, 2000 (bytes reversed)
TEL_CT_REGEX += TEL_CT_RECORD_END     # CT Record separator
TEL_CT_MATCHER = re.compile(TEL_CT_REGEX)

# The TEL_CT_MATCHER produces the following groups:
TEL_CT_GROUP_ID = 1
TEL_CT_GROUP_SCIENCE_DATA = 2
TEL_CT_GROUP_TIME = 3

# Recovered and Telemetered CO Data record (binary):
CO_RECORD_END = b'[\x13|\x0D]'     # records separated by sentinel 0x13 or 0x0D
CO_SAMPLE_BYTES = 6

CO_REGEX = b'([\x00-\xFF])'        # Inductive ID
CO_REGEX += b'([\x00-\xFF]{4})'    # Time offset in seconds
CO_REGEX += CO_RECORD_END          # CO Record separator
CO_MATCHER = re.compile(CO_REGEX)

# The CO_MATCHER produces the following groups:
CO_GROUP_ID = 1
CO_GROUP_TIME_OFFSET = 2

# Indices into raw_data tuples for recovered CT data
RAW_INDEX_REC_CT_ID = 0
RAW_INDEX_REC_CT_SERIAL = 1
RAW_INDEX_REC_CT_TEMPERATURE = 2
RAW_INDEX_REC_CT_CONDUCTIVITY = 3
RAW_INDEX_REC_CT_PRESSURE = 4
RAW_INDEX_REC_CT_PRESSURE_TEMP = 5
RAW_INDEX_REC_CT_TIME = 6

# Indices into raw_data tuples for telemetered CT data
RAW_INDEX_TEL_CT_SIO_TIMESTAMP = 0
RAW_INDEX_TEL_CT_ID = 1
RAW_INDEX_TEL_CT_SCIENCE = 2
RAW_INDEX_TEL_CT_TIME = 3

# Indices into raw_data tuples for recovered and telemetered CO data
RAW_INDEX_CO_SIO_TIMESTAMP = 0
RAW_INDEX_CO_ID = 1
RAW_INDEX_CO_TIME_OFFSET = 2

INDUCTIVE_ID_KEY = 'inductive_id'


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
    REC_CO_PARTICLE = 'ctdmo_ghqr_offset_recovered'
    REC_CT_PARTICLE = 'ctdmo_ghqr_instrument_recovered'
    TEL_CO_PARTICLE = 'ctdmo_ghqr_sio_offset'
    TEL_CT_PARTICLE = 'ctdmo_ghqr_sio_mule_instrument'


class CtdmoInstrumentDataParticleKey(BaseEnum):
    CONTROLLER_TIMESTAMP = "sio_controller_timestamp"
    INDUCTIVE_ID = "inductive_id"
    SERIAL_NUMBER = "serial_number"
    TEMPERATURE = "temperature"
    CONDUCTIVITY = "conductivity"
    PRESSURE = "pressure"
    PRESSURE_TEMP = "pressure_temp"
    CTD_TIME = "ctd_time"


class CtdmoGhqrRecoveredInstrumentDataParticle(DataParticle):
    """
    Class for generating Instrument Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_CT_PARTICLE

    def _build_parsed_values(self):
        """
        Build parsed values for Telemetered Recovered Data Particle.
        Take something in the hex ASCII data values and turn it into a
        particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        #
        # The particle timestamp is the time contained in the CT instrument data.
        # This time field is number of seconds since Jan 1, 2000.
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
        #   pressure
        #   pressure temperature
        #   time of science data
        #
        particle = [
            self._encode_value(CtdmoInstrumentDataParticleKey.INDUCTIVE_ID,
                               self.raw_data[RAW_INDEX_REC_CT_ID], int),
            self._encode_value(CtdmoInstrumentDataParticleKey.SERIAL_NUMBER,
                               self.raw_data[RAW_INDEX_REC_CT_SERIAL], str),
            self._encode_value(CtdmoInstrumentDataParticleKey.TEMPERATURE,
                               self.raw_data[RAW_INDEX_REC_CT_TEMPERATURE],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.CONDUCTIVITY,
                               self.raw_data[RAW_INDEX_REC_CT_CONDUCTIVITY],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.PRESSURE,
                               self.raw_data[RAW_INDEX_REC_CT_PRESSURE],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.PRESSURE_TEMP,
                               self.raw_data[RAW_INDEX_REC_CT_PRESSURE_TEMP],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.CTD_TIME,
                               self.raw_data[RAW_INDEX_REC_CT_TIME],
                               convert_hex_ascii_to_int)
        ]

        return particle


class CtdmoGhqrSioTelemeteredInstrumentDataParticle(DataParticle):
    """
    Class for generating Instrument Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_CT_PARTICLE

    def _build_parsed_values(self):
        """
        Build parsed values for Telemetered Instrument Data Particle.
        Take something in the binary data values and turn it into a
        particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        #
        # Convert science data time to hex ascii.
        # The 4 byte time field is in reverse byte order.
        #
        hex_time = binascii.b2a_hex(self.raw_data[RAW_INDEX_TEL_CT_TIME])
        reversed_hex_time = hex_time[6:8] + hex_time[4:6] + hex_time[2:4] + hex_time[0:2]

        # convert from epoch in 2000 to epoch in 1900.
        time_stamp = generate_particle_timestamp(reversed_hex_time)
        self.set_internal_timestamp(timestamp=time_stamp)

        try:
            #
            # Convert binary science data to hex ascii string.
            # 7 binary bytes get turned into 14 hex ascii bytes.
            # The 2 byte pressure field is in reverse byte order.
            #
            science_data = binascii.b2a_hex(self.raw_data[RAW_INDEX_TEL_CT_SCIENCE])
            pressure = science_data[12:14] + science_data[10:12]

        except (ValueError, TypeError, IndexError) as ex:
            log.warn("Error (%s) while decoding parameters in data: [%s]", ex, self.raw_data)
            raise RecoverableSampleException("Error (%s) while decoding parameters in data: [%s]"
                                             % (ex, self.raw_data))

        particle = [
            self._encode_value(CtdmoInstrumentDataParticleKey.CONTROLLER_TIMESTAMP,
                               self.raw_data[RAW_INDEX_TEL_CT_SIO_TIMESTAMP],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.INDUCTIVE_ID,
                               struct.unpack('>B',
                               self.raw_data[RAW_INDEX_TEL_CT_ID])[0],
                               int),
            self._encode_value(CtdmoInstrumentDataParticleKey.TEMPERATURE,
                               science_data[0:5],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.CONDUCTIVITY,
                               science_data[5:10],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.PRESSURE,
                               pressure,
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoInstrumentDataParticleKey.CTD_TIME,
                               reversed_hex_time,
                               convert_hex_ascii_to_int)
        ]

        return particle


class CtdmoOffsetDataParticleKey(BaseEnum):
    CONTROLLER_TIMESTAMP = "sio_controller_timestamp"
    INDUCTIVE_ID = "inductive_id"
    CTD_OFFSET = "ctd_time_offset"


class CtdmoGhqrSioOffsetDataParticle(DataParticle):
    """
    Class for generating the Offset Data Particle from the CTDMO instrument
    on a MSFM platform node
    """

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Offset Data Particle.
        Take something in the binary data values and turn it into a
        particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        #
        # The particle timestamp for CO data is the SIO header timestamp.
        #
        time_stamp = convert_hex_ascii_to_int(self.raw_data[RAW_INDEX_CO_SIO_TIMESTAMP])
        self.set_internal_timestamp(unix_time=time_stamp)

        particle = [
            self._encode_value(CtdmoOffsetDataParticleKey.CONTROLLER_TIMESTAMP,
                               self.raw_data[RAW_INDEX_CO_SIO_TIMESTAMP],
                               convert_hex_ascii_to_int),
            self._encode_value(CtdmoOffsetDataParticleKey.INDUCTIVE_ID,
                               struct.unpack('>B', self.raw_data[RAW_INDEX_CO_ID])[0],
                               int),
            self._encode_value(CtdmoOffsetDataParticleKey.CTD_OFFSET,
                               struct.unpack('>i',
                               self.raw_data[RAW_INDEX_CO_TIME_OFFSET])[0],
                               int)
        ]

        return particle


class CtdmoGhqrSioRecoveredOffsetDataParticle(CtdmoGhqrSioOffsetDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_CO_PARTICLE


class CtdmoGhqrSioTelemeteredOffsetDataParticle(CtdmoGhqrSioOffsetDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_CO_PARTICLE


def parse_co_data(particle_class, chunk, sio_header_timestamp, extract_sample):
    """
    This function parses a CO record and returns a list of samples.
    The CO input record is the same for both recovered and telemetered data.
    """
    particles = []
    last_index = len(chunk)
    start_index = 0
    had_error = (False, 0)

    while start_index < last_index:
        #
        # Look for a match in the next group of bytes
        #
        co_match = CO_MATCHER.match(
            chunk[start_index:start_index+CO_SAMPLE_BYTES])

        if co_match is not None:
            #
            # If the inductive ID is the one we're looking for,
            # generate a data particle.
            # The ID needs to be converted from a byte string to an integer
            # for the comparison.
            #
            inductive_id = co_match.group(CO_GROUP_ID)
            #
            # Generate the data particle.
            # Data stored for each particle is a tuple of the following:
            #   SIO header timestamp (input parameter)
            #   inductive ID (from chunk)
            #   Time Offset (from chunk)
            #
            sample = extract_sample(particle_class, None, (sio_header_timestamp, inductive_id,
                                    co_match.group(CO_GROUP_TIME_OFFSET)), None)
            if sample is not None:
                #
                # Add this particle to the list of particles generated
                # so far for this chunk of input data.
                #
                particles.append(sample)

            start_index += CO_SAMPLE_BYTES
        #
        # If there wasn't a match, the input data is messed up.
        #
        else:
            had_error = (True, start_index)
            break

    #
    # Once we reach the end of the input data,
    # return the number of particles generated and the list of particles.
    #
    return particles, had_error


class CtdmoGhqrSioRecoveredCoParser(SioParser):

    """
    Parser for Ctdmo recovered CO data.
    """

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        if non_data is not None and non_end <= start:
            message = "Found %d bytes of un-expected non-data %s" % (len(non_data), binascii.b2a_hex(non_data))
            log.warn(message)
            self._exception_callback(UnexpectedDataException(message))

    def parse_chunks(self):
        """
        Parse chunks for the Recovered CO parser.
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()
        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:
            header_match = SIO_HEADER_MATCHER.match(chunk)
            header_timestamp = header_match.group(SIO_HEADER_GROUP_TIMESTAMP)

            #
            # Start processing at the end of the header.
            #
            chunk_idx = header_match.end(0)

            if header_match.group(SIO_HEADER_GROUP_ID) == ID_OFFSET:
                (particles, had_error) = parse_co_data(CtdmoGhqrSioRecoveredOffsetDataParticle,
                                                       chunk[chunk_idx:-1], header_timestamp,
                                                       self._extract_sample)

                if had_error[0]:
                    log.error('unknown data found in CO chunk %s at %d, leaving out the rest',
                              binascii.b2a_hex(chunk), had_error[1])
                    self._exception_callback(SampleException(
                        'unknown data found in CO chunk at %d, leaving out the rest' % had_error[1]))

                result_particles.extend(particles)

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()
            self.handle_non_data(non_data, non_end, start)

        return result_particles


class CtdmoGhqrRecoveredCtParser(SimpleParser):

    """
    Parser for Ctdmo recovered CT data.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        #
        # Verify that the required parameters are in the parser configuration.
        #
        if not INDUCTIVE_ID_KEY in config:
            raise DatasetParserException("Parser config is missing %s" % INDUCTIVE_ID_KEY)

        #
        # File is ASCII with records separated by newlines.
        #
        super(CtdmoGhqrRecoveredCtParser, self).__init__(config, stream_handle, exception_callback)

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
        # See if this record the serial number.
        # If found, convert from decimal ASCII and save.
        #
        match = REC_CT_SERIAL_MATCHER.match(chunk)
        if match is not None:
            self._serial_number = int(match.group(REC_CT_SERIAL_GROUP_SERIAL_NUMBER))

    def parse_file(self):
        """
        Parser the file for the recovered CT parser
        :return: list of result particles
        """

        # read the first line in the file
        line = self._stream_handle.readline()

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
            # Once the end of the configuration is reached, all remaining records
            # are supposedly CT data records.
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
            #   pressure
            #   pressure temperature
            #   time of science data
            #
            sample = self._extract_sample(CtdmoGhqrRecoveredInstrumentDataParticle, None,
                                         (self._config.get(INDUCTIVE_ID_KEY),
                                          self._serial_number,
                                          ct_match.group(REC_CT_GROUP_TEMPERATURE),
                                          ct_match.group(REC_CT_GROUP_CONDUCTIVITY),
                                          ct_match.group(REC_CT_GROUP_PRESSURE),
                                          ct_match.group(REC_CT_GROUP_PRESSURE_TEMP),
                                          ct_match.group(REC_CT_GROUP_TIME)), None)

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


class CtdmoGhqrSioTelemeteredParser(SioParser):
    """
    Parser for Ctdmo telemetered data (SIO Mule).
    This parser handles both CT and CO data from the SIO Mule.
    """

    def parse_chunks(self):
        """
        Parse chunks for the Telemetered parser.
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()
        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:
            header_match = SIO_HEADER_MATCHER.match(chunk)
            if header_match:
                header_timestamp = header_match.group(SIO_HEADER_GROUP_TIMESTAMP)

                # start looping at the end of the header
                chunk_idx = header_match.end(0)

                if header_match.group(SIO_HEADER_GROUP_ID) == ID_INSTRUMENT:
                    #
                    # Parse the CT record, up to but not including the end of SIO block.
                    #
                    particles = self.parse_ct_record(chunk[chunk_idx:-1], header_timestamp)
                    result_particles.extend(particles)

                elif header_match.group(SIO_HEADER_GROUP_ID) == ID_OFFSET:
                    (particles, had_error) = parse_co_data(CtdmoGhqrSioTelemeteredOffsetDataParticle,
                                                           chunk[chunk_idx:-1], header_timestamp,
                                                           self._extract_sample)

                    if had_error[0]:
                        log.error('unknown data found in CO chunk %s at %d, leaving out the rest',
                                  binascii.b2a_hex(chunk), had_error[1])
                        self._exception_callback(SampleException(
                            'unknown data found in CO chunk at %d, leaving out the rest' % had_error[1]))

                    result_particles.extend(particles)
                else:
                    message = 'Unexpected Sio Header ID %s' % header_match.group(SIO_HEADER_GROUP_ID)
                    log.warn(message)
                    self._exception_callback(UnexpectedDataException(message))

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()
            self.handle_non_data(non_data, non_end, start)

        return result_particles

    def parse_ct_record(self, ct_record, sio_header_timestamp):
        """
        This function parses a Telemetered CT record and
        returns a list of data particles.
        Parameters:
          chunk - the input which is being parsed
          sio_header_timestamp - required for particle, passed through
        """
        particles = []
        last_index = len(ct_record)
        start_index = 0

        while start_index < last_index:
            #
            # Look for a match in the next group of bytes
            #
            ct_match = TEL_CT_MATCHER.match(
                ct_record[start_index:start_index+TEL_CT_SAMPLE_BYTES])

            if ct_match is not None:
                #
                # Generate the data particle.
                # Data stored for each particle is a tuple of the following:
                #   SIO header timestamp (input parameter)
                #   inductive ID
                #   science data (temperature, conductivity, pressure)
                #   time of science data
                #
                sample = self._extract_sample(CtdmoGhqrSioTelemeteredInstrumentDataParticle, None,
                                              (sio_header_timestamp,
                                               ct_match.group(TEL_CT_GROUP_ID),
                                               ct_match.group(TEL_CT_GROUP_SCIENCE_DATA),
                                               ct_match.group(TEL_CT_GROUP_TIME)), None)
                if sample is not None:
                    #
                    # Add this particle to the list of particles generated
                    # so far for this chunk of input data.
                    #
                    particles.append(sample)

                start_index += TEL_CT_SAMPLE_BYTES

            #
            # If there wasn't a match, the input data is messed up.
            #
            else:
                log.error('unknown data found in CT record %s at %d, leaving out the rest',
                          binascii.b2a_hex(ct_record), start_index)
                self._exception_callback(SampleException(
                    'unknown data found in CT record at %d, leaving out the rest' % start_index))
                break

        #
        # Once we reach the end of the input data,
        # return the number of particles generated and the list of particles.
        #
        return particles

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        if non_data is not None and non_end <= start:
            message = "Found %d bytes of un-expected non-data %s" % (len(non_data), binascii.b2a_hex(non_data))
            log.warn(message)
            self._exception_callback(UnexpectedDataException(message))
