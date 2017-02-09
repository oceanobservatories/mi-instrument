#!/usr/bin/env python

"""
@package mi.dataset.parser.optaa_dj_dcl
@file marine-integrations/mi/dataset/parser/optaa_dj_dcl.py
@author Jeff Roy (Raytheon)
@brief Parser for the optaa_dj_dcl dataset driver

This file contains code for the optaa_dj_dcl parsers and code to produce data particles.
For telemetered data, there is one parser which produces two types of data particles.
For recovered data, there is one parser which produces two types of data particles.
Both parsers produce instrument and metadata data particles.
There is 1 metadata data particle produced for each file.
There is 1 instrument data particle produced for each record in a file.
The input files and the content of the data particles are the same for both
recovered and telemetered.
Only the names of the output particle streams are different.

Input files are binary with variable length records.

Release notes:

Initial release
"""

import calendar
from ctypes import BigEndianStructure, c_ushort, c_uint, c_ubyte
from _ctypes import sizeof

from io import BytesIO
from collections import OrderedDict, namedtuple
import re
import ntplib
import struct

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.exceptions import \
    DatasetParserException, \
    RecoverableSampleException, \
    UnexpectedDataException

from mi.core.instrument.dataset_data_particle import DataParticle
from mi.dataset.dataset_parser import SimpleParser

log = get_logger()

__author__ = 'Jeff Roy (Raytheon)'
__license__ = 'Apache 2.0'

SIZE_CHECKSUM = 2                    # number of bytes for checksum in the input
SIZE_PAD = 1                         # number of bytes for trailing pad in the input
MARKER_SIZE = 4  # 4 bytes are used for the packet marker
START_MARKER = b'\xFF\x00\xFF\x00'        # all packets start with 0xFF00FF00

DATE = r'(\d{4})(\d{2})(\d{2})'      # Date: YYYYMMDD
TIME = r'(\d{2})(\d{2})(\d{2})'      # Time: HHMMSS

# Define a regex to parse the filename.  The dot character was added to ensure that the uFrame
# filename changing still results in capturing the date and time and not other extraneous characters
FILENAME_REGEX = DATE + '_' + TIME + '\.'
FILENAME_MATCHER = re.compile(FILENAME_REGEX)

# FILENAME_MATCHER produces the following match.group() indices.
GROUP_YEAR = 1
GROUP_MONTH = 2
GROUP_DAY = 3
GROUP_HOUR = 4
GROUP_MINUTE = 5
GROUP_SECOND = 6


# define all the string literals used as keys
class Keys (BaseEnum):
    # parameters in the instrument particle
    A_REFERENCE_DARK_COUNTS = 'a_reference_dark_counts'
    PRESSURE_COUNTS = 'pressure_counts'
    A_SIGNAL_DARK_COUNTS = 'a_signal_dark_counts'
    EXTERNAL_TEMP_RAW = 'external_temp_raw'
    INTERNAL_TEMP_RAW = 'internal_temp_raw'
    C_REFERENCE_DARK_COUNTS = 'c_reference_dark_counts'
    C_SIGNAL_DARK_COUNTS = 'c_signal_dark_counts'
    ELAPSED_RUN_TIME = 'elapsed_run_time'
    NUM_WAVELENGTHS = 'num_wavelengths'
    C_REFERENCE_COUNTS = 'c_reference_counts'
    A_REFERENCE_COUNTS = 'a_reference_counts'
    C_SIGNAL_COUNTS = 'c_signal_counts'
    A_SIGNAL_COUNTS = 'a_signal_counts'
    # parameters in the metadata particle
    START_DATE = 'start_date'
    PACKET_TYPE = 'packet_type'
    METER_TYPE = 'meter_type'
    SERIAL_NUMBER = 'serial_number'
    # fields in the input record that are not directly output
    PACKET_MARKER = 'packet_marker'
    RECORD_LENGTH = 'record_length'
    RESERVED = 'reserved'
    SERIAL_NUMBER_HIGH = 'serial_number_high'
    SERIAL_NUMBER_LOW = 'serial_number_low'
    TIME_HIGH = 'time_high'
    TIME_LOW = 'time_low'
    RESERVED2 = 'reserved2'

ParameterMap = namedtuple('ParameterKey', ['name', 'function'])

INSTRUMENT_PARTICLE_MAP = [
    ParameterMap(Keys.A_REFERENCE_DARK_COUNTS, int),
    ParameterMap(Keys.PRESSURE_COUNTS, int),
    ParameterMap(Keys.A_SIGNAL_DARK_COUNTS, int),
    ParameterMap(Keys.EXTERNAL_TEMP_RAW, int),
    ParameterMap(Keys.INTERNAL_TEMP_RAW, int),
    ParameterMap(Keys.C_REFERENCE_DARK_COUNTS, int),
    ParameterMap(Keys.C_SIGNAL_DARK_COUNTS, int),
    ParameterMap(Keys.ELAPSED_RUN_TIME, int),
    ParameterMap(Keys.NUM_WAVELENGTHS, int),
    ParameterMap(Keys.C_REFERENCE_COUNTS, list),
    ParameterMap(Keys.A_REFERENCE_COUNTS, list),
    ParameterMap(Keys.C_SIGNAL_COUNTS, list),
    ParameterMap(Keys.A_SIGNAL_COUNTS, list)
]

METADATA_PARTICLE_MAP = [
    ParameterMap(Keys.START_DATE, str),
    ParameterMap(Keys.PACKET_TYPE, int),
    ParameterMap(Keys.METER_TYPE, int),
    ParameterMap(Keys.SERIAL_NUMBER, str)
]


class DataParticleType(BaseEnum):
    REC_INSTRUMENT_PARTICLE = 'optaa_dj_dcl_instrument_recovered'
    REC_METADATA_PARTICLE = 'optaa_dj_dcl_metadata_recovered'
    TEL_INSTRUMENT_PARTICLE = 'optaa_dj_dcl_instrument'
    TEL_METADATA_PARTICLE = 'optaa_dj_dcl_metadata'


# helper class for simple unpacking of data
class OptaaSampleHeader(BigEndianStructure):
    _fields_ = [
        (Keys.PACKET_MARKER, c_uint),
        (Keys.RECORD_LENGTH, c_ushort),
        (Keys.PACKET_TYPE, c_ubyte),
        (Keys.RESERVED, c_ubyte),
        (Keys.METER_TYPE, c_ubyte),
        (Keys.SERIAL_NUMBER_HIGH, c_ubyte),
        (Keys.SERIAL_NUMBER_LOW, c_ushort),
        (Keys.A_REFERENCE_DARK_COUNTS, c_ushort),
        (Keys.PRESSURE_COUNTS, c_ushort),
        (Keys.A_SIGNAL_DARK_COUNTS, c_ushort),
        (Keys.EXTERNAL_TEMP_RAW, c_ushort),
        (Keys.INTERNAL_TEMP_RAW, c_ushort),
        (Keys.C_REFERENCE_DARK_COUNTS, c_ushort),
        (Keys.C_SIGNAL_DARK_COUNTS, c_ushort),
        (Keys.TIME_HIGH, c_ushort),
        (Keys.TIME_LOW, c_ushort),
        (Keys.RESERVED2, c_ubyte),
        (Keys.NUM_WAVELENGTHS, c_ubyte),
    ]

    @staticmethod
    def from_string(input_str):
        header = OptaaSampleHeader()
        BytesIO(input_str).readinto(header)
        return header

    def __str__(self):
        d = OrderedDict()
        for name, _ in self._fields_:
            d[name] = getattr(self, name)
        return str(d)


class OptaaDjDclInstrumentDataParticle(DataParticle):
    """
    Class for generating the Optaa_dj instrument particle.
    """

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[name], function)
                for name, function in INSTRUMENT_PARTICLE_MAP]


class OptaaDjDclRecoveredInstrumentDataParticle(OptaaDjDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_INSTRUMENT_PARTICLE


class OptaaDjDclTelemeteredInstrumentDataParticle(OptaaDjDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_INSTRUMENT_PARTICLE


class OptaaDjDclMetadataDataParticle(DataParticle):
    """
    Class for generating the Optaa_dj Metadata particle.
    """

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Metadata Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Metadata Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[name], function)
                for name, function in METADATA_PARTICLE_MAP]


class OptaaDjDclRecoveredMetadataDataParticle(OptaaDjDclMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_METADATA_PARTICLE


class OptaaDjDclTelemeteredMetadataDataParticle(OptaaDjDclMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_METADATA_PARTICLE


class OptaaDjDclParser(SimpleParser):
    """
    Parser for Optaa_dj_dcl data.
    In addition to the standard parser constructor parameters,
    this constructor needs the following additional parameters:
      filename - Name of file being parsed
      is_telemetered - Set True if telemetered particle streams are desired.

    """
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 filename,
                 is_telemetered):

        super(OptaaDjDclParser, self).__init__(config, stream_handle, exception_callback)

        if is_telemetered:
            self.instrument_particle_class = OptaaDjDclTelemeteredInstrumentDataParticle
            self.metadata_particle_class = OptaaDjDclTelemeteredMetadataDataParticle
        else:
            self.instrument_particle_class = OptaaDjDclRecoveredInstrumentDataParticle
            self.metadata_particle_class = OptaaDjDclRecoveredMetadataDataParticle

        # Extract the start date and time from the filename and convert
        # it to the format expected for the output particle.
        # Calculate the ntp_time timestamp, the number of seconds since Jan 1, 1900,
        # based on the date and time from the filename.
        # This is the start time.  Timestamps for each particle are derived from
        # the start time.

        filename_match = FILENAME_MATCHER.search(filename)
        if filename_match is not None:
            self.start_date = \
                filename_match.group(GROUP_YEAR) + '-' + \
                filename_match.group(GROUP_MONTH) + '-' + \
                filename_match.group(GROUP_DAY) + ' ' + \
                filename_match.group(GROUP_HOUR) + ':' + \
                filename_match.group(GROUP_MINUTE) + ':' + \
                filename_match.group(GROUP_SECOND)
            timestamp = (
                int(filename_match.group(GROUP_YEAR)),
                int(filename_match.group(GROUP_MONTH)),
                int(filename_match.group(GROUP_DAY)),
                int(filename_match.group(GROUP_HOUR)),
                int(filename_match.group(GROUP_MINUTE)),
                int(filename_match.group(GROUP_SECOND)),
                0, 0, 0)

            # The timestamp for each particle is:
            # timestamp = start_time_from_file_name + (tn - t0)
            # where t0 is the time since power-up in the first record.

            elapsed_seconds = calendar.timegm(timestamp)
            self.ntp_time = ntplib.system_to_ntp_time(elapsed_seconds)

        else:
            error_message = 'Invalid filename %s' % filename
            log.warn(error_message)
            raise DatasetParserException(error_message)

    def parse_file(self):

        position = 0
        metadata_generated = False

        packet_id_bytes = self._stream_handle.read(MARKER_SIZE)  # read the first four bytes of the file

        while packet_id_bytes:  # will be None when EOF is found

            if packet_id_bytes == START_MARKER:  # we found the marker
                length_bytes = self._stream_handle.read(2)
                packet_length = struct.unpack('>H', length_bytes)[0]

                self._stream_handle.seek(position)  # reset to beginning of packet
                # read entire packet
                packet_buffer = self._stream_handle.read(packet_length + SIZE_CHECKSUM + SIZE_PAD)

                # first check that the packet passes the checksum
                expected_checksum = struct.unpack_from('>H', packet_buffer, packet_length)[0]

                actual_checksum = sum(bytearray(packet_buffer[:-(SIZE_CHECKSUM + SIZE_PAD)])) & 0xFFFF

                if actual_checksum == expected_checksum:

                    # unpack the header part of the packet using BigEndianStructure utility
                    packet_header = OptaaSampleHeader.from_string(packet_buffer)
                    # unpack the rest of the packet data now that we have num_wavelengths
                    packet_data = struct.unpack_from('>%dH' % (packet_header.num_wavelengths*4),
                                                     packet_buffer, sizeof(packet_header))

                    cref = packet_data[::4]  # slice the data up using a step of 4
                    aref = packet_data[1::4]
                    csig = packet_data[2::4]
                    asig = packet_data[3::4]

                    # Extract the number of milliseconds since power-up.
                    elapsed_milli = (packet_header.time_high << 16) + packet_header.time_low
                    time_since_power_up = elapsed_milli / 1000.0

                    if not metadata_generated:  # generate 1 metadata particle per file

                        serial_number = (packet_header.serial_number_high << 16) + packet_header.serial_number_low
                        # package up the metadata parameters for the particle to decode
                        metadata = {
                            Keys.START_DATE: self.start_date,
                            Keys.PACKET_TYPE: packet_header.packet_type,
                            Keys.METER_TYPE: packet_header.meter_type,
                            Keys.SERIAL_NUMBER: serial_number
                        }

                        # create the metadata particle
                        meta_particle = self._extract_sample(self.metadata_particle_class,
                                                             None, metadata, self.ntp_time)
                        self._record_buffer.append(meta_particle)

                        # Adjust the ntp_time at power-up by
                        # the time since power-up of the first record.
                        self.ntp_time -= time_since_power_up

                        metadata_generated = True

                    # package up the instrument data parameters for the particle to decode
                    instrument_data = {Keys.A_REFERENCE_DARK_COUNTS: packet_header.a_reference_dark_counts,
                                       Keys.PRESSURE_COUNTS: packet_header.pressure_counts,
                                       Keys.A_SIGNAL_DARK_COUNTS: packet_header.a_signal_dark_counts,
                                       Keys.EXTERNAL_TEMP_RAW: packet_header.external_temp_raw,
                                       Keys.INTERNAL_TEMP_RAW: packet_header.internal_temp_raw,
                                       Keys.C_REFERENCE_DARK_COUNTS: packet_header.c_reference_dark_counts,
                                       Keys.C_SIGNAL_DARK_COUNTS: packet_header.c_signal_dark_counts,
                                       Keys.ELAPSED_RUN_TIME: elapsed_milli,
                                       Keys.NUM_WAVELENGTHS: packet_header.num_wavelengths,
                                       Keys.C_REFERENCE_COUNTS: cref,
                                       Keys.A_REFERENCE_COUNTS: aref,
                                       Keys.C_SIGNAL_COUNTS: csig,
                                       Keys.A_SIGNAL_COUNTS: asig
                                       }

                    # create the instrument particle
                    data_particle = self._extract_sample(self.instrument_particle_class,
                                                         None, instrument_data, self.ntp_time + time_since_power_up)
                    self._record_buffer.append(data_particle)

                else:  # bad checksum
                    self._exception_callback(RecoverableSampleException(
                        'Checksum error.  Actual %d vs Expected %d' %
                        (actual_checksum, expected_checksum)))

            else:  # packet_id did not match, unexpected data

                self._exception_callback(UnexpectedDataException(
                    'Invalid OPTAA Packet ID found, checking next 4 bytes'))

            position = self._stream_handle.tell()  # set the new file position
            packet_id_bytes = self._stream_handle.read(MARKER_SIZE)  # read the next two bytes of the file
