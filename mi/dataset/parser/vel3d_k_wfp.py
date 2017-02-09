#!/usr/bin/env python

"""
@package mi.dataset.parser.vel3d_k_wfp
@file marine-integrations/mi/dataset/parser/vel3d_k_wfp.py
@author Steve Myerson (Raytheon)
@brief Parser for the vel3d_k_wfp dataset driver
Release notes:

Initial Release
"""

#
# The VEL3D_K_WFP input file is a binary file.
# The file header is a 4 byte field which is the total size of all the data records.
# The file header is not used.
#
# The data records consist of 2 parts: a data header and a data payload.
# The data header contains a sync word, IDs, field lengths, and checksums.
# The data payload contains the parameters needed to generate instrument particles.
#
# The last record in the file is a time record containing the start and end times.
#

import ntplib
import re
import struct

from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import \
    RecoverableSampleException, \
    SampleException, \
    UnexpectedDataException

from mi.dataset.dataset_parser import SimpleParser

log = get_logger()

__author__ = 'Steve Myerson (Raytheon)'
__license__ = 'Apache 2.0'

FILE_HEADER_RECORD_SIZE = 4  # bytes

#
# Divide the data header into groups.
# Group Field           Size   Description
#       Sync            byte   Always 0xA5
#  1    Header Size     ubyte  Number of bytes in the header (should be 10)
#  2    ID              byte   Data type: 0x15 = Burst, 0x16 = CP, 0xA0 = string
#  3    Family          byte   Instrument Family: 0x10 = AD2CP
#  4    Data Size       uint16 Number of bytes in following data record
#  5    Data checksum   int16  Checksum of the following data record
#  6    Header checksum int16  Checksum of the data header, excluding itself
#
DATA_HEADER_REGEX = b"""\xA5(?P<header_size>[\x00-\xFF])
                        (?P<header_id>[\x15|\x16|\xA0])
                        (?P<header_family>\x10)
                        (?P<data_size>[\x00-\xFF]{2})
                        (?P<data_checksum>[\x00-\xFF]{2})
                        (?P<header_checksum>[\x00-\xFF]{2})"""

DATA_HEADER_MATCHER = re.compile(DATA_HEADER_REGEX, re.VERBOSE)

DATA_HEADER_SIZE = 10        # expected length in bytes
DATA_HEADER_FAMILY = 0x10    # expected instrument family
DATA_HEADER_ID_BURST_DATA = 0x15
DATA_HEADER_ID_CP_DATA = 0x16
DATA_HEADER_ID_STRING = 0xA0
DATA_HEADER_CHECKSUM_LENGTH = (DATA_HEADER_SIZE / 2) - 1  # sum of 16-bit values

#
# Keys and unpack formats to be used when generating instrument particles.
# They are listed in order corresponding to the data record payload.
# Note that the ID field, extracted from the data record header,
# is added to the end of the list.
#
INSTRUMENT_PARTICLE_MAP = \
    [
        ('vel3d_k_version', 'B'),
        (None, 'B'),                    # offsetOfData not included in particle
        ('vel3d_k_serial', 'I'),
        ('vel3d_k_configuration', 'H'),
        ('date_time_array',      '6B'),   # year, month, day, hour, minute, seconds
        ('vel3d_k_micro_second', 'H'),
        ('vel3d_k_speed_sound', 'H'),
        ('vel3d_k_temp_c', 'h'),
        ('vel3d_k_pressure',  'I'),
        ('vel3d_k_heading', 'H'),
        ('vel3d_k_pitch', 'h'),
        ('vel3d_k_roll', 'h'),
        ('vel3d_k_error', 'H'),
        ('vel3d_k_status', 'H'),
        ('vel3d_k_beams_coordinate', 'H'),
        ('vel3d_k_cell_size', 'H'),
        ('vel3d_k_blanking', 'H'),
        ('vel3d_k_velocity_range', 'H'),
        ('vel3d_k_battery_voltage', 'H'),
        ('vel3d_k_mag_x', 'h'),
        ('vel3d_k_mag_y', 'h'),
        ('vel3d_k_mag_z', 'h'),
        ('vel3d_k_acc_x', 'h'),
        ('vel3d_k_acc_y', 'h'),
        ('vel3d_k_acc_z', 'h'),
        ('vel3d_k_ambiguity', 'H'),
        ('vel3d_k_data_set_description', 'H'),
        ('vel3d_k_transmit_energy', 'H'),
        ('vel3d_k_v_scale', 'b'),
        ('vel3d_k_power_level', 'b'),
        (None,   'l'),             # unused not included in particle
        ('vel3d_k_vel0', 'h'),
        ('vel3d_k_vel1', 'h'),
        ('vel3d_k_vel2', 'h'),
        ('vel3d_k_amp0', 'B'),
        ('vel3d_k_amp1', 'B'),
        ('vel3d_k_amp2', 'B'),
        ('vel3d_k_corr0', 'B'),
        ('vel3d_k_corr1', 'B'),
        ('vel3d_k_corr2', 'B'),
        ('vel3d_k_id', '')   # this parameter is from the header and not unpacked as part of payload
    ]


DATE_TIME_ARRAY = 'date_time_array'    # This one needs to be special-cased
DATE_TIME_SIZE = 6                     # 6 bytes for the output date time field
DATA_SET_DESCRIPTION = 'vel3d_k_data_set_description'    # special case

INDEX_STRING_ID = 0   # field number within a string record
INDEX_STRING = 1      # field number within a string record

TIME_RECORD_SIZE = 8  # bytes
TIME_FORMAT = '>2I'   # 2 32-bit unsigned integers big endian
INDEX_TIME_ON = 0     # field number within Time record and raw_data
INDEX_TIME_OFF = 1    # field number within Time record and raw_data
SAMPLE_RATE = .5      # data records sample rate


class Vel3dKWfpDataParticleType(BaseEnum):
    INSTRUMENT_PARTICLE = 'vel3d_k_wfp_instrument'
    METADATA_PARTICLE = 'vel3d_k_wfp_metadata'
    STRING_PARTICLE = 'vel3d_k_wfp_string'


class Vel3dKWfpMetadataParticleKey(BaseEnum):
    TIME_OFF = 'vel3d_k_time_off'
    TIME_ON = 'vel3d_k_time_on'


class Vel3dKWfpStringParticleKey(BaseEnum):
    STRING_ID = 'vel3d_k_str_id'
    STRING = 'vel3d_k_string'


class Vel3dKWfpInstrumentParticle(DataParticle):
    """
    Class for generating vel3d_k_wfp instrument particles.
    """

    _data_particle_type = Vel3dKWfpDataParticleType.INSTRUMENT_PARTICLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into an array of
        dictionaries defining the data in the data_array with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        #
        # Generate an Instrument data data_array.
        # Note that raw_data already contains the individual fields
        # extracted and unpacked from the data record.
        #
        data_array = []
        field = 0
        for key, code in INSTRUMENT_PARTICLE_MAP:
            if key is not None:
                if key == DATE_TIME_ARRAY:
                    time_array = self.raw_data[field: field + DATE_TIME_SIZE]
                    data_array.append({DataParticleKey.VALUE_ID: key,
                                      DataParticleKey.VALUE: list(time_array)})
                    field += DATE_TIME_SIZE

                elif key == DATA_SET_DESCRIPTION:
                    #
                    # The data set description field contains 5 3-bit values.
                    # We extract each 3-bit value and put them in the data_array
                    # as an array.
                    #
                    value = self.raw_data[field]

                    data_array.append({DataParticleKey.VALUE_ID: key,
                                      DataParticleKey.VALUE: [value & 0x7,
                                                              (value >> 3) & 0x7,
                                                              (value >> 6) & 0x7,
                                                              (value >> 9) & 0x7,
                                                              (value >> 12) & 0x7]})

                    field += 1

                else:
                    key_value = self._encode_value(key, self.raw_data[field], int)
                    data_array.append(key_value)
                    field += 1
            else:
                field += 1

        return data_array


class Vel3dKWfpMetadataParticle(DataParticle):
    """
    Class for generating vel3d_k_wfp metadata particles.
    """

    _data_particle_type = Vel3dKWfpDataParticleType.METADATA_PARTICLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into an array of
        dictionaries defining the data in the data_array with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        #
        # Generate a Metadata data data_array.
        # Note that raw_data already contains the individual fields
        # extracted and unpacked from the data record.
        #
        data_array = [
            self._encode_value(Vel3dKWfpMetadataParticleKey.TIME_ON,
                               self.raw_data[INDEX_TIME_ON], int),
            self._encode_value(Vel3dKWfpMetadataParticleKey.TIME_OFF,
                               self.raw_data[INDEX_TIME_OFF], int)
        ]

        return data_array


class Vel3dKWfpStringParticle(DataParticle):
    """
    Class for generating vel3d_k_wfp string particles.
    """

    _data_particle_type = Vel3dKWfpDataParticleType.STRING_PARTICLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into an array of
        dictionaries defining the data in the data_array with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        #
        # Generate a String data data_array.
        # Note that raw_data already contains the individual fields
        # extracted and unpacked from the time data record.
        #
        data_array = [
            self._encode_value(Vel3dKWfpStringParticleKey.STRING_ID,
                               self.raw_data[INDEX_STRING_ID], int),
            self._encode_value(Vel3dKWfpStringParticleKey.STRING,
                               self.raw_data[INDEX_STRING], str)
        ]

        return data_array


class Vel3dKWfpParser(SimpleParser):

    @staticmethod
    def calculate_checksum(input_buffer, values):
        """
        This function calculates a 16-bit unsigned sum of 16-bit data.
        Parameters:
          @param input_buffer - Buffer containing the values to be summed
          @param values - Number of 16-bit values to sum
        Returns:
          Calculated checksum
        """

        checksum = 0xB58C  # initial value per Nortek's Integrator's Guide
        index = 0
        for x in range(0, values):
            checksum += struct.unpack('<H', input_buffer[index: index + 2])[0]
            index += 2

        #
        # Modulo 65535
        #
        return checksum & 0xFFFF

    def report_error(self, exception, error_message):
        """
        This function reports an error condition by issuing a warning
        and raising an exception.
        Parameters:
          @param exception - type of exception to raise
          @param error_message - accompanying text
        """
        log.warn(error_message)
        self._exception_callback(exception(error_message))

    @staticmethod
    def build_payload_format():

        format_string = '<'
        for key, code in INSTRUMENT_PARTICLE_MAP:
            format_string += code

        return format_string

    def parse_file(self):

        # Read the Time record which is at the very end of the file.
        # Check for end of file.
        # If not reached, parse the Time record.
        # 2 = from end of file

        payload_format = self.build_payload_format()

        self._stream_handle.seek(0 - TIME_RECORD_SIZE, 2)
        time_record = self._stream_handle.read(TIME_RECORD_SIZE)

        if len(time_record) != TIME_RECORD_SIZE:
            self.report_error(SampleException, 'EOF reading time record')
            # if there was less than 8 bytes exit
            return

        times = struct.unpack(TIME_FORMAT, time_record)

        # go back to beginning of file, skip the file header
        self._stream_handle.seek(FILE_HEADER_RECORD_SIZE, 0)

        record_count = 0

        while True:

            header = self._stream_handle.read(DATA_HEADER_SIZE)

            if len(header) != DATA_HEADER_SIZE:
                # must have hit the time record or EOF
                if len(header) == TIME_RECORD_SIZE:
                    record_time = times[INDEX_TIME_ON]
                    ntp_time = ntplib.system_to_ntp_time(record_time)

                    particle = self._extract_sample(Vel3dKWfpMetadataParticle, None, times, ntp_time)
                    self._record_buffer.append(particle)
                    break

                else:
                    # must have hit EOF
                    # all timestamps are suspect
                    self.report_error(UnexpectedDataException,
                                      'Unexpectedly hit EOF, when expecting time record '
                                      'All particle timestamps from this file are suspect')
                    break

            header_match = DATA_HEADER_MATCHER.match(header)
            if header_match:  # validate the header checksum before going further
                expected_checksum = struct.unpack('<H', header_match.group('header_checksum'))[0]

                actual_checksum = self.calculate_checksum(header,
                                                          DATA_HEADER_CHECKSUM_LENGTH)

                if actual_checksum != expected_checksum:
                    self.report_error(SampleException,
                                      'Invalid Data Header checksum. '
                                      'Actual 0x%04X. Expected 0x%04X.' %
                                      (actual_checksum, expected_checksum))
                    # if the checksum fails need to stop processing data
                    break

                # header checks out, read body of data record
                data_size = struct.unpack('<H', header_match.group('data_size'))[0]
                expected_checksum = struct.unpack('<H', header_match.group('data_checksum'))[0]

                payload = self._stream_handle.read(data_size)

                actual_checksum = self.calculate_checksum(payload, data_size/2)

                if actual_checksum != expected_checksum:
                    self.report_error(RecoverableSampleException,
                                      'Invalid Data Header checksum. '
                                      'Actual 0x%04X. Expected 0x%04X.' %
                                      (actual_checksum, expected_checksum))
                    # if the header was good but data is bad, try the next header record
                    continue

                # payload checks out process the data
                header_id = struct.unpack('<B', header_match.group('header_id'))[0]

                record_time = times[INDEX_TIME_ON] + record_count * SAMPLE_RATE
                ntp_time = ntplib.system_to_ntp_time(record_time)

                if header_id == DATA_HEADER_ID_BURST_DATA or header_id == DATA_HEADER_ID_CP_DATA:
                    data_fields = struct.unpack(payload_format, payload)
                    particle_fields = data_fields + (header_id,)
                    particle_type = Vel3dKWfpInstrumentParticle
                    record_count += 1

                else:
                    string_format = '<B%ds' % (data_size - 2)
                    # ignore the terminating 0 from the string record
                    particle_fields = struct.unpack_from(string_format, payload)
                    particle_type = Vel3dKWfpStringParticle

                particle = self._extract_sample(particle_type, None, particle_fields, ntp_time)
                self._record_buffer.append(particle)

            else:  # invalid header
                self.report_error(SampleException,
                                  'Invalid Data Header encountered')
                # if the checksum fails need to stop processing data
                break

        # end of while

        return

