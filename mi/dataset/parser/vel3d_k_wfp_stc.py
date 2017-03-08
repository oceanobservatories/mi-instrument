#!/usr/bin/env python

"""
@package mi.dataset.parser.vel3d_k_wfp_stc
@file marine-integrations/mi/dataset/parser/vel3d_k_wfp_stc.py
@author Steve Myerson (Raytheon), Mark Worden
@brief Parser for the Vel3dKWfpStc dataset driver
Release notes:

Initial Release
"""

"""
The VEL3D input file is a binary file.
The first record is the Flag record that indicates which of the data
fields are to be expected in the Velocity data records.
Following the Flag record are some number of Velocity data records,
terminated by a Velocity data record with all fields set to zero.
Following the all zero Velocity data record is a Time record.
This design assumes that only one set of data records
(Flag, N * Velocity, Time) is in each file.
"""

__author__ = 'Steve Myerson (Raytheon)'
__license__ = 'Apache 2.0'

import ntplib
import re
import struct

from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logger
log = get_logger()
from mi.dataset.dataset_parser import SimpleParser

FLAG_RECORD_SIZE = 26
FLAG_RECORD_REGEX = b'(\x00|\x01){26}'  # 26 bytes of zeroes or ones
FLAG_FORMAT = '<26?'                    # 26 booleans
FLAG_RECORD_MATCHER = re.compile(FLAG_RECORD_REGEX)
INDEX_FLAG_TIME = 0                     # Index into the flags for time field
OUTPUT_TIME_SIZE = 6                    # 6 bytes for the output time field

TIME_RECORD_SIZE = 8                    # bytes
TIME_FORMAT = '>2I'                     # 2 32-bit unsigned integers big endian

INDEX_TIME_ON = 0            # field number within Time record
INDEX_TIME_OFF = 1           # field number within Time record
INDEX_RECORDS = 2

SAMPLE_RATE = .5             # Velocity records sample rate

INDEX_DATA_BYTES = 0
INDEX_FORMAT = 1
PARAM_NAME_KEY_INDEX = 2


class Vel3dKWfpStcParticleKey(BaseEnum):
    DATE_TIME_ARRAY = 'date_time_array'
    VEL3D_K_SOUNDSPEED = 'vel3d_k_soundSpeed'
    VEL3D_K_TEMP_C = 'vel3d_k_temp_c'
    VEL3D_K_HEADING = 'vel3d_k_heading'
    VEL3D_K_PITCH = 'vel3d_k_pitch'
    VEL3D_K_ROLL = 'vel3d_k_roll'
    VEL3D_K_MAG_X = 'vel3d_k_mag_x'
    VEL3D_K_MAG_Y = 'vel3d_k_mag_y'
    VEL3D_K_MAG_Z = 'vel3d_k_mag_z'
    VEL3D_K_BEAMS = 'vel3d_k_beams'
    VEL3D_K_CELLS = 'vel3d_k_cells'
    VEL3D_K_DATA_SET_DESCRIPTION = 'vel3d_k_data_set_description'
    VEL3D_K_V_SCALE = 'vel3d_k_v_scale'
    VEL3D_K_VEL0 = 'vel3d_k_vel0'
    VEL3D_K_VEL1 = 'vel3d_k_vel1'
    VEL3D_K_VEL2 = 'vel3d_k_vel2'
    VEL3D_K_AMP0 = 'vel3d_k_amp0'
    VEL3D_K_AMP1 = 'vel3d_k_amp1'
    VEL3D_K_AMP2 = 'vel3d_k_amp2'
    VEL3D_K_COR0 = 'vel3d_k_cor0'
    VEL3D_K_COR1 = 'vel3d_k_cor1'
    VEL3D_K_COR2 = 'vel3d_k_cor2'


class Vel3dKWfpStcBeamParams(BaseEnum):
    VEL3D_K_BEAM1 = 'vel3d_k_beam1'
    VEL3D_K_BEAM2 = 'vel3d_k_beam2'
    VEL3D_K_BEAM3 = 'vel3d_k_beam3'
    VEL3D_K_BEAM4 = 'vel3d_k_beam4'
    VEL3D_K_BEAM5 = 'vel3d_k_beam5'

flags = []


#
# VEL3D_PARAMETERS is a table containing the following parameters
# for the VEL3D data:
# The order of the entries corresponds to the order of the flags as
# described in the IDD.
#   The number of bytes for the field.
#   A format expression component to be added to the velocity data
#     format if that data item is to be collected.
#   A text string (key) used when generating the output data particle.
#
VEL3D_PARAMETERS = \
    [
        # Bytes Format Key
        [6,    '6b',  Vel3dKWfpStcParticleKey.DATE_TIME_ARRAY],
        [2,    'H',   Vel3dKWfpStcParticleKey.VEL3D_K_SOUNDSPEED],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_TEMP_C],
        [2,    'H',   Vel3dKWfpStcParticleKey.VEL3D_K_HEADING],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_PITCH],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_ROLL],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_MAG_X],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_MAG_Y],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_MAG_Z],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_BEAMS],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_CELLS],
        [1,    'B',   Vel3dKWfpStcBeamParams.VEL3D_K_BEAM1],
        [1,    'B',   Vel3dKWfpStcBeamParams.VEL3D_K_BEAM2],
        [1,    'B',   Vel3dKWfpStcBeamParams.VEL3D_K_BEAM3],
        [1,    'B',   Vel3dKWfpStcBeamParams.VEL3D_K_BEAM4],
        [1,    'B',   Vel3dKWfpStcBeamParams.VEL3D_K_BEAM5],
        [1,    'b',   Vel3dKWfpStcParticleKey.VEL3D_K_V_SCALE],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_VEL0],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_VEL1],
        [2,    'h',   Vel3dKWfpStcParticleKey.VEL3D_K_VEL2],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_AMP0],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_AMP1],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_AMP2],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_COR0],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_COR1],
        [1,    'B',   Vel3dKWfpStcParticleKey.VEL3D_K_COR2]
    ]


class Vel3dKWfpStcDataParticleType(BaseEnum):
    METADATA_PARTICLE = 'vel3d_k_wfp_stc_metadata'
    INSTRUMENT_PARTICLE = 'vel3d_k_wfp_stc_instrument'


class Vel3dKWfpStcMetadataParticleKey(BaseEnum):
    NUMBER_OF_RECORDS = 'vel3d_k_number_of_records'
    TIME_OFF = 'vel3d_k_time_off'
    TIME_ON = 'vel3d_k_time_on'

DATE_TIME_SIZE = 6                     # 6 bytes for the output date time field
DATA_SET_DESCRIPTION_SIZE = 5


class Vel3dKWfpStcMetadataParticle(DataParticle):
    """
    Class for parsing TIME data from the VEL3D_K__stc_imodem data set
    """

    _data_particle_type = Vel3dKWfpStcDataParticleType.METADATA_PARTICLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a value_array with the appropriate tag.
        """

        """
        Generate a data value_array.
        Note that raw_data already contains the individual fields
        extracted and unpacked from the time data record.
        """
        value_array = [
            {
                DataParticleKey.VALUE_ID:
                Vel3dKWfpStcMetadataParticleKey.TIME_ON,
                DataParticleKey.VALUE: self.raw_data[INDEX_TIME_ON]
            },
            {
                DataParticleKey.VALUE_ID:
                Vel3dKWfpStcMetadataParticleKey.TIME_OFF,
                DataParticleKey.VALUE: self.raw_data[INDEX_TIME_OFF]
            },
            {
                DataParticleKey.VALUE_ID:
                Vel3dKWfpStcMetadataParticleKey.NUMBER_OF_RECORDS,
                DataParticleKey.VALUE: self.raw_data[INDEX_RECORDS]
            }
        ]

        return value_array


class Vel3dKWfpStcInstrumentParticle(DataParticle):
    """
    Class for parsing VELOCITY data from the VEL3D_K__stc_imodem data set
    """

    _data_particle_type = Vel3dKWfpStcDataParticleType.INSTRUMENT_PARTICLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a value_array with the appropriate tag.
        """

        """
        Generate a velocity data value_array.
        Note that raw_data already contains the individual fields
        extracted and unpacked from the velocity data record.
        """
        global flags
        value_array = []
        field = 0

        data_set_description_param_values = []

        for flag_index in range(0, FLAG_RECORD_SIZE):

            # If the flags indicated that this field is to be expected,
            # store the next unpacked value into the data value_array.
            key = VEL3D_PARAMETERS[flag_index][PARAM_NAME_KEY_INDEX]
            if key == Vel3dKWfpStcParticleKey.DATE_TIME_ARRAY:
                time_array = self.raw_data[field: field + DATE_TIME_SIZE]
                value_array.append({DataParticleKey.VALUE_ID: key,
                                    DataParticleKey.VALUE: list(time_array)})
                field += DATE_TIME_SIZE

            elif key in (
                    Vel3dKWfpStcBeamParams.VEL3D_K_BEAM1,
                    Vel3dKWfpStcBeamParams.VEL3D_K_BEAM2,
                    Vel3dKWfpStcBeamParams.VEL3D_K_BEAM3,
                    Vel3dKWfpStcBeamParams.VEL3D_K_BEAM4,
                    Vel3dKWfpStcBeamParams.VEL3D_K_BEAM5):
                if flags[flag_index]:
                    data_set_description_param_values.append(
                        int(self.raw_data[field]))
                    field += 1
                else:
                    data_set_description_param_values.append(None)

            elif key == Vel3dKWfpStcParticleKey.VEL3D_K_BEAMS:
                if flags[flag_index]:
                    key_value = self._encode_value(key,
                                                   self.raw_data[field],
                                                   int)
                    value_array.append(key_value)
                    field += 1
                else:
                    value_array.append({DataParticleKey.VALUE_ID: key,
                                        DataParticleKey.VALUE: None})
            else:
                if flags[flag_index]:
                    key_value = self._encode_value(key,
                                                   self.raw_data[field],
                                                   int)
                    value_array.append(key_value)
                    field += 1

        value_array.append(self._encode_value(
            Vel3dKWfpStcParticleKey.VEL3D_K_DATA_SET_DESCRIPTION,
            data_set_description_param_values,
            list))

        return value_array


class Vel3dKWfpStcParser(SimpleParser):

    @staticmethod
    def parse_flag_record(record):
        """
        This function parses the Flag record.
        A Flag record consists of 26 binary bytes,
        with each byte being either 0 or 1.
        Each byte corresponds to a data item in the Velocity record.
        Then we use the received Flag record fields to override
        the expected flag fields.
        Arguments:
          record - a buffer of binary bytes
        Returns:
          True/False indicating whether or not the flag record is valid.
          A regular expression based on the received flag fields,
            to be used in pattern matching.
          A regular expression for detecting end of Velocity record.
          A format based on the flag fields, to be used to unpack the data.
          The number of bytes expected in each velocity data record.
        """

        # See if we've got a valid flag record.

        global flags
        flag_record = FLAG_RECORD_MATCHER.match(record)
        if not flag_record:
            valid_flag_record = False
            regex_velocity_record = None
            regex_end_velocity_record = None
            format_unpack_velocity = None
            record_length = 0
        else:

            # If the flag record is valid,
            # interpret each field as a boolean value.
            valid_flag_record = True
            flags = struct.unpack(FLAG_FORMAT,
                                  flag_record.group(0)[0:FLAG_RECORD_SIZE])

            # The format string for unpacking the velocity data record
            # fields must be constructed based on which fields the Flag
            # record indicates we'll be receiving.
            # Start with the little endian symbol for the format.
            # We also compute the record length for each velocity data
            # record, again based on the Flag record.

            format_unpack_velocity = '<'
            record_length = 0

            # Check each field from the input Flag record.

            for x in range(0, len(VEL3D_PARAMETERS)):

                # If the flag field is True,
                # increment the total number of bytes expected in each
                # velocity data record and add the corresponding text to
                # the format.

                if flags[x]:
                    record_length += VEL3D_PARAMETERS[x][INDEX_DATA_BYTES]
                    format_unpack_velocity = format_unpack_velocity + \
                        VEL3D_PARAMETERS[x][INDEX_FORMAT]

            # Create the velocity data record regular expression
            # (some number of any hex digits)
            # and the end of velocity data record indicator
            # (the same number of all zeroes).
            # Note that the backslash needs to be doubled because
            # we're not using the b'' syntax.

            regex_velocity_record = "[\\x00-\\xFF]{%d}" % record_length
            regex_end_velocity_record = "[\\x00]{%d}" % record_length

        return valid_flag_record, regex_velocity_record, \
            regex_end_velocity_record, format_unpack_velocity, record_length

    def parse_file(self):

        self._stream_handle.seek(0, 0)  # 0 = from start of file
        record = self._stream_handle.read(FLAG_RECORD_SIZE)

        # Check for end of file.
        # If not reached, check for and parse a Flag record.

        if len(record) != FLAG_RECORD_SIZE:
            log.warn("EOF reading for flag record")
            self._exception_callback(SampleException('EOF reading for flag record'))
            return

        # parse the flag record
        (valid_flag_record, velocity_regex,
         end_of_velocity_regex, velocity_format,
         velocity_record_size) = Vel3dKWfpStcParser.parse_flag_record(record)

        # If the Flag record was valid,
        # create the pattern matchers for the Velocity record.
        # and check for valid time record to create timestamps

        if valid_flag_record:

            # This one will match any Velocity record.
            velocity_record_matcher = re.compile(velocity_regex)

            # This one checks for the end of the Velocity record.
            velocity_end_record_matcher = re.compile(end_of_velocity_regex)

            # Read the Time record which is at the very end of the file.
            # Note that we don't check if the number of bytes between the
            # Flag record and the Time record we only verify the end record is
            # present in the correct location

            # Verify there is an end record so we can trust the time record
            # note using seek with 2 = from end of file
            self._stream_handle.seek(0 - (TIME_RECORD_SIZE + velocity_record_size), 2)
            record = self._stream_handle.read(velocity_record_size)
            velocity_test = velocity_end_record_matcher.match(record)

            if velocity_test:
                record = self._stream_handle.read(TIME_RECORD_SIZE)
                time_fields = struct.unpack(TIME_FORMAT, record)

                time_on = int(time_fields[INDEX_TIME_ON])

            else:
                message = 'no valid end record, cant trust time records'
                log.warn(message)
                self._exception_callback(SampleException(message))
                # per the IDD can't do anything without an end record record
                return

        else:
            message = "Invalid Flag record"
            log.warn(message)
            self._exception_callback(SampleException(message))
            # can't do anything without flag record
            return

        self._stream_handle.seek(FLAG_RECORD_SIZE, 0)  # go to byte after flag record

        instrument_record_counter = 0

        # read first velocity record
        record = self._stream_handle.read(velocity_record_size)

        # loop through all the velocity records
        # stop when you find the end record.
        while record:

            # check to see if we found the velocity end record
            velocity_end = velocity_end_record_matcher.match(record)

            if velocity_end:
                break  # exit while loop

            # If the file is missing an end of velocity record,
            # meaning we'll exhaust the file and run off the end,
            # this test will catch it.

            velocity_record = velocity_record_matcher.match(record)
            if velocity_record:
                velocity_fields = struct.unpack(
                    velocity_format,
                    velocity_record.group(0)[0:velocity_record_size])

                # Generate a data particle for this record and add
                # it to the end of the particles collected so far.

                instrument_record_counter += 1
                timestamp = ((instrument_record_counter - 1) * SAMPLE_RATE) + time_on
                ntp_time = ntplib.system_to_ntp_time(timestamp)

                particle = self._extract_sample(
                    Vel3dKWfpStcInstrumentParticle,
                    None, velocity_fields, ntp_time)

                self._record_buffer.append(particle)

            # Ran off the end of the file.  Tell 'em the bad news.
            else:
                message = "EOF reading velocity records"
                log.warn(message)
                self._exception_callback(SampleException(message))

            # read next velocity record
            record = self._stream_handle.read(velocity_record_size)

        # end of while loop

        # if we found a valid end record the time record has already been upacked
        # otherwise we would have exited without processing data
        # Add the number of Velocity records received 

        metadata_fields_tuple = time_fields + (instrument_record_counter,)
        ntp_time = ntplib.system_to_ntp_time(time_on)

        particle = self._extract_sample(
            Vel3dKWfpStcMetadataParticle,
            None, metadata_fields_tuple, ntp_time)

        self._record_buffer.append(particle)

        return
