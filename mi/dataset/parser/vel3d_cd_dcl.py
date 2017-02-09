"""
@package mi.dataset.parser
@file /mi/dataset/parser/vel3d_cd_dcl.py
@author Emily Hahn
@brief Parser for the vel3d instrument series c,d through dcl dataset driver
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'


import struct
import re
import binascii
import base64
from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.exceptions import UnexpectedDataException, SampleException
from mi.dataset.dataset_parser import SimpleParser
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.dataset.parser.common_regexes import DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX
import vel3d_velpt_common

DATE_TIME_REGEX = DATE_YYYY_MM_DD_REGEX + r' ' + TIME_HR_MIN_SEC_MSEC_REGEX + r' '
DATE_TIME_MATCHER = re.compile(DATE_TIME_REGEX)

VELOCITY_ID = b'\x10'
SYSTEM_ID = b'\x11'
HEADER_DATA_ID = b'\x12'

# some records do not contain size, store their sizes here
RECORD_SIZE_DICT = {
    VELOCITY_ID: 24,
    b'\x36': 24,
    b'\x51': 22
}

# map bit index to hex bit mask
BIT_MASK_DICT = {
    0: 0x0001,
    1: 0x0002,
    2: 0x0004,
    3: 0x0008,
    4: 0x0010,
    5: 0x0020,
    6: 0x0040,
    7: 0x0080,
    8: 0x0100,
    9: 0x0200
}


class Vel3dCdDclDataParticleType(BaseEnum):
    USER_CONFIG = 'vel3d_cd_dcl_user_configuration'
    USER_CONFIG_RECOV = 'vel3d_cd_dcl_user_configuration_recovered'
    HARDWARE_CONFIG = 'vel3d_cd_dcl_hardware_configuration'
    HARDWARE_CONFIG_RECOV = 'vel3d_cd_dcl_hardware_configuration_recovered'
    HEAD_CONFIG = 'vel3d_cd_dcl_head_configuration'
    HEAD_CONFIG_RECOV = 'vel3d_cd_dcl_head_configuration_recovered'
    DATA_HEADER = 'vel3d_cd_dcl_data_header'
    DATA_HEADER_RECOV = 'vel3d_cd_dcl_data_header_recovered'
    VELOCITY = 'vel3d_cd_dcl_velocity_data'
    VELOCITY_RECOV = 'vel3d_cd_dcl_velocity_data_recovered'
    SYSTEM = 'vel3d_cd_dcl_system_data'
    SYSTEM_RECOV = 'vel3d_cd_dcl_system_data_recovered'


class Vel3dCdDclUserConfigCommonParticle(DataParticle):

    # dictionary for unpacking ints that directly map to a parameter
    UNPACK_DICT = {
        'transmit_pulse_length': 0,
        'blanking_distance': 1,
        'receive_length': 2,
        'time_between_pings': 3,
        'time_between_bursts': 4,
        'number_pings': 5,
        'average_interval': 6,
        'number_beams': 7,
        'compass_update_rate': 16,
        'coordinate_system': 17,
        'number_cells': 18,
        'cell_size': 19,
        'measurement_interval': 20,
        'wrap_mode': 22,
        'diagnostics_interval': 29,
        'sound_speed_adjust_factor': 31,
        'number_diagnostics_samples': 32,
        'number_beams_per_cell': 33,
        'number_pings_diagnostic': 34,
        'analog_input_address': 36,
        'software_version': 37,
        'percent_wave_cell_position': 42,
        'wave_transmit_pulse': 43,
        'fixed_wave_blanking_distance': 44,
        'wave_measurement_cell_size': 45,
        'number_diagnostics_per_wave': 46,
        'number_samples_per_burst': 49,
        'analog_scale_factor': 51,
        'correlation_threshold': 52,
        'transmit_pulse_length_2nd': 54
    }

    # map for unpacking bits, contains name, index of unpacked byte, bit index within that byte
    UNPACK_BIT_MAP = (
        ('profile_type', 8, 1),
        ('mode_type', 8, 2),
        ('power_level_tcm1', 8, 5),
        ('power_level_tcm2', 8, 6),
        ('sync_out_position', 8, 7),
        ('sample_on_sync', 8, 8),
        ('start_on_sync', 8, 9),
        ('power_level_pcr1', 9, 5),
        ('power_level_pcr2', 9, 6),
        ('use_specified_sound_speed', 30, 0),
        ('diagnostics_mode_enable', 30, 1),
        ('analog_output_enable', 30, 2),
        ('output_format_nortek', 30, 3),
        ('scaling', 30, 4),
        ('serial_output_enable', 30, 5),
        ('stage_enable', 30, 7),
        ('analog_power_output', 30, 8),
        ('use_dsp_filter', 35, 0),
        ('filter_data_output', 35, 1),
        ('wave_data_rate', 41, 0),
        ('wave_cell_position', 41, 1),
        ('dynamic_position_type', 41, 2),
    )

    def _build_parsed_values(self):
        """
        Return an array of dictionaries containing parameters for the user config data particle
        :return: array of dictionary of parameters
        """
        parameters = []
        # unpack binary raw data string into an array of individual values
        unpacked_data = struct.unpack_from('<10H 6B 5H 6s H 6B I 9H 180s 180s 14H 30B 16s', self.raw_data, 4)

        deployment_start = unpacked_data[23:29]
        parameters.append(self._encode_value('deployment_start_time', deployment_start, list))

        # string encoding based on nortek instrument driver

        # these strings may have extra nulls at the end, remove them
        parameters.append(self._encode_value('deployment_name', unpacked_data[21].split('\x00', 1)[0], str))
        parameters.append(self._encode_value('file_comments', unpacked_data[40].split('\x00', 1)[0], str))
        # encode as base 64
        parameters.append(self._encode_value('velocity_adjustment_factor', base64.b64encode(unpacked_data[39]), str))
        parameters.append(self._encode_value('filter_constants', base64.b64encode(unpacked_data[85]), str))

        # unpack dict contains all ints
        for name, index in self.UNPACK_DICT.iteritems():
            parameters.append(self._encode_value(name, unpacked_data[index], int))

        for name, index, bit_index in self.UNPACK_BIT_MAP:
            parameters.append(self._encode_value(name, (unpacked_data[index] & BIT_MASK_DICT[bit_index]) >> bit_index,
                                                 int))
        return parameters


class Vel3dCdDclUserConfigTelemeteredParticle(Vel3dCdDclUserConfigCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.USER_CONFIG


class Vel3dCdDclUserConfigRecoveredParticle(Vel3dCdDclUserConfigCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.USER_CONFIG_RECOV


class Vel3dCdDclHardwareConfigCommonParticle(DataParticle):

    # map for unpacking ints and strings that directly map to a parameter
    UNPACK_MAP = [
        ('board_frequency', 2, int),
        ('pic_version', 3, int),
        ('hardware_revision', 4, int),
        ('recorder_size', 5, int),
        ('firmware_version', 19, str)
    ]

    # map for unpacking bits, contains name, index of unpacked byte, bit index within that byte
    UNPACK_BIT_MAP = [
        ('recorder_installed', 1, 0),
        ('compass_installed', 1, 1),
        ('velocity_range', 6, 0)
    ]

    def _build_parsed_values(self):
        """
        Return an array of dictionaries containing parameters for the hardware configuration particle
        :return: array of dictionary of parameters
        """
        parameters = []
        # unpack binary raw data string into an array of individual values, starting at byte 4
        unpacked_data = struct.unpack_from('<14s 6H 12B 4s', self.raw_data, 4)

        # this string may have extra nulls at the end, remove them
        parameters.append(self._encode_value('instrmt_type_serial_number', unpacked_data[0].split('\x00', 1)[0], str))

        for name, index, data_type in self.UNPACK_MAP:
            parameters.append(self._encode_value(name, unpacked_data[index], data_type))

        # unpack bit fields
        for name, index, bit_index in self.UNPACK_BIT_MAP:
            parameters.append(self._encode_value(name,
                                                 (unpacked_data[index] & BIT_MASK_DICT.get(bit_index)) >> bit_index,
                                                 int))
        return parameters


class Vel3dCdDclHardwareConfigTelemeteredParticle(Vel3dCdDclHardwareConfigCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.HARDWARE_CONFIG


class Vel3dCdDclHardwareConfigRecoveredParticle(Vel3dCdDclHardwareConfigCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.HARDWARE_CONFIG_RECOV


class Vel3dCdDclHeadConfigCommonParticle(DataParticle):

    # dictionary for unpacking ints that directly map to a parameter
    UNPACK_DICT = {
        'head_frequency': 1,
        'number_beams': 27
    }

    # map for unpacking bits, contains name, index of unpacked byte, bit index within that byte
    UNPACK_BIT_MAP = [
        ('pressure_sensor', 0, 0),
        ('magnetometer_sensor', 0, 1),
        ('tilt_sensor', 0, 2),
        ('tilt_sensor_mounting', 0, 3),
    ]

    def _build_parsed_values(self):
        """
        Return an array of dictionaries containing parameters for the head configuration particle
        :return: array of dictionary of parameters
        """
        parameters = []
        # unpack binary raw data string into an array of individual values, starting at byte 4
        unpacked_data = struct.unpack_from('<2H 2s 12s 176s 22B 2H', self.raw_data, 4)

        # string encoding based on nortek instrument driver

        # these strings may have extra nulls at the end, remove them
        parameters.append(self._encode_value('head_type', unpacked_data[2].split('\x00', 1)[0], str))
        parameters.append(self._encode_value('head_serial_number', unpacked_data[3].split('\x00', 1)[0], str))
        parameters.append(self._encode_value('system_data', base64.b64encode(unpacked_data[4]), str))

        # pull out bits from head config, the first unpacked byte
        for name, index, bit_index in self.UNPACK_BIT_MAP:
            parameters.append(self._encode_value(name,
                                                 (unpacked_data[index] & BIT_MASK_DICT.get(bit_index)) >> bit_index,
                                                 int))

        for name, index in self.UNPACK_DICT.iteritems():
            parameters.append(self._encode_value(name, unpacked_data[index], int))

        return parameters


class Vel3dCdDclHeadConfigTelemeteredParticle(Vel3dCdDclHeadConfigCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.HEAD_CONFIG


class Vel3dCdDclHeadConfigRecoveredParticle(Vel3dCdDclHeadConfigCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.HEAD_CONFIG_RECOV


class Vel3dCdDclDataHeaderCommonParticle(DataParticle):

    # store index into unpacked raw data by parameter name, starting from byte 9, all are ints
    UNPACK_DICT = {
        'number_velocity_records': 0,
        'noise_amp_beam1': 1,
        'noise_amp_beam2': 2,
        'noise_amp_beam3': 3,
        # index 4 is spare
        'noise_correlation_beam1': 5,
        'noise_correlation_beam2': 6,
        'noise_correlation_beam3': 7,
    }

    def _build_parsed_values(self):
        """
        Return an array of dictionaries containing parameters for the data header particle
        :return: array of dictionary of parameters
        """
        # unpack the raw data starting at byte 9
        unpacked_data = struct.unpack_from('<H 7B', self.raw_data, 10)

        # get the date time string
        date_time_string = vel3d_velpt_common.get_date_time_string(self.raw_data)
        parameters = [self._encode_value('date_time_string', date_time_string, str)]

        for name, index in self.UNPACK_DICT.iteritems():
            parameters.append(self._encode_value(name, unpacked_data[index], int))

        return parameters


class Vel3dCdDclDataHeaderTelemeteredParticle(Vel3dCdDclDataHeaderCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.DATA_HEADER


class Vel3dCdDclDataHeaderRecoveredParticle(Vel3dCdDclDataHeaderCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.DATA_HEADER_RECOV


class Vel3dCdDclVelocityCommonParticle(DataParticle):

    # store index into unpacked raw data by parameter name, all are ints
    UNPACK_DICT = {
        'ensemble_counter': 3,
        'analog_input_1': 7,
        'turbulent_velocity_east': 8,
        'turbulent_velocity_north': 9,
        'turbulent_velocity_vertical': 10,
        'amplitude_beam_1': 11,
        'amplitude_beam_2': 12,
        'amplitude_beam_3': 13,
        'correlation_beam_1': 14,
        'correlation_beam_2': 15,
        'correlation_beam_3': 16,
    }

    def _build_parsed_values(self):
        """
        Return an array of dictionaries containing parameters for the velocity particle
        :return: array of dictionary of parameters
        """
        parameters = []
        # unpack binary raw data string into an array of individual values
        unpacked_data = struct.unpack('<6B 2H 3h 6B H', self.raw_data)

        # unpack the data into parameters and values using the dictionary
        for name, index in self.UNPACK_DICT.iteritems():
            parameters.append(self._encode_value(name, unpacked_data[index], int))

        # some parameters need extra calculations

        analog_2_lsb = unpacked_data[2]
        analog_2_msb = unpacked_data[5]
        # combine least and most significant byte
        analog_2 = (analog_2_msb << 8) + analog_2_lsb
        parameters.append(self._encode_value('analog_input_2', analog_2, int))

        pressure_msb = unpacked_data[4]
        pressure_lsw = unpacked_data[6]
        # combine least significant word and byte,
        pressure = (pressure_msb << 16) + pressure_lsw
        parameters.append(self._encode_value('seawater_pressure_mbar', pressure, int))

        return parameters


class Vel3dCdDclVelocityTelemeteredParticle(Vel3dCdDclVelocityCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.VELOCITY


class Vel3dCdDclVelocityRecoveredParticle(Vel3dCdDclVelocityCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.VELOCITY_RECOV


class Vel3dCdDclSystemCommonParticle(DataParticle):

    def _build_parsed_values(self):
        """
        Return an array of dictionaries containing parameters for the system particle
        :return: array of dictionary of parameters
        """
        unpacked_data = struct.unpack_from('<2H 4h 2B H', self.raw_data, 10)

        # get the date time string from the raw data
        date_time_string = vel3d_velpt_common.get_date_time_string(self.raw_data)

        parameters = [self._encode_value('date_time_string', date_time_string, str),
                      self._encode_value('battery_voltage_dV', unpacked_data[0], int),
                      self._encode_value('sound_speed_dms', unpacked_data[1], int),
                      self._encode_value('heading_decidegree', unpacked_data[2], int),
                      self._encode_value('pitch_decidegree', unpacked_data[3] , int),
                      self._encode_value('roll_decidegree', unpacked_data[4], int),
                      self._encode_value('temperature_centidegree', unpacked_data[5], int),
                      self._encode_value('error_code', unpacked_data[6], int),
                      self._encode_value('status_code', unpacked_data[7], int),
                      self._encode_value('analog_input', unpacked_data[8], int)]

        return parameters


class Vel3dCdDclSystemTelemeteredParticle(Vel3dCdDclSystemCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.SYSTEM


class Vel3dCdDclSystemRecoveredParticle(Vel3dCdDclSystemCommonParticle):
    _data_particle_type = Vel3dCdDclDataParticleType.SYSTEM_RECOV


class Vel3dCdDclParser(SimpleParser):
    """
    Class used to parse the vel3d_cd_dcl data set.
    """
    def __init__(self,
                 file_handle,
                 exception_callback,
                 is_telemetered):

        self._file_handle = file_handle
        self.stored_velocity_records = []
        self.stored_n_velocity_records = 0
        self.previous_system_timestamp = None
        self.first_timestamp = None
        self.stored_hardware_config = None
        self.stored_head_config = None

        if is_telemetered:
            # use telemetered classes
            self.user_config_class = Vel3dCdDclUserConfigTelemeteredParticle
            self.hardware_config_class = Vel3dCdDclHardwareConfigTelemeteredParticle
            self.head_config_class = Vel3dCdDclHeadConfigTelemeteredParticle
            self.data_header_class = Vel3dCdDclDataHeaderTelemeteredParticle
            self.velocity_class = Vel3dCdDclVelocityTelemeteredParticle
            self.system_class = Vel3dCdDclSystemTelemeteredParticle
        else:
            # use recovered classes
            self.user_config_class = Vel3dCdDclUserConfigRecoveredParticle
            self.hardware_config_class = Vel3dCdDclHardwareConfigRecoveredParticle
            self.head_config_class = Vel3dCdDclHeadConfigRecoveredParticle
            self.data_header_class = Vel3dCdDclDataHeaderRecoveredParticle
            self.velocity_class = Vel3dCdDclVelocityRecoveredParticle
            self.system_class = Vel3dCdDclSystemRecoveredParticle

        # no config for this parser, pass in empty dict
        super(Vel3dCdDclParser, self).__init__({}, file_handle, exception_callback)

    def parse_file(self):
        """
        Main parsing function which loops through the file and interprets it by building particles
        """
        end_of_file = False

        # loop until the entire file is read
        while not end_of_file:

            # read up to the start of a record by finding the sync marker
            end_of_file = self.find_record_start()
            if end_of_file:
                # make sure we break out of this loop if there are no more bytes in the file
                continue

            # now that the sync marker has been found, get the record type which follows
            record_type = self._file_handle.read(1)

            if record_type in RECORD_SIZE_DICT.keys():
                # this record type does not contain the record size, get it from the dictionary
                record_size_bytes = RECORD_SIZE_DICT.get(record_type)
                full_record = vel3d_velpt_common.SYNC_MARKER + record_type
            else:
                # this record type does contain the record size, read it from the file
                record_size_words = self._file_handle.read(2)

                # unpack and convert from words to bytes
                record_size_bytes = struct.unpack('<H', record_size_words)[0] * 2
                full_record = vel3d_velpt_common.SYNC_MARKER + record_type + record_size_words

            # based on the obtained record size, read the rest of the record
            remain_bytes = record_size_bytes - len(full_record)
            remain_record = self._file_handle.read(remain_bytes)
            # store the full record
            full_record += remain_record

            if len(remain_record) < remain_bytes:
                # if we did not read as many bytes as were requested, we ran into the end of the file
                msg = 'Incomplete record 0x%s' % binascii.hexlify(full_record)
                log.warning(msg)
                self._exception_callback(SampleException(msg))
                end_of_file = True
                continue

            # compare checksums
            if not vel3d_velpt_common.match_checksum(full_record):
                # checksums did not match, do not process this record further
                msg = 'Checksums do not match for record type 0x%s' % binascii.hexlify(record_type)
                log.warn(msg)
                self._exception_callback(SampleException(msg))
                continue

            # process record based on the type
            self.process_records(record_type, full_record)

        if self.stored_velocity_records:
            # If stored velocity records are present here, we only got a partial set at the end of the file
            # without a terminating system record.  Use the previous number of samples.
            if self.stored_n_velocity_records != 0:
                time_offset = 1.0/float(self.stored_n_velocity_records)
                self.extract_velocities(time_offset)
            else:
                msg = 'Unable to calculating timestamp for last set of velocity records'
                log.warn(msg)
                self._exception_callback(SampleException(msg))

    def find_record_start(self):
        """
        Find the start of the next record by looking for the sync marker
        :return: True if the end of the file was found, False if it was not
        """
        end_of_file = False
        read_buffer = ''

        # read one byte at a time until the sync marker is found
        one_byte = self._file_handle.read(1)
        while one_byte != vel3d_velpt_common.SYNC_MARKER:
            # store anything we find before the sync marker in the read buffer
            read_buffer += one_byte
            one_byte = self._file_handle.read(1)
            if one_byte == '':
                # no more bytes to read, break out of this loop
                end_of_file = True
                break

        if len(read_buffer) > 1 and not DATE_TIME_MATCHER.match(read_buffer):
            # we expect a version of the file to have ascii date time strings prior to each record, if this
            # is something other than that call the exception
            msg = 'Found unexpected data 0x%s' % binascii.hexlify(read_buffer)
            log.warning(msg)
            self._exception_callback(UnexpectedDataException(msg))

        return end_of_file

    def process_records(self, record_type, full_record):
        """
        based on the record type process the data, if the record type is not mentioned here it is ignored
        :param record_type: the record type associated with this record
        :param full_record: the full data string associated with this record
        """
        if record_type == vel3d_velpt_common.USER_CONFIGURATION_ID:
            self.process_user_config(full_record)

        elif record_type == vel3d_velpt_common.HARDWARE_CONFIGURATION_ID:
            self.process_hardware_config(full_record)

        elif record_type == vel3d_velpt_common.HEAD_CONFIGURATION_ID:
            self.process_head_config(full_record)

        elif record_type == VELOCITY_ID:
            # append velocity record to buffer, these are collected until the timestamp can be calculated
            self.stored_velocity_records.append(full_record)

        elif record_type == SYSTEM_ID:
            self.process_system(full_record)

        elif record_type == HEADER_DATA_ID:
            self.process_header_data(full_record)

    def process_user_config(self, full_record):
        """
        Extract the user config particle, and set the first timestamp if it has not been set yet
        :param full_record: The raw data string of the user config particle
        """
        # get the timestamp for this particle
        timestamp = vel3d_velpt_common.get_timestamp(full_record, start_byte=48)

        # if the first timestamp has not been set, set it here
        if self.first_timestamp is None:
            self.first_timestamp = timestamp
            # check if head or hardware messages have been received and not sent yet
            self.extract_h_config()

        self.simple_extract(self.user_config_class, full_record, timestamp)

    def process_hardware_config(self, full_record):
        """
        If the first timestamp has been set, use this as the timestamp of this particle and extract it,
        otherwise store it until the first timestamp has been set
        :param full_record: The raw data string to pass into the hardware configuration particle
        """
        # first_timestamp is used as the timestamp of this particle, if it is not set yet wait until it is
        if self.first_timestamp:
            self.simple_extract(self.hardware_config_class, full_record, self.first_timestamp)
        else:
            self.stored_hardware_config = full_record

    def process_head_config(self, full_record):
        """
        If the first timestamp has been set, use this as the timestamp of this particle and extract it,
        otherwise store it until the first timestamp has been set
        :param full_record: The raw data string to pass into the head configuration particle
        """
        # first_timestamp is used as the timestamp of this particle, if it is not set yet wait until it is
        if self.first_timestamp:
            self.simple_extract(self.head_config_class, full_record, self.first_timestamp)
        else:
            self.stored_head_config = full_record

    def process_system(self, full_record):
        """
        Extract a system record, and if there is a pair of system records with velocities in between determine
        the time offset between velocity timestamps and extract the velocity records.  Also if the first timestamp
        has not been set yet, set it
        :param full_record: The raw data string to pass into the system particle
        """

        if self.previous_system_timestamp is not None and self.stored_velocity_records != []:
            # there has been a pair of system records and with velocity records in between
            n_vel_records = len(self.stored_velocity_records)

            time_offset = 1.0/float(n_vel_records)

            # calculate the timestamps and extract velocity records
            self.extract_velocities(time_offset)

            self.stored_n_velocity_records = n_vel_records

        # get the timestamp associated with this system record
        timestamp = vel3d_velpt_common.get_timestamp(full_record)

        # extract the system record
        self.simple_extract(self.system_class, full_record, timestamp)

        self.previous_system_timestamp = float(timestamp)

        if self.first_timestamp is None:
            self.first_timestamp = timestamp
            # check if head or hardware messages have been received and not sent yet
            self.extract_h_config()

    def extract_velocities(self, time_offset):
        """
        loop calculating timestamp and extracting stored velocity records
        :param time_offset: The time offset (in seconds) between velocity records to use in calculating the timestamp
        """
        for i in range(0, len(self.stored_velocity_records)):
            timestamp = self.previous_system_timestamp + (i * time_offset)
            self.simple_extract(self.velocity_class, self.stored_velocity_records[i], timestamp)

        # now that they have been extracted, clear the velocity record buffer
        self.stored_velocity_records = []

    def extract_h_config(self):
        """
        If hardware config or head config messages have been received and not extracted yet, extract them here
        """
        if self.stored_hardware_config:
            self.simple_extract(self.hardware_config_class, self.stored_hardware_config, self.first_timestamp)
            self.stored_hardware_config = None

        if self.stored_head_config:
            self.simple_extract(self.head_config_class, self.stored_head_config, self.first_timestamp)
            self.stored_head_config = None

    def process_header_data(self, full_record):
        """
        Extract the header data particle, and set the first timestamp if it has not been set
        :param full_record: The raw data string to pass into the header data particle
        """
        # get the timestamp for this particle
        timestamp = vel3d_velpt_common.get_timestamp(full_record)

        # check if the first timestamp has been set, if not set it
        if self.first_timestamp is None:
            self.first_timestamp = timestamp
            # check if head or hardware messages have been received and not sent yet
            self.extract_h_config()

        # extract the data header particle
        self.simple_extract(self.data_header_class, full_record, timestamp)

    def simple_extract(self, class_type, data, timestamp):
        """
        Extract the particle and appending it to the record buffer
        :param class_type: The class of the particle to extract
        :param data: The raw data to pass into the particle
        :param timestamp: The timestamp to pass into the particle
        """
        particle = self._extract_sample(class_type, None, data, timestamp)
        self._record_buffer.append(particle)










