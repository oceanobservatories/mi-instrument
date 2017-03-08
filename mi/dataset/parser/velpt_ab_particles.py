#!/usr/bin/env python

"""
@package mi.dataset.parser
@file /mi/dataset/parser/velpt_ab_particles.py
@author Chris Goodrich
@brief Particle definitions for the velpt_ab recovered dataset driver
Release notes:

initial release
"""
__author__ = 'Chris Goodrich'
__license__ = 'Apache 2.0'

import struct
import calendar
from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
import ntplib


class VelptAbDataParticleType(BaseEnum):
    VELPT_AB_INSTRUMENT_METADATA_RECOVERED = 'velpt_ab_instrument_metadata_recovered'
    VELPT_AB_INSTRUMENT_RECOVERED = 'velpt_ab_instrument_recovered'
    VELPT_AB_DIAGNOSTICS_METADATA_RECOVERED = 'velpt_ab_diagnostics_metadata_recovered'
    VELPT_AB_DIAGNOSTICS_RECOVERED = 'velpt_ab_diagnostics_recovered'


class VelptAbDataParticleKey(BaseEnum):

    # Particle field enums for extracting from Velocity and Diagnostics Data records
    DATE_TIME_STRING = 'date_time_string'                                        # PD93
    ERROR_CODE = 'error_code'                                                    # PD433
    ANALOG1 = 'analog1'                                                          # PD434
    BATTERY_VOLTAGE_DV = 'battery_voltage_dV'                                    # PD3242
    SOUND_SPEED_DMS = 'sound_speed_dms'                                          # PD3243
    HEADING_DECIDEGREE = 'heading_decidegree'                                    # PD3244
    PITCH_DECIDEGREE = 'pitch_decidegree'                                        # PD3246
    ROLL_DECIDEGREE = 'roll_decidegree'                                          # PD3245
    PRESSURE_MBAR = 'pressure_mbar'                                              # PD3248
    STATUS = 'status'                                                            # PD439
    TEMPERATURE_CENTIDEGREE = 'temperature_centidegree'        # PD3247
    VELOCITY_BEAM1 = 'velocity_beam1'                                            # PD441
    VELOCITY_BEAM2 = 'velocity_beam2'                                            # PD442
    VELOCITY_BEAM3 = 'velocity_beam3'                                            # PD443
    AMPLITUDE_BEAM1 = 'amplitude_beam1'                                          # PD444
    AMPLITUDE_BEAM2 = 'amplitude_beam2'                                          # PD445
    AMPLITUDE_BEAM3 = 'amplitude_beam3'                                          # PD446

    # Particle field enums for extracting from Diagnostics Header records
    RECORDS_TO_FOLLOW = 'records_to_follow'                                      # PD447
    CELL_NUMBER_DIAGNOSTICS = 'cell_number_diagnostics'                          # PD448
    NOISE_AMPLITUDE_BEAM1 = 'noise_amplitude_beam1'                              # PD449
    NOISE_AMPLITUDE_BEAM2 = 'noise_amplitude_beam2'                              # PD450
    NOISE_AMPLITUDE_BEAM3 = 'noise_amplitude_beam3'                              # PD451
    NOISE_AMPLITUDE_BEAM4 = 'noise_amplitude_beam4'                              # PD452
    PROCESSING_MAGNITUDE_BEAM1 = 'processing_magnitude_beam1'                    # PD453
    PROCESSING_MAGNITUDE_BEAM2 = 'processing_magnitude_beam2'                    # PD454
    PROCESSING_MAGNITUDE_BEAM3 = 'processing_magnitude_beam3'                    # PD455
    PROCESSING_MAGNITUDE_BEAM4 = 'processing_magnitude_beam4'                    # PD456
    DISTANCE_BEAM1 = 'distance_beam1'                                            # PD457
    DISTANCE_BEAM2 = 'distance_beam2'                                            # PD458
    DISTANCE_BEAM3 = 'distance_beam3'                                            # PD459
    DISTANCE_BEAM4 = 'distance_beam4'                                            # PD460

    # Particle field enums for extracting from the Hardware Configuration record
    INSTRUMENT_TYPE_SERIAL_NUMBER = 'instrmt_type_serial_number'                 # PD461
    PIC_VERSION = 'pic_version'                                                  # PD465
    HARDWARE_REVISION = 'hardware_revision'                                      # PD466
    RECORDER_SIZE = 'recorder_size'                                              # PD467
    VELOCITY_RANGE = 'velocity_range'                                            # PD468
    FIRMWARE_VERSION = 'firmware_version'                                        # PD113

    # Particle field enums for extracting from the Head Configuration record
    PRESSURE_SENSOR = 'pressure_sensor'                                          # PD470
    MAGNETOMETER = 'magnetometer_sensor'                                         # PD471
    TILT_SENSOR = 'tilt_sensor'                                                  # PD472
    TILT_SENSOR_MOUNTING = 'tilt_sensor_mounting'                                # PD473
    HEAD_FREQUENCY = 'head_frequency'                                            # PD474
    HEAD_TYPE = 'head_type'                                                      # PD475
    HEAD_SERIAL_NUMBER = 'head_serial_number'                                    # PD476
    NUMBER_OF_BEAMS = 'number_beams'                                             # PD478

    # Particle field enums for extracting from the User Configuration record
    TRANSMIT_PULSE_LENGTH = 'transmit_pulse_length'                              # PD479
    BLANKING_DISTANCE = 'blanking_distance'                                      # PD480
    RECEIVE_LENGTH = 'receive_length'                                            # PD481
    TIME_BETWEEN_PINGS = 'time_between_pings'                                    # PD482
    TIME_BETWEEN_BURSTS = 'time_between_bursts'                                  # PD483
    NUMBER_OF_BEAM_SEQUENCES = 'number_of_beam_sequences'                        # PD2931
    AVERAGE_INTERVAL = 'average_interval'                                        # PD485
    COMPASS_UPDATE_RATE = 'compass_update_rate'                                  # PD496
    COORDINATE_SYSTEM = 'coordinate_system'                                      # PD497
    NUMBER_CELLS = 'number_cells'                                                # PD498
    MEASUREMENT_INTERVAL = 'measurement_interval'                                # PD500
    DEPLOYMENT_NAME = 'deployment_name'                                          # PD501
    DIAGNOSTICS_INTERVAL = 'diagnostics_interval'                                # PD504
    USE_SPECIFIED_SOUND_SPEED = 'use_specified_sound_speed'                      # PD505
    DIAGNOSTICS_MODE_ENABLE = 'diagnostics_mode_enable'                          # PD506
    ANALOG_OUTPUT_ENABLE = 'analog_output_enable'                                # PD507
    OUTPUT_FORMAT_NORTEK = 'output_format_nortek'                                # PD508
    SCALING = 'scaling'                                                          # PD509
    SERIAL_OUTPUT_ENABLE = 'serial_output_enable'                                # PD510
    RESERVED_BIT_EASYQ = 'reserved_bit_easyq'                                    # PD2933
    STAGE_ENABLE = 'stage_enable'                                                # PD511
    ANALOG_POWER_OUTPUT = 'analog_power_output'                                  # PD512
    SOUND_SPEED_ADJUST_FACTOR = 'sound_speed_adjust_factor'                      # PD513
    NUMBER_DIAGNOSTIC_SAMPLES = 'number_diagnostics_samples'                      # PD514
    NUMBER_OF_BEAMS_IN_DIAGNOSTICS_MODE = 'number_of_beams_in_diagnostics_mode'  # PD2932
    NUMBER_PINGS_DIAGNOSTIC = 'number_pings_diagnostic'                          # PD516
    SOFTWARE_VERSION = 'software_version'                                        # PD520
    CORRELATION_THRESHOLD = 'correlation_threshold'                              # PD533


class VelptAbDataParticle(DataParticle):
    """
    Class for creating the metadata & data particles for velpt_ab

    Note that data in the velpt_ab instrument is little endian and
    the bits within a word are numbered right to left.
    (See the System Integrator Manual, page 11.)
    """
    # Offsets for date-time group in velocity and diagnostics data records
    minute_offset = 4
    second_offset = 5
    day_offset = 6
    hour_offset = 7
    year_offset = 8
    month_offset = 9

    # Offsets for Velocity and Diagnostics Data records
    error_code_offset = 10
    analog1_offset = 12
    battery_voltage_offset = 14
    sound_speed_analog2_offset = 16
    heading_offset = 18
    pitch_offset = 20
    roll_offset = 22
    pressure_msb_offset = 24
    status_offset = 25
    pressure_lsw_offset = 26
    temperature_offset = 28
    velocity_beam1_offset = 30
    velocity_beam2_offset = 32
    velocity_beam3_offset = 34
    amplitude_beam1_offset = 36
    amplitude_beam2_offset = 37
    amplitude_beam3_offset = 38

    # Offsets for Diagnostics Header records
    records_to_follow_offset = 4
    cell_number_diagnostics_offset = 6
    noise_amplitude_beam1_offset = 8
    noise_amplitude_beam2_offset = 9
    noise_amplitude_beam3_offset = 10
    noise_amplitude_beam4_offset = 11
    processing_magnitude_beam1_offset = 12
    processing_magnitude_beam2_offset = 14
    processing_magnitude_beam3_offset = 16
    processing_magnitude_beam4_offset = 18
    distance_beam1_offset = 20
    distance_beam2_offset = 22
    distance_beam3_offset = 24
    distance_beam4_offset = 26

    # Offsets for the Hardware Configuration record
    instrument_type_serial_number_offset = 4
    end_instrument_serial_number_offset = 18
    pic_version_offset = 22
    hardware_revision_offset = 24
    recorder_size_offset = 26
    velocity_range_offset = 28
    end_velocity_range_offset = 30
    firmware_version_offset = 42       # 4 bytes

    # Bit masks for the velocity range word
    velocity_range_mask = 0x0001       # Bit 0

    # Offsets for the Head Configuration record
    config_offset = 4
    head_frequency_offset = 6
    head_type_offset = 8
    head_serial_number_offset = 10
    end_head_serial_number_offset = 22
    number_of_beams_offset = 220        # 2 bytes

    # Bit masks for the config word
    pressure_sensor_mask = 0x0001       # Bit 0
    magnetometer_mask = 0x0002          # Bit 1
    tilt_sensor_mask = 0x0004           # Bit 2
    tilt_sensor_mounting_mask = 0x0008  # Bit 3

    # Offsets for the User Configuration record
    transmit_pulse_length_offset = 4
    blanking_distance_offset = 6
    receive_length_offset = 8
    time_between_pings_offset = 10
    time_between_bursts_offset = 12
    number_of_beam_sequences_offset = 14
    average_interval_offset = 16
    end_average_interval_offset = 18
    compass_update_rate_offset = 30
    coordinate_system_offset = 32
    number_cells_offset = 34
    end_number_cells_offset = 36
    measurement_interval_offset = 38
    deployment_name_offset = 40
    end_deployment_name_offset = 46
    diagnostics_interval_offset = 54
    mode_offset = 58
    sound_speed_adjust_factor_offset = 60
    number_diagnostic_samples_offset = 62
    number_of_beams_in_diagnostics_mode_offset = 64
    number_pings_diagnostic_offset = 66
    end_number_pings_diagnostic_offset = 68
    software_version_offset = 72
    end_software_version_offset = 74
    correlation_threshold_offset = 458      # 2 bytes

    # Bit masks for the mode word
    use_specified_sound_speed_mask = 0x0001  # Bit 0
    diagnostics_mode_enable_mask = 0x0002    # Bit 1
    analog_output_enable_mask = 0x0004       # Bit 2
    output_format_nortek_mask = 0x0008       # Bit 3
    scaling_mask = 0x0010                    # Bit 4
    serial_output_enable_mask = 0x0020       # Bit 5
    reserved_bit_easyq_mask = 0x0040         # Bit 6
    stage_enable_mask = 0x0080               # Bit 7
    analog_power_output_mask = 0x0100        # Bit 8

    @staticmethod
    def _rstrip_non_ascii(in_string):
        return ''.join(c for c in in_string if 0 < ord(c) < 127)

    @staticmethod
    def _convert_bcd_to_decimal(in_val):
        """
        Converts Binary Coded Decimal to a decimal value
        :param in_val: The value to convert
        :return: The decimal value
        """
        tens = (struct.unpack('B', in_val)[0]) >> 4
        actual = struct.unpack('B', in_val)[0]
        low_byte = tens << 4
        return (tens*10) + (actual-low_byte)

    @staticmethod
    def _convert_bcd_to_string(in_val):
        """
        Converts Binary Coded Decimal to a string
        :param in_val: The value to convert
        :return: The string value
        """
        tens = (struct.unpack('B', in_val)[0]) >> 4
        part1 = struct.pack('B', tens+48)
        actual = struct.unpack('B', in_val)[0]
        low_byte = tens << 4
        part2 = struct.pack('B', (actual-low_byte)+48)
        return part1 + part2

    @staticmethod
    def get_date_time_string(record):
        """
        Convert the date and time from the record to the standard string YYYY/MM/DD HH:MM:SS
        :param record: The record read from the file which contains the date and time
        :return: The date time string
        """
        year = '20' + VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.year_offset])
        month = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.month_offset])
        day = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.day_offset])
        hour = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.hour_offset])
        minute = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.minute_offset])
        second = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.second_offset])
        return year+'/'+month+'/'+day+' '+hour+':'+minute+':'+second

    @staticmethod
    def get_timestamp(record):
        """
        Convert the date and time from the record to a Unix timestamp
        :param record: The record read from the file which contains the date and time
        :return: the Unix timestamp
        """
        year = 2000 + VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.year_offset])
        month = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.month_offset])
        day = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.day_offset])
        hour = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.hour_offset])
        minute = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.minute_offset])
        second = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.second_offset])
        timestamp = (year, month, day, hour, minute, second, 0, 0, 0)
        elapsed_seconds = calendar.timegm(timestamp)

        return float(ntplib.system_to_ntp_time(elapsed_seconds))

    @staticmethod
    def get_diagnostics_count(record):
        """
        Read the expected number of diagnostics records to follow the header
        :param record: The record read from the file which contains the date and time
        :return: The number of expected diagnostics records.
        """
        return struct.unpack('<h', record[VelptAbDataParticle.records_to_follow_offset:
                                          VelptAbDataParticle.cell_number_diagnostics_offset])[0]

    @staticmethod
    def generate_data_dict(record):
        """
        Pull the needed fields from the data file and convert them
        to the format needed for the particle per the IDD. Then put
        that data in a dictionary
        :param record: The record read from the file which contains the date and time
        :return: The dictionary
        """

        date_time_string = VelptAbDataParticle.get_date_time_string(record)

        error_code = struct.unpack_from('<H', record, VelptAbDataParticle.error_code_offset)[0]
        analog_1 = struct.unpack_from('<H', record, VelptAbDataParticle.analog1_offset)[0]
        battery_voltage = struct.unpack_from('<H', record, VelptAbDataParticle.battery_voltage_offset)[0]
        sound_speed_analog_2 = struct.unpack_from('<H', record, VelptAbDataParticle.sound_speed_analog2_offset)[0]
        heading = struct.unpack_from('<h', record, VelptAbDataParticle.heading_offset)[0]
        pitch = struct.unpack_from('<h', record, VelptAbDataParticle.pitch_offset)[0]
        roll = struct.unpack_from('<h', record, VelptAbDataParticle.roll_offset)[0]

        pressure_mbar = (struct.unpack_from('B', record, VelptAbDataParticle.pressure_msb_offset)[0] << 16) + \
                        (struct.unpack_from('<H', record, VelptAbDataParticle.pressure_lsw_offset)[0])

        status = struct.unpack_from('B', record, VelptAbDataParticle.status_offset)[0]
        temperature = struct.unpack_from('<h', record, VelptAbDataParticle.temperature_offset)[0]
        velocity_beam_1 = struct.unpack_from('<h', record, VelptAbDataParticle.velocity_beam1_offset)[0]
        velocity_beam_2 = struct.unpack_from('<h', record, VelptAbDataParticle.velocity_beam2_offset)[0]
        velocity_beam_3 = struct.unpack_from('<h', record, VelptAbDataParticle.velocity_beam3_offset)[0]
        amplitude_beam_1 = struct.unpack_from('B', record, VelptAbDataParticle.amplitude_beam1_offset)[0]
        amplitude_beam_2 = struct.unpack_from('B', record, VelptAbDataParticle.amplitude_beam2_offset)[0]
        amplitude_beam_3 = struct.unpack_from('B', record, VelptAbDataParticle.amplitude_beam3_offset)[0]

        return {VelptAbDataParticleKey.DATE_TIME_STRING: date_time_string,
                VelptAbDataParticleKey.ERROR_CODE: error_code,
                VelptAbDataParticleKey.ANALOG1: analog_1,
                VelptAbDataParticleKey.BATTERY_VOLTAGE_DV: battery_voltage,
                VelptAbDataParticleKey.SOUND_SPEED_DMS: sound_speed_analog_2,
                VelptAbDataParticleKey.HEADING_DECIDEGREE: heading,
                VelptAbDataParticleKey.PITCH_DECIDEGREE: pitch,
                VelptAbDataParticleKey.ROLL_DECIDEGREE: roll,
                VelptAbDataParticleKey.PRESSURE_MBAR: pressure_mbar,
                VelptAbDataParticleKey.STATUS: status,
                VelptAbDataParticleKey.TEMPERATURE_CENTIDEGREE: temperature,
                VelptAbDataParticleKey.VELOCITY_BEAM1: velocity_beam_1,
                VelptAbDataParticleKey.VELOCITY_BEAM2: velocity_beam_2,
                VelptAbDataParticleKey.VELOCITY_BEAM3: velocity_beam_3,
                VelptAbDataParticleKey.AMPLITUDE_BEAM1: amplitude_beam_1,
                VelptAbDataParticleKey.AMPLITUDE_BEAM2: amplitude_beam_2,
                VelptAbDataParticleKey.AMPLITUDE_BEAM3: amplitude_beam_3}

    @staticmethod
    def generate_diagnostics_header_dict(date_time_string, record):
        """
        Pull the needed fields from the data file and convert them
        to the format needed for the particle per the IDD. Then put
        that data in a dictionary
        :param record: The record read from the file which contains the date and time
        :return: The dictionary
        """
        records_to_follow = struct.unpack_from('<H', record, VelptAbDataParticle.records_to_follow_offset)[0]
        cell_number_diagnostics = struct.unpack_from('<H', record,
                                                     VelptAbDataParticle.cell_number_diagnostics_offset)[0]
        noise_amplitude_beam1 = struct.unpack_from('B', record, VelptAbDataParticle.noise_amplitude_beam1_offset)[0]
        noise_amplitude_beam2 = struct.unpack_from('B', record, VelptAbDataParticle.noise_amplitude_beam2_offset)[0]
        noise_amplitude_beam3 = struct.unpack_from('B', record, VelptAbDataParticle.noise_amplitude_beam3_offset)[0]
        noise_amplitude_beam4 = struct.unpack_from('B', record, VelptAbDataParticle.noise_amplitude_beam4_offset)[0]
        processing_magnitude_beam1 = struct.unpack_from('<H', record,
                                                        VelptAbDataParticle.processing_magnitude_beam1_offset)[0]
        processing_magnitude_beam2 = struct.unpack_from('<H', record,
                                                        VelptAbDataParticle.processing_magnitude_beam2_offset)[0]
        processing_magnitude_beam3 = struct.unpack_from('<H', record,
                                                        VelptAbDataParticle.processing_magnitude_beam3_offset)[0]
        processing_magnitude_beam4 = struct.unpack_from('<H', record,
                                                        VelptAbDataParticle.processing_magnitude_beam4_offset)[0]
        distance_beam1 = struct.unpack_from('<H', record, VelptAbDataParticle.distance_beam1_offset)[0]
        distance_beam2 = struct.unpack_from('<H', record, VelptAbDataParticle.distance_beam2_offset)[0]
        distance_beam3 = struct.unpack_from('<H', record, VelptAbDataParticle.distance_beam3_offset)[0]
        distance_beam4 = struct.unpack_from('<H', record, VelptAbDataParticle.distance_beam4_offset)[0]

        return {VelptAbDataParticleKey.DATE_TIME_STRING: date_time_string,
                VelptAbDataParticleKey.RECORDS_TO_FOLLOW: records_to_follow,
                VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS: cell_number_diagnostics,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1: noise_amplitude_beam1,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2: noise_amplitude_beam2,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3: noise_amplitude_beam3,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4: noise_amplitude_beam4,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1: processing_magnitude_beam1,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2: processing_magnitude_beam2,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3: processing_magnitude_beam3,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4: processing_magnitude_beam4,
                VelptAbDataParticleKey.DISTANCE_BEAM1: distance_beam1,
                VelptAbDataParticleKey.DISTANCE_BEAM2: distance_beam2,
                VelptAbDataParticleKey.DISTANCE_BEAM3: distance_beam3,
                VelptAbDataParticleKey.DISTANCE_BEAM4: distance_beam4}

    @staticmethod
    def generate_empty_hardware_config_dict():
        """
        In the even that no hardware configuration record was found in the recovered file,
        have a dummy dict set up to act as a placeholder in the particle.
        """
        return {VelptAbDataParticleKey.INSTRUMENT_TYPE_SERIAL_NUMBER: '',
                VelptAbDataParticleKey.PIC_VERSION: 0,
                VelptAbDataParticleKey.HARDWARE_REVISION: 0,
                VelptAbDataParticleKey.RECORDER_SIZE: 0,
                VelptAbDataParticleKey.VELOCITY_RANGE: 0,
                VelptAbDataParticleKey.FIRMWARE_VERSION: ''}

    @staticmethod
    def generate_empty_head_config_dict():
        """
        In the even that no head configuration record was found in the recovered file,
        have a dummy dict set up to act as a placeholder in the particle.
        """
        return {VelptAbDataParticleKey.PRESSURE_SENSOR: 0,
                VelptAbDataParticleKey.MAGNETOMETER: 0,
                VelptAbDataParticleKey.TILT_SENSOR: 0,
                VelptAbDataParticleKey.TILT_SENSOR_MOUNTING: 0,
                VelptAbDataParticleKey.HEAD_FREQUENCY: 0,
                VelptAbDataParticleKey.HEAD_TYPE: 0,
                VelptAbDataParticleKey.HEAD_SERIAL_NUMBER: '',
                VelptAbDataParticleKey.NUMBER_OF_BEAMS: 0}

    @staticmethod
    def generate_empty_user_config_dict():
        """
        In the even that no user configuration record was found in the recovered file,
        have a dummy dict set up to act as a placeholder in the particle.
        """
        return {VelptAbDataParticleKey.TRANSMIT_PULSE_LENGTH: 0,
                VelptAbDataParticleKey.BLANKING_DISTANCE: 0,
                VelptAbDataParticleKey.RECEIVE_LENGTH: 0,
                VelptAbDataParticleKey.TIME_BETWEEN_PINGS: 0,
                VelptAbDataParticleKey.TIME_BETWEEN_BURSTS: 0,
                VelptAbDataParticleKey.NUMBER_OF_BEAM_SEQUENCES: 0,
                VelptAbDataParticleKey.AVERAGE_INTERVAL: 0,
                VelptAbDataParticleKey.COMPASS_UPDATE_RATE: 0,
                VelptAbDataParticleKey.COORDINATE_SYSTEM: 0,
                VelptAbDataParticleKey.NUMBER_CELLS: 0,
                VelptAbDataParticleKey.MEASUREMENT_INTERVAL: 0,
                VelptAbDataParticleKey.DEPLOYMENT_NAME: '',
                VelptAbDataParticleKey.DIAGNOSTICS_INTERVAL: 0,
                VelptAbDataParticleKey.USE_SPECIFIED_SOUND_SPEED: 0,
                VelptAbDataParticleKey.DIAGNOSTICS_MODE_ENABLE: 0,
                VelptAbDataParticleKey.ANALOG_OUTPUT_ENABLE: 0,
                VelptAbDataParticleKey.OUTPUT_FORMAT_NORTEK: 0,
                VelptAbDataParticleKey.SCALING: 0,
                VelptAbDataParticleKey.SERIAL_OUTPUT_ENABLE: 0,
                VelptAbDataParticleKey.RESERVED_BIT_EASYQ: 0,
                VelptAbDataParticleKey.STAGE_ENABLE: 0,
                VelptAbDataParticleKey.ANALOG_POWER_OUTPUT: 0,
                VelptAbDataParticleKey.SOUND_SPEED_ADJUST_FACTOR: 0,
                VelptAbDataParticleKey.NUMBER_DIAGNOSTIC_SAMPLES: 0,
                VelptAbDataParticleKey.NUMBER_OF_BEAMS_IN_DIAGNOSTICS_MODE: 0,
                VelptAbDataParticleKey.NUMBER_PINGS_DIAGNOSTIC: 0,
                VelptAbDataParticleKey.SOFTWARE_VERSION: '',
                VelptAbDataParticleKey.CORRELATION_THRESHOLD: 0}

    @staticmethod
    def generate_hardware_config_dict(record):
        """
        Builds up the metadata dict from the hardware configuration record
        """
        instrument_type_serial_number = struct.unpack_from('14s', record,
                                                           VelptAbDataParticle.instrument_type_serial_number_offset)[0]
        instrument_type_serial_number = instrument_type_serial_number.rstrip()
        instrument_type_serial_number = VelptAbDataParticle._rstrip_non_ascii(instrument_type_serial_number)

        pic_version = struct.unpack_from('<H', record, VelptAbDataParticle.pic_version_offset)[0]
        hardware_revision = struct.unpack_from('<H', record, VelptAbDataParticle.hardware_revision_offset)[0]
        recorder_size = struct.unpack_from('<H', record, VelptAbDataParticle.recorder_size_offset)[0] << 16
        velocity_range = struct.unpack_from('<H', record, VelptAbDataParticle.velocity_range_offset)[0] & \
            VelptAbDataParticle.velocity_range_mask

        firmware_version = struct.unpack_from('4s', record, VelptAbDataParticle.firmware_version_offset)[0]
        firmware_version = firmware_version.rstrip()
        firmware_version = VelptAbDataParticle._rstrip_non_ascii(firmware_version)

        return {VelptAbDataParticleKey.INSTRUMENT_TYPE_SERIAL_NUMBER: instrument_type_serial_number,
                VelptAbDataParticleKey.PIC_VERSION: pic_version,
                VelptAbDataParticleKey.HARDWARE_REVISION: hardware_revision,
                VelptAbDataParticleKey.RECORDER_SIZE: recorder_size,
                VelptAbDataParticleKey.VELOCITY_RANGE: velocity_range,
                VelptAbDataParticleKey.FIRMWARE_VERSION: firmware_version}

    @staticmethod
    def generate_head_config_dict(record):
        """
        Builds up the metadata dict from the head configuration record
        """
        head_config = struct.unpack_from('<H', record, VelptAbDataParticle.config_offset)[0]
        pressure_sensor = head_config & VelptAbDataParticle.pressure_sensor_mask
        magnetometer = (head_config & VelptAbDataParticle.magnetometer_mask) >> 1
        tilt_sensor = (head_config & VelptAbDataParticle.tilt_sensor_mask) >> 2
        tilt_sensor_mounting = (head_config & VelptAbDataParticle.tilt_sensor_mounting_mask) >> 3

        head_frequency = struct.unpack_from('<H', record, VelptAbDataParticle.head_frequency_offset)[0]
        head_type = struct.unpack_from('<H', record, VelptAbDataParticle.head_type_offset)[0]
        head_serial_number = struct.unpack_from('12s', record, VelptAbDataParticle.head_serial_number_offset)[0]
        head_serial_number = head_serial_number.rstrip()
        head_serial_number = VelptAbDataParticle._rstrip_non_ascii(head_serial_number)

        number_of_beams = struct.unpack_from('<H', record, VelptAbDataParticle.number_of_beams_offset)[0]

        return {VelptAbDataParticleKey.PRESSURE_SENSOR: pressure_sensor,
                VelptAbDataParticleKey.MAGNETOMETER: magnetometer,
                VelptAbDataParticleKey.TILT_SENSOR: tilt_sensor,
                VelptAbDataParticleKey.TILT_SENSOR_MOUNTING: tilt_sensor_mounting,
                VelptAbDataParticleKey.HEAD_FREQUENCY: head_frequency,
                VelptAbDataParticleKey.HEAD_TYPE: head_type,
                VelptAbDataParticleKey.HEAD_SERIAL_NUMBER: head_serial_number,
                VelptAbDataParticleKey.NUMBER_OF_BEAMS: number_of_beams}

    @staticmethod
    def generate_user_config_dict(record):
        """
        Builds up the metadata dict from the user configuration record
        """
        transmit_pulse_length = struct.unpack_from('<H', record, VelptAbDataParticle.transmit_pulse_length_offset)[0]
        blanking_distance = struct.unpack_from('<H', record, VelptAbDataParticle.blanking_distance_offset)[0]
        receive_length = struct.unpack_from('<H', record, VelptAbDataParticle.receive_length_offset)[0]
        time_between_pings = struct.unpack_from('<H', record, VelptAbDataParticle.time_between_pings_offset)[0]
        time_between_bursts = struct.unpack_from('<H', record, VelptAbDataParticle.time_between_bursts_offset)[0]
        number_of_beam_sequences = struct.unpack_from('<H', record,
                                                      VelptAbDataParticle.number_of_beam_sequences_offset)[0]
        average_interval = struct.unpack_from('<H', record, VelptAbDataParticle.average_interval_offset)[0]
        compass_update_rate = struct.unpack_from('<H', record, VelptAbDataParticle.compass_update_rate_offset)[0]
        coordinate_system = struct.unpack_from('<H', record, VelptAbDataParticle.coordinate_system_offset)[0]
        number_cells = struct.unpack_from('<H', record, VelptAbDataParticle.number_cells_offset)[0]
        measurement_interval = struct.unpack_from('<H', record, VelptAbDataParticle.measurement_interval_offset)[0]
        deployment_name = struct.unpack_from('6s', record, VelptAbDataParticle.deployment_name_offset)[0]
        deployment_name = deployment_name.rstrip()
        deployment_name = VelptAbDataParticle._rstrip_non_ascii(deployment_name)

        diagnostics_interval = struct.unpack_from('<I', record, VelptAbDataParticle.diagnostics_interval_offset)[0]
        mode = struct.unpack_from('<H', record, VelptAbDataParticle.mode_offset)[0]
        sound_speed_adjust_factor = struct.unpack_from('<H', record,
                                                       VelptAbDataParticle.sound_speed_adjust_factor_offset)[0]
        number_diagnostic_samples = struct.unpack_from('<H', record,
                                                       VelptAbDataParticle.number_diagnostic_samples_offset)[0]
        number_of_beams_in_diagnostics_mode = struct.unpack_from('<H', record,
                                                                 VelptAbDataParticle.
                                                                 number_of_beams_in_diagnostics_mode_offset)[0]
        number_pings_diagnostic = struct.unpack_from('<H', record,
                                                     VelptAbDataParticle.number_pings_diagnostic_offset)[0]
        software_version = struct.unpack_from('2s', record, VelptAbDataParticle.software_version_offset)[0]
        software_version = software_version.rstrip()
        software_version = VelptAbDataParticle._rstrip_non_ascii(software_version)

        correlation_threshold = struct.unpack_from('<H', record, VelptAbDataParticle.correlation_threshold_offset)[0]

        use_specified_sound_speed = (mode & VelptAbDataParticle.use_specified_sound_speed_mask)
        diagnostics_mode_enable = (mode & VelptAbDataParticle.diagnostics_mode_enable_mask) >> 1
        analog_output_enable = (mode & VelptAbDataParticle.analog_output_enable_mask) >> 2
        output_format_nortek = (mode & VelptAbDataParticle.output_format_nortek_mask) >> 3
        scaling = (mode & VelptAbDataParticle.scaling_mask) >> 4
        serial_output_enable = (mode & VelptAbDataParticle.serial_output_enable_mask) >> 5
        reserved_bit_easyq = (mode & VelptAbDataParticle.reserved_bit_easyq_mask) >> 6
        stage_enable = (mode & VelptAbDataParticle.stage_enable_mask) >> 7
        analog_power_output = (mode & VelptAbDataParticle.analog_power_output_mask) >> 8

        return {VelptAbDataParticleKey.TRANSMIT_PULSE_LENGTH: transmit_pulse_length,
                VelptAbDataParticleKey.BLANKING_DISTANCE: blanking_distance,
                VelptAbDataParticleKey.RECEIVE_LENGTH: receive_length,
                VelptAbDataParticleKey.TIME_BETWEEN_PINGS: time_between_pings,
                VelptAbDataParticleKey.TIME_BETWEEN_BURSTS: time_between_bursts,
                VelptAbDataParticleKey.NUMBER_OF_BEAM_SEQUENCES: number_of_beam_sequences,
                VelptAbDataParticleKey.AVERAGE_INTERVAL: average_interval,
                VelptAbDataParticleKey.COMPASS_UPDATE_RATE: compass_update_rate,
                VelptAbDataParticleKey.COORDINATE_SYSTEM: coordinate_system,
                VelptAbDataParticleKey.NUMBER_CELLS: number_cells,
                VelptAbDataParticleKey.MEASUREMENT_INTERVAL: measurement_interval,
                VelptAbDataParticleKey.DEPLOYMENT_NAME: deployment_name,
                VelptAbDataParticleKey.DIAGNOSTICS_INTERVAL: diagnostics_interval,
                VelptAbDataParticleKey.USE_SPECIFIED_SOUND_SPEED: use_specified_sound_speed,
                VelptAbDataParticleKey.DIAGNOSTICS_MODE_ENABLE: diagnostics_mode_enable,
                VelptAbDataParticleKey.ANALOG_OUTPUT_ENABLE: analog_output_enable,
                VelptAbDataParticleKey.OUTPUT_FORMAT_NORTEK: output_format_nortek,
                VelptAbDataParticleKey.SCALING: scaling,
                VelptAbDataParticleKey.SERIAL_OUTPUT_ENABLE: serial_output_enable,
                VelptAbDataParticleKey.RESERVED_BIT_EASYQ: reserved_bit_easyq,
                VelptAbDataParticleKey.STAGE_ENABLE: stage_enable,
                VelptAbDataParticleKey.ANALOG_POWER_OUTPUT: analog_power_output,
                VelptAbDataParticleKey.SOUND_SPEED_ADJUST_FACTOR: sound_speed_adjust_factor,
                VelptAbDataParticleKey.NUMBER_DIAGNOSTIC_SAMPLES: number_diagnostic_samples,
                VelptAbDataParticleKey.NUMBER_OF_BEAMS_IN_DIAGNOSTICS_MODE: number_of_beams_in_diagnostics_mode,
                VelptAbDataParticleKey.NUMBER_PINGS_DIAGNOSTIC: number_pings_diagnostic,
                VelptAbDataParticleKey.SOFTWARE_VERSION: software_version,
                VelptAbDataParticleKey.CORRELATION_THRESHOLD: correlation_threshold}

    @staticmethod
    def generate_instrument_metadata_dict(date_time_group,
                                          hardware_config_dict,
                                          head_config_dict,
                                          user_config_dict):
        """
        Builds the metadata dict from the hardware, head and user configuration records.
        Also adds in the date time string extracted from the first instrument record.
        """
        metadata_dict = {VelptAbDataParticleKey.DATE_TIME_STRING: date_time_group}
        metadata_dict.update(hardware_config_dict)
        metadata_dict.update(head_config_dict)
        metadata_dict.update(user_config_dict)

        return metadata_dict


class VelptAbDiagnosticsHeaderParticle(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDataParticleType.VELPT_AB_DIAGNOSTICS_METADATA_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.RECORDS_TO_FOLLOW,
                                                      self.raw_data[VelptAbDataParticleKey.RECORDS_TO_FOLLOW], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS,
                                                      self.raw_data[VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM4], int))

        return particle_parameters


class VelptAbDiagnosticsDataParticle(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDataParticleType.VELPT_AB_DIAGNOSTICS_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.BATTERY_VOLTAGE_DV,
                                                      self.raw_data[VelptAbDataParticleKey.BATTERY_VOLTAGE_DV], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOUND_SPEED_DMS,
                                                      self.raw_data[VelptAbDataParticleKey.SOUND_SPEED_DMS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEADING_DECIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.HEADING_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PITCH_DECIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.PITCH_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ROLL_DECIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.ROLL_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PRESSURE_MBAR,
                                                      self.raw_data[VelptAbDataParticleKey.PRESSURE_MBAR], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TEMPERATURE_CENTIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.TEMPERATURE_CENTIDEGREE],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters


class VelptAbInstrumentMetadataParticle(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDataParticleType.VELPT_AB_INSTRUMENT_METADATA_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.INSTRUMENT_TYPE_SERIAL_NUMBER,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.INSTRUMENT_TYPE_SERIAL_NUMBER], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PIC_VERSION,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.PIC_VERSION], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HARDWARE_REVISION,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.HARDWARE_REVISION], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.RECORDER_SIZE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.RECORDER_SIZE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_RANGE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.VELOCITY_RANGE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.FIRMWARE_VERSION,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.FIRMWARE_VERSION], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PRESSURE_SENSOR,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.PRESSURE_SENSOR], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.MAGNETOMETER,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.MAGNETOMETER], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TILT_SENSOR,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.TILT_SENSOR], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TILT_SENSOR_MOUNTING,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.TILT_SENSOR_MOUNTING], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEAD_FREQUENCY,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.HEAD_FREQUENCY], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEAD_TYPE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.HEAD_TYPE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEAD_SERIAL_NUMBER,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.HEAD_SERIAL_NUMBER], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NUMBER_OF_BEAMS,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.NUMBER_OF_BEAMS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TRANSMIT_PULSE_LENGTH,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.TRANSMIT_PULSE_LENGTH], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.BLANKING_DISTANCE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.BLANKING_DISTANCE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.RECEIVE_LENGTH,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.RECEIVE_LENGTH], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TIME_BETWEEN_PINGS,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.TIME_BETWEEN_PINGS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TIME_BETWEEN_BURSTS,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.TIME_BETWEEN_BURSTS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NUMBER_OF_BEAM_SEQUENCES,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.NUMBER_OF_BEAM_SEQUENCES], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AVERAGE_INTERVAL,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.AVERAGE_INTERVAL], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.COMPASS_UPDATE_RATE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.COMPASS_UPDATE_RATE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.COORDINATE_SYSTEM,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.COORDINATE_SYSTEM], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NUMBER_CELLS,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.NUMBER_CELLS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.MEASUREMENT_INTERVAL,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.MEASUREMENT_INTERVAL], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DEPLOYMENT_NAME,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.DEPLOYMENT_NAME], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DIAGNOSTICS_INTERVAL,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.DIAGNOSTICS_INTERVAL], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.USE_SPECIFIED_SOUND_SPEED,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.USE_SPECIFIED_SOUND_SPEED], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DIAGNOSTICS_MODE_ENABLE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.DIAGNOSTICS_MODE_ENABLE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG_OUTPUT_ENABLE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.ANALOG_OUTPUT_ENABLE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.OUTPUT_FORMAT_NORTEK,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.OUTPUT_FORMAT_NORTEK], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SCALING,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.SCALING], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SERIAL_OUTPUT_ENABLE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.SERIAL_OUTPUT_ENABLE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.RESERVED_BIT_EASYQ,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.RESERVED_BIT_EASYQ], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.STAGE_ENABLE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.STAGE_ENABLE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG_POWER_OUTPUT,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.ANALOG_POWER_OUTPUT], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOUND_SPEED_ADJUST_FACTOR,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.SOUND_SPEED_ADJUST_FACTOR], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NUMBER_DIAGNOSTIC_SAMPLES,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.NUMBER_DIAGNOSTIC_SAMPLES], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NUMBER_OF_BEAMS_IN_DIAGNOSTICS_MODE,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.NUMBER_OF_BEAMS_IN_DIAGNOSTICS_MODE],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NUMBER_PINGS_DIAGNOSTIC,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.NUMBER_PINGS_DIAGNOSTIC], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOFTWARE_VERSION,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.SOFTWARE_VERSION], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.CORRELATION_THRESHOLD,
                                                      self.raw_data[
                                                          VelptAbDataParticleKey.CORRELATION_THRESHOLD], int))

        return particle_parameters


class VelptAbInstrumentDataParticle(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDataParticleType.VELPT_AB_INSTRUMENT_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.BATTERY_VOLTAGE_DV,
                                                      self.raw_data[VelptAbDataParticleKey.BATTERY_VOLTAGE_DV], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOUND_SPEED_DMS,
                                                      self.raw_data[VelptAbDataParticleKey.SOUND_SPEED_DMS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEADING_DECIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.HEADING_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PITCH_DECIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.PITCH_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ROLL_DECIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.ROLL_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PRESSURE_MBAR,
                                                      self.raw_data[VelptAbDataParticleKey.PRESSURE_MBAR], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TEMPERATURE_CENTIDEGREE,
                                                      self.raw_data[VelptAbDataParticleKey.TEMPERATURE_CENTIDEGREE],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters

