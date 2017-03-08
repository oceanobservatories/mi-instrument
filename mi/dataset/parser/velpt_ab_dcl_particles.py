#!/usr/bin/env python

"""
@package mi.dataset.parser
@file /mi/dataset/parser/velpt_ab_dcl.py
@author Chris Goodrich
@brief Parser for the velpt_ab_dcl recovered and telemetered dataset driver
Release notes:

initial release
"""
__author__ = 'Chris Goodrich'
__license__ = 'Apache 2.0'

import struct
import calendar
import ntplib
from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle


class VelptAbDclDataParticleType(BaseEnum):
    VELPT_AB_DCL_INSTRUMENT = 'velpt_ab_dcl_instrument'
    VELPT_AB_DCL_DIAGNOSTICS_METADATA = 'velpt_ab_dcl_diagnostics_metadata'
    VELPT_AB_DCL_DIAGNOSTICS = 'velpt_ab_dcl_diagnostics'
    VELPT_AB_DCL_INSTRUMENT_RECOVERED = 'velpt_ab_dcl_instrument_recovered'
    VELPT_AB_DCL_DIAGNOSTICS_METADATA_RECOVERED = 'velpt_ab_dcl_diagnostics_metadata_recovered'
    VELPT_AB_DCL_DIAGNOSTICS_RECOVERED = 'velpt_ab_dcl_diagnostics_recovered'


class VelptAbDclDataParticleKey(BaseEnum):
    DATE_TIME_STRING = 'date_time_string'                      # PD93
    ERROR_CODE = 'error_code'                                  # PD433
    ANALOG1 = 'analog1'                                        # PD434
    BATTERY_VOLTAGE_DV = 'battery_voltage_dV'                  # PD3242
    SOUND_SPEED_DMS = 'sound_speed_dms'                        # PD3243
    HEADING_DECIDEGREE = 'heading_decidegree'                  # PD3244
    PITCH_DECIDEGREE = 'pitch_decidegree'                      # PD3246
    ROLL_DECIDEGREE = 'roll_decidegree'                        # PD3245
    PRESSURE_MBAR = 'pressure_mbar'                            # PD3248
    STATUS = 'status'                                          # PD439
    TEMPERATURE_CENTIDEGREE = 'temperature_centidegree'        # PD3247
    VELOCITY_BEAM1 = 'velocity_beam1'                          # PD441
    VELOCITY_BEAM2 = 'velocity_beam2'                          # PD442
    VELOCITY_BEAM3 = 'velocity_beam3'                          # PD443
    AMPLITUDE_BEAM1 = 'amplitude_beam1'                        # PD444
    AMPLITUDE_BEAM2 = 'amplitude_beam2'                        # PD445
    AMPLITUDE_BEAM3 = 'amplitude_beam3'                        # PD446
    RECORDS_TO_FOLLOW = 'records_to_follow'                    # PD447
    CELL_NUMBER_DIAGNOSTICS = 'cell_number_diagnostics'        # PD448
    NOISE_AMPLITUDE_BEAM1 = 'noise_amplitude_beam1'            # PD449
    NOISE_AMPLITUDE_BEAM2 = 'noise_amplitude_beam2'            # PD450
    NOISE_AMPLITUDE_BEAM3 = 'noise_amplitude_beam3'            # PD451
    NOISE_AMPLITUDE_BEAM4 = 'noise_amplitude_beam4'            # PD452
    PROCESSING_MAGNITUDE_BEAM1 = 'processing_magnitude_beam1'  # PD453
    PROCESSING_MAGNITUDE_BEAM2 = 'processing_magnitude_beam2'  # PD454
    PROCESSING_MAGNITUDE_BEAM3 = 'processing_magnitude_beam3'  # PD455
    PROCESSING_MAGNITUDE_BEAM4 = 'processing_magnitude_beam4'  # PD456
    DISTANCE_BEAM1 = 'distance_beam1'                          # PD457
    DISTANCE_BEAM2 = 'distance_beam2'                          # PD458
    DISTANCE_BEAM3 = 'distance_beam3'                          # PD459
    DISTANCE_BEAM4 = 'distance_beam4'                          # PD460


class VelptAbDclDataParticle(DataParticle):
    """
    Class for creating the metadata & data particles for velpt_ab_dcl
    """
    # Offsets for date-time group in velocity and diagnostics data records
    minute_offset = 4
    second_offset = 5
    day_offset = 6
    hour_offset = 7
    year_offset = 8
    month_offset = 9

    # Offsets for data needed to build particles
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

    # Offsets for diagnostics header records
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
        year = '20' + VelptAbDclDataParticle._convert_bcd_to_string(record[VelptAbDclDataParticle.year_offset])
        month = VelptAbDclDataParticle._convert_bcd_to_string(record[VelptAbDclDataParticle.month_offset])
        day = VelptAbDclDataParticle._convert_bcd_to_string(record[VelptAbDclDataParticle.day_offset])
        hour = VelptAbDclDataParticle._convert_bcd_to_string(record[VelptAbDclDataParticle.hour_offset])
        minute = VelptAbDclDataParticle._convert_bcd_to_string(record[VelptAbDclDataParticle.minute_offset])
        second = VelptAbDclDataParticle._convert_bcd_to_string(record[VelptAbDclDataParticle.second_offset])
        return year+'/'+month+'/'+day+' '+hour+':'+minute+':'+second

    @staticmethod
    def get_timestamp(record):
        """
        Convert the date and time from the record to a Unix timestamp
        :param record: The record read from the file which contains the date and time
        :return: the Unix timestamp
        """
        year = 2000 + VelptAbDclDataParticle._convert_bcd_to_decimal(record[VelptAbDclDataParticle.year_offset])
        month = VelptAbDclDataParticle._convert_bcd_to_decimal(record[VelptAbDclDataParticle.month_offset])
        day = VelptAbDclDataParticle._convert_bcd_to_decimal(record[VelptAbDclDataParticle.day_offset])
        hour = VelptAbDclDataParticle._convert_bcd_to_decimal(record[VelptAbDclDataParticle.hour_offset])
        minute = VelptAbDclDataParticle._convert_bcd_to_decimal(record[VelptAbDclDataParticle.minute_offset])
        second = VelptAbDclDataParticle._convert_bcd_to_decimal(record[VelptAbDclDataParticle.second_offset])
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
        return struct.unpack('<h', record[VelptAbDclDataParticle.records_to_follow_offset:
                                          VelptAbDclDataParticle.cell_number_diagnostics_offset])[0]

    @staticmethod
    def generate_data_dict(record):
        """
        Pull the needed fields from the data file and convert them
        to the format needed for the particle per the IDD. Then put
        that data in a dictionary
        :param record: The record read from the file which contains the date and time
        :return: The dictionary
        """

        date_time_string = VelptAbDclDataParticle.get_date_time_string(record)

        error_code = struct.unpack_from('<H', record, VelptAbDclDataParticle.error_code_offset)[0]
        analog_1 = struct.unpack_from('<H', record, VelptAbDclDataParticle.analog1_offset)[0]
        battery_voltage = struct.unpack_from('<H', record, VelptAbDclDataParticle.battery_voltage_offset)[0]
        sound_speed_analog_2 = struct.unpack_from('<H', record, VelptAbDclDataParticle.sound_speed_analog2_offset)[0]
        heading = struct.unpack_from('<h', record, VelptAbDclDataParticle.heading_offset)[0]
        pitch = struct.unpack_from('<h', record, VelptAbDclDataParticle.pitch_offset)[0]
        roll = struct.unpack_from('<h', record, VelptAbDclDataParticle.roll_offset)[0]

        pressure_mbar = (struct.unpack_from('B', record, VelptAbDclDataParticle.pressure_msb_offset)[0] << 16) +\
                        (struct.unpack_from('<H', record, VelptAbDclDataParticle.pressure_lsw_offset)[0])

        status = struct.unpack_from('B', record, VelptAbDclDataParticle.status_offset)[0]
        temperature = struct.unpack_from('<h', record, VelptAbDclDataParticle.temperature_offset)[0]
        velocity_beam_1 = struct.unpack_from('<h', record, VelptAbDclDataParticle.velocity_beam1_offset)[0]
        velocity_beam_2 = struct.unpack_from('<h', record, VelptAbDclDataParticle.velocity_beam2_offset)[0]
        velocity_beam_3 = struct.unpack_from('<h', record, VelptAbDclDataParticle.velocity_beam3_offset)[0]
        amplitude_beam_1 = struct.unpack_from('B', record, VelptAbDclDataParticle.amplitude_beam1_offset)[0]
        amplitude_beam_2 = struct.unpack_from('B', record, VelptAbDclDataParticle.amplitude_beam2_offset)[0]
        amplitude_beam_3 = struct.unpack_from('B', record, VelptAbDclDataParticle.amplitude_beam3_offset)[0]

        return {VelptAbDclDataParticleKey.DATE_TIME_STRING: date_time_string,
                VelptAbDclDataParticleKey.ERROR_CODE: error_code,
                VelptAbDclDataParticleKey.ANALOG1: analog_1,
                VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV: battery_voltage,
                VelptAbDclDataParticleKey.SOUND_SPEED_DMS: sound_speed_analog_2,
                VelptAbDclDataParticleKey.HEADING_DECIDEGREE: heading,
                VelptAbDclDataParticleKey.PITCH_DECIDEGREE: pitch,
                VelptAbDclDataParticleKey.ROLL_DECIDEGREE: roll,
                VelptAbDclDataParticleKey.PRESSURE_MBAR: pressure_mbar,
                VelptAbDclDataParticleKey.STATUS: status,
                VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE: temperature,
                VelptAbDclDataParticleKey.VELOCITY_BEAM1: velocity_beam_1,
                VelptAbDclDataParticleKey.VELOCITY_BEAM2: velocity_beam_2,
                VelptAbDclDataParticleKey.VELOCITY_BEAM3: velocity_beam_3,
                VelptAbDclDataParticleKey.AMPLITUDE_BEAM1: amplitude_beam_1,
                VelptAbDclDataParticleKey.AMPLITUDE_BEAM2: amplitude_beam_2,
                VelptAbDclDataParticleKey.AMPLITUDE_BEAM3: amplitude_beam_3}

    @staticmethod
    def generate_diagnostics_header_dict(date_time_string, record):
        """
        Pull the needed fields from the data file and convert them
        to the format needed for the particle per the IDD. Then put
        that data in a dictionary
        :param record: The record read from the file which contains the date and time
        :return: The dictionary
        """
        records_to_follow = struct.unpack_from('<h', record, VelptAbDclDataParticle.records_to_follow_offset)[0]
        cell_number_diagnostics = struct.unpack_from('<h', record,
                                                     VelptAbDclDataParticle.cell_number_diagnostics_offset)[0]
        noise_amplitude_beam1 = struct.unpack_from('B', record, VelptAbDclDataParticle.noise_amplitude_beam1_offset)[0]
        noise_amplitude_beam2 = struct.unpack_from('B', record, VelptAbDclDataParticle.noise_amplitude_beam2_offset)[0]
        noise_amplitude_beam3 = struct.unpack_from('B', record, VelptAbDclDataParticle.noise_amplitude_beam3_offset)[0]
        noise_amplitude_beam4 = struct.unpack_from('B', record, VelptAbDclDataParticle.noise_amplitude_beam4_offset)[0]
        processing_magnitude_beam1 = struct.unpack_from('<h', record,
                                                        VelptAbDclDataParticle.processing_magnitude_beam1_offset)[0]
        processing_magnitude_beam2 = struct.unpack_from('<h', record,
                                                        VelptAbDclDataParticle.processing_magnitude_beam2_offset)[0]
        processing_magnitude_beam3 = struct.unpack_from('<h', record,
                                                        VelptAbDclDataParticle.processing_magnitude_beam3_offset)[0]
        processing_magnitude_beam4 = struct.unpack_from('<h', record,
                                                        VelptAbDclDataParticle.processing_magnitude_beam4_offset)[0]
        distance_beam1 = struct.unpack_from('<h', record, VelptAbDclDataParticle.distance_beam1_offset)[0]
        distance_beam2 = struct.unpack_from('<h', record, VelptAbDclDataParticle.distance_beam2_offset)[0]
        distance_beam3 = struct.unpack_from('<h', record, VelptAbDclDataParticle.distance_beam3_offset)[0]
        distance_beam4 = struct.unpack_from('<h', record, VelptAbDclDataParticle.distance_beam4_offset)[0]

        return {VelptAbDclDataParticleKey.DATE_TIME_STRING: date_time_string,
                VelptAbDclDataParticleKey.RECORDS_TO_FOLLOW: records_to_follow,
                VelptAbDclDataParticleKey.CELL_NUMBER_DIAGNOSTICS: cell_number_diagnostics,
                VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM1: noise_amplitude_beam1,
                VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM2: noise_amplitude_beam2,
                VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM3: noise_amplitude_beam3,
                VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM4: noise_amplitude_beam4,
                VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM1: processing_magnitude_beam1,
                VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM2: processing_magnitude_beam2,
                VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM3: processing_magnitude_beam3,
                VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM4: processing_magnitude_beam4,
                VelptAbDclDataParticleKey.DISTANCE_BEAM1: distance_beam1,
                VelptAbDclDataParticleKey.DISTANCE_BEAM2: distance_beam2,
                VelptAbDclDataParticleKey.DISTANCE_BEAM3: distance_beam3,
                VelptAbDclDataParticleKey.DISTANCE_BEAM4: distance_beam4}


class VelptAbDclInstrumentDataParticle(VelptAbDclDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDclDataParticleType.VELPT_AB_DCL_INSTRUMENT

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDclDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDclDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV,
                                                      self.raw_data[VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.SOUND_SPEED_DMS,
                                                      self.raw_data[VelptAbDclDataParticleKey.SOUND_SPEED_DMS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.HEADING_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.HEADING_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PITCH_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.PITCH_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ROLL_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ROLL_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PRESSURE_MBAR,
                                                      self.raw_data[VelptAbDclDataParticleKey.PRESSURE_MBAR], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDclDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters


class VelptAbDclDiagnosticsHeaderParticle(VelptAbDclDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDclDataParticleType.VELPT_AB_DCL_DIAGNOSTICS_METADATA

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDclDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.RECORDS_TO_FOLLOW,
                                                      self.raw_data[VelptAbDclDataParticleKey.RECORDS_TO_FOLLOW], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.CELL_NUMBER_DIAGNOSTICS,
                                                      self.raw_data[VelptAbDclDataParticleKey.CELL_NUMBER_DIAGNOSTICS],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM1],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM2],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM3],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM4,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM4],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM1,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM1],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM2,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM2],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM3,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM3],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM4,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM4],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM4,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM4], int))

        return particle_parameters


class VelptAbDclDiagnosticsDataParticle(VelptAbDclDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDclDataParticleType.VELPT_AB_DCL_DIAGNOSTICS

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDclDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDclDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV,
                                                      self.raw_data[VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.SOUND_SPEED_DMS,
                                                      self.raw_data[VelptAbDclDataParticleKey.SOUND_SPEED_DMS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.HEADING_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.HEADING_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PITCH_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.PITCH_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ROLL_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ROLL_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PRESSURE_MBAR,
                                                      self.raw_data[VelptAbDclDataParticleKey.PRESSURE_MBAR], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDclDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters


class VelptAbDclInstrumentDataParticleRecovered(VelptAbDclDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDclDataParticleType.VELPT_AB_DCL_INSTRUMENT_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDclDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDclDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV,
                                                      self.raw_data[VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.SOUND_SPEED_DMS,
                                                      self.raw_data[VelptAbDclDataParticleKey.SOUND_SPEED_DMS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.HEADING_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.HEADING_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PITCH_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.PITCH_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ROLL_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ROLL_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PRESSURE_MBAR,
                                                      self.raw_data[VelptAbDclDataParticleKey.PRESSURE_MBAR], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDclDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters


class VelptAbDclDiagnosticsHeaderParticleRecovered(VelptAbDclDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDclDataParticleType.VELPT_AB_DCL_DIAGNOSTICS_METADATA_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDclDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.RECORDS_TO_FOLLOW,
                                                      self.raw_data[VelptAbDclDataParticleKey.RECORDS_TO_FOLLOW], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.CELL_NUMBER_DIAGNOSTICS,
                                                      self.raw_data[VelptAbDclDataParticleKey.CELL_NUMBER_DIAGNOSTICS],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM1],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM2],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM3],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM4,
                                                      self.raw_data[VelptAbDclDataParticleKey.NOISE_AMPLITUDE_BEAM4],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM1,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM1],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM2,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM2],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM3,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM3],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM4,
                                                      self.raw_data[
                                                          VelptAbDclDataParticleKey.PROCESSING_MAGNITUDE_BEAM4],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DISTANCE_BEAM4,
                                                      self.raw_data[VelptAbDclDataParticleKey.DISTANCE_BEAM4], int))

        return particle_parameters


class VelptAbDclDiagnosticsDataParticleRecovered(VelptAbDclDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptAbDclDataParticleType.VELPT_AB_DCL_DIAGNOSTICS_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDclDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDclDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV,
                                                      self.raw_data[VelptAbDclDataParticleKey.BATTERY_VOLTAGE_DV], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.SOUND_SPEED_DMS,
                                                      self.raw_data[VelptAbDclDataParticleKey.SOUND_SPEED_DMS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.HEADING_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.HEADING_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PITCH_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.PITCH_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.ROLL_DECIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.ROLL_DECIDEGREE], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.PRESSURE_MBAR,
                                                      self.raw_data[VelptAbDclDataParticleKey.PRESSURE_MBAR], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDclDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE,
                                                      self.raw_data[VelptAbDclDataParticleKey.TEMPERATURE_CENTIDEGREE],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.VELOCITY_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDclDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDclDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters