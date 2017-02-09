#!/usr/bin/env python

"""
@package mi.dataset.parser.adcp_pd0
@file marine-integrations/mi/dataset/parser/adcp_pd0.py
@author Jeff Roy
@brief Parser for the adcps_jln and moas_gl_adcpa dataset drivers
Release notes:

initial release
"""
import datetime as dt
import struct

from mi.core.common import BaseEnum
from mi.core.exceptions import RecoverableSampleException
from mi.core.exceptions import UnexpectedDataException
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logger
from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys
from mi.dataset.parser.pd0_parser import AdcpPd0Record, \
    PD0ParsingException, BadHeaderException, \
    BadOffsetException, ChecksumException, UnhandledBlockException

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'


log = get_logger()
ADCPS_PD0_HEADER_REGEX = b'\x7f\x7f'  # header bytes in PD0 files flagged by 7F7F


class AdcpPd0ParsedKey(BaseEnum):
    """
    Data particles for the Teledyne ADCPs Workhorse PD0 formatted data files
    """
    FIRMWARE_VERSION = 'firmware_version'
    FIRMWARE_REVISION = 'firmware_revision'
    SYSCONFIG_FREQUENCY = 'sysconfig_frequency'
    SYSCONFIG_BEAM_PATTERN = 'sysconfig_beam_pattern'
    SYSCONFIG_SENSOR_CONFIG = 'sysconfig_sensor_config'
    SYSCONFIG_HEAD_ATTACHED = 'sysconfig_head_attached'
    SYSCONFIG_VERTICAL_ORIENTATION = 'sysconfig_vertical_orientation'
    SYSCONFIG_BEAM_ANGLE = 'sysconfig_beam_angle'
    SYSCONFIG_BEAM_CONFIG = 'sysconfig_beam_config'
    DATA_FLAG = 'data_flag'
    LAG_LENGTH = 'lag_length'
    NUM_BEAMS = 'num_beams'
    NUM_CELLS = 'num_cells'
    PINGS_PER_ENSEMBLE = 'pings_per_ensemble'
    DEPTH_CELL_LENGTH = 'cell_length'
    BLANK_AFTER_TRANSMIT = 'blank_after_transmit'
    SIGNAL_PROCESSING_MODE = 'signal_processing_mode'
    LOW_CORR_THRESHOLD = 'low_corr_threshold'
    NUM_CODE_REPETITIONS = 'num_code_repetitions'
    PERCENT_GOOD_MIN = 'percent_good_min'
    ERROR_VEL_THRESHOLD = 'error_vel_threshold'
    TIME_PER_PING_MINUTES = 'time_per_ping_minutes'
    TIME_PER_PING_SECONDS = 'time_per_ping_seconds'
    TIME_PER_PING_HUNDREDTHS = 'time_per_ping_hundredths'
    COORD_TRANSFORM_TYPE = 'coord_transform_type'
    COORD_TRANSFORM_TILTS = 'coord_transform_tilts'
    COORD_TRANSFORM_BEAMS = 'coord_transform_beams'
    COORD_TRANSFORM_MAPPING = 'coord_transform_mapping'
    HEADING_ALIGNMENT = 'heading_alignment'
    HEADING_BIAS = 'heading_bias'
    SENSOR_SOURCE_SPEED = 'sensor_source_speed'
    SENSOR_SOURCE_DEPTH = 'sensor_source_depth'
    SENSOR_SOURCE_HEADING = 'sensor_source_heading'
    SENSOR_SOURCE_PITCH = 'sensor_source_pitch'
    SENSOR_SOURCE_ROLL = 'sensor_source_roll'
    SENSOR_SOURCE_CONDUCTIVITY = 'sensor_source_conductivity'
    SENSOR_SOURCE_TEMPERATURE = 'sensor_source_temperature'
    SENSOR_SOURCE_TEMPERATURE_EU = 'sensor_source_temperature_eu'  # ADCPA Only
    SENSOR_AVAILABLE_SPEED = 'sensor_available_speed'
    SENSOR_AVAILABLE_DEPTH = 'sensor_available_depth'
    SENSOR_AVAILABLE_HEADING = 'sensor_available_heading'
    SENSOR_AVAILABLE_PITCH = 'sensor_available_pitch'
    SENSOR_AVAILABLE_ROLL = 'sensor_available_roll'
    SENSOR_AVAILABLE_CONDUCTIVITY = 'sensor_available_conductivity'
    SENSOR_AVAILABLE_TEMPERATURE = 'sensor_available_temperature'
    SENSOR_AVAILABLE_TEMPERATURE_EU = 'sensor_available_temperature_eu'  # ADCPA Only
    BIN_1_DISTANCE = 'bin_1_distance'
    TRANSMIT_PULSE_LENGTH = 'transmit_pulse_length'
    REFERENCE_LAYER_START = 'reference_layer_start'
    REFERENCE_LAYER_STOP = 'reference_layer_stop'
    FALSE_TARGET_THRESHOLD = 'false_target_threshold'
    LOW_LATENCY_TRIGGER = 'low_latency_trigger'
    TRANSMIT_LAG_DISTANCE = 'transmit_lag_distance'
    CPU_SERIAL_NUM = 'cpu_board_serial_number'  # ADCPS Only
    SYSTEM_BANDWIDTH = 'system_bandwidth'  # ADCPS & ADCPA (glider) Only
    SYSTEM_POWER = 'system_power'  # ADCPS Only
    SERIAL_NUMBER = 'serial_number'
    BEAM_ANGLE = 'beam_angle'  # ADCPS & ADCPA AUV Only

    # Variable Leader Data
    ENSEMBLE_NUMBER = 'ensemble_number'
    SPEED_OF_SOUND = 'speed_of_sound'
    TRANSDUCER_DEPTH = 'transducer_depth'
    HEADING = 'heading'
    PITCH = 'pitch'
    ROLL = 'roll'
    SALINITY = 'salinity'
    TEMPERATURE = 'temperature'
    MPT_MINUTES = 'mpt_minutes'
    MPT_SECONDS = 'mpt_seconds'
    MPT_HUNDREDTHS = 'mpt_hundredths'
    HEADING_STDEV = 'heading_stdev'
    PITCH_STDEV = 'pitch_stdev'
    ROLL_STDEV = 'roll_stdev'
    ADC_TRANSMIT_CURRENT = 'adc_transmit_current'  # ADCPS & ADCPA AUV Only
    ADC_TRANSMIT_VOLTAGE = 'adc_transmit_voltage'  # ADCPS & ADCPA AUV Only
    ADC_AMBIENT_TEMP = 'adc_ambient_temp'  # ADCPS & ADCPA AUV Only
    ADC_PRESSURE_PLUS = 'adc_pressure_plus'  # ADCPS & ADCPA AUV Only
    ADC_PRESSURE_MINUS = 'adc_pressure_minus'  # ADCPS & ADCPA AUV Only
    ADC_ATTITUDE_TEMP = 'adc_attitude_temp'  # ADCPS & ADCPA AUV Only
    ADC_ATTITUDE = 'adc_attitude'  # ADCPS & ADCPA AUV Only
    ADC_CONTAMINATION_SENSOR = 'adc_contamination_sensor'  # ADCPS & ADCPA AUV Only
    BIT_RESULT = 'bit_result'
    ERROR_STATUS_WORD = 'error_status_word'
    PRESSURE = 'pressure'  # ADCPS and ADCPA (glider) Only
    PRESSURE_VARIANCE = 'pressure_variance'  # ADCPS and ADCPA (glider) Only

    # Velocity Data
    WATER_VELOCITY_EAST = 'water_velocity_east'  # ADCPS and ADCPA (glider) Only
    WATER_VELOCITY_NORTH = 'water_velocity_north'  # ADCPS and ADCPA (glider) Only
    WATER_VELOCITY_UP = 'water_velocity_up'  # ADCPS and ADCPA (glider) Only
    WATER_VELOCITY_FORWARD = 'water_velocity_forward'  # ADCPA AUV Only
    WATER_VELOCITY_STARBOARD = 'water_velocity_starboard'  # ADCPA AUV Only
    WATER_VELOCITY_VERTICAL = 'water_velocity_vertical'  # ADCPA AUV Only
    ERROR_VELOCITY = 'error_velocity'

    # Correlation Magnitude Data
    CORRELATION_MAGNITUDE_BEAM1 = 'correlation_magnitude_beam1'
    CORRELATION_MAGNITUDE_BEAM2 = 'correlation_magnitude_beam2'
    CORRELATION_MAGNITUDE_BEAM3 = 'correlation_magnitude_beam3'
    CORRELATION_MAGNITUDE_BEAM4 = 'correlation_magnitude_beam4'

    # Echo Intensity Data
    ECHO_INTENSITY_BEAM1 = 'echo_intensity_beam1'
    ECHO_INTENSITY_BEAM2 = 'echo_intensity_beam2'
    ECHO_INTENSITY_BEAM3 = 'echo_intensity_beam3'
    ECHO_INTENSITY_BEAM4 = 'echo_intensity_beam4'

    # Percent Good Data
    PERCENT_GOOD_3BEAM = 'percent_good_3beam'
    PERCENT_TRANSFORMS_REJECT = 'percent_transforms_reject'
    PERCENT_BAD_BEAMS = 'percent_bad_beams'
    PERCENT_GOOD_4BEAM = 'percent_good_4beam'

    # Bottom Track Data (only produced for ADCPA
    # when the glider is in less than 65 m of water)
    BT_PINGS_PER_ENSEMBLE = 'bt_pings_per_ensemble'
    BT_DELAY_BEFORE_REACQUIRE = 'bt_delay_before_reacquire'
    BT_CORR_MAGNITUDE_MIN = 'bt_corr_magnitude_min'
    BT_EVAL_MAGNITUDE_MIN = 'bt_eval_magnitude_min'
    BT_PERCENT_GOOD_MIN = 'bt_percent_good_min'
    BT_MODE = 'bt_mode'
    BT_ERROR_VELOCITY_MAX = 'bt_error_velocity_max'

    BT_BEAM1_RANGE = 'bt_beam1_range'
    BT_BEAM2_RANGE = 'bt_beam2_range'
    BT_BEAM3_RANGE = 'bt_beam3_range'
    BT_BEAM4_RANGE = 'bt_beam4_range'

    BT_EASTWARD_VELOCITY = 'bt_eastward_velocity'  # ADCPS and ADCPA (glider) Only
    BT_NORTHWARD_VELOCITY = 'bt_northward_velocity'  # ADCPS and ADCPA (glider) Only
    BT_UPWARD_VELOCITY = 'bt_upward_velocity'  # ADCPS and ADCPA (glider) Only
    BT_FORWARD_VELOCITY = 'bt_forward_velocity'  # ADCPA AUV Only
    BT_STARBOARD_VELOCITY = 'bt_starboard_velocity'  # ADCPA AUV Only
    BT_VERTICAL_VELOCITY = 'bt_vertical_velocity'  # ADCPA AUV Only
    BT_ERROR_VELOCITY = 'bt_error_velocity'
    BT_BEAM1_CORRELATION = 'bt_beam1_correlation'
    BT_BEAM2_CORRELATION = 'bt_beam2_correlation'
    BT_BEAM3_CORRELATION = 'bt_beam3_correlation'
    BT_BEAM4_CORRELATION = 'bt_beam4_correlation'
    BT_BEAM1_EVAL_AMP = 'bt_beam1_eval_amp'
    BT_BEAM2_EVAL_AMP = 'bt_beam2_eval_amp'
    BT_BEAM3_EVAL_AMP = 'bt_beam3_eval_amp'
    BT_BEAM4_EVAL_AMP = 'bt_beam4_eval_amp'
    BT_BEAM1_PERCENT_GOOD = 'bt_beam1_percent_good'
    BT_BEAM2_PERCENT_GOOD = 'bt_beam2_percent_good'
    BT_BEAM3_PERCENT_GOOD = 'bt_beam3_percent_good'
    BT_BEAM4_PERCENT_GOOD = 'bt_beam4_percent_good'
    BT_REF_LAYER_MIN = 'bt_ref_layer_min'
    BT_REF_LAYER_NEAR = 'bt_ref_layer_near'
    BT_REF_LAYER_FAR = 'bt_ref_layer_far'
    BT_EASTWARD_REF_LAYER_VELOCITY = 'bt_eastward_ref_layer_velocity'  # ADCPS and ADCPA (glider) Only
    BT_NORTHWARD_REF_LAYER_VELOCITY = 'bt_northward_ref_layer_velocity'  # ADCPS and ADCPA (glider) Only
    BT_UPWARD_REF_LAYER_VELOCITY = 'bt_upward_ref_layer_velocity'  # ADCPS and ADCPA (glider) Only
    BT_FORWARD_REF_LAYER_VELOCITY = 'bt_forward_ref_layer_velocity'  # ADCPA AUV Only
    BT_STARBOARD_REF_LAYER_VELOCITY = 'bt_starboard_ref_layer_velocity'  # ADCPA AUV Only
    BT_VERTICAL_REF_LAYER_VELOCITY = 'bt_vertical_ref_layer_velocity'  # ADCPA AUV Only
    BT_ERROR_REF_LAYER_VELOCITY = 'bt_error_ref_layer_velocity'
    BT_BEAM1_REF_CORRELATION = 'bt_beam1_ref_correlation'
    BT_BEAM2_REF_CORRELATION = 'bt_beam2_ref_correlation'
    BT_BEAM3_REF_CORRELATION = 'bt_beam3_ref_correlation'
    BT_BEAM4_REF_CORRELATION = 'bt_beam4_ref_correlation'
    BT_BEAM1_REF_INTENSITY = 'bt_beam1_ref_intensity'
    BT_BEAM2_REF_INTENSITY = 'bt_beam2_ref_intensity'
    BT_BEAM3_REF_INTENSITY = 'bt_beam3_ref_intensity'
    BT_BEAM4_REF_INTENSITY = 'bt_beam4_ref_intensity'
    BT_BEAM1_REF_PERCENT_GOOD = 'bt_beam1_ref_percent_good'
    BT_BEAM2_REF_PERCENT_GOOD = 'bt_beam2_ref_percent_good'
    BT_BEAM3_REF_PERCENT_GOOD = 'bt_beam3_ref_percent_good'
    BT_BEAM4_REF_PERCENT_GOOD = 'bt_beam4_ref_percent_good'
    BT_MAX_DEPTH = 'bt_max_depth'
    BT_BEAM1_RSSI_AMPLITUDE = 'bt_beam1_rssi_amplitude'
    BT_BEAM2_RSSI_AMPLITUDE = 'bt_beam2_rssi_amplitude'
    BT_BEAM3_RSSI_AMPLITUDE = 'bt_beam3_rssi_amplitude'
    BT_BEAM4_RSSI_AMPLITUDE = 'bt_beam4_rssi_amplitude'
    BT_GAIN = 'bt_gain'


class AdcpDataParticleType(BaseEnum):
    """
    Stream types of data particles
    """
    VELOCITY_EARTH = 'adcp_velocity_earth'
    VELOCITY_INST = 'adcp_velocity_inst'
    VELOCITY_GLIDER = 'adcp_velocity_glider'
    PD0_ENGINEERING = 'adcp_engineering'
    PD0_CONFIG = 'adcp_config'
    PD0_ERROR_STATUS = 'adcp_error_status'
    BOTTOM_TRACK_EARTH = 'adcp_bottom_track_earth'
    BOTTOM_TRACK_INST = 'adcp_bottom_track_inst'
    BOTTOM_TRACK_CONFIG = 'adcp_bottom_track_config'


class Pd0Base(DataParticle):
    ntp_epoch = dt.datetime(1900, 1, 1)

    def __init__(self, *args, **kwargs):
        if 'preferred_timestamp' not in kwargs:
            kwargs['preferred_timestamp'] = DataParticleKey.INTERNAL_TIMESTAMP
        super(Pd0Base, self).__init__(*args, **kwargs)
        record = self.raw_data
        dts = dt.datetime(2000 + record.variable_data.rtc_year,
                          record.variable_data.rtc_month,
                          record.variable_data.rtc_day,
                          record.variable_data.rtc_hour,
                          record.variable_data.rtc_minute,
                          record.variable_data.rtc_second)

        rtc_time = (dts - self.ntp_epoch).total_seconds() + record.variable_data.rtc_hundredths / 100.0
        self.set_internal_timestamp(rtc_time)


class VelocityBase(Pd0Base):
    def _build_base_values(self):
        """
        Build the BASE values for all ADCP VELOCITY particles
        """
        record = self.raw_data
        ensemble_number = (record.variable_data.ensemble_roll_over << 16) + record.variable_data.ensemble_number

        return [
            # FIXED LEADER
            (AdcpPd0ParsedKey.NUM_CELLS, record.fixed_data.number_of_cells),
            (AdcpPd0ParsedKey.DEPTH_CELL_LENGTH, record.fixed_data.depth_cell_length),
            (AdcpPd0ParsedKey.BIN_1_DISTANCE, record.fixed_data.bin_1_distance),
            # VARIABLE LEADER
            (AdcpPd0ParsedKey.ENSEMBLE_NUMBER, ensemble_number),
            (AdcpPd0ParsedKey.HEADING, record.variable_data.heading),
            (AdcpPd0ParsedKey.PITCH, record.variable_data.pitch),
            (AdcpPd0ParsedKey.ROLL, record.variable_data.roll),
            (AdcpPd0ParsedKey.SALINITY, record.variable_data.salinity),
            (AdcpPd0ParsedKey.TEMPERATURE, record.variable_data.temperature),
            (AdcpPd0ParsedKey.TRANSDUCER_DEPTH, record.variable_data.depth_of_transducer),
            # SYSCONFIG BITMAP
            (AdcpPd0ParsedKey.SYSCONFIG_VERTICAL_ORIENTATION, record.sysconfig.beam_facing),
            # CORRELATION MAGNITUDES
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM1, record.correlation_magnitudes.beam1),
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM2, record.correlation_magnitudes.beam2),
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM3, record.correlation_magnitudes.beam3),
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM4, record.correlation_magnitudes.beam4),
            # ECHO INTENSITIES
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM1, record.echo_intensity.beam1),
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM2, record.echo_intensity.beam2),
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM3, record.echo_intensity.beam3),
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM4, record.echo_intensity.beam4),
        ]


class VelocityEarth(VelocityBase):
    _data_particle_type = AdcpDataParticleType.VELOCITY_EARTH

    def _build_parsed_values(self):
        """
        Add the fields specific to EARTH coordinate values
        """
        record = self.raw_data
        fields = self._build_base_values()

        fields.extend([
            # EARTH VELOCITIES
            (AdcpPd0ParsedKey.WATER_VELOCITY_EAST, record.velocities.beam1),
            (AdcpPd0ParsedKey.WATER_VELOCITY_NORTH, record.velocities.beam2),
            (AdcpPd0ParsedKey.WATER_VELOCITY_UP, record.velocities.beam3),
            (AdcpPd0ParsedKey.ERROR_VELOCITY, record.velocities.beam4),
            (AdcpPd0ParsedKey.PERCENT_GOOD_3BEAM, record.percent_good.beam1),
            (AdcpPd0ParsedKey.PERCENT_TRANSFORMS_REJECT, record.percent_good.beam2),
            (AdcpPd0ParsedKey.PERCENT_BAD_BEAMS, record.percent_good.beam3),
            (AdcpPd0ParsedKey.PERCENT_GOOD_4BEAM, record.percent_good.beam4),
            (AdcpPd0ParsedKey.PRESSURE, record.variable_data.pressure),
        ])

        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class VelocityGlider(VelocityEarth):
    _data_particle_type = AdcpDataParticleType.VELOCITY_GLIDER


class VelocityInst(VelocityBase):
    _data_particle_type = AdcpDataParticleType.VELOCITY_INST

    def _build_parsed_values(self):
        """
        Add the fields specific to Instrument coordinate values
        """
        record = self.raw_data
        fields = self._build_base_values()

        fields.extend([
            # INSTRUMENT VELOCITIES
            (AdcpPd0ParsedKey.WATER_VELOCITY_FORWARD, record.velocities.beam1),
            (AdcpPd0ParsedKey.WATER_VELOCITY_STARBOARD, record.velocities.beam2),
            (AdcpPd0ParsedKey.WATER_VELOCITY_VERTICAL, record.velocities.beam3),
            (AdcpPd0ParsedKey.ERROR_VELOCITY, record.velocities.beam4),
            (AdcpPd0ParsedKey.PERCENT_GOOD_3BEAM, record.percent_good.beam1),
            (AdcpPd0ParsedKey.PERCENT_TRANSFORMS_REJECT, record.percent_good.beam2),
            (AdcpPd0ParsedKey.PERCENT_BAD_BEAMS, record.percent_good.beam3),
            (AdcpPd0ParsedKey.PERCENT_GOOD_4BEAM, record.percent_good.beam4)])

        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class EngineeringBase(Pd0Base):
    """
    ADCP PD0 data particle
    @throw SampleException if when break happens
    """
    _data_particle_type = AdcpDataParticleType.PD0_ENGINEERING

    def _build_base_fields(self):
        """
        Parse the base portion of the particle
        """
        record = self.raw_data

        fields = [
            # FIXED LEADER
            (AdcpPd0ParsedKey.TRANSMIT_PULSE_LENGTH, record.fixed_data.transmit_pulse_length),
            # VARIABLE LEADER
            (AdcpPd0ParsedKey.SPEED_OF_SOUND, record.variable_data.speed_of_sound),
            (AdcpPd0ParsedKey.MPT_MINUTES, record.variable_data.mpt_minutes),
            (AdcpPd0ParsedKey.MPT_SECONDS, record.variable_data.mpt_seconds),
            (AdcpPd0ParsedKey.MPT_HUNDREDTHS, record.variable_data.mpt_hundredths),
            (AdcpPd0ParsedKey.HEADING_STDEV, record.variable_data.heading_standard_deviation),
            (AdcpPd0ParsedKey.PITCH_STDEV, record.variable_data.pitch_standard_deviation),
            (AdcpPd0ParsedKey.ROLL_STDEV, record.variable_data.roll_standard_deviation),
            (AdcpPd0ParsedKey.ADC_TRANSMIT_VOLTAGE, record.variable_data.transmit_voltage),
            (AdcpPd0ParsedKey.BIT_RESULT, record.variable_data.bit_result),
        ]

        return fields


class GliderEngineering(EngineeringBase):
    """
    ADCP PD0 data particle
    @throw SampleException if when break happens
    """
    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_base_fields()
        fields.extend([
            (AdcpPd0ParsedKey.PRESSURE_VARIANCE, record.variable_data.pressure_variance)
        ])
        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class AuvEngineering(EngineeringBase):
    """
    ADCP PD0 data particle
    @throw SampleException if when break happens
    """
    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_base_fields()
        fields.extend([
            (AdcpPd0ParsedKey.ADC_TRANSMIT_CURRENT, record.variable_data.transmit_current),
            (AdcpPd0ParsedKey.ADC_AMBIENT_TEMP, record.variable_data.ambient_temperature),
            (AdcpPd0ParsedKey.ADC_PRESSURE_PLUS, record.variable_data.pressure_positive),
            (AdcpPd0ParsedKey.ADC_PRESSURE_MINUS, record.variable_data.pressure_negative),
            (AdcpPd0ParsedKey.ADC_ATTITUDE_TEMP, record.variable_data.attitude_temperature),
            (AdcpPd0ParsedKey.ADC_ATTITUDE, record.variable_data.attitude),
            (AdcpPd0ParsedKey.ADC_CONTAMINATION_SENSOR, record.variable_data.contamination_sensor),
            (AdcpPd0ParsedKey.ERROR_STATUS_WORD, record.variable_data.error_status_word),
        ])
        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class AdcpsEngineering(EngineeringBase):
    """
    ADCP PD0 data particle
    @throw SampleException if when break happens
    """
    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_base_fields()
        fields.extend([
            (AdcpPd0ParsedKey.PRESSURE_VARIANCE, record.variable_data.pressure_variance),
            (AdcpPd0ParsedKey.ADC_TRANSMIT_CURRENT, record.variable_data.transmit_current),
            (AdcpPd0ParsedKey.ADC_AMBIENT_TEMP, record.variable_data.ambient_temperature),
            (AdcpPd0ParsedKey.ADC_PRESSURE_PLUS, record.variable_data.pressure_positive),
            (AdcpPd0ParsedKey.ADC_PRESSURE_MINUS, record.variable_data.pressure_negative),
            (AdcpPd0ParsedKey.ADC_ATTITUDE_TEMP, record.variable_data.attitude_temperature),
            (AdcpPd0ParsedKey.ADC_ATTITUDE, record.variable_data.attitude),
            (AdcpPd0ParsedKey.ADC_CONTAMINATION_SENSOR, record.variable_data.contamination_sensor),
            (AdcpPd0ParsedKey.ERROR_STATUS_WORD, record.variable_data.error_status_word),
        ])
        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class BaseConfig(Pd0Base):
    """
    ADCP PD0 data particle
    @throw SampleException if when break happens
    """
    _data_particle_type = AdcpDataParticleType.PD0_CONFIG

    def _build_base_fields(self):
        """
        Parse the base portion of the particle
        """
        record = self.raw_data

        fields = [
            # FIXED LEADER
            (AdcpPd0ParsedKey.FIRMWARE_VERSION, record.fixed_data.cpu_firmware_version),
            (AdcpPd0ParsedKey.FIRMWARE_REVISION, record.fixed_data.cpu_firmware_revision),
            (AdcpPd0ParsedKey.DATA_FLAG, record.fixed_data.simulation_data_flag),
            (AdcpPd0ParsedKey.LAG_LENGTH, record.fixed_data.lag_length),
            (AdcpPd0ParsedKey.NUM_BEAMS, record.fixed_data.number_of_beams),
            (AdcpPd0ParsedKey.NUM_CELLS, record.fixed_data.number_of_cells),
            (AdcpPd0ParsedKey.PINGS_PER_ENSEMBLE, record.fixed_data.pings_per_ensemble),
            (AdcpPd0ParsedKey.DEPTH_CELL_LENGTH, record.fixed_data.depth_cell_length),
            (AdcpPd0ParsedKey.BLANK_AFTER_TRANSMIT, record.fixed_data.blank_after_transmit),
            (AdcpPd0ParsedKey.SIGNAL_PROCESSING_MODE, record.fixed_data.signal_processing_mode),
            (AdcpPd0ParsedKey.LOW_CORR_THRESHOLD, record.fixed_data.low_corr_threshold),
            (AdcpPd0ParsedKey.NUM_CODE_REPETITIONS, record.fixed_data.num_code_reps),
            (AdcpPd0ParsedKey.PERCENT_GOOD_MIN, record.fixed_data.minimum_percentage),
            (AdcpPd0ParsedKey.ERROR_VEL_THRESHOLD, record.fixed_data.error_velocity_max),
            (AdcpPd0ParsedKey.TIME_PER_PING_MINUTES, record.fixed_data.tpp_minutes),
            (AdcpPd0ParsedKey.TIME_PER_PING_SECONDS, record.fixed_data.tpp_seconds),
            (AdcpPd0ParsedKey.TIME_PER_PING_HUNDREDTHS, record.fixed_data.tpp_hundredths),
            (AdcpPd0ParsedKey.HEADING_ALIGNMENT, record.fixed_data.heading_alignment),
            (AdcpPd0ParsedKey.HEADING_BIAS, record.fixed_data.heading_bias),
            (AdcpPd0ParsedKey.REFERENCE_LAYER_START, record.fixed_data.starting_depth_cell),
            (AdcpPd0ParsedKey.REFERENCE_LAYER_STOP, record.fixed_data.ending_depth_cell),
            (AdcpPd0ParsedKey.FALSE_TARGET_THRESHOLD, record.fixed_data.false_target_threshold),
            (AdcpPd0ParsedKey.TRANSMIT_LAG_DISTANCE, record.fixed_data.transmit_lag_distance),
            (AdcpPd0ParsedKey.SERIAL_NUMBER, str(record.fixed_data.serial_number)),
            # SYSCONFIG BITMAP
            (AdcpPd0ParsedKey.SYSCONFIG_FREQUENCY, record.sysconfig.frequency),
            (AdcpPd0ParsedKey.SYSCONFIG_BEAM_PATTERN, record.sysconfig.beam_pattern),
            (AdcpPd0ParsedKey.SYSCONFIG_SENSOR_CONFIG, record.sysconfig.sensor_config),
            (AdcpPd0ParsedKey.SYSCONFIG_HEAD_ATTACHED, record.sysconfig.xdcr_head_attached),
            (AdcpPd0ParsedKey.SYSCONFIG_VERTICAL_ORIENTATION, record.sysconfig.beam_facing),
            (AdcpPd0ParsedKey.SYSCONFIG_BEAM_ANGLE, record.sysconfig.beam_angle),
            (AdcpPd0ParsedKey.SYSCONFIG_BEAM_CONFIG, record.sysconfig.janus_config),
            # COORD TRANSFORM BITMAP
            (AdcpPd0ParsedKey.COORD_TRANSFORM_TYPE, record.coord_transform.coord_transform),
            (AdcpPd0ParsedKey.COORD_TRANSFORM_TILTS, record.coord_transform.tilts_used),
            (AdcpPd0ParsedKey.COORD_TRANSFORM_BEAMS, record.coord_transform.three_beam_used),
            (AdcpPd0ParsedKey.COORD_TRANSFORM_MAPPING, record.coord_transform.bin_mapping_used),
            # SENSOR SOURCE BITMAP
            (AdcpPd0ParsedKey.SENSOR_SOURCE_SPEED, record.sensor_source.calculate_ec),
            (AdcpPd0ParsedKey.SENSOR_SOURCE_DEPTH, record.sensor_source.depth_used),
            (AdcpPd0ParsedKey.SENSOR_SOURCE_HEADING, record.sensor_source.heading_used),
            (AdcpPd0ParsedKey.SENSOR_SOURCE_PITCH, record.sensor_source.pitch_used),
            (AdcpPd0ParsedKey.SENSOR_SOURCE_ROLL, record.sensor_source.roll_used),
            (AdcpPd0ParsedKey.SENSOR_SOURCE_CONDUCTIVITY, record.sensor_source.conductivity_used),
            (AdcpPd0ParsedKey.SENSOR_SOURCE_TEMPERATURE, record.sensor_source.temperature_used),
            # SENSOR AVAIL BITMAP
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_SPEED, record.sensor_avail.speed_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_DEPTH, record.sensor_avail.depth_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_HEADING, record.sensor_avail.heading_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_PITCH, record.sensor_avail.pitch_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_ROLL, record.sensor_avail.roll_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_CONDUCTIVITY, record.sensor_avail.conductivity_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_TEMPERATURE, record.sensor_avail.temperature_avail),
            ]

        return fields


class GliderConfig(BaseConfig):
    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_base_fields()
        fields.extend([
            (AdcpPd0ParsedKey.SYSTEM_BANDWIDTH, record.fixed_data.system_bandwidth),
            (AdcpPd0ParsedKey.SENSOR_SOURCE_TEMPERATURE_EU, record.sensor_source.temperature_eu_used),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_TEMPERATURE_EU, record.sensor_avail.temperature_eu_avail),
        ])
        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class AdcpsConfig(BaseConfig):
    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_base_fields()
        fields.extend([
            (AdcpPd0ParsedKey.LOW_LATENCY_TRIGGER, record.fixed_data.spare1),
            (AdcpPd0ParsedKey.CPU_SERIAL_NUM, str(record.fixed_data.cpu_board_serial_number)),
            (AdcpPd0ParsedKey.SYSTEM_BANDWIDTH, record.fixed_data.system_bandwidth),
            (AdcpPd0ParsedKey.SYSTEM_POWER, record.fixed_data.system_power),
            (AdcpPd0ParsedKey.BEAM_ANGLE, record.fixed_data.beam_angle),
        ])
        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class AuvConfig(BaseConfig):
    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_base_fields()
        fields.extend([
            (AdcpPd0ParsedKey.LOW_LATENCY_TRIGGER, record.fixed_data.spare1),
            (AdcpPd0ParsedKey.BEAM_ANGLE, record.fixed_data.beam_angle),
        ])
        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class BaseBottom(Pd0Base):
    def _build_fields(self):
        record = self.raw_data

        # need to combine LSBs and MSBs of ranges
        beam1_bt_range = record.bottom_track.range_1 + (record.bottom_track.range_msb_1 << 16)
        beam2_bt_range = record.bottom_track.range_2 + (record.bottom_track.range_msb_2 << 16)
        beam3_bt_range = record.bottom_track.range_3 + (record.bottom_track.range_msb_3 << 16)
        beam4_bt_range = record.bottom_track.range_4 + (record.bottom_track.range_msb_4 << 16)

        fields = [
            (AdcpPd0ParsedKey.BT_BEAM1_RANGE, beam1_bt_range),
            (AdcpPd0ParsedKey.BT_BEAM2_RANGE, beam2_bt_range),
            (AdcpPd0ParsedKey.BT_BEAM3_RANGE, beam3_bt_range),
            (AdcpPd0ParsedKey.BT_BEAM4_RANGE, beam4_bt_range),
            (AdcpPd0ParsedKey.BT_BEAM1_CORRELATION, record.bottom_track.corr_1),
            (AdcpPd0ParsedKey.BT_BEAM2_CORRELATION, record.bottom_track.corr_2),
            (AdcpPd0ParsedKey.BT_BEAM3_CORRELATION, record.bottom_track.corr_3),
            (AdcpPd0ParsedKey.BT_BEAM4_CORRELATION, record.bottom_track.corr_4),
            (AdcpPd0ParsedKey.BT_BEAM1_EVAL_AMP, record.bottom_track.amp_1),
            (AdcpPd0ParsedKey.BT_BEAM2_EVAL_AMP, record.bottom_track.amp_2),
            (AdcpPd0ParsedKey.BT_BEAM3_EVAL_AMP, record.bottom_track.amp_3),
            (AdcpPd0ParsedKey.BT_BEAM4_EVAL_AMP, record.bottom_track.amp_4),
            (AdcpPd0ParsedKey.BT_BEAM1_PERCENT_GOOD, record.bottom_track.pcnt_1),
            (AdcpPd0ParsedKey.BT_BEAM2_PERCENT_GOOD, record.bottom_track.pcnt_2),
            (AdcpPd0ParsedKey.BT_BEAM3_PERCENT_GOOD, record.bottom_track.pcnt_3),
            (AdcpPd0ParsedKey.BT_BEAM4_PERCENT_GOOD, record.bottom_track.pcnt_4),
            (AdcpPd0ParsedKey.BT_BEAM1_REF_CORRELATION, record.bottom_track.ref_corr_1),
            (AdcpPd0ParsedKey.BT_BEAM2_REF_CORRELATION, record.bottom_track.ref_corr_2),
            (AdcpPd0ParsedKey.BT_BEAM3_REF_CORRELATION, record.bottom_track.ref_corr_3),
            (AdcpPd0ParsedKey.BT_BEAM4_REF_CORRELATION, record.bottom_track.ref_corr_4),
            (AdcpPd0ParsedKey.BT_BEAM1_REF_INTENSITY, record.bottom_track.ref_amp_1),
            (AdcpPd0ParsedKey.BT_BEAM2_REF_INTENSITY, record.bottom_track.ref_amp_2),
            (AdcpPd0ParsedKey.BT_BEAM3_REF_INTENSITY, record.bottom_track.ref_amp_3),
            (AdcpPd0ParsedKey.BT_BEAM4_REF_INTENSITY, record.bottom_track.ref_amp_4),
            (AdcpPd0ParsedKey.BT_BEAM1_REF_PERCENT_GOOD, record.bottom_track.ref_pcnt_1),
            (AdcpPd0ParsedKey.BT_BEAM2_REF_PERCENT_GOOD, record.bottom_track.ref_pcnt_2),
            (AdcpPd0ParsedKey.BT_BEAM3_REF_PERCENT_GOOD, record.bottom_track.ref_pcnt_3),
            (AdcpPd0ParsedKey.BT_BEAM4_REF_PERCENT_GOOD, record.bottom_track.ref_pcnt_4),
            (AdcpPd0ParsedKey.BT_BEAM1_RSSI_AMPLITUDE, record.bottom_track.rssi_1),
            (AdcpPd0ParsedKey.BT_BEAM2_RSSI_AMPLITUDE, record.bottom_track.rssi_2),
            (AdcpPd0ParsedKey.BT_BEAM3_RSSI_AMPLITUDE, record.bottom_track.rssi_3),
            (AdcpPd0ParsedKey.BT_BEAM4_RSSI_AMPLITUDE, record.bottom_track.rssi_4),
            (AdcpPd0ParsedKey.BT_REF_LAYER_MIN, record.bottom_track.ref_layer_min),
            (AdcpPd0ParsedKey.BT_REF_LAYER_NEAR, record.bottom_track.ref_layer_near),
            (AdcpPd0ParsedKey.BT_REF_LAYER_FAR, record.bottom_track.ref_layer_far),
            (AdcpPd0ParsedKey.BT_GAIN, record.bottom_track.gain),
            ]
        return fields


class EarthBottom(BaseBottom):
    _data_particle_type = AdcpDataParticleType.BOTTOM_TRACK_EARTH

    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_fields()
        fields.extend([
            (AdcpPd0ParsedKey.BT_EASTWARD_VELOCITY, record.bottom_track.velocity_1),
            (AdcpPd0ParsedKey.BT_NORTHWARD_VELOCITY, record.bottom_track.velocity_2),
            (AdcpPd0ParsedKey.BT_UPWARD_VELOCITY, record.bottom_track.velocity_3),
            (AdcpPd0ParsedKey.BT_ERROR_VELOCITY, record.bottom_track.velocity_4),

            (AdcpPd0ParsedKey.BT_EASTWARD_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_1),
            (AdcpPd0ParsedKey.BT_NORTHWARD_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_2),
            (AdcpPd0ParsedKey.BT_UPWARD_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_3),
            (AdcpPd0ParsedKey.BT_ERROR_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_4),
        ])

        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class InstBottom(BaseBottom):
    _data_particle_type = AdcpDataParticleType.BOTTOM_TRACK_INST

    def _build_parsed_values(self):
        record = self.raw_data
        fields = self._build_fields()
        fields.extend([
            (AdcpPd0ParsedKey.BT_FORWARD_VELOCITY, record.bottom_track.velocity_1),
            (AdcpPd0ParsedKey.BT_STARBOARD_VELOCITY, record.bottom_track.velocity_2),
            (AdcpPd0ParsedKey.BT_VERTICAL_VELOCITY, record.bottom_track.velocity_3),
            (AdcpPd0ParsedKey.BT_ERROR_VELOCITY, record.bottom_track.velocity_4),

            (AdcpPd0ParsedKey.BT_FORWARD_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_1),
            (AdcpPd0ParsedKey.BT_STARBOARD_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_2),
            (AdcpPd0ParsedKey.BT_VERTICAL_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_3),
            (AdcpPd0ParsedKey.BT_ERROR_REF_LAYER_VELOCITY, record.bottom_track.ref_velocity_4)
        ])

        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class BottomConfig(Pd0Base):
    _data_particle_type = AdcpDataParticleType.BOTTOM_TRACK_CONFIG

    def _build_parsed_values(self):
        record = self.raw_data

        fields = [
            (AdcpPd0ParsedKey.BT_PINGS_PER_ENSEMBLE, record.bottom_track.pings_per_ensemble),
            (AdcpPd0ParsedKey.BT_DELAY_BEFORE_REACQUIRE, record.bottom_track.delay_before_reacquire),
            (AdcpPd0ParsedKey.BT_CORR_MAGNITUDE_MIN, record.bottom_track.correlation_mag_min),
            (AdcpPd0ParsedKey.BT_EVAL_MAGNITUDE_MIN, record.bottom_track.eval_amplitude_min),
            (AdcpPd0ParsedKey.BT_PERCENT_GOOD_MIN, record.bottom_track.percent_good_minimum),
            (AdcpPd0ParsedKey.BT_MODE, record.bottom_track.mode),
            (AdcpPd0ParsedKey.BT_ERROR_VELOCITY_MAX, record.bottom_track.error_velocity_max),
            (AdcpPd0ParsedKey.BT_MAX_DEPTH, record.bottom_track.max_depth),
        ]

        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


class AdcpPd0Parser(SimpleParser):
    def __init__(self, *args, **kwargs):
        super(AdcpPd0Parser, self).__init__(*args, **kwargs)
        self._particle_classes = self._config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT]
        self._particle_classes = {k: globals()[v] for k, v in self._particle_classes.iteritems()}
        self._glider = GliderConfig in self._particle_classes.values()
        self._last_values = {}

    def _changed(self, particle):
        particle_dict = particle.generate_dict()
        stream = particle_dict.get('stream_name')
        values = particle_dict.get('values')
        last_values = self._last_values.get(stream)
        if values == last_values:
            return False

        self._last_values[stream] = values
        return True

    def parse_file(self):
        """
        Entry point into parsing the file
        Loop through the file one ensemble at a time
        """

        position = 0  # set position to beginning of file
        header_id_bytes = self._stream_handle.read(2)  # read the first two bytes of the file

        while header_id_bytes:  # will be None when EOF is found

            if header_id_bytes == ADCPS_PD0_HEADER_REGEX:

                # get the ensemble size from the next 2 bytes (excludes checksum bytes)
                num_bytes = struct.unpack("<H", self._stream_handle.read(2))[0]

                self._stream_handle.seek(position)  # reset to beginning of ensemble
                input_buffer = self._stream_handle.read(num_bytes + 2)  # read entire ensemble

                if len(input_buffer) == num_bytes + 2:  # make sure there are enough bytes including checksum

                    try:
                        pd0 = AdcpPd0Record(input_buffer, glider=self._glider)

                        velocity = self._particle_classes['velocity'](pd0)
                        self._record_buffer.append(velocity)

                        config = self._particle_classes['config'](pd0)
                        engineering = self._particle_classes['engineering'](pd0)

                        for particle in [config, engineering]:
                            if self._changed(particle):
                                self._record_buffer.append(particle)

                        if hasattr(pd0, 'bottom_track'):
                            bt = self._particle_classes['bottom_track'](pd0)
                            bt_config = self._particle_classes['bottom_track_config'](pd0)
                            self._record_buffer.append(bt)

                            if self._changed(bt_config):
                                self._record_buffer.append(bt_config)

                    except (BadOffsetException, UnhandledBlockException, BadHeaderException):
                        self._stream_handle.seek(position + 2)

                    except PD0ParsingException:
                        # seek to just past this header match
                        self._stream_handle.seek(position + 2)
                        self._exception_callback(RecoverableSampleException("Exception parsing PD0"))

                else:  # reached EOF
                    log.warn("not enough bytes left for complete ensemble")
                    self._exception_callback(UnexpectedDataException("Found incomplete ensemble at end of file"))

            else:  # did not get header ID bytes
                # if we do not find the header ID bytes go to next bytes and try again
                # we are not logging anything or passing an exception back to reduce
                # log noise.
                pass

            position = self._stream_handle.tell()  # set the new file position
            header_id_bytes = self._stream_handle.read(2)  # read the next two bytes of the file

