"""
@package mi.instrument.teledyne.particles
@file marine-integrations/mi/instrument/teledyne/driver.py
@author SUng Ahn
@brief Driver particle code for the teledyne particles
Release notes:
"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import re
import time as time
import datetime as dt

from mi.core.log import get_logger

log = get_logger()

from mi.core.common import BaseEnum
from mi.instrument.teledyne.driver import NEWLINE
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.instrument.teledyne.pd0_parser import AdcpPd0Record
from mi.core.exceptions import SampleException

BASE_YEAR = 2000

#
# Particle Regex's'
#
ADCP_PD0_PARSED_REGEX = r'\x7f\x7f(..)'  # .*
ADCP_PD0_PARSED_REGEX_MATCHER = re.compile(ADCP_PD0_PARSED_REGEX, re.DOTALL)
ADCP_SYSTEM_CONFIGURATION_REGEX = r'(Instrument S/N.*?)\>'
ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER = re.compile(ADCP_SYSTEM_CONFIGURATION_REGEX, re.DOTALL)
ADCP_COMPASS_CALIBRATION_REGEX = r'(ACTIVE FLUXGATE CALIBRATION MATRICES in NVRAM.*?)\>'
ADCP_COMPASS_CALIBRATION_REGEX_MATCHER = re.compile(ADCP_COMPASS_CALIBRATION_REGEX, re.DOTALL)
ADCP_ANCILLARY_SYSTEM_DATA_REGEX = r'(Ambient  Temperature.*\n.*\n.*)\n>'
ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER = re.compile(ADCP_ANCILLARY_SYSTEM_DATA_REGEX)
ADCP_TRANSMIT_PATH_REGEX = r'(IXMT.*\n.*\n.*\n.*)\n>'
ADCP_TRANSMIT_PATH_REGEX_MATCHER = re.compile(ADCP_TRANSMIT_PATH_REGEX)


# ##############################################################################
# Data Particles
# ##############################################################################
class DataParticleType(BaseEnum):
    """
    Stream types of data particles
    """
    RAW = CommonDataParticleType.RAW
    ADCP_PD0_PARSED_BEAM = 'adcp_pd0_beam_parsed'
    ADCP_PD0_PARSED_EARTH = 'adcp_pd0_earth_parsed'
    ADCP_SYSTEM_CONFIGURATION = 'adcp_system_configuration'
    ADCP_COMPASS_CALIBRATION = 'adcp_compass_calibration'
    ADCP_ANCILLARY_SYSTEM_DATA = "adcp_ancillary_system_data"
    ADCP_TRANSMIT_PATH = "adcp_transmit_path"


class VADCPDataParticleType(DataParticleType):
    """
    VADCP Stream types of data particles
    """
    VADCP_PD0_BEAM_PARSED = 'vadcp_pd0_beam_parsed'
    VADCP_PD0_EARTH_PARSED = 'vadcp_pd0_earth_parsed'

    VADCP_4BEAM_SYSTEM_CONFIGURATION = "vadcp_4beam_system_configuration"
    VADCP_5THBEAM_SYSTEM_CONFIGURATION = "vadcp_5thbeam_system_configuration"

    VADCP_ANCILLARY_SYSTEM_DATA = "vadcp_ancillary_system_data"
    VADCP_TRANSMIT_PATH = "vadcp_transmit_path"

    VADCP_PD0_PARSED_BEAM = 'vadcp_5thbeam_pd0_beam_parsed'
    VADCP_PD0_PARSED_EARTH = 'vadcp_5thbeam_pd0_earth_parsed'
    VADCP_COMPASS_CALIBRATION = 'vadcp_5thbeam_compass_calibration'


class ADCP_PD0_PARSED_KEY(BaseEnum):
    """
    ADCP PD0 parsed keys
    """
    HEADER_ID = "header_id"
    DATA_SOURCE_ID = "data_source_id"
    NUM_BYTES = "num_bytes"
    NUM_DATA_TYPES = "num_data_types"
    OFFSET_DATA_TYPES = "offset_data_types"
    FIXED_LEADER_ID = "fixed_leader_id"
    FIRMWARE_VERSION = "firmware_version"
    FIRMWARE_REVISION = "firmware_revision"
    SYSCONFIG_FREQUENCY = "sysconfig_frequency"
    SYSCONFIG_BEAM_PATTERN = "sysconfig_beam_pattern"
    SYSCONFIG_SENSOR_CONFIG = "sysconfig_sensor_config"
    SYSCONFIG_HEAD_ATTACHED = "sysconfig_head_attached"
    SYSCONFIG_VERTICAL_ORIENTATION = "sysconfig_vertical_orientation"
    DATA_FLAG = "data_flag"
    LAG_LENGTH = "lag_length"
    NUM_BEAMS = "num_beams"
    NUM_CELLS = "num_cells"
    PINGS_PER_ENSEMBLE = "pings_per_ensemble"
    DEPTH_CELL_LENGTH = "cell_length"
    BLANK_AFTER_TRANSMIT = "blank_after_transmit"
    SIGNAL_PROCESSING_MODE = "signal_processing_mode"
    LOW_CORR_THRESHOLD = "low_corr_threshold"
    NUM_CODE_REPETITIONS = "num_code_repetitions"
    PERCENT_GOOD_MIN = "percent_good_min"
    ERROR_VEL_THRESHOLD = "error_vel_threshold"
    TIME_PER_PING_MINUTES = "time_per_ping_minutes"
    TIME_PER_PING_SECONDS = "time_per_ping_seconds"
    COORD_TRANSFORM_TYPE = "coord_transform_type"
    COORD_TRANSFORM_TILTS = "coord_transform_tilts"
    COORD_TRANSFORM_BEAMS = "coord_transform_beams"
    COORD_TRANSFORM_MAPPING = "coord_transform_mapping"
    HEADING_ALIGNMENT = "heading_alignment"
    HEADING_BIAS = "heading_bias"

    SENSOR_SOURCE_SPEED = "sensor_source_speed"
    SENSOR_SOURCE_DEPTH = "sensor_source_depth"
    SENSOR_SOURCE_HEADING = "sensor_source_heading"
    SENSOR_SOURCE_PITCH = "sensor_source_pitch"
    SENSOR_SOURCE_ROLL = "sensor_source_roll"
    SENSOR_SOURCE_CONDUCTIVITY = "sensor_source_conductivity"
    SENSOR_SOURCE_TEMPERATURE = "sensor_source_temperature"
    SENSOR_AVAILABLE_DEPTH = "sensor_available_depth"
    SENSOR_AVAILABLE_HEADING = "sensor_available_heading"
    SENSOR_AVAILABLE_PITCH = "sensor_available_pitch"
    SENSOR_AVAILABLE_ROLL = "sensor_available_roll"
    SENSOR_AVAILABLE_CONDUCTIVITY = "sensor_available_conductivity"
    SENSOR_AVAILABLE_TEMPERATURE = "sensor_available_temperature"

    BIN_1_DISTANCE = "bin_1_distance"
    TRANSMIT_PULSE_LENGTH = "transmit_pulse_length"
    REFERENCE_LAYER_START = "reference_layer_start"
    REFERENCE_LAYER_STOP = "reference_layer_stop"
    FALSE_TARGET_THRESHOLD = "false_target_threshold"
    LOW_LATENCY_TRIGGER = "low_latency_trigger"
    TRANSMIT_LAG_DISTANCE = "transmit_lag_distance"
    CPU_BOARD_SERIAL_NUMBER = "cpu_board_serial_number"
    SYSTEM_BANDWIDTH = "system_bandwidth"
    SYSTEM_POWER = "system_power"
    SERIAL_NUMBER = "serial_number"
    BEAM_ANGLE = "beam_angle"
    VARIABLE_LEADER_ID = "variable_leader_id"
    ENSEMBLE_NUMBER = "ensemble_number"
    REAL_TIME_CLOCK = "real_time_clock"
    ENSEMBLE_START_TIME = "ensemble_start_time"
    ENSEMBLE_NUMBER_INCREMENT = "ensemble_number_increment"
    BIT_RESULT_DEMOD_0 = "bit_result_demod_0"
    BIT_RESULT_DEMOD_1 = "bit_result_demod_1"
    BIT_RESULT_TIMING = "bit_result_timing"
    SPEED_OF_SOUND = "speed_of_sound"
    TRANSDUCER_DEPTH = "transducer_depth"
    HEADING = "heading"
    PITCH = "pitch"
    ROLL = "roll"
    SALINITY = "salinity"
    TEMPERATURE = "temperature"
    MPT_MINUTES = "mpt_minutes"
    MPT_SECONDS = "mpt_seconds"
    HEADING_STDEV = "heading_stdev"
    PITCH_STDEV = "pitch_stdev"
    ROLL_STDEV = "roll_stdev"
    ADC_TRANSMIT_CURRENT = "adc_transmit_current"
    ADC_TRANSMIT_VOLTAGE = "adc_transmit_voltage"
    ADC_AMBIENT_TEMP = "adc_ambient_temp"
    ADC_PRESSURE_PLUS = "adc_pressure_plus"
    ADC_PRESSURE_MINUS = "adc_pressure_minus"
    ADC_ATTITUDE_TEMP = "adc_attitude_temp"
    ADC_ATTITUDE = "adc_attitude"
    ADC_CONTAMINATION_SENSOR = "adc_contamination_sensor"
    BUS_ERROR_EXCEPTION = "bus_error_exception"
    ADDRESS_ERROR_EXCEPTION = "address_error_exception"
    ILLEGAL_INSTRUCTION_EXCEPTION = "illegal_instruction_exception"
    ZERO_DIVIDE_INSTRUCTION = "zero_divide_instruction"
    EMULATOR_EXCEPTION = "emulator_exception"
    UNASSIGNED_EXCEPTION = "unassigned_exception"
    WATCHDOG_RESTART_OCCURRED = "watchdog_restart_occurred"
    BATTERY_SAVER_POWER = "battery_saver_power"
    PINGING = "pinging"
    COLD_WAKEUP_OCCURRED = "cold_wakeup_occurred"
    UNKNOWN_WAKEUP_OCCURRED = "unknown_wakeup_occurred"
    CLOCK_READ_ERROR = "clock_read_error"
    UNEXPECTED_ALARM = "unexpected_alarm"
    CLOCK_JUMP_FORWARD = "clock_jump_forward"
    CLOCK_JUMP_BACKWARD = "clock_jump_backward"
    POWER_FAIL = "power_fail"
    SPURIOUS_DSP_INTERRUPT = "spurious_dsp_interrupt"
    SPURIOUS_UART_INTERRUPT = "spurious_uart_interrupt"
    SPURIOUS_CLOCK_INTERRUPT = "spurious_clock_interrupt"
    LEVEL_7_INTERRUPT = "level_7_interrupt"
    ABSOLUTE_PRESSURE = "pressure"
    PRESSURE_VARIANCE = "pressure_variance"
    VELOCITY_DATA_ID = "velocity_data_id"
    BEAM_1_VELOCITY = "beam_1_velocity"
    BEAM_2_VELOCITY = "beam_2_velocity"
    BEAM_3_VELOCITY = "beam_3_velocity"
    BEAM_4_VELOCITY = "beam_4_velocity"
    WATER_VELOCITY_EAST = "water_velocity_east"
    WATER_VELOCITY_NORTH = "water_velocity_north"
    WATER_VELOCITY_UP = "water_velocity_up"
    ERROR_VELOCITY = "error_velocity"
    CORRELATION_MAGNITUDE_ID = "correlation_magnitude_id"
    CORRELATION_MAGNITUDE_BEAM1 = "correlation_magnitude_beam1"
    CORRELATION_MAGNITUDE_BEAM2 = "correlation_magnitude_beam2"
    CORRELATION_MAGNITUDE_BEAM3 = "correlation_magnitude_beam3"
    CORRELATION_MAGNITUDE_BEAM4 = "correlation_magnitude_beam4"
    ECHO_INTENSITY_ID = "echo_intensity_id"
    ECHO_INTENSITY_BEAM1 = "echo_intensity_beam1"
    ECHO_INTENSITY_BEAM2 = "echo_intensity_beam2"
    ECHO_INTENSITY_BEAM3 = "echo_intensity_beam3"
    ECHO_INTENSITY_BEAM4 = "echo_intensity_beam4"
    PERCENT_GOOD_BEAM1 = "percent_good_beam1"
    PERCENT_GOOD_BEAM2 = "percent_good_beam2"
    PERCENT_GOOD_BEAM3 = "percent_good_beam3"
    PERCENT_GOOD_BEAM4 = "percent_good_beam4"
    PERCENT_GOOD_ID = "percent_good_id"
    PERCENT_GOOD_3BEAM = "percent_good_3beam"
    PERCENT_TRANSFORMS_REJECT = "percent_transforms_reject"
    PERCENT_BAD_BEAMS = "percent_bad_beams"
    PERCENT_GOOD_4BEAM = "percent_good_4beam"
    CHECKSUM = "checksum"


# The data particle type will be overwritten based on coordinate (Earth/Beam)
class ADCP_PD0_PARSED_DataParticle(DataParticle):
    """
    ADCP PD0 data particle
    @throw SampleException if when break happens
    """
    _data_particle_type = 'UNASSIGNED IN mi.instrument.teledyne.workhorse ADCP_PD0_PARSED_DataParticle'
    _slave = False
    _master = False

    def _build_parsed_values(self):
        """
        Parse the base portion of the particle
        """
        if "[BREAK Wakeup A]" in self.raw_data:
            raise SampleException("BREAK found; likely partial sample while escaping autosample mode.")

        record = AdcpPd0Record(self.raw_data)
        record.process()
        record.parse_bitmapped_fields()
        print record

        tpp_float_seconds = float(record.fixed_data.seconds + (record.fixed_data.hundredths / 100))
        dts = dt.datetime(record.variable_data.rtc_y2k_century * 100 + record.variable_data.rtc_y2k_year,
                          record.variable_data.rtc_y2k_month,
                          record.variable_data.rtc_y2k_day,
                          record.variable_data.rtc_y2k_hour,
                          record.variable_data.rtc_y2k_minute,
                          record.variable_data.rtc_y2k_seconds)

        mpt_seconds = float(record.variable_data.mpt_seconds + (record.variable_data.mpt_hundredths / 100))
        rtc_time = time.mktime(dts.timetuple())
        self.set_internal_timestamp(unix_time=rtc_time + record.variable_data.rtc_y2k_hundredths / 100.0)

        fields = []
        if record.coord_transform.coord_transform == 0:
            self._data_particle_type = DataParticleType.ADCP_PD0_PARSED_BEAM
            if self._slave:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_PARSED_BEAM
            elif self._master:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_BEAM_PARSED

            fields.extend([
                (ADCP_PD0_PARSED_KEY.BEAM_1_VELOCITY, record.velocities.beam1),
                (ADCP_PD0_PARSED_KEY.BEAM_2_VELOCITY, record.velocities.beam2),
                (ADCP_PD0_PARSED_KEY.BEAM_3_VELOCITY, record.velocities.beam3),
                (ADCP_PD0_PARSED_KEY.BEAM_4_VELOCITY, record.velocities.beam4),
                (ADCP_PD0_PARSED_KEY.PERCENT_GOOD_BEAM1, record.percent_good.beam1),
                (ADCP_PD0_PARSED_KEY.PERCENT_GOOD_BEAM2, record.percent_good.beam2),
                (ADCP_PD0_PARSED_KEY.PERCENT_GOOD_BEAM3, record.percent_good.beam3),
                (ADCP_PD0_PARSED_KEY.PERCENT_GOOD_BEAM4, record.percent_good.beam4)])

        elif record.coord_transform.coord_transform == 3:
            self._data_particle_type = DataParticleType.ADCP_PD0_PARSED_EARTH
            if self._slave:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_PARSED_EARTH
            elif self._master:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_EARTH_PARSED

            fields.extend([
                (ADCP_PD0_PARSED_KEY.WATER_VELOCITY_EAST, record.velocities.beam1),
                (ADCP_PD0_PARSED_KEY.WATER_VELOCITY_NORTH, record.velocities.beam2),
                (ADCP_PD0_PARSED_KEY.WATER_VELOCITY_UP, record.velocities.beam3),
                (ADCP_PD0_PARSED_KEY.ERROR_VELOCITY, record.velocities.beam4),
                (ADCP_PD0_PARSED_KEY.PERCENT_GOOD_3BEAM, record.percent_good.beam1),
                (ADCP_PD0_PARSED_KEY.PERCENT_TRANSFORMS_REJECT, record.percent_good.beam2),
                (ADCP_PD0_PARSED_KEY.PERCENT_BAD_BEAMS, record.percent_good.beam3),
                (ADCP_PD0_PARSED_KEY.PERCENT_GOOD_4BEAM, record.percent_good.beam4)])

        else:
            raise SampleException("coord_transform_type not coded for; %d" % record.coord_transform.coord_transform)

        fields.extend([
            # FIXED LEADER
            (ADCP_PD0_PARSED_KEY.HEADER_ID, record.header.id),
            (ADCP_PD0_PARSED_KEY.DATA_SOURCE_ID, record.header.data_source),
            (ADCP_PD0_PARSED_KEY.NUM_BYTES, record.header.num_bytes),
            (ADCP_PD0_PARSED_KEY.NUM_DATA_TYPES, record.header.num_data_types),
            (ADCP_PD0_PARSED_KEY.FIXED_LEADER_ID, record.fixed_data.id),
            (ADCP_PD0_PARSED_KEY.FIRMWARE_VERSION, record.fixed_data.cpu_firmware_version),
            (ADCP_PD0_PARSED_KEY.FIRMWARE_REVISION, record.fixed_data.cpu_firmware_revision),
            (ADCP_PD0_PARSED_KEY.DATA_FLAG, record.fixed_data.simulation_data_flag),
            (ADCP_PD0_PARSED_KEY.LAG_LENGTH, record.fixed_data.lag_length),
            (ADCP_PD0_PARSED_KEY.NUM_BEAMS, record.fixed_data.number_of_beams),
            (ADCP_PD0_PARSED_KEY.NUM_CELLS, record.fixed_data.number_of_cells),
            (ADCP_PD0_PARSED_KEY.PINGS_PER_ENSEMBLE, record.fixed_data.pings_per_ensemble),
            (ADCP_PD0_PARSED_KEY.DEPTH_CELL_LENGTH, record.fixed_data.depth_cell_length),
            (ADCP_PD0_PARSED_KEY.BLANK_AFTER_TRANSMIT, record.fixed_data.blank_after_transmit),
            (ADCP_PD0_PARSED_KEY.SIGNAL_PROCESSING_MODE, record.fixed_data.signal_processing_mode),
            (ADCP_PD0_PARSED_KEY.LOW_CORR_THRESHOLD, record.fixed_data.low_corr_threshold),
            (ADCP_PD0_PARSED_KEY.NUM_CODE_REPETITIONS, record.fixed_data.num_code_reps),
            (ADCP_PD0_PARSED_KEY.PERCENT_GOOD_MIN, record.fixed_data.minimum_percentage),
            (ADCP_PD0_PARSED_KEY.ERROR_VEL_THRESHOLD, record.fixed_data.error_velocity_max),
            (ADCP_PD0_PARSED_KEY.TIME_PER_PING_MINUTES, record.fixed_data.minutes),
            (ADCP_PD0_PARSED_KEY.HEADING_ALIGNMENT, record.fixed_data.heading_alignment),
            (ADCP_PD0_PARSED_KEY.HEADING_BIAS, record.fixed_data.heading_bias),
            (ADCP_PD0_PARSED_KEY.BIN_1_DISTANCE, record.fixed_data.bin_1_distance),
            (ADCP_PD0_PARSED_KEY.TRANSMIT_PULSE_LENGTH, record.fixed_data.transmit_pulse_length),
            (ADCP_PD0_PARSED_KEY.REFERENCE_LAYER_START, record.fixed_data.starting_depth_cell),
            (ADCP_PD0_PARSED_KEY.REFERENCE_LAYER_STOP, record.fixed_data.ending_depth_cell),
            (ADCP_PD0_PARSED_KEY.FALSE_TARGET_THRESHOLD, record.fixed_data.false_target_threshold),
            (ADCP_PD0_PARSED_KEY.LOW_LATENCY_TRIGGER, record.fixed_data.spare1),
            (ADCP_PD0_PARSED_KEY.TRANSMIT_LAG_DISTANCE, record.fixed_data.transmit_lag_distance),
            (ADCP_PD0_PARSED_KEY.CPU_BOARD_SERIAL_NUMBER, str(record.fixed_data.cpu_board_serial_number)),
            (ADCP_PD0_PARSED_KEY.SYSTEM_BANDWIDTH, record.fixed_data.system_bandwidth),
            (ADCP_PD0_PARSED_KEY.SYSTEM_POWER, record.fixed_data.system_power),
            (ADCP_PD0_PARSED_KEY.SERIAL_NUMBER, str(record.fixed_data.serial_number)),
            (ADCP_PD0_PARSED_KEY.BEAM_ANGLE, record.fixed_data.beam_angle),
            # VARIABLE LEADER
            (ADCP_PD0_PARSED_KEY.ENSEMBLE_NUMBER, record.variable_data.ensemble_number),
            (ADCP_PD0_PARSED_KEY.ENSEMBLE_NUMBER_INCREMENT, record.variable_data.ensemble_roll_over),
            (ADCP_PD0_PARSED_KEY.SPEED_OF_SOUND, record.variable_data.speed_of_sound),
            (ADCP_PD0_PARSED_KEY.TRANSDUCER_DEPTH, record.variable_data.depth_of_transducer),
            (ADCP_PD0_PARSED_KEY.HEADING, record.variable_data.heading),
            (ADCP_PD0_PARSED_KEY.PITCH, record.variable_data.pitch),
            (ADCP_PD0_PARSED_KEY.ROLL, record.variable_data.roll),
            (ADCP_PD0_PARSED_KEY.SALINITY, record.variable_data.salinity),
            (ADCP_PD0_PARSED_KEY.TEMPERATURE, record.variable_data.temperature),
            (ADCP_PD0_PARSED_KEY.MPT_MINUTES, record.variable_data.mpt_minutes),
            (ADCP_PD0_PARSED_KEY.HEADING_STDEV, record.variable_data.heading_standard_deviation),
            (ADCP_PD0_PARSED_KEY.PITCH_STDEV, record.variable_data.pitch_standard_deviation),
            (ADCP_PD0_PARSED_KEY.ROLL_STDEV, record.variable_data.roll_standard_deviation),
            (ADCP_PD0_PARSED_KEY.ADC_TRANSMIT_CURRENT, record.variable_data.transmit_current),
            (ADCP_PD0_PARSED_KEY.ADC_TRANSMIT_VOLTAGE, record.variable_data.transmit_voltage),
            (ADCP_PD0_PARSED_KEY.ADC_AMBIENT_TEMP, record.variable_data.ambient_temperature),
            (ADCP_PD0_PARSED_KEY.ADC_PRESSURE_PLUS, record.variable_data.pressure_positive),
            (ADCP_PD0_PARSED_KEY.ADC_PRESSURE_MINUS, record.variable_data.pressure_negative),
            (ADCP_PD0_PARSED_KEY.ADC_ATTITUDE_TEMP, record.variable_data.attitude_temperature),
            (ADCP_PD0_PARSED_KEY.ADC_ATTITUDE, record.variable_data.attitude),
            (ADCP_PD0_PARSED_KEY.ADC_CONTAMINATION_SENSOR, record.variable_data.contamination_sensor),
            (ADCP_PD0_PARSED_KEY.ABSOLUTE_PRESSURE, record.variable_data.pressure),
            (ADCP_PD0_PARSED_KEY.PRESSURE_VARIANCE, record.variable_data.pressure_variance),
            # SYSCONFIG BITMAP
            (ADCP_PD0_PARSED_KEY.SYSCONFIG_FREQUENCY, record.sysconfig.frequency),
            (ADCP_PD0_PARSED_KEY.SYSCONFIG_BEAM_PATTERN, record.sysconfig.beam_pattern),
            (ADCP_PD0_PARSED_KEY.SYSCONFIG_SENSOR_CONFIG, record.sysconfig.sensor_config),
            (ADCP_PD0_PARSED_KEY.SYSCONFIG_HEAD_ATTACHED, record.sysconfig.xdcr_head_attached),
            (ADCP_PD0_PARSED_KEY.SYSCONFIG_VERTICAL_ORIENTATION, record.sysconfig.beam_facing),
            # COORD TRANSFORM BITMAP
            (ADCP_PD0_PARSED_KEY.COORD_TRANSFORM_TYPE, record.coord_transform.coord_transform),
            (ADCP_PD0_PARSED_KEY.COORD_TRANSFORM_TILTS, record.coord_transform.tilts_used),
            (ADCP_PD0_PARSED_KEY.COORD_TRANSFORM_BEAMS, record.coord_transform.three_beam_used),
            (ADCP_PD0_PARSED_KEY.COORD_TRANSFORM_MAPPING, record.coord_transform.bin_mapping_used),
            # SENSOR SOURCE BITMAP
            (ADCP_PD0_PARSED_KEY.SENSOR_SOURCE_SPEED, record.sensor_source.calculate_ec),
            (ADCP_PD0_PARSED_KEY.SENSOR_SOURCE_DEPTH, record.sensor_source.depth_used),
            (ADCP_PD0_PARSED_KEY.SENSOR_SOURCE_HEADING, record.sensor_source.heading_used),
            (ADCP_PD0_PARSED_KEY.SENSOR_SOURCE_PITCH, record.sensor_source.pitch_used),
            (ADCP_PD0_PARSED_KEY.SENSOR_SOURCE_ROLL, record.sensor_source.roll_used),
            (ADCP_PD0_PARSED_KEY.SENSOR_SOURCE_CONDUCTIVITY, record.sensor_source.conductivity_used),
            (ADCP_PD0_PARSED_KEY.SENSOR_SOURCE_TEMPERATURE, record.sensor_source.temperature_used),
            # SENSOR AVAIL BITMAP
            (ADCP_PD0_PARSED_KEY.SENSOR_AVAILABLE_DEPTH, record.sensor_avail.depth_avail),
            (ADCP_PD0_PARSED_KEY.SENSOR_AVAILABLE_HEADING, record.sensor_avail.heading_avail),
            (ADCP_PD0_PARSED_KEY.SENSOR_AVAILABLE_PITCH, record.sensor_avail.pitch_avail),
            (ADCP_PD0_PARSED_KEY.SENSOR_AVAILABLE_ROLL, record.sensor_avail.roll_avail),
            (ADCP_PD0_PARSED_KEY.SENSOR_AVAILABLE_CONDUCTIVITY, record.sensor_avail.conductivity_avail),
            (ADCP_PD0_PARSED_KEY.SENSOR_AVAILABLE_TEMPERATURE, record.sensor_avail.temperature_avail),
            # BIT RESULT BITMAP
            (ADCP_PD0_PARSED_KEY.BIT_RESULT_DEMOD_0, record.bit_result.demod0_error),
            (ADCP_PD0_PARSED_KEY.BIT_RESULT_DEMOD_1, record.bit_result.demod1_error),
            (ADCP_PD0_PARSED_KEY.BIT_RESULT_TIMING, record.bit_result.timing_card_error),
            # ERROR WORD BITMAP
            (ADCP_PD0_PARSED_KEY.BUS_ERROR_EXCEPTION, record.error_word.bus_error),
            (ADCP_PD0_PARSED_KEY.ADDRESS_ERROR_EXCEPTION, record.error_word.address_error),
            (ADCP_PD0_PARSED_KEY.ILLEGAL_INSTRUCTION_EXCEPTION, record.error_word.illegal_instruction),
            (ADCP_PD0_PARSED_KEY.ZERO_DIVIDE_INSTRUCTION, record.error_word.zero_divide),
            (ADCP_PD0_PARSED_KEY.EMULATOR_EXCEPTION, record.error_word.emulator),
            (ADCP_PD0_PARSED_KEY.UNASSIGNED_EXCEPTION, record.error_word.unassigned),
            (ADCP_PD0_PARSED_KEY.WATCHDOG_RESTART_OCCURRED, record.error_word.watchdog_restart),
            (ADCP_PD0_PARSED_KEY.BATTERY_SAVER_POWER, record.error_word.battery_saver),
            (ADCP_PD0_PARSED_KEY.PINGING, record.error_word.pinging),
            (ADCP_PD0_PARSED_KEY.COLD_WAKEUP_OCCURRED, record.error_word.cold_wakeup),
            (ADCP_PD0_PARSED_KEY.UNKNOWN_WAKEUP_OCCURRED, record.error_word.unknown_wakeup),
            (ADCP_PD0_PARSED_KEY.CLOCK_READ_ERROR, record.error_word.clock_read),
            (ADCP_PD0_PARSED_KEY.UNEXPECTED_ALARM, record.error_word.unexpected_alarm),
            (ADCP_PD0_PARSED_KEY.CLOCK_JUMP_FORWARD, record.error_word.clock_jump_forward),
            (ADCP_PD0_PARSED_KEY.CLOCK_JUMP_BACKWARD, record.error_word.clock_jump_backward),
            (ADCP_PD0_PARSED_KEY.POWER_FAIL, record.error_word.power_fail),
            (ADCP_PD0_PARSED_KEY.SPURIOUS_DSP_INTERRUPT, record.error_word.spurious_dsp),
            (ADCP_PD0_PARSED_KEY.SPURIOUS_UART_INTERRUPT, record.error_word.spurious_uart),
            (ADCP_PD0_PARSED_KEY.SPURIOUS_CLOCK_INTERRUPT, record.error_word.spurious_clock),
            (ADCP_PD0_PARSED_KEY.LEVEL_7_INTERRUPT, record.error_word.level_7_interrupt),
            # COMPUTED VALUES
            (ADCP_PD0_PARSED_KEY.TIME_PER_PING_SECONDS, tpp_float_seconds),
            (ADCP_PD0_PARSED_KEY.MPT_SECONDS, mpt_seconds),
            # CORRELATION MAGNITUDES
            (ADCP_PD0_PARSED_KEY.CORRELATION_MAGNITUDE_BEAM1, record.correlation_magnitudes.beam1),
            (ADCP_PD0_PARSED_KEY.CORRELATION_MAGNITUDE_BEAM2, record.correlation_magnitudes.beam2),
            (ADCP_PD0_PARSED_KEY.CORRELATION_MAGNITUDE_BEAM3, record.correlation_magnitudes.beam3),
            (ADCP_PD0_PARSED_KEY.CORRELATION_MAGNITUDE_BEAM4, record.correlation_magnitudes.beam4),
            # ECHO INTENSITIES
            (ADCP_PD0_PARSED_KEY.ECHO_INTENSITY_BEAM1, record.echo_intensity.beam1),
            (ADCP_PD0_PARSED_KEY.ECHO_INTENSITY_BEAM2, record.echo_intensity.beam2),
            (ADCP_PD0_PARSED_KEY.ECHO_INTENSITY_BEAM3, record.echo_intensity.beam3),
            (ADCP_PD0_PARSED_KEY.ECHO_INTENSITY_BEAM4, record.echo_intensity.beam4)])

        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


# ADCP System Configuration keys will be varied in VADCP
class ADCP_SYSTEM_CONFIGURATION_KEY(BaseEnum):
    # https://confluence.oceanobservatories.org/display/instruments/ADCP+Driver
    # from PS0
    SERIAL_NUMBER = "serial_number"
    TRANSDUCER_FREQUENCY = "transducer_frequency"
    CONFIGURATION = "configuration"
    MATCH_LAYER = "match_layer"
    BEAM_ANGLE = "beam_angle"
    BEAM_PATTERN = "beam_pattern"
    ORIENTATION = "orientation"
    SENSORS = "sensors"
    PRESSURE_COEFF_c3 = "pressure_coeff_c3"
    PRESSURE_COEFF_c2 = "pressure_coeff_c2"
    PRESSURE_COEFF_c1 = "pressure_coeff_c1"
    PRESSURE_COEFF_OFFSET = "pressure_coeff_offset"
    TEMPERATURE_SENSOR_OFFSET = "temperature_sensor_offset"
    CPU_FIRMWARE = "cpu_firmware"
    BOOT_CODE_REQUIRED = "boot_code_required"
    BOOT_CODE_ACTUAL = "boot_code_actual"
    DEMOD_1_VERSION = "demod_1_version"
    DEMOD_1_TYPE = "demod_1_type"
    DEMOD_2_VERSION = "demod_2_version"
    DEMOD_2_TYPE = "demod_2_type"
    POWER_TIMING_VERSION = "power_timing_version"
    POWER_TIMING_TYPE = "power_timing_type"
    BOARD_SERIAL_NUMBERS = "board_serial_numbers"


# ADCP System Configuration keys will be varied in VADCP
# Some of the output lines will not be available in VADCP as it support only
# 4 beams and 5th beam
class ADCP_SYSTEM_CONFIGURATION_DataParticle(DataParticle):
    _data_particle_type = DataParticleType.ADCP_SYSTEM_CONFIGURATION
    _slave = False
    _master = False
    _offset = 0

    RE00 = re.compile(r'Instrument S/N: +(\d+)')
    RE01 = re.compile(r'       Frequency: +(\d+) HZ')
    RE02 = re.compile(r'   Configuration: +([a-zA-Z0-9, ]+)')
    RE03 = re.compile(r'     Match Layer: +(\d+)')
    RE04 = re.compile(r'      Beam Angle:  ([0-9.]+) DEGREES')
    RE05 = re.compile(r'    Beam Pattern:  ([a-zA-Z]+)')
    RE06 = re.compile(r'     Orientation:  ([a-zA-Z]+)')
    RE07 = re.compile(r'       Sensor\(s\):  ([a-zA-Z0-9 ]+)')

    RE09 = re.compile(r'              c3 = ([\+\-0-9.E]+)')
    RE10 = re.compile(r'              c2 = ([\+\-0-9.E]+)')
    RE11 = re.compile(r'              c1 = ([\+\-0-9.E]+)')
    RE12 = re.compile(r'          Offset = ([\+\-0-9.E]+)')

    RE14 = re.compile(r'Temp Sens Offset: +([\+\-0-9.]+) degrees C')

    RE16 = re.compile(r'    CPU Firmware:  ([0-9.\[\] ]+)')
    RE17 = re.compile(r'   Boot Code Ver:  Required: +([0-9.]+) +Actual: +([0-9.]+)')
    RE18 = re.compile(r'    DEMOD #1 Ver: +([a-zA-Z0-9]+), Type: +([a-zA-Z0-9]+)')
    RE19 = re.compile(r'    DEMOD #2 Ver: +([a-zA-Z0-9]+), Type: +([a-zA-Z0-9]+)')
    RE20 = re.compile(r'    PWRTIMG  Ver: +([a-zA-Z0-9]+), Type: +([a-zA-Z0-9]+)')

    RE23 = re.compile(r' +([0-9a-zA-Z\- ]+)')
    RE24 = re.compile(r' +([0-9a-zA-Z\- ]+)')
    RE25 = re.compile(r' +([0-9a-zA-Z\- ]+)')
    RE26 = re.compile(r' +([0-9a-zA-Z\- ]+)')
    RE27 = re.compile(r' +([0-9a-zA-Z\- ]+)')
    RE28 = re.compile(r' +([0-9a-zA-Z\- ]+)')

    def _build_parsed_values(self):
        # Initialize
        matches = {}

        lines = self.raw_data.split(NEWLINE)

        match = self.RE00.match(lines[0])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.SERIAL_NUMBER] = str(match.group(1))
        match = self.RE01.match(lines[1])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.TRANSDUCER_FREQUENCY] = int(match.group(1))
        match = self.RE02.match(lines[2])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.CONFIGURATION] = match.group(1)
        match = self.RE03.match(lines[3])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.MATCH_LAYER] = match.group(1)
        match = self.RE04.match(lines[4])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BEAM_ANGLE] = int(match.group(1))
        match = self.RE05.match(lines[5])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BEAM_PATTERN] = match.group(1)
        match = self.RE06.match(lines[6])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.ORIENTATION] = match.group(1)
        match = self.RE07.match(lines[7])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.SENSORS] = match.group(1)

        # Only available for ADCP and VADCP master
        if not self._slave:
            match = self.RE09.match(lines[9 - self._offset])
            matches[ADCP_SYSTEM_CONFIGURATION_KEY.PRESSURE_COEFF_c3] = float(match.group(1))
            match = self.RE10.match(lines[10 - self._offset])
            matches[ADCP_SYSTEM_CONFIGURATION_KEY.PRESSURE_COEFF_c2] = float(match.group(1))
            match = self.RE11.match(lines[11 - self._offset])
            matches[ADCP_SYSTEM_CONFIGURATION_KEY.PRESSURE_COEFF_c1] = float(match.group(1))
            match = self.RE12.match(lines[12 - self._offset])
            matches[ADCP_SYSTEM_CONFIGURATION_KEY.PRESSURE_COEFF_OFFSET] = float(match.group(1))

        match = self.RE14.match(lines[14 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.TEMPERATURE_SENSOR_OFFSET] = float(match.group(1))
        match = self.RE16.match(lines[16 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.CPU_FIRMWARE] = match.group(1)
        match = self.RE17.match(lines[17 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOOT_CODE_REQUIRED] = match.group(1)
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOOT_CODE_ACTUAL] = match.group(2)
        match = self.RE18.match(lines[18 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.DEMOD_1_VERSION] = match.group(1)
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.DEMOD_1_TYPE] = match.group(2)
        match = self.RE19.match(lines[19 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.DEMOD_2_VERSION] = match.group(1)
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.DEMOD_2_TYPE] = match.group(2)
        match = self.RE20.match(lines[20 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.POWER_TIMING_VERSION] = match.group(1)
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.POWER_TIMING_TYPE] = match.group(2)
        match = self.RE23.match(lines[23 - self._offset])

        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOARD_SERIAL_NUMBERS] = str(match.group(1)) + "\n"
        match = self.RE24.match(lines[24 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"
        match = self.RE25.match(lines[25 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"
        match = self.RE26.match(lines[26 - self._offset])
        matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"

        # Only available for ADCP
        if not self._slave and not self._master:
            match = self.RE27.match(lines[27 - self._offset])
            matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"
            match = self.RE28.match(lines[28 - self._offset])
            matches[ADCP_SYSTEM_CONFIGURATION_KEY.BOARD_SERIAL_NUMBERS] += str(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})
        return result


# AC command
class ADCP_COMPASS_CALIBRATION_KEY(BaseEnum):
    """
    Keys for ADCP Compass Calibration
    """
    FLUXGATE_CALIBRATION_TIMESTAMP = "fluxgate_calibration_timestamp"
    S_INVERSE_BX = "s_inverse_bx"
    S_INVERSE_BY = "s_inverse_by"
    S_INVERSE_BZ = "s_inverse_bz"
    S_INVERSE_ERR = "s_inverse_err"
    COIL_OFFSET = "coil_offset"
    ELECTRICAL_NULL = "electrical_null"
    TILT_CALIBRATION_TIMESTAMP = "tilt_calibration_timestamp"
    CALIBRATION_TEMP = "calibration_temp"
    ROLL_UP_DOWN = "roll_up_down"
    PITCH_UP_DOWN = "pitch_up_down"
    OFFSET_UP_DOWN = "offset_up_down"
    TILT_NULL = "tilt_null"


class ADCP_COMPASS_CALIBRATION_DataParticle(DataParticle):
    """
    ADCP Compass Calibration data particle
    """
    _data_particle_type = DataParticleType.ADCP_COMPASS_CALIBRATION

    RE01 = re.compile(r' +Calibration date and time: ([/0-9: ]+)')
    RE04 = re.compile(r' +Bx +. +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) .')
    RE05 = re.compile(r' +By +. +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) .')
    RE06 = re.compile(r' +Bz +. +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) .')
    RE07 = re.compile(r' +Err +. +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) +([0-9e+-.]+) .')

    RE11 = re.compile(r' +. +([0-9e+-.]+) +.')
    RE12 = re.compile(r' +. +([0-9e+-.]+) +.')
    RE13 = re.compile(r' +. +([0-9e+-.]+) +.')
    RE14 = re.compile(r' +. +([0-9e+-.]+) +.')

    RE18 = re.compile(r' +. ([0-9.]+) .')
    RE21 = re.compile(r' +Calibration date and time: ([/0-9: ]+)')
    RE22 = re.compile(r' +Average Temperature During Calibration was +([0-9.]+) .')
    RE27 = re.compile(r' Roll +. +([0-9e+-.]+) +([0-9e+-.]+) +. +. +([0-9e+-.]+) +([0-9e+-.]+) +.')
    RE28 = re.compile(r' Pitch +. +([0-9e+-.]+) +([0-9e+-.]+) +. +. +([0-9e+-.]+) +([0-9e+-.]+) +.')
    RE32 = re.compile(r' Offset . +([0-9e+-.]+) +([0-9e+-.]+) +. +. +([0-9e+-.]+) +([0-9e+-.]+) +.')
    RE36 = re.compile(r' +Null +. (\d+) +.')

    def _build_parsed_values(self):
        # Initialize
        matches = {}
        lines = self.raw_data.split(NEWLINE)
        match = self.RE01.match(lines[1])
        timestamp = match.group(1)
        matches[ADCP_COMPASS_CALIBRATION_KEY.FLUXGATE_CALIBRATION_TIMESTAMP] = time.mktime(
            time.strptime(timestamp, "%m/%d/%Y  %H:%M:%S"))

        match = self.RE04.match(lines[4])
        matches[ADCP_COMPASS_CALIBRATION_KEY.S_INVERSE_BX] = [float(match.group(1)), float(match.group(2)),
                                                              float(match.group(3)), float(match.group(4))]
        match = self.RE05.match(lines[5])
        matches[ADCP_COMPASS_CALIBRATION_KEY.S_INVERSE_BY] = [float(match.group(1)), float(match.group(2)),
                                                              float(match.group(3)), float(match.group(4))]
        match = self.RE06.match(lines[6])
        matches[ADCP_COMPASS_CALIBRATION_KEY.S_INVERSE_BZ] = [float(match.group(1)), float(match.group(2)),
                                                              float(match.group(3)), float(match.group(4))]
        match = self.RE07.match(lines[7])
        matches[ADCP_COMPASS_CALIBRATION_KEY.S_INVERSE_ERR] = [float(match.group(1)), float(match.group(2)),
                                                               float(match.group(3)), float(match.group(4))]

        match = self.RE11.match(lines[11])
        matches[ADCP_COMPASS_CALIBRATION_KEY.COIL_OFFSET] = [float(match.group(1))]
        match = self.RE12.match(lines[12])
        matches[ADCP_COMPASS_CALIBRATION_KEY.COIL_OFFSET].append(float(match.group(1)))
        match = self.RE13.match(lines[13])
        matches[ADCP_COMPASS_CALIBRATION_KEY.COIL_OFFSET].append(float(match.group(1)))
        match = self.RE14.match(lines[14])
        matches[ADCP_COMPASS_CALIBRATION_KEY.COIL_OFFSET].append(float(match.group(1)))

        match = self.RE18.match(lines[18])
        matches[ADCP_COMPASS_CALIBRATION_KEY.ELECTRICAL_NULL] = float(match.group(1))

        match = self.RE21.match(lines[21])
        timestamp = match.group(1)
        matches[ADCP_COMPASS_CALIBRATION_KEY.TILT_CALIBRATION_TIMESTAMP] = time.mktime(
            time.strptime(timestamp, "%m/%d/%Y  %H:%M:%S"))

        match = self.RE22.match(lines[22])
        matches[ADCP_COMPASS_CALIBRATION_KEY.CALIBRATION_TEMP] = float(match.group(1))

        match = self.RE27.match(lines[27])
        matches[ADCP_COMPASS_CALIBRATION_KEY.ROLL_UP_DOWN] = [float(match.group(1)), float(match.group(2)),
                                                              float(match.group(3)), float(match.group(4))]
        match = self.RE28.match(lines[28])
        matches[ADCP_COMPASS_CALIBRATION_KEY.PITCH_UP_DOWN] = [float(match.group(1)), float(match.group(2)),
                                                               float(match.group(3)), float(match.group(4))]
        match = self.RE32.match(lines[32])
        matches[ADCP_COMPASS_CALIBRATION_KEY.OFFSET_UP_DOWN] = [float(match.group(1)), float(match.group(2)),
                                                                float(match.group(3)), float(match.group(4))]

        match = self.RE36.match(lines[36])
        matches[ADCP_COMPASS_CALIBRATION_KEY.TILT_NULL] = float(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result


# for keys for PT2 command
class ADCP_ANCILLARY_SYSTEM_DATA_KEY(BaseEnum):
    """
    Keys for PT2 command
    """
    ADCP_AMBIENT_CURRENT = "adcp_ambient_temp"
    ADCP_ATTITUDE_TEMP = "adcp_attitude_temp"
    ADCP_INTERNAL_MOISTURE = "adcp_internal_moisture"


# PT2 command data particle
class ADCP_ANCILLARY_SYSTEM_DATA_PARTICLE(DataParticle):
    """
    Data particle for PT2 command
    """
    _data_particle_type = DataParticleType.ADCP_ANCILLARY_SYSTEM_DATA

    RE01 = re.compile(r'Ambient  Temperature = +([\+\-0-9.]+) Degrees C')
    RE02 = re.compile(r'Attitude Temperature = +([\+\-0-9.]+) Degrees C')
    RE03 = re.compile(r'Internal Moisture    = +([a-zA-Z0-9]+)')

    def _build_parsed_values(self):
        # Initialize
        matches = {}

        for key, regex, formatter in [
            (ADCP_ANCILLARY_SYSTEM_DATA_KEY.ADCP_AMBIENT_CURRENT, self.RE01, float),
            (ADCP_ANCILLARY_SYSTEM_DATA_KEY.ADCP_ATTITUDE_TEMP, self.RE02, float),
            (ADCP_ANCILLARY_SYSTEM_DATA_KEY.ADCP_INTERNAL_MOISTURE, self.RE03, str),
        ]:
            match = regex.search(self.raw_data)
            matches[key] = formatter(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result


# keys for PT4 command
class ADCP_TRANSMIT_PATH_KEY(BaseEnum):
    ADCP_TRANSIT_CURRENT = "adcp_transmit_current"
    ADCP_TRANSIT_VOLTAGE = "adcp_transmit_voltage"
    ADCP_TRANSIT_IMPEDANCE = "adcp_transmit_impedance"
    ADCP_TRANSIT_TEST_RESULT = "adcp_transmit_test_results"


# Data particle for PT4 command
class ADCP_TRANSMIT_PATH_PARTICLE(DataParticle):
    _data_particle_type = DataParticleType.ADCP_TRANSMIT_PATH

    RE01 = re.compile(r'IXMT += +([\+\-0-9.]+) Amps')
    RE02 = re.compile(r'VXMT += +([\+\-0-9.]+) Volts')
    RE03 = re.compile(r' +Z += +([\+\-0-9.]+) Ohms')
    RE04 = re.compile(r'Transmit Test Results = +(.*)\r')

    def _build_parsed_values(self):
        # Initialize
        matches = {}
        for key, regex, formatter in [
            (ADCP_TRANSMIT_PATH_KEY.ADCP_TRANSIT_CURRENT, self.RE01, float),
            (ADCP_TRANSMIT_PATH_KEY.ADCP_TRANSIT_VOLTAGE, self.RE02, float),
            (ADCP_TRANSMIT_PATH_KEY.ADCP_TRANSIT_IMPEDANCE, self.RE03, float),
            (ADCP_TRANSMIT_PATH_KEY.ADCP_TRANSIT_TEST_RESULT, self.RE04, str),
        ]:
            match = regex.search(self.raw_data)
            matches[key] = formatter(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result




