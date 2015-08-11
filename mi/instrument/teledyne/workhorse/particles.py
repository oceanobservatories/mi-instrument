import re
import time
import struct
from datetime import datetime

from mi.core.log import get_logger

log = get_logger()

from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException
from mi.core.instrument.data_particle import CommonDataParticleType, DataParticle, DataParticleKey
from mi.instrument.teledyne.workhorse.pd0_parser import AdcpPd0Record

NEWLINE = '\r\n'


class WorkhorseDataParticleType(BaseEnum):
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


class VADCPDataParticleType(WorkhorseDataParticleType):
    """
    VADCP Stream types of data particles
    """
    VADCP_PD0_BEAM_MASTER = 'vadcp_pd0_beam_parsed'
    VADCP_PD0_EARTH_MASTER = 'vadcp_pd0_earth_parsed'
    VADCP_4BEAM_SYSTEM_CONFIGURATION = "vadcp_4beam_system_configuration"
    VADCP_5THBEAM_SYSTEM_CONFIGURATION = "vadcp_5thbeam_system_configuration"
    VADCP_ANCILLARY_SYSTEM_DATA = "vadcp_ancillary_system_data"
    VADCP_TRANSMIT_PATH = "vadcp_transmit_path"
    VADCP_PD0_BEAM_SLAVE = 'vadcp_5thbeam_pd0_beam_parsed'
    VADCP_PD0_EARTH_SLAVE = 'vadcp_5thbeam_pd0_earth_parsed'
    VADCP_COMPASS_CALIBRATION = 'vadcp_5thbeam_compass_calibration'


class AdcpPd0ParsedKey(BaseEnum):
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


class Pd0CoordinateTransformType(BaseEnum):
    BEAM = 0
    EARTH = 3


# The data particle type will be overwritten based on coordinate (Earth/Beam)
class AdcpPd0ParsedDataParticle(DataParticle):
    """
    ADCP PD0 data particle
    @throw SampleException if when break happens
    """
    _data_particle_type = None
    _slave = False
    _master = False
    ntp_epoch = datetime(1900, 1, 1)

    def _build_parsed_values(self):
        """
        Parse the base portion of the particle
        """
        record = AdcpPd0Record(self.raw_data)
        record.process()
        record.parse_bitmapped_fields()

        tpp_float_seconds = float(record.fixed_data.seconds + (record.fixed_data.hundredths / 100))
        dts = datetime(record.variable_data.rtc_y2k_century * 100 + record.variable_data.rtc_y2k_year,
                       record.variable_data.rtc_y2k_month,
                       record.variable_data.rtc_y2k_day,
                       record.variable_data.rtc_y2k_hour,
                       record.variable_data.rtc_y2k_minute,
                       record.variable_data.rtc_y2k_seconds)

        mpt_seconds = float(record.variable_data.mpt_seconds + (record.variable_data.mpt_hundredths / 100.0))
        rtc_time = (dts - self.ntp_epoch).total_seconds() + record.variable_data.rtc_y2k_hundredths / 100.0
        self.set_internal_timestamp(rtc_time)

        ensemble_time = rtc_time

        fields = [(AdcpPd0ParsedKey.ENSEMBLE_START_TIME, ensemble_time),
                  (AdcpPd0ParsedKey.CHECKSUM, record.stored_checksum),
                  (AdcpPd0ParsedKey.OFFSET_DATA_TYPES, record.offsets),
                  (AdcpPd0ParsedKey.REAL_TIME_CLOCK, (record.variable_data.rtc_y2k_century,
                                                      record.variable_data.rtc_y2k_year,
                                                      record.variable_data.rtc_y2k_month,
                                                      record.variable_data.rtc_y2k_day,
                                                      record.variable_data.rtc_y2k_hour,
                                                      record.variable_data.rtc_y2k_minute,
                                                      record.variable_data.rtc_y2k_seconds,
                                                      record.variable_data.rtc_y2k_hundredths))]

        if record.coord_transform.coord_transform == Pd0CoordinateTransformType.BEAM:
            self._data_particle_type = WorkhorseDataParticleType.ADCP_PD0_PARSED_BEAM
            if self._slave:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_BEAM_SLAVE
            elif self._master:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_BEAM_MASTER

            fields.extend([
                (AdcpPd0ParsedKey.VELOCITY_DATA_ID, record.velocities.id),
                (AdcpPd0ParsedKey.BEAM_1_VELOCITY, record.velocities.beam1),
                (AdcpPd0ParsedKey.BEAM_2_VELOCITY, record.velocities.beam2),
                (AdcpPd0ParsedKey.BEAM_3_VELOCITY, record.velocities.beam3),
                (AdcpPd0ParsedKey.BEAM_4_VELOCITY, record.velocities.beam4),
                (AdcpPd0ParsedKey.PERCENT_GOOD_ID, record.percent_good.id),
                (AdcpPd0ParsedKey.PERCENT_GOOD_BEAM1, record.percent_good.beam1),
                (AdcpPd0ParsedKey.PERCENT_GOOD_BEAM2, record.percent_good.beam2),
                (AdcpPd0ParsedKey.PERCENT_GOOD_BEAM3, record.percent_good.beam3),
                (AdcpPd0ParsedKey.PERCENT_GOOD_BEAM4, record.percent_good.beam4)])

        elif record.coord_transform.coord_transform == Pd0CoordinateTransformType.EARTH:
            self._data_particle_type = WorkhorseDataParticleType.ADCP_PD0_PARSED_EARTH
            if self._slave:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_EARTH_SLAVE
            elif self._master:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_EARTH_MASTER

            fields.extend([
                (AdcpPd0ParsedKey.VELOCITY_DATA_ID, record.velocities.id),
                (AdcpPd0ParsedKey.PERCENT_GOOD_ID, record.percent_good.id),
                (AdcpPd0ParsedKey.WATER_VELOCITY_EAST, record.velocities.beam1),
                (AdcpPd0ParsedKey.WATER_VELOCITY_NORTH, record.velocities.beam2),
                (AdcpPd0ParsedKey.WATER_VELOCITY_UP, record.velocities.beam3),
                (AdcpPd0ParsedKey.ERROR_VELOCITY, record.velocities.beam4),
                (AdcpPd0ParsedKey.PERCENT_GOOD_3BEAM, record.percent_good.beam1),
                (AdcpPd0ParsedKey.PERCENT_TRANSFORMS_REJECT, record.percent_good.beam2),
                (AdcpPd0ParsedKey.PERCENT_BAD_BEAMS, record.percent_good.beam3),
                (AdcpPd0ParsedKey.PERCENT_GOOD_4BEAM, record.percent_good.beam4)])

        else:
            raise SampleException("coord_transform_type not coded for; %d" % record.coord_transform.coord_transform)

        fields.extend([
            # FIXED LEADER
            (AdcpPd0ParsedKey.HEADER_ID, record.header.id),
            (AdcpPd0ParsedKey.DATA_SOURCE_ID, record.header.data_source),
            (AdcpPd0ParsedKey.NUM_BYTES, record.header.num_bytes),
            (AdcpPd0ParsedKey.NUM_DATA_TYPES, record.header.num_data_types),
            (AdcpPd0ParsedKey.FIXED_LEADER_ID, record.fixed_data.id),
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
            (AdcpPd0ParsedKey.TIME_PER_PING_MINUTES, record.fixed_data.minutes),
            (AdcpPd0ParsedKey.HEADING_ALIGNMENT, record.fixed_data.heading_alignment),
            (AdcpPd0ParsedKey.HEADING_BIAS, record.fixed_data.heading_bias),
            (AdcpPd0ParsedKey.BIN_1_DISTANCE, record.fixed_data.bin_1_distance),
            (AdcpPd0ParsedKey.TRANSMIT_PULSE_LENGTH, record.fixed_data.transmit_pulse_length),
            (AdcpPd0ParsedKey.REFERENCE_LAYER_START, record.fixed_data.starting_depth_cell),
            (AdcpPd0ParsedKey.REFERENCE_LAYER_STOP, record.fixed_data.ending_depth_cell),
            (AdcpPd0ParsedKey.FALSE_TARGET_THRESHOLD, record.fixed_data.false_target_threshold),
            (AdcpPd0ParsedKey.LOW_LATENCY_TRIGGER, record.fixed_data.spare1),
            (AdcpPd0ParsedKey.TRANSMIT_LAG_DISTANCE, record.fixed_data.transmit_lag_distance),
            (AdcpPd0ParsedKey.CPU_BOARD_SERIAL_NUMBER, str(record.fixed_data.cpu_board_serial_number)),
            (AdcpPd0ParsedKey.SYSTEM_BANDWIDTH, record.fixed_data.system_bandwidth),
            (AdcpPd0ParsedKey.SYSTEM_POWER, record.fixed_data.system_power),
            (AdcpPd0ParsedKey.SERIAL_NUMBER, str(record.fixed_data.serial_number)),
            (AdcpPd0ParsedKey.BEAM_ANGLE, record.fixed_data.beam_angle),
            # VARIABLE LEADER
            (AdcpPd0ParsedKey.VARIABLE_LEADER_ID, record.variable_data.id),
            (AdcpPd0ParsedKey.ENSEMBLE_NUMBER, record.variable_data.ensemble_number),
            (AdcpPd0ParsedKey.ENSEMBLE_NUMBER_INCREMENT, record.variable_data.ensemble_roll_over),
            (AdcpPd0ParsedKey.SPEED_OF_SOUND, record.variable_data.speed_of_sound),
            (AdcpPd0ParsedKey.TRANSDUCER_DEPTH, record.variable_data.depth_of_transducer),
            (AdcpPd0ParsedKey.HEADING, record.variable_data.heading),
            (AdcpPd0ParsedKey.PITCH, record.variable_data.pitch),
            (AdcpPd0ParsedKey.ROLL, record.variable_data.roll),
            (AdcpPd0ParsedKey.SALINITY, record.variable_data.salinity),
            (AdcpPd0ParsedKey.TEMPERATURE, record.variable_data.temperature),
            (AdcpPd0ParsedKey.MPT_MINUTES, record.variable_data.mpt_minutes),
            (AdcpPd0ParsedKey.HEADING_STDEV, record.variable_data.heading_standard_deviation),
            (AdcpPd0ParsedKey.PITCH_STDEV, record.variable_data.pitch_standard_deviation),
            (AdcpPd0ParsedKey.ROLL_STDEV, record.variable_data.roll_standard_deviation),
            (AdcpPd0ParsedKey.ADC_TRANSMIT_CURRENT, record.variable_data.transmit_current),
            (AdcpPd0ParsedKey.ADC_TRANSMIT_VOLTAGE, record.variable_data.transmit_voltage),
            (AdcpPd0ParsedKey.ADC_AMBIENT_TEMP, record.variable_data.ambient_temperature),
            (AdcpPd0ParsedKey.ADC_PRESSURE_PLUS, record.variable_data.pressure_positive),
            (AdcpPd0ParsedKey.ADC_PRESSURE_MINUS, record.variable_data.pressure_negative),
            (AdcpPd0ParsedKey.ADC_ATTITUDE_TEMP, record.variable_data.attitude_temperature),
            (AdcpPd0ParsedKey.ADC_ATTITUDE, record.variable_data.attitude),
            (AdcpPd0ParsedKey.ADC_CONTAMINATION_SENSOR, record.variable_data.contamination_sensor),
            (AdcpPd0ParsedKey.ABSOLUTE_PRESSURE, record.variable_data.pressure),
            (AdcpPd0ParsedKey.PRESSURE_VARIANCE, record.variable_data.pressure_variance),
            # SYSCONFIG BITMAP
            (AdcpPd0ParsedKey.SYSCONFIG_FREQUENCY, record.sysconfig.frequency),
            (AdcpPd0ParsedKey.SYSCONFIG_BEAM_PATTERN, record.sysconfig.beam_pattern),
            (AdcpPd0ParsedKey.SYSCONFIG_SENSOR_CONFIG, record.sysconfig.sensor_config),
            (AdcpPd0ParsedKey.SYSCONFIG_HEAD_ATTACHED, record.sysconfig.xdcr_head_attached),
            (AdcpPd0ParsedKey.SYSCONFIG_VERTICAL_ORIENTATION, record.sysconfig.beam_facing),
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
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_DEPTH, record.sensor_avail.depth_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_HEADING, record.sensor_avail.heading_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_PITCH, record.sensor_avail.pitch_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_ROLL, record.sensor_avail.roll_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_CONDUCTIVITY, record.sensor_avail.conductivity_avail),
            (AdcpPd0ParsedKey.SENSOR_AVAILABLE_TEMPERATURE, record.sensor_avail.temperature_avail),
            # BIT RESULT BITMAP
            (AdcpPd0ParsedKey.BIT_RESULT_DEMOD_0, record.bit_result.demod0_error),
            (AdcpPd0ParsedKey.BIT_RESULT_DEMOD_1, record.bit_result.demod1_error),
            (AdcpPd0ParsedKey.BIT_RESULT_TIMING, record.bit_result.timing_card_error),
            # ERROR WORD BITMAP
            (AdcpPd0ParsedKey.BUS_ERROR_EXCEPTION, record.error_word.bus_error),
            (AdcpPd0ParsedKey.ADDRESS_ERROR_EXCEPTION, record.error_word.address_error),
            (AdcpPd0ParsedKey.ILLEGAL_INSTRUCTION_EXCEPTION, record.error_word.illegal_instruction),
            (AdcpPd0ParsedKey.ZERO_DIVIDE_INSTRUCTION, record.error_word.zero_divide),
            (AdcpPd0ParsedKey.EMULATOR_EXCEPTION, record.error_word.emulator),
            (AdcpPd0ParsedKey.UNASSIGNED_EXCEPTION, record.error_word.unassigned),
            (AdcpPd0ParsedKey.WATCHDOG_RESTART_OCCURRED, record.error_word.watchdog_restart),
            (AdcpPd0ParsedKey.BATTERY_SAVER_POWER, record.error_word.battery_saver),
            (AdcpPd0ParsedKey.PINGING, record.error_word.pinging),
            (AdcpPd0ParsedKey.COLD_WAKEUP_OCCURRED, record.error_word.cold_wakeup),
            (AdcpPd0ParsedKey.UNKNOWN_WAKEUP_OCCURRED, record.error_word.unknown_wakeup),
            (AdcpPd0ParsedKey.CLOCK_READ_ERROR, record.error_word.clock_read),
            (AdcpPd0ParsedKey.UNEXPECTED_ALARM, record.error_word.unexpected_alarm),
            (AdcpPd0ParsedKey.CLOCK_JUMP_FORWARD, record.error_word.clock_jump_forward),
            (AdcpPd0ParsedKey.CLOCK_JUMP_BACKWARD, record.error_word.clock_jump_backward),
            (AdcpPd0ParsedKey.POWER_FAIL, record.error_word.power_fail),
            (AdcpPd0ParsedKey.SPURIOUS_DSP_INTERRUPT, record.error_word.spurious_dsp),
            (AdcpPd0ParsedKey.SPURIOUS_UART_INTERRUPT, record.error_word.spurious_uart),
            (AdcpPd0ParsedKey.SPURIOUS_CLOCK_INTERRUPT, record.error_word.spurious_clock),
            (AdcpPd0ParsedKey.LEVEL_7_INTERRUPT, record.error_word.level_7_interrupt),
            # COMPUTED VALUES
            (AdcpPd0ParsedKey.TIME_PER_PING_SECONDS, tpp_float_seconds),
            (AdcpPd0ParsedKey.MPT_SECONDS, mpt_seconds),
            # CORRELATION MAGNITUDES
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_ID, record.correlation_magnitudes.id),
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM1, record.correlation_magnitudes.beam1),
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM2, record.correlation_magnitudes.beam2),
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM3, record.correlation_magnitudes.beam3),
            (AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM4, record.correlation_magnitudes.beam4),
            # ECHO INTENSITIES
            (AdcpPd0ParsedKey.ECHO_INTENSITY_ID, record.echo_intensity.id),
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM1, record.echo_intensity.beam1),
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM2, record.echo_intensity.beam2),
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM3, record.echo_intensity.beam3),
            (AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM4, record.echo_intensity.beam4)])

        return [{DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value} for key, value in fields]


# ADCP System Configuration keys will be varied in VADCP
class AdcpSystemConfigurationKey(BaseEnum):
    # https://confluence.oceanobservatories.org/display/instruments/ADCP+Driver PS0
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
class AdcpSystemConfigurationDataParticle(DataParticle):
    _data_particle_type = WorkhorseDataParticleType.ADCP_SYSTEM_CONFIGURATION

    @staticmethod
    def regex():
        groups = [
            r'Instrument S/N: +(\w+)',
            r'Frequency: +(\d+) HZ',
            r'Configuration: +([\w, ]+)',
            r'Match Layer: +(\w+)',
            r'Beam Angle: +([\d.]+) DEGREES',
            r'Beam Pattern: +([\w]+)',
            r'Orientation: +([\w]+)',
            r'Sensor\(s\): +([\w ]+)',
            r'(Pressure Sens Coefficients:)?',
            r'(c3 = ([\+\-\d.E]+))?',
            r'(c2 = ([\+\-\d.E]+))?',
            r'(c1 = ([\+\-\d.E]+))?',
            r'(Offset = ([\+\-\d.E]+))?',
            r'Temp Sens Offset: +([\+\-\d.]+) degrees C',
            r'CPU Firmware: +([\w.\[\] ]+)',
            r'Boot Code Ver: +Required: +([\w.]+) +Actual: +([\w.]+)',
            r'DEMOD #1 Ver: +(\w+), Type: +(\w+)',
            r'DEMOD #2 Ver: +(\w+), Type: +(\w+)',
            r'PWRTIMG  Ver: +(\w+), Type: +(\w+)',
            r'Board Serial Number Data:',
            r'([\w\- ]+)',
            r'([\w\- ]+)',
            r'([\w\- ]+)',
            r'([\w\- ]+)',
            r'([\w\- ]+)?',
            r'([\w\- ]+)?',
            r'>'
        ]
        return r'\s*'.join(groups)

    @staticmethod
    def regex_compiled():
        return re.compile(AdcpSystemConfigurationDataParticle.regex())

    def _build_parsed_values(self):
        results = []
        match = self.regex_compiled().search(self.raw_data)
        key = AdcpSystemConfigurationKey
        results.append(self._encode_value(key.SERIAL_NUMBER, match.group(1), str))
        results.append(self._encode_value(key.TRANSDUCER_FREQUENCY, match.group(2), int))
        results.append(self._encode_value(key.CONFIGURATION, match.group(3), str))
        results.append(self._encode_value(key.MATCH_LAYER, match.group(4), str))
        results.append(self._encode_value(key.BEAM_ANGLE, match.group(5), int))
        results.append(self._encode_value(key.BEAM_PATTERN, match.group(6), str))
        results.append(self._encode_value(key.ORIENTATION, match.group(7), str))
        results.append(self._encode_value(key.SENSORS, match.group(8), str))
        if match.group(11):
            results.append(self._encode_value(key.PRESSURE_COEFF_c3, match.group(11), float))
        if match.group(13):
            results.append(self._encode_value(key.PRESSURE_COEFF_c2, match.group(13), float))
        if match.group(15):
            results.append(self._encode_value(key.PRESSURE_COEFF_c1, match.group(15), float))
        if match.group(17):
            results.append(self._encode_value(key.PRESSURE_COEFF_OFFSET, match.group(17), float))
        results.append(self._encode_value(key.TEMPERATURE_SENSOR_OFFSET, match.group(18), float))
        results.append(self._encode_value(key.CPU_FIRMWARE, match.group(19), str))
        results.append(self._encode_value(key.BOOT_CODE_REQUIRED, match.group(20), str))
        results.append(self._encode_value(key.BOOT_CODE_ACTUAL, match.group(21), str))
        results.append(self._encode_value(key.DEMOD_1_VERSION, match.group(22), str))
        results.append(self._encode_value(key.DEMOD_1_TYPE, match.group(23), str))
        results.append(self._encode_value(key.DEMOD_2_VERSION, match.group(24), str))
        results.append(self._encode_value(key.DEMOD_2_TYPE, match.group(25), str))
        results.append(self._encode_value(key.POWER_TIMING_VERSION, match.group(26), str))
        results.append(self._encode_value(key.POWER_TIMING_TYPE, match.group(27), str))
        results.append(self._encode_value(key.BOARD_SERIAL_NUMBERS,
                                          [match.group(28),
                                           match.group(29),
                                           match.group(30),
                                           match.group(31),
                                           match.group(32),
                                           match.group(33)], lambda y: ','.join(x.strip() for x in y if x)))

        return results


# AC command
class AdcpCompassCalibrationKey(BaseEnum):
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


class AdcpCompassCalibrationDataParticle(DataParticle):
    """
    ADCP Compass Calibration data particle
    """
    _data_particle_type = WorkhorseDataParticleType.ADCP_COMPASS_CALIBRATION

    RE01 = re.compile(r' +Calibration date and time: ([/\d: ]+)')
    RE04 = re.compile(r' +Bx +. +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) .')
    RE05 = re.compile(r' +By +. +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) .')
    RE06 = re.compile(r' +Bz +. +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) .')
    RE07 = re.compile(r' +Err +. +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) +([\deE\+\-.]+) .')

    RE11 = re.compile(r' +. +([\deE\+\-.]+) +.')
    RE12 = re.compile(r' +. +([\deE\+\-.]+) +.')
    RE13 = re.compile(r' +. +([\deE\+\-.]+) +.')
    RE14 = re.compile(r' +. +([\deE\+\-.]+) +.')

    RE18 = re.compile(r' +. ([\d.]+) .')
    RE21 = re.compile(r' +Calibration date and time: ([/\d: ]+)')
    RE22 = re.compile(r' +Average Temperature During Calibration was +([\d.]+) .')
    RE27 = re.compile(r' Roll +. +([\deE\+\-.]+) +([\deE\+\-.]+) +. +. +([\deE\+\-.]+) +([\deE\+\-.]+) +.')
    RE28 = re.compile(r' Pitch +. +([\deE\+\-.]+) +([\deE\+\-.]+) +. +. +([\deE\+\-.]+) +([\deE\+\-.]+) +.')
    RE32 = re.compile(r' Offset . +([\deE\+\-.]+) +([\deE\+\-.]+) +. +. +([\deE\+\-.]+) +([\deE\+\-.]+) +.')
    RE36 = re.compile(r' +Null +. (\d+) +.')

    def _build_parsed_values(self):
        # Initialize
        matches = {}
        lines = self.raw_data.split(NEWLINE)
        match = self.RE01.match(lines[1])
        timestamp = match.group(1)
        matches[AdcpCompassCalibrationKey.FLUXGATE_CALIBRATION_TIMESTAMP] = time.mktime(
            time.strptime(timestamp, "%m/%d/%Y  %H:%M:%S"))

        match = self.RE04.match(lines[4])
        matches[AdcpCompassCalibrationKey.S_INVERSE_BX] = [float(match.group(1)), float(match.group(2)),
                                                           float(match.group(3)), float(match.group(4))]
        match = self.RE05.match(lines[5])
        matches[AdcpCompassCalibrationKey.S_INVERSE_BY] = [float(match.group(1)), float(match.group(2)),
                                                           float(match.group(3)), float(match.group(4))]
        match = self.RE06.match(lines[6])
        matches[AdcpCompassCalibrationKey.S_INVERSE_BZ] = [float(match.group(1)), float(match.group(2)),
                                                           float(match.group(3)), float(match.group(4))]
        match = self.RE07.match(lines[7])
        matches[AdcpCompassCalibrationKey.S_INVERSE_ERR] = [float(match.group(1)), float(match.group(2)),
                                                            float(match.group(3)), float(match.group(4))]

        match = self.RE11.match(lines[11])
        matches[AdcpCompassCalibrationKey.COIL_OFFSET] = [float(match.group(1))]
        match = self.RE12.match(lines[12])
        matches[AdcpCompassCalibrationKey.COIL_OFFSET].append(float(match.group(1)))
        match = self.RE13.match(lines[13])
        matches[AdcpCompassCalibrationKey.COIL_OFFSET].append(float(match.group(1)))
        match = self.RE14.match(lines[14])
        matches[AdcpCompassCalibrationKey.COIL_OFFSET].append(float(match.group(1)))

        match = self.RE18.match(lines[18])
        matches[AdcpCompassCalibrationKey.ELECTRICAL_NULL] = float(match.group(1))

        match = self.RE21.match(lines[21])
        timestamp = match.group(1)
        matches[AdcpCompassCalibrationKey.TILT_CALIBRATION_TIMESTAMP] = time.mktime(
            time.strptime(timestamp, "%m/%d/%Y  %H:%M:%S"))

        match = self.RE22.match(lines[22])
        matches[AdcpCompassCalibrationKey.CALIBRATION_TEMP] = float(match.group(1))

        match = self.RE27.match(lines[27])
        matches[AdcpCompassCalibrationKey.ROLL_UP_DOWN] = [float(match.group(1)), float(match.group(2)),
                                                           float(match.group(3)), float(match.group(4))]
        match = self.RE28.match(lines[28])
        matches[AdcpCompassCalibrationKey.PITCH_UP_DOWN] = [float(match.group(1)), float(match.group(2)),
                                                            float(match.group(3)), float(match.group(4))]
        match = self.RE32.match(lines[32])
        matches[AdcpCompassCalibrationKey.OFFSET_UP_DOWN] = [float(match.group(1)), float(match.group(2)),
                                                             float(match.group(3)), float(match.group(4))]

        match = self.RE36.match(lines[36])
        matches[AdcpCompassCalibrationKey.TILT_NULL] = float(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result


# for keys for PT2 command
class AdcpAncillarySystemDataKey(BaseEnum):
    """
    Keys for PT2 command
    """
    ADCP_AMBIENT_CURRENT = "adcp_ambient_temp"
    ADCP_ATTITUDE_TEMP = "adcp_attitude_temp"
    ADCP_INTERNAL_MOISTURE = "adcp_internal_moisture"


# PT2 command data particle
class AdcpAncillarySystemDataParticle(DataParticle):
    """
    Data particle for PT2 command
    """
    _data_particle_type = WorkhorseDataParticleType.ADCP_ANCILLARY_SYSTEM_DATA

    RE01 = re.compile(r'Ambient +Temperature = +([\+\-\d.]+) Degrees C')
    RE02 = re.compile(r'Attitude Temperature = +([\+\-\d.]+) Degrees C')
    RE03 = re.compile(r'Internal Moisture    = +(\w+)')

    def _build_parsed_values(self):
        # Initialize
        matches = {}

        for key, regex, formatter in [
            (AdcpAncillarySystemDataKey.ADCP_AMBIENT_CURRENT, self.RE01, float),
            (AdcpAncillarySystemDataKey.ADCP_ATTITUDE_TEMP, self.RE02, float),
            (AdcpAncillarySystemDataKey.ADCP_INTERNAL_MOISTURE, self.RE03, lambda hexval: int(hexval[:-1], 16))
        ]:
            match = regex.search(self.raw_data)
            matches[key] = formatter(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result


# keys for PT4 command
class AdcpTransmitPathKey(BaseEnum):
    ADCP_TRANSIT_CURRENT = "adcp_transmit_current"
    ADCP_TRANSIT_VOLTAGE = "adcp_transmit_voltage"
    ADCP_TRANSIT_IMPEDANCE = "adcp_transmit_impedance"
    ADCP_TRANSIT_TEST_RESULT = "adcp_transmit_test_results"


# Data particle for PT4 command
class AdcpTransmitPathParticle(DataParticle):
    _data_particle_type = WorkhorseDataParticleType.ADCP_TRANSMIT_PATH

    RE01 = re.compile(r'IXMT += +([\+\-\d.]+) Amps')
    RE02 = re.compile(r'VXMT += +([\+\-\d.]+) Volts')
    RE03 = re.compile(r' +Z += +([\+\-\d.]+) Ohms')
    RE04 = re.compile(r'Transmit Test Results = +(.*)\r')

    def _build_parsed_values(self):
        # Initialize
        matches = {}
        for key, regex, formatter in [
            (AdcpTransmitPathKey.ADCP_TRANSIT_CURRENT, self.RE01, float),
            (AdcpTransmitPathKey.ADCP_TRANSIT_VOLTAGE, self.RE02, float),
            (AdcpTransmitPathKey.ADCP_TRANSIT_IMPEDANCE, self.RE03, float),
            (AdcpTransmitPathKey.ADCP_TRANSIT_TEST_RESULT, self.RE04, str),
        ]:
            match = regex.search(self.raw_data)
            matches[key] = formatter(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result
