"""
@package mi.instrument.teledyne.workhorse.adcp.driver
@file marine-integrations/mi/instrument/teledyne/workhorse/adcp/driver.py
@author Sung Ahn
@brief Driver for the ADCP
Release notes:

Generic Driver for ADCPS-K, ADCPS-I, ADCPT-B and ADCPT-DE
"""
import ntplib
import struct

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import re
import base64
import time
from datetime import datetime
from struct import unpack

from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()

from mi.core.common import Units, Prefixes
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.chunker import StringChunker
from mi.core.common import BaseEnum
from mi.core.time_tools import get_timestamp_delayed
from mi.core.exceptions import InstrumentParameterException, NotImplementedException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.driver_scheduler import DriverSchedulerConfigKey
from mi.core.driver_scheduler import TriggerType
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.util import dict_equal
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.instrument.teledyne.workhorse.adcp.pd0_parser import AdcpPd0Record
from mi.core.exceptions import SampleException


# default timeout.
TIMEOUT = 20

# newline.
NEWLINE = '\r\n'

DEFAULT_CMD_TIMEOUT = 20
DEFAULT_WRITE_DELAY = 0

ZERO_TIME_INTERVAL = '00:00:00'

BASE_YEAR = 2000


class Prompt(BaseEnum):
    """
    Device i/o prompts..
    """
    COMMAND = '\r\n>\r\n>'
    ERR = 'ERR:'
    BREAK = 'BREAK'


class EngineeringParameter(BaseEnum):
    # Engineering parameters for the scheduled commands
    CLOCK_SYNCH_INTERVAL = 'clockSynchInterval'
    GET_STATUS_INTERVAL = 'getStatusInterval'


class Parameter(DriverParameter):
    """
    Device parameters
    """
    #
    # set-able parameters
    #
    SERIAL_DATA_OUT = 'CD'  # 000 000 000 Serial Data Out (Vel;Cor;Amp PG;St;P0 P1;P2;P3)
    INSTRUMENT_ID = 'CI'  # Int 0-255
    XMIT_POWER = 'CQ'  # 0=Low, 255=High
    SPEED_OF_SOUND = 'EC'  # 1500  Speed Of Sound (m/s)
    SALINITY = 'ES'  # 35 (0-40 pp thousand)
    COORDINATE_TRANSFORMATION = 'EX'  #
    SENSOR_SOURCE = 'EZ'  # Sensor Source (C;D;H;P;R;S;T)
    TIME_PER_ENSEMBLE = 'TE'  # 01:00:00.00 (hrs:min:sec.sec/100)
    TIME_OF_FIRST_PING = 'TG'  # ****/**/**,**:**:** (CCYY/MM/DD,hh:mm:ss)
    TIME_PER_PING = 'TP'  # 00:00.20  (min:sec.sec/100)
    TIME = 'TT'  # 2013/02/26,05:28:23 (CCYY/MM/DD,hh:mm:ss)
    FALSE_TARGET_THRESHOLD = 'WA'  # 255,001 (Max)(0-255),Start Bin # <--------- TRICKY.... COMPLEX TYPE
    BANDWIDTH_CONTROL = 'WB'  # Bandwidth Control (0=Wid,1=Nar)
    CORRELATION_THRESHOLD = 'WC'  # 064  Correlation Threshold
    SERIAL_OUT_FW_SWITCHES = 'WD'  # 111100000  Data Out (Vel;Cor;Amp PG;St;P0 P1;P2;P3)
    ERROR_VELOCITY_THRESHOLD = 'WE'  # 5000  Error Velocity Threshold (0-5000 mm/s)
    BLANK_AFTER_TRANSMIT = 'WF'  # 0088  Blank After Transmit (cm)
    CLIP_DATA_PAST_BOTTOM = 'WI'  # 0 Clip Data Past Bottom (0=OFF,1=ON)
    RECEIVER_GAIN_SELECT = 'WJ'  # 1  Rcvr Gain Select (0=Low,1=High)
    NUMBER_OF_DEPTH_CELLS = 'WN'  # Number of depth cells (1-255)
    PINGS_PER_ENSEMBLE = 'WP'  # Pings per Ensemble (0-16384)
    DEPTH_CELL_SIZE = 'WS'  # 0800  Depth Cell Size (cm)
    TRANSMIT_LENGTH = 'WT'  # 0000 Transmit Length 0 to 3200(cm) 0 = Bin Length
    PING_WEIGHT = 'WU'  # 0 Ping Weighting (0=Box,1=Triangle)
    AMBIGUITY_VELOCITY = 'WV'  # 175 Mode 1 Ambiguity Vel (cm/s radial)

    #
    # Workhorse parameters
    #
    SERIAL_FLOW_CONTROL = 'CF'  # Flow Control
    BANNER = 'CH'  # Banner
    SLEEP_ENABLE = 'CL'  # SLEEP Enable
    SAVE_NVRAM_TO_RECORDER = 'CN'  # Save NVRAM to RECORD
    POLLED_MODE = 'CP'  # Polled Mode
    PITCH = 'EP'  # Pitch
    ROLL = 'ER'  # Roll

    LATENCY_TRIGGER = 'CX'  # Latency Trigger
    HEADING_ALIGNMENT = 'EA'  # Heading Alignment
    HEADING_BIAS = 'EB'  # Heading Bias
    DATA_STREAM_SELECTION = 'PD'  # Data Stream selection
    ENSEMBLE_PER_BURST = 'TC'  # Ensemble per Burst
    BUFFERED_OUTPUT_PERIOD = 'TX'  # Buffered Output Period
    SAMPLE_AMBIENT_SOUND = 'WQ'  # Sample Ambient sound
    TRANSDUCER_DEPTH = 'ED'  # Transducer Depth

    # Engineering parameters for the scheduled commands
    CLOCK_SYNCH_INTERVAL = EngineeringParameter.CLOCK_SYNCH_INTERVAL
    GET_STATUS_INTERVAL = EngineeringParameter.GET_STATUS_INTERVAL


class InstrumentCmds(BaseEnum):
    """
    Device specific commands
    Represents the commands the driver implements and the string that
    must be sent to the instrument to execute the command.
    """
    OUTPUT_CALIBRATION_DATA = 'AC'
    BREAK = 'break'  # < case sensitive!!!!
    SAVE_SETUP_TO_RAM = 'CK'
    FACTORY_SETS = 'CR1'  # Factory default set
    USER_SETS = 'CR0'  # User default set
    START_LOGGING = 'CS'
    CLEAR_ERROR_STATUS_WORD = 'CY0'  # May combine with next
    DISPLAY_ERROR_STATUS_WORD = 'CY1'  # May combine with prior
    CLEAR_FAULT_LOG = 'FC'
    GET_FAULT_LOG = 'FD'

    GET_SYSTEM_CONFIGURATION = 'PS0'
    RUN_TEST_200 = 'PT200'
    SET = 'set'  # leading spaces are OK. set is just PARAM_NAME next to VALUE
    GET = 'get '
    OUTPUT_PT2 = 'PT2'
    OUTPUT_PT4 = 'PT4'


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    DISCOVER = DriverEvent.DISCOVER

    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET_CALIBRATION = "PROTOCOL_EVENT_GET_CALIBRATION"
    GET_CONFIGURATION = "PROTOCOL_EVENT_GET_CONFIGURATION"
    SAVE_SETUP_TO_RAM = "PROTOCOL_EVENT_SAVE_SETUP_TO_RAM"
    GET_ERROR_STATUS_WORD = "PROTOCOL_EVENT_GET_ERROR_STATUS_WORD"
    CLEAR_ERROR_STATUS_WORD = "PROTOCOL_EVENT_CLEAR_ERROR_STATUS_WORD"
    GET_FAULT_LOG = "PROTOCOL_EVENT_GET_FAULT_LOG"
    CLEAR_FAULT_LOG = "PROTOCOL_EVENT_CLEAR_FAULT_LOG"
    RUN_TEST_200 = "PROTOCOL_EVENT_RUN_TEST_200"
    FACTORY_SETS = "FACTORY_DEFAULT_SETTINGS"
    USER_SETS = "USER_DEFAULT_SETTINGS"
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    PING_DRIVER = DriverEvent.PING_DRIVER

    # Different event because we don't want to expose this as a capability
    SCHEDULED_CLOCK_SYNC = 'PROTOCOL_EVENT_SCHEDULED_CLOCK_SYNC'
    SCHEDULED_GET_STATUS = 'PROTOCOL_EVENT_SCHEDULED_GET_STATUS'
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC

    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    RECOVER_AUTOSAMPLE = 'PROTOCOL_EVENT_RECOVER_AUTOSAMPLE'
    RESTORE_FACTORY_PARAMS = "PROTOCOL_EVENT_RESTORE_FACTORY_PARAMS"
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS  # The command will execute "AC, PT2, PT4"


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = ProtocolEvent.CLOCK_SYNC
    GET_CALIBRATION = ProtocolEvent.GET_CALIBRATION
    RUN_TEST_200 = ProtocolEvent.RUN_TEST_200
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS


class ScheduledJob(BaseEnum):
    CLOCK_SYNC = 'clock_sync'
    GET_CONFIGURATION = 'acquire_configuration'
    GET_CALIBRATION = 'acquire_calibration'


class ADCPUnits(Units):
    CDEGREE = '1/100 degree'
    DM = Prefixes.DECI + Units.METER
    MPERS = 'm/s'
    PPTHOUSAND = 'ppt'
    ENSEMBLEPERBURST = 'Ensembles Per Burst'
    CMPERSRADIAL = 'cm/s radial'
    TENTHMILLISECOND = '1/10 msec'


class ADCPDescription(BaseEnum):
    INTERVALTIME = 'hh:mm:ss'
    INTERVALTIMEHundredth = 'hh:mm:ss.ss/100'
    DATETIME = 'CCYY/MM/DD,hh:mm:ss'
    PINGTIME = "mm:ss.ss/100"
    SETTIME = 'CCYY/MM/DD,hh:mm:ss'
    SERIALDATAOUT = 'Vel Cor Amp'
    FLOWCONTROL = 'BITS: EnsCyc PngCyc Binry Ser Rec'
    SLEEP = '0 = Disable, 1 = Enable, 2 See Manual'
    XMTPOWER = 'XMT Power 0-255'
    TRUEON = 'False=OFF,True=ON'
    TRUEOFF = "False=ON,True=OFF"


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

    def _build_parsed_values(self):
        """
        Parse the base portion of the particle
        """
        if "[BREAK Wakeup A]" in self.raw_data:
            raise SampleException("BREAK found; likely partial sample while escaping autosample mode.")

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

        mpt_seconds = float(record.variable_data.mpt_seconds + (record.variable_data.mpt_hundredths / 100))
        rtc_time = time.mktime(dts.timetuple()) + record.variable_data.rtc_y2k_hundredths / 100.0
        self.set_internal_timestamp(unix_time=rtc_time)

        ensemble_time = ntplib.system_to_ntp_time(rtc_time)

        fields = [(AdcpPd0ParsedKey.ENSEMBLE_START_TIME, ensemble_time),
                  (AdcpPd0ParsedKey.CHECKSUM, record.stored_checksum),
                  (AdcpPd0ParsedKey.OFFSET_DATA_TYPES, record.offsets),
                  (AdcpPd0ParsedKey.REAL_TIME_CLOCK, (record.variable_data.rtc_y2k_century,
                                                      record.variable_data.rtc_y2k_year,
                                                      record.variable_data.rtc_y2k_month,
                                                      record.variable_data.rtc_y2k_day,
                                                      record.variable_data.rtc_y2k_hour,
                                                      record.variable_data.rtc_y2k_minute,
                                                      record.variable_data.rtc_y2k_seconds))]

        if record.coord_transform.coord_transform == Pd0CoordinateTransformType.BEAM:
            self._data_particle_type = DataParticleType.ADCP_PD0_PARSED_BEAM
            if self._slave:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_PARSED_BEAM
            elif self._master:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_BEAM_PARSED

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
            self._data_particle_type = DataParticleType.ADCP_PD0_PARSED_EARTH
            if self._slave:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_PARSED_EARTH
            elif self._master:
                self._data_particle_type = VADCPDataParticleType.VADCP_PD0_EARTH_PARSED

            fields.extend([
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
        matches[AdcpSystemConfigurationKey.SERIAL_NUMBER] = str(match.group(1))
        match = self.RE01.match(lines[1])
        matches[AdcpSystemConfigurationKey.TRANSDUCER_FREQUENCY] = int(match.group(1))
        match = self.RE02.match(lines[2])
        matches[AdcpSystemConfigurationKey.CONFIGURATION] = match.group(1)
        match = self.RE03.match(lines[3])
        matches[AdcpSystemConfigurationKey.MATCH_LAYER] = match.group(1)
        match = self.RE04.match(lines[4])
        matches[AdcpSystemConfigurationKey.BEAM_ANGLE] = int(match.group(1))
        match = self.RE05.match(lines[5])
        matches[AdcpSystemConfigurationKey.BEAM_PATTERN] = match.group(1)
        match = self.RE06.match(lines[6])
        matches[AdcpSystemConfigurationKey.ORIENTATION] = match.group(1)
        match = self.RE07.match(lines[7])
        matches[AdcpSystemConfigurationKey.SENSORS] = match.group(1)

        # Only available for ADCP and VADCP master
        if not self._slave:
            match = self.RE09.match(lines[9 - self._offset])
            matches[AdcpSystemConfigurationKey.PRESSURE_COEFF_c3] = float(match.group(1))
            match = self.RE10.match(lines[10 - self._offset])
            matches[AdcpSystemConfigurationKey.PRESSURE_COEFF_c2] = float(match.group(1))
            match = self.RE11.match(lines[11 - self._offset])
            matches[AdcpSystemConfigurationKey.PRESSURE_COEFF_c1] = float(match.group(1))
            match = self.RE12.match(lines[12 - self._offset])
            matches[AdcpSystemConfigurationKey.PRESSURE_COEFF_OFFSET] = float(match.group(1))

        match = self.RE14.match(lines[14 - self._offset])
        matches[AdcpSystemConfigurationKey.TEMPERATURE_SENSOR_OFFSET] = float(match.group(1))
        match = self.RE16.match(lines[16 - self._offset])
        matches[AdcpSystemConfigurationKey.CPU_FIRMWARE] = match.group(1)
        match = self.RE17.match(lines[17 - self._offset])
        matches[AdcpSystemConfigurationKey.BOOT_CODE_REQUIRED] = match.group(1)
        matches[AdcpSystemConfigurationKey.BOOT_CODE_ACTUAL] = match.group(2)
        match = self.RE18.match(lines[18 - self._offset])
        matches[AdcpSystemConfigurationKey.DEMOD_1_VERSION] = match.group(1)
        matches[AdcpSystemConfigurationKey.DEMOD_1_TYPE] = match.group(2)
        match = self.RE19.match(lines[19 - self._offset])
        matches[AdcpSystemConfigurationKey.DEMOD_2_VERSION] = match.group(1)
        matches[AdcpSystemConfigurationKey.DEMOD_2_TYPE] = match.group(2)
        match = self.RE20.match(lines[20 - self._offset])
        matches[AdcpSystemConfigurationKey.POWER_TIMING_VERSION] = match.group(1)
        matches[AdcpSystemConfigurationKey.POWER_TIMING_TYPE] = match.group(2)
        match = self.RE23.match(lines[23 - self._offset])

        matches[AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS] = str(match.group(1)) + "\n"
        match = self.RE24.match(lines[24 - self._offset])
        matches[AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"
        match = self.RE25.match(lines[25 - self._offset])
        matches[AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"
        match = self.RE26.match(lines[26 - self._offset])
        matches[AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"

        # Only available for ADCP
        if not self._slave and not self._master:
            match = self.RE27.match(lines[27 - self._offset])
            matches[AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS] += str(match.group(1)) + "\n"
            match = self.RE28.match(lines[28 - self._offset])
            matches[AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS] += str(match.group(1))

        result = []
        for key, value in matches.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})
        return result


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
    _data_particle_type = DataParticleType.ADCP_ANCILLARY_SYSTEM_DATA

    RE01 = re.compile(r'Ambient  Temperature = +([\+\-0-9.]+) Degrees C')
    RE02 = re.compile(r'Attitude Temperature = +([\+\-0-9.]+) Degrees C')
    RE03 = re.compile(r'Internal Moisture    = +([a-zA-Z0-9]+)')

    def _build_parsed_values(self):
        # Initialize
        matches = {}

        for key, regex, formatter in [
            (AdcpAncillarySystemDataKey.ADCP_AMBIENT_CURRENT, self.RE01, float),
            (AdcpAncillarySystemDataKey.ADCP_ATTITUDE_TEMP, self.RE02, float),
            (AdcpAncillarySystemDataKey.ADCP_INTERNAL_MOISTURE, self.RE03, str),
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
    _data_particle_type = DataParticleType.ADCP_TRANSMIT_PATH

    RE01 = re.compile(r'IXMT += +([\+\-0-9.]+) Amps')
    RE02 = re.compile(r'VXMT += +([\+\-0-9.]+) Volts')
    RE03 = re.compile(r' +Z += +([\+\-0-9.]+) Ohms')
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


class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    Specialization for this version of the workhorse ADCP driver
    """

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


class Protocol(CommandResponseInstrumentProtocol):
    """
    Specialization for this version of the workhorse driver
    """
    __metaclass__ = get_logging_metaclass()

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        # Build ADCPT protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(ProtocolState, ProtocolEvent, ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        handlers = {
            ProtocolState.UNKNOWN: (
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.EXIT, self._handler_unknown_exit),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ),
            ProtocolState.COMMAND: (
                (ProtocolEvent.ENTER, self._handler_command_enter),
                (ProtocolEvent.EXIT, self._handler_command_exit),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample),
                (ProtocolEvent.SET, self._handler_command_set),
                (ProtocolEvent.CLOCK_SYNC, self._handler_command_clock_sync),
                (ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_command_clock_sync),
                (ProtocolEvent.SCHEDULED_GET_STATUS, self._handler_command_get_status),
                (ProtocolEvent.GET_CALIBRATION, self._handler_command_get_calibration),
                (ProtocolEvent.GET_CONFIGURATION, self._handler_command_get_configuration),
                (ProtocolEvent.SAVE_SETUP_TO_RAM, self._handler_command_save_setup_to_ram),
                (ProtocolEvent.START_DIRECT, self._handler_command_start_direct),
                (ProtocolEvent.RUN_TEST_200, self._handler_command_run_test_200),
                (ProtocolEvent.FACTORY_SETS, self._handler_command_factory_sets),
                (ProtocolEvent.USER_SETS, self._handler_command_user_sets),
                (ProtocolEvent.GET_ERROR_STATUS_WORD, self._handler_command_acquire_error_status_word),
                (ProtocolEvent.CLEAR_ERROR_STATUS_WORD, self._handler_command_clear_error_status_word),
                (ProtocolEvent.CLEAR_FAULT_LOG, self._handler_command_clear_fault_log),
                (ProtocolEvent.GET_FAULT_LOG, self._handler_command_display_fault_log),
                (ProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status),
                (ProtocolEvent.RECOVER_AUTOSAMPLE, self._handler_recover_autosample),
            ),
            ProtocolState.AUTOSAMPLE: (
                (ProtocolEvent.ENTER, self._handler_autosample_enter),
                (ProtocolEvent.EXIT, self._handler_autosample_exit),
                (ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_autosample_clock_sync),
                (ProtocolEvent.SCHEDULED_GET_STATUS, self._handler_autosample_get_status),
                (ProtocolEvent.GET_CALIBRATION, self._handler_autosample_get_calibration),
                (ProtocolEvent.GET_CONFIGURATION, self._handler_autosample_get_configuration),
            ),
            ProtocolState.DIRECT_ACCESS: (
                (ProtocolEvent.ENTER, self._handler_direct_access_enter),
                (ProtocolEvent.EXIT, self._handler_direct_access_exit),
                (ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct),
                (ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct),
            )
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Build dictionaries for driver schema
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        # Add build handlers for device commands.
        self._add_build_handler(InstrumentCmds.OUTPUT_CALIBRATION_DATA, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.SAVE_SETUP_TO_RAM, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.START_LOGGING, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.DISPLAY_ERROR_STATUS_WORD, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.CLEAR_ERROR_STATUS_WORD, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.CLEAR_FAULT_LOG, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.GET_FAULT_LOG, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.GET_SYSTEM_CONFIGURATION, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.RUN_TEST_200, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.FACTORY_SETS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.USER_SETS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.SET, self._build_set_command)
        self._add_build_handler(InstrumentCmds.GET, self._build_get_command)
        self._add_build_handler(InstrumentCmds.OUTPUT_PT2, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.OUTPUT_PT4, self._build_simple_command)
        # Add response handlers
        self._add_response_handler(InstrumentCmds.OUTPUT_CALIBRATION_DATA,
                                   self._parse_output_calibration_data_response)
        self._add_response_handler(InstrumentCmds.SAVE_SETUP_TO_RAM, self._parse_save_setup_to_ram_response)
        self._add_response_handler(InstrumentCmds.CLEAR_ERROR_STATUS_WORD,
                                   self._parse_clear_error_status_response)
        self._add_response_handler(InstrumentCmds.DISPLAY_ERROR_STATUS_WORD, self._parse_error_status_response)
        self._add_response_handler(InstrumentCmds.CLEAR_FAULT_LOG, self._parse_clear_fault_log_response)
        self._add_response_handler(InstrumentCmds.GET_FAULT_LOG, self._parse_fault_log_response)
        self._add_response_handler(InstrumentCmds.GET_SYSTEM_CONFIGURATION,
                                   self._parse_get_system_configuration)
        self._add_response_handler(InstrumentCmds.RUN_TEST_200, self._parse_test_response)

        self._add_response_handler(InstrumentCmds.FACTORY_SETS, self._parse_factory_set_response)
        self._add_response_handler(InstrumentCmds.USER_SETS, self._parse_user_set_response)

        self._add_response_handler(InstrumentCmds.SET, self._parse_set_response)
        self._add_response_handler(InstrumentCmds.GET, self._parse_get_response)

        self._add_response_handler(InstrumentCmds.OUTPUT_PT2, self._parse_output_calibration_data_response)
        self._add_response_handler(InstrumentCmds.OUTPUT_PT4, self._parse_output_calibration_data_response)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # commands sent sent to device to be
        # filtered in responses for telnet DA
        self._sent_cmds = []
        self.disable_autosample_recover = False
        self._chunker = StringChunker(self.sieve_function)
        self.initialize_scheduler()

    def _build_param_dict(self):
        self._param_dict.add(Parameter.SERIAL_DATA_OUT,
                             r'CD = (\d\d\d \d\d\d \d\d\d) \-+ Serial Data Out ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Serial Data Out",
                             value_description=ADCPDescription.SERIALDATAOUT,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value='000 000 000')

        self._param_dict.add(Parameter.SERIAL_FLOW_CONTROL,
                             r'CF = (\d+) \-+ Flow Ctrl ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Serial Flow Control",
                             value_description=ADCPDescription.FLOWCONTROL,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value='11110')

        self._param_dict.add(Parameter.BANNER,
                             r'CH = (\d) \-+ Suppress Banner',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Banner",
                             value_description=ADCPDescription.TRUEON,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=False)

        self._param_dict.add(Parameter.INSTRUMENT_ID,
                             r'CI = (\d+) \-+ Instrument ID ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Instrument id",
                             direct_access=True,
                             startup_param=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=0)

        self._param_dict.add(Parameter.SLEEP_ENABLE,
                             r'CL = (\d) \-+ Sleep Enable',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Sleep enable",
                             value_description=ADCPDescription.SLEEP,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=False)

        self._param_dict.add(Parameter.SAVE_NVRAM_TO_RECORDER,
                             r'CN = (\d) \-+ Save NVRAM to recorder',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Save nvram to recorder",
                             value_description=ADCPDescription.TRUEOFF,
                             startup_param=True,
                             default_value=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)

        self._param_dict.add(Parameter.POLLED_MODE,
                             r'CP = (\d) \-+ PolledMode ',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Polled Mode",
                             value_description=ADCPDescription.TRUEON,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=False)

        self._param_dict.add(Parameter.XMIT_POWER,
                             r'CQ = (\d+) \-+ Xmt Power ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Xmit Power",
                             startup_param=True,
                             value_description=ADCPDescription.XMTPOWER,
                             direct_access=True,
                             default_value=255)

        self._param_dict.add(Parameter.LATENCY_TRIGGER,
                             r'CX = (\d) \-+ Trigger Enable ',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Latency trigger",
                             value_description=ADCPDescription.TRUEON,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=False)

        self._param_dict.add(Parameter.HEADING_ALIGNMENT,
                             r'EA = ([+-]\d+) \-+ Heading Alignment',
                             lambda match: int(match.group(1)),
                             lambda value: '%+06d' % value,
                             type=ParameterDictType.INT,
                             display_name="Heading alignment",
                             units=ADCPUnits.CDEGREE,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             direct_access=True,
                             startup_param=True,
                             default_value=+00000)

        self._param_dict.add(Parameter.HEADING_BIAS,
                             r'EB = ([+-]\d+) \-+ Heading Bias',
                             lambda match: int(match.group(1)),
                             lambda value: '%+06d' % value,
                             type=ParameterDictType.INT,
                             display_name="Heading Bias",
                             units=ADCPUnits.CDEGREE,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=+00000)

        self._param_dict.add(Parameter.SPEED_OF_SOUND,
                             r'EC = (\d+) \-+ Speed Of Sound',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Speed of Sound",
                             units=ADCPUnits.MPERS,
                             startup_param=True,
                             direct_access=True,
                             default_value=1485)

        self._param_dict.add(Parameter.TRANSDUCER_DEPTH,
                             r'ED = (\d+) \-+ Transducer Depth ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Transducer Depth",
                             units=ADCPUnits.DM,
                             startup_param=True,
                             direct_access=True,
                             default_value=8000)

        self._param_dict.add(Parameter.PITCH,
                             r'EP = ([\+\-\d]+) \-+ Tilt 1 Sensor ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Pitch",
                             units=ADCPUnits.CDEGREE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.ROLL,
                             r'ER = ([\+\-\d]+) \-+ Tilt 2 Sensor ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Roll",
                             units=ADCPUnits.CDEGREE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.SALINITY,
                             r'ES = (\d+) \-+ Salinity ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Salinity",
                             units=ADCPUnits.PPTHOUSAND,
                             startup_param=True,
                             direct_access=True,
                             default_value=35)

        self._param_dict.add(Parameter.COORDINATE_TRANSFORMATION,
                             r'EX = (\d+) \-+ Coord Transform ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Coordinate transformation",
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value='00111')

        self._param_dict.add(Parameter.SENSOR_SOURCE,
                             r'EZ = (\d+) \-+ Sensor Source ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Sensor source",
                             startup_param=True,
                             direct_access=True,
                             default_value='1111101')

        self._param_dict.add(Parameter.DATA_STREAM_SELECTION,
                             r'PD = (\d+) \-+ Data Stream Select',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Data Stream Selection",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.ENSEMBLE_PER_BURST,
                             r'TC (\d+) \-+ Ensembles Per Burst',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Ensemble per burst",
                             units=ADCPUnits.ENSEMBLEPERBURST,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.TIME_PER_ENSEMBLE,
                             r'TE (\d\d:\d\d:\d\d.\d\d) \-+ Time per Ensemble ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time per ensemble",
                             value_description=ADCPDescription.INTERVALTIMEHundredth,
                             startup_param=True,
                             direct_access=True,
                             default_value='00:00:00.00')

        self._param_dict.add(Parameter.TIME_OF_FIRST_PING,
                             r'TG (..../../..,..:..:..) - Time of First Ping ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time of first ping",
                             value_description=ADCPDescription.DATETIME,
                             startup_param=False,
                             direct_access=False,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.TIME_PER_PING,
                             r'TP (\d\d:\d\d.\d\d) \-+ Time per Ping',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time per ping",
                             value_description=ADCPDescription.PINGTIME,
                             startup_param=True,
                             direct_access=True,
                             default_value='00:01.00')

        self._param_dict.add(Parameter.TIME,
                             r'TT (\d\d\d\d/\d\d/\d\d,\d\d:\d\d:\d\d) \- Time Set ',
                             lambda match: str(match.group(1) + " UTC"),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time",
                             value_description=ADCPDescription.SETTIME,
                             expiration=86400)  # expire once per day 60 * 60 * 24

        self._param_dict.add(Parameter.BUFFERED_OUTPUT_PERIOD,
                             r'TX (\d\d:\d\d:\d\d) \-+ Buffer Output Period:',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Buffered output period",
                             value_description=ADCPDescription.INTERVALTIME,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value='00:00:00')

        self._param_dict.add(Parameter.FALSE_TARGET_THRESHOLD,
                             r'WA (\d+,\d+) \-+ False Target Threshold ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="False Target Threshold",
                             startup_param=True,
                             direct_access=True,
                             default_value='050,001')

        self._param_dict.add(Parameter.BANDWIDTH_CONTROL,
                             r'WB (\d) \-+ Bandwidth Control ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Bandwidth Control",
                             value_description="0=Wid,1=Nar",
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.CORRELATION_THRESHOLD,
                             r'WC (\d+) \-+ Correlation Threshold',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Correlation threshold",
                             startup_param=True,
                             direct_access=True,
                             default_value=64)

        self._param_dict.add(Parameter.SERIAL_OUT_FW_SWITCHES,
                             r'WD ([\d ]+) \-+ Data Out ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Serial Out fw Switches",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value='111100000')

        self._param_dict.add(Parameter.ERROR_VELOCITY_THRESHOLD,
                             r'WE (\d+) \-+ Error Velocity Threshold',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Error Velocity Threshold",
                             units=ADCPUnits.MPERS,
                             startup_param=True,
                             direct_access=True,
                             default_value=2000)

        self._param_dict.add(Parameter.BLANK_AFTER_TRANSMIT,
                             r'WF (\d+) \-+ Blank After Transmit',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Blank After Transmit",
                             units=ADCPUnits.CENTIMETER,
                             startup_param=True,
                             direct_access=True,
                             default_value=704)

        self._param_dict.add(Parameter.CLIP_DATA_PAST_BOTTOM,
                             r'WI (\d) \-+ Clip Data Past Bottom',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Clip Data Past Bottom",
                             value_description=ADCPDescription.TRUEON,
                             startup_param=True,
                             direct_access=True,
                             default_value=False)

        self._param_dict.add(Parameter.RECEIVER_GAIN_SELECT,
                             r'WJ (\d) \-+ Rcvr Gain Select \(0=Low,1=High\)',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Receiver Gain Select",
                             value_description="0=Low,1=High",
                             startup_param=True,
                             direct_access=True,
                             default_value=1)

        self._param_dict.add(Parameter.NUMBER_OF_DEPTH_CELLS,
                             r'WN (\d+) \-+ Number of depth cells',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Number of Depth Cells",
                             startup_param=True,
                             direct_access=True,
                             default_value=100)

        self._param_dict.add(Parameter.PINGS_PER_ENSEMBLE,
                             r'WP (\d+) \-+ Pings per Ensemble ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Pings Per Ensemble",
                             startup_param=True,
                             direct_access=True,
                             default_value=1)

        self._param_dict.add(Parameter.SAMPLE_AMBIENT_SOUND,
                             r'WQ (\d) \-+ Sample Ambient Sound',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Sample Ambient Sound",
                             value_description=ADCPDescription.TRUEON,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=False)

        self._param_dict.add(Parameter.DEPTH_CELL_SIZE,
                             r'WS (\d+) \-+ Depth Cell Size \(cm\)',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Depth Cell Size",
                             units=ADCPUnits.CENTIMETER,
                             startup_param=True,
                             direct_access=True,
                             default_value=800)

        self._param_dict.add(Parameter.TRANSMIT_LENGTH,
                             r'WT (\d+) \-+ Transmit Length ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Transmit Length",
                             units=ADCPUnits.CENTIMETER,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.PING_WEIGHT,
                             r'WU (\d) \-+ Ping Weighting ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Ping Weight",
                             value_description="0=Box,1=Triangle",
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.AMBIGUITY_VELOCITY,
                             r'WV (\d+) \-+ Mode 1 Ambiguity Vel ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Ambiguity Velocity",
                             units=ADCPUnits.CMPERSRADIAL,
                             startup_param=True,
                             direct_access=True,
                             default_value=175)

        # Engineering parameters
        self._param_dict.add(Parameter.CLOCK_SYNCH_INTERVAL,
                             r'BOGUS',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Clock synch interval",
                             value_description=ADCPDescription.INTERVALTIME,
                             startup_param=True,
                             direct_access=False,
                             default_value="00:00:00")

        self._param_dict.add(Parameter.GET_STATUS_INTERVAL,
                             r'BOGUS',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Get status interval",
                             value_description=ADCPDescription.INTERVALTIME,
                             startup_param=True,
                             direct_access=False,
                             default_value="00:00:00")

        self._param_dict.set_default(Parameter.CLOCK_SYNCH_INTERVAL)
        self._param_dict.set_default(Parameter.GET_STATUS_INTERVAL)

    @staticmethod
    def sieve_function(raw_data):
        """
        Chunker sieve method to help the chunker identify chunks.
        @returns a list of chunks identified, if any.
        The chunks are all the same type.
        """
        sieve_matchers = [ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                          ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                          ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                          ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                          ADCP_PD0_PARSED_REGEX_MATCHER]

        return_list = []

        for matcher in sieve_matchers:
            if matcher == ADCP_PD0_PARSED_REGEX_MATCHER:
                #
                # Have to cope with variable length binary records...
                # lets grab the length, then write a proper query to
                # snag it.
                #
                matcher2 = re.compile(r'\x7f\x7f(..)', re.DOTALL)
                for match in matcher2.finditer(raw_data):
                    length = struct.unpack('<H', match.group(1))[0]
                    end_index = match.start() + length
                    # read the checksum and compute our own
                    # if they match we have a PD0 record
                    if len(raw_data) > end_index+1:
                        checksum = struct.unpack_from('<H', raw_data, end_index)[0]
                        calculated = sum(bytearray(raw_data[match.start():end_index])) & 0xffff
                        if checksum == calculated:
                            # include the checksum in our match... (2 bytes)
                            return_list.append((match.start(), end_index+2))
            else:
                for match in matcher.finditer(raw_data):
                    return_list.append((match.start(), match.end()))

        return return_list

    def _build_command_dict(self):
        """
        Build command dictionary
        """
        self._cmd_dict.add(Capability.START_AUTOSAMPLE,
                           display_name="Start Autosample",
                           description="Place the instrument into autosample mode")
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE,
                           display_name="Stop Autosample",
                           description="Exit autosample mode and return to command mode")
        self._cmd_dict.add(Capability.CLOCK_SYNC,
                           display_name="Sync Clock")
        self._cmd_dict.add(Capability.GET_CALIBRATION,
                           display_name="Get Calibration")
        self._cmd_dict.add(Capability.RUN_TEST_200,
                           display_name="Run Test 200")
        self._cmd_dict.add(Capability.ACQUIRE_STATUS,
                           display_name="Acquire Status")

    # #######################################################################
    # Private helpers.
    # #######################################################################
    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """
        if self._extract_sample(AdcpCompassCalibrationDataParticle,
                                ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                                chunk,
                                timestamp):
            log.debug("_got_chunk - successful match for ADCP_COMPASS_CALIBRATION_DataParticle")

        elif self._extract_sample(AdcpPd0ParsedDataParticle,
                                  ADCP_PD0_PARSED_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.debug("_got_chunk - successful match for ADCP_PD0_PARSED_DataParticle")

        elif self._extract_sample(AdcpSystemConfigurationDataParticle,
                                  ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.debug("_got_chunk - successful match for ADCP_SYSTEM_CONFIGURATION_DataParticle")

        elif self._extract_sample(AdcpAncillarySystemDataParticle,
                                  ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.trace("_got_chunk - successful match for ADCP_ANCILLARY_SYSTEM_DATA_PARTICLE")

        elif self._extract_sample(AdcpTransmitPathParticle,
                                  ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.trace("_got_chunk - successful match for ADCP_TRANSMIT_PATH_PARTICLE")

    def _send_break_cmd(self, delay):
        """
        Send a BREAK to attempt to wake the device.
        """
        self._connection.send_break(delay)

    def stop_scheduled_job(self, schedule_job):
        """
        Remove the scheduled job
        @param schedule_job scheduling job.
        """
        log.debug("Attempting to remove the scheduler")
        if self._scheduler is not None:
            try:
                self._remove_scheduler(schedule_job)
                log.debug("successfully removed scheduler")
            except KeyError:
                log.debug("_remove_scheduler could not find %s", schedule_job)

    def start_scheduled_job(self, param, schedule_job, protocol_event):
        """
        Add a scheduled job
        """
        self.stop_scheduled_job(schedule_job)

        interval = self._param_dict.get(param).split(':')
        hours = interval[0]
        minutes = interval[1]
        seconds = interval[2]
        log.debug("Setting scheduled interval to: %s %s %s", hours, minutes, seconds)

        if hours == '00' and minutes == '00' and seconds == '00':
            # if interval is all zeroed, then stop scheduling jobs
            self.stop_scheduled_job(schedule_job)
        else:
            config = {
                DriverConfigKey.SCHEDULER: {
                    schedule_job: {
                        DriverSchedulerConfigKey.TRIGGER: {
                            DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.INTERVAL,
                            DriverSchedulerConfigKey.HOURS: int(hours),
                            DriverSchedulerConfigKey.MINUTES: int(minutes),
                            DriverSchedulerConfigKey.SECONDS: int(seconds)
                        }
                    }
                }
            }
            self.set_init_params(config)
            self._add_scheduler_event(schedule_job, protocol_event)

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    def _sync_clock(self, command, date_time_param, timeout=TIMEOUT, delay=1, time_format="%d %b %Y %H:%M:%S"):
        """
        Send the command to the instrument to synchronize the clock
        @param command set command
        @param date_time_param: date time parameter that we want to set
        @param timeout: command timeout
        @param delay: wakeup delay
        @param time_format: time format string for set command
        """
        str_val = get_timestamp_delayed(time_format)
        self._do_cmd_direct(date_time_param + str_val + NEWLINE)
        time.sleep(1)
        self._get_response(TIMEOUT)

    # #######################################################################
    # Startup parameter handlers
    ########################################################################
    def apply_startup_params(self):
        """
        Apply all startup parameters.  First we check the instrument to see
        if we need to set the parameters.  If they are they are set
        correctly then we don't do anything.

        If we need to set parameters then we might need to transition to
        command first.  Then we will transition back when complete.

        @throws: InstrumentProtocolException if not in command or streaming
        """
        # Let's give it a try in unknown state
        if self.get_current_state() not in [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]:
            raise InstrumentProtocolException("Not in command or autosample state. Unable to apply startup params")

        logging = self._is_logging()
        # If we are in streaming mode and our configuration on the
        # instrument matches what we think it should be then we
        # don't need to do anything.

        if not self._instrument_config_dirty():
            return True

        error = None

        try:
            if logging:
                # Switch to command mode,
                self._stop_logging()

            config = self.get_startup_config()
            # Pass true to _set_params so we know these are startup values
            self._set_params(config, True)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            log.error("EXCEPTION WAS " + str(e))
            error = e

        finally:
            # Switch back to streaming
            if logging:
                log.debug("GOING BACK INTO LOGGING")
                my_state = self._protocol_fsm.get_current_state()
                log.trace("current_state = %s", my_state)
                self._start_logging()

        if error:
            raise error

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary.
        """
        logging = self._is_logging()
        results = []

        # see if we passed in a list of parameters to query
        # if not, use the whole parameter list
        parameters = kwargs.get('params')
        if parameters is None:
            parameters = Parameter.list()
        # filter out the engineering parameters and ALL
        parameters = [p for p in parameters if not EngineeringParameter.has(p) and p != Parameter.ALL]

        try:
            if logging:
                # Switch to command mode,
                self._stop_logging()

            # Get old param dict config.
            old_config = self._param_dict.get_config()

            # Clear out the linebuffer
            # Send ALL get commands sequentially, then grab them all at once
            self._linebuf = ''

            if parameters:
                command = ''.join(['%s?%s' % (p, NEWLINE) for p in parameters])
                self._do_cmd_direct(command)

                resp = self._get_response(response_regex=re.compile(r'(%s.*?%s.*?>)'
                                                                    % (parameters[0], parameters[-1]), re.DOTALL))
                self._param_dict.update(resp)

            new_config = self._param_dict.get_config()

            # Check if there is any changes. Ignore TT
            if not dict_equal(new_config, old_config, ['TT']) or kwargs.get('force'):
                self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        finally:
            # Switch back to streaming
            if logging:
                self._start_logging()

        return NEWLINE.join(results)

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        result = None
        self._verify_not_readonly(*args, **kwargs)
        params = args[0]

        old_config = self._param_dict.get_config()

        for key, val in params.iteritems():
            if key.find('_') == -1:  # Not found, Master parameters
                if not EngineeringParameter.has(key) and val != old_config.get(key):
                    result = self._do_cmd_resp(InstrumentCmds.SET, key, val, **kwargs)

        # Handle engineering parameters
        changed = False

        if Parameter.CLOCK_SYNCH_INTERVAL in params:
            if (params[Parameter.CLOCK_SYNCH_INTERVAL] != self._param_dict.get(
                    Parameter.CLOCK_SYNCH_INTERVAL)):
                self._param_dict.set_value(Parameter.CLOCK_SYNCH_INTERVAL,
                                           params[Parameter.CLOCK_SYNCH_INTERVAL])
                self.start_scheduled_job(Parameter.CLOCK_SYNCH_INTERVAL, ScheduledJob.CLOCK_SYNC,
                                         ProtocolEvent.SCHEDULED_CLOCK_SYNC)
                changed = True

        if Parameter.GET_STATUS_INTERVAL in params:
            if (params[Parameter.GET_STATUS_INTERVAL] != self._param_dict.get(
                    Parameter.GET_STATUS_INTERVAL)):
                self._param_dict.set_value(Parameter.GET_STATUS_INTERVAL,
                                           params[Parameter.GET_STATUS_INTERVAL])
                self.start_scheduled_job(Parameter.GET_STATUS_INTERVAL,
                                         ScheduledJob.GET_CONFIGURATION,
                                         ProtocolEvent.SCHEDULED_GET_STATUS)
                changed = True

        self._update_params(params=params.keys(), force=changed)
        return result

    def _send_break(self, duration=3000):
        """
        Send a BREAK to attempt to wake the device.
        """
        self._linebuf = ''
        self._send_break_cmd(duration)
        self._get_response(expected_prompt=Prompt.BREAK)

    def _send_wakeup(self):
        """
        Send a newline to attempt to wake the device.
        """
        self._connection.send(NEWLINE)

    def _instrument_config_dirty(self):
        """
        Read the startup config and compare that to what the instrument
        is configured too.  If they differ then return True
        @return: True if the startup config doesn't match the instrument
        @throws: InstrumentParameterException
        """
        log.trace("in _instrument_config_dirty")
        # Refresh the param dict cache
        # self._update_params()

        startup_params = self._param_dict.get_startup_list()
        log.trace("Startup Parameters: %s" % startup_params)

        for param in startup_params:
            if not Parameter.has(param):
                raise InstrumentParameterException("A param is unknown")

            if self._param_dict.get(param) != self._param_dict.get_config_value(param):
                log.trace("DIRTY: %s %s != %s" % (
                    param, self._param_dict.get(param), self._param_dict.get_config_value(param)))
                return True

        log.trace("Clean instrument config")
        return False

    def _is_logging(self):
        """
        Poll the instrument to see if we are in logging mode.  Return True
        if we are, False if not.
        @param: timeout - Command timeout
        @return: True - instrument logging, False - not logging
        """
        try:
            self._wakeup(3)
            return False
        except InstrumentTimeoutException:
            return True

    def _start_logging(self, timeout=TIMEOUT):
        """
        Command the instrument to start logging
        @param timeout: how long to wait for a prompt
        @throws: InstrumentProtocolException if failed to start logging
        """
        if self._is_logging():
            return True
        self._do_cmd_no_resp(InstrumentCmds.START_LOGGING, timeout=timeout)

    def _stop_logging(self):
        """
        Command the instrument to stop logging
        @param timeout: how long to wait for a prompt
        @throws: InstrumentTimeoutException if prompt isn't seen
        @throws: InstrumentProtocolException failed to stop logging
        """
        self._send_break()

        if self._is_logging():
            log.error("FAILED TO STOP LOGGING in _stop_logging")
            raise InstrumentProtocolException("failed to stop logging")

    def _sanitize(self, s):
        s = s.replace('\xb3', '_')
        s = s.replace('\xbf', '_')
        s = s.replace('\xc0', '_')
        s = s.replace('\xd9', '_')
        s = s.replace('\xda', '_')
        s = s.replace('\xf8', '_')

        return s

    ########################################################################
    # handlers.
    ########################################################################

    def _handler_command_run_test_200(self, *args, **kwargs):
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.RUN_TEST_200, *args, **kwargs)

        return next_state, result

    def _handler_command_factory_sets(self, *args, **kwargs):
        """
        run Factory set
        """
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.FACTORY_SETS, *args, **kwargs)
        self._update_params()
        return next_state, result

    def _handler_command_user_sets(self, *args, **kwargs):
        """
        run user set
        """
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.USER_SETS, *args, **kwargs)
        self._update_params()
        return next_state, result

    def _handler_command_save_setup_to_ram(self, *args, **kwargs):
        """
        save setup to ram.
        """
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.SAVE_SETUP_TO_RAM, *args, **kwargs)

        return next_state, result

    def _handler_command_clear_error_status_word(self, *args, **kwargs):
        """
        clear the error status word
        """
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.CLEAR_ERROR_STATUS_WORD, *args, **kwargs)
        return next_state, result

    def _handler_command_acquire_error_status_word(self, *args, **kwargs):
        """
        read the error status word
        """
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.DISPLAY_ERROR_STATUS_WORD, *args, **kwargs)
        return next_state, result

    def _handler_command_display_fault_log(self, *args, **kwargs):
        """
        display the error log.
        """
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.GET_FAULT_LOG, *args, **kwargs)
        return next_state, result

    def _handler_command_clear_fault_log(self, *args, **kwargs):
        """
        clear the error log.
        """
        next_state = None
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND
        result = self._do_cmd_resp(InstrumentCmds.CLEAR_FAULT_LOG, *args, **kwargs)
        return next_state, result

    def _init_params(self):
        """
        Overridden to call update_params before any sets
        """
        if self._init_type == InitializationType.STARTUP:
            logging = self._is_logging()
            if logging:
                self._stop_logging()
            log.debug("_init_params: Apply Startup Config")
            self._update_params()
            self.apply_startup_params()
            self._init_type = InitializationType.NONE
            if logging:
                self._start_logging()
        elif self._init_type == InitializationType.DIRECTACCESS:
            log.debug("_init_params: Apply DA Config")
            self._update_params()
            self.apply_direct_access_params()
            self._init_type = InitializationType.NONE
            pass
        elif self._init_type == InitializationType.NONE:
            log.debug("_init_params: No initialization required")
            pass
        elif self._init_type is None:
            raise InstrumentProtocolException("initialization type not set")
        else:
            raise InstrumentProtocolException("Unknown initialization type: %s" % self._init_type)

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        # Command device to initialize parameters and send a config change event.
        self._init_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        # # Generate the adcp_compass_calibration and adcp_system_configuration particles
        # self._handler_command_get_calibration(*args, **kwargs)
        # self._handler_command_get_configuration(*args, **kwargs)
        #
        # # start scheduled event for clock synch only if the interval is not "00:00:00
        # clock_interval = self._param_dict.get(Parameter.CLOCK_SYNCH_INTERVAL)
        # if clock_interval != ZERO_TIME_INTERVAL:
        # self.start_scheduled_job(Parameter.CLOCK_SYNCH_INTERVAL, ScheduledJob.CLOCK_SYNC,
        # ProtocolEvent.SCHEDULED_CLOCK_SYNC)
        #
        # # start scheduled event for get_status only if the interval is not "00:00:00
        # status_interval = self._param_dict.get(Parameter.GET_STATUS_INTERVAL)
        # if status_interval != ZERO_TIME_INTERVAL:
        #     self.start_scheduled_job(Parameter.GET_STATUS_INTERVAL, ScheduledJob.GET_CONFIGURATION,
        #                              ProtocolEvent.SCHEDULED_GET_STATUS)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        pass

    def _handler_unknown_enter(self, *args, **kwargs):
        """
        Enter unknown state.
        """
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_unknown_exit(self, *args, **kwargs):
        """
        Exit unknown state.
        """

    ######################################################
    # #
    ######################################################

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE.
        @return protocol_state, agent_state if successful
        """
        protocol_state, agent_state = self._discover()
        if protocol_state == ProtocolState.COMMAND:
            agent_state = ResourceAgentState.IDLE

        return protocol_state, agent_state

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        try:
            self._init_params()

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as error:
            log.error("Error in apply_startup_params: %s", error)
            raise

        finally:
            # Switch back to streaming
            log.debug("starting logging")
            self._start_logging()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @return next_state, (next_agent_state, result) if successful.
        """
        result = None
        kwargs['expected_prompt'] = Prompt.COMMAND
        kwargs['timeout'] = 30

        log.info("SYNCING TIME WITH SENSOR.")
        self._do_cmd_resp(InstrumentCmds.SET, Parameter.TIME,
                          get_timestamp_delayed("%Y/%m/%d, %H:%M:%S"), **kwargs)

        # Issue start command and switch to autosample if successful.
        self._start_logging()

        next_state = ProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        return next_state, (next_agent_state, result)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return  next_state, (next_agent_state, result) if successful.
        incorrect prompt received.
        """
        result = None
        self._stop_logging()

        next_state = ProtocolState.COMMAND
        next_agent_state = ResourceAgentState.COMMAND

        return next_state, (next_agent_state, result)

    def _handler_autosample_get_calibration(self, *args, **kwargs):
        """
        execute a get calibration from autosample mode.
        For this command we have to move the instrument
        into command mode, get calibration, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @return (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        next_state = None
        next_agent_state = None
        output = ""
        error = None

        try:
            # Switch to command mode,
            self._stop_logging()

            kwargs['timeout'] = 120
            output = self._do_cmd_resp(InstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            error = e

        finally:
            # Switch back to streaming
            self._start_logging()

        if error:
            raise error

        result = self._sanitize(base64.b64decode(output))
        return next_state, (next_agent_state, result)

    def _handler_autosample_get_configuration(self, *args, **kwargs):
        """
        execute a get configuration from autosample mode.
        For this command we have to move the instrument
        into command mode, get configuration, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @return (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """

        next_state = None
        next_agent_state = None
        output = ""
        error = None

        try:
            # Switch to command mode,
            self._stop_logging()

            # Sync the clock
            output = self._do_cmd_resp(InstrumentCmds.GET_SYSTEM_CONFIGURATION, *args, **kwargs)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            error = e

        finally:
            # Switch back to streaming
            self._start_logging()

        if error:
            raise error

        result = self._sanitize(base64.b64decode(output))

        return next_state, (next_agent_state, result)

    def _handler_recover_autosample(self, *args, **kwargs):
        """
        Reenter autosample mode.  Used when our data handler detects
        as data sample.
        @return next_state, next_agent_state
        """
        next_state = ProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        self._async_agent_state_change(ResourceAgentState.STREAMING)

        return next_state, next_agent_state

    def _handler_autosample_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change from
        autosample mode.  For this command we have to move the instrument
        into command mode, do the clock sync, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @return (next_state, (next_agent_state, result) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        next_state = None
        next_agent_state = None
        result = None
        error = None

        logging = False

        self._promptbuf = ""
        self._linebuf = ""

        if self._is_logging():
            logging = True
            # Switch to command mode,
            self._stop_logging()

        log.debug("in _handler_autosample_clock_sync")
        try:
            # Sync the clock
            timeout = kwargs.get('timeout', TIMEOUT)

            self._sync_clock(InstrumentCmds.SET, Parameter.TIME, timeout,
                             time_format="%Y/%m/%d,%H:%M:%S")

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            error = e

        finally:
            # Switch back to streaming
            if logging:
                self._start_logging()

        if error:
            raise error

        return next_state, (next_agent_state, result)

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @return (next_state, result) tuple, (None, None).
        @throws InstrumentParameterException if missing set parameters, if set parameters not ALL and
        not a dict, or if parameter can't be properly formatted.
        @throws InstrumentTimeoutException if device cannot be woken for set command.
        @throws InstrumentProtocolException if set command could not be built or misunderstood.
        """
        log.trace("IN _handler_command_set")
        next_state = None
        startup = False
        changed = False

        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('_handler_command_set Set command requires a parameter dict.')

        try:
            startup = args[1]
        except IndexError:
            pass

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')

        result = self._set_params(params, startup)
        return next_state, result

    def _handler_command_get_calibration(self, *args, **kwargs):
        """
        execute output_calibration_data command(AC)
        @return next_state, (next_agent_state, result)
        """
        log.trace("IN _handler_command_get_calibration")
        next_state = None
        next_agent_state = None

        kwargs['timeout'] = 120

        output = self._do_cmd_resp(InstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
        result = self._sanitize(base64.b64decode(output))
        return next_state, (next_agent_state, result)

    def _handler_command_get_configuration(self, *args, **kwargs):
        """
        Get raw format of system configuration data
        @return next_state, (next_agent_state, {'result': result})
        """
        next_state = None
        next_agent_state = None

        kwargs['timeout'] = 120  # long time to get params.
        log.debug("in _handler_command_get_configuration")
        output = self._do_cmd_resp(InstrumentCmds.GET_SYSTEM_CONFIGURATION, *args, **kwargs)
        result = self._sanitize(base64.b64decode(output))
        return next_state, (next_agent_state, {'result': result})

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change
        @return next_state, (next_agent_state, result) if successful.
        """

        next_state = None
        next_agent_state = None
        result = None

        timeout = kwargs.get('timeout', TIMEOUT)
        self._sync_clock(InstrumentCmds.SET, Parameter.TIME, timeout, time_format="%Y/%m/%d,%H:%M:%S")
        return next_state, (next_agent_state, result)

    def _handler_command_get_status(self, *args, **kwargs):
        """
        execute a get status on the leading edge of a second change
        @return next_state, (next_agent_state, result) if successful.
        @throws InstrumentProtocolException from _do_cmd_no_resp.
        """

        next_state = None
        next_agent_state = None
        result = None

        try:
            # Get calibration, PT2 and PT4 events
            self._do_cmd_no_resp(InstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
            self._do_cmd_no_resp(InstrumentCmds.OUTPUT_PT2, *args, **kwargs)
            self._do_cmd_no_resp(InstrumentCmds.OUTPUT_PT4, *args, **kwargs)

        except Exception as e:
            log.error("InstrumentProtocolException in _do_cmd_no_resp()")
            raise InstrumentProtocolException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        return next_state, (next_agent_state, result)

    def _handler_autosample_get_status(self, *args, **kwargs):
        """
        execute a get status on the leading edge of a second change
        @return next_state, (next_agent_state, result) if successful.
        @throws InstrumentProtocolException from _do_cmd_no_resp
        """

        next_state = None
        next_agent_state = None
        result = None

        logging = False

        if self._is_logging():
            logging = True
            # Switch to command mode,
            self._stop_logging()
        try:
            self._do_cmd_no_resp(InstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
            self._do_cmd_no_resp(InstrumentCmds.OUTPUT_PT2, *args, **kwargs)
            self._do_cmd_no_resp(InstrumentCmds.OUTPUT_PT4, *args, **kwargs)

        except Exception as e:
            log.error("InstrumentProtocolException in _do_cmd_no_resp()")
            raise

        finally:
            # Switch back to streaming
            if logging:
                self._start_logging()
        return next_state, (next_agent_state, result)

    def _handler_command_start_direct(self, *args, **kwargs):
        result = None

        next_state = ProtocolState.DIRECT_ACCESS
        next_agent_state = ResourceAgentState.DIRECT_ACCESS
        return next_state, (next_agent_state, result)

    def _handler_direct_access_enter(self, *args, **kwargs):
        """
        Enter direct access state.
        """
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
        self._sent_cmds = []

    def _handler_direct_access_exit(self, *args, **kwargs):
        """
        Exit direct access state.
        """
        self._send_break()

        result = self._do_cmd_resp(InstrumentCmds.GET, Parameter.TIME_OF_FIRST_PING)
        if "****/**/**,**:**:**" not in result:
            log.error("TG not allowed to be set. sending a break to clear it.")

            self._send_break()

    def _handler_direct_access_execute_direct(self, data):
        next_state = None
        result = None
        next_agent_state = None
        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, (next_agent_state, result)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Get the raw format outputs of the following commands, AC, PT2, PT4
        """
        log.debug("IN _handler_command_acquire_status")
        next_state = None

        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = Prompt.COMMAND

        self._do_cmd_no_resp(InstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
        self._do_cmd_no_resp(InstrumentCmds.OUTPUT_PT2, *args, **kwargs)
        self._do_cmd_no_resp(InstrumentCmds.OUTPUT_PT4, *args, **kwargs)

        return next_state, (None, None)

    def _discover(self):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE or UNKNOWN.
        @return (next_protocol_state, next_agent_state)
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentStateException if the device response does not correspond to
        an expected state.
        """
        logging = self._is_logging()
        if logging:
            return ProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING
        return ProtocolState.COMMAND, ResourceAgentState.COMMAND

    def _handler_direct_access_stop_direct(self):
        """
        @reval next_state, (next_agent_state, result)
        """
        result = None
        (next_state, next_agent_state) = self._discover()

        return next_state, (next_agent_state, result)

    def _handler_command_restore_factory_params(self):
        """
        """

    def _build_set_command(self, cmd, param, val):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """
        try:
            str_val = self._param_dict.format(param, val)
            set_cmd = '%s%s' % (param, str_val) + NEWLINE
            log.trace("IN _build_set_command CMD = '%s'", set_cmd)
        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter. %s' % param)

        return set_cmd

    def _parse_set_response(self, response, prompt):
        """
        Parse handler for set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if set command misunderstood.
        """
        if prompt == Prompt.ERR:
            raise InstrumentParameterException(
                'Protocol._parse_set_response : Set command not recognized: %s' % response)

        if " ERR" in response:
            raise InstrumentParameterException('Protocol._parse_set_response : Set command failed: %s' % response)

    def _build_get_command(self, cmd, param, **kwargs):
        """
        param=val followed by newline.
        @param cmd get command
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The get command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """
        kwargs['expected_prompt'] = Prompt.COMMAND + NEWLINE + Prompt.COMMAND
        try:
            self.get_param = param
            get_cmd = param + '?' + NEWLINE
        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter.. %s' % param)

        return get_cmd

    def _parse_get_response(self, response, prompt):
        log.trace("GET RESPONSE = " + repr(response))

        if prompt == Prompt.ERR:
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : Set command not recognized: %s' % response)

        while (not response.endswith('\r\n>\r\n>')) or ('?' not in response):
            prompt, response = self._get_raw_response(30, Prompt.COMMAND)
            time.sleep(.05)  # was 1
        self._param_dict.update(response)

        if self.get_param not in response:
            raise InstrumentParameterException('Failed to get a response for lookup of ' + self.get_param)

        self.get_count = 0
        return response

    ########################################################################
    # response handlers.
    ########################################################################
    ### Not sure if these are needed, since I'm creating data particles
    ### for the information.

    def _parse_output_calibration_data_response(self, response, prompt):
        """
        Return the output from the calibration request base 64 encoded
        """
        return base64.b64encode(response)

    def _parse_get_system_configuration(self, response, prompt):
        """
        return the output from the get system configuration request base 64 encoded
        """
        return base64.b64encode(response)

    def _parse_save_setup_to_ram_response(self, response, prompt):
        """
        save settings to nv ram. return response.
        """
        # Cleanup the results
        response = re.sub("CK\r\n", "", response)
        response = re.sub("\[", "", response)
        response = re.sub("\]", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_clear_error_status_response(self, response, prompt):
        """
        Remove the sent command from the response and return it
        """
        response = re.sub("CY0\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_error_status_response(self, response, prompt):
        """
        get the error status word, it should be 8 bytes of hexidecimal.
        """

        response = re.sub("CY1\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_clear_fault_log_response(self, response, prompt):
        """
        clear the clear fault log.
        """
        response = re.sub("FC\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_fault_log_response(self, response, prompt):
        """
        display the fault log.
        """
        response = re.sub("FD\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_instrument_transform_matrix_response(self, response, prompt):
        """
        display the transform matrix.
        """
        response = re.sub("PS3\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_test_response(self, response, prompt):
        """
        display the test log.
        """
        response = re.sub("PT200\r\n\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_factory_set_response(self, response, prompt):
        """
        Display factory set.
        """
        response = re.sub("CR1\r\n\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_user_set_response(self, response, prompt):
        """
        display user set.
        """
        response = re.sub("CR0\r\n\r\n", "", response)
        response = re.sub("\r\n>", "", response)
        return True, response

    def _parse_restore_factory_params_response(self):
        """
        """