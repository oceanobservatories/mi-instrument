#!/usr/bin/env python
# coding=utf-8

"""
@package mi.instrument.nortek.driver
@file mi/instrument/nortek/driver.py
@author Bill Bollenbacher
@author Steve Foley
@author Ronald Ronquillo
@brief Base class for Nortek instruments
"""
import struct


__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

import re
import time
import base64

from mi.core.log import get_logger, get_logging_metaclass
log = get_logger()

from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, DEFAULT_WRITE_DELAY, \
    InitializationType
from mi.core.instrument.driver_dict import DriverDict, DriverDictKey
from mi.core.instrument.protocol_cmd_dict import ProtocolCommandDict
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
from mi.core.instrument.protocol_param_dict import ParameterDictType

from mi.core.driver_scheduler import DriverSchedulerConfigKey, TriggerType

from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState

from mi.core.exceptions import InstrumentCommandException
from mi.core.exceptions import InstrumentStateException
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import SampleException

from mi.core.time_tools import get_timestamp_delayed
from mi.core.common import BaseEnum, Units

# newline.
NEWLINE = '\n\r'

# default timeout.
TIMEOUT = 15
# allowable time delay for sync the clock
TIME_DELAY = 2
# sample collection is ~60 seconds, add padding
SAMPLE_TIMEOUT = 70
# set up the 'structure' lengths (in bytes) and sync/id/size constants
CHECK_SUM_SEED = 0xb58c

HW_CONFIG_LEN = 48
HW_CONFIG_SYNC_BYTES   = '\xa5\x05\x18\x00'
HARDWARE_CONFIG_DATA_PATTERN = r'(%s)(.{44})(\x06\x06)' % HW_CONFIG_SYNC_BYTES
HARDWARE_CONFIG_DATA_REGEX = re.compile(HARDWARE_CONFIG_DATA_PATTERN, re.DOTALL)

HEAD_CONFIG_LEN = 224
HEAD_CONFIG_SYNC_BYTES = '\xa5\x04\x70\x00'
HEAD_CONFIG_DATA_PATTERN = r'(%s)(.{220})(\x06\x06)' % HEAD_CONFIG_SYNC_BYTES
HEAD_CONFIG_DATA_REGEX = re.compile(HEAD_CONFIG_DATA_PATTERN, re.DOTALL)

USER_CONFIG_LEN = 512
USER_CONFIG_SYNC_BYTES = '\xa5\x00\x00\x01'
USER_CONFIG_DATA_PATTERN = r'(%s)(.{508})(\x06\x06)' % USER_CONFIG_SYNC_BYTES
USER_CONFIG_DATA_REGEX = re.compile(USER_CONFIG_DATA_PATTERN, re.DOTALL)

# min, sec, day, hour, year, month
CLOCK_DATA_PATTERN = r'([\x00-\x60])([\x00-\x60])([\x01-\x31])([\x00-\x24])([\x00-\x99])([\x01-\x12])\x06\x06'
CLOCK_DATA_REGEX = re.compile(CLOCK_DATA_PATTERN, re.DOTALL)

# Special combined regex to give battery voltage a "unique sync byte" to search for (non-unique regex workaround)
ID_BATTERY_DATA_PATTERN = r'(?:AQD|VEC) [0-9]{4} {0,6}\x06\x06([\x00-\xFF][\x13-\x46])\x06\x06'
ID_BATTERY_DATA_REGEX = re.compile(ID_BATTERY_DATA_PATTERN, re.DOTALL)

# ~5000mV (0x1388) minimum to ~18000mv (0x4650) maximum
BATTERY_DATA_PATTERN = r'([\x00-\xFF][\x13-\x46])\x06\x06'
BATTERY_DATA_REGEX = re.compile(BATTERY_DATA_PATTERN, re.DOTALL)

# [\x00, \x01, \x02, \x04, and \x05]
MODE_DATA_PATTERN = r'([\x00-\x02,\x04,\x05]\x00)(\x06\x06)'
MODE_DATA_REGEX = re.compile(MODE_DATA_PATTERN, re.DOTALL)

# ["VEC 8181", "AQD 8493      "]
ID_DATA_PATTERN = r'((?:AQD|VEC) [0-9]{4}) {0,6}\x06\x06'
ID_DATA_REGEX = re.compile(ID_DATA_PATTERN, re.DOTALL)

NORTEK_COMMON_REGEXES = [USER_CONFIG_DATA_REGEX,
                         HARDWARE_CONFIG_DATA_REGEX,
                         HEAD_CONFIG_DATA_REGEX,
                         ID_BATTERY_DATA_REGEX,
                         CLOCK_DATA_REGEX]

INTERVAL_TIME_REGEX = r"([0-9][0-9]:[0-9][0-9]:[0-9][0-9])"


class ParameterUnits(BaseEnum):
    TIME_INTERVAL = 'HH:MM:SS'
    PARTS_PER_TRILLION = 'ppt'


class ScheduledJob(BaseEnum):
    """
    List of schedulable events
    """
    CLOCK_SYNC = 'clock_sync'
    ACQUIRE_STATUS = 'acquire_status'


class NortekDataParticleType(BaseEnum):
    """
    List of particles
    """
    RAW = CommonDataParticleType.RAW
    HARDWARE_CONFIG = 'vel3d_cd_hardware_configuration'
    HEAD_CONFIG = 'vel3d_cd_head_configuration'
    USER_CONFIG = 'vel3d_cd_user_configuration'
    CLOCK = 'vel3d_clock_data'
    BATTERY = 'vel3d_cd_battery_voltage'
    ID_STRING = 'vel3d_cd_identification_string'


class InstrumentPrompts(BaseEnum):
    """
    Device prompts.
    """
    AWAKE_NACKS   = '\x15\x15\x15\x15\x15\x15'
    COMMAND_MODE  = 'Command mode'
    CONFIRMATION  = 'Confirm:'
    Z_ACK         = '\x06\x06'  # attach a 'Z' to the front of these two items to force them to the end of the list
    Z_NACK        = '\x15\x15'  # so the other responses will have priority to be detected if they are present


class InstrumentCmds(BaseEnum):
    """
    List of instrument commands
    """
    CONFIGURE_INSTRUMENT               = 'CC'        # sets the user configuration
    SOFT_BREAK_FIRST_HALF              = '@@@@@@'
    SOFT_BREAK_SECOND_HALF             = 'K1W%!Q'
    AUTOSAMPLE_BREAK                   = '@'
    READ_REAL_TIME_CLOCK               = 'RC'
    SET_REAL_TIME_CLOCK                = 'SC'
    CMD_WHAT_MODE                      = 'II'        # to determine the mode of the instrument
    READ_USER_CONFIGURATION            = 'GC'
    READ_HW_CONFIGURATION              = 'GP'
    READ_HEAD_CONFIGURATION            = 'GH'
    READ_BATTERY_VOLTAGE               = 'BV'
    READ_ID                            = 'ID'
    START_MEASUREMENT_WITHOUT_RECORDER = 'ST'
    ACQUIRE_DATA                       = 'AD'
    CONFIRMATION                       = 'MC'        # confirm a break request
    SAMPLE_WHAT_MODE                   = 'I'


class InstrumentModes(BaseEnum):
    """
    List of possible modes the instrument can be in
    """
    FIRMWARE_UPGRADE = '\x00\x00\x06\x06'
    MEASUREMENT      = '\x01\x00\x06\x06'
    COMMAND          = '\x02\x00\x06\x06'
    DATA_RETRIEVAL   = '\x04\x00\x06\x06'
    CONFIRMATION     = '\x05\x00\x06\x06'


class ProtocolState(BaseEnum):
    """
    List of protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class ProtocolEvent(BaseEnum):
    """
    List of protocol events
    """
    # common events from base class
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    SCHEDULED_CLOCK_SYNC = DriverEvent.SCHEDULED_CLOCK_SYNC
    RESET = DriverEvent.RESET

    # instrument specific events
    SET_CONFIGURATION = "PROTOCOL_EVENT_CMD_SET_CONFIGURATION"
    READ_CLOCK = "PROTOCOL_EVENT_CMD_READ_CLOCK"
    READ_MODE = "PROTOCOL_EVENT_CMD_READ_MODE"
    POWER_DOWN = "PROTOCOL_EVENT_CMD_POWER_DOWN"
    READ_BATTERY_VOLTAGE = "PROTOCOL_EVENT_CMD_READ_BATTERY_VOLTAGE"
    READ_ID = "PROTOCOL_EVENT_CMD_READ_ID"
    GET_HW_CONFIGURATION = "PROTOCOL_EVENT_CMD_GET_HW_CONFIGURATION"
    GET_HEAD_CONFIGURATION = "PROTOCOL_EVENT_CMD_GET_HEAD_CONFIGURATION"
    GET_USER_CONFIGURATION = "PROTOCOL_EVENT_GET_USER_CONFIGURATION"
    SCHEDULED_ACQUIRE_STATUS = "PROTOCOL_EVENT_SCHEDULED_ACQUIRE_STATUS"


class Capability(BaseEnum):
    """
    Capabilities that are exposed to the user (subset of above)
    """
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET
    ACQUIRE_SAMPLE = ProtocolEvent.ACQUIRE_SAMPLE
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = ProtocolEvent.CLOCK_SYNC
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    DISCOVER = DriverEvent.DISCOVER


def validate_checksum(str_struct, raw_data, checksum):
    if (0xb58c + sum(struct.unpack_from(str_struct, raw_data))) & 0xffff != checksum:
        log.warn("Bad vel3d_cd_velocity_data from instrument (%r)", raw_data)
        return False
    return True


def _check_configuration(input_bytes, sync, length):
        """
        Perform a check on the configuration:
        1. Correct length
        2. Contains ACK bytes
        3. Correct sync byte
        4. Correct checksum
        """
        if len(input_bytes) != length + 2:
            log.debug('_check_configuration: wrong length, expected length %d != %d' % (length + 2, len(input_bytes)))
            return False

        # check for ACK bytes
        if input_bytes[length:length + 2] != InstrumentPrompts.Z_ACK:
            log.debug('_check_configuration: ACK bytes in error %s != %s',
                      input_bytes[length:length + 2].encode('hex'),
                      InstrumentPrompts.Z_ACK.encode('hex'))
            return False

        # check the sync bytes
        if input_bytes[0:4] != sync:
            log.debug('_check_configuration: sync bytes in error %s != %s',
                      input_bytes[0:4], sync)
            return False

        # check checksum
        calculated_checksum = CHECK_SUM_SEED
        if length is None:
            length = len(input_bytes)

        for word_index in range(0, length - 2, 2):
            word_value = NortekProtocolParameterDict.convert_word_to_int(input_bytes[word_index:word_index + 2])
            calculated_checksum = (calculated_checksum + word_value) % 0x10000

        # calculate checksum
        log.debug('_check_configuration: user c_c = %s', calculated_checksum)
        sent_checksum = NortekProtocolParameterDict.convert_word_to_int(input_bytes[length - 2:length])
        if sent_checksum != calculated_checksum:
            log.debug('_check_configuration: user checksum in error %s != %s',
                      calculated_checksum, sent_checksum)
            return False

        return True


class NortekHardwareConfigDataParticleKey(BaseEnum):
    """
    Particle key for the hw config
    """
    SERIAL_NUM = 'instrmt_type_serial_number'
    RECORDER_INSTALLED = 'recorder_installed'
    COMPASS_INSTALLED = 'compass_installed'
    BOARD_FREQUENCY = 'board_frequency'
    PIC_VERSION = 'pic_version'
    HW_REVISION = 'hardware_revision'
    RECORDER_SIZE = 'recorder_size'
    VELOCITY_RANGE = 'velocity_range'
    FW_VERSION = 'firmware_version'
    STATUS = 'status'
    CONFIG = 'config'
    CHECKSUM = 'checksum'


class NortekHardwareConfigDataParticle(DataParticle):
    """
    Routine for parsing hardware config data into a data particle structure for the Nortek sensor.
    """

    _data_particle_type = NortekDataParticleType.HARDWARE_CONFIG

    def _build_parsed_values(self):
        """
        Take the hardware config data and parse it into
        values with appropriate tags.
        """
        try:
            unpack_string = '<4s14s2s4H2s12s4sh2s'
            sync, serial_num, config, board_frequency, pic_version, hw_revision, recorder_size, status, spare, fw_version, cksum, _ = \
                struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<23H', self.raw_data[0:HW_CONFIG_LEN-2], cksum):
                log.warn("_parse_read_hw_config: Bad read hw response from instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            config = NortekProtocolParameterDict.convert_bytes_to_bit_field(config)
            status = NortekProtocolParameterDict.convert_bytes_to_bit_field(status)
            recorder_installed = config[-1]
            compass_installed = config[-2]
            velocity_range = status[-1]

        except Exception:
            log.error('Error creating particle hardware config, raw data: %r', self.raw_data)
            raise SampleException

        result = [{DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.SERIAL_NUM, DataParticleKey.VALUE: serial_num},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.RECORDER_INSTALLED, DataParticleKey.VALUE: recorder_installed},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.COMPASS_INSTALLED, DataParticleKey.VALUE: compass_installed},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.BOARD_FREQUENCY, DataParticleKey.VALUE: board_frequency},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.PIC_VERSION, DataParticleKey.VALUE: pic_version},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.HW_REVISION, DataParticleKey.VALUE: hw_revision},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.RECORDER_SIZE, DataParticleKey.VALUE: recorder_size},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.VELOCITY_RANGE, DataParticleKey.VALUE: velocity_range},
                  {DataParticleKey.VALUE_ID: NortekHardwareConfigDataParticleKey.FW_VERSION, DataParticleKey.VALUE: fw_version}]

        log.debug('NortekHardwareConfigDataParticle: particle=%r', result)
        return result


class NortekHeadConfigDataParticleKey(BaseEnum):
    """
    Particle key for the head config
    """
    PRESSURE_SENSOR = 'pressure_sensor'
    MAG_SENSOR = 'magnetometer_sensor'
    TILT_SENSOR = 'tilt_sensor'
    TILT_SENSOR_MOUNT = 'tilt_sensor_mounting'
    HEAD_FREQ = 'head_frequency'
    HEAD_TYPE = 'head_type'
    HEAD_SERIAL = 'head_serial_number'
    SYSTEM_DATA = 'system_data'
    NUM_BEAMS = 'number_beams'
    CONFIG = 'config'
    CHECKSUM = 'checksum'


class NortekHeadConfigDataParticle(DataParticle):
    """
    Routine for parsing head config data into a data particle structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.HEAD_CONFIG

    def _build_parsed_values(self):
        """
        Take the head config data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        try:
            unpack_string = '<4s2s2H12s176s22sHh2s'
            sync, config, head_freq, head_type, head_serial, system_data, _, num_beams, cksum, _ = struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<111H', self.raw_data[0:HEAD_CONFIG_LEN-2], cksum):
                log.warn("_parse_read_head_config: Bad read hw response from instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            config = NortekProtocolParameterDict.convert_bytes_to_bit_field(config)
            system_data = base64.b64encode(system_data)
            head_serial = head_serial.split('\x00', 1)[0]

            pressure_sensor = config[-1]
            mag_sensor = config[-2]
            tilt_sensor = config[-3]
            tilt_mount = config[-4]

        except Exception:
            log.error('Error creating particle head config, raw data: %r', self.raw_data)
            raise SampleException

        result = [{DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.PRESSURE_SENSOR, DataParticleKey.VALUE: pressure_sensor},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.MAG_SENSOR, DataParticleKey.VALUE: mag_sensor},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.TILT_SENSOR, DataParticleKey.VALUE: tilt_sensor},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.TILT_SENSOR_MOUNT, DataParticleKey.VALUE: tilt_mount},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.HEAD_FREQ, DataParticleKey.VALUE: head_freq},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.HEAD_TYPE, DataParticleKey.VALUE: head_type},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.HEAD_SERIAL, DataParticleKey.VALUE: head_serial},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.SYSTEM_DATA, DataParticleKey.VALUE: system_data, DataParticleKey.BINARY: True},
                  {DataParticleKey.VALUE_ID: NortekHeadConfigDataParticleKey.NUM_BEAMS, DataParticleKey.VALUE: num_beams}]

        log.debug('NortekHeadConfigDataParticle: particle=%r', result)
        return result


class NortekUserConfigDataParticleKey(BaseEnum):
    """
    User Config particle keys
    """
    TX_LENGTH = 'transmit_pulse_length'
    BLANK_DIST = 'blanking_distance'
    RX_LENGTH = 'receive_length'
    TIME_BETWEEN_PINGS = 'time_between_pings'
    TIME_BETWEEN_BURSTS = 'time_between_bursts'
    NUM_PINGS = 'number_pings'
    AVG_INTERVAL = 'average_interval'
    NUM_BEAMS = 'number_beams'
    PROFILE_TYPE = 'profile_type'
    MODE_TYPE = 'mode_type'
    TCR = 'tcr'
    PCR = 'pcr'
    POWER_TCM1 = 'power_level_tcm1'
    POWER_TCM2 = 'power_level_tcm2'
    SYNC_OUT_POSITION = 'sync_out_position'
    SAMPLE_ON_SYNC = 'sample_on_sync'
    START_ON_SYNC = 'start_on_sync'
    POWER_PCR1 = 'power_level_pcr1'
    POWER_PCR2 = 'power_level_pcr2'
    COMPASS_UPDATE_RATE = 'compass_update_rate'
    COORDINATE_SYSTEM = 'coordinate_system'
    NUM_CELLS = 'number_cells'
    CELL_SIZE = 'cell_size'
    MEASUREMENT_INTERVAL = 'measurement_interval'
    DEPLOYMENT_NAME = 'deployment_name'
    WRAP_MODE = 'wrap_mode'
    DEPLOY_START_TIME = 'deployment_start_time'
    DIAG_INTERVAL = 'diagnostics_interval'
    MODE = 'mode'
    USE_SPEC_SOUND_SPEED = 'use_specified_sound_speed'
    DIAG_MODE_ON = 'diagnostics_mode_enable'
    ANALOG_OUTPUT_ON = 'analog_output_enable'
    OUTPUT_FORMAT = 'output_format_nortek'
    SCALING = 'scaling'
    SERIAL_OUT_ON = 'serial_output_enable'
    STAGE_ON = 'stage_enable'
    ANALOG_POWER_OUTPUT = 'analog_power_output'
    SOUND_SPEED_ADJUST = 'sound_speed_adjust_factor'
    NUM_DIAG_SAMPLES = 'number_diagnostics_samples'
    NUM_BEAMS_PER_CELL = 'number_beams_per_cell'
    NUM_PINGS_DIAG = 'number_pings_diagnostic'
    MODE_TEST = 'mode_test'
    USE_DSP_FILTER = 'use_dsp_filter'
    FILTER_DATA_OUTPUT = 'filter_data_output'
    ANALOG_INPUT_ADDR = 'analog_input_address'
    SW_VER = 'software_version'
    VELOCITY_ADJ_FACTOR = 'velocity_adjustment_factor'
    FILE_COMMENTS = 'file_comments'
    WAVE_MODE = 'wave_mode'
    WAVE_DATA_RATE = 'wave_data_rate'
    WAVE_CELL_POS = 'wave_cell_position'
    DYNAMIC_POS_TYPE = 'dynamic_position_type'
    PERCENT_WAVE_CELL_POS = 'percent_wave_cell_position'
    WAVE_TX_PULSE = 'wave_transmit_pulse'
    FIX_WAVE_BLANK_DIST = 'fixed_wave_blanking_distance'
    WAVE_CELL_SIZE = 'wave_measurement_cell_size'
    NUM_DIAG_PER_WAVE = 'number_diagnostics_per_wave'
    NUM_SAMPLE_PER_BURST = 'number_samples_per_burst'
    ANALOG_SCALE_FACTOR = 'analog_scale_factor'
    CORRELATION_THRS = 'correlation_threshold'
    TX_PULSE_LEN_2ND = 'transmit_pulse_length_2nd'
    FILTER_CONSTANTS = 'filter_constants'
    CHECKSUM = 'checksum'


class Parameter(DriverParameter):
    """
    Device parameters
    """
    # user configuration
    TRANSMIT_PULSE_LENGTH = NortekUserConfigDataParticleKey.TX_LENGTH
    BLANKING_DISTANCE = NortekUserConfigDataParticleKey.BLANK_DIST                          # T2
    RECEIVE_LENGTH = NortekUserConfigDataParticleKey.RX_LENGTH                              # T3
    TIME_BETWEEN_PINGS = NortekUserConfigDataParticleKey.TIME_BETWEEN_PINGS                 # T4
    TIME_BETWEEN_BURST_SEQUENCES = NortekUserConfigDataParticleKey.TIME_BETWEEN_BURSTS      # T5
    NUMBER_PINGS = NortekUserConfigDataParticleKey.NUM_PINGS                        # number of beam sequences per burst
    AVG_INTERVAL = NortekUserConfigDataParticleKey.AVG_INTERVAL
    USER_NUMBER_BEAMS = NortekUserConfigDataParticleKey.NUM_BEAMS
    TIMING_CONTROL_REGISTER = NortekUserConfigDataParticleKey.TCR
    POWER_CONTROL_REGISTER = NortekUserConfigDataParticleKey.PCR
    A1_1_SPARE = 'a1_1spare'
    B0_1_SPARE = 'b0_1spare'
    B1_1_SPARE = 'b1_1spare'
    COMPASS_UPDATE_RATE = NortekUserConfigDataParticleKey.COMPASS_UPDATE_RATE
    COORDINATE_SYSTEM = NortekUserConfigDataParticleKey.COORDINATE_SYSTEM
    NUMBER_BINS = NortekUserConfigDataParticleKey.NUM_CELLS
    BIN_LENGTH = NortekUserConfigDataParticleKey.CELL_SIZE
    MEASUREMENT_INTERVAL = NortekUserConfigDataParticleKey.MEASUREMENT_INTERVAL
    DEPLOYMENT_NAME = NortekUserConfigDataParticleKey.DEPLOYMENT_NAME
    WRAP_MODE = NortekUserConfigDataParticleKey.WRAP_MODE
    CLOCK_DEPLOY = NortekUserConfigDataParticleKey.DEPLOY_START_TIME
    DIAGNOSTIC_INTERVAL = NortekUserConfigDataParticleKey.DIAG_INTERVAL
    MODE = NortekUserConfigDataParticleKey.MODE
    ADJUSTMENT_SOUND_SPEED = NortekUserConfigDataParticleKey.SOUND_SPEED_ADJUST
    NUMBER_SAMPLES_DIAGNOSTIC = NortekUserConfigDataParticleKey.NUM_DIAG_SAMPLES
    NUMBER_BEAMS_CELL_DIAGNOSTIC = NortekUserConfigDataParticleKey.NUM_BEAMS_PER_CELL
    NUMBER_PINGS_DIAGNOSTIC = NortekUserConfigDataParticleKey.NUM_PINGS_DIAG
    MODE_TEST = NortekUserConfigDataParticleKey.MODE_TEST
    ANALOG_INPUT_ADDR = NortekUserConfigDataParticleKey.ANALOG_INPUT_ADDR
    SW_VERSION = NortekUserConfigDataParticleKey.SW_VER
    USER_1_SPARE = 'spare_1'
    VELOCITY_ADJ_TABLE = NortekUserConfigDataParticleKey.VELOCITY_ADJ_FACTOR
    COMMENTS = NortekUserConfigDataParticleKey.FILE_COMMENTS
    WAVE_MEASUREMENT_MODE = NortekUserConfigDataParticleKey.WAVE_MODE
    DYN_PERCENTAGE_POSITION = NortekUserConfigDataParticleKey.PERCENT_WAVE_CELL_POS
    WAVE_TRANSMIT_PULSE = NortekUserConfigDataParticleKey.WAVE_TX_PULSE
    WAVE_BLANKING_DISTANCE = NortekUserConfigDataParticleKey.FIX_WAVE_BLANK_DIST
    WAVE_CELL_SIZE = NortekUserConfigDataParticleKey.WAVE_CELL_SIZE
    NUMBER_DIAG_SAMPLES = NortekUserConfigDataParticleKey.NUM_DIAG_PER_WAVE
    A1_2_SPARE = 'a1_2spare'
    B0_2_SPARE = 'b0_2spare'
    NUMBER_SAMPLES_PER_BURST = NortekUserConfigDataParticleKey.NUM_SAMPLE_PER_BURST
    USER_2_SPARE = 'spare_2'
    SAMPLE_RATE = 'sample_rate'
    ANALOG_OUTPUT_SCALE = NortekUserConfigDataParticleKey.ANALOG_SCALE_FACTOR
    CORRELATION_THRESHOLD = NortekUserConfigDataParticleKey.CORRELATION_THRS
    USER_3_SPARE = 'spare_3'
    TRANSMIT_PULSE_LENGTH_SECOND_LAG = NortekUserConfigDataParticleKey.TX_PULSE_LEN_2ND
    USER_4_SPARE = 'spare_4'
    QUAL_CONSTANTS = NortekUserConfigDataParticleKey.FILTER_CONSTANTS


class EngineeringParameter(DriverParameter):
    """
    Driver Parameters (aka, engineering parameters)
    """
    CLOCK_SYNC_INTERVAL = 'ClockSyncInterval'
    ACQUIRE_STATUS_INTERVAL = 'AcquireStatusInterval'


class NortekUserConfigDataParticle(DataParticle):
    """
    Routine for parsing user config data into a data particle structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.USER_CONFIG

    def _build_parsed_values(self):
        """
        Take the user config data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """

        try:
            unpack_string = '<4s8H2s2s6s5H6sH6sI2s4H2s2H2s180s180s2s5H4sH2s2H2sH30s16sH2s'
            sync, tx_length, blank_dist, rx_length, time_bw_pings, time_bw_bursts, num_pings, avg_interval, num_beams,\
                tcr, pcr, _, compass_update_rate, coordinate_system, num_cells, cell_size, measurement_interval,\
                deployment_name, wrap_mode, deploy_start_time, diag_interval, mode, sound_speed_adjust, num_diag_samples,\
                num_beams_cell, num_pings_diag, mode_test, analog_input_addr, sw_ver, _, velocity_adj_factor,\
                file_comments, wave_mode, percent_wave_cell_pos, wave_tx_pulse, fix_wave_blank_dist, wave_cell_size,\
                num_diag_per_wave, _, num_sample_burst, _, analog_scale_factor, correlation_thrs, _, tx_pulse_len_2nd,\
                _, filter_constants, cksum, _ = struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<255H', self.raw_data[0:USER_CONFIG_LEN-2], cksum):
                log.warn("_parse_read_head_config: Bad read hw response from instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            tcr = NortekProtocolParameterDict.convert_bytes_to_bit_field(tcr)
            pcr = NortekProtocolParameterDict.convert_bytes_to_bit_field(pcr)

            deploy_start_time = NortekProtocolParameterDict.convert_words_to_datetime(deploy_start_time)

            mode = NortekProtocolParameterDict.convert_bytes_to_bit_field(mode)
            mode_test = NortekProtocolParameterDict.convert_bytes_to_bit_field(mode_test)
            wave_mode = NortekProtocolParameterDict.convert_bytes_to_bit_field(wave_mode)
            velocity_adj_factor = base64.b64encode(velocity_adj_factor)
            filter_constants = base64.b64encode(filter_constants)
            file_comments, _ = file_comments.split('\x00', 1)
            deployment_name, _ = deployment_name.split('\x00', 1)

            profiler_type = tcr[-2]
            mode_type = tcr[-3]
            power_tcm1 = tcr[-6]
            power_tcm2 = tcr[-7]
            sync_out_position = tcr[-8]
            sample_on_sync = tcr[-9]
            start_on_sync = tcr[-10]

            power_pcr1 = pcr[-6]
            power_pcr2 = pcr[-7]

            use_spec_sound_speed = mode[-1]
            diag_mode_on = mode[-2]
            analog_output_on = mode[-3]
            output_format = mode[-4]
            scaling = mode[-5]
            serial_out_on = mode[-6]
            stage_on = mode[-8]
            analog_power_output = mode[-9]

            use_dsp_filter = mode_test[-1]
            filter_data_output = mode[-2]

            wave_data_rate = wave_mode[-1]
            wave_cell_pos = wave_mode[-2]
            dynamic_pos_type = wave_mode[-3]

        except Exception:
            log.error('Error creating particle user config, raw data: %r', self.raw_data)
            raise SampleException

        result = [{DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.TX_LENGTH, DataParticleKey.VALUE: tx_length},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.BLANK_DIST, DataParticleKey.VALUE: blank_dist},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.RX_LENGTH, DataParticleKey.VALUE: rx_length},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.TIME_BETWEEN_PINGS, DataParticleKey.VALUE: time_bw_pings},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.TIME_BETWEEN_BURSTS, DataParticleKey.VALUE: time_bw_bursts},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_PINGS, DataParticleKey.VALUE: num_pings},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.AVG_INTERVAL, DataParticleKey.VALUE: avg_interval},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_BEAMS, DataParticleKey.VALUE: num_beams},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.PROFILE_TYPE, DataParticleKey.VALUE: profiler_type},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.MODE_TYPE, DataParticleKey.VALUE: mode_type},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.POWER_TCM1, DataParticleKey.VALUE: power_tcm1},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.POWER_TCM2, DataParticleKey.VALUE: power_tcm2},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.SYNC_OUT_POSITION, DataParticleKey.VALUE: sync_out_position},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.SAMPLE_ON_SYNC, DataParticleKey.VALUE: sample_on_sync},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.START_ON_SYNC, DataParticleKey.VALUE: start_on_sync},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.POWER_PCR1, DataParticleKey.VALUE: power_pcr1},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.POWER_PCR2, DataParticleKey.VALUE: power_pcr2},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.COMPASS_UPDATE_RATE, DataParticleKey.VALUE: compass_update_rate},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.COORDINATE_SYSTEM, DataParticleKey.VALUE: coordinate_system},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_CELLS, DataParticleKey.VALUE: num_cells},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.CELL_SIZE, DataParticleKey.VALUE: cell_size},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.MEASUREMENT_INTERVAL, DataParticleKey.VALUE: measurement_interval},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.DEPLOYMENT_NAME, DataParticleKey.VALUE: deployment_name},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.WRAP_MODE, DataParticleKey.VALUE: wrap_mode},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.DEPLOY_START_TIME, DataParticleKey.VALUE: deploy_start_time},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.DIAG_INTERVAL, DataParticleKey.VALUE: diag_interval},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.USE_SPEC_SOUND_SPEED, DataParticleKey.VALUE: use_spec_sound_speed},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.DIAG_MODE_ON, DataParticleKey.VALUE: diag_mode_on},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.ANALOG_OUTPUT_ON, DataParticleKey.VALUE: analog_output_on},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.OUTPUT_FORMAT, DataParticleKey.VALUE: output_format},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.SCALING, DataParticleKey.VALUE: scaling},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.SERIAL_OUT_ON, DataParticleKey.VALUE: serial_out_on},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.STAGE_ON, DataParticleKey.VALUE: stage_on},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.ANALOG_POWER_OUTPUT, DataParticleKey.VALUE: analog_power_output},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.SOUND_SPEED_ADJUST, DataParticleKey.VALUE: sound_speed_adjust},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_DIAG_SAMPLES, DataParticleKey.VALUE: num_diag_samples},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_BEAMS_PER_CELL, DataParticleKey.VALUE: num_beams_cell},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_PINGS_DIAG, DataParticleKey.VALUE: num_pings_diag},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.USE_DSP_FILTER, DataParticleKey.VALUE: use_dsp_filter},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.FILTER_DATA_OUTPUT, DataParticleKey.VALUE: filter_data_output},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.ANALOG_INPUT_ADDR, DataParticleKey.VALUE: analog_input_addr},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.SW_VER, DataParticleKey.VALUE: sw_ver},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.VELOCITY_ADJ_FACTOR, DataParticleKey.VALUE: velocity_adj_factor},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.FILE_COMMENTS, DataParticleKey.VALUE: file_comments},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.WAVE_DATA_RATE, DataParticleKey.VALUE: wave_data_rate},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.WAVE_CELL_POS, DataParticleKey.VALUE: wave_cell_pos},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.DYNAMIC_POS_TYPE, DataParticleKey.VALUE: dynamic_pos_type},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.PERCENT_WAVE_CELL_POS, DataParticleKey.VALUE: percent_wave_cell_pos},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.WAVE_TX_PULSE, DataParticleKey.VALUE: wave_tx_pulse},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.FIX_WAVE_BLANK_DIST, DataParticleKey.VALUE: fix_wave_blank_dist},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.WAVE_CELL_SIZE, DataParticleKey.VALUE: wave_cell_size},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_DIAG_PER_WAVE, DataParticleKey.VALUE: num_diag_samples},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.NUM_SAMPLE_PER_BURST, DataParticleKey.VALUE: num_sample_burst},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.ANALOG_SCALE_FACTOR, DataParticleKey.VALUE: analog_scale_factor},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.CORRELATION_THRS, DataParticleKey.VALUE: correlation_thrs},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.TX_PULSE_LEN_2ND, DataParticleKey.VALUE: tx_pulse_len_2nd},
                  {DataParticleKey.VALUE_ID: NortekUserConfigDataParticleKey.FILTER_CONSTANTS, DataParticleKey.VALUE: filter_constants}]

        log.debug('NortekUserConfigDataParticle: particle=%r', result)
        return result


class NortekEngClockDataParticleKey(BaseEnum):
    """
    Particles for the clock data
    """
    DATE_TIME_ARRAY = "date_time_array"


class NortekEngClockDataParticle(DataParticle):
    """
    Routine for parsing clock engineering data into a data particle structure
    for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.CLOCK

    def _build_parsed_values(self):
        """
        Take the clock data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        try:
            minutes, seconds, day, hour, year, month, _ = struct.unpack('<6B2s', self.raw_data)
        except Exception:
            log.error('Error creating particle clock data raw data: %r', self.raw_data)
            raise SampleException

        minutes = int('%02x' % minutes)
        seconds = int('%02x' % seconds)
        day = int('%02x' % day)
        hour = int('%02x' % hour)
        year = int('%02x' % year)
        month = int('%02x' % month)

        result = [{DataParticleKey.VALUE_ID: NortekEngClockDataParticleKey.DATE_TIME_ARRAY,
                   DataParticleKey.VALUE: [minutes, seconds, day, hour, year, month]}]

        log.debug('NortekEngClockDataParticle: particle=%r', result)
        return result


class NortekEngBatteryDataParticleKey(BaseEnum):
    """
    Particles for the battery data
    """
    BATTERY_VOLTAGE = "battery_voltage_mv"


class NortekEngBatteryDataParticle(DataParticle):
    """
    Routine for parsing battery engineering data into a data particle
    structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.BATTERY

    def _build_parsed_values(self):
        """
        Take the battery data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        match = BATTERY_DATA_REGEX.search(self.raw_data)
        if not match:
            raise SampleException("NortekEngBatteryDataParticle: No regex match of parsed sample data: [%r]" % self.raw_data)

        # Calculate value
        battery_voltage = NortekProtocolParameterDict.convert_word_to_int(match.group(1))
        if battery_voltage is None:
            raise SampleException("No battery_voltage value parsed")

        # report values
        result = [{DataParticleKey.VALUE_ID: NortekEngBatteryDataParticleKey.BATTERY_VOLTAGE,
                   DataParticleKey.VALUE: battery_voltage}]
        log.debug('NortekEngBatteryDataParticle: particle=%r', result)
        return result


class NortekEngIdDataParticleKey(BaseEnum):
    """
    Particles for identification data
    """
    ID = "identification_string"


class NortekEngIdDataParticle(DataParticle):
    """
    Routine for parsing id engineering data into a data particle
    structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.ID_STRING

    def _build_parsed_values(self):
        """
        Take the id data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        match = ID_DATA_REGEX.match(self.raw_data)
        if not match:
            raise SampleException("NortekEngIdDataParticle: No regex match of parsed sample data: [%r]" % self.raw_data)

        id_str = match.group(1).split('\x00', 1)[0]

        # report values
        result = [{DataParticleKey.VALUE_ID: NortekEngIdDataParticleKey.ID, DataParticleKey.VALUE: id_str}]
        log.debug('NortekEngIdDataParticle: particle=%r', result)
        return result


###############################################################################
# Param dictionary helpers
###############################################################################
class NortekProtocolParameterDict(ProtocolParameterDict):

    def get_config(self):
        """
        Retrieve the configuration (all key values not ending in 'Spare').
        """
        config = {}
        for (key, val) in self._param_dict.iteritems():
            config[key] = val.get_value()
        return config

    def set_from_value(self, name, value):
        """
        Set a parameter value in the dictionary.
        @param name The parameter name.
        @param value The parameter value.
        @raises InstrumentParameterException if the name is invalid.
        """
        retval = False

        if name not in self._param_dict:
            raise InstrumentParameterException('Unable to set parameter %s to %s: parameter %s not an dictionary' % (name, value, name))

        if ((self._param_dict[name].value.f_format == NortekProtocolParameterDict.word_to_string) or
                (self._param_dict[name].value.f_format == NortekProtocolParameterDict.double_word_to_string)):
            if not isinstance(value, int):
                raise InstrumentParameterException('Unable to set parameter %s to %s: value not an integer' % (name, value))
        elif self._param_dict[name].value.f_format == NortekProtocolParameterDict.convert_datetime_to_words:
            if not isinstance(value, list):
                raise InstrumentParameterException('Unable to set parameter %s to %s: value not a list' % (name, value))

        if value != self._param_dict[name].value.get_value():
            log.debug("old value: %s, new value: %s", self._param_dict[name].value.get_value(), value)
            retval = True
        self._param_dict[name].value.set_value(value)

        return retval

    @staticmethod
    def word_to_string(value):
        """
        Converts a word into a string field
        """
        low_byte = value & 0xff
        high_byte = (value & 0xff00) >> 8
        return chr(low_byte) + chr(high_byte)

    @staticmethod
    def convert_word_to_int(word):
        """
        Converts a word into an integer field
        """
        if len(word) != 2:
            raise SampleException("Invalid number of bytes in word input! Found %s with input %s" % (word, len(word)))

        convert, = struct.unpack('<H', word)
        log.trace('word %r, convert %r', word, convert)

        return convert

    @staticmethod
    def double_word_to_string(value):
        """
        Converts an int to a hex string
        """
        r = struct.pack('<I', value)
        return r

    @staticmethod
    def convert_double_word_to_int(dword):
        """
        Converts 2 words into an integer field
        """
        if len(dword) != 4:
            raise SampleException("Invalid number of bytes in double word input! Found %s" % len(dword))

        r, = struct.unpack('<I', dword)
        return r

    @staticmethod
    def convert_bytes_to_bit_field(input_bytes):
        """
        Convert bytes to a bit field, reversing bytes in the process.
        ie ['\x05', '\x01'] becomes [0, 0, 0, 1, 0, 1, 0, 1]
        @param input_bytes an array of string literal bytes.
        @retval an list of 1 or 0 in order 
        """
        byte_list = list(input_bytes)
        byte_list.reverse()
        result = []
        for byte in byte_list:
            bin_string = bin(ord(byte))[2:].rjust(8, '0')
            result.extend([int(x) for x in list(bin_string)])
        log.trace("Returning a bitfield of %s for input string: [%s]", result, input_bytes)
        return result

    @staticmethod
    def convert_words_to_datetime(input_bytes):
        """
        Convert block of 6 words into a date/time structure for the
        instrument family
        @param input_bytes 6 bytes
        @retval An array of 6 ints corresponding to the date/time structure
        @raise SampleException If the date/time cannot be found
        """
        if len(input_bytes) != 6:
            raise SampleException("Invalid number of bytes in input! Found %s" % len(input_bytes))

        minutes, seconds, day, hour, year, month, = struct.unpack('<6B', input_bytes)

        minutes = int('%02x' % minutes)
        seconds = int('%02x' % seconds)
        day = int('%02x' % day)
        hour = int('%02x' % hour)
        year = int('%02x' % year)
        month = int('%02x' % month)

        return [minutes, seconds, day, hour, year, month]

    @staticmethod
    def convert_datetime_to_words(int_array):
        """
        Convert array if integers into a block of 6 words that could be fed
        back to the instrument as a timestamp.
        @param int_array An array of 6 hex values corresponding to a vector
        date/time stamp.
        @retval A string of 6 binary characters
        """
        if len(int_array) != 6:
            raise SampleException("Invalid number of bytes in date/time input! Found %s" % len(int_array))

        output = [chr(int(str(n), 16)) for n in int_array]
        return "".join(output)

    @staticmethod
    def convert_time(response):
        """
        Converts the timestamp in hex to D/M/YYYY HH:MM:SS
        """
        minutes, seconds, day, hour, year, month = struct.unpack('6B', response)
        return '%02x/%02x/20%02x %02x:%02x:%02x' % (day, month, year, hour, minutes, seconds)


    @staticmethod
    def convert_bytes_to_string(bytes_in):
        """
        Convert a list of bytes into a string, remove trailing nulls
        ie. ['\x65', '\x66'] turns into "ef"
        @param bytes_in The byte list to take in
        @retval The string to return
        """
        ba = bytearray(bytes_in)
        return str(ba).split('\x00', 1)[0]

###############################################################################
# Driver
###############################################################################
class NortekInstrumentDriver(SingleConnectionInstrumentDriver):
    """
    Base class for all seabird instrument drivers.
    """

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = NortekInstrumentProtocol(InstrumentPrompts, NEWLINE, self._driver_event)

    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return Parameter.list()


###############################################################################
# Protocol
###############################################################################
class NortekInstrumentProtocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class for Nortek driver.
    Subclasses CommandResponseInstrumentProtocol
    """
    __metaclass__ = get_logging_metaclass(log_level='debug')

    velocity_data_regex = []
    velocity_sync_bytes = ''

    # user configuration order of params, this needs to match the configuration order for setting params
    order_of_user_config = [
        Parameter.TRANSMIT_PULSE_LENGTH,
        Parameter.BLANKING_DISTANCE,
        Parameter.RECEIVE_LENGTH,
        Parameter.TIME_BETWEEN_PINGS,
        Parameter.TIME_BETWEEN_BURST_SEQUENCES,
        Parameter.NUMBER_PINGS,
        Parameter.AVG_INTERVAL,
        Parameter.USER_NUMBER_BEAMS,
        Parameter.TIMING_CONTROL_REGISTER,
        Parameter.POWER_CONTROL_REGISTER,
        Parameter.A1_1_SPARE,
        Parameter.B0_1_SPARE,
        Parameter.B1_1_SPARE,
        Parameter.COMPASS_UPDATE_RATE,
        Parameter.COORDINATE_SYSTEM,
        Parameter.NUMBER_BINS,
        Parameter.BIN_LENGTH,
        Parameter.MEASUREMENT_INTERVAL,
        Parameter.DEPLOYMENT_NAME,
        Parameter.WRAP_MODE,
        Parameter.CLOCK_DEPLOY,
        Parameter.DIAGNOSTIC_INTERVAL,
        Parameter.MODE,
        Parameter.ADJUSTMENT_SOUND_SPEED,
        Parameter.NUMBER_SAMPLES_DIAGNOSTIC,
        Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC,
        Parameter.NUMBER_PINGS_DIAGNOSTIC,
        Parameter.MODE_TEST,
        Parameter.ANALOG_INPUT_ADDR,
        Parameter.SW_VERSION,
        Parameter.USER_1_SPARE,
        Parameter.VELOCITY_ADJ_TABLE,
        Parameter.COMMENTS,
        Parameter.WAVE_MEASUREMENT_MODE,
        Parameter.DYN_PERCENTAGE_POSITION,
        Parameter.WAVE_TRANSMIT_PULSE,
        Parameter.WAVE_BLANKING_DISTANCE,
        Parameter.WAVE_CELL_SIZE,
        Parameter.NUMBER_DIAG_SAMPLES,
        Parameter.A1_2_SPARE,
        Parameter.B0_2_SPARE,
        Parameter.NUMBER_SAMPLES_PER_BURST,
        Parameter.USER_2_SPARE,
        Parameter.ANALOG_OUTPUT_SCALE,
        Parameter.CORRELATION_THRESHOLD,
        Parameter.USER_3_SPARE,
        Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG,
        Parameter.USER_4_SPARE,
        Parameter.QUAL_CONSTANTS]

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        self._protocol_fsm = InstrumentFSM(ProtocolState,
                                           ProtocolEvent,
                                           ProtocolEvent.ENTER,
                                           ProtocolEvent.EXIT)

        handlers = {
            ProtocolState.UNKNOWN: [
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
                (ProtocolEvent.READ_MODE, self._handler_unknown_read_mode),
                (ProtocolEvent.EXIT, self._handler_unknown_exit)
            ],
            ProtocolState.COMMAND: [
                (ProtocolEvent.ENTER, self._handler_command_enter),
                (ProtocolEvent.EXIT, self._handler_command_exit),
                (ProtocolEvent.ACQUIRE_SAMPLE, self._handler_command_acquire_sample),
                (ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample),
                (ProtocolEvent.SET, self._handler_command_set),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status),
                (ProtocolEvent.START_DIRECT, self._handler_command_start_direct),
                (ProtocolEvent.CLOCK_SYNC, self._handler_command_clock_sync),
                (ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_command_clock_sync),
                (ProtocolEvent.SCHEDULED_ACQUIRE_STATUS, self._handler_command_acquire_status)
            ],
            ProtocolState.AUTOSAMPLE: [
                (ProtocolEvent.ENTER, self._handler_autosample_enter),
                (ProtocolEvent.EXIT, self._handler_autosample_exit),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.READ_MODE, self._handler_autosample_read_mode),
                (ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample),
                (ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_autosample_clock_sync),
                (ProtocolEvent.SCHEDULED_ACQUIRE_STATUS, self._handler_autosample_acquire_status)
            ],
            ProtocolState.DIRECT_ACCESS: [
                (ProtocolEvent.ENTER, self._handler_direct_access_enter),
                (ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct),
                (ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct),
                (ProtocolEvent.READ_MODE, self._handler_unknown_read_mode),
                (ProtocolEvent.EXIT, self._handler_direct_access_exit)
            ]
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # Add build handlers for device commands.
        self._add_build_handler(InstrumentCmds.SET_REAL_TIME_CLOCK, self._build_set_real_time_clock_command)

        # Add response handlers for device commands.
        self._add_response_handler(InstrumentCmds.ACQUIRE_DATA, self._parse_acquire_data_response)
        self._add_response_handler(InstrumentCmds.CMD_WHAT_MODE, self._parse_what_mode_response)
        self._add_response_handler(InstrumentCmds.SAMPLE_WHAT_MODE, self._parse_what_mode_response)
        self._add_response_handler(InstrumentCmds.READ_REAL_TIME_CLOCK, self._parse_read_clock_response)
        self._add_response_handler(InstrumentCmds.READ_HW_CONFIGURATION, self._parse_read_hw_config)
        self._add_response_handler(InstrumentCmds.READ_HEAD_CONFIGURATION, self._parse_read_head_config)
        self._add_response_handler(InstrumentCmds.READ_USER_CONFIGURATION, self._parse_read_user_config)
        self._add_response_handler(InstrumentCmds.SOFT_BREAK_SECOND_HALF, self._parse_second_break_response)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_param_dict()
        self._build_cmd_dict()
        self._build_driver_dict()

        # create chunker for processing instrument samples.
        self._chunker = StringChunker(self.sieve_function)

    @classmethod
    def sieve_function(cls, raw_data):
        """
        The method that detects data sample structures from instrument
        Should be in the format [[structure_sync_bytes, structure_len]*]
        """
        return_list = []
        sieve_matchers = NORTEK_COMMON_REGEXES + cls.velocity_data_regex

        for matcher in sieve_matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))
                log.debug("sieve_function: regex found %r", raw_data[match.start():match.end()])

        return return_list

    def _got_chunk_base(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        self._extract_sample(NortekUserConfigDataParticle, USER_CONFIG_DATA_REGEX, structure, timestamp)
        self._extract_sample(NortekHardwareConfigDataParticle, HARDWARE_CONFIG_DATA_REGEX, structure, timestamp)
        self._extract_sample(NortekHeadConfigDataParticle, HEAD_CONFIG_DATA_REGEX, structure, timestamp)

        self._extract_sample(NortekEngClockDataParticle, CLOCK_DATA_REGEX, structure, timestamp)
        self._extract_sample(NortekEngIdDataParticle, ID_DATA_REGEX, structure, timestamp)

        # Note: This appears to be the same size and data structure as average interval & measurement interval
        # need to copy over the exact value to match
        self._extract_sample(NortekEngBatteryDataParticle, ID_BATTERY_DATA_REGEX, structure, timestamp)

    ########################################################################
    # overridden superclass methods
    ########################################################################
    def _filter_capabilities(self, events):
        """
        Filters capabilities
        """
        events_out = [x for x in events if Capability.has(x)]
        return events_out

    def set_init_params(self, config):
        """
        over-ridden to handle binary block configuration
        Set the initialization parameters to the given values in the protocol parameter dictionary.
        @param config A driver configuration dict that should contain an
        enclosed dict with key DriverConfigKey.PARAMETERS. This should include
        either param_name/value pairs or
           {DriverParameter.ALL: base64-encoded string of raw values as the
           instrument would return them from a get config}. If the desired value
           is false, nothing will happen.
        @raise InstrumentParameterException If the config cannot be set
        """
        if not isinstance(config, dict):
            raise InstrumentParameterException("Invalid init config format")

        param_config = config.get(DriverConfigKey.PARAMETERS)
        log.debug('%s', param_config)

        if DriverParameter.ALL in param_config:
            binary_config = base64.b64decode(param_config[DriverParameter.ALL])
            # make the configuration string look like it came from instrument to get all the methods to be happy
            binary_config += InstrumentPrompts.Z_ACK
            log.debug("binary_config len=%d, binary_config=%s",
                      len(binary_config), binary_config.encode('hex'))

            if len(binary_config) == USER_CONFIG_LEN + 2:
                if _check_configuration(binary_config, USER_CONFIG_SYNC_BYTES, USER_CONFIG_LEN):
                    self._param_dict.update(binary_config)
                else:
                    raise InstrumentParameterException("bad configuration")
            else:
                raise InstrumentParameterException("configuration not the correct length")
        else:
            for name in param_config.keys():
                self._param_dict.set_init_value(name, param_config[name])

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        Also called when setting parameters during startup and direct access

        Issue commands to the instrument to set various parameters.  If
        startup is set to true that means we are setting startup values
        and immutable parameters can be set.  Otherwise only READ_WRITE
        parameters can be set.

        @param params dictionary containing parameter name and value
        @param startup bool True is we are initializing, False otherwise
        @raise InstrumentParameterException
        """
        # Retrieve required parameter from args.
        # Raise exception if no parameter provided, or not a dict.
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set params requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)

        old_config = self._param_dict.get_config()

        # For each key, value in the params list set the value in parameters copy.
        try:
            for name, value in params.iteritems():
                log.debug('_set_params: setting %s to %s', name, value)
                self._param_dict.set_from_value(name, value)
        except Exception as ex:
            raise InstrumentParameterException('Unable to set parameter %s to %s: %s' % (name, value, ex))

        output = self._create_set_output(self._param_dict)

        # Clear the prompt buffer.
        self._promptbuf = ''
        self._linebuf = ''

        log.debug('_set_params: writing instrument configuration to instrument')
        self._connection.send(InstrumentCmds.CONFIGURE_INSTRUMENT)
        self._connection.send(output)

        result = self._get_response(timeout=30,
                                    expected_prompt=[InstrumentPrompts.Z_ACK, InstrumentPrompts.Z_NACK])

        log.debug('_set_params: result=%r', result)
        if result[1] == InstrumentPrompts.Z_NACK:
            raise InstrumentParameterException("NortekInstrumentProtocol._set_params(): Invalid configuration file! ")

        self._update_params()

        new_config = self._param_dict.get_config()
        log.trace("_set_params: old_config: %s", old_config)
        log.trace("_set_params: new_config: %s", new_config)
        if old_config != new_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)
            log.debug('_set_params: config updated!')

    def _send_wakeup(self):
        """
        Send a wakeup to the device. Overridden by device specific
        subclasses.
        """
        self._connection.send(InstrumentCmds.SOFT_BREAK_FIRST_HALF)

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Perform a command-response on the device.
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional command timeout.
        @retval resp_result The (possibly parsed) response result.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """
        
        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', TIMEOUT)
        response_regex = kwargs.get('response_regex', None)
        if response_regex is None:
            expected_prompt = kwargs.get('expected_prompt', InstrumentPrompts.Z_ACK)
        else:
            expected_prompt = None
        write_delay = kwargs.get('write_delay', DEFAULT_WRITE_DELAY)

        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            self._add_build_handler(cmd, self._build_command_default)

        return super(NortekInstrumentProtocol, self)._do_cmd_resp(cmd, timeout=timeout,
                                                                  expected_prompt=expected_prompt,
                                                                  response_regex=response_regex,
                                                                  write_delay=write_delay,
                                                                  *args)

    ########################################################################
    # Unknown handlers.
    ########################################################################

    def _handler_unknown_enter(self, *args, **kwargs):
        """
        Enter unknown state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state of instrument; can be COMMAND or AUTOSAMPLE.
        @retval (next_state, next_agent_state)
        """
        ret_mode = self._protocol_fsm.on_event(ProtocolEvent.READ_MODE)
        prompt = ret_mode[1]

        if prompt == 0:
            log.debug('_handler_unknown_discover: FIRMWARE_UPGRADE')
            raise InstrumentStateException('Firmware upgrade state.')
        elif prompt == 1:
            log.debug('_handler_unknown_discover: MEASUREMENT_MODE')
            next_state = ProtocolState.AUTOSAMPLE
            next_agent_state = ResourceAgentState.STREAMING
        elif prompt == 2:
            log.debug('_handler_unknown_discover: COMMAND_MODE')
            next_state = ProtocolState.COMMAND
            next_agent_state = ResourceAgentState.IDLE
        elif prompt == 4:
            log.debug('_handler_unknown_discover: DATA_RETRIEVAL_MODE')
            next_state = ProtocolState.AUTOSAMPLE
            next_agent_state = ResourceAgentState.STREAMING
        elif prompt == 5:
            log.debug('_handler_unknown_discover: CONFIRMATION_MODE')
            next_state = ProtocolState.AUTOSAMPLE
            next_agent_state = ResourceAgentState.STREAMING
        else:
            raise InstrumentStateException('Unknown state: %s' % ret_mode[1])

        log.debug('_handler_unknown_discover: state=%s', next_state)

        return next_state, next_agent_state

    def _handler_unknown_exit(self, *args, **kwargs):
        """
        Exiting Unknown state
        """
        pass

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state. Configure the instrument and driver, sync the clock, and start scheduled events
        if they are set
        """
        if self._init_type != InitializationType.NONE:
            self._update_params()

        self._init_params()

        if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) is not None:
            log.debug("Configuring the scheduler to sync clock %s", self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL))
            if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.CLOCK_SYNC_INTERVAL, ScheduledJob.CLOCK_SYNC, ProtocolEvent.CLOCK_SYNC)

        if self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL) is not None:
            log.debug("Configuring the scheduler to acquire status %s", self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL))
            if self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.ACQUIRE_STATUS_INTERVAL, ScheduledJob.ACQUIRE_STATUS, ProtocolEvent.ACQUIRE_STATUS)

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        self.stop_scheduled_job(ScheduledJob.ACQUIRE_STATUS)
        self.stop_scheduled_job(ScheduledJob.CLOCK_SYNC)
        pass

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        Command the instrument to acquire sample data. Instrument will enter Power Down mode when finished
        """
        self._do_cmd_resp(InstrumentCmds.ACQUIRE_DATA, expected_prompt=self.velocity_sync_bytes,
                                   timeout=SAMPLE_TIMEOUT)

        return None, (None, None)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        High level command for the operator to get all of the status from the instrument from autosample state:
        Battery voltage, clock, hw configuration, head configuration, user configuration, and identification string
        """

        # break out of measurement mode in order to issue the status related commands
        self._handler_autosample_stop_autosample()
        self._handler_command_acquire_status()
        # return to measurement mode
        self._handler_command_start_autosample()

        return None, (None, None)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        High level command for the operator to get all of the status from the instrument:
        Battery voltage, clock, hw configuration, head configuration, user configuration, and identification string
        """

        #ID + BV    Call these commands at the same time, so their responses are combined (non-unique regex workaround)
        # Issue read id, battery voltage, & clock commands all at the same time (non-unique REGEX workaround).
        self._do_cmd_resp(InstrumentCmds.READ_ID + InstrumentCmds.READ_BATTERY_VOLTAGE,
                          response_regex=ID_BATTERY_DATA_REGEX, timeout=30)

        #RC
        self._do_cmd_resp(InstrumentCmds.READ_REAL_TIME_CLOCK, response_regex=CLOCK_DATA_REGEX)

        #GP
        self._do_cmd_resp(InstrumentCmds.READ_HW_CONFIGURATION, response_regex=HARDWARE_CONFIG_DATA_REGEX)

        #GH
        self._do_cmd_resp(InstrumentCmds.READ_HEAD_CONFIGURATION, response_regex=HEAD_CONFIG_DATA_REGEX)

        #GC
        self._do_cmd_resp(InstrumentCmds.READ_USER_CONFIGURATION, response_regex=USER_CONFIG_DATA_REGEX)

        return None, (None, None)

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @retval None, None
        """
        self._verify_not_readonly(*args, **kwargs)
        self._set_params(*args, **kwargs)

        return None, None

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode
        @retval (next_state, next_resource_state, result) tuple
        """
        result = self._do_cmd_resp(InstrumentCmds.START_MEASUREMENT_WITHOUT_RECORDER, timeout=SAMPLE_TIMEOUT, *args, **kwargs)
        return ProtocolState.AUTOSAMPLE, (ResourceAgentState.STREAMING, result)

    def _handler_command_start_direct(self):
        return ProtocolState.DIRECT_ACCESS, (ResourceAgentState.DIRECT_ACCESS, None)

    def _handler_command_read_mode(self):
        """
        Issue read mode command.
        """
        result = self._do_cmd_resp(InstrumentCmds.CMD_WHAT_MODE)
        return None, (None, result)

    def _handler_autosample_read_mode(self):
        """
        Issue read mode command.
        """
        self._connection.send(InstrumentCmds.AUTOSAMPLE_BREAK)
        time.sleep(.1)
        result = self._do_cmd_resp(InstrumentCmds.SAMPLE_WHAT_MODE)
        return None, (None, result)

    def _handler_unknown_read_mode(self):
        """
        Issue read mode command.
        """
        next_state = None
        next_agent_state = None

        try:
            self._connection.send(InstrumentCmds.AUTOSAMPLE_BREAK)
            time.sleep(.1)
            result = self._do_cmd_resp(InstrumentCmds.SAMPLE_WHAT_MODE, timeout=0.6, response_regex=MODE_DATA_REGEX)
        except InstrumentTimeoutException:
            log.debug('_handler_unknown_read_mode: no response to "I", sending "II"')
            # if there is no response, catch timeout exception and issue 'II' command instead
            result = self._do_cmd_resp(InstrumentCmds.CMD_WHAT_MODE, response_regex=MODE_DATA_REGEX)

        return next_state, (next_agent_state, result)

    def _clock_sync(self, *args, **kwargs):
        """
        The mechanics of synchronizing a clock
        @throws InstrumentCommandException if the clock was not synchronized
        """
        str_time = get_timestamp_delayed("%M %S %d %H %y %m")
        byte_time = ''
        for v in str_time.split():
            byte_time += chr(int('0x' + v, base=16))
        values = str_time.split()
        log.debug("_clock_sync: time set to %s:m %s:s %s:d %s:h %s:y %s:M (%s)",
                 values[0], values[1], values[2], values[3], values[4], values[5],
                 byte_time.encode('hex'))
        self._do_cmd_resp(InstrumentCmds.SET_REAL_TIME_CLOCK, byte_time, **kwargs)

        response = self._do_cmd_resp(InstrumentCmds.READ_REAL_TIME_CLOCK, *args, **kwargs)
        minutes, seconds, day, hour, year, month, _ = struct.unpack('<6B2s', response)
        response = '%02x/%02x/20%02x %02x:%02x:%02x' % (day, month, year, hour, minutes, seconds)

        # verify that the dates match
        date_str = get_timestamp_delayed('%d/%m/%Y %H:%M:%S')

        if date_str[:10] != response[:10]:
            raise InstrumentCommandException("Syncing the clock did not work!")

        # verify that the times match closely
        hours = int(date_str[11:12])
        minutes = int(date_str[14:15])
        seconds = int(date_str[17:18])
        total_time = (hours * 3600) + (minutes * 60) + seconds

        hours = int(response[11:12])
        minutes = int(response[14:15])
        seconds = int(response[17:18])
        total_time2 = (hours * 3600) + (minutes * 60) + seconds

        if total_time - total_time2 > TIME_DELAY:
            raise InstrumentCommandException("Syncing the clock did not work! Off by %s seconds" %
                                             (total_time - total_time2))

        return response

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        sync clock close to a second edge 
        @retval (next_state, result) tuple, (None, None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        result = self._clock_sync()
        return None, (None, result)

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_autosample_clock_sync(self, *args, **kwargs):
        """
        While in autosample, sync a clock close to a second edge 
        @retval next_state, (next_agent_state, result) tuple, AUTOSAMPLE, (STREAMING, None) if successful.
        """

        next_state = None
        next_agent_state = None
        result = None
        try:
            self._protocol_fsm._on_event(ProtocolEvent.STOP_AUTOSAMPLE)
            next_state = ProtocolState.COMMAND
            next_agent_state = ResourceAgentState.COMMAND
            self._clock_sync()
            self._protocol_fsm._on_event(ProtocolEvent.START_AUTOSAMPLE)
            next_state = ProtocolState.AUTOSAMPLE
            next_agent_state = ResourceAgentState.STREAMING
        finally:
            return next_state, (next_agent_state, result)

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        if self._init_type != InitializationType.NONE:
            self._handler_autosample_stop_autosample()
            self._update_params()
            self._init_params()
            self._do_cmd_resp(InstrumentCmds.START_MEASUREMENT_WITHOUT_RECORDER, timeout=SAMPLE_TIMEOUT, *args, **kwargs)

        if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) is not None:
            log.debug("Configuring the scheduler to sync clock %s", self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL))
            if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.CLOCK_SYNC_INTERVAL, ScheduledJob.CLOCK_SYNC, ProtocolEvent.CLOCK_SYNC)

        if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) is not None:
            log.debug("Configuring the scheduler to acquire status %s", self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL))
            if self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.ACQUIRE_STATUS_INTERVAL, ScheduledJob.ACQUIRE_STATUS, ProtocolEvent.ACQUIRE_STATUS)

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """
        self.stop_scheduled_job(ScheduledJob.ACQUIRE_STATUS)
        self.stop_scheduled_job(ScheduledJob.CLOCK_SYNC)
        pass

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @retval ProtocolState.COMMAND, (ResourceAgentState.COMMAND, None) if successful.
        @throws InstrumentProtocolException if command misunderstood or incorrect prompt received.
        """
        self._connection.send(InstrumentCmds.SOFT_BREAK_FIRST_HALF)
        time.sleep(.1)
        ret_prompt = self._do_cmd_resp(InstrumentCmds.SOFT_BREAK_SECOND_HALF,
                                       expected_prompt=[InstrumentPrompts.CONFIRMATION, InstrumentPrompts.COMMAND_MODE],
                                       *args, **kwargs)

        log.debug('_handler_autosample_stop_autosample, ret_prompt: %s', ret_prompt)

        if ret_prompt == InstrumentPrompts.CONFIRMATION:
            # Issue the confirmation command.
            self._do_cmd_resp(InstrumentCmds.CONFIRMATION, *args, **kwargs)

        return ProtocolState.COMMAND, (ResourceAgentState.COMMAND, None)

    def stop_scheduled_job(self, schedule_job):
        """
        Remove the scheduled job
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
        interval = self._param_dict.get(param).split(':')
        hours = interval[0]
        minutes = interval[1]
        seconds = interval[2]
        log.debug("Setting scheduled interval to: %s %s %s", hours, minutes, seconds)

        config = {DriverConfigKey.SCHEDULER: {
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

        log.debug("Adding job %s", schedule_job)
        try:
            self._add_scheduler_event(schedule_job, protocol_event)
        except KeyError:
            log.debug("scheduler already exists for '%s'", schedule_job)

    ########################################################################
    # Direct access handlers.
    ########################################################################
    def _handler_direct_access_enter(self, *args, **kwargs):
        """
        Enter direct access state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
        self._sent_cmds = []

    def _handler_direct_access_exit(self, *args, **kwargs):
        """
        Exit direct access state.
        """
        pass

    def _handler_direct_access_execute_direct(self, data):
        """
        Execute Direct Access command(s)
        """
        next_state = None
        result = None

        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, result

    def _handler_direct_access_stop_direct(self, *args, **kwargs):
        """
        Stop Direct Access, and put the driver into a healthy state by reverting itself back to the previous
        state before stopping Direct Access.
        """
        #discover the state to go to next
        next_state, next_agent_state = self._handler_unknown_discover()
        if next_state == DriverProtocolState.COMMAND:
            next_agent_state = ResourceAgentState.COMMAND

        if next_state == DriverProtocolState.AUTOSAMPLE:
            #go into command mode in order to set parameters
            self._handler_autosample_stop_autosample()

        #restore parameters
        log.debug("da_param_restore = %s,", self._param_dict.get_direct_access_list())
        self._init_params()

        if next_state == DriverProtocolState.AUTOSAMPLE:
            #go back into autosample mode
            self._do_cmd_resp(InstrumentCmds.START_MEASUREMENT_WITHOUT_RECORDER, timeout=SAMPLE_TIMEOUT)

        return next_state, (next_agent_state, None)

    ########################################################################
    # Common handlers.
    ########################################################################
    def _handler_get(self, *args, **kwargs):
        """
        Get device parameters from the parameter dict.
        @param args[0] list of parameters to retrieve, or DriverParameter.ALL.
        @throws InstrumentParameterException if missing or invalid parameter.
        """
        next_state = None

        # Retrieve the required parameter, raise if not present.
        try:
            params = args[0]

        except IndexError:
            raise InstrumentParameterException('Get command requires a parameter list or tuple.')
        # If all params requested, retrieve config.
        if (params == DriverParameter.ALL) or (params == [DriverParameter.ALL]):
            result = self._param_dict.get_config()

        # If not all params, confirm a list or tuple of params to retrieve.
        # Raise if not a list or tuple.
        # Retrieve each key in the list, raise if any are invalid.
        else:
            if not isinstance(params, (list, tuple)):
                raise InstrumentParameterException('Get argument not a list or tuple.')
            result = {}
            for key in params:
                try:
                    val = self._param_dict.get(key)
                    result[key] = val

                except KeyError:
                    raise InstrumentParameterException(('%s is not a valid parameter.' % key))

        return next_state, result

    def _build_driver_dict(self):
        """
        Build a driver dictionary structure, load the strings for the metadata
        from a file if present.
        """
        self._driver_dict = DriverDict()
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, False)

    def _build_cmd_dict(self):
        """
        Build a command dictionary structure, load the strings for the metadata
        from a file if present.
        """
        self._cmd_dict = ProtocolCommandDict()

        self._cmd_dict.add(Capability.ACQUIRE_SAMPLE, display_name='Acquire Sample')
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name='Start Autosample')
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, display_name='Stop Autosample')
        self._cmd_dict.add(Capability.CLOCK_SYNC, display_name='Synchronize Clock')
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name='Acquire Status')
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        self._param_dict = NortekProtocolParameterDict()

        self._param_dict.add(Parameter.USER_NUMBER_BEAMS,
                             r'^.{%s}(.{2}).*' % str(18),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name="Number of Beams",
                             description="Number of beams on the instrument.",
                             value=3,
                             direct_access=True)
        self._param_dict.add(Parameter.TIMING_CONTROL_REGISTER,
                             r'^.{%s}(.{2}).*' % str(20),
                             lambda match:
                             NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Timing Control Register",
                             description="See manual for usage.",
                             direct_access=True,
                             value=130)
        self._param_dict.add(Parameter.COMPASS_UPDATE_RATE,
                             r'^.{%s}(.{2}).*' % str(30),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Compass Update Rate",
                             description="Rate at which compass is reoriented.",
                             default_value=1,
                             units=Units.SECOND,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.POWER_CONTROL_REGISTER,
                             r'^.{%s}(.{2}).*' % str(22),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name="Power Control Register",
                             description="See manual for usage.",
                             direct_access=True,
                             value=0)
        self._param_dict.add(Parameter.COORDINATE_SYSTEM,
                             r'^.{%s}(.{2}).*' % str(32),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Coordinate System",
                             description='Coordinate System (0:ENU | 1:XYZ | 2:Beam)',
                             default_value=2,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.NUMBER_BINS,
                             r'^.{%s}(.{2}).*' % str(34),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Number of Bins",
                             description="Number of sampling cells.",
                             default_value=1,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.BIN_LENGTH,
                             r'^.{%s}(.{2}).*' % str(36),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Bin Length",
                             description="Size of water volume analyzed.",
                             default_value=7,
                             units=Units.MILLIMETER + '³',
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.DEPLOYMENT_NAME,
                             r'^.{%s}(.{6}).*' % str(40),
                             lambda match: NortekProtocolParameterDict.convert_bytes_to_string(match.group(1)),
                             lambda string: string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Deployment Name",
                             description="Name of current deployment.",
                             default_value='',
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.WRAP_MODE,
                             r'^.{%s}(.{2}).*' % str(46),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Wrap Mode",
                             description='Recorder wrap mode (0:no wrap | 1:wrap when full)',
                             default_value=0,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.CLOCK_DEPLOY,
                             r'^.{%s}(.{6}).*' % str(48),
                             lambda match: NortekProtocolParameterDict.convert_words_to_datetime(match.group(1)),
                             NortekProtocolParameterDict.convert_datetime_to_words,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.LIST,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Clock Deploy",
                             description='Deployment start time.',
                             default_value=[0, 0, 0, 0, 0, 0],
                             units="[min, s, d, h, y, m]",
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.MODE,
                             r'^.{%s}(.{2}).*' % str(58),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Mode",
                             description="See manual for usage.",
                             default_value=48,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC,
                             r'^.{%s}(.{2}).*' % str(64),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Number Beams Cell Diagnostic",
                             description='Beams/cell number to measure in diagnostics mode.',
                             default_value=1,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.NUMBER_PINGS_DIAGNOSTIC,
                             r'^.{%s}(.{2}).*' % str(66),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Number Pings Diagnostic",
                             description='Pings in diagnostics/wave mode.',
                             default_value=1,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.MODE_TEST,
                             r'^.{%s}(.{2}).*' % str(68),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Mode Test",
                             description="See manual for usage.",
                             default_value=4,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.ANALOG_INPUT_ADDR,
                             r'^.{%s}(.{2}).*' % str(70),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Analog Input Address",
                             description="External input 1 and 2 to analog. Not using.",
                             default_value=0,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.VELOCITY_ADJ_TABLE,
                             r'^.{%s}(.{180}).*' % str(76),
                             lambda match: base64.b64encode(match.group(1)),
                             lambda string: string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name="Velocity Adj Table",
                             description="Scaling factors to account for the speed of sound variation as a function of "
                                         "temperature and salinity.",
                             units=ParameterUnits.PARTS_PER_TRILLION,
                             direct_access=True)
        self._param_dict.add(Parameter.COMMENTS,
                             r'^.{%s}(.{180}).*' % str(256),
                             lambda match: NortekProtocolParameterDict.convert_bytes_to_string(match.group(1)),
                             lambda string: string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Comments",
                             description="File comments.",
                             default_value='',
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.NUMBER_SAMPLES_PER_BURST,
                             r'^.{%s}(.{2}).*' % str(452),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             expiration=None,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Number of Samples per Burst",
                             description="Number of samples to take during given period.",
                             default_value=0,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.CORRELATION_THRESHOLD,
                             r'^.{%s}(.{2}).*' % str(458),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Correlation Threshold",
                             description='Correlation threshold for resolving ambiguities.',
                             default_value=0,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG,
                             r'^.{%s}(.{2}).*' % str(462),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Transmit Pulse Length Second Lag",
                             description="Lag time between pulses.",
                             units=Units.COUNTS,
                             default_value=2,
                             startup_param=True,
                             direct_access=True)

        ############################################################################
        # ENGINEERING PARAMETERS
        ###########################################################################
        self._param_dict.add(EngineeringParameter.CLOCK_SYNC_INTERVAL,
                             INTERVAL_TIME_REGEX,
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Clock Sync Interval",
                             description='Interval for synchronizing the clock.',
                             units=ParameterUnits.TIME_INTERVAL,
                             default_value='00:00:00',
                             startup_param=True)
        self._param_dict.add(EngineeringParameter.ACQUIRE_STATUS_INTERVAL,
                             INTERVAL_TIME_REGEX,
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Acquire Status Interval",
                             description='Interval for gathering status particles.',
                             units=ParameterUnits.TIME_INTERVAL,
                             default_value='00:00:00',
                             startup_param=True)

    def _dump_config(self, input_config):
        """
        For debug purposes, dump the configuration block
        """
        dump = []
        for byte_index in xrange(0, len(input_config)):
            if byte_index % 0x10 == 0:
                dump.append('\n')   # no linefeed on first line
                dump.append('{:03x}  '.format(byte_index))
            dump.append('{:02x} '.format(ord(input_config[byte_index])))
        return "".join(dump)

    def _update_params(self):
        """
        Update the parameter dictionary. Issue the read config command. The response
        needs to be saved to param dictionary.
        """
        ret_config = self._do_cmd_resp(InstrumentCmds.READ_USER_CONFIGURATION, response_regex=USER_CONFIG_DATA_REGEX)
        self._param_dict.update(ret_config)

    def _create_set_output(self, parameters):
        """
        load buffer with sync byte (A5), ID byte (01), and size word (# of words in little-endian form)
        'user' configuration is 512 bytes = 256 words long = size 0x100
        """
        output = ['\xa5\x00\x00\x01']

        for param in self.order_of_user_config:
            log.trace('_create_set_output: adding %s to list', param)
            if param == Parameter.COMMENTS:
                output.append(parameters.format(param).ljust(180, "\x00"))
            elif param == Parameter.DEPLOYMENT_NAME:
                output.append(parameters.format(param).ljust(6, "\x00"))
            elif param == Parameter.QUAL_CONSTANTS:
                output.append('\x00'.ljust(16, "\x00"))
            elif param == Parameter.VELOCITY_ADJ_TABLE:
                output.append(base64.b64decode(parameters.format(param)))
            elif param in [Parameter.A1_1_SPARE, Parameter.B0_1_SPARE, Parameter.B1_1_SPARE, Parameter.USER_1_SPARE,
                           Parameter.A1_2_SPARE, Parameter.B0_2_SPARE, Parameter.USER_2_SPARE, Parameter.USER_3_SPARE,
                           Parameter.WAVE_MEASUREMENT_MODE, Parameter.WAVE_TRANSMIT_PULSE, Parameter.WAVE_BLANKING_DISTANCE,
                           Parameter.WAVE_CELL_SIZE, Parameter.NUMBER_DIAG_SAMPLES, Parameter.DYN_PERCENTAGE_POSITION]:
                output.append('\x00'.ljust(2, "\x00"))
            elif param == Parameter.USER_4_SPARE:
                output.append('\x00'.ljust(30, "\x00"))
            else:
                output.append(parameters.format(param))
            log.trace('_create_set_output: ADDED %s output size = %s', param, len(output))

        log.debug("Created set output: %r with length: %s", output, len(output))

        checksum = CHECK_SUM_SEED
        output = "".join(output)
        for word_index in range(0, len(output), 2):
            word_value = NortekProtocolParameterDict.convert_word_to_int(output[word_index:word_index+2])
            checksum = (checksum + word_value) % 0x10000
        log.debug('_create_set_output: user checksum = %r', checksum)

        output += (NortekProtocolParameterDict.word_to_string(checksum))

        return output

    def _build_command_default(self, cmd):
        return cmd

    def _build_set_real_time_clock_command(self, cmd, str_time, **kwargs):
        """
        Build the set clock command
        """
        return cmd + str_time

    def _parse_acquire_data_response(self, response, prompt):
        """
        Parse the response from the instrument for a acquire data command.
        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The [value] as a string
        @raise InstrumentProtocolException When a bad response is encountered
        """
        key = self.velocity_sync_bytes
        start = response.find(key)
        if start != -1:
            log.debug("_parse_acquire_data_response: response=%r", response[start:start+len(key)])
            self._handler_autosample_stop_autosample()
            return response[start:start+len(key)]

        log.error("_parse_acquire_data_response: Bad acquire data response from instrument (%r)", response)
        raise InstrumentProtocolException("Invalid acquire data response. (%r)" % response)

    def _parse_what_mode_response(self, response, prompt):
        """
        Parse the response from the instrument for a 'what mode' command.
        
        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The mode as an int
        @raise InstrumentProtocolException When a bad response is encountered
        """
        search_obj = re.search(MODE_DATA_REGEX, response)
        if search_obj:
            log.debug("_parse_what_mode_response: response=%r", search_obj.group(1))
            return NortekProtocolParameterDict.convert_word_to_int(search_obj.group(1))
        else:
            log.error("_parse_what_mode_response: Bad what mode response from instrument (%r)", response)
            raise InstrumentProtocolException("Invalid what mode response. (%r)" % response)

    def _parse_second_break_response(self, response, prompt):
        """
        Parse the response from the instrument for a 'what mode' command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The response as is
        @raise InstrumentProtocolException When a bad response is encountered
        """
        for search_prompt in (InstrumentPrompts.CONFIRMATION, InstrumentPrompts.COMMAND_MODE):
            start = response.find(search_prompt)
            if start != -1:
                log.debug("_parse_second_break_response: response=%r", response[start:start+len(search_prompt)])
                return response[start:start+len(search_prompt)]

        log.error("_parse_second_break_response: Bad second break response from instrument (%r)", response)
        raise InstrumentProtocolException("Invalid second break response. (%r)" % response)

    # DEBUG TEST PURPOSE ONLY
    def _parse_read_battery_voltage_response(self, response, prompt):
        """
        Parse the response from the instrument for a read battery voltage command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The battery voltage in mV int
        @raise InstrumentProtocolException When a bad response is encountered
        """
        match = BATTERY_DATA_REGEX.search(response)
        if not match:
            log.error("Bad response from instrument (%r)" % response)
            raise InstrumentProtocolException("Invalid response. (%r)" % response)

        return NortekProtocolParameterDict.convert_word_to_int(match.group(1))

    def _parse_read_clock_response(self, response, prompt):
        """
        Parse the response from the instrument for a read clock command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        """
        return response

    # DEBUG TEST PURPOSE ONLY
    def _parse_read_id_response(self, response, prompt):
        """
        Parse the response from the instrument for a read ID command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The id as a string
        @raise InstrumentProtocolException When a bad response is encountered
        """
        match = ID_DATA_REGEX.search(response)
        if not match:
            log.error("Bad response from instrument (%r)" % response)
            raise InstrumentProtocolException("Invalid response. (%r)" % response)

        return match.group(1)

    def _parse_read_hw_config(self, response, prompt):
        """ Parse the response from the instrument for a read hw config command.
        
        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval response
        @raise InstrumentProtocolException When a bad response is encountered
        """
        if not _check_configuration(self._promptbuf, HW_CONFIG_SYNC_BYTES, HW_CONFIG_LEN):
            log.error("_parse_read_hw_config: Bad read hw response from instrument (%r)", response)
            raise InstrumentProtocolException("Invalid read hw response. (%r)" % response)

        return response

    def _parse_read_head_config(self, response, prompt):
        """
        Parse the response from the instrument for a read head command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval response
        @raise InstrumentProtocolException When a bad response is encountered
        """
        if not _check_configuration(self._promptbuf, HEAD_CONFIG_SYNC_BYTES, HEAD_CONFIG_LEN):
            log.error("_parse_read_head_config: Bad read head response from instrument (%r)", response)
            raise InstrumentProtocolException("Invalid read head response. (%r)" % response)

        return response

    def _parse_read_user_config(self, response, prompt):
        """
        Parse the response from the instrument for a read user command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval response
        @raise InstrumentProtocolException When a bad response is encountered
        """
        if not _check_configuration(response, USER_CONFIG_SYNC_BYTES, USER_CONFIG_LEN):
            log.error("_parse_read_user_config: Bad read user response from instrument (%r)", response)
            raise InstrumentProtocolException("Invalid read user response. (%r)" % response)

        return response
