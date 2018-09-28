#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@package mi.instrument.nobska.mavs4.ooicore.driver
@file /marine-integrations/mi/instrument/nobska/mavs4/ooicore/driver.py
@author Bill Bollenbacher
@brief Driver for the mavs4
Release notes:

initial release
"""
import ctypes
import struct
import time
import re

from mi.core.util import dict_equal
from mi.core.time_tools import timegm_to_float
from mi.core.common import BaseEnum, Units, Prefixes
from mi.core.time_tools import get_timestamp_delayed
from mi.core.instrument.driver_dict import DriverDict, DriverDictKey
from mi.core.instrument.instrument_protocol import MenuInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.exceptions import InstrumentTimeoutException, \
    InstrumentParameterException, \
    InstrumentProtocolException, \
    SampleException, \
    InstrumentStateException
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility, ParameterDictType
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
from mi.core.instrument.protocol_param_dict import RegexParameter
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticle, DataParticleKey, CommonDataParticleType, DataParticleValue
from mi.core.log import get_logger, get_logging_metaclass

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'


log = get_logger()

SAMPLE_DATA_PATTERN = (r'(\d+\s+\d+\s+\d+)' +  # date
                       '\s+(\d+\s+\d+\s+\d+)' +  # time
                       '\.(\d+)' +  # fractional second
                       '\s+(\w+)' +  # vector A
                       '\s+(\w+)' +  # vector B
                       '\s+(\w+)' +  # vector C
                       '\s+(\w+)' +  # vector D
                       '\s+(-*\d+\.\d+)' +  # east
                       '\s+(-*\d+\.\d+)' +  # north
                       '\s+(-*\d+\.\d+)' +  # west
                       '\s+(-*\d+\.\d+)' +  # temperature
                       '\s+(-*\d+\.\d+)' +  # MX
                       '\s+(-*\d+\.\d+)' +  # MY
                       '\s+(-*\d+\.\d+)' +  # pitch
                       '\s+(-*\d+\.\d+)\s+')  # roll

SAMPLE_DATA_REGEX = re.compile(SAMPLE_DATA_PATTERN)

INSTRUMENT_NEWLINE = '\r\n'
WRITE_DELAY = 0
YES = 'y'
NO = 'n'
ON = 'On'
OFF = 'Off'
ENABLED = 'Enabled'
DISABLED = 'Disabled'

# default timeout.
INSTRUMENT_TIMEOUT = 5
STATUS_TIMEOUT = 20

common_matches = {
    'float': r'[-+]?\d*\.?\d+',
    'int': r'[-+]?\d+'
}


class ScheduledJob(BaseEnum):
    CLOCK_SYNC = 'clock_sync'


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    SAMPLE = 'vel3d_b_sample'
    STATUS = 'vel3d_b_engineering'


# Device prompts.
class InstrumentPrompts(BaseEnum):
    """
    MAVS-4 prompts.
    The main menu prompt has 2 bells and the sub menu prompts have one; the
    PicoDOS prompt has none.
    """
    DISPLAY_FORMAT = 'Set display format (HDS) [S] ?'
    MAIN_MENU = '\a\b ? \a\b'
    SUB_MENU = '\a\b'
    PICO_DOS = 'Enter command >> '
    SLEEPING = 'Sleeping . . .'
    SLEEP_WAKEUP = 'Enter <CTRL>-<C> now to wake up?'
    DEPLOY_WAKEUP = '>>> <CTRL>-<C> to terminate deployment <<<'
    SET_DONE = 'New parameters accepted.'
    SET_FAILED = 'Invalid entry'
    SET_TIME = '] ? \a\b'
    GET_TIME = 'Enter correct time ['
    CHANGE_TIME = 'Change time & date (Yes/No) [N] ?\a\b'
    NOTE_INPUT = '> '
    DEPLOY_MENU = 'G| Go (<CTRL>-<G> skips checks)\r\n\r\n'
    SELECTION = 'Selection  ?'
    VELOCITY_FRAME = ' <3> Earth Frame (E, N, W)'
    MONITOR = 'Enable Data Monitor (Yes/No) ['
    LOG_DISPLAY = 'with each sample (Yes/No) [Y] ?'
    VELOCITY_FORMAT = 'Set acoustic axis velocity format (HDS) [S] ?'
    QUERY = 'Enable Query Mode (Yes/No) ['
    FREQUENCY = 'Enter Measurement Frequency [Hz] (0.01 to 50.0) ?'
    MEAS_PER_SAMPLE = 'Enter number of measurements per sample (1 to 10000) ?'
    SAMPLE_PERIOD = 'Enter Sample Period [sec] (0.02 to   10000) ?'
    SAMPLES_PER_BURST = 'Enter number of samples per burst (1 to 100000) ?'
    BURST_INTERVAL_DAYS = 'Days     (  0 to   366) ?'
    BURST_INTERVAL_HOURS = 'Hours    (  0 to    23) ?'
    BURST_INTERVAL_MINUTES = 'Minutes  (  0 to    59) ?'
    BURST_INTERVAL_SECONDS = 'Seconds  (  0 to    59) ?'
    BEGIN_MEASUREMENT = 'MAVS4 is ready to deploy'
    SYSTEM_CONFIGURATION_MENU = '<X> Save Changes and Exit'
    SYSTEM_CONFIGURATION_PASSWORD = 'Password:'
    SI_CONVERSION = 'Enter binary to SI velocity conversion (0.0010000 to 0.0200000) ?'
    WARM_UP_INTERVAL = '[F]ast or [S]low sensor warm up interval [F] ?'
    THREE_AXIS_COMPASS = '3-Axis compass enabled (Yes/No) ['
    SOLID_STATE_TILT = 'Solid state tilt enabled (Yes/No) ['
    LOAD_DEFAULT_TILT = 'Load default tilt coefficients (Yes/No) ['
    THERMISTOR = 'Thermistor enabled (Yes/No) ['
    THERMISTOR_OFFSET = 'Set thermistor offset to 0.0 (default) (Yes/No) [N] ?'
    PRESSURE = 'Pressure enabled (Yes/No) ['
    AUXILIARY = 'Auxiliary * enabled (Yes/No) ['
    SENSOR_ORIENTATION = '<7> Horizontal/Bent Up'
    CALIBRATION_MENU = '<X> Save Constants and Exit'
    VELOCITY_OFFSETS = 'Velocity Offsets:'
    VELOCITY_OFFSETS_SET = 'Current path offsets:'
    COMPASS_OFFSETS = 'Compass Offsets:'
    COMPASS_OFFSETS_SET = 'Current compass offsets:'
    COMPASS_SCALE_FACTORS = 'Compass Scale Factors:'
    COMPASS_SCALE_FACTORS_SET = 'Current compass scale factors:'
    TILT_OFFSETS = 'Tilt Offsets:'
    TILT_OFFSETS_SET = 'Current tilt offsets:'
    SOLID_STATE_TILT_NOT_ENABLED = 'Solid State Tilt is not currently enabled.'


class InstrumentCmds(BaseEnum):  # these all must be unique for the fsm and dictionaries to work correctly
    CONTROL_C = '\x03'  # CTRL-C (end of text)
    DEPLOY_GO = '\a'  # CTRL-G (bell)
    SET_TIME = '1'
    ENTER_TIME = 'enter_time'
    DEPLOY_MENU = '6'
    SET_NOTE = 'set_note'
    ENTER_NOTE = 'enter_note'
    SET_VELOCITY_FRAME = 'F'
    ENTER_VELOCITY_FRAME = 'enter_velocity_frame'
    SET_MONITOR = 'M'
    ENTER_MONITOR = 'enter_monitor'
    ENTER_LOG_DISPLAY_TIME = 'enter_log_display_time'
    ENTER_LOG_DISPLAY_FRACTIONAL_SECOND = 'enter_log_display_fractional_second'
    ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES = 'enter_log_display_acoustic_axis_velocities'
    ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT = 'enter_log_display_acoustic_axis_velocity_format'
    SET_QUERY = 'Q'
    ENTER_QUERY = 'enter_query'
    SET_FREQUENCY = '4'
    ENTER_FREQUENCY = 'enter_frequency'
    SET_MEAS_PER_SAMPLE = '5'
    ENTER_MEAS_PER_SAMPLE = 'enter_measurements_per_sample'
    SET_SAMPLE_PERIOD = ' 6'  # make different from DEPLOY_MENU with leading space
    ENTER_SAMPLE_PERIOD = 'enter_sample_period'
    SET_SAMPLES_PER_BURST = '7'
    ENTER_SAMPLES_PER_BURST = 'enter_samples_per_burst'
    SET_BURST_INTERVAL_DAYS = '8'
    ENTER_BURST_INTERVAL_DAYS = 'enter_burst_interval_days'
    ENTER_BURST_INTERVAL_HOURS = 'enter_burst_interval_hours'
    ENTER_BURST_INTERVAL_MINUTES = 'enter_burst_interval_minutes'
    ENTER_BURST_INTERVAL_SECONDS = 'enter_burst_interval_seconds'
    SYSTEM_CONFIGURATION_MENU = 's'  # intentionally lower case to differentiate it from other commands
    SYSTEM_CONFIGURATION_PASSWORD = 'whipr'
    SYSTEM_CONFIGURATION_EXIT = 'x'
    SET_SI_CONVERSION = 'C\nn'
    ENTER_SI_CONVERSION = 'enter_si_conversion'
    SET_WARM_UP_INTERVAL = 'W'
    ENTER_WARM_UP_INTERVAL = 'enter_warm_up_interval'
    SET_THREE_AXIS_COMPASS = ' 1'  # make different from SET_TIME with leading space
    ENTER_THREE_AXIS_COMPASS = 'enter_3_axis_compass'
    SET_SOLID_STATE_TILT = '2'
    ENTER_SOLID_STATE_TILT = 'enter_solid_state_tilt'
    ANSWER_SOLID_STATE_TILT_YES = YES
    SET_THERMISTOR = ' 3'  # make different from CALIBRATION_MENU with leading space
    ENTER_THERMISTOR = 'enter_thermistor'
    ANSWER_THERMISTOR_NO = NO
    ANSWER_THERMISTOR_YES = YES
    SET_PRESSURE = ' 4'  # make different from SET_FREQUENCY with leading space
    ENTER_PRESSURE = 'enter_pressure'
    SET_AUXILIARY = 'set_auxiliary'
    ENTER_AUXILIARY = 'enter_auxiliary'
    SET_SENSOR_ORIENTATION = 'o'
    ENTER_SENSOR_ORIENTATION = 'enter_sensor_orientation'
    CALIBRATION_MENU = '3'
    VELOCITY_OFFSETS = 'V'
    VELOCITY_OFFSETS_SET = 'S'  # intentionally upper case to differentiate it from other commands
    COMPASS_OFFSETS = 'C'
    COMPASS_OFFSETS_SET = ' S'  # make different from VELOCITY_OFFSETS_SET with leading space
    COMPASS_SCALE_FACTORS = ' F'  # make different from SET_VEL_FRAME with leading space
    COMPASS_SCALE_FACTORS_SET = '  S'  # make different from COMPASS_OFFSETS_SET with 2 leading spaces
    TILT_OFFSETS = 'T'
    TILT_OFFSETS_SET = '   S'  # make different from COMPASS_SCALE_FACTORS_SET with 3 leading spaces


class ProtocolStates(BaseEnum):
    """
    Protocol states for MAVS-4. Cherry picked from DriverProtocolState enum.
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class ProtocolEvent(BaseEnum):
    """
    Protocol events for MAVS-4. Cherry picked from DriverEvent enum.
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    SCHEDULED_CLOCK_SYNC = DriverEvent.SCHEDULED_CLOCK_SYNC
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS


class Capability(BaseEnum):
    """
    Capabilities that are exposed to the user (subset of above)
    """
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = ProtocolEvent.CLOCK_SYNC
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    START_DIRECT = ProtocolEvent.START_DIRECT
    STOP_DIRECT = ProtocolEvent.STOP_DIRECT
    DISCOVER = ProtocolEvent.DISCOVER


class InstrumentParameters(DriverParameter):
    """
    Device parameters for MAVS-4.
    """
    # main menu parameters
    SYS_CLOCK = 'sys_clock'

    # deploy menu parameters
    NOTE1 = 'note1'
    NOTE2 = 'note2'
    NOTE3 = 'note3'
    VELOCITY_FRAME = 'velocity_frame'
    MONITOR = 'monitor'
    LOG_DISPLAY_TIME = 'log_display_time'
    LOG_DISPLAY_FRACTIONAL_SECOND = 'log_display_fractional_second'
    LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES = 'log_display_acoustic_axis_velocities'
    LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT = 'log_display_acoustic_axis_velocities_format'
    QUERY_MODE = 'query_mode'
    FREQUENCY = 'frequency'
    MEASUREMENTS_PER_SAMPLE = 'measurements_per_sample'
    SAMPLE_PERIOD = 'sample_period'
    SAMPLES_PER_BURST = 'samples_per_burst'
    BURST_INTERVAL_DAYS = 'burst_interval_days'
    BURST_INTERVAL_HOURS = 'burst_interval_hours'
    BURST_INTERVAL_MINUTES = 'burst_interval_minutes'
    BURST_INTERVAL_SECONDS = 'burst_interval_seconds'

    # system configuration menu parameters
    SI_CONVERSION = 'si_conversion'
    WARM_UP_INTERVAL = 'warm_up_interval'
    THREE_AXIS_COMPASS = '3_axis_compass'
    SOLID_STATE_TILT = 'solid_state_tilt'
    THERMISTOR = 'thermistor'
    PRESSURE = 'pressure'
    AUXILIARY_1 = 'auxiliary_1'
    AUXILIARY_2 = 'auxiliary_2'
    AUXILIARY_3 = 'auxiliary_3'
    SENSOR_ORIENTATION = 'sensor_orientation'
    SERIAL_NUMBER = 'serial_number'

    # calibration menu parameters
    VELOCITY_OFFSET_PATH_A = 'velocity_offset_path_a'
    VELOCITY_OFFSET_PATH_B = 'velocity_offset_path_b'
    VELOCITY_OFFSET_PATH_C = 'velocity_offset_path_c'
    VELOCITY_OFFSET_PATH_D = 'velocity_offset_path_d'
    COMPASS_OFFSET_0 = 'compass_offset_0'
    COMPASS_OFFSET_1 = 'compass_offset_1'
    COMPASS_OFFSET_2 = 'compass_offset_2'
    COMPASS_SCALE_FACTORS_0 = 'compass_scale_factors_0'
    COMPASS_SCALE_FACTORS_1 = 'compass_scale_factors_1'
    COMPASS_SCALE_FACTORS_2 = 'compass_scale_factors_2'
    TILT_PITCH_OFFSET = 'tilt_pitch_offset'
    TILT_ROLL_OFFSET = 'tilt_roll_offset'


class DeployMenuParameters(BaseEnum):
    NOTE1 = InstrumentParameters.NOTE1
    NOTE2 = InstrumentParameters.NOTE2
    NOTE3 = InstrumentParameters.NOTE3
    VELOCITY_FRAME = InstrumentParameters.VELOCITY_FRAME
    MONITOR = InstrumentParameters.MONITOR
    LOG_DISPLAY_TIME = InstrumentParameters.LOG_DISPLAY_TIME
    LOG_DISPLAY_FRACTIONAL_SECOND = InstrumentParameters.LOG_DISPLAY_FRACTIONAL_SECOND
    LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES = InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES
    LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT = InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT
    QUERY_MODE = InstrumentParameters.QUERY_MODE
    FREQUENCY = InstrumentParameters.FREQUENCY
    MEASUREMENTS_PER_SAMPLE = InstrumentParameters.MEASUREMENTS_PER_SAMPLE
    SAMPLE_PERIOD = InstrumentParameters.SAMPLE_PERIOD
    SAMPLES_PER_BURST = InstrumentParameters.SAMPLES_PER_BURST
    BURST_INTERVAL_DAYS = InstrumentParameters.BURST_INTERVAL_DAYS
    BURST_INTERVAL_HOURS = InstrumentParameters.BURST_INTERVAL_HOURS
    BURST_INTERVAL_MINUTES = InstrumentParameters.BURST_INTERVAL_MINUTES
    BURST_INTERVAL_SECONDS = InstrumentParameters.BURST_INTERVAL_SECONDS


class SystemConfigurationMenuParameters(BaseEnum):
    SI_CONVERSION = InstrumentParameters.SI_CONVERSION
    WARM_UP_INTERVAL = InstrumentParameters.WARM_UP_INTERVAL
    THREE_AXIS_COMPASS = InstrumentParameters.THREE_AXIS_COMPASS
    SOLID_STATE_TILT = InstrumentParameters.SOLID_STATE_TILT
    THERMISTOR = InstrumentParameters.THERMISTOR
    PRESSURE = InstrumentParameters.PRESSURE
    AUXILIARY_1 = InstrumentParameters.AUXILIARY_1
    AUXILIARY_2 = InstrumentParameters.AUXILIARY_2
    AUXILIARY_3 = InstrumentParameters.AUXILIARY_3
    SENSOR_ORIENTATION = InstrumentParameters.SENSOR_ORIENTATION
    SERIAL_NUMBER = InstrumentParameters.SERIAL_NUMBER


class VelocityOffsetParameters(BaseEnum):
    VELOCITY_OFFSET_PATH_A = InstrumentParameters.VELOCITY_OFFSET_PATH_A
    VELOCITY_OFFSET_PATH_B = InstrumentParameters.VELOCITY_OFFSET_PATH_B
    VELOCITY_OFFSET_PATH_C = InstrumentParameters.VELOCITY_OFFSET_PATH_C
    VELOCITY_OFFSET_PATH_D = InstrumentParameters.VELOCITY_OFFSET_PATH_D


class CompassOffsetParameters(BaseEnum):
    COMPASS_OFFSET_0 = InstrumentParameters.COMPASS_OFFSET_0
    COMPASS_OFFSET_1 = InstrumentParameters.COMPASS_OFFSET_1
    COMPASS_OFFSET_2 = InstrumentParameters.COMPASS_OFFSET_2


class CompassScaleFactorsParameters(BaseEnum):
    COMPASS_SCALE_FACTORS_0 = InstrumentParameters.COMPASS_SCALE_FACTORS_0
    COMPASS_SCALE_FACTORS_1 = InstrumentParameters.COMPASS_SCALE_FACTORS_1
    COMPASS_SCALE_FACTORS_2 = InstrumentParameters.COMPASS_SCALE_FACTORS_2


class TiltOffsetParameters(BaseEnum):
    TILT_PITCH_OFFSET = InstrumentParameters.TILT_PITCH_OFFSET
    TILT_ROLL_OFFSET = InstrumentParameters.TILT_ROLL_OFFSET


class SubMenues(BaseEnum):
    ROOT = 'root_menu'
    SET_TIME = 'set_time'
    FLASH_CARD = 'flash_card'
    CALIBRATION = 'calibration'
    SLEEP = 'sleep'
    BENCH_TESTS = 'bench_tests'
    DEPLOY = 'deploy'
    OFFLOAD = 'offload'
    CONFIGURATION = 'configuration'
    PICO_DOS = 'pico_dos'
    DUMMY = 'dummy'


class Mavs4ProtocolParameterDict(ProtocolParameterDict):
    def update(self, name, response):
        response = self._param_dict[name].update(response)
        return response


class mavs4InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    Instrument driver class for MAVS-4 driver.
    Uses CommandResponseInstrumentProtocol to communicate with the device
    """

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = mavs4InstrumentProtocol(InstrumentPrompts,
                                                 INSTRUMENT_NEWLINE,
                                                 self._driver_event)


###############################################################################
# Data particles
###############################################################################
class Mavs4SampleDataParticleKey(BaseEnum):
    DATE_TIME_STRING = "date_time_string"
    ACOUSTIC_AXIS_VELOCITY_A = 'velocity_beam_a'
    ACOUSTIC_AXIS_VELOCITY_B = 'velocity_beam_b'
    ACOUSTIC_AXIS_VELOCITY_C = 'velocity_beam_c'
    ACOUSTIC_AXIS_VELOCITY_D = 'velocity_beam_d'
    VELOCITY_FRAME_UP = 'turbulent_velocity_up'
    VELOCITY_FRAME_NORTH = 'turbulent_velocity_north'
    VELOCITY_FRAME_EAST = 'turbulent_velocity_east'
    TEMPERATURE = 'temperature'
    COMPASS_MX = 'mag_comp_x'
    COMPASS_MY = 'mag_comp_y'
    PITCH = 'pitch'
    ROLL = 'roll'


class Mavs4SampleDataParticle(DataParticle):
    """
    Class for parsing sample data into a data particle structure for the MAVS-4 sensor.
    """
    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take something in the data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        match = SAMPLE_DATA_REGEX.match(self.raw_data)

        if not match:
            raise SampleException("Mavs4SampleDataParticle: No regex match of \
                                  parsed sample data: [%s]", self.raw_data)

        try:
            fractional_second = int(match.group(3))
            datetime = "%s %s.%s" % (match.group(1), match.group(2), fractional_second)
            datetime_nofrac = "%s %s" % (match.group(1), match.group(2))
            timestamp = time.strptime(datetime_nofrac, "%m %d %Y %H %M %S")
            self.set_internal_timestamp(unix_time=(timegm_to_float(timestamp) + fractional_second))
            acoustic_axis_velocity_a = struct.unpack('>h', match.group(4).decode('hex'))[0]
            acoustic_axis_velocity_b = struct.unpack('>h', match.group(5).decode('hex'))[0]
            acoustic_axis_velocity_c = struct.unpack('>h', match.group(6).decode('hex'))[0]
            acoustic_axis_velocity_d = struct.unpack('>h', match.group(7).decode('hex'))[0]
            velocity_frame_east = float(match.group(8))
            velocity_frame_north = float(match.group(9))
            velocity_frame_up = float(match.group(10))
            temperature = float(match.group(11))
            compass_mx = float(match.group(12))
            compass_my = float(match.group(13))
            pitch = float(match.group(14))
            roll = float(match.group(15))
        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, self.raw_data))

        if any([acoustic_axis_velocity_a == -0x8000,
                acoustic_axis_velocity_b == -0x8000,
                acoustic_axis_velocity_c == -0x8000,
                acoustic_axis_velocity_d == -0x8000,
                velocity_frame_east == 999,
                velocity_frame_north == 999,
                velocity_frame_up == 999]):

            self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.INVALID

        result = []

        self._add_value(result, Mavs4SampleDataParticleKey.DATE_TIME_STRING, datetime)
        self._add_value(result, Mavs4SampleDataParticleKey.ACOUSTIC_AXIS_VELOCITY_A, acoustic_axis_velocity_a, -0x8000)
        self._add_value(result, Mavs4SampleDataParticleKey.ACOUSTIC_AXIS_VELOCITY_B, acoustic_axis_velocity_b, -0x8000)
        self._add_value(result, Mavs4SampleDataParticleKey.ACOUSTIC_AXIS_VELOCITY_C, acoustic_axis_velocity_c, -0x8000)
        self._add_value(result, Mavs4SampleDataParticleKey.ACOUSTIC_AXIS_VELOCITY_D, acoustic_axis_velocity_d, -0x8000)
        self._add_value(result, Mavs4SampleDataParticleKey.VELOCITY_FRAME_EAST, velocity_frame_east, 999)
        self._add_value(result, Mavs4SampleDataParticleKey.VELOCITY_FRAME_NORTH, velocity_frame_north, 999)
        self._add_value(result, Mavs4SampleDataParticleKey.VELOCITY_FRAME_UP, velocity_frame_up, 999)
        self._add_value(result, Mavs4SampleDataParticleKey.TEMPERATURE, temperature)
        self._add_value(result, Mavs4SampleDataParticleKey.COMPASS_MX, compass_mx)
        self._add_value(result, Mavs4SampleDataParticleKey.COMPASS_MY, compass_my)
        self._add_value(result, Mavs4SampleDataParticleKey.PITCH, pitch)
        self._add_value(result, Mavs4SampleDataParticleKey.ROLL, roll)

        log.debug('Mavs4SampleDataParticle: particle=%s', result)

        return result

    @staticmethod
    def _add_value(particle, value_id, value, fill_value=None):
        """
        Append the parameter value to the particle, if it is not a fill value.
        """
        if value != fill_value:
            particle.append({DataParticleKey.VALUE_ID: value_id, DataParticleKey.VALUE: value})
        else:
            log.debug('Mavs4SampleDataParticle: parameter fill value dropped (%s = %s)', value_id, value)


class Mavs4StatusDataParticleKey(BaseEnum):
    VELOCITY_OFFSET_PATH_A = "velocity_offset_a"
    VELOCITY_OFFSET_PATH_B = "velocity_offset_b"
    VELOCITY_OFFSET_PATH_C = "velocity_offset_c"
    VELOCITY_OFFSET_PATH_D = "velocity_offset_d"
    COMPASS_OFFSET_0 = "compass_offset_0"
    COMPASS_OFFSET_1 = "compass_offset_1"
    COMPASS_OFFSET_2 = "compass_offset_2"
    COMPASS_SCALE_FACTORS_0 = "compass_scale_factor_0"
    COMPASS_SCALE_FACTORS_1 = "compass_scale_factor_1"
    COMPASS_SCALE_FACTORS_2 = "compass_scale_factor_2"
    TILT_PITCH_OFFSET = "tilt_offset_pitch"
    TILT_ROLL_OFFSET = "tilt_offset_roll"
    SAMPLE_PERIOD = "sample_period"
    SAMPLES_PER_BURST = "samples_per_burst"
    BURST_INTERVAL_DAYS = "burst_interval_days"
    BURST_INTERVAL_HOURS = "burst_interval_hours"
    BURST_INTERVAL_MINUTES = "burst_interval_minutes"
    BURST_INTERVAL_SECONDS = "burst_interval_seconds"
    SI_CONVERSION = "bin_to_si_conversion"


class Mavs4StatusDataParticle(DataParticle):
    """
    Class for constructing status data into a status particle structure for
    the MAVS-4 sensor. The raw_data variable in the DataParticle base class
    needs to be initialized to a reference to a dictionary that contains the
    status parameters.
    """
    _data_particle_type = DataParticleType.STATUS

    def _build_parsed_values(self):
        """
        Build the status particle from a dictionary of parameters adding the
        appropriate tags.
        NOTE: raw_data references a dictionary with the status parameters, not
        a line of input
        @throws SampleException If there is a problem with particle creation
        """

        if not isinstance(self.raw_data, dict):
            raise SampleException("Error: raw_data is not a dictionary")

        log.debug('Mavs4StatusDataParticle: raw_data=%s', self.raw_data)

        result = []
        for (k, v) in self.raw_data.iteritems():
            result.append({DataParticleKey.VALUE_ID: k,
                           DataParticleKey.VALUE: v})

        log.debug('Mavs4StatusDataParticle: particle=%s', result)
        return result


class mavs4InstrumentProtocol(MenuInstrumentProtocol):

    __metaclass__ = get_logging_metaclass(log_level='trace')
    """
    This protocol implements a simple command-response interaction for the
    menu based MAVs-4 instrument. It utilizes a dictionary that holds info on
    the more complex commands as well as command builders and response handles
    that can dynamically create and process the instrument interactions.
    """
    monitor_sub_parameters = (InstrumentParameters.MONITOR,
                              InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES,
                              InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT,
                              InstrumentParameters.LOG_DISPLAY_FRACTIONAL_SECOND,
                              InstrumentParameters.LOG_DISPLAY_TIME)

    burst_interval_parameters = (InstrumentParameters.BURST_INTERVAL_DAYS,
                                 InstrumentParameters.BURST_INTERVAL_HOURS,
                                 InstrumentParameters.BURST_INTERVAL_MINUTES,
                                 InstrumentParameters.BURST_INTERVAL_SECONDS)

    # Lookup dictionary which contains the response, the next command, and the
    # possible parameter name for a given instrument command if it is needed.
    # The value None for the next command means there is no next command (the
    # interaction is complete). Commands that decide how to construct the
    # command or any of these values dynamically have there own build handlers
    # and are not in this table.
    Command_Response = {InstrumentCmds.SET_TIME: [InstrumentPrompts.SET_TIME, None, None],
                        InstrumentCmds.ENTER_TIME: [InstrumentPrompts.SET_TIME, None, None],
                        InstrumentCmds.ENTER_NOTE: [InstrumentPrompts.DEPLOY_MENU, None, None],
                        InstrumentCmds.SET_VELOCITY_FRAME: [InstrumentPrompts.VELOCITY_FRAME,
                                                            InstrumentCmds.ENTER_VELOCITY_FRAME, None],
                        InstrumentCmds.ENTER_VELOCITY_FRAME: [InstrumentPrompts.DEPLOY_MENU, None, None],
                        InstrumentCmds.SET_MONITOR: [InstrumentPrompts.MONITOR, InstrumentCmds.ENTER_MONITOR, None],
                        InstrumentCmds.ENTER_LOG_DISPLAY_TIME: [InstrumentPrompts.LOG_DISPLAY,
                                                                InstrumentCmds.ENTER_LOG_DISPLAY_FRACTIONAL_SECOND,
                                                                InstrumentParameters.LOG_DISPLAY_TIME],
                        InstrumentCmds.ENTER_LOG_DISPLAY_FRACTIONAL_SECOND:
                            [InstrumentPrompts.LOG_DISPLAY,
                             InstrumentCmds.ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES,
                             InstrumentParameters.LOG_DISPLAY_FRACTIONAL_SECOND],
                        InstrumentCmds.ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES:
                            [InstrumentPrompts.LOG_DISPLAY,
                             InstrumentCmds.ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT,
                             InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES],
                        InstrumentCmds.ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT:
                            [InstrumentPrompts.LOG_DISPLAY,
                             None,
                             InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT],
                        InstrumentCmds.SET_QUERY:
                            [InstrumentPrompts.QUERY,
                             InstrumentCmds.ENTER_QUERY,
                             None],
                        InstrumentCmds.ENTER_QUERY:
                            [InstrumentPrompts.DEPLOY_MENU, None, None],
                        InstrumentCmds.SET_FREQUENCY:
                            [InstrumentPrompts.FREQUENCY,
                             InstrumentCmds.ENTER_FREQUENCY,
                             None],
                        InstrumentCmds.ENTER_FREQUENCY:
                            [InstrumentPrompts.DEPLOY_MENU, None, None],
                        InstrumentCmds.SET_MEAS_PER_SAMPLE:
                            [InstrumentPrompts.MEAS_PER_SAMPLE,
                             InstrumentCmds.ENTER_MEAS_PER_SAMPLE,
                             None],
                        InstrumentCmds.ENTER_MEAS_PER_SAMPLE:
                            [InstrumentPrompts.DEPLOY_MENU, None, None],
                        InstrumentCmds.SET_SAMPLE_PERIOD:
                            [InstrumentPrompts.SAMPLE_PERIOD,
                             InstrumentCmds.ENTER_SAMPLE_PERIOD,
                             None],
                        InstrumentCmds.ENTER_SAMPLE_PERIOD:
                            [InstrumentPrompts.DEPLOY_MENU, None, None],
                        InstrumentCmds.SET_SAMPLES_PER_BURST:
                            [InstrumentPrompts.SAMPLES_PER_BURST,
                             InstrumentCmds.ENTER_SAMPLES_PER_BURST,
                             None],
                        InstrumentCmds.ENTER_SAMPLES_PER_BURST:
                            [InstrumentPrompts.DEPLOY_MENU, None, None],
                        InstrumentCmds.SET_BURST_INTERVAL_DAYS:
                            [InstrumentPrompts.BURST_INTERVAL_DAYS,
                             InstrumentCmds.ENTER_BURST_INTERVAL_DAYS,
                             None],
                        InstrumentCmds.ENTER_BURST_INTERVAL_DAYS:
                            [InstrumentPrompts.BURST_INTERVAL_HOURS,
                             InstrumentCmds.ENTER_BURST_INTERVAL_HOURS,
                             InstrumentParameters.BURST_INTERVAL_DAYS],
                        InstrumentCmds.ENTER_BURST_INTERVAL_HOURS:
                            [InstrumentPrompts.BURST_INTERVAL_MINUTES,
                             InstrumentCmds.ENTER_BURST_INTERVAL_MINUTES,
                             InstrumentParameters.BURST_INTERVAL_HOURS],
                        InstrumentCmds.ENTER_BURST_INTERVAL_MINUTES:
                            [InstrumentPrompts.BURST_INTERVAL_SECONDS,
                             InstrumentCmds.ENTER_BURST_INTERVAL_SECONDS,
                             InstrumentParameters.BURST_INTERVAL_MINUTES],
                        InstrumentCmds.ENTER_BURST_INTERVAL_SECONDS:
                            [InstrumentPrompts.DEPLOY_MENU,
                             None,
                             InstrumentParameters.BURST_INTERVAL_SECONDS],
                        InstrumentCmds.DEPLOY_GO:
                            [InstrumentPrompts.BEGIN_MEASUREMENT, None, None],
                        InstrumentCmds.SET_SI_CONVERSION:
                            [InstrumentPrompts.SI_CONVERSION,
                             InstrumentCmds.ENTER_SI_CONVERSION,
                             None],
                        InstrumentCmds.ENTER_SI_CONVERSION:
                            [InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                             InstrumentCmds.SYSTEM_CONFIGURATION_EXIT,
                             None],
                        InstrumentCmds.SYSTEM_CONFIGURATION_EXIT:
                            [InstrumentPrompts.MAIN_MENU, None, None],
                        InstrumentCmds.SET_WARM_UP_INTERVAL:
                            [InstrumentPrompts.WARM_UP_INTERVAL,
                             InstrumentCmds.ENTER_WARM_UP_INTERVAL,
                             None],
                        InstrumentCmds.SET_THREE_AXIS_COMPASS:
                            [InstrumentPrompts.THREE_AXIS_COMPASS,
                             InstrumentCmds.ENTER_THREE_AXIS_COMPASS,
                             None],
                        InstrumentCmds.ENTER_THREE_AXIS_COMPASS:
                            [InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                             InstrumentCmds.SYSTEM_CONFIGURATION_EXIT,
                             None],
                        InstrumentCmds.SET_SOLID_STATE_TILT:
                            [InstrumentPrompts.SOLID_STATE_TILT,
                             InstrumentCmds.ENTER_SOLID_STATE_TILT,
                             None],
                        InstrumentCmds.ANSWER_SOLID_STATE_TILT_YES:
                            [InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                             InstrumentCmds.SYSTEM_CONFIGURATION_EXIT,
                             None],
                        InstrumentCmds.SET_THERMISTOR:
                            [InstrumentPrompts.THERMISTOR,
                             InstrumentCmds.ENTER_THERMISTOR,
                             None],
                        InstrumentCmds.ANSWER_THERMISTOR_NO:
                            [InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                             InstrumentCmds.SYSTEM_CONFIGURATION_EXIT,
                             None],
                        InstrumentCmds.SET_PRESSURE:
                            [InstrumentPrompts.PRESSURE,
                             InstrumentCmds.ENTER_PRESSURE,
                             None],
                        InstrumentCmds.ENTER_PRESSURE:
                            [InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                             InstrumentCmds.SYSTEM_CONFIGURATION_EXIT,
                             None],
                        InstrumentCmds.SET_SENSOR_ORIENTATION:
                            [InstrumentPrompts.SENSOR_ORIENTATION,
                             InstrumentCmds.ENTER_SENSOR_ORIENTATION,
                             None],
                        InstrumentCmds.ENTER_SENSOR_ORIENTATION:
                            [InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                             InstrumentCmds.SYSTEM_CONFIGURATION_EXIT,
                             None],
                        InstrumentCmds.VELOCITY_OFFSETS:
                            [InstrumentPrompts.VELOCITY_OFFSETS,
                             InstrumentCmds.VELOCITY_OFFSETS_SET,
                             None],
                        InstrumentCmds.VELOCITY_OFFSETS_SET:
                            [InstrumentPrompts.VELOCITY_OFFSETS_SET, None, None],
                        InstrumentCmds.COMPASS_OFFSETS:
                            [InstrumentPrompts.COMPASS_OFFSETS,
                             InstrumentCmds.COMPASS_OFFSETS_SET, None],
                        InstrumentCmds.COMPASS_OFFSETS_SET:
                            [InstrumentPrompts.COMPASS_OFFSETS_SET, None, None],
                        InstrumentCmds.COMPASS_SCALE_FACTORS:
                            [InstrumentPrompts.COMPASS_SCALE_FACTORS,
                             InstrumentCmds.COMPASS_SCALE_FACTORS_SET,
                             None],
                        InstrumentCmds.COMPASS_SCALE_FACTORS_SET:
                            [InstrumentPrompts.COMPASS_SCALE_FACTORS_SET,
                             None,
                             None],
                        InstrumentCmds.TILT_OFFSETS:
                            [InstrumentPrompts.TILT_OFFSETS,
                             InstrumentCmds.TILT_OFFSETS_SET,
                             None],
                        InstrumentCmds.TILT_OFFSETS_SET:
                            [InstrumentPrompts.TILT_OFFSETS_SET, None, None],
                        }

    def __init__(self, prompts, newline, driver_event):
        """
        """
        self.write_delay = WRITE_DELAY
        self._last_data_timestamp = None
        self.eoln = INSTRUMENT_NEWLINE
        self._location = None

        # create short alias for Directions class
        directions = MenuInstrumentProtocol.MenuTree.Directions

        # create MenuTree object for navigating to sub-menus
        menu = MenuInstrumentProtocol.MenuTree({
            SubMenues.ROOT: [],
            SubMenues.SET_TIME: [directions(InstrumentCmds.SET_TIME,
                                            InstrumentPrompts.SET_TIME)],
            SubMenues.DEPLOY: [directions(InstrumentCmds.DEPLOY_MENU,
                                          InstrumentPrompts.DEPLOY_MENU,
                                          20)],
            SubMenues.CONFIGURATION: [directions(InstrumentCmds.SYSTEM_CONFIGURATION_MENU,
                                                 InstrumentPrompts.SYSTEM_CONFIGURATION_PASSWORD),
                                      directions(InstrumentCmds.SYSTEM_CONFIGURATION_PASSWORD,
                                                 InstrumentPrompts.SYSTEM_CONFIGURATION_MENU)],
            SubMenues.CALIBRATION: [directions(InstrumentCmds.CALIBRATION_MENU,
                                               InstrumentPrompts.CALIBRATION_MENU)],
        })

        MenuInstrumentProtocol.__init__(self, menu, prompts, newline, driver_event)

        self._protocol_fsm = ThreadSafeFSM(ProtocolStates,
                                           ProtocolEvent,
                                           ProtocolEvent.ENTER,
                                           ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(ProtocolStates.UNKNOWN, ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolStates.UNKNOWN, ProtocolEvent.EXIT, self._handler_do_nothing)
        self._protocol_fsm.add_handler(ProtocolStates.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.EXIT, self._handler_do_nothing)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.START_AUTOSAMPLE,
                                       self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.SET, self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.START_DIRECT,
                                       self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.CLOCK_SYNC,
                                       self._handler_command_clock_sync)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                       self._handler_command_clock_sync)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.ENTER, self._handler_autosample_enter)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.EXIT, self._handler_do_nothing)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE,
                                       self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                       self._handler_autosample_clock_sync)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.ENTER,
                                       self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.EXIT,
                                       self._handler_do_nothing)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.EXECUTE_DIRECT,
                                       self._handler_direct_access_execute_direct)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.STOP_DIRECT,
                                       self._handler_direct_access_stop_direct)

        # Set state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolStates.UNKNOWN)

        self._build_command_handlers()

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_param_dict()
        self._build_cmd_dict()
        self._build_driver_dict()

        # create chunker for processing instrument samples.
        self._chunker = StringChunker(mavs4InstrumentProtocol.chunker_sieve_function)

        self._add_scheduler_event(ScheduledJob.CLOCK_SYNC, ProtocolEvent.SCHEDULED_CLOCK_SYNC)

        # Build a little conversion between the status particle keys and the
        # instrument parameter keys for flexibility down the road
        particle_status_dict = Mavs4StatusDataParticleKey.dict()
        params_mapping_dict = InstrumentParameters.dict()
        self.status_particle_mapping = {}
        for key in particle_status_dict.keys():
            self.status_particle_mapping[particle_status_dict[key]] = params_mapping_dict[key]

    @staticmethod
    def chunker_sieve_function(raw_data):
        # The method that detects data sample structures from instrument

        return_list = []

        for match in SAMPLE_DATA_REGEX.finditer(raw_data):
            return_list.append((match.start(), match.end()))

        return return_list

    def _filter_capabilities(self, events):
        """
        """
        events_out = [x for x in events if Capability.has(x)]
        return events_out

    def _got_chunk(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.
        Pass it to extract_sample with the appropriate particle objects and
        REGEXes.
        """
        log.debug("_got_chunk: detected structure = <%s>", structure)
        self._extract_sample(Mavs4SampleDataParticle, SAMPLE_DATA_REGEX,
                             structure, timestamp)

    ########################################################################
    # overridden superclass methods
    ########################################################################

    def _get_response(self, timeout=10, expected_prompt=None, **kwargs):
        """
        Get a response from the instrument, and do not ignore white space as in
        base class method.
        @param timeout The timeout in seconds
        @param expected_prompt Only consider the specific expected prompt as
        presented by this string
        @throw InstrumentProtocolException on timeout
        """
        # Grab time for timeout and wait for prompt.

        starttime = time.time()
        if expected_prompt is None:
            prompt_list = self._prompts.list()
        else:
            if isinstance(expected_prompt, basestring):
                prompt_list = [expected_prompt]
            else:
                prompt_list = expected_prompt

        while True:
            for item in prompt_list:
                if item in self._promptbuf:
                    return item, self._linebuf

            if time.time() > starttime + timeout:
                log.debug("_get_response: promptbuf=%r, prompt_list: %s",
                          self._promptbuf, prompt_list)
                raise InstrumentTimeoutException("in InstrumentProtocol._get_response()")

            time.sleep(.1)

    def _navigate_and_execute(self, cmd, **kwargs):
        """
        Navigate to a sub-menu and execute a list of commands instead of just
        one command as in the base class.
        @param cmds The list of commands to execute.
        @param expected_prompt optional kwarg passed through to do_cmd_resp.
        @param timeout=timeout optional wakeup and command timeout.
        @param write_delay optional kwarg passed through to do_cmd_resp.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """

        # go to root menu.
        got_prompt = False
        for i in range(10):
            try:
                self._go_to_root_menu()
                got_prompt = True
                break
            except Exception as e:
                log.info('failed to return to root menu [%r], retrying...', e)

        if not got_prompt:
            self._location = None
            raise InstrumentTimeoutException()

        # Get dest_submenu
        dest_submenu = kwargs.pop('dest_submenu', None)
        if dest_submenu is None:
            raise InstrumentParameterException('_navigate_and_execute(): dest_submenu parameter missing')

        # save timeout and expected_prompt for the execution of the actual command after any traversing of the menu
        cmd_timeout = kwargs.pop('timeout', None)
        cmd_expected_prompt = kwargs.pop('expected_prompt', None)

        if dest_submenu != self._location:
            # iterate through the menu traversing directions
            directions_list = self._menu.get_directions(dest_submenu)
            for directions in directions_list:
                log.debug('_navigate_and_execute: directions: %s', directions)
                command = directions.get_command()
                response = directions.get_response()
                timeout = directions.get_timeout()
                self._do_cmd_resp(command, expected_prompt=response, timeout=timeout, **kwargs)
            self._location = dest_submenu
        else:
            log.debug('_navigate_and_execute: took shortcut (already at %s)', dest_submenu)

        # restore timeout and expected_prompt for the execution of the actual command
        kwargs['timeout'] = cmd_timeout
        kwargs['expected_prompt'] = cmd_expected_prompt
        command = cmd
        while command is not None:
            log.debug('_navigate_and_execute: sending cmd:%s, kwargs: %s to _do_cmd_resp.',
                      command, kwargs)
            command = self._do_cmd_resp(command, **kwargs)
        return self._location, dest_submenu

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Perform a command-response on the device.
        Send commands a character at a time to spoon feed instrument so it
        doesn't drop characters!
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional command timeout.
        @retval resp_result The (possibly parsed) response result.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """

        # Get timeout and final response.
        timeout = kwargs.get('timeout', 10)
        expected_prompt = kwargs.get('expected_prompt', None)

        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException('_do_cmd_resp: Cannot build command: %s' % cmd)

        (cmd_line, expected_response, next_cmd) = build_handler(command=cmd, **kwargs)
        if expected_prompt is None:
            expected_prompt = expected_response

        # Send command.
        log.debug(
            'mavs4InstrumentProtocol._do_cmd_resp: <%r>, timeout=%s, expected_prompt=%s',
            cmd_line, timeout, expected_prompt)
        for char in cmd_line:
            self._linebuf = ''  # Clear line and prompt buffers for result.
            self._promptbuf = ''
            log.debug('mavs4InstrumentProtocol._do_cmd_resp: sending char <%s>', char)
            self._connection.send(char)
            # Wait for the character to be echoed, timeout exception
            self._get_response(timeout, expected_prompt='%s' % char)
        self._connection.send(INSTRUMENT_NEWLINE)
        log.debug('mavs4InstrumentProtocol._do_cmd_resp: command sent, looking for response')
        (prompt, result) = self._get_response(timeout, expected_prompt=expected_prompt)
        resp_handler = self._response_handlers.get(cmd, None)
        if resp_handler:
            resp_result = resp_handler(result, prompt, **kwargs)
        else:
            resp_result = None
        if next_cmd is None:
            next_cmd = resp_result
        return next_cmd

    @staticmethod
    def _float_to_string(v):
        """
        Write a float value to string formatted for "generic" set operations.
        Subclasses should overload this as needed for instrument-specific
        formatting.

        @param v A float val.
        @retval a float string formatted for "generic" set operations.
        @throws InstrumentParameterException if value is not a float.
        """

        if not isinstance(v, float):
            if isinstance(v, str):
                try:
                    float(v)
                    return v
                except ValueError:
                    raise InstrumentParameterException('Cannot coerce "%s" into a number' % v)
            raise InstrumentParameterException('Value %s is not a float.' % v)
        else:
            return str(v)

    def _build_keypress_command(self, **kwargs):
        """
        Builder for simple, non-EOLN-terminated commands
        over-ridden to return dictionary expected by this classes
        _do_cmd_resp() method

        @param cmd The command to build
        @param args Unused arguments
        @retval list with:
            The command to be sent to the device,
            The response expected from the device (set to None to indicate not
            specified),
            The next command to be sent to device (set to None to indicate not
            specified)
        """
        cmd = kwargs.get('command', None)
        if cmd is None:
            raise InstrumentParameterException('_build_keypress_command: command not specified.')
        return "%s" % cmd, None, None

    ########################################################################
    # State Unknown handlers.
    ########################################################################

    def _handler_unknown_enter(self, *args, **kwargs):
        """
        Enter unknown state.
        """
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    @staticmethod
    def _handler_do_nothing(*args, **kwargs):
        """
        Generic pass-through handler.
        """
        pass

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE.  If the
        instrument is sleeping consider that to be in command state.
        """
        next_state = ProtocolStates.COMMAND
        result = []

        # Typically the samples are at 1 Hz, but can be as infrequent as 0.01 Hz
        # Give up to 10 seconds for a particle to be present, if so, next state should be autosample
        samples = self.wait_for_particles([DataParticleType.SAMPLE], time.time()+10)
        if samples:
            next_state = ProtocolStates.AUTOSAMPLE

        # try to get root menu prompt from the device using timeout if passed.
        # NOTE: this driver always tries to put instrument into command mode
        # so that parameters can be initialized
        try:
            self._go_to_root_menu()  # this will also interrupt autosample
        except InstrumentTimeoutException:
            # didn't get root menu prompt, so indicate that there is trouble
            # with the instrument
            raise InstrumentStateException('Unknown state.')
        # got root menu prompt, so device is in command mode

        return next_state, (next_state, result)

    ########################################################################
    # State Command handlers.
    ########################################################################

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not
        recognized.
        """
        if self._init_type != InitializationType.NONE:
            self._update_params()
            self._init_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _set_param(self, parameter, value):
        """
        :param parameter:  parameter to set
        :param value:  unformatted value to set parameter
        :return:  <none>
        """
        dest_submenu = self._param_dict.get_menu_path_write(parameter)
        command = self._param_dict.get_submenu_write(parameter)
        formatted_value = self._param_dict.format(parameter, value)
        self._navigate_and_execute(command, name=parameter, value=formatted_value,
                                   dest_submenu=dest_submenu, timeout=15)

    def _set_params(self, *args, **kwargs):
        self._verify_not_readonly(*args, **kwargs)

        # Retrieve required parameter from args.
        # Raise exception if no parameter provided, or not a dict.
        try:
            params_to_set = args[0]
            if not isinstance(params_to_set, dict):
                raise InstrumentParameterException('Set parameters not a dict.')
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        old_config = self._param_dict.get_config()
        log.debug('old_config:           %r', old_config)
        log.debug('params_to_set before: %r', params_to_set)
        new_params = {}
        for key, value in params_to_set.iteritems():
            if old_config.get(key) != value:
                new_params[key] = value
        params_to_set = new_params
        log.debug('params_to_set after:  %r', params_to_set)

        enforce_defaults = [InstrumentParameters.QUERY_MODE,
                            InstrumentParameters.VELOCITY_FRAME,
                            InstrumentParameters.MONITOR,
                            InstrumentParameters.LOG_DISPLAY_TIME,
                            InstrumentParameters.LOG_DISPLAY_FRACTIONAL_SECOND,
                            InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES,
                            InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT]

        # make sure the requested settings match with enforced defaults
        for parameter in enforce_defaults:
            set_value = params_to_set.get(parameter)
            cur_value = old_config.get(parameter)
            default_value = self._param_dict.get_default_value(parameter)
            if set_value is not None:
                if set_value != default_value:
                    raise InstrumentParameterException('Parameter %s must be set to %s' % (parameter, default_value))
            if cur_value != default_value:
                log.warn('Enforcing parameter %s to required default %s', parameter, default_value)
                params_to_set[parameter] = default_value
                self._param_dict.set_value(parameter, default_value)

        # query must be set first
        query = params_to_set.pop(InstrumentParameters.QUERY_MODE, None)
        if query is not None:
            self._set_param(InstrumentParameters.QUERY_MODE, query)

        # monitor and burst parameters are set together
        monitor = set(self.monitor_sub_parameters).intersection(params_to_set)
        if monitor:
            self._set_param(InstrumentParameters.MONITOR, self._param_dict.get(InstrumentParameters.MONITOR))
            for each in monitor:
                params_to_set.pop(each)

        burst = set(self.burst_interval_parameters).intersection(params_to_set)
        if burst:
            for each in burst:
                self._param_dict.set_value(each, params_to_set.pop(each))
            self._set_param(InstrumentParameters.BURST_INTERVAL_DAYS,
                            self._param_dict.get(InstrumentParameters.BURST_INTERVAL_DAYS))

        # set the remaining parameters
        for parameter in params_to_set:
            self._set_param(parameter, params_to_set[parameter])

        # update only the deploy menu parameters
        self._get_param(InstrumentParameters.MONITOR)
        new_config = self._param_dict.get_config()
        if not dict_equal(old_config, new_config, ignore_keys=InstrumentParameters.SYS_CLOCK):
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict configuration
        @param kwargs['startup'] startup boolean: True if we are starting up,
            false otherwise
        """
        next_state = None
        self._go_to_root_menu()
        result = self._set_params(*args, **kwargs)

        return next_state, (next_state, result)

    def _handler_command_start_autosample(self, **kwargs):
        """
        Switch into autosample mode.
        @retval (next_state, result) tuple, (ProtocolStates.AUTOSAMPLE,
        None) if successful.
        """
        next_state = ProtocolStates.AUTOSAMPLE
        result = self._navigate_and_execute(InstrumentCmds.DEPLOY_GO, dest_submenu=SubMenues.DEPLOY, timeout=20,
                                            **kwargs)

        return next_state, (next_state, result)

    @staticmethod
    def _handler_command_start_direct(*args, **kwargs):
        """
        """
        next_state = ProtocolStates.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    def _clock_sync(self):
        """
        Logic for syncing the clock
        """
        str_time = get_timestamp_delayed("%m/%d/%Y %H:%M:%S")
        log.trace("_clock_sync: time set to %s" % str_time)
        dest_submenu = self._param_dict.get_menu_path_write(InstrumentParameters.SYS_CLOCK)
        command = self._param_dict.get_submenu_write(InstrumentParameters.SYS_CLOCK)
        return self._navigate_and_execute(command, name=InstrumentParameters.SYS_CLOCK, value=str_time,
                                          dest_submenu=dest_submenu, timeout=5)

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        sync clock close to a second edge
        @retval (next_state, result) tuple, (None, None) if successful.
        """
        next_state = None
        result = self._clock_sync()
        return next_state, (next_state, result)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        next_state = None

        particles = [self._generate_status_event()]

        return next_state, (next_state, particles)

    ########################################################################
    # Autosample handlers.
    ########################################################################

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        if self._init_type != InitializationType.NONE:
            self._update_params()
            self._init_params()
            self._handler_command_start_autosample()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        :retval: next_state, (next_state, result)
        """
        next_state = ProtocolStates.COMMAND
        result = []

        # Issue stop command and switch to command if successful.
        got_root_prompt = False
        for i in range(2):
            try:
                self._go_to_root_menu()
                got_root_prompt = True
                break
            except InstrumentTimeoutException:
                pass

        if not got_root_prompt:
            raise InstrumentTimeoutException()

        return next_state, (next_state, result)

    def _handler_autosample_clock_sync(self, *args, **kwargs):
        """
        Execute a clock sync from autosample mode.
        :retval: next_state, (next_state, result)
        """
        next_state = None
        result = self._clock_sync()

        return next_state, (next_state, result)

    ########################################################################
    # Direct access handlers.
    ########################################################################

    def _handler_direct_access_enter(self, *args, **kwargs):
        """
        Enter direct access state.
        """
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        self._sent_cmds = []

    def _handler_direct_access_execute_direct(self, data, *args, **kwargs):
        """
        """
        next_state = None
        result = []
        self._do_cmd_direct(data)
        return next_state, (next_state, result)

    @staticmethod
    def _handler_direct_access_stop_direct(*args, **kwargs):
        """
        @throw InstrumentProtocolException on invalid command
        """
        next_state = ProtocolStates.COMMAND
        result = []
        return next_state, (next_state, result)

    ########################################################################
    # Private helpers.
    ########################################################################

    def _generate_status_event(self):
        # only the clock will have changed
        self._get_param(InstrumentParameters.SYS_CLOCK)

        # build a dictionary of the parameters that are to be returned in the
        # status data particle
        status_params = {}
        for name in Mavs4StatusDataParticleKey.list():
            status_params[name] = self._param_dict.get(self.status_particle_mapping[name])

        # Create status data particle, but pass in a reference to the
        # dictionary just created as first parameter instead of the 'line'.
        # The status data particle class will use the 'raw_data' variable as a
        # reference to a dictionary object to get access to parameter values
        # (see the Mavs4StatusDataParticle class).
        particle = Mavs4StatusDataParticle(status_params,
                                           preferred_timestamp=DataParticleKey.DRIVER_TIMESTAMP)
        status = particle.generate()

        # send particle as an event
        self._driver_event(DriverAsyncEvent.SAMPLE, status)

        return status

    def _send_control_c(self, count):
        """
        Spoon feed the control-c characters so instrument doesn't drop them if they are sent too fast
        """
        for n in range(count):
            self._connection.send(InstrumentCmds.CONTROL_C)
            time.sleep(.1)

    def _go_to_root_menu(self):
        """
        Try to get root menu presuming the instrument is not sleeping by sending single control-C
        """
        if self._location == SubMenues.ROOT:
            log.debug('_go_to_root_menu: took shortcut (already at %s)', self._location)
            return

        for attempt in range(0, 2):
            self._linebuf = ''
            self._promptbuf = ''
            self._connection.send(InstrumentCmds.CONTROL_C)
            try:
                prompt, result = self._get_response(timeout=4,
                                                    expected_prompt=[InstrumentPrompts.MAIN_MENU,
                                                                     InstrumentPrompts.SLEEPING])
            except InstrumentTimeoutException:
                log.debug('_go_to_root_menu: TIMED_OUT WAITING FOR ROOT MENU FROM ONE CONTROL-C !')
                pass
            else:
                if prompt == InstrumentPrompts.MAIN_MENU:
                    log.debug("_go_to_root_menu: got root menu prompt")
                    self._location = SubMenues.ROOT
                    return
                if prompt == InstrumentPrompts.SLEEPING:
                    # instrument says it is sleeping, so try to wake it up
                    log.debug("_go_to_root_menu: GOT SLEEPING PROMPT !")
                    break
        # instrument acts like it's asleep, so try to wake it up and get to root menu
        count = 3  # send 3 control-c characters to get the instruments attention
        for attempt in range(0, 5):
            self._linebuf = ''
            self._promptbuf = ''
            prompt = 'no prompt received'
            log.debug("_go_to_root_menu: sending %d control-c characters to wake up sleeping instrument", count)
            self._send_control_c(count)
            try:
                prompt, result = self._get_response(timeout=4,
                                                    expected_prompt=[InstrumentPrompts.MAIN_MENU,
                                                                     InstrumentPrompts.SLEEP_WAKEUP,
                                                                     InstrumentPrompts.SLEEPING])
                log.debug("_go_to_root_menu: prompt after sending %d control-c characters = <%s>",
                          count, prompt)
                if prompt == InstrumentPrompts.MAIN_MENU:
                    self._location = SubMenues.ROOT
                    return
                if prompt == InstrumentPrompts.SLEEP_WAKEUP:
                    count = 1  # send 1 control=c to get the root menu
                if prompt == InstrumentPrompts.SLEEPING:
                    count = 3  # send 3 control-c chars to get the instruments attention
            except InstrumentTimeoutException:
                log.debug('_go_to_root_menu: TIMED_OUT WAITING FOR PROMPT FROM 3 CONTROL-Cs !')
                pass

        self._location = None
        raise InstrumentTimeoutException("failed to get to root menu.")

    @staticmethod
    def _parse_sensor_orientation(sensor_orientation):

        if 'Vertical/Down' in sensor_orientation:
            return '1'
        if 'Vertical/Up' in sensor_orientation:
            return '2'
        if 'Horizontal/Straight' in sensor_orientation:
            return '3'
        if 'Horizontal/Bent Left' in sensor_orientation:
            return '4'
        if 'Horizontal/Bent Right' in sensor_orientation:
            return '5'
        if 'Horizontal/Bent Down' in sensor_orientation:
            return '6'
        if 'Horizontal/Bent Up' in sensor_orientation:
            return '7'
        return ''

    @staticmethod
    def _parse_velocity_frame(velocity_frame):
        if 'No Velocity Frame' in velocity_frame:
            return '1'
        if '(U, V, W)' in velocity_frame:
            return '2'
        if '(E, N, W)' in velocity_frame:
            return '3'
        if '(S, \xE9, W)' in velocity_frame:
            return '4'
        return '0'

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with MAVS4 metadata information.
        """
        self._driver_dict = DriverDict()
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _build_cmd_dict(self):
        """
        Populate the command dictionary with MAVS4 metadata information. Empty
        for the MAVS4 instrument as no additional commands are supported.
        """
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name='Acquire Status', timeout=40)
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name='Start Autosample')
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, display_name='Stop Autosample', timeout=40)
        self._cmd_dict.add(Capability.CLOCK_SYNC, display_name='Synchronize Clock')
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover', timeout=59)

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with MAVS4 parameters.
        For each parameter key add value formatting function for set commands.
        """

        def bool_to_on_off(flag):
            if flag:
                return ON
            return OFF

        # The parameter dictionary.
        self._param_dict = Mavs4ProtocolParameterDict()

        # Add parameter handlers to parameter dictionary for instrument configuration parameters.
        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.SYS_CLOCK,
                           r'.*\[(.*)\].*',
                           lambda match: match.group(1),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.SET_TIME,
                           submenu_read=None,
                           menu_path_write=SubMenues.SET_TIME,
                           submenu_write=InstrumentCmds.ENTER_TIME,
                           display_name="System Clock",
                           description="Clock time (UTC) of the instrument at last clock sync event.",
                           type=ParameterDictType.STRING,
                           units="'MM/DD/YY HH:MM:SS'"))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.NOTE1,
                           r'.*Notes 1\| (.*?)\r\n.*',
                           lambda match: match.group(1),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_NOTE,
                           description="Deployment details",
                           display_name="Note Line 1",
                           type=ParameterDictType.STRING))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.NOTE2,
                           r'.*2\| (.*?)\r\n.*',
                           lambda match: match.group(1),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_NOTE,
                           description="Deployment details",
                           display_name="Note Line 2",
                           type=ParameterDictType.STRING))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.NOTE3,
                           r'.*3\| (.*?)\r\n.*',
                           lambda match: match.group(1),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_NOTE,
                           description="Deployment details",
                           display_name="Note Line 3",
                           type=ParameterDictType.STRING))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.VELOCITY_FRAME,
                           r'.*Data  F\| Velocity Frame (.*?) TTag FSec Axes.*',
                           lambda match: self._parse_velocity_frame(match.group(1)),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           startup_param=True,
                           visibility=ParameterDictVisibility.IMMUTABLE,
                           direct_access=True,
                           default_value='3',
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_VELOCITY_FRAME,
                           display_name="Velocity Frame",
                           range={'No Velocity Frame': '1', 'MAVS4(U, V, W)': '2', 'Earth(E, N, W)': '3',
                                  'Earth(S, θ, W)': '4'},
                           type=ParameterDictType.ENUM,
                           description=r"Frame type: "
                           "(1:no velocity frame | 2:MAVS4(U, V, W) | 3:Earth(E, N, W) | 4:Earth(S, θ, W)"))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.MONITOR,
                           r'.*M\| Monitor\s+(\w+).*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           value='',
                           visibility=ParameterDictVisibility.IMMUTABLE,
                           startup_param=True,
                           direct_access=True,
                           default_value=True,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_MONITOR,
                           description="Enable data monitor (true | false)",
                           display_name="Data Monitor",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.LOG_DISPLAY_TIME,
                           r'Monitor\s+(?:\w+\s+){1}(\w+)',
                           lambda match: True if match.group(1) == ON else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.IMMUTABLE,
                           startup_param=True,
                           direct_access=True,
                           default_value=True,
                           description="Enable log display time while monitoring (true | false)",
                           display_name="Log Display Time",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.LOG_DISPLAY_FRACTIONAL_SECOND,
                           r'Monitor\s+(?:\w+\s+){2}(\w+)',
                           lambda match: True if match.group(1) == ON else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.IMMUTABLE,
                           startup_param=True,
                           direct_access=True,
                           default_value=True,
                           description="Enable log/display time with fractional seconds (true | false)",
                           display_name="Display Fractional Seconds",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES,
                           r'Monitor\s+(?:\w+\s+){3}(\w+)',
                           lambda match: False if match.group(1) == OFF else True,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.IMMUTABLE,
                           startup_param=True,
                           direct_access=True,
                           default_value=True,
                           description="Enable log/display format acoustic axis velocities (true | false)",
                           display_name="Acoustic Axis Velocities",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT,
                           r'Monitor\s+(?:\w+\s+){3}(\w+)',
                           lambda match: match.group(1)[:1],
                           str,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.IMMUTABLE,
                           startup_param=True,
                           direct_access=True,
                           default_value='H',
                           description="Format: (H:Hexadecimal | D:Decimal | S:SI units cm/s) - must be Hex",
                           display_name="Format of Acoustic Axis Velocities",
                           range={'Hexadecimal': 'H', 'Decimal': 'D', 'SI Units cm/s': 'S'},
                           type=ParameterDictType.STRING))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.QUERY_MODE,
                           r'.*Q\| Query Mode\s+(\w+).*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.IMMUTABLE,
                           default_value=False,
                           startup_param=True,
                           direct_access=True,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_QUERY,
                           description="Enable query mode (true | false) - must be enabled",
                           display_name="Query Mode",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.FREQUENCY,
                           r'4\| Measurement Frequency\s+(%(float)s)\s+\[Hz\]' % common_matches,
                           lambda match: float(match.group(1)),
                           self._float_to_string,
                           regex_flags=re.DOTALL,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_FREQUENCY,
                           description="Measurement rate: (0.01 - 50.0)",
                           display_name="Measurement Frequency",
                           range=(0.01, 50.0),
                           type=ParameterDictType.FLOAT,
                           units=Units.HERTZ))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.MEASUREMENTS_PER_SAMPLE,
                           r'5\| Measurements/Sample\s+(%(int)s)\s+\[M/S\]' % common_matches,
                           lambda match: int(match.group(1)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_MEAS_PER_SAMPLE,
                           description="Number of measurements averaged: (1 - 10000)",
                           display_name="Measurements per Sample",
                           range=(1, 10000),
                           type=ParameterDictType.INT))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.SAMPLE_PERIOD,
                           '6\| Sample Period\s+(%(float)s)' % common_matches,
                           lambda match: float(match.group(1)),
                           self._float_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_SAMPLE_PERIOD,
                           description="Interval between samples: (0.02 - 10000)",
                           display_name="Sample Period",
                           range=(0.02, 10000),
                           type=ParameterDictType.FLOAT,
                           units=Units.SECOND))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.SAMPLES_PER_BURST,
                           r'7\| Samples/Burst\s+(%(int)s)\s+\[S/B\]' % common_matches,
                           lambda match: int(match.group(1)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_SAMPLES_PER_BURST,
                           description="Number of samples in a burst: (1 to 100000)",
                           display_name="Samples per Burst",
                           range=(1, 100000),
                           type=ParameterDictType.INT))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.BURST_INTERVAL_DAYS,
                           r'8\| Burst Interval\s+(%(int)s)\s+(%(int)s):(%(int)s):(%(int)s)' % common_matches,
                           lambda match: int(match.group(1)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           menu_path_read=SubMenues.DEPLOY,
                           submenu_read=None,
                           menu_path_write=SubMenues.DEPLOY,
                           submenu_write=InstrumentCmds.SET_BURST_INTERVAL_DAYS,
                           description="Day interval between bursts: (0=continuous sampling, 1 - 366)",
                           display_name="Burst Interval Days",
                           range=(0, 366),
                           type=ParameterDictType.INT,
                           units=Units.DAY))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.BURST_INTERVAL_HOURS,
                           r'8\| Burst Interval\s+(%(int)s)\s+(%(int)s):(%(int)s):(%(int)s)' % common_matches,
                           lambda match: int(match.group(2)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           description="Hour interval between bursts: (0=continuous sampling, 1 - 23)",
                           display_name="Burst Interval Hours",
                           range=(0, 23),
                           type=ParameterDictType.INT,
                           units=Units.HOUR))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.BURST_INTERVAL_MINUTES,
                           r'8\| Burst Interval\s+(%(int)s)\s+(%(int)s):(%(int)s):(%(int)s)' % common_matches,
                           lambda match: int(match.group(3)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           description="Minute interval between bursts: (0=continuous sampling, 1 - 59)",
                           display_name="Burst Interval Minutes",
                           range=(0, 59),
                           type=ParameterDictType.INT,
                           units=Units.MINUTE))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.BURST_INTERVAL_SECONDS,
                           r'8\| Burst Interval\s+(%(int)s)\s+(%(int)s):(%(int)s):(%(int)s)' % common_matches,
                           lambda match: int(match.group(4)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           description="Seconds interval between bursts: (0=continuous sampling, 1 - 59)",
                           display_name="Burst Interval Seconds",
                           range=(0, 59),
                           type=ParameterDictType.INT,
                           units=Units.SECOND))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.SI_CONVERSION,
                           r'<C> Binary to SI Conversion\s+(%(float)s)' % common_matches,
                           lambda match: float(match.group(1)),
                           self._float_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_SI_CONVERSION,
                           description="Coefficient to use during conversion from binary to SI: "
                                       "(0.0010000 - 0.0200000)",
                           display_name="SI Conversion Coefficient",
                           range=(0.001000, 0.0200000),
                           type=ParameterDictType.FLOAT))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.WARM_UP_INTERVAL,
                           r'.*<W> Warm up interval\s+(\w)\w*\s+.*',
                           lambda match: match.group(1),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_WARM_UP_INTERVAL,
                           description="Adjusts warm up time to allow for working with auxiliary sensors "
                                       "that have slower response times to get the required accuracy: "
                                       "(F:Fast | S:Slow)",
                           display_name="Warm Up Interval for Sensors",
                           range={'Fast': 'F', 'Slow': 'S'},
                           type=ParameterDictType.ENUM))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.THREE_AXIS_COMPASS,
                           r'.*<1> 3-Axis Compass\s+(\w+)\s+.*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_THREE_AXIS_COMPASS,
                           description="Enable 3-axis compass sensor (true | false)",
                           display_name="3-axis Compass Sensor",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.SOLID_STATE_TILT,
                           r'.*<2> Solid State Tilt\s+(\w+)\s+.*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_SOLID_STATE_TILT,
                           description="Enable the solid state tilt sensor: (true | false)",
                           display_name="Solid State Tilt Sensor",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.THERMISTOR,
                           r'.*<3> Thermistor\s+(\w+)\s+.*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda string: bool_to_on_off(string),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_THERMISTOR,
                           description="Enable the thermistor sensor (true | false)",
                           display_name="Thermistor Sensor",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.PRESSURE,
                           r'.*<4> Pressure\s+(\w+)\s+.*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_PRESSURE,
                           description="Enable the pressure sensor (true | false)",
                           display_name="Pressure Sensor",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.AUXILIARY_1,
                           r'.*<5> Auxiliary 1\s+(\w+)\s+.*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_AUXILIARY,
                           description="Enable auxiliary sensor 1 (true | false)",
                           display_name="Auxiliary sensor 1",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.AUXILIARY_2,
                           r'.*<6> Auxiliary 2\s+(\w+)\s+.*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_AUXILIARY,
                           description="Enable auxiliary sensor 2 (true | false)",
                           display_name="Auxiliary sensor 2",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.AUXILIARY_3,
                           r'.*<7> Auxiliary 3\s+(\w+)\s+.*',
                           lambda match: True if match.group(1) == ENABLED else False,
                           lambda x: YES if x else NO,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_AUXILIARY,
                           description="Enable auxiliary sensor 3 (true | false)",
                           display_name="Auxiliary sensor 3",
                           range={'True': True, 'False': False},
                           type=ParameterDictType.BOOL))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.SENSOR_ORIENTATION,
                           r'.*<O> Sensor Orientation\s+(.*)\n.*',
                           lambda match: self._parse_sensor_orientation(match.group(1)),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=SubMenues.CONFIGURATION,
                           submenu_write=InstrumentCmds.SET_SENSOR_ORIENTATION,
                           display_name="Sensor Orientation",
                           description="Orientation: (1:Vertical/Down | 2:Vertical/Up | 3:Horizontal/Straight | "
                                       "4:Horizontal/Bent Left | 5:Horizontal/Bent Right | 6:Horizontal/Bent Down | "
                                       "7:Horizontal/Bent Up)",
                           range={'Vertical/Down': '1', 'Vertical/Up': '2', 'Horizontal/Straight': '3',
                                  'Horizontal/Bent Left': '4', 'Horizontal/Bent Right': '5',
                                  'Horizontal/Bent Down': '6', 'Horizontal/Bent Up': '7'},
                           type=ParameterDictType.ENUM))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.SERIAL_NUMBER,
                           r'.*<S> Serial Number\s+(\w+)\s+.*',
                           lambda match: match.group(1),
                           lambda string: str(string),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CONFIGURATION,
                           submenu_read=None,
                           menu_path_write=None,
                           submenu_write=None,
                           description="The instrument serial number",
                           display_name="Serial Number",
                           type=ParameterDictType.INT))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.VELOCITY_OFFSET_PATH_A,
                           r'.*Current path offsets:\s+(\w+)\s+.*',
                           lambda match: ctypes.c_short(int(match.group(1), 16)).value,
                           lambda num: '{:04x}'.format(ctypes.c_ushort(num).value),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.VELOCITY_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           description="The velocity offset value for path A: (-3328 - 3328)",
                           display_name="Velocity Offset Path A",
                           range=(-3328, 3328),
                           type=ParameterDictType.INT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.VELOCITY_OFFSET_PATH_B,
                           r'.*Current path offsets:\s+\w+\s+(\w+)\s+.*',
                           lambda match: ctypes.c_short(int(match.group(1), 16)).value,
                           lambda num: '{:04x}'.format(ctypes.c_ushort(num).value),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.VELOCITY_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           description="The velocity offset value for path B: (-3328 - 3328)",
                           display_name="Velocity Offset Path B",
                           range=(-3328, 3328),
                           type=ParameterDictType.INT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.VELOCITY_OFFSET_PATH_C,
                           r'.*Current path offsets:\s+\w+\s+\w+\s+(\w+)\s+.*',
                           lambda match: ctypes.c_short(int(match.group(1), 16)).value,
                           lambda num: '{:04x}'.format(ctypes.c_ushort(num).value),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.VELOCITY_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           description="The velocity offset value for path C: (-3328 - 3328)",
                           display_name="Velocity Offset Path C",
                           range=(-3328, 3328),
                           type=ParameterDictType.INT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.VELOCITY_OFFSET_PATH_D,
                           r'.*Current path offsets:\s+\w+\s+\w+\s+\w+\s+(\w+)\s+.*',
                           lambda match: ctypes.c_short(int(match.group(1), 16)).value,
                           lambda num: '{:04x}'.format(ctypes.c_ushort(num).value),
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.VELOCITY_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           description="The velocity offset value for path D: (-3328 - 3328)",
                           display_name="Velocity Offset Path D",
                           range=(-3328, 3328),
                           type=ParameterDictType.INT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.COMPASS_OFFSET_0,
                           r'Current compass offsets:\s+(%(float)s)\s+(%(float)s)\s+(%(float)s)' % common_matches,
                           lambda match: int(match.group(1)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.COMPASS_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           display_name="Compass Offset 0",
                           description="The offset value for compass 0: (-400 - 400)",
                           range=(-400, 400),
                           type=ParameterDictType.INT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.COMPASS_OFFSET_1,
                           r'Current compass offsets:\s+(%(float)s)\s+(%(float)s)\s+(%(float)s)' % common_matches,
                           lambda match: int(match.group(2)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.COMPASS_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           display_name="Compass Offset 1",
                           description="The offset value for compass 1: (-400 - 400)",
                           range=(-400, 400),
                           type=ParameterDictType.INT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.COMPASS_OFFSET_2,
                           r'Current compass offsets:\s+(%(float)s)\s+(%(float)s)\s+(%(float)s)' % common_matches,
                           lambda match: int(match.group(3)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.COMPASS_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           display_name="Compass Offset 2",
                           description="The offset value for compass 2: (-400 - 400)",
                           range=(-400, 400),
                           type=ParameterDictType.INT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.COMPASS_SCALE_FACTORS_0,
                           r'Current compass scale factors:\s+(%(float)s)\s+(%(float)s)\s+(%(float)s)' % common_matches,
                           lambda match: float(match.group(1)),
                           self._float_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.COMPASS_SCALE_FACTORS,
                           menu_path_write=None,
                           submenu_write=None,
                           display_name="Compass Scale Factor 0",
                           description="The scale factor for compass 0: (0.200 - 5.000)",
                           range=(0.200, 5.000),
                           type=ParameterDictType.FLOAT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.COMPASS_SCALE_FACTORS_1,
                           r'Current compass scale factors:\s+(%(float)s)\s+(%(float)s)\s+(%(float)s)\s+' %
                           common_matches,
                           lambda match: float(match.group(2)),
                           self._float_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.COMPASS_SCALE_FACTORS,
                           menu_path_write=None,
                           submenu_write=None,
                           display_name="Compass Scale Factor 1",
                           description="The scale factor for compass 1: (0.200 - 5.000)",
                           range=(0.200, 5.000),
                           type=ParameterDictType.FLOAT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.COMPASS_SCALE_FACTORS_2,
                           r'Current compass scale factors:\s+(%(float)s)\s+(%(float)s)\s+(%(float)s)\s+' %
                           common_matches,
                           lambda match: float(match.group(3)),
                           self._float_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.COMPASS_SCALE_FACTORS,
                           menu_path_write=None,
                           submenu_write=None,
                           display_name="Compass Scale Factor 2",
                           description="The scale factor for compass 2: (0.200 - 5.000)",
                           range=(0.200, 5.000),
                           type=ParameterDictType.FLOAT,
                           units=Units.COUNTS))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.TILT_PITCH_OFFSET,
                           r'Current tilt offsets:\s+(%(int)s)\s+(%(int)s)\s+' % common_matches,
                           lambda match: int(match.group(1)),
                           self._int_to_string,
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           value=-1,  # to indicate that the parameter has not been read from the instrument
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.TILT_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           description="Tilt offset for pitch axis: (0 to 30000)",
                           display_name="Tilt Offset (Pitch)",
                           range=(0, 30000),
                           type=ParameterDictType.INT,
                           units=Prefixes.MILLI + Units.VOLT))

        self._param_dict.add_parameter(
            RegexParameter(InstrumentParameters.TILT_ROLL_OFFSET,
                           r'Current tilt offsets:\s+(%(int)s)\s+(%(int)s)\s+' % common_matches,
                           lambda match: int(match.group(2)),
                           self._int_to_string,
                           value=-1,  # to indicate that the parameter has not been read from the instrument
                           regex_flags=re.DOTALL,
                           visibility=ParameterDictVisibility.READ_ONLY,
                           menu_path_read=SubMenues.CALIBRATION,
                           submenu_read=InstrumentCmds.TILT_OFFSETS,
                           menu_path_write=None,
                           submenu_write=None,
                           description="Tilt offset for roll axis: (0 to 30000)",
                           display_name="Tilt Offset (Roll)",
                           range=(0, 30000),
                           type=ParameterDictType.INT,
                           units=Prefixes.MILLI + Units.VOLT))

    def _build_command_handlers(self):
        # these build handlers will be called by the base class during the navigate_and_execute sequence.
        self._add_build_handler(InstrumentCmds.TILT_OFFSETS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.TILT_OFFSETS_SET, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.COMPASS_SCALE_FACTORS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.COMPASS_SCALE_FACTORS_SET, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.COMPASS_OFFSETS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.COMPASS_OFFSETS_SET, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.VELOCITY_OFFSETS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.VELOCITY_OFFSETS_SET, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_SENSOR_ORIENTATION, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_SENSOR_ORIENTATION, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_AUXILIARY, self._build_enter_auxiliary_command)
        self._add_build_handler(InstrumentCmds.SET_AUXILIARY, self._build_set_auxiliary_command)
        self._add_build_handler(InstrumentCmds.ENTER_PRESSURE, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_PRESSURE, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ANSWER_THERMISTOR_NO, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ANSWER_THERMISTOR_YES, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_THERMISTOR, self._build_enter_thermistor_command)
        self._add_build_handler(InstrumentCmds.SET_THERMISTOR, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ANSWER_SOLID_STATE_TILT_YES, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_SOLID_STATE_TILT, self._build_enter_solid_state_tilt_command)
        self._add_build_handler(InstrumentCmds.SET_SOLID_STATE_TILT, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_THREE_AXIS_COMPASS, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_THREE_AXIS_COMPASS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_WARM_UP_INTERVAL, self._build_enter_warm_up_interval_command)
        self._add_build_handler(InstrumentCmds.SET_WARM_UP_INTERVAL, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_SI_CONVERSION, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_SI_CONVERSION, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_BURST_INTERVAL_SECONDS,
                                self._build_simple_sub_parameter_enter_command)
        self._add_build_handler(InstrumentCmds.ENTER_BURST_INTERVAL_MINUTES,
                                self._build_simple_sub_parameter_enter_command)
        self._add_build_handler(InstrumentCmds.ENTER_BURST_INTERVAL_HOURS,
                                self._build_simple_sub_parameter_enter_command)
        self._add_build_handler(InstrumentCmds.ENTER_BURST_INTERVAL_DAYS,
                                self._build_simple_sub_parameter_enter_command)
        self._add_build_handler(InstrumentCmds.SET_BURST_INTERVAL_DAYS, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_SAMPLES_PER_BURST, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_SAMPLES_PER_BURST, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_SAMPLE_PERIOD, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_SAMPLE_PERIOD, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_MEAS_PER_SAMPLE, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_MEAS_PER_SAMPLE, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_FREQUENCY, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_FREQUENCY, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_QUERY, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_QUERY, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT,
                                self._build_enter_log_display_acoustic_axis_velocity_format_command)
        self._add_build_handler(InstrumentCmds.ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES,
                                self._build_enter_log_display_acoustic_axis_velocities_command)
        self._add_build_handler(InstrumentCmds.ENTER_LOG_DISPLAY_FRACTIONAL_SECOND,
                                self._build_simple_sub_parameter_enter_command)
        self._add_build_handler(InstrumentCmds.ENTER_LOG_DISPLAY_TIME, self._build_simple_sub_parameter_enter_command)
        self._add_build_handler(InstrumentCmds.ENTER_MONITOR, self._build_enter_monitor_command)
        self._add_build_handler(InstrumentCmds.SET_MONITOR, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_VELOCITY_FRAME, self._build_enter_velocity_frame_command)
        self._add_build_handler(InstrumentCmds.SET_VELOCITY_FRAME, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.ENTER_NOTE, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_NOTE, self._build_set_note_command)
        self._add_build_handler(InstrumentCmds.ENTER_TIME, self._build_simple_enter_command)
        self._add_build_handler(InstrumentCmds.SET_TIME, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.DEPLOY_MENU, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.SYSTEM_CONFIGURATION_MENU, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.SYSTEM_CONFIGURATION_PASSWORD, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.SYSTEM_CONFIGURATION_EXIT, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.CALIBRATION_MENU, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.DEPLOY_GO, self._build_simple_command)

        # Add response handlers for device commands.
        self._add_response_handler(InstrumentCmds.SET_TIME,
                                   self._parse_time_response)
        self._add_response_handler(InstrumentCmds.DEPLOY_MENU,
                                   self._parse_deploy_menu_response)
        self._add_response_handler(InstrumentCmds.SYSTEM_CONFIGURATION_PASSWORD,
                                   self._parse_system_configuration_menu_response)
        self._add_response_handler(InstrumentCmds.VELOCITY_OFFSETS_SET,
                                   self._parse_velocity_offset_set_response)
        self._add_response_handler(InstrumentCmds.COMPASS_OFFSETS_SET,
                                   self._parse_compass_offset_set_response)
        self._add_response_handler(InstrumentCmds.COMPASS_SCALE_FACTORS_SET,
                                   self._parse_compass_scale_factors_set_response)
        self._add_response_handler(InstrumentCmds.TILT_OFFSETS_SET,
                                   self._parse_tilt_offset_set_response)

    def _build_enter_auxiliary_command(self, **kwargs):
        """
        Build handler for auxiliary enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('auxiliary enter command requires a name.')
        # THIS PARAMETER ONLY SUPPORTS THE NO VALUE IN THIS IMPLEMENTATION
        # THE YES VALUE WOULD REQUIRE MORE DIALOG WITH INSTRUMENT
        cmd = self._param_dict.format(name)
        log.debug("_build_enter_auxiliary_command: cmd=%s", cmd)
        return (cmd, InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                InstrumentCmds.SYSTEM_CONFIGURATION_EXIT)

    @staticmethod
    def _build_set_auxiliary_command(**kwargs):
        """
        Build handler for auxiliary set command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('set auxiliary command requires a name.')
        cmd = "%s" % (int(name[-1]) + 4)
        response = InstrumentPrompts.AUXILIARY.replace("*", name[-1])
        log.debug("_build_set_auxiliary_command: cmd=%s", cmd)
        return cmd, response, InstrumentCmds.ENTER_AUXILIARY

    def _build_enter_solid_state_tilt_command(self, **kwargs):
        """
        Build handler for solid state tilt enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('solid state tilt enter command requires a name.')
        value = kwargs.get('value', None)
        if value is None:
            raise InstrumentParameterException('solid state tilt  enter command requires a value.')
        cmd = "%s" % (self._param_dict.format(name, value)[0])
        log.debug("_build_enter_solid_state_tilt_command: cmd=%s", cmd)
        if cmd != InstrumentCmds.ANSWER_SOLID_STATE_TILT_YES:
            return (cmd, InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                    InstrumentCmds.SYSTEM_CONFIGURATION_EXIT)
        else:
            return (cmd, InstrumentPrompts.LOAD_DEFAULT_TILT,
                    InstrumentCmds.ANSWER_SOLID_STATE_TILT_YES)

    def _build_enter_thermistor_command(self, **kwargs):
        """
        Build handler for thermistor enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('thermistor enter command requires a name.')
        value = kwargs.get('value', None)
        if value is None:
            raise InstrumentParameterException('thermistor enter command requires a value.')
        cmd = "%s" % (self._param_dict.format(name, value)[0])
        log.debug("_build_enter_thermistor_command: cmd=%s", cmd)
        if cmd == InstrumentCmds.ANSWER_THERMISTOR_NO:
            return (cmd, InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                    InstrumentCmds.SYSTEM_CONFIGURATION_EXIT)
        else:
            return (InstrumentCmds.ANSWER_THERMISTOR_YES,
                    InstrumentPrompts.THERMISTOR_OFFSET,
                    InstrumentCmds.ANSWER_THERMISTOR_NO)

    def _build_enter_warm_up_interval_command(self, **kwargs):
        """
        Build handler for warm up interval enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('warm up interval enter command requires a name.')
        value = kwargs.get('value', None)
        if value is None:
            raise InstrumentParameterException('warm up interval enter command requires a value.')
        cmd = "%s" % (self._param_dict.format(name, value)[0])
        return (cmd, InstrumentPrompts.SYSTEM_CONFIGURATION_MENU,
                InstrumentCmds.SYSTEM_CONFIGURATION_EXIT)

    def _build_enter_log_display_acoustic_axis_velocities_command(self, *args, **kwargs):
        """
        Build handler for log display acoustic axis velocities enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device (set to None to indicate there isn't one for the NO cmd)
        """
        cmd = self._param_dict.get(InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES)
        log.debug("_build_enter_log_display_acoustic_axis_velocities_command: cmd=%s", cmd)
        if not cmd:
            return cmd, InstrumentPrompts.DEPLOY_MENU, None
        return YES, InstrumentPrompts.VELOCITY_FORMAT, InstrumentCmds.ENTER_LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT

    def _build_enter_log_display_acoustic_axis_velocity_format_command(self, *args, **kwargs):
        """
        Build handler for log display acoustic axis velocity format enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device (set to None to indicate there isn't one)
        """
        cmd = "%s" % (self._param_dict.get(InstrumentParameters.LOG_DISPLAY_ACOUSTIC_AXIS_VELOCITIES_FORMAT)[0])
        log.debug("_build_enter_log_display_acoustic_axis_velocity_format_command: cmd=%s", cmd)
        return cmd, InstrumentPrompts.DEPLOY_MENU, None

    def _build_simple_sub_parameter_enter_command(self, **kwargs):
        """
        Build handler for simple sub parameter enter command
        String cmd constructed by param dict formatting function.
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('simple sub parameter enter command requires a command.')
        parameter_name = self.Command_Response[cmd_name][2]
        if parameter_name is None:
            raise InstrumentParameterException('simple sub parameter enter command requires a parameter name.')
        cmd = self._param_dict.format(parameter_name)
        response = self.Command_Response[cmd_name][0]
        next_cmd = self.Command_Response[cmd_name][1]
        log.debug("_build_simple_sub_parameter_enter_command: cmd=%s", cmd)
        return cmd, response, next_cmd

    def _build_enter_monitor_command(self, **kwargs):
        """
        Build handler for monitor enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('enter monitor command requires a name.')
        value = kwargs.get('value', None)
        if value is None:
            raise InstrumentParameterException('enter monitor command requires a value.')
        cmd = self._param_dict.format(name, value)
        log.debug("_build_enter_monitor_command: cmd=%s", cmd)
        if cmd == NO:
            return cmd, InstrumentPrompts.DEPLOY_MENU, None
        return cmd, InstrumentPrompts.LOG_DISPLAY, InstrumentCmds.ENTER_LOG_DISPLAY_TIME

    def _build_enter_velocity_frame_command(self, **kwargs):
        """
        Build handler for velocity frame enter command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device (set to None to indicate there isn't one)
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('enter velocity frame command requires a name.')
        value = kwargs.get('value', None)
        if value is None:
            raise InstrumentParameterException('enter velocity frame command requires a value.')
        cmd = self._param_dict.format(name, value)
        log.debug("_build_enter_velocity_frame_command: cmd=%s", cmd)
        if value == 1:
            return cmd, InstrumentPrompts.DISPLAY_FORMAT, None

        return cmd, InstrumentPrompts.SELECTION, None

    @staticmethod
    def _build_set_note_command(**kwargs):
        """
        Build handler for note set command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('set note command requires a name.')
        cmd = "%s" % (name[-1])
        log.debug("_build_set_note_command: cmd=%s", cmd)
        return cmd, InstrumentPrompts.NOTE_INPUT, InstrumentCmds.ENTER_NOTE

    def _build_simple_enter_command(self, **kwargs):
        """
        Build handler for simple enter command
        String cmd constructed by param dict formatting function.
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('simple enter command requires a command.')
        name = kwargs.get('name', None)
        if name is None:
            raise InstrumentParameterException('simple enter command requires a name.')
        value = kwargs.get('value', None)
        if value is None:
            raise InstrumentParameterException('simple enter command requires a value.')
        response = self.Command_Response[cmd_name][0]
        next_cmd = self.Command_Response[cmd_name][1]
        log.debug("_build_simple_enter_command: cmd=%r", value)
        return value, response, next_cmd

    def _build_simple_command(self, **kwargs):
        """
        Build handler for simple set command
        @retval list with:
            The command to be sent to the device
            The response expected from the device
            The next command to be sent to device
        """
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('simple command requires a command.')
        cmd = cmd_name
        if cmd_name in self.Command_Response:
            response = self.Command_Response[cmd_name][0]
            next_cmd = self.Command_Response[cmd_name][1]
        else:
            response = None
            next_cmd = None
        log.debug("_build_simple_command: cmd=%s", cmd)
        return cmd, response, next_cmd

    def _parse_time_response(self, response, prompt, **kwargs):
        """
        Parse handler for time command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if upload command misunderstood.
        @ retval The next command to be sent to device (set to None to indicate there isn't one)
        """
        if InstrumentPrompts.GET_TIME not in response:
            raise InstrumentProtocolException('get time command not recognized by instrument: %s.' % response)

        log.debug("_parse_time_response: response=%s", response)

        if not self._param_dict.update(InstrumentParameters.SYS_CLOCK, response.splitlines()[-1]):
            log.debug('_parse_time_response: Failed to parse %s', InstrumentParameters.SYS_CLOCK)
        return None

    def _parse_deploy_menu_response(self, response, prompt, **kwargs):
        """
        Parse handler for deploy menu command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if upload command misunderstood.
        @ retval The next command to be sent to device (set to None to indicate there isn't one)
        """
        if InstrumentPrompts.DEPLOY_MENU not in response:
            raise InstrumentProtocolException('deploy menu command not recognized by instrument: %s.' % response)

        name = kwargs.get('name', None)
        if name != InstrumentParameters.ALL:
            # only get the parameter values if called from _update_params()
            return None
        for parameter in DeployMenuParameters.list():
            if not self._param_dict.update(parameter, response):
                log.debug('_parse_deploy_menu_response: Failed to parse %s', parameter)
        return None

    def _parse_system_configuration_menu_response(self, response, prompt, **kwargs):
        """
        Parse handler for system configuration menu command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if upload command misunderstood.
        @ retval The next command to be sent to device (set to None to indicate there isn't one)
        """
        if InstrumentPrompts.SYSTEM_CONFIGURATION_MENU not in response:
            raise InstrumentProtocolException(
                'system configuration menu command not recognized by instrument: %s.' % response)

        name = kwargs.get('name', None)
        if name != InstrumentParameters.ALL:
            # only get the parameter values if called from _update_params()
            return None
        for parameter in SystemConfigurationMenuParameters.list():
            if not self._param_dict.update(parameter, response):
                log.debug('_parse_system_configuration_menu_response: Failed to parse %s', parameter)
        return None

    def _parse_velocity_offset_set_response(self, response, prompt, **kwargs):
        """
        Parse handler for velocity offset set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if upload command misunderstood.
        @ retval The next command to be sent to device (set to None to indicate there isn't one)
        """
        if InstrumentPrompts.VELOCITY_OFFSETS_SET not in response:
            raise InstrumentProtocolException('velocity offset set command not recognized by instrument: %s.', response)

        name = kwargs.get('name', None)
        if name != InstrumentParameters.ALL:
            # only get the parameter values if called from _update_params()
            return None
        for parameter in VelocityOffsetParameters.list():
            if not self._param_dict.update(parameter, response):
                log.debug('_parse_velocity_offset_set_response: Failed to parse %s', parameter)
        # don't leave instrument in calibration menu because it doesn't wakeup from sleeping correctly
        self._go_to_root_menu()
        return None

    def _parse_compass_offset_set_response(self, response, prompt, **kwargs):
        """
        Parse handler for compass offset set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if upload command misunderstood.
        @ retval The next command to be sent to device (set to None to indicate there isn't one)
        """
        if InstrumentPrompts.COMPASS_OFFSETS_SET not in response:
            raise InstrumentProtocolException('compass offset set command not recognized by instrument: %s.' % response)

        name = kwargs.get('name', None)
        if name != InstrumentParameters.ALL:
            # only get the parameter values if called from _update_params()
            return None
        for parameter in CompassOffsetParameters.list():
            if not self._param_dict.update(parameter, response):
                log.debug('_parse_compass_offset_set_response: Failed to parse %s' % parameter)
        # don't leave instrument in calibration menu because it doesn't wakeup from sleeping correctly
        self._go_to_root_menu()
        return None

    def _parse_compass_scale_factors_set_response(self, response, prompt, **kwargs):
        """
        Parse handler for compass scale factors set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if upload command misunderstood.
        @ retval The next command to be sent to device (set to None to indicate there isn't one)
        """
        if InstrumentPrompts.COMPASS_SCALE_FACTORS_SET not in response:
            raise InstrumentProtocolException(
                'compass scale factors set command not recognized by instrument: %s.' % response)

        name = kwargs.get('name', None)
        if name != InstrumentParameters.ALL:
            # only get the parameter values if called from _update_params()
            return None
        for parameter in CompassScaleFactorsParameters.list():
            if not self._param_dict.update(parameter, response):
                log.debug('_parse_compass_scale_factors_set_response: Failed to parse %s', parameter)
        # don't leave instrument in calibration menu because it doesn't wakeup from sleeping correctly
        self._go_to_root_menu()
        return None

    def _parse_tilt_offset_set_response(self, response, prompt, **kwargs):
        """
        Parse handler for tilt offset set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if upload command misunderstood.
        @ retval The next command to be sent to device (set to None to indicate there isn't one)
        """
        if InstrumentPrompts.TILT_OFFSETS_SET not in response:
            raise InstrumentProtocolException('tilt offset set command not recognized by instrument: %s.' % response)

        name = kwargs.get('name', None)
        if name != InstrumentParameters.ALL:
            # only get the parameter values if called from _update_params()
            return None
        for parameter in TiltOffsetParameters.list():
            if not self._param_dict.update(parameter, response):
                log.debug('_parse_tilt_offset_set_response: Failed to parse %s', parameter)
        # don't leave instrument in calibration menu because it doesn't wakeup from sleeping correctly
        self._go_to_root_menu()
        return None

    def _get_prompt(self, timeout=8, delay=4):
        """
        _wakeup is replaced by this method for this instrument to search for
        prompt strings at other than just the end of the line.  There is no
        'wakeup' for this instrument when it is in 'deployed' mode,
        so the best that can be done is to see if it responds or not.

        Clear buffers and send some CRs to the instrument
        @param timeout The timeout to wake the device.
        @param delay The time to wait between consecutive wakeups.
        @throw InstrumentTimeoutException if the device could not be woken.
        """
        # Grab time for timeout.
        starttime = time.time()

        # get longest prompt to match by sorting the prompts longest to shortest
        prompts = self._sorted_longest_to_shortest(self._prompts.list())
        log.debug("prompts=%s", prompts)

        while True:
            # Clear the prompt buffer.
            self._promptbuf = ''

            # Send a line return and wait a 4 sec.
            log.debug('Sending newline to get a response from the instrument.')
            self._connection.send(INSTRUMENT_NEWLINE)
            time.sleep(delay)

            for item in prompts:
                if item in self._promptbuf:
                    log.debug('_get_prompt got prompt: %s', repr(item))
                    return item

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException()

    def _get_param(self, key):
        dest_submenu = self._param_dict.get_menu_path_read(key)
        command = self._param_dict.get_submenu_read(key)
        self._navigate_and_execute(command, name=InstrumentParameters.ALL, dest_submenu=dest_submenu, timeout=10)

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary. Issue the upload command. The response
        needs to be iterated through a line at a time and values saved.
        @throws InstrumentTimeoutException if device cannot be timely woken.
        @throws InstrumentProtocolException if ds/dc misunderstood.
        """
        # Get old param dict config.
        old_config = self._param_dict.get_config()

        # only get one parameter from each menu to speed up the parameter fetch process
        # get all parameters from menu 1> Time
        self._get_param(InstrumentParameters.SYS_CLOCK)
        # get all parameters from menu S> System Configuration
        self._get_param(InstrumentParameters.SOLID_STATE_TILT)
        # get all parameters from menu 3> Calibration
        self._get_param(InstrumentParameters.VELOCITY_OFFSET_PATH_A)
        self._get_param(InstrumentParameters.COMPASS_OFFSET_0)
        self._get_param(InstrumentParameters.COMPASS_SCALE_FACTORS_0)
        # get all parameters from menu 6> Deploy
        self._get_param(InstrumentParameters.MONITOR)
        # get the tilt offsets only if enabled
        if self._param_dict.get(InstrumentParameters.SOLID_STATE_TILT) == NO:
            self._param_dict.set_value(InstrumentParameters.TILT_PITCH_OFFSET, -1)
            self._param_dict.set_value(InstrumentParameters.TILT_ROLL_OFFSET, -1)
        else:
            self._get_param(InstrumentParameters.TILT_PITCH_OFFSET)

        # Get new param dict config. If it differs from the old config,
        # tell driver superclass to publish a config change event.
        new_config = self._param_dict.get_config()

        log.debug('new config: %r' % new_config)
        log.debug('old config: %r' % old_config)
        if not dict_equal(new_config, old_config, ignore_keys=InstrumentParameters.SYS_CLOCK):
            log.debug('dictionaries are not equal :(')
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    @staticmethod
    def _sorted_longest_to_shortest(param_list):
        sorted_list = sorted(param_list, key=len, reverse=True)
        return sorted_list


def create_playback_protocol(callback):
    return mavs4InstrumentProtocol(None, None, callback)
