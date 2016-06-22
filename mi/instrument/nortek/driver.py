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
import base64
import binascii
import time
from datetime import datetime, timedelta

import re

from mi.core.common import BaseEnum, Units
from mi.core.driver_scheduler import DriverSchedulerConfigKey, TriggerType
from mi.core.exceptions import InstrumentCommandException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentStateException
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.instrument.driver_dict import DriverDict, DriverDictKey
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import (CommandResponseInstrumentProtocol,
                                                    DEFAULT_WRITE_DELAY, InitializationType)
from mi.core.instrument.protocol_cmd_dict import ProtocolCommandDict
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
from mi.core.log import get_logger, get_logging_metaclass
from mi.instrument.nortek import common
from mi.instrument.nortek.particles import validate_checksum
from mi.instrument.nortek.user_configuration import UserConfigKey, UserConfiguration, UserConfigCompositeKey

__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

log = get_logger()


class ParameterConstraint(BaseEnum):
    """
    Constraints for parameters
    (type, min, max)
    """
    average_interval = (int, 1, 65535)
    cell_size = (int, 1, 65535)
    blanking_distance = (int, 1, 65535)
    coordinate_system = (int, 0, 2)  # enum 0, 1, 2
    measurement_interval = (int, 0, 65535)


class ParameterUnits(BaseEnum):
    TIME_INTERVAL = 'HH:MM:SS'
    PARTS_PER_TRILLION = 'ppt'


class ScheduledJob(BaseEnum):
    """
    List of schedulable events
    """
    CLOCK_SYNC = 'clock_sync'
    ACQUIRE_STATUS = 'acquire_status'


class InstrumentPrompts(BaseEnum):
    """
    Device prompts.
    """
    AWAKE_NACKS = '\x15\x15\x15\x15\x15\x15'
    COMMAND_MODE = 'Command mode'
    CONFIRMATION = 'Confirm:'
    Z_ACK = '\x06\x06'  # attach a 'Z' to the front of these two items to force them to the end of the list
    Z_NACK = '\x15\x15'  # so the other responses will have priority to be detected if they are present


class InstrumentCommands(BaseEnum):
    """
    List of instrument commands
    """
    CONFIGURE_INSTRUMENT = 'CC'  # sets the user configuration
    SOFT_BREAK_FIRST_HALF = '@@@@@@'
    SOFT_BREAK_SECOND_HALF = 'K1W%!Q'
    AUTOSAMPLE_BREAK = '@'
    READ_REAL_TIME_CLOCK = 'RC'
    SET_REAL_TIME_CLOCK = 'SC'
    CMD_WHAT_MODE = 'II'  # to determine the mode of the instrument
    READ_USER_CONFIGURATION = 'GC'
    READ_HW_CONFIGURATION = 'GP'
    READ_HEAD_CONFIGURATION = 'GH'
    READ_BATTERY_VOLTAGE = 'BV'
    READ_ID = 'ID'
    START_MEASUREMENT_WITHOUT_RECORDER = 'ST'
    ACQUIRE_DATA = 'AD'
    CONFIRMATION = 'MC'  # confirm a break request
    SAMPLE_WHAT_MODE = 'I'


class InstrumentCommandNames(BaseEnum):
    """
    List of instrument commands
    """
    CONFIGURE_INSTRUMENT = 'Configure>'  # sets the user configuration
    SOFT_BREAK_FIRST_HALF = 'Break Part 1'
    SOFT_BREAK_SECOND_HALF = 'Break Part 2'
    AUTOSAMPLE_BREAK = 'Stop Autosample'
    READ_REAL_TIME_CLOCK = 'Read Clock'
    SET_REAL_TIME_CLOCK = 'Set Clock>'
    CMD_WHAT_MODE = 'Command Mode'  # to determine the mode of the instrument
    READ_USER_CONFIGURATION = 'User Config'
    READ_HW_CONFIGURATION = 'Hardware Config'
    READ_HEAD_CONFIGURATION = 'Head Config'
    READ_BATTERY_VOLTAGE = 'Battery Voltage'
    READ_ID = 'Read ID'
    START_MEASUREMENT_WITHOUT_RECORDER = 'Measurement'
    ACQUIRE_DATA = 'Acquire Data'
    CONFIRMATION = 'Confirm Break'  # confirm a break request
    SAMPLE_WHAT_MODE = 'Sample Mode'


class InstrumentModes(BaseEnum):
    """
    List of possible modes the instrument can be in
    """
    FIRMWARE_UPGRADE = '\x00\x00\x06\x06'
    MEASUREMENT = '\x01\x00\x06\x06'
    COMMAND = '\x02\x00\x06\x06'
    DATA_RETRIEVAL = '\x04\x00\x06\x06'
    CONFIRMATION = '\x05\x00\x06\x06'


class ProtocolState(BaseEnum):
    """
    List of protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS
    ACQUIRING_SAMPLE = 'DRIVER_STATE_ACQUIRING_SAMPLE'


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
    GET_SAMPLE = 'DRIVER_EVENT_GET_SAMPLE'

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
    data_length = length - 2
    data_struct = '<%dH' % (data_length / 2)
    valid = validate_checksum(data_struct, input_bytes, offset=data_length)
    if not valid:
        log.debug('_check_configuration: user checksum in error')

    return valid


class Parameter(DriverParameter):
    """
    Device parameters
    """
    # composite parameters
    TIMING_CONTROL_REGISTER = UserConfigCompositeKey.TCR
    POWER_CONTROL_REGISTER = UserConfigCompositeKey.PCR
    DIAGNOSTIC_INTERVAL = UserConfigCompositeKey.DIAG_INTERVAL
    MODE = UserConfigCompositeKey.MODE
    MODE_TEST = UserConfigCompositeKey.MODE_TEST
    WAVE_MEASUREMENT_MODE = UserConfigCompositeKey.WAVE_MODE
    # translated parameters
    CLOCK_DEPLOY = UserConfigCompositeKey.DEPLOY_START_TIME
    VELOCITY_ADJ_TABLE = UserConfigCompositeKey.VELOCITY_ADJ_FACTOR
    # QUAL_CONSTANTS = UserConfigCompositeKey.FILTER_CONSTANTS
    # user configuration
    TRANSMIT_PULSE_LENGTH = UserConfigKey.TX_LENGTH
    BLANKING_DISTANCE = UserConfigKey.BLANK_DIST  # T2
    RECEIVE_LENGTH = UserConfigKey.RX_LENGTH  # T3
    TIME_BETWEEN_PINGS = UserConfigKey.TIME_BETWEEN_PINGS  # T4
    TIME_BETWEEN_BURST_SEQUENCES = UserConfigKey.TIME_BETWEEN_BURSTS  # T5
    NUMBER_PINGS = UserConfigKey.NUM_PINGS  # number of beam sequences per burst
    AVG_INTERVAL = UserConfigKey.AVG_INTERVAL
    USER_NUMBER_BEAMS = UserConfigKey.NUM_BEAMS
    COMPASS_UPDATE_RATE = UserConfigKey.COMPASS_UPDATE_RATE
    COORDINATE_SYSTEM = UserConfigKey.COORDINATE_SYSTEM
    NUMBER_BINS = UserConfigKey.NUM_CELLS
    BIN_LENGTH = UserConfigKey.CELL_SIZE
    MEASUREMENT_INTERVAL = UserConfigKey.MEASUREMENT_INTERVAL
    DEPLOYMENT_NAME = UserConfigKey.DEPLOYMENT_NAME
    WRAP_MODE = UserConfigKey.WRAP_MODE
    ADJUSTMENT_SOUND_SPEED = UserConfigKey.SOUND_SPEED_ADJUST
    NUMBER_SAMPLES_DIAGNOSTIC = UserConfigKey.NUM_DIAG_SAMPLES
    NUMBER_BEAMS_CELL_DIAGNOSTIC = UserConfigKey.NUM_BEAMS_PER_CELL
    NUMBER_PINGS_DIAGNOSTIC = UserConfigKey.NUM_PINGS_DIAG
    ANALOG_INPUT_ADDR = UserConfigKey.ANALOG_INPUT_ADDR
    SW_VERSION = UserConfigKey.SW_VER
    COMMENTS = UserConfigKey.FILE_COMMENTS
    # DYN_PERCENTAGE_POSITION = UserConfigKey.PERCENT_WAVE_CELL_POS
    # WAVE_TRANSMIT_PULSE = UserConfigKey.WAVE_TX_PULSE
    # WAVE_BLANKING_DISTANCE = UserConfigKey.FIX_WAVE_BLANK_DIST
    # WAVE_CELL_SIZE = UserConfigKey.WAVE_CELL_SIZE
    # NUMBER_DIAG_SAMPLES = UserConfigKey.NUM_DIAG_PER_WAVE
    NUMBER_SAMPLES_PER_BURST = UserConfigKey.NUM_SAMPLE_PER_BURST
    SAMPLE_RATE = UserConfigKey.SAMPLE_RATE
    ANALOG_OUTPUT_SCALE = UserConfigKey.ANALOG_SCALE_FACTOR
    CORRELATION_THRESHOLD = UserConfigKey.CORRELATION_THRS
    TRANSMIT_PULSE_LENGTH_SECOND_LAG = UserConfigKey.TX_PULSE_LEN_2ND


class EngineeringParameter(DriverParameter):
    """
    Driver Parameters (aka, engineering parameters)
    """
    CLOCK_SYNC_INTERVAL = 'ClockSyncInterval'
    ACQUIRE_STATUS_INTERVAL = 'AcquireStatusInterval'


###############################################################################
# Param dictionary helpers
###############################################################################
class NortekProtocolParameterDict(ProtocolParameterDict):
    def add_basic(self, name,
                  type=ParameterDictType.INT,
                  visibility=ParameterDictVisibility.IMMUTABLE,
                  startup_param=True,
                  direct_access=True,
                  **kwargs):
        super(NortekProtocolParameterDict, self).add(name, '', None, None,
                                                     type=type,
                                                     visibility=visibility,
                                                     startup_param=startup_param,
                                                     direct_access=direct_access,
                                                     **kwargs)


###############################################################################
# Protocol
###############################################################################
# noinspection PyUnusedLocal,PyMethodMayBeStatic
class NortekInstrumentProtocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class for Nortek driver.
    Subclasses CommandResponseInstrumentProtocol
    """
    __metaclass__ = get_logging_metaclass(log_level='debug')

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        self._protocol_fsm = ThreadSafeFSM(ProtocolState,
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
            ProtocolState.ACQUIRING_SAMPLE: [
                (ProtocolEvent.ENTER, self._handler_acquiring_sample_enter),
                (ProtocolEvent.GET_SAMPLE, self._handler_acquiring_sample_do),
                (ProtocolEvent.EXIT, self._handler_acquiring_sample_exit)
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
        self._add_build_handler(InstrumentCommands.SET_REAL_TIME_CLOCK, self._build_set_real_time_clock_command)
        self._add_build_handler(InstrumentCommands.CONFIGURE_INSTRUMENT, self._build_set_configuration)

        # Add response handlers for device commands.
        self._add_response_handler(InstrumentCommands.ACQUIRE_DATA, self._parse_acquire_data_response)
        self._add_response_handler(InstrumentCommands.CMD_WHAT_MODE, self._parse_what_mode_response)
        self._add_response_handler(InstrumentCommands.SAMPLE_WHAT_MODE, self._parse_what_mode_response)
        self._add_response_handler(InstrumentCommands.READ_REAL_TIME_CLOCK, self._parse_read_clock_response)
        self._add_response_handler(InstrumentCommands.READ_HW_CONFIGURATION, self._parse_read_hw_config)
        self._add_response_handler(InstrumentCommands.READ_HEAD_CONFIGURATION, self._parse_read_head_config)
        self._add_response_handler(InstrumentCommands.READ_USER_CONFIGURATION, self._parse_read_user_config)
        self._add_response_handler(InstrumentCommands.SOFT_BREAK_SECOND_HALF, self._parse_second_break_response)
        self._add_response_handler(InstrumentCommands.CONFIGURE_INSTRUMENT, self._parse_configure_response)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_param_dict()
        self._build_cmd_dict()
        self._build_driver_dict()

        self.velocity_sync_bytes = ''

        self._direct_commands['Newline'] = self._newline
        command_dict = InstrumentCommands.dict()
        label_dict = InstrumentCommandNames.dict()
        for key in command_dict:
            label = label_dict.get(key)
            command = command_dict[key]
            if command in [InstrumentCommands.SET_REAL_TIME_CLOCK, InstrumentCommands.CONFIGURE_INSTRUMENT]:
                self._direct_commands[label] = command
            else:
                self._direct_commands[label] = command + common.NEWLINE

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that detects data sample structures from instrument
        Should be in the format [[structure_sync_bytes, structure_len]*]
        """
        return_list = []
        sieve_matchers = common.NORTEK_COMMON_REGEXES

        for matcher in sieve_matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))
                log.debug("sieve_function: regex found %r", raw_data[match.start():match.end()])

        return return_list

    ########################################################################
    # overridden superclass methods
    ########################################################################
    def _filter_capabilities(self, events):
        """
        Filters capabilities
        """
        events_out = [x for x in events if Capability.has(x)]
        return events_out

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
        constraints = ParameterConstraint.dict()
        set_params = False

        # For each key, value in the params list set the value in parameters copy.
        for name, value in params.iteritems():
            try:
                if name in constraints:
                    var_type, minimum, maximum = constraints[name]
                    constraint_string = 'Parameter: %s Value: %s Type: %s Minimum: %s Maximum: %s' % \
                                        (name, value, var_type, minimum, maximum)
                    log.debug('SET CONSTRAINT: %s', constraint_string)
                    try:
                        var_type(value)
                    except ValueError:
                        raise InstrumentParameterException('Type mismatch: %s' % constraint_string)

                    if value < minimum or value > maximum:
                        raise InstrumentParameterException('Out of range: %s' % constraint_string)

                old_val = self._param_dict.get(name)

                if old_val != value:
                    log.debug('_set_params: setting %s to %s', name, value)
                    self._param_dict.set_value(name, value)

                    if name not in [EngineeringParameter.ACQUIRE_STATUS_INTERVAL,
                                    EngineeringParameter.CLOCK_SYNC_INTERVAL]:
                        set_params = True

            except Exception:
                self._update_params()
                raise InstrumentParameterException('Unable to set parameter %s to %s' % (name, value))

        if set_params:
            output = self._create_set_output(self._param_dict)

            result = super(NortekInstrumentProtocol, self)._do_cmd_resp(InstrumentCommands.CONFIGURE_INSTRUMENT,
                                                                        output, timeout=common.TIMEOUT,
                                                                        expected_prompt=[
                                                                            InstrumentPrompts.Z_ACK,
                                                                            InstrumentPrompts.Z_NACK])

            log.debug('_set_params: result=%s', result)
            if result == InstrumentPrompts.Z_NACK:
                self._update_params()
                raise InstrumentParameterException(
                    "Instrument rejected parameter change")

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
        self._connection.send(InstrumentCommands.SOFT_BREAK_FIRST_HALF)

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
        timeout = kwargs.get('timeout', common.TIMEOUT)
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
        @retval next_state (next_state, result)
        """
        ret_mode = self._protocol_fsm.on_event(ProtocolEvent.READ_MODE)
        prompt = ret_mode[1]

        if prompt == 0:
            log.info('FIRMWARE_UPGRADE in progress')
            raise InstrumentStateException('Firmware upgrade state.')
        elif prompt == 1:
            result = ['MEASUREMENT_MODE']
            next_state = ProtocolState.AUTOSAMPLE
        elif prompt == 2:
            result = ['COMMAND_MODE']
            next_state = ProtocolState.COMMAND
        elif prompt == 4:
            result = ['DATA_RETRIEVAL_MODE']
            next_state = ProtocolState.AUTOSAMPLE
        elif prompt == 5:
            result = ['CONFIRMATION_MODE']
            next_state = ProtocolState.AUTOSAMPLE
        else:
            raise InstrumentStateException('Unknown state: %s' % ret_mode[1])

        log.debug('_handler_unknown_discover: %s, next state=%s', result, next_state)

        return next_state, (next_state, result)

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
            log.debug("Configuring the scheduler to sync clock %s",
                      self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL))
            if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.CLOCK_SYNC_INTERVAL, ScheduledJob.CLOCK_SYNC,
                                         ProtocolEvent.CLOCK_SYNC)

        if self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL) is not None:
            log.debug("Configuring the scheduler to acquire status %s",
                      self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL))
            if self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.ACQUIRE_STATUS_INTERVAL, ScheduledJob.ACQUIRE_STATUS,
                                         ProtocolEvent.ACQUIRE_STATUS)

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
        next_state = ProtocolState.ACQUIRING_SAMPLE
        particles = []

        return next_state, (next_state, particles)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        High level command for the operator to get all of the status from the instrument from autosample state:
        Battery voltage, clock, hw configuration, head configuration, user configuration, and identification string
        """
        next_state = None
        particles = []  # TODO - need to wait for particles and return list to UI

        # break out of measurement mode in order to issue the status related commands
        self._handler_autosample_stop_autosample()
        self._handler_command_acquire_status()
        # return to measurement mode
        self._handler_command_start_autosample()

        return next_state, (next_state, particles)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        High level command for the operator to get all of the status from the instrument:
        Battery voltage, clock, hw configuration, head configuration, user configuration, and identification string
        """
        next_state = None

        # ID + BV    Call these commands at the same time, so their responses are combined (non-unique regex workaround)
        # Issue read id, battery voltage, & clock commands all at the same time (non-unique REGEX workaround).
        self._do_cmd_resp(InstrumentCommands.READ_ID + InstrumentCommands.READ_BATTERY_VOLTAGE,
                          response_regex=common.ID_BATTERY_DATA_REGEX, timeout=30)

        # RC
        self._do_cmd_resp(InstrumentCommands.READ_REAL_TIME_CLOCK, response_regex=common.CLOCK_DATA_REGEX)

        # GP
        self._do_cmd_resp(InstrumentCommands.READ_HW_CONFIGURATION, response_regex=common.HARDWARE_CONFIG_DATA_REGEX)

        # GH
        self._do_cmd_resp(InstrumentCommands.READ_HEAD_CONFIGURATION, response_regex=common.HEAD_CONFIG_DATA_REGEX)

        # GC
        self._do_cmd_resp(InstrumentCommands.READ_USER_CONFIGURATION, response_regex=common.USER_CONFIG_DATA_REGEX)

        return next_state, (next_state, [])

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @retval next_state, (next_state, result)
        """
        next_state = None
        result = []
        self._verify_not_readonly(*args, **kwargs)
        self._set_params(*args, **kwargs)

        return next_state, (next_state, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode
        @retval (next_state, next_state, result) tuple
        """
        next_state = ProtocolState.AUTOSAMPLE
        result = self._do_cmd_resp(InstrumentCommands.START_MEASUREMENT_WITHOUT_RECORDER, timeout=common.SAMPLE_TIMEOUT,
                                   *args, **kwargs)
        return next_state, (next_state, result)

    def _handler_command_start_direct(self):
        next_state = ProtocolState.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    def _handler_command_read_mode(self):
        """
        Issue read mode command.
        """
        next_state = None
        result = self._do_cmd_resp(InstrumentCommands.CMD_WHAT_MODE)
        return next_state, (next_state, result)

    def _handler_autosample_read_mode(self):
        """
        Issue read mode command.
        """
        next_state = None
        self._connection.send(InstrumentCommands.AUTOSAMPLE_BREAK)
        time.sleep(.1)
        result = self._do_cmd_resp(InstrumentCommands.SAMPLE_WHAT_MODE)
        return next_state, (next_state, result)

    def _handler_unknown_read_mode(self):
        """
        Issue read mode command.
        """
        next_state = None

        try:
            self._connection.send(InstrumentCommands.AUTOSAMPLE_BREAK)
            time.sleep(.1)
            result = self._do_cmd_resp(InstrumentCommands.SAMPLE_WHAT_MODE, timeout=0.6,
                                       response_regex=common.MODE_DATA_REGEX)
        except InstrumentTimeoutException:
            log.debug('_handler_unknown_read_mode: no response to "I", sending "II"')
            # if there is no response, catch timeout exception and issue 'II' command instead
            result = self._do_cmd_resp(InstrumentCommands.CMD_WHAT_MODE, response_regex=common.MODE_DATA_REGEX)

        return next_state, (next_state, result)

    def _clock_sync(self, *args, **kwargs):
        """
        The mechanics of synchronizing a clock
        @throws InstrumentCommandException if the clock was not synchronized
        """

        now = self._get_time_delayed()

        # Apply offset
        now = now + timedelta(seconds=common.CLOCK_SYNC_OFFSET)

        # Convert to instrument format
        str_time = now.strftime("%M %S %d %H %y %m")
        byte_time = ''
        for v in str_time.split():
            byte_time += chr(int('0x' + v, base=16))
        values = str_time.split()
        log.debug("_clock_sync: time set to %s:m %s:s %s:d %s:h %s:y %s:M (%s)",
                  values[0], values[1], values[2], values[3], values[4], values[5],
                  byte_time.encode('hex'))
        self._do_cmd_resp(InstrumentCommands.SET_REAL_TIME_CLOCK, byte_time, **kwargs)

        # Read clock back to verify time setting
        groups = self._do_cmd_resp(InstrumentCommands.READ_REAL_TIME_CLOCK, response_regex=common.CLOCK_DATA_REGEX,
                                   *args, **kwargs)
        minutes, seconds, day, hour, year, month = [int(binascii.hexlify(c)) for c in groups]
        instrument_time = datetime(year + 2000, month, day, hour, minutes, seconds)

        # Get local time and compare
        now = datetime.utcnow()
        time_diff = abs((now - instrument_time).total_seconds())

        log.debug("Instrument time: %s  Local time: %s  seconds difference: %d", instrument_time, now, time_diff)
        if time_diff > common.CLOCK_SYNC_MAX_DIFF:
            raise InstrumentCommandException("Syncing the clock did not work! Off by %s seconds" % time_diff)

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        sync clock close to a second edge
        @retval (next_state, result) tuple, (None, None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        next_state = None
        result = self._clock_sync()
        return next_state, (next_state, result)

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_autosample_clock_sync(self, *args, **kwargs):
        """
        While in autosample, sync a clock close to a second edge
        @retval next_state, (next_agent_state, result) tuple, AUTOSAMPLE, (STREAMING, None) if successful.
        """

        next_state = None
        result = None
        try:
            self._protocol_fsm._on_event(ProtocolEvent.STOP_AUTOSAMPLE)
            next_state = ProtocolState.COMMAND
            self._clock_sync()
            self._protocol_fsm._on_event(ProtocolEvent.START_AUTOSAMPLE)
            next_state = ProtocolState.AUTOSAMPLE
        finally:
            return next_state, (next_state, result)

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        if self._init_type != InitializationType.NONE:
            self._handler_autosample_stop_autosample()
            self._update_params()
            self._init_params()
            self._do_cmd_resp(InstrumentCommands.START_MEASUREMENT_WITHOUT_RECORDER, timeout=common.SAMPLE_TIMEOUT, *args,
                              **kwargs)

        if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) is not None:
            log.debug("Configuring the scheduler to sync clock %s",
                      self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL))
            if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.CLOCK_SYNC_INTERVAL, ScheduledJob.CLOCK_SYNC,
                                         ProtocolEvent.CLOCK_SYNC)

        if self._param_dict.get(EngineeringParameter.CLOCK_SYNC_INTERVAL) is not None:
            log.debug("Configuring the scheduler to acquire status %s",
                      self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL))
            if self._param_dict.get(EngineeringParameter.ACQUIRE_STATUS_INTERVAL) != '00:00:00':
                self.start_scheduled_job(EngineeringParameter.ACQUIRE_STATUS_INTERVAL, ScheduledJob.ACQUIRE_STATUS,
                                         ProtocolEvent.ACQUIRE_STATUS)

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """
        self.stop_scheduled_job(ScheduledJob.ACQUIRE_STATUS)
        self.stop_scheduled_job(ScheduledJob.CLOCK_SYNC)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @retval ProtocolState.COMMAND, (ResourceAgentState.COMMAND, None) if successful.
        @throws InstrumentProtocolException if command misunderstood or incorrect prompt received.
        """
        next_state = ProtocolState.COMMAND
        self._connection.send(InstrumentCommands.SOFT_BREAK_FIRST_HALF)
        time.sleep(.1)
        result = self._do_cmd_resp(InstrumentCommands.SOFT_BREAK_SECOND_HALF,
                                   expected_prompt=[InstrumentPrompts.CONFIRMATION, InstrumentPrompts.COMMAND_MODE],
                                   *args, **kwargs)

        log.debug('_handler_autosample_stop_autosample, ret_prompt: %s', result)

        if result == InstrumentPrompts.CONFIRMATION:
            # Issue the confirmation command.
            result = self._do_cmd_resp(InstrumentCommands.CONFIRMATION, *args, **kwargs)

        return next_state, (next_state, result)

    def stop_scheduled_job(self, schedule_job):
        """
        Remove the scheduled job
        :param schedule_job: job to be removed
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

        _ = {DriverConfigKey.SCHEDULER: {
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
    #  Acquiring Sample handlers.
    ########################################################################

    def _handler_acquiring_sample_enter(self):
        """
        enter the acquiring sample state and
        :return: new state (new state, particles)
        """

        self._async_raise_fsm_event(ProtocolEvent.GET_SAMPLE)

    def _handler_acquiring_sample_do(self):
        """
        tell the instrument to acquire data, then
        return back to the command state
        """
        next_state = ProtocolState.COMMAND
        result = self._do_cmd_resp(InstrumentCommands.ACQUIRE_DATA,
                                   expected_prompt=self.velocity_sync_bytes, timeout=common.SAMPLE_TIMEOUT)
        return next_state, (next_state, result)

    def _handler_acquiring_sample_exit(self):
        """
        exit acquiring_sample
        """

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
        result = []

        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, (next_state, result)

    def _handler_direct_access_stop_direct(self, *args, **kwargs):
        """
        Stop Direct Access, and put the driver into a healthy state by reverting itself back to the previous
        state before stopping Direct Access.
        """
        return self._handler_unknown_discover(*args, **kwargs)

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
        # return next_state, (next_state, result)

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
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, timeout=30, display_name='Acquire Status')
        self._cmd_dict.add(Capability.DISCOVER, timeout=20, display_name='Discover')

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        self._param_dict = NortekProtocolParameterDict()

        self._param_dict.add_basic(Parameter.USER_NUMBER_BEAMS,
                                   display_name="Number of Beams",
                                   description="Number of beams on the instrument.",
                                   value=3,
                                   startup_param=False)
        self._param_dict.add_basic(Parameter.TIMING_CONTROL_REGISTER,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Timing Control Register",
                                   range=(1, 65535),
                                   description="See manual for usage. (1-65535)",
                                   startup_param=False,
                                   default_value=130)
        self._param_dict.add_basic(Parameter.COMPASS_UPDATE_RATE,
                                   display_name="Compass Update Rate",
                                   description="Rate at which compass is reoriented.",
                                   default_value=1,
                                   units=Units.SECOND)
        self._param_dict.add_basic(Parameter.POWER_CONTROL_REGISTER,
                                   display_name="Power Control Register",
                                   description="See manual for usage.",
                                   startup_param=False,
                                   value=0)
        self._param_dict.add_basic(Parameter.COORDINATE_SYSTEM,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Coordinate System",
                                   range={'ENU': 0, 'XYZ': 1, 'Beam': 2},
                                   description='Coordinate System (0:ENU | 1:XYZ | 2:Beam)',
                                   default_value=2)
        self._param_dict.add_basic(Parameter.NUMBER_BINS,
                                   display_name="Number of Bins",
                                   description="Number of sampling cells.",
                                   default_value=1)
        self._param_dict.add_basic(Parameter.BIN_LENGTH,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Bin Length",
                                   range=(1, 65535),
                                   description="Length of the section of the beam used to analyze water. (1-65535)",
                                   default_value=7,
                                   units=Units.METER)
        self._param_dict.add_basic(Parameter.DEPLOYMENT_NAME,
                                   type=ParameterDictType.STRING,
                                   display_name="Deployment Name",
                                   description="Name of current deployment.",
                                   default_value='')
        self._param_dict.add_basic(Parameter.WRAP_MODE,
                                   display_name="Wrap Mode",
                                   description='Recorder wrap mode (0:no wrap | 1:wrap when full)',
                                   default_value=0,
                                   range={'No Wrap': 0, 'Wrap when Full': 1})
        self._param_dict.add_basic(Parameter.CLOCK_DEPLOY,
                                   type=ParameterDictType.LIST,
                                   display_name="Clock Deploy",
                                   description='Deployment start time.',
                                   default_value=[0, 0, 0, 0, 0, 0],
                                   units="[min, s, d, h, y, m]")
        self._param_dict.add_basic(Parameter.MODE,
                                   display_name="Mode",
                                   description="See manual for usage.",
                                   default_value=48)
        self._param_dict.add_basic(Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC,
                                   display_name="Number Beams Cell Diagnostic",
                                   description='Beams/cell number to measure in diagnostics mode.',
                                   default_value=1)
        self._param_dict.add_basic(Parameter.NUMBER_PINGS_DIAGNOSTIC,
                                   display_name="Number Pings Diagnostic",
                                   description='Pings in diagnostics/wave mode.',
                                   default_value=1)
        self._param_dict.add_basic(Parameter.MODE_TEST,
                                   type=ParameterDictType.STRING,
                                   display_name="Mode Test",
                                   description="See manual for usage.",
                                   default_value=4)
        self._param_dict.add_basic(Parameter.ANALOG_INPUT_ADDR,
                                   type=ParameterDictType.STRING,
                                   display_name="Analog Input Address",
                                   description="External input 1 and 2 to analog. Not using.",
                                   default_value=0)
        self._param_dict.add_basic(Parameter.VELOCITY_ADJ_TABLE,
                                   type=ParameterDictType.STRING,
                                   range=(-4, 40),
                                   display_name="Velocity Adj Table",
                                   description="Scaling factors to account for the speed of sound variation as a function of "
                                               "temperature and salinity.",
                                   units=ParameterUnits.PARTS_PER_TRILLION,
                                   startup_param=False)
        self._param_dict.add_basic(Parameter.COMMENTS,
                                   type=ParameterDictType.STRING,
                                   display_name="Comments",
                                   description="File comments.",
                                   default_value='')
        self._param_dict.add_basic(Parameter.NUMBER_SAMPLES_PER_BURST,
                                   expiration=None,
                                   display_name="Number of Samples per Burst",
                                   description="Number of samples to take during given period.",
                                   default_value=0)
        self._param_dict.add_basic(Parameter.CORRELATION_THRESHOLD,
                                   display_name="Correlation Threshold",
                                   description='Correlation threshold for resolving ambiguities.',
                                   default_value=0)
        self._param_dict.add_basic(Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG,
                                   display_name="Transmit Pulse Length Second Lag",
                                   description="Lag time between pulses.",
                                   units=Units.COUNTS,
                                   default_value=2)

        ############################################################################
        # ENGINEERING PARAMETERS
        ###########################################################################
        self._param_dict.add_basic(EngineeringParameter.CLOCK_SYNC_INTERVAL,
                                   type=ParameterDictType.STRING,
                                   display_name="Clock Sync Interval",
                                   description='Interval for synchronizing the clock.',
                                   units=ParameterUnits.TIME_INTERVAL,
                                   default_value='00:00:00',
                                   direct_access=False)
        self._param_dict.add_basic(EngineeringParameter.ACQUIRE_STATUS_INTERVAL,
                                   type=ParameterDictType.STRING,
                                   display_name="Acquire Status Interval",
                                   description='Interval for gathering status particles.',
                                   units=ParameterUnits.TIME_INTERVAL,
                                   default_value='00:00:00',
                                   direct_access=False)

    def _update_params(self):
        """
        Update the parameter dictionary. Issue the read config command. The response
        needs to be saved to param dictionary.
        """
        config_string = self._do_cmd_resp(InstrumentCommands.READ_USER_CONFIGURATION,
                                          response_regex=common.USER_CONFIG_DATA_REGEX)
        user_config = UserConfiguration(config_string)

        for each in Parameter.list():
            if hasattr(user_config, each):
                self._param_dict.set_value(each, getattr(user_config, each))

    def _create_set_output(self, parameters):
        user_config = UserConfiguration()

        for each in Parameter.list():
            if hasattr(user_config, each):
                try:
                    setattr(user_config, each, parameters.get(each))
                except (ValueError, TypeError):
                    log.error('Received invalid value for %s (%r)', each, parameters.get(each))
                    raise InstrumentParameterException('Received invalid value for %s' % each)

        return repr(user_config)

    def _build_command_default(self, cmd):
        return cmd

    def _build_set_real_time_clock_command(self, cmd, str_time, **kwargs):
        """
        Build the set clock command
        """
        return cmd + str_time

    def _build_set_configuration(self, cmd, str_config, **kwargs):
        return cmd + str_config

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
            log.debug("_parse_acquire_data_response: response=%r", response[start:start + len(key)])
            self._handler_autosample_stop_autosample()
            return response[start:start + len(key)]

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
        search_obj = re.search(common.MODE_DATA_REGEX, response)
        if search_obj:
            log.debug("_parse_what_mode_response: response=%r", search_obj.group(1))
            return common.convert_word_to_int(search_obj.group(1))
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
                log.debug("_parse_second_break_response: response=%r", response[start:start + len(search_prompt)])
                return response[start:start + len(search_prompt)]

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
        match = common.BATTERY_DATA_REGEX.search(response)
        if not match:
            log.error("Bad response from instrument (%r)" % response)
            raise InstrumentProtocolException("Invalid response. (%r)" % response)

        return common.convert_word_to_int(match.group(1))

    def _parse_read_clock_response(self, response, prompt):
        """
        Parse the response from the instrument for a read clock command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        """
        return response

    def _parse_configure_response(self, response, prompt):
        """
        Parse the response from the instrument for a set configuration command.

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
        match = common.ID_DATA_REGEX.search(response)
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
        if not _check_configuration(self._promptbuf, common.HW_CONFIG_SYNC_BYTES, common.HW_CONFIG_LEN):
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
        if not _check_configuration(self._promptbuf, common.HEAD_CONFIG_SYNC_BYTES, common.HEAD_CONFIG_LEN):
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
        if not _check_configuration(response, common.USER_CONFIG_SYNC_BYTES, common.USER_CONFIG_LEN):
            log.error("_parse_read_user_config: Bad read user response from instrument (%r)", response)
            raise InstrumentProtocolException("Invalid read user response. (%r)" % response)

        return response

    def _get_time_delayed(self, resolution_ms=100000):
        """
        Utility function.
        Sleeps until the current time is close to the next whole second
        :param resolution_ms: function will attempt to be accurate to this amount
        :return: current time as date_time (may be rounded up to next whole second)
        """
        now = datetime.utcnow()

        # Delay until the next whole second
        if now.microsecond > resolution_ms:
            delay_sec = 1 - now.microsecond / 1000000.0
            log.debug("delaying for %s seconds", delay_sec)
            time.sleep(delay_sec)

        now = datetime.utcnow()

        # If time is close to the next second, round up
        round_up_ms = 1000000 - now.microsecond
        if round_up_ms < resolution_ms:
            now = now + timedelta(microseconds=round_up_ms)

        return now
