#!/usr/bin/env python


"""
@package mi.instrument.satlantic.par_ser_600m.driver Satlantic PAR driver module
@file mi/instrument/satlantic/par_ser_600m/driver.py
@author Steve Foley, Ronald Ronquillo
@brief Instrument driver classes that provide structure towards interaction
with the Satlantic PAR sensor (PARAD in RSN nomenclature).
"""

__author__ = 'Ronald Ronquilllo'
__license__ = 'Apache 2.0'

import time

import re

from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()

from mi.core.common import BaseEnum, Units
from mi.core.driver_scheduler import DriverSchedulerConfigKey, TriggerType
from mi.core.instrument.driver_dict import DriverDict, DriverDictKey
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.protocol_cmd_dict import ProtocolCommandDict

from mi.core.instrument.instrument_protocol import DEFAULT_CMD_TIMEOUT, RE_PATTERN

from mi.core.common import InstErrorCode
from mi.core.instrument.instrument_fsm import ThreadSafeFSM

from mi.core.exceptions import InstrumentCommandException, InstrumentException, InstrumentTimeoutException
from mi.core.exceptions import InstrumentParameterException, InstrumentProtocolException, SampleException

from mi.core.instrument.protocol_param_dict import ParameterDictVisibility, ParameterDictType
from mi.core.instrument.chunker import StringChunker

from mi.core.instrument.data_particle import DataParticle, DataParticleKey, DataParticleValue, CommonDataParticleType

####################################################################
# Module-wide values
####################################################################

INSTRUMENT = 'SATPAR'

# ex SATPAR4278190306,55713.85,2206748544,234
SAMPLE_PATTERN = r'SATPAR(?P<sernum>\d+),(?P<timer>\d+\.\d+),(?P<counts>\d+),(?P<checksum>\d+)\r\n'
SAMPLE_REGEX = re.compile(SAMPLE_PATTERN)

SAMPLE_NEW_PATTERN = R'SATPRL(?P<sernum>\d+),(?P<timer>[-+]?\d+\.\d+),(?P<PAR>[-+]?\d+\.\d+),(?P<pitch>[-+]?\d+\.\d+),(?P<roll>[-+]?\d+\.\d+),(?P<itemp>[-+]?\d+\.\d+),(?P<amode>LOG|LIN),(?P<counts>[-+]?\d+),(?P<adcv>[-+]?\d+\.\d+),(?P<vout>[-+]?\d+\.\d+),(?P<xaxis>[-+]?\d+),(?P<yaxis>[-+]?\d+),(?P<zaxis>[-+]?\d+),(?P<tcounts>[-+]?\d+),(?P<tvolts>[-+]?\d+\.\d+),(?P<status>[-+]?\d+),(?P<checksum>\d+)\r\n'
SAMPLE_NEW_REGEX = re.compile(SAMPLE_NEW_PATTERN)

HEADER_PATTERN = r'S/N: (?P<sernum>\d+)\r\nFirmware: (?P<firm>\S+)\r\n'
HEADER_REGEX = re.compile(HEADER_PATTERN)

COMMAND_PATTERN = 'Command Console'
COMMAND_REGEX = re.compile(COMMAND_PATTERN)

MAXRATE_PATTERN = 'Maximum Frame Rate:\s+(?P<maxrate>\d+\.?\d*) Hz'
MAXRATE_REGEX = re.compile(MAXRATE_PATTERN)

# 9600, 19200, 38400, and 57600.
BAUDRATE_PATTERN = 'Telemetry Baud Rate:\s+(?P<baud>\d{4,5}) bps'
BAUDRATE_REGEX = re.compile(BAUDRATE_PATTERN)

MAXANDBAUDRATE_PATTERN = '%s\r\n\%s' % (MAXRATE_PATTERN, BAUDRATE_PATTERN)
MAXANDBAUDRATE_REGEX = re.compile(MAXANDBAUDRATE_PATTERN)

GET_PATTERN = '^show (?P<param>.*)\r\n(?P<resp>.+)\r?\n?(?P<respbaud>.*)\r?\n?\$'
GET_REGEX = re.compile(GET_PATTERN)

INIT_PATTERN = 'Initializing system. Please wait...'

INTERVAL_TIME_REGEX = r"([0-9][0-9]:[0-9][0-9]:[0-9][0-9])"

VALID_MAXRATES = (0, 0.125, 0.5, 1, 2, 4, 8, 10, 12)
EOLN = "\r\n"

TIMEOUT = 15


class ParameterUnits(BaseEnum):
    TIME_INTERVAL = 'HH:MM:SS'


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    PARSED = 'parad_sa_sample'
    SCIENCE = 'parad_sample'
    CONFIG = 'parad_sa_config'


class EngineeringParameter(DriverParameter):
    """
    Driver Parameters (aka, engineering parameters)
    """
    ACQUIRE_STATUS_INTERVAL = 'AcquireStatusInterval'


class ScheduledJob(BaseEnum):
    """
    List of schedulable events
    """
    ACQUIRE_STATUS = 'acquire_status'


####################################################################
# Static enumerations for this class
####################################################################
class Commands(BaseEnum):
    SAVE = 'save'
    EXIT = 'exit'
    GET = 'show'
    SET = 'set'
    BREAK = '\x03'                 # CTRL-C
    SWITCH_TO_POLL = '\x13'        # CTRL-S
    SWITCH_TO_AUTOSAMPLE = '\x01'  # CTRL-A
    SAMPLE = '\x0D'                # CR

    # PARAD-A instruments (Satlantic digital PAR) enter an error state when a soft reset command (exit! OR ctrl-R)
    # is received. The only known remedy once the instrument gets in this state is to cycle power.
    # As a work-around, the following commands must never be implemented:
    #
    # RESET = '\x12'  #CTRL-R
    # EXIT_AND_RESET = 'exit!'


class CommandNames(BaseEnum):
    SAVE = 'Save'
    EXIT = 'Exit'
    GET = 'Show>'
    SET = 'Set>'
    BREAK = 'Interrupt'
    SWITCH_TO_POLL = 'Polling Mode'
    SWITCH_TO_AUTOSAMPLE = 'Autosample'
    SAMPLE = 'Sample'


class PARProtocolState(BaseEnum):
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    UNKNOWN = DriverProtocolState.UNKNOWN
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class PARProtocolEvent(BaseEnum):
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    SCHEDULED_ACQUIRE_STATUS = "DRIVER_EVENT_SCHEDULED_ACQUIRE_STATUS"


class PARCapability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    ACQUIRE_SAMPLE = PARProtocolEvent.ACQUIRE_SAMPLE
    ACQUIRE_STATUS = PARProtocolEvent.ACQUIRE_STATUS
    START_AUTOSAMPLE = PARProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = PARProtocolEvent.STOP_AUTOSAMPLE
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    DISCOVER = DriverEvent.DISCOVER


class Parameter(DriverParameter):
    MAXRATE = 'maxrate'
    FIRMWARE = 'firmware'
    SERIAL = 'serial'
    ACQUIRE_STATUS_INTERVAL = EngineeringParameter.ACQUIRE_STATUS_INTERVAL


class PARProtocolError(BaseEnum):
    INVALID_COMMAND = "Invalid command"


class Prompt(BaseEnum):
    """
    Command Prompts
    """
    COMMAND = '$'
    NULL = ''
    ENTER_EXIT_CMD_MODE = '\x0c'
    SAMPLES = 'SATPAR'


###############################################################################
# Satlantic PAR Sensor Driver.
###############################################################################
class SatlanticPARInstrumentDriver(SingleConnectionInstrumentDriver):
    """
    The InstrumentDriver class for the Satlantic PAR sensor PARAD.
    @note If using this via Ethernet, must use a delayed send
    or commands may not make it to the PAR successfully. This is accomplished
    below sending a command one character at a time & confirmation each character
    arrived in the line buffer. This is more reliable then an arbitrary delay time,
    as the digi may buffer characters & attempt to send more then one character at once.
    Note that single character control commands need not be delayed.
    """

    def _build_protocol(self):
        """ Construct driver protocol state machine """
        self._protocol = SatlanticPARInstrumentProtocol(self._driver_event)


class PARDataKey(BaseEnum):
    SERIAL_NUM = "serial_number"
    COUNTS = "par"
    TIMER = "elapsed_time"


class PARDataKeyNew(BaseEnum):
    SERIAL_NUM = "serial_number"
    TIMER = "elapsed_time"
    PAR = "par_measured"
    PITCH = "pitch"
    ROLL = "roll"
    TEMP = "temp_interior"
    COUNTS = "par"
    V_IN = "input_voltage"
    V_OUT = "voltage_out"
    X_AXIS = "x_accel_counts"
    Y_AXIS = "y_accel_counts"
    Z_AXIS = "z_accel_counts"
    T_COUNTS = "raw_internal_temp"
    T_VOLTS = "temperature_volts"


class PARParticleBase(DataParticle):
    """
    Virtual base class for the PARAD data particle types.
    """

    def _build_parsed_values(self):
        raise NotImplemented

    @staticmethod
    def _checksum_check(data, checksum_value, line_end):
        """
        Confirm that the checksum is valid for the data line
        @param data The entire line of data, including the checksum
        @retval True if the checksum fits, False if the checksum is bad
        """
        line = data[:line_end + 1]
        # Calculate checksum on line
        checksum = 0
        for char in line:
            checksum += ord(char)

        checksum = (~checksum + 0x01) & 0xFF

        if checksum != checksum_value:
            log.warn("Calculated checksum %s did not match packet checksum %s.", checksum, checksum_value)
            return False
        return True


class PARParticle(PARParticleBase):
    """
    Routines for parsing raw data into a data particle structure for the Satlantic PAR sensor.
    Overrides the building of values, and the rest comes along for free.
    """
    _data_particle_type = DataParticleType.PARSED

    def _build_parsed_values(self):
        """
        Take something in the sample format and split it into PAR values (with an appropriate tag)
        @throws SampleException If there is a problem with sample creation
        """
        match = SAMPLE_REGEX.match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" % self.raw_data)

        try:
            sernum = match.group('sernum')
            timer = float(match.group('timer'))
            counts = int(match.group('counts'))
        except ValueError:
            log.warn("_build_parsed_values")
            raise SampleException('malformed particle - missing required value(s)')
        except TypeError:
            log.warn("_build_parsed_values")
            raise SampleException('malformed particle - missing required value(s)')

        if not self._checksum_check(self.raw_data):
            self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

        result = [{DataParticleKey.VALUE_ID: PARDataKey.SERIAL_NUM, DataParticleKey.VALUE: sernum},
                  {DataParticleKey.VALUE_ID: PARDataKey.TIMER, DataParticleKey.VALUE: timer},
                  {DataParticleKey.VALUE_ID: PARDataKey.COUNTS, DataParticleKey.VALUE: counts}]

        return result

    def _checksum_check(self, data):
        status = False
        match = SAMPLE_REGEX.match(data)
        if not match:
            return status
        try:
            received_checksum = int(match.group('checksum'))
            line_end = match.start('checksum') - 1
            status = PARParticleBase._checksum_check(data, received_checksum, line_end)
        except IndexError:
            # Didn't have a checksum!
            return status
        return status


class PARParticleNew(PARParticleBase):
    """
    Routines for parsing raw data into a data particle structure for the Satlantic PAR sensor.
    Overrides the building of values, and the rest comes along for free.
    """

    _data_particle_type = DataParticleType.PARSED

    def _build_parsed_values(self):
        """
        Take something in the sample format and split it into PAR values (with an appropriate tag)
        @throws SampleException If there is a problem with sample creation
        """
        match = SAMPLE_NEW_REGEX.match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" % self.raw_data)

        try:
            sernum = match.group('sernum')
            timer = float(match.group('timer'))
            par = float(match.group('PAR'))
            pitch = float(match.group('pitch'))
            roll = float(match.group('roll'))
            itemp = float(match.group('itemp'))
            counts = int(match.group('counts'))
            v_in = float(match.group('adcv'))
            v_out = float(match.group('vout'))
            xaxis = int(match.group('xaxis'))
            yaxis = int(match.group('yaxis'))
            zaxis = int(match.group('zaxis'))
            tcount = int(match.group('tcounts'))
            tvolts = float(match.group('tvolts'))
        except ValueError:
            log.warn("_build_parsed_values")
            raise SampleException('malformed particle - missing required value(s)')
        except TypeError:
            log.warn("_build_parsed_values")
            raise SampleException('malformed particle - missing required value(s)')

        if not self._checksum_check(self.raw_data):
            self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

        result = [{DataParticleKey.VALUE_ID: PARDataKeyNew.SERIAL_NUM, DataParticleKey.VALUE: sernum},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.TIMER, DataParticleKey.VALUE: timer},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.PAR, DataParticleKey.VALUE: par},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.PITCH, DataParticleKey.VALUE: pitch},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.ROLL, DataParticleKey.VALUE: roll},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.TEMP, DataParticleKey.VALUE: itemp},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.COUNTS, DataParticleKey.VALUE: counts},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.V_IN, DataParticleKey.VALUE: v_in},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.V_OUT, DataParticleKey.VALUE: v_out},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.X_AXIS, DataParticleKey.VALUE: xaxis},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.Y_AXIS, DataParticleKey.VALUE: yaxis},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.Z_AXIS, DataParticleKey.VALUE: zaxis},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.T_COUNTS, DataParticleKey.VALUE: tcount},
                  {DataParticleKey.VALUE_ID: PARDataKeyNew.T_VOLTS, DataParticleKey.VALUE: tvolts}]

        return result

    def _checksum_check(self, data):
        status = False
        match = SAMPLE_NEW_REGEX.match(data)
        if not match:
            return status
        try:
            received_checksum = int(match.group('checksum'))
            line_end = match.start('checksum') - 1
            status = PARParticleBase._checksum_check(data, received_checksum, line_end)
        except IndexError:
            # Didn't have a checksum!
            return status
        return status


class SatlanticPARConfigParticleKey(BaseEnum):
    BAUD_RATE = "parad_telbaud"
    MAX_RATE = "parad_maxrate"
    SERIAL_NUM = "serial_number"
    FIRMWARE = "parad_firmware"
    TYPE = "parad_type"


class SatlanticPARConfigParticle(DataParticle):
    """
    Routines for parsing raw data into a config particle structure for the Satlantic PAR sensor.
    Overrides the building of values, and the rest comes along for free.
    Serial Number, Firmware, & Instrument are read only values retrieved from the param dictionary.
    """
    def __init__(self, serial_num, firmware, *args, **kwargs):
        self._serial_num = serial_num
        self._firmware = firmware
        super(SatlanticPARConfigParticle, self).__init__(*args, **kwargs)

    _data_particle_type = DataParticleType.CONFIG

    def _build_parsed_values(self):
        """
        Take something in the "show configuration" format and split it into PARAD configuration values
        @throws SampleException If there is a problem with sample creation
        """
        match = MAXANDBAUDRATE_REGEX.match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" % self.raw_data)

        try:
            maxrate = float(match.group('maxrate'))
            baud = int(match.group('baud'))
        except ValueError:
            log.warn("_build_parsed_values")
            raise SampleException('malformed particle - missing required value(s)')
        except TypeError:
            log.warn("_build_parsed_values")
            raise SampleException('malformed particle - missing required value(s)')

        log.trace("_build_parsed_values: %s, %s, %s, %s, %s",
                  maxrate, baud, self._serial_num, self._firmware, INSTRUMENT)

        result = [{DataParticleKey.VALUE_ID: SatlanticPARConfigParticleKey.BAUD_RATE, DataParticleKey.VALUE: baud},
                  {DataParticleKey.VALUE_ID: SatlanticPARConfigParticleKey.MAX_RATE, DataParticleKey.VALUE: maxrate},
                  {DataParticleKey.VALUE_ID: SatlanticPARConfigParticleKey.SERIAL_NUM,
                   DataParticleKey.VALUE: self._serial_num},
                  {DataParticleKey.VALUE_ID: SatlanticPARConfigParticleKey.FIRMWARE,
                   DataParticleKey.VALUE: self._firmware},
                  {DataParticleKey.VALUE_ID: SatlanticPARConfigParticleKey.TYPE,
                   DataParticleKey.VALUE: INSTRUMENT}]

        return result


####################################################################
# Satlantic PAR Sensor Protocol
####################################################################
class SatlanticPARInstrumentProtocol(CommandResponseInstrumentProtocol):
    """The instrument protocol classes to deal with a Satlantic PAR sensor.
    The protocol is a very simple command/response protocol with a few show
    commands and a few set commands.
    Note protocol state machine must be called "self._protocol_fsm"
    """

    __metaclass__ = get_logging_metaclass(log_level='debug')

    def __init__(self, callback=None):
        CommandResponseInstrumentProtocol.__init__(self, Prompt, EOLN, callback)

        self._protocol_fsm = ThreadSafeFSM(PARProtocolState, PARProtocolEvent, PARProtocolEvent.ENTER, PARProtocolEvent.EXIT)

        self._protocol_fsm.add_handler(PARProtocolState.UNKNOWN, PARProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(PARProtocolState.UNKNOWN, PARProtocolEvent.DISCOVER, self._handler_unknown_discover)

        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.SET, self._handler_command_set)
        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.ACQUIRE_SAMPLE, self._handler_poll_acquire_sample)
        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.ACQUIRE_STATUS, self._handler_acquire_status)
        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.SCHEDULED_ACQUIRE_STATUS, self._handler_acquire_status)
        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(PARProtocolState.COMMAND, PARProtocolEvent.START_DIRECT, self._handler_command_start_direct)

        self._protocol_fsm.add_handler(PARProtocolState.AUTOSAMPLE, PARProtocolEvent.ENTER, self._handler_autosample_enter)
        self._protocol_fsm.add_handler(PARProtocolState.AUTOSAMPLE, PARProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(PARProtocolState.AUTOSAMPLE, PARProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(PARProtocolState.AUTOSAMPLE, PARProtocolEvent.SCHEDULED_ACQUIRE_STATUS, self._handler_autosample_acquire_status)

        self._protocol_fsm.add_handler(PARProtocolState.DIRECT_ACCESS, PARProtocolEvent.ENTER, self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(PARProtocolState.DIRECT_ACCESS, PARProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct)
        self._protocol_fsm.add_handler(PARProtocolState.DIRECT_ACCESS, PARProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct)

        self._protocol_fsm.start(PARProtocolState.UNKNOWN)

        self._add_response_handler(Commands.GET, self._parse_get_response)
        self._add_response_handler(Commands.SET, self._parse_set_response)
        self._add_response_handler(Commands.SAMPLE, self._parse_response)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_cmd_dict()
        self._build_driver_dict()

        self._param_dict.add(Parameter.MAXRATE,
                             MAXRATE_PATTERN,
                             lambda match: float(match.group(1)),
                             self._float_or_int_to_string,
                             direct_access=True,
                             startup_param=True,
                             init_value=4,
                             display_name='Max Rate',
                             description='Maximum sampling rate (0 (Auto) | 0.125 | 0.5 | 1 | 2 | 4 | 8 | 10 | 12)',
                             range={'Auto': 0, '0.125': 0.125, '0.5': 0.5, '1': 1, '2': 2, '4': 4, '8': 8, '10': 10,
                                    '12': 12},
                             type=ParameterDictType.FLOAT,
                             units=Units.HERTZ,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.SERIAL,
                             HEADER_PATTERN,
                             lambda match: match.group('sernum'),
                             str,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name='Serial Number',
                             description="",
                             type=ParameterDictType.STRING)

        self._param_dict.add(Parameter.FIRMWARE,
                             HEADER_PATTERN,
                             lambda match: match.group('firm'),
                             str,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name='Firmware Version',
                             description="",
                             type=ParameterDictType.STRING)

        self._param_dict.add(Parameter.ACQUIRE_STATUS_INTERVAL,
                             INTERVAL_TIME_REGEX,
                             lambda match: match.group(1),
                             str,
                             display_name="Acquire Status Interval",
                             description='Interval for gathering status particles.',
                             type=ParameterDictType.STRING,
                             units=ParameterUnits.TIME_INTERVAL,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             default_value='00:00:00',
                             startup_param=True)

        self._chunker = StringChunker(SatlanticPARInstrumentProtocol.sieve_function)

        command_dict = Commands.dict()
        label_dict = CommandNames.dict()
        for key in command_dict:
            label = label_dict.get(key)
            command = command_dict[key]
            if command in [CommandNames.SET, CommandNames.GET]:
                command += ' '
            self._direct_commands[label] = command

    def _build_cmd_dict(self):
        """
        Build a command dictionary structure, load the strings for the metadata from a file if present.
        """
        self._cmd_dict = ProtocolCommandDict()
        self._cmd_dict.add(PARCapability.ACQUIRE_SAMPLE, display_name='Acquire Sample')
        self._cmd_dict.add(PARCapability.ACQUIRE_STATUS, display_name='Acquire Status')
        self._cmd_dict.add(PARCapability.START_AUTOSAMPLE, display_name='Start Autosample', timeout=40)
        self._cmd_dict.add(PARCapability.STOP_AUTOSAMPLE, display_name='Stop Autosample', timeout=40)
        self._cmd_dict.add(PARCapability.DISCOVER, display_name='Discover', timeout=50)

    def _build_driver_dict(self):
        """
        Build a driver dictionary structure, load the strings for the metadata from a file if present.
        """
        self._driver_dict = DriverDict()
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        """
        matchers = [SAMPLE_REGEX, SAMPLE_NEW_REGEX, MAXANDBAUDRATE_REGEX, HEADER_REGEX]
        return_list = []

        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))
                log.trace("sieve_function: regex found %r", raw_data[match.start():match.end()])

        return return_list

    def _filter_capabilities(self, events):
        """
        """
        events_out = [x for x in events if PARCapability.has(x)]
        return events_out

    def _do_cmd(self, cmd, *args, **kwargs):
        """
        Issue a command to the instrument after clearing of buffers.

        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @retval The fully built command that was sent
        @raises InstrumentProtocolException if command could not be built.
        """
        expected_prompt = kwargs.get('expected_prompt', None)
        cmd_line = self._build_default_command(cmd, *args)

        # Send command.
        if len(cmd_line) == 1:
            self._connection.send(cmd_line)
        else:
            for char in cmd_line:
                starttime = time.time()
                self._connection.send(char)
                while len(self._promptbuf) == 0 or char not in self._promptbuf[-1]:
                    time.sleep(0.0015)
                    if time.time() > starttime + 3:
                        break

            # Keep for reference: This is a reliable alternative, but not fully explained & may not work in the future.
            # It somehow corrects bit rate timing issues across the driver-digi-instrument network interface,
            # & allows the entire line of a commands to be sent successfully.
            if EOLN not in cmd_line:    # Note: Direct access commands may already include an EOLN
                time.sleep(0.115)
                starttime = time.time()
                self._connection.send(EOLN)
                while EOLN not in self._promptbuf[len(cmd_line):len(cmd_line) + 2] and Prompt.ENTER_EXIT_CMD_MODE \
                           not in self._promptbuf[len(cmd_line):len(cmd_line) + 2]:
                    time.sleep(0.0015)
                    if time.time() > starttime + 3:
                        break

            if cmd is Commands.EXIT:
                self._connection.send(EOLN)

        return cmd_line

    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        Issue a command to the instrument after clearing of buffers. No response is handled as a result of the command.
        Overridden: special "write delay" & command resending
        reliability improvements, no need for wakeup, default build command used for all commands
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @raises InstrumentProtocolException if command could not be built.
        """
        self._do_cmd(cmd, *args, **kwargs)

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Perform a command-response on the device. Overridden: special "write delay" & command resending
        reliability improvements, no need for wakeup, default build command used for all commands
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param expected_prompt kwarg offering a specific prompt to look for
        other than the ones in the protocol class itself.
        @param response_regex kwarg with a compiled regex for the response to
        match. Groups that match will be returned as a string.
        Cannot be supplied with expected_prompt. May be helpful for instruments that do not have a prompt.
        @retval resp_result The (possibly parsed) response result including the
        first instance of the prompt matched. If a regex was used, the prompt
        will be an empty string and the response will be the joined collection of matched groups.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response was not recognized.
        """
        timeout = kwargs.get('timeout', DEFAULT_CMD_TIMEOUT)
        expected_prompt = kwargs.get('expected_prompt', None)
        response_regex = kwargs.get('response_regex', None)

        if response_regex and not isinstance(response_regex, RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        retry_count = 5
        retry_num = 0
        cmd_line = ""
        result = ""
        prompt = ""
        for retry_num in xrange(retry_count):
            # Clear line and prompt buffers for result.
            self._linebuf = ''
            self._promptbuf = ''

            cmd_line = self._do_cmd(cmd, *args, **kwargs)

            # Wait for the prompt, prepare result and return, timeout exception
            if response_regex:
                result_tuple = self._get_response(timeout, response_regex=response_regex,
                                                  expected_prompt=expected_prompt)
                result = "".join(result_tuple)
            else:
                (prompt, result) = self._get_response(timeout, expected_prompt=expected_prompt)

            # Confirm the entire command was sent, otherwise resend retry_count number of times
            if len(cmd_line) > 1 and \
                (expected_prompt is not None or
                (response_regex is not None))\
                    and not result.startswith(cmd_line):
                    # and cmd_line not in result:
                log.debug("_do_cmd_resp: Send command: %r failed %r attempt, result = %r.", cmd, retry_num, result)
                if retry_num >= retry_count:
                    raise InstrumentCommandException('_do_cmd_resp: Failed %d attempts sending command: %r' %
                                                     (retry_count, cmd))
            else:
                break

        log.debug("_do_cmd_resp: Sent command: %r, %d reattempts, expected_prompt=%r, result=%r.",
                  cmd_line, retry_num, expected_prompt, result)

        resp_handler = self._response_handlers.get((self.get_current_state(), cmd), None) or \
            self._response_handlers.get(cmd, None)
        resp_result = None
        if resp_handler:
            resp_result = resp_handler(result, prompt)

        time.sleep(0.3)     # give some time for the instrument connection to keep up

        return resp_result

    ########################################################################
    # Unknown handlers.
    ########################################################################
    def _handler_unknown_enter(self):
        """
        Enter unknown state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_unknown_discover(self):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE.
        """
        result = []
        try:
            probe_resp = self._do_cmd_resp(Commands.SAMPLE, timeout=2,
                                           expected_prompt=[Prompt.SAMPLES, PARProtocolError.INVALID_COMMAND])
        except InstrumentTimeoutException:
            self._do_cmd_resp(Commands.SWITCH_TO_AUTOSAMPLE, expected_prompt=Prompt.SAMPLES, timeout=15)
            next_state = PARProtocolState.AUTOSAMPLE
            return next_state, (next_state, result)

        if probe_resp == PARProtocolError.INVALID_COMMAND:
            next_state = PARProtocolState.COMMAND
        else:
            # Put the instrument into full autosample in case it isn't already (could be in polled mode)
            result = self._do_cmd_resp(Commands.SWITCH_TO_AUTOSAMPLE, expected_prompt=Prompt.SAMPLES, timeout=15)
            next_state = PARProtocolState.AUTOSAMPLE

        return next_state, (next_state, result)

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_command_enter(self):
        """
        Enter command state.
        """
        # Command device to update parameters and send a config change event.
        if self._init_type != InitializationType.NONE:
            self._update_params()
            # we need to briefly start sampling so we can stop sampling
            # and get the serial number and firmware version
            self._do_cmd_no_resp(Commands.EXIT)
            self._send_break()

        self._init_params()
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _update_params(self):
        """
        Fetch the parameters from the device, and update the param dict.
        """
        max_rate_response = self._do_cmd_resp(Commands.GET, Parameter.MAXRATE, expected_prompt=Prompt.COMMAND)
        self._param_dict.update(max_rate_response)

    def _set_params(self, params, startup=False, *args, **kwargs):
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

        scheduling_interval_changed = False
        instrument_params_changed = False
        old_config = self._param_dict.get_all()

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set params requires a parameter dict.')

        self._verify_not_readonly(params, startup)

        if Parameter.ACQUIRE_STATUS_INTERVAL in params:

            old_val = self._param_dict.format(Parameter.ACQUIRE_STATUS_INTERVAL)
            new_val = self._param_dict.format(Parameter.ACQUIRE_STATUS_INTERVAL,
                                              params[Parameter.ACQUIRE_STATUS_INTERVAL])
            if old_val != new_val:
                valid_value_regex = r'^\d{2}:[0-5]\d:[0-5]\d$'
                range_checker = re.compile(valid_value_regex)
                if range_checker.match(new_val):
                    self._setup_scheduler_config(new_val)
                    self._param_dict.set_value(Parameter.ACQUIRE_STATUS_INTERVAL, new_val)
                else:
                    raise InstrumentParameterException("invalid time string parameter for acquire status interval")

        for name, value in params.iteritems():

            old_val = self._param_dict.format(name)
            new_val = self._param_dict.format(name, params[name])

            log.debug('Changing param %r OLD = %r, NEW %r', name, old_val, new_val)

            if name == Parameter.MAXRATE:
                if value not in VALID_MAXRATES:
                    raise InstrumentParameterException("Maxrate %s out of range" % value)

                if old_val != new_val:
                    if self._do_cmd_resp(Commands.SET, name, new_val, expected_prompt=Prompt.COMMAND):
                        instrument_params_changed = True
            elif name == Parameter.ACQUIRE_STATUS_INTERVAL:
                pass
            else:
                raise InstrumentParameterException("Parameter not in dictionary: %s" % name)

        if instrument_params_changed:
            self._do_cmd_resp(Commands.SAVE, expected_prompt=Prompt.COMMAND)
            self._update_params()

        new_config = self._param_dict.get_all()
        log.debug("Updated parameter dict: old_config = %r, new_config = %r", old_config, new_config)
        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        for name in params.keys():
            if self._param_dict.format(name, params[name]) != self._param_dict.format(name):
                raise InstrumentParameterException('Failed to update parameter: %r' % name)

    def _setup_scheduler_config(self, event_value):
        """
        Set up auto scheduler configuration.
        """
        try:
            interval = event_value.split(':')
            hours = int(interval[0])
            minutes = int(interval[1])
            seconds = int(interval[2])
            log.debug("Setting scheduled interval to: %s %s %s", hours, minutes, seconds)
        except(KeyError, ValueError) as e:
            log.debug("invalid value for acquire status interval: %r", e)
            raise InstrumentParameterException('invalid value for Acquire Status Interval: ' + event_value)

        if DriverConfigKey.SCHEDULER in self._startup_config:
            self._startup_config[DriverConfigKey.SCHEDULER][ScheduledJob.ACQUIRE_STATUS] = {
                DriverSchedulerConfigKey.TRIGGER: {
                    DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.INTERVAL,
                    DriverSchedulerConfigKey.HOURS: int(hours),
                    DriverSchedulerConfigKey.MINUTES: int(minutes),
                    DriverSchedulerConfigKey.SECONDS: int(seconds)}
            }
        else:

            self._startup_config[DriverConfigKey.SCHEDULER] = {
                ScheduledJob.ACQUIRE_STATUS: {
                    DriverSchedulerConfigKey.TRIGGER: {
                        DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.INTERVAL,
                        DriverSchedulerConfigKey.HOURS: int(hours),
                        DriverSchedulerConfigKey.MINUTES: int(minutes),
                        DriverSchedulerConfigKey.SECONDS: int(seconds)}
                },
            }

        # Start the scheduler if it is not running
        if not self._scheduler:
            self.initialize_scheduler()

        # First remove the scheduler, if it exists
        if not self._scheduler_callback.get(ScheduledJob.ACQUIRE_STATUS) is None:
            self._remove_scheduler(ScheduledJob.ACQUIRE_STATUS)
            log.debug("Removed scheduler for acquire status")

        # Now Add the scheduler
        if hours > 0 or minutes > 0 or seconds > 0:
            self._add_scheduler_event(ScheduledJob.ACQUIRE_STATUS, PARProtocolEvent.SCHEDULED_ACQUIRE_STATUS)

    def _handler_command_set(self, *args, **kwargs):
        """
        Handle setting data from command mode.
        @param params Dict of the parameters and values to pass to the state
        """
        next_state = None
        result = []
        self._set_params(*args, **kwargs)
        return next_state, result

    def _handler_command_start_autosample(self):
        """
        Handle getting a start autosample event when in command mode
        """
        next_state = PARProtocolState.AUTOSAMPLE
        result = []

        self._do_cmd_resp(Commands.EXIT, expected_prompt=Prompt.SAMPLES, timeout=15)
        time.sleep(0.115)
        self._do_cmd_resp(Commands.SWITCH_TO_AUTOSAMPLE, expected_prompt=Prompt.SAMPLES, timeout=15)

        return next_state, (next_state, result)

    def _handler_command_start_direct(self):
        next_state = PARProtocolState.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_autosample_enter(self):
        """
        Handle PARProtocolState.AUTOSAMPLE PARProtocolEvent.ENTER
        """
        next_state = None
        result = []

        if self._init_type != InitializationType.NONE:
            next_state, (_, result) = self._handler_autosample_stop_autosample()
            self._update_params()
            self._init_params()
            next_state, (_, result) = self._handler_command_start_autosample()

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        return next_state, (next_state, result)

    def _handler_autosample_stop_autosample(self):
        """
        Handle PARProtocolState.AUTOSAMPLE stop
        @throw InstrumentProtocolException For hardware error
        """
        next_state = PARProtocolState.COMMAND
        try:
            self._send_break()
            result = ['Autosample break successful, returning to command mode']
        except InstrumentException, e:
            log.debug("_handler_autosample_stop_autosample error: %s", e)
            raise InstrumentProtocolException(error_code=InstErrorCode.HARDWARE_ERROR,
                                              msg="Couldn't break from autosample!")

        return next_state, (next_state, result)

    def _handler_autosample_acquire_status(self):
        """
        High level command for the operator to get the status from the instrument in autosample state
        """
        try:
            next_state, (_, result) = self._handler_autosample_stop_autosample()
            next_state, (_, result) = self._handler_acquire_status()
            next_state, (_, result) = self._handler_command_start_autosample()

        # Since this is registered only for autosample mode, make sure this ends w/ instrument in autosample mode
        except InstrumentTimeoutException:
            next_state, (_, result) = self._handler_unknown_discover()
            if next_state != DriverProtocolState.AUTOSAMPLE:
                next_state, (_, result) = self._handler_command_start_autosample()

        return next_state, (next_state, result)

    ########################################################################
    # Poll handlers.
    ########################################################################
    def _get_poll(self):
        self._do_cmd_resp(Commands.EXIT, expected_prompt=Prompt.SAMPLES, timeout=15)
        # switch to poll
        time.sleep(0.115)
        self._connection.send(Commands.SWITCH_TO_POLL)
        # return to command mode
        time.sleep(0.115)
        self._do_cmd_resp(Commands.BREAK, response_regex=COMMAND_REGEX, timeout=5)

    def _handler_poll_acquire_sample(self):
        """
        Handle PARProtocolEvent.ACQUIRE_SAMPLE
        @retval return (next state, result)
        """
        next_state = None
        timeout = time.time() + TIMEOUT

        self._get_poll()

        particles = self.wait_for_particles([DataParticleType.PARSED, DataParticleType.SCIENCE], timeout)

        return next_state, (next_state, particles)

    def _handler_acquire_status(self):
        """
        Return parad_sa_config particle containing telbaud, maxrate, serial number, firmware, & type
        Retrieve both telbaud & maxrate from instrument with a "show all" command,
        the last three values are retrieved from the values stored in the param dictionary
        and combined through the got_chunk function.
        @retval return (next state, result)
        """
        next_state = None
        timeout = time.time() + TIMEOUT

        self._do_cmd_resp(Commands.GET, "all", expected_prompt=Prompt.COMMAND)

        particles = self.wait_for_particles([DataParticleType.CONFIG], timeout)

        return next_state, (next_state, particles)

    ########################################################################
    # Direct access handlers.
    ########################################################################
    def _handler_direct_access_enter(self):
        """
        Enter direct access state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
        self._sent_cmds = []

    def _handler_direct_access_exit(self):
        """
        Exit direct access state.
        """
        pass

    def _do_cmd_direct(self, cmd):
        """
        Issue an untranslated command to the instrument. No response is handled as a result of the command.
        Overridden: Use _do_cmd to send commands reliably. Remove if digi-serial interface is ever fixed.

        @param cmd The high level command to issue
        """
        self._do_cmd(cmd)

    def _handler_direct_access_execute_direct(self, data):
        """
        Execute Direct Access command(s)
        """
        next_state = None
        self._do_cmd_direct(data)
        # add sent command to list for 'echo' filtering in callback
        result = self._sent_cmds.append(data)
        return next_state, (next_state, [result])

    def _handler_direct_access_stop_direct(self):
        """
        Stop Direct Access, and put the driver into a healthy state
        """
        next_state, (_, result) = self._handler_unknown_discover()
        return next_state, (next_state, result)

    ###################################################################
    # Builders
    ###################################################################
    def _build_default_command(self, *args):
        """
        Join each command component into a string with spaces in between
        """
        return " ".join(str(x) for x in args)

    ##################################################################
    # Response parsers
    ##################################################################
    def _parse_response(self, response, prompt):
        """
        Default response handler
        @param response What was sent back from the command that was sent
        @param prompt The prompt that was returned from the device
        """
        return prompt

    def _parse_set_response(self, response, prompt):
        """
        Determine if a set was successful or not
        @param response What was sent back from the command that was sent
        @param prompt The prompt that was returned from the device
        """
        if PARProtocolError.INVALID_COMMAND in response:
            return InstErrorCode.SET_DEVICE_ERR
        elif prompt == Prompt.COMMAND:
            return True
        else:
            return InstErrorCode.HARDWARE_ERROR

    def _parse_get_response(self, response, prompt):
        """
        Parse the response from the instrument for a couple of different query responses.
        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The numerical value of the parameter
        @raise InstrumentProtocolException When a bad response is encountered
        """
        match = GET_REGEX.search(response)
        if not match:
            log.warn("Bad response from instrument")
            raise InstrumentProtocolException("Invalid response. Bad command? %s" % response)
        else:
            log.debug("_parse_get_response: response=%r", match.group(1, 2))
            return match.group('resp')

    ###################################################################
    # Helpers
    ###################################################################
    @staticmethod
    def _float_or_int_to_string(v):
        """
        Write a float or int value to string formatted for "generic" set operations.
        Overloaded to print ints and floats without trailing zeros after the decimal point.
        Also supports passing through a "None" value for the empty param dictionary in startup.
        @param v A float or int val.
        @retval a numerical string formatted for "generic" set operations.
        @throws InstrumentParameterException if value is not a float or an int.
        """
        if isinstance(v, float):
            return ('%0.3f' % v).rstrip('0').rstrip('.')
        elif isinstance(v, int):
            return '%d' % v
        elif v is None:
            return None
        else:
            raise InstrumentParameterException('Value %s is not a float or an int.' % v)

    def _send_break_poll(self):
        """
        Send stop auto poll commands (^S) and wait to confirm success based on current max rate setting.
        Note: Current maxrate can be either 0(maximum output), 0.125, 0.5, 1, 2, 4, 8, 10, or 12.
        At maxrates above 4, sending a single stop auto poll command is highly unreliable.
        Generally, a Digital PAR sensor cannot exceed a frame rate faster than 7.5 Hz.
        """
        send_flag = True
        starttime = time.time()
        current_maxrate = self._param_dict.get(Parameter.MAXRATE)
        if current_maxrate is None:
            current_maxrate = 0.125     # During startup, assume the slowest sample rate
        elif current_maxrate <= 0 or current_maxrate > 8:
            current_maxrate = 8
        time_between_samples = (1.0 / current_maxrate) + 1

        log.trace("_send_break_poll: maxrate = %s", current_maxrate)

        while True:
            if send_flag:
                if current_maxrate < 8:
                    self._connection.send(Commands.SWITCH_TO_POLL)
                else:
                    # Send a burst of stop auto poll commands for high maxrates
                    for _ in xrange(25):    # 25 x 0.15 seconds = 3.75 seconds
                        self._connection.send(Commands.SWITCH_TO_POLL)
                        time.sleep(.15)
                send_flag = False
            time.sleep(0.1)

            # Check for incoming samples. Reset timer & resend stop command if found.
            if SAMPLE_REGEX.search(self._promptbuf) or SAMPLE_NEW_REGEX.search(self._promptbuf):
                self._promptbuf = ''
                starttime = time.time()
                send_flag = True

            # Wait the appropriate amount of time to confirm samples are no longer arriving
            elif time.time() > starttime + time_between_samples:
                break

        # For high maxrates, give some time for the buffer to clear from the burst of stop commands
        if current_maxrate >= 8:
            extra_sleep = 5 - (time.time() - (starttime + time_between_samples))
            if extra_sleep > 0:
                time.sleep(extra_sleep)

    def _send_continuous_break(self):
        """
        send break every 0.3 seconds until the Command Console banner
        """
        self._promptbuf = ""
        self._connection.send(Commands.BREAK)
        starttime = time.time()
        resendtime = time.time()
        while True:
            time.sleep(0.1)
            if time.time() > resendtime + 0.3:
                log.debug("Sending break again.")
                self._connection.send(Commands.BREAK)
                resendtime = time.time()

            if COMMAND_PATTERN in self._promptbuf:
                break

            if time.time() > starttime + 5:
                raise InstrumentTimeoutException("Break command failing to stop autosample!")

    def _send_break(self):
        """
        Send the break command to enter Command Mode, but first stop the incoming samples
        """
        self._send_break_poll()
        self._send_continuous_break()

    def _got_chunk(self, chunk, timestamp):
        """
        Extract samples from a chunk of data
        @param chunk: bytes to parse into a sample.
        """
        if self._extract_sample(PARParticleNew, SAMPLE_NEW_REGEX, chunk, timestamp):
            return
        if self._extract_sample(PARParticle, SAMPLE_REGEX, chunk, timestamp):
            return
        if self._extract_sample_param_dict(self._param_dict.get(Parameter.SERIAL),
                                           self._param_dict.get(Parameter.FIRMWARE),
                                           SatlanticPARConfigParticle, MAXANDBAUDRATE_REGEX, chunk, timestamp):
            return
        if HEADER_REGEX.match(chunk):
            self._param_dict.update_many(chunk)

    def _extract_sample_param_dict(self, serial_num, firmware,
                                   particle_class, regex, line, timestamp, publish=True):
        """
        Extract sample from a response line if present and publish parsed particle
        This is overridden to pass in parameters stored in the param dictionary to make the particle

        @param particle_class The class to instantiate for this specific
            data particle. Parameterizing this allows for simple, standard
            behavior from this routine
        @param regex The regular expression that matches a data sample
        @param line string to match for sample.
        @param timestamp port agent timestamp to include with the particle
        @param publish boolean to publish samples (default True). If True,
               two different events are published: one to notify raw data and
               the other to notify parsed data.

        @retval dict of dicts {'parsed': parsed_sample, 'raw': raw_sample} if
                the line can be parsed for a sample. Otherwise, None.
        """
        if regex.match(line):

            particle = particle_class(serial_num, firmware, line, port_timestamp=timestamp)
            parsed_sample = particle.generate()

            self._particle_dict[particle.data_particle_type()] = parsed_sample

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample


def create_playback_protocol(callback):
    return SatlanticPARInstrumentProtocol(callback)
