"""
@package mi.instrument.subc_control.onecam.ooicore.driver
@file marine-integrations/mi/instrument/subc_control/onecam/ooicore/driver.py
@author Richard Han
@brief Driver for the ooicore
Release notes:

CAMHD Driver

"""
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.protocol_param_dict import ParameterDictType

__author__ = 'Richard Han'
__license__ = 'Apache 2.0'

import string

from mi.core.log import get_logger ; log = get_logger()

from mi.core.common import BaseEnum, Units
from mi.core.exceptions import InstrumentProtocolException, InstrumentTimeoutException
from mi.core.exceptions import InstrumentParameterException
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker


# newline.
NEWLINE = '\r\n'

# default timeout.
TIMEOUT = 10

###
#    Driver Constant Definitions
###

class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver
    """
    RAW = CommonDataParticleType.RAW

class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS
    TEST = DriverProtocolState.TEST

class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT

    TAKE_PICTURE = 'DRIVER_EVENT_TAKE_PICTURE'
    START_STOP_RECORDING = 'DRIVER_EVENT_START_STOP_RECORDING'
    ZOOM_IN = 'DRIVER_EVENT_ZOOM_IN'
    ZOOM_OUT = 'DRIVER_EVENT_ZOOM_OUT'
    UP = 'DRIVER_EVENT_UP'
    LEFT = 'DRIVER_EVENT_LEFT'
    SELECT = 'DRIVER_EVENT_SELECT'
    RIGHT = 'DRIVER_EVENT_RIGHT'
    DOWN = 'DRIVER_EVENT_DOWN'
    POWER_ON_OFF = 'DRIVER_EVENT_POWER_ON_OFF'
    HD_SD_SWITCH = 'DRIVER_EVENT_HD_SD_SWITCH'
    USB_MODE = 'DRIVER_USB_MODE'
    IR_ON_OFF = 'DRIVER_IR_ON_OFF'
    LASER_ON_OFF = 'DRIVER_LASER_ON_OFF'
    EXTERNAL_FLASH_ENABLE = 'DRIVER_EVENT_EXTERNAL_FLASH_ENABLE'
    EXTERNAL_FLASH_DISABLE = 'DRIVER_EVENT_EXTERNAL_FLASH_DISABLE'
    ENTER_SLEEP_INTERVAL = 'DRIVER_EVENT_ENTER_SLEEP_INTERVAL'
    ENTER_RECORD_INTERVAL = 'DRIVER_EVENT_ENTER_RECORD_INTERVAL'
    ENTER_PICTURE_INTERVAL = 'DRIVER_EVENT_ENTER_PICTURE_INTERVAL'
    EXECUTE_INTERVAL_MODE = 'DRIVER_EVENT_EXECUTE_INTERVAL_MODE'
    ENABLE_EXTERNAL_TRIGGER = 'DRIVER_EVENT_ENABLE_EXTERNAL_TRIGGER'
    EXIT_INTERVAL_TRIGGER_MODE = 'DRIVER_EVENT_EXIT_INTERVAL_TRIGGER_MODE'
    RESET_EEPROM = 'DRIVER_EVENT_RESET_EEPROM'


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    ACQUIRE_SAMPLE = ProtocolEvent.ACQUIRE_SAMPLE
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET
    ZOOM_IN = ProtocolEvent.ZOOM_IN
    ZOOM_OUT = ProtocolEvent.ZOOM_OUT
    UP = ProtocolEvent.UP
    DOWN = ProtocolEvent.DOWN
    LEFT = ProtocolEvent.LEFT
    RIGHT = ProtocolEvent.RIGHT
    SELECT = ProtocolEvent.SELECT
    POWER_ON_OFF = ProtocolEvent.POWER_ON_OFF
    ENTER_SLEEP_INTERVAL = ProtocolEvent.ENTER_SLEEP_INTERVAL
    ENTER_PICTURE_INTERVAL = ProtocolEvent.ENTER_PICTURE_INTERVAL
    ENTER_RECORD_INTERVAL = ProtocolEvent.ENTER_RECORD_INTERVAL
    EXTERNAL_FLASH_ENABLE = ProtocolEvent.EXTERNAL_FLASH_ENABLE
    EXTERNAL_FLASH_DISABLE = ProtocolEvent.EXTERNAL_FLASH_DISABLE


class Command(BaseEnum):
    """
    CAMHD Instrument command strings
    """
    TAKE_PICTURE = '$1'
    START_STOP_RECORDING = '$2'
    ZOOM_IN = '$3'
    ZOOM_OUT = '$4'
    UP = '$5'
    LEFT = '$6'
    SELECT = '$7'
    RIGHT = '$8'
    DOWN = '$9'
    POWER_ON_OFF = '$0'
    HD_SD_SWITCH = '$v'
    USB_MODE = '$u'
    IR_ON_OFF = '$n'
    LASER_ON_OFF = '$I'
    EXTERNAL_FLASH_ENABLE = '$f'
    EXTERNAL_FLASH_DISABLE = '$F'
    ENTER_SLEEP_INTERVAL = '$i'
    ENTER_RECORD_INTERVAL = '$r'
    ENTER_PICTURE_INTERVAL = '$p'
    EXECUTE_INTERVAL_MODE = '$#'
    ENABLE_EXTERNAL_TRIGGER = '$x'
    EXIT_INTERVAL_TRIGGER_MODE = '$G'   # $I in the manual which is duplicate with Laser On Off Cmd
    RESET_EEPROM = '$^'


class Parameter(DriverParameter):
    """
    Device specific parameters for CAMHD.

    """
    SERIAL_BAUD_RATE = 'SerialBaudRate'
    BYTE_SIZE = 'ByteSize'
    PARITY = 'Parity'
    STOP_BIT = 'StopBit'
    DATA_FLOW_CONTROL = 'DataFlowControl'
    INPUT_BUFFER_SIZE = 'InputBufferSize'
    OUTPUT_BUFFER_SIZE = 'OutputBufferSize'
    SLEEP_INTERVAL = 'SleepInterval'
    RECORD_INTERVAL = 'RecordInterval'
    PICTURE_INTERVAL = 'PictureInterval'


class Prompt(BaseEnum):
    """
    Device i/o response..
    """
    BAUD_RATE_RESP = "%buad_rate"
    TAKE_PICTURE_RESP = '%1'
    START_STOP_RECORDING_RESP = '%2'
    ZOOM_IN_RESP = '%3'
    ZOOM_OUT_RESP = '%4'
    UP_RESP = '%5'
    LEFT_RESP = '%6'
    SELECT_RESP= '%7'
    RIGHT_RESP = '%8'
    DOWN_RESP = '%9'
    POWER_ON_OFF_RESP = '%0'
    HD_SD_SWITCH_RESP = '%v'
    USB_MODE_RESP = '%u'
    IR_ON_OFF_RESP = '%n'
    LASER_ON_OFF_RESP = '%I'
    EXTERNAL_FLASH_ENABLE_RESP = '%f'
    EXTERNAL_FLASH_DISABLE_RESP = '%F'
    ENTER_SLEEP_INTERVAL_RESP = '%i'
    ENTER_RECORD_INTERVAL_RESP = '%r'
    ENTER_PICTURE_INTERVAL_RESP = '%p'
    EXECUTE_INTERVAL_MODE_RESP = '%#'
    ENABLE_EXTERNAL_TRIGGER_RESP = '%x'
    EXIT_INTERVAL_TRIGGER_MODE_RESP = '%i'
    RESET_EEPROM_RESP = '%^'


###############################################################################
# Data Particles
###############################################################################


###############################################################################
# Driver
###############################################################################

class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """
    def __init__(self, evt_callback):
        """
        Driver constructor.
        @param evt_callback Driver process event callback.
        """
        #Construct superclass.
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)

    ########################################################################
    # Protocol builder.
    ########################################################################

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################

class Protocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """
    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        # Build protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(ProtocolState, ProtocolEvent,
                            ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.EXIT, self._handler_unknown_exit)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.START_DIRECT, self._handler_command_start_direct)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXIT, self._handler_command_exit)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_DIRECT, self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET, self._handler_command_get)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET, self._handler_command_set)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_SAMPLE, self._handler_command_acquire_sample)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.STOP_AUTOSAMPLE, self._handler_command_stop_autosample)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.TAKE_PICTURE, self._handler_command_take_picture)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_STOP_RECORDING, self._handler_command_start_stop_recording)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ZOOM_IN, self._handler_command_zoom_in)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ZOOM_OUT, self._handler_command_zoom_out)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.UP, self._handler_command_up)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.DOWN, self._handler_command_down)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SELECT, self._handler_command_select)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LEFT, self._handler_command_left)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.RIGHT, self._handler_command_right)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.POWER_ON_OFF, self._handler_command_power_on_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.HD_SD_SWITCH, self._handler_command_hd_sd_switch)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.USB_MODE, self._handler_command_usb_mode)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.IR_ON_OFF, self._handler_command_ir_on_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASER_ON_OFF, self._handler_command_laser_on_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXTERNAL_FLASH_ENABLE, self._handler_command_external_flash_enable)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXTERNAL_FLASH_DISABLE, self._handler_command_external_flash_disable)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER_SLEEP_INTERVAL, self._handler_command_enter_sleep_interval)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER_PICTURE_INTERVAL, self._handler_command_enter_picture_interval)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER_RECORD_INTERVAL, self._handler_command_enter_record_interval)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXECUTE_INTERVAL_MODE, self._handler_command_execute_interval_mode)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENABLE_EXTERNAL_TRIGGER, self._handler_command_enable_external_trigger)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXIT_INTERVAL_TRIGGER_MODE, self._handler_command_exit_interval_trigger_mode)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.RESET_EEPROM, self._handler_command_reset_eeprom)

        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.ENTER, self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXIT, self._handler_direct_access_exit)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_driver_dict()
        self._build_command_dict()
        self._build_param_dict()

        # Add build handlers for device commands.
        self._add_build_handler(Command.TAKE_PICTURE, self._build_simple_command)
        self._add_build_handler(Command.START_STOP_RECORDING, self._build_simple_command)
        self._add_build_handler(Command.ZOOM_IN, self._build_simple_command)
        self._add_build_handler(Command.ZOOM_OUT, self._build_simple_command)
        self._add_build_handler(Command.UP, self._build_simple_command)
        self._add_build_handler(Command.ENTER_RECORD_INTERVAL, self._build_camhd_command)
        self._add_build_handler(Command.ENTER_PICTURE_INTERVAL, self._build_camhd_command)
        self._add_build_handler(Command.ENTER_SLEEP_INTERVAL, self._build_camhd_command)
        self._add_build_handler(Command.EXTERNAL_FLASH_ENABLE, self._build_simple_command)
        self._add_build_handler(Command.EXECUTE_INTERVAL_MODE, self._build_simple_command)
        self._add_build_handler(Command.DOWN, self._build_simple_command)
        self._add_build_handler(Command.ENABLE_EXTERNAL_TRIGGER, self._build_simple_command)
        self._add_build_handler(Command.EXTERNAL_FLASH_DISABLE, self._build_simple_command)
        self._add_build_handler(Command.EXIT_INTERVAL_TRIGGER_MODE, self._build_simple_command)
        self._add_build_handler(Command.HD_SD_SWITCH, self._build_simple_command)
        self._add_build_handler(Command.IR_ON_OFF, self._build_simple_command)
        self._add_build_handler(Command.LASER_ON_OFF, self._build_simple_command)
        self._add_build_handler(Command.LEFT, self._build_simple_command)
        self._add_build_handler(Command.POWER_ON_OFF, self._build_simple_command)
        self._add_build_handler(Command.LEFT, self._build_simple_command)
        self._add_build_handler(Command.IR_ON_OFF, self._build_simple_command)
        self._add_build_handler(Command.SELECT, self._build_simple_command)
        self._add_build_handler(Command.USB_MODE, self._build_simple_command)

        self._add_build_handler(Command.DOWN, self._build_simple_command)
        self._add_build_handler(Command.RESET_EEPROM, self._build_simple_command)

        # Add response handlers for device commands.
        self._add_response_handler(Command.TAKE_PICTURE, self._parse_take_picture)
        self._add_response_handler(Command.START_STOP_RECORDING, self._parse_start_stop_recording)
        self._add_response_handler(Command.ZOOM_IN, self._parse_zoom_in)
        self._add_response_handler(Command.ZOOM_OUT, self._parse_zoom_out)
        self._add_response_handler(Command.UP, self._parse_up)
        self._add_response_handler(Command.DOWN, self._parse_down)
        self._add_response_handler(Command.SELECT, self._parse_select)
        self._add_response_handler(Command.LEFT, self._parse_left)
        self._add_response_handler(Command.RIGHT, self._parse_right)
        self._add_response_handler(Command.POWER_ON_OFF, self._parse_power_on_off)
        self._add_response_handler(Command.HD_SD_SWITCH, self._parse_hd_sd_switch)
        self._add_response_handler(Command.USB_MODE, self._parse_usb_mode)
        self._add_response_handler(Command.IR_ON_OFF, self._parse_ir_on_off)
        self._add_response_handler(Command.LASER_ON_OFF, self._parse_laser_on_off)
        self._add_response_handler(Command.LASER_ON_OFF, self._parse_laser_on_off)
        self._add_response_handler(Command.EXTERNAL_FLASH_ENABLE, self._parse_external_flash_enable)
        self._add_response_handler(Command.EXTERNAL_FLASH_DISABLE, self._parse_external_flash_disable)
        self._add_response_handler(Command.ENTER_PICTURE_INTERVAL, self._parse_enter_picture_interval)
        self._add_response_handler(Command.ENTER_SLEEP_INTERVAL, self._parse_enter_sleep_interval)
        self._add_response_handler(Command.ENTER_RECORD_INTERVAL, self._parse_enter_record_interval)
        self._add_response_handler(Command.EXECUTE_INTERVAL_MODE, self._parse_execute_interval_mode)
        self._add_response_handler(Command.ENABLE_EXTERNAL_TRIGGER, self._parse_enable_external_trigger)
        self._add_response_handler(Command.EXIT_INTERVAL_TRIGGER_MODE, self._parse_exit_interval_trigger_mode)
        self._add_response_handler(Command.RESET_EEPROM, self._parse_reset_eeprom)

        # Add sample handlers.

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # commands sent sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

        #
        self._chunker = StringChunker(Protocol.sieve_function)


    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        """

        return_list = []

        return return_list

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _build_command_dict(self):
        """
        Populate the command dictionary with command.
        """
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name="start autosample")
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, display_name="stop autosample")
        self._cmd_dict.add(Capability.ACQUIRE_SAMPLE, display_name="acquire sample")
        self._cmd_dict.add(Capability.SET, display_name="set")
        self._cmd_dict.add(Capability.GET, display_name="get")

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match stirng, match lambda function,
        and value formatting function for set commands.
        """

        # Add parameter handlers to parameter dict.
        self._param_dict.add(Parameter.SERIAL_BAUD_RATE,
                             r'Serial Baud Rate = (\d\d\d\d)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             units=Units.BAUD,
                             display_name="Baud Rate",
                             startup_param=True,
                             direct_access=False,
                             default_value=9600)

        self._param_dict.add(Parameter.BYTE_SIZE,
                             r'Byte Size = (\d)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             units=Units.BIT,
                             display_name="Byte Size",
                             startup_param=True,
                             direct_access=False,
                             default_value=8)

        self._param_dict.add(Parameter.PARITY,
                             r'Parity = (\d)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             display_name="Instrument Series",
                             startup_param=True,
                             direct_access=False,
                             default_value=0)

        self._param_dict.add(Parameter.STOP_BIT,
                             r'Stop Bit = (\d)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             display_name="Stop Bit",
                             startup_param=True,
                             direct_access=False,
                             default_value=1)

        self._param_dict.add(Parameter.DATA_FLOW_CONTROL,
                             r'Data Flow Control = (w+)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Data Flow Control",
                             startup_param=True,
                             direct_access=False,
                             default_value='None')

        self._param_dict.add(Parameter.INPUT_BUFFER_SIZE,
                             r'Input Buffer Size = (\d\d\d\d)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             display_name="Input Buffer Size",
                             startup_param=True,
                             direct_access=False,
                             default_value=1024)

        self._param_dict.add(Parameter.OUTPUT_BUFFER_SIZE,
                             r'Output Buffer Size = (\d+)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             display_name="Output Buffer Size",
                             startup_param=True,
                             direct_access=False,
                             default_value=800)

        self._param_dict.add(Parameter.SLEEP_INTERVAL,
                             r'Sleep Interval = (\d+)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             display_name="Sleep Interval",
                             startup_param=True,
                             direct_access=False,
                             default_value=800)

        self._param_dict.add(Parameter.PICTURE_INTERVAL,
                             r'Picture Interval = (\d+)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             display_name="Picture Interval",
                             startup_param=True,
                             direct_access=False,
                             default_value=800)

        self._param_dict.add(Parameter.RECORD_INTERVAL,
                             r'Picture Interval = (\d+)',
                             lambda match: int(match.group(1)),
                             int,
                             type=ParameterDictType.INT,
                             display_name="Record Interval",
                             startup_param=True,
                             direct_access=False,
                             default_value=800)

    def _got_chunk(self, chunk):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    def _build_simple_command(self, cmd, *args):
        """
        Build handler for basic THSPH commands.
        @param cmd the simple ooicore command to format.
        @retval The command to be sent to the device.
        """
        return "%s%s" % (cmd, NEWLINE)

    def _build_camhd_command(self, cmd, *args):
        """
        Build handler for CAMHD commands.
        @param cmd the CAMHD command to format.
        @retval The command to be sent to the device.
        """
        if cmd == Command.ENTER_RECORD_INTERVAL:
            interval = int(self._param_dict.get(Parameter.RECORD_INTERVAL))
            instrument_cmd =  cmd + interval + self._newline
        elif cmd == Command.ENTER_PICTURE_INTERVAL:
            interval = int(self._param_dict.get(Parameter.PICTURE_INTERVAL))
            instrument_cmd =  cmd + interval + self._newline
        elif cmd == Command.ENTER_SLEEP_INTERVAL:
            interval = int(self._param_dict.get(Parameter.SLEEP_INTERVAL))
            instrument_cmd =  cmd + interval + self._newline
        else:
            raise InstrumentProtocolException("Unknown command %s" % cmd)
        return "%s%s" % (instrument_cmd, NEWLINE)

    ########################################################################
    # Unknown handlers.
    ########################################################################

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
        pass

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state
        @retval (next_state, result)
        """
        return (ProtocolState.COMMAND, ResourceAgentState.IDLE)

    # def _parse_set_response(self, response, prompt):
    #     """
    #     Parse handler for set command.
    #     @param response command response string.
    #     @param prompt prompt following command response.
    #     @throws InstrumentProtocolException if set command misunderstood.
    #     """
    #     error = self._find_error(response)
    #
    #     if error:
    #         log.error("Set command encountered error; type='%s' msg='%s'", error[0], error[1])
    #         raise InstrumentParameterException('Set command failure: type="%s" msg="%s"' % (error[0], error[1]))
    #
    #     if prompt not in [Prompt.EXECUTED, Prompt.COMMAND]:
    #         log.error("Set command encountered error; instrument returned: %s", response)
    #         raise InstrumentProtocolException('Set command not recognized: %s' % response)

    ########################################################################
    # Command handlers.
    ########################################################################

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        # Command device to update parameters and send a config change event.
        #self._update_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._init_params()
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_get(self, params=None, *args, **kwargs):
        """
        Get parameters while in the command state.
        @param params List of the parameters to pass to the state
        @retval returns (next_state, result) where result is a dict {}. No
        agent state changes happening with Get, so no next_agent_state
        @throw InstrumentParameterException for invalid parameter
        """
        next_state = None
        result_vals = {}

        if params is None:
            raise InstrumentParameterException("GET parameter list empty!")

        if Parameter.ALL in params:
            params = Parameter.list()
            params.remove(Parameter.ALL)

        if not isinstance(params, list):
            raise InstrumentParameterException("GET parameter list not a list!")

        # Do a bulk update from the instrument since they are all on one page
        #self._update_params()

        # fill the return values from the update
        for param in params:
            if not Parameter.has(param):
                raise InstrumentParameterException("Invalid parameter!")
            result_vals[param] = self._param_dict.get(param)
        result = result_vals

        log.debug("Get finished, next: %s, result: %s", next_state, result)
        return next_state, result

    def _handler_command_set(self, *args, **kwargs):
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        # set parameters are only allowed in COMMAND state
        if self.get_current_state() != ProtocolState.COMMAND:
            raise InstrumentProtocolException("Not in command state. Unable to set params")

        self._verify_not_readonly(*args, **kwargs)

        old_config = self._param_dict.get_config()

        self._set_camhd_params(params)

        new_config = self._param_dict.get_config()
        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _set_camhd_params(self, params):
        """
        Issue commands to the instrument to set various parameters
        """
        for (key, val) in params.iteritems():
            if not Parameter.has(key):
                raise InstrumentParameterException()

            self._param_dict.set_value(key, val)

            try:

                if key == Parameter.RECORD_INTERVAL:
                    result = self._do_cmd_resp(Command.ENTER_RECORD_INTERVAL, int(self._param_dict.get(key)),
                                               expected_prompt=Prompt.ENTER_RECORD_INTERVAL_RESP)
                    if not result:
                        raise InstrumentParameterException("Could not set param %s" % key)

                elif key == Parameter.PICTURE_INTERVAL:
                    result = self._do_cmd_resp(Command.ENTER_PICTURE_INTERVAL, int(self._param_dict.get(key)),
                                               expected_prompt=Prompt.ENTER_PICTURE_INTERVAL_RESP)
                    if not result:
                        raise InstrumentParameterException("Could not set param %s" % key)

                elif key == Parameter.SLEEP_INTERVAL:

                    result = self._do_cmd_resp(Command.ENTER_SLEEP_INTERVAL, int(self._param_dict.get(key)),
                                               expected_prompt=Prompt.ENTER_SLEEP_INTERVAL_RESP)
                    if not result:
                        raise InstrumentParameterException("Could not set param %s" % key)

            except InstrumentParameterException:
                raise InstrumentProtocolException("Could not set %s" % key)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        pass

    def _handler_command_start_direct(self):
        """
        Start direct access
        """
        next_state = ProtocolState.DIRECT_ACCESS
        next_agent_state = ResourceAgentState.DIRECT_ACCESS
        result = None
        log.debug("_handler_command_start_direct: entering DA mode")
        return (next_state, (next_agent_state, result))

    def _handler_command_take_picture(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.TAKE_PICTURE, expected_prompt=Prompt.TAKE_PICTURE_RESP)
            if response != Prompt.TAKE_PICTURE_RESP:
                raise InstrumentProtocolException("Not able to get valid command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument time out exception. Instrument not responding to take picture cmd.")

        return None, (None, None)

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.TAKE_PICTURE, expected_prompt=Prompt.TAKE_PICTURE_RESP)
            if response != Prompt.START_STOP_RECORDING_RESP:
                raise InstrumentProtocolException("Not able to get valid Take Picture command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument time out exception. Instrument not responding to Take Picture cmd.")

        return None, (None, None)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.START_STOP_RECORDING, expected_prompt=Prompt.START_STOP_RECORDING_RESP)
            if response != Prompt.START_STOP_RECORDING_RESP:
                raise InstrumentProtocolException("Not able to get valid Start Stop Recording command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument time out exception. Instrument not responding to Start Stop recording cmd.")

        return None, (None, None)

    def _handler_command_stop_autosample(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.START_STOP_RECORDING, expected_prompt=Prompt.START_STOP_RECORDING_RESP)
            if response != Prompt.START_STOP_RECORDING_RESP:
                raise InstrumentProtocolException("Not able to get valid Start Stop Recording command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument time out exception. Instrument not responding to Start Stop recording cmd.")

        return None, (None, None)

    def _handler_command_start_stop_recording(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.START_STOP_RECORDING, expected_prompt=Prompt.START_STOP_RECORDING_RESP)
            if response != Prompt.START_STOP_RECORDING_RESP:
                raise InstrumentProtocolException("Not able to get valid Start Stop Recording command response.")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Start Stop Recording cmd.")

        return None, (None, None)

    def _handler_command_zoom_in(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.ZOOM_IN, expected_prompt=Prompt.ZOOM_IN_RESP)
            if response != Prompt.ZOOM_IN_RESP:
                raise InstrumentProtocolException("Not able to get zoom in command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument time out exception. Instrument not responding to zoom in cmd.")
        return None, (None, None)

    def _handler_command_zoom_out(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.ZOOM_OUT, expected_prompt=Prompt.ZOOM_OUT_RESP)
            if response != Prompt.ZOOM_OUT_RESP:
                raise InstrumentProtocolException("Not able to get zoom out command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to zoom out cmd.")
        return None, (None, None)

    def _handler_command_up(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.UP, expected_prompt=Prompt.UP_RESP)
            if response != Prompt.UP_RESP:
                raise InstrumentProtocolException("Not able to get Up command response.")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Up cmd.")
        return None, (None, None)

    def _handler_command_down(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.DOWN, expected_prompt=Prompt.DOWN_RESP)
            if response != Prompt.DOWN_RESP:
                raise InstrumentProtocolException("Not able to get Down command response.")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument time out exception. Instrument not responding to Down cmd.")
        return None, (None, None)

    def _handler_command_left(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.LEFT, expected_prompt=Prompt.LEFT_RESP)
            if response != Prompt.LEFT_RESP:
                raise InstrumentProtocolException("Not able to get Left command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Left cmd.")
        return None, (None, None)

    def _handler_command_right(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.RIGHT, expected_prompt=Prompt.RIGHT_RESP)
            if response != Prompt.RIGHT:
                raise InstrumentProtocolException("Not able to get Right command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Right cmd.")
        return None, (None, None)

    def _handler_command_select(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.SELECT, expected_prompt=Prompt.SELECT_RESP)
            if response != Prompt.SELECT_RESP:
                raise InstrumentProtocolException("Not able to get Select command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Select cmd.")
        return None, (None, None)

    def _handler_command_power_on_off(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.POWER_ON_OFF, expected_prompt=Prompt.SELECT_RESP)
            if response != Prompt.POWER_ON_OFF_RESP:
                raise InstrumentProtocolException("Not able to get Power On Off command response.")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Power On Off cmd.")
        return None, (None, None)

    def _handler_command_hd_sd_switch(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.HD_SD_SWITCH, expected_prompt=Prompt.HD_SD_SWITCH_RESP)
            if response != Prompt.HD_SD_SWITCH_RESP:
                raise InstrumentProtocolException("Not able to get HD SD Switch command response.")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to HD SD Switch cmd.")
        return None, (None, None)

    def _handler_command_usb_mode(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.USB_MODE, expected_prompt=Prompt.USB_MODE_RESP)
            if response != Prompt.HD_SD_SWITCH_RESP:
                raise InstrumentProtocolException("Not able to get USB Mode command response.")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to USB Mode cmd.")
        return None, (None, None)

    def _handler_command_ir_on_off(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.IR_ON_OFF, expected_prompt=Prompt.IR_ON_OFF_RESP)
            if response != Prompt.IR_ON_OFF_RESP:
                raise InstrumentProtocolException("Not able to get IR On Off command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to IR On Off cmd.")
        return None, (None, None)

    def _handler_command_laser_on_off(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.LASER_ON_OFF, expected_prompt=Prompt.IR_ON_OFF_RESP)
            if response != Prompt.IR_ON_OFF_RESP:
                raise InstrumentProtocolException("Not able to get Laser On Off command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Laser On Off cmd.")
        return None, (None, None)

    def _handler_command_external_flash_enable(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.EXTERNAL_FLASH_ENABLE, expected_prompt=Prompt.EXTERNAL_FLASH_ENABLE_RESP)
            if response != Prompt.EXTERNAL_FLASH_ENABLE_RESP:
                raise InstrumentProtocolException("Not able to get External Flash Enable command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to External Flash Enable cmd.")
        return None, (None, None)

    def _handler_command_external_flash_disable(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.EXTERNAL_FLASH_DISABLE, expected_prompt=Prompt.EXTERNAL_FLASH_DISABLE_RESP)
            if response != Prompt.EXTERNAL_FLASH_DISABLE_RESP:
                raise InstrumentProtocolException("Not able to get External Flash Disable command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to External Flash Disable cmd.")
        return None, (None, None)

    def _handler_command_enter_sleep_interval(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.ENTER_SLEEP_INTERVAL, expected_prompt=Prompt.EXTERNAL_FLASH_DISABLE_RESP)
            if response != Prompt.ENTER_SLEEP_INTERVAL_RESP:
                raise InstrumentProtocolException("Not able to get Enter Sleep Interval command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Enter Sleep Interval cmd.")
        return None, (None, None)

    def _handler_command_enter_picture_interval(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.ENTER_PICTURE_INTERVAL, expected_prompt=Prompt.EXTERNAL_FLASH_DISABLE_RESP)
            if response != Prompt.ENTER_PICTURE_INTERVAL_RESP:
                raise InstrumentProtocolException("Not able to get Enter Picture Interval command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Enter Picture Interval cmd.")
        return None, (None, None)

    def _handler_command_enter_record_interval(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.ENTER_PICTURE_INTERVAL, expected_prompt=Prompt.EXTERNAL_FLASH_DISABLE_RESP)
            if response != Prompt.ENTER_PICTURE_INTERVAL_RESP:
                raise InstrumentProtocolException("Not able to get Enter Record Interval command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Enter Record Interval cmd.")
        return None, (None, None)

    def _handler_command_execute_interval_mode(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.EXECUTE_INTERVAL_MODE, expected_prompt=Prompt.EXECUTE_INTERVAL_MODE_RESP)
            if response != Prompt.EXECUTE_INTERVAL_MODE_RESP:
                raise InstrumentProtocolException("Not able to get Execute Interval Mode command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Execute Interval cmd.")
        return None, (None, None)

    def _handler_command_exit_interval_trigger_mode(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.EXIT_INTERVAL_TRIGGER_MODE, expected_prompt=Prompt.EXIT_INTERVAL_TRIGGER_MODE_RESP)
            if response != Prompt.EXIT_INTERVAL_TRIGGER_MODE_RESP:
                raise InstrumentProtocolException("Not able to get Exit Interval Trigger Mode command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Exit Interval Trigger Mode cmd.")
        return None, (None, None)

    def _handler_command_enable_external_trigger(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.ENABLE_EXTERNAL_TRIGGER, expected_prompt=Prompt.ENABLE_EXTERNAL_TRIGGER_RESP)
            if response != Prompt.ENABLE_EXTERNAL_TRIGGER_RESP:
                raise InstrumentProtocolException("Not able to get Exit Interval Trigger command response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Exit Interval Trigger cmd.")
        return None, (None, None)

    def _handler_command_reset_eeprom(self, *args, **kwargs):
        """
        @retval return (next state, (next_agent_state, result))
        """

        try:
            response = self._do_cmd_resp(Command.RESET_EEPROM, expected_prompt=Prompt.RESET_EEPROM_RESP)
            if response != Prompt.RESET_EEPROM_RESP:
                raise InstrumentProtocolException("Not able to get Reset EEPROM coomand response. ")

        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Instrument not responding to Reset EEPROM cmd.")
        return None, (None, None)

    def _parse_take_picture(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.TAKE_PICTURE_RESP:
            raise InstrumentProtocolException('CAMHD command not recognized: %s.' % response)

        return response

    def _parse_enter_picture_interval(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.ENTER_PICTURE_INTERVAL_RESP:
            raise InstrumentProtocolException('CAMHD Enter Picture Interval command not recognized: %s.' % response)

        return response

    def _parse_enter_sleep_interval(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.ENTER_SLEEP_INTERVAL_RESP:
            raise InstrumentProtocolException('CAMHD Enter Sleep Interval command not recognized: %s.' % response)

        return response

    def _parse_enter_record_interval(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.ENTER_RECORD_INTERVAL_RESP:
            raise InstrumentProtocolException('CAMHD Enter Record Interval command not recognized: %s.' % response)

        return response

    def _parse_enter_record_interval(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.ENTER_RECORD_INTERVAL_RESP:
            raise InstrumentProtocolException('CAMHD Enter Record Interval command not recognized: %s.' % response)

        return response

    def _parse_start_stop_recording(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.START_STOP_RECORDING_RESP:
            raise InstrumentProtocolException('CAMHD Start Stop Recording command not recognized: %s.' % response)

        return response

    def _parse_zoom_in(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.ZOOM_IN_RESP:
            raise InstrumentProtocolException('CAMHD Zoom In command not recognized: %s.' % response)

        return response

    def _parse_zoom_out(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.ZOOM_OUT_RESP:
            raise InstrumentProtocolException('CAMHD Zoom Out command not recognized: %s.' % response)

        return response

    def _parse_up(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.UP_RESP:
            raise InstrumentProtocolException('CAMHD UP command not recognized: %s.' % response)

        return response

    def _parse_down(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.DOWN_RESP:
            raise InstrumentProtocolException('CAMHD Down command not recognized: %s.' % response)

        return response

    def _parse_select(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.SELECT_RESP:
            raise InstrumentProtocolException('CAMHD Select command not recognized: %s.' % response)

        return response

    def _parse_left(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.LEFT_RESP:
            raise InstrumentProtocolException('CAMHD Left command not recognized: %s.' % response)

        return response

    def _parse_right(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.RIGHT_RESP:
            raise InstrumentProtocolException('CAMHD Right command not recognized: %s.' % response)

        return response

    def _parse_power_on_off(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.POWER_ON_OFF_RESP:
            raise InstrumentProtocolException('CAMHD Power On Off command not recognized: %s.' % response)

        return response

    def _parse_hd_sd_switch(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.HD_SD_SWITCH_RESP:
            raise InstrumentProtocolException('CAMHD HD SD Switch command not recognized: %s.' % response)

        return response

    def _parse_usb_mode(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.USB_MODE_RESP:
            raise InstrumentProtocolException('CAMHD USB Mode command not recognized: %s.' % response)

        return response

    def _parse_ir_on_off(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.IR_ON_OFF_RESP:
            raise InstrumentProtocolException('CAMHD IR On Off command not recognized: %s.' % response)

        return response

    def _parse_laser_on_off(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.LASER_ON_OFF_RESP:
            raise InstrumentProtocolException('CAMHD Laser On Off command not recognized: %s.' % response)

        return response

    def _parse_external_flash_enable(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.EXTERNAL_FLASH_ENABLE_RESP:
            raise InstrumentProtocolException('CAMHD External Flash Enable command not recognized: %s.' % response)

        return response

    def _parse_external_flash_disable(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.EXTERNAL_FLASH_DISABLE_RESP:
            raise InstrumentProtocolException('CAMHD External Flash Disable command not recognized: %s.' % response)

        return response

    def _parse_execute_interval_mode(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.EXECUTE_INTERVAL_MODE_RESP:
            raise InstrumentProtocolException('CAMHD Execute Interval Mode command not recognized: %s.' % response)

        return response

    def _parse_enable_external_trigger(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.ENABLE_EXTERNAL_TRIGGER_RESP:
            raise InstrumentProtocolException('CAMHD Enable External Trigger command not recognized: %s.' % response)

        return response

    def _parse_exit_interval_trigger_mode(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.EXIT_INTERVAL_TRIGGER_MODE_RESP:
            raise InstrumentProtocolException('CAMHD Exit Interval Trigger Mode command not recognized: %s.' % response)

        return response

    def _parse_reset_eeprom(self, response, prompt):
        """
        Parse handler for CAMHD commands.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if CAMHD command misunderstood.
        """
        if prompt != Prompt.RESET_EEPROM_RESP:
            raise InstrumentProtocolException('CAMHD Reset EEPROM Response command not recognized: %s.' % response)

        return response


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

    def _handler_direct_access_exit(self, *args, **kwargs):
        """
        Exit direct access state.
        """
        pass

    def _handler_direct_access_execute_direct(self, data):
        """
        """
        next_state = None
        result = None
        next_agent_state = None

        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return (next_state, (next_agent_state, result))

    def _handler_direct_access_stop_direct(self):
        """
        @throw InstrumentProtocolException on invalid command
        """
        result = None

        next_state = ProtocolState.COMMAND
        next_agent_state = ResourceAgentState.COMMAND

        return (next_state, (next_agent_state, result))
