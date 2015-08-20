"""
@package mi.instrument.uw.bars.ooicore.driver
@file mi/instrument/uw/bars/ooicore/driver.py
@author Steve Foley
@brief Driver for the ooicore
Release notes:
This supports the UW BARS instrument from the Marv Tilley lab

"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

import re
import time

from mi.core.common import BaseEnum, Units
from mi.core.exceptions import SampleException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentTimeoutException

from mi.core.instrument.data_particle import DataParticle, DataParticleKey, CommonDataParticleType
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.chunker import StringChunker

from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_protocol import MenuInstrumentProtocol
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict, ParameterDictVisibility, ParameterDictType

from mi.core.log import get_logger
from mi.core.log import get_logging_metaclass

common_matches = {
    'float': r'-?\d*\.?\d+',
    'int': r'-?\d+',
    'str': r'\w+',
    'fn': r'\S+',
    'rest': r'.*\r\n',
    'tod': r'\d{8}T\d{6}',
    'data': r'[^\*]+',
    'crc': r'[0-9a-fA-F]{4}'
}

log = get_logger()

Directions = MenuInstrumentProtocol.MenuTree.Directions

SAMPLE_PATTERN = '\s+'.join(['(-?\d+\.\d+)'] * 12) + '\r\n'
SAMPLE_REGEX = re.compile(SAMPLE_PATTERN)

# newline.
NEWLINE = '\r'

# default timeout.
TIMEOUT = 10


class ScheduledJob(BaseEnum):
    ACQUIRE_STATUS = 'acquire_status'


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    TRHPH_PARSED = 'trhph_sample'
    TRHPH_STATUS = 'trhph_status'


class Command(BaseEnum):
    DIRECT_SET = "SET"
    BACK_MENU = "BACK_MENU"
    BLANK = "BLANK"
    BREAK = "BREAK"
    START_AUTOSAMPLE = "START_AUTOSAMPLE"
    CHANGE_PARAM = "CHANGE_PARAM"
    SHOW_PARAM = "SHOW_PARAM"
    SHOW_STATUS = "SHOW_STATUS"
    SENSOR_POWER = "SENSOR_POWER"
    CHANGE_CYCLE_TIME = "CHANGE_CYCLE_TIME"
    CHANGE_VERBOSE = "CHANGE_VERBOSE"
    CHANGE_METADATA_POWERUP = "CHANGE_METADATA_POWERUP"
    CHANGE_METADATA_RESTART = "CHANGE_METADATA_RESTART"
    CHANGE_RES_SENSOR_POWER = "CHANGE_RES_SENSOR_POWER"
    CHANGE_INST_AMP_POWER = "CHANGE_INST_AMP_POWER"
    CHANGE_EH_ISOLATION_AMP_POWER = "CHANGE_EH_ISOLATION_AMP_POWER"
    CHANGE_HYDROGEN_POWER = "CHANGE_HYDROGEN_POWER"
    CHANGE_REFERENCE_TEMP_POWER = "CHANGE_REFERENCE_TEMP_POWER"


# Strings should line up with Command class
COMMAND_CHAR = {
    'BACK_MENU': '9',
    'BLANK': '\r',
    'BREAK': chr(0x13),  # Ctrl-S
    'START_AUTOSAMPLE': '1',
    'CHANGE_PARAM': '2',
    'SHOW_PARAM': '6',
    'SHOW_STATUS': '5',
    'SENSOR_POWER': '4',
    'CHANGE_CYCLE_TIME': '1',
    'CHANGE_VERBOSE': '2',
    'CHANGE_METADATA_POWERUP': '3',
    'CHANGE_METADATA_RESTART': '4',
    'CHANGE_RES_SENSOR_POWER': '1',
    'CHANGE_INST_AMP_POWER': '2',
    'CHANGE_EH_ISOLATION_AMP_POWER': '3',
    'CHANGE_HYDROGEN_POWER': '4',
    'CHANGE_REFERENCE_TEMP_POWER': '5',
}


class SubMenu(BaseEnum):
    MAIN = "SUBMENU_MAIN"
    CHANGE_PARAM = "SUBMENU_CHANGE_PARAM"
    SHOW_PARAM = "SUBMENU_SHOW_PARAM"
    SHOW_STATUS = "SUBMENU_SHOW_STATUS"
    SENSOR_POWER = "SUBMENU_SENSOR_POWER"
    CYCLE_TIME = "SUBMENU_CYCLE_TIME"
    VERBOSE = "SUBMENU_VERBOSE"
    METADATA_POWERUP = "SUBMENU_METADATA_POWERUP"
    METADATA_RESTART = "SUBMENU_METADATA_RESTART"
    RES_SENSOR_POWER = "SUBMENU_RES_SENSOR_POWER"
    INST_AMP_POWER = "SUBMENU_INST_AMP_POWER"
    EH_ISOLATION_AMP_POWER = "SUBMENU_EH_ISOLATION_AMP_POWER"
    HYDROGEN_POWER = "SUBMENU_HYDROGEN_POWER"
    REFERENCE_TEMP_POWER = "SUBMENU_REFERENCE_TEMP_POWER"


class ProtocolState(BaseEnum):
    """
    Protocol states
    enum.
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE


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
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    SCHEDULED_ACQUIRE_STATUS = 'DRIVER_EVENT_SCHEDULED_ACQUIRE_STATUS'


class Capability(BaseEnum):
    """
    Capabilities exposed to user
    """
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    DISCOVER = DriverEvent.DISCOVER


# Device specific parameters.
class Parameter(DriverParameter):
    """
    Device parameters
    """
    CYCLE_TIME = "trhph_cycle_time"
    VERBOSE = "verbose"
    METADATA_POWERUP = "trhph_metadata_on_powerup"
    METADATA_RESTART = "trhph_metadata_on_restart"
    RES_SENSOR_POWER = "trhph_res_power_status"
    INST_AMP_POWER = "trhph_thermo_hydro_amp_power_status"
    EH_ISOLATION_AMP_POWER = "trhph_eh_amp_power_status"
    HYDROGEN_POWER = "trhph_hydro_sensor_power_status"
    REFERENCE_TEMP_POWER = "trhph_ref_temp_power_status"


# Device prompts.
class Prompt(BaseEnum):
    """
    io prompts.
    """
    CMD_PROMPT = "-->"
    BREAK_ACK = "\r\n"
    NONE = ""

    DEAD_END_PROMPT = "Press Enter to return to the Main Menu. -->"
    CONTINUE_PROMPT = "Press ENTER to continue."

    MAIN_MENU = "Enter 0, 1, 2, 3, 4, 5, or 6 here  -->"
    CHANGE_PARAM_MENU = "Enter 0 through 9 here  -->"
    SENSOR_POWER_MENU = "Enter 0 through 9 here  -->"

    CYCLE_TIME_PROMPT = "Enter 1 for Seconds, 2 for Minutes -->"
    CYCLE_TIME_SEC_VALUE_PROMPT = "Enter a new value between 15 and 59 here -->"
    CYCLE_TIME_MIN_VALUE_PROMPT = "Enter a new value between 1 and 60 here -->"
    VERBOSE_PROMPT = "Enter 2 for Verbose, 1 for just Data. -->"
    METADATA_PROMPT = "Enter 2 for Yes, 1 for No. -->"


MENU_PROMPTS = [Prompt.MAIN_MENU, Prompt.CHANGE_PARAM_MENU,
                Prompt.SENSOR_POWER_MENU, Prompt.CYCLE_TIME_PROMPT,
                Prompt.DEAD_END_PROMPT, Prompt.CONTINUE_PROMPT]

MENU = MenuInstrumentProtocol.MenuTree({

    SubMenu.MAIN: [Directions(command=Command.BLANK, response=Prompt.MAIN_MENU)],
    SubMenu.CHANGE_PARAM: [Directions(command=Command.CHANGE_PARAM,
                                      response=Prompt.CHANGE_PARAM_MENU)],
    SubMenu.SHOW_PARAM: [Directions(SubMenu.CHANGE_PARAM),
                         Directions(command=Command.SHOW_PARAM,
                                    response=Prompt.CONTINUE_PROMPT)],
    SubMenu.SHOW_STATUS: [Directions(command=Command.SHOW_STATUS,
                                     response=Prompt.DEAD_END_PROMPT)],
    SubMenu.SENSOR_POWER: [Directions(command=Command.SENSOR_POWER,
                                      response=Prompt.SENSOR_POWER_MENU)],
    SubMenu.CYCLE_TIME: [Directions(SubMenu.CHANGE_PARAM),
                         Directions(command=Command.CHANGE_CYCLE_TIME,
                                    response=Prompt.CYCLE_TIME_PROMPT)],
    SubMenu.VERBOSE: [Directions(SubMenu.CHANGE_PARAM),
                      Directions(command=Command.CHANGE_VERBOSE,
                                 response=Prompt.VERBOSE_PROMPT)],
    SubMenu.METADATA_POWERUP: [Directions(SubMenu.CHANGE_PARAM),
                               Directions(command=Command.CHANGE_METADATA_POWERUP,
                                          response=Prompt.METADATA_PROMPT)],
    SubMenu.METADATA_RESTART: [Directions(SubMenu.CHANGE_PARAM),
                               Directions(command=Command.CHANGE_METADATA_RESTART,
                                          response=Prompt.METADATA_PROMPT)],
    SubMenu.RES_SENSOR_POWER: [Directions(SubMenu.SENSOR_POWER),
                               Directions(command=Command.CHANGE_RES_SENSOR_POWER,
                                          response=Prompt.SENSOR_POWER_MENU)],
    SubMenu.INST_AMP_POWER: [Directions(SubMenu.SENSOR_POWER),
                             Directions(command=Command.CHANGE_INST_AMP_POWER,
                                        response=Prompt.SENSOR_POWER_MENU)],
    SubMenu.EH_ISOLATION_AMP_POWER: [Directions(SubMenu.SENSOR_POWER),
                                     Directions(command=Command.CHANGE_EH_ISOLATION_AMP_POWER,
                                                response=Prompt.SENSOR_POWER_MENU)],
    SubMenu.HYDROGEN_POWER: [Directions(SubMenu.SENSOR_POWER),
                             Directions(command=Command.CHANGE_HYDROGEN_POWER,
                                        response=Prompt.SENSOR_POWER_MENU)],
    SubMenu.REFERENCE_TEMP_POWER: [Directions(SubMenu.SENSOR_POWER),
                                   Directions(command=Command.CHANGE_REFERENCE_TEMP_POWER,
                                              response=Prompt.SENSOR_POWER_MENU)],
})


class BarsStatusParticleKey(BaseEnum):
    SYSTEM_INFO = "trhph_system_info"
    EPROM_STATUS = "trhph_eprom_status"
    CYCLE_TIME = "trhph_cycle_time"
    CYCLE_TIME_UNIT = "trhph_cycle_time_units"
    POWER_CONTROL_WORD = "trhph_power_control_word"
    RES_POWER = "trhph_res_power_status"
    THERMO_HYDRO_AMP_POWER = "trhph_thermo_hydro_amp_power_status"
    EH_AMP_POWER = "trhph_eh_amp_power_status"
    HYDRO_SENSOR_POWER = "trhph_hydro_sensor_power_status"
    REF_TEMP_POWER = "trhph_ref_temp_power_status"
    METADATA_ON_POWERUP = "trhph_metadata_on_powerup"
    METADATA_ON_RESTART = "trhph_metadata_on_restart"


class BarsStatusParticle(DataParticle):
    """
    Routines for parsing raw data into a status particle structure for the
    Satlantic PAR sensor. Overrides the building of values, and the rest comes
    along for free.
    """
    _data_particle_type = DataParticleType.TRHPH_STATUS

    @staticmethod
    def regex():
        """
        Regular expression to match a status pattern
        @return: regex string
        """
        pattern = r"""
            (?x)
            (?P<system_info>       System \s Name: (%(rest)s){7})  %(rest)s  %(rest)s
            (?P<eprom_status>      %(int)s)                        \s+ = \s+ Eprom .*\r\n
            (?P<cycle_time>        %(int)s)                        \s+ = \s+ Cycle \s Time .*\r\n
            (?P<unit>              %(int)s)                        \s+ = \s+ Minutes \s or \s Seconds .*\r\n
            (?P<power_control>     %(int)s)                        \s+ = \s+ Power \s Control .*\r\n
            (?P<res_power>         %(int)s)                        \s+ = \s+ Res \s Power .*\r\n
            (?P<thermo_hydro_amp>  %(int)s)                        \s+ = \s+ Thermocouple \s \& \s Hydrogen .*\r\n
            (?P<eh_amp_power>      %(int)s)                        \s+ = \s+ eh \s Amp .*\r\n
            (?P<hydro_sensor_power>%(int)s)                        \s+ = \s+ Hydrogen \s Sensor .*\r\n
            (?P<ref_temp_power>    %(int)s)                        \s+ = \s+ Reference \s Temperature .*\r\n
            (?P<print_on_powerup>  %(int)s)                        \s+ = \s+ .* Power \s up.*\r\n
            (?P<print_on_restart>  %(int)s)                        \s+ = \s+ .* Restart \s Data.*\r\n
            """ % common_matches
        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(BarsStatusParticle.regex())

    def _build_parsed_values(self):
        """
        Take something in the status format and split it into
        a PAR status values (with an appropriate tag)

        @throw SampleException If there is a problem with status creation
        """

        match = BarsStatusParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of status data: [%s]" % self.raw_data)

        log.trace("Matching sample %r", match.groups())

        system_info = match.group('system_info')
        eprom_status = int(match.group('eprom_status'))
        cycle_time = int(match.group('cycle_time'))
        unit = int(match.group('unit'))
        power_control = int(match.group('power_control'))
        res_power = int(match.group('res_power'))
        thermo_hydro_amp = int(match.group('thermo_hydro_amp'))
        eh_amp_power = int(match.group('eh_amp_power'))
        hydro_sensor_power = int(match.group('hydro_sensor_power'))
        ref_temp_power = int(match.group('ref_temp_power'))
        print_on_powerup = int(match.group('print_on_powerup'))
        print_on_restart = int(match.group('print_on_powerup'))

        result = [{DataParticleKey.VALUE_ID: BarsStatusParticleKey.SYSTEM_INFO,
                   DataParticleKey.VALUE: system_info},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.EPROM_STATUS,
                  DataParticleKey.VALUE: eprom_status},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.CYCLE_TIME,
                  DataParticleKey.VALUE: cycle_time},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.CYCLE_TIME_UNIT,
                  DataParticleKey.VALUE: unit},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.POWER_CONTROL_WORD,
                  DataParticleKey.VALUE: power_control},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.RES_POWER,
                  DataParticleKey.VALUE: res_power},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.THERMO_HYDRO_AMP_POWER,
                  DataParticleKey.VALUE: thermo_hydro_amp},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.EH_AMP_POWER,
                  DataParticleKey.VALUE: eh_amp_power},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.HYDRO_SENSOR_POWER,
                  DataParticleKey.VALUE: hydro_sensor_power},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.REF_TEMP_POWER,
                  DataParticleKey.VALUE: ref_temp_power},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.METADATA_ON_POWERUP,
                  DataParticleKey.VALUE: print_on_powerup},
                  {DataParticleKey.VALUE_ID: BarsStatusParticleKey.METADATA_ON_RESTART,
                  DataParticleKey.VALUE: print_on_restart}]
        return result


class BarsDataParticleKey(BaseEnum):
    RESISTIVITY_5 = "resistivity_5"
    RESISTIVITY_X1 = "resistivity_x1"
    RESISTIVITY_X5 = "resistivity_x5"
    HYDROGEN_5 = "hydrogen_5"
    HYDROGEN_X1 = "hydrogen_x1"
    HYDROGEN_X5 = "hydrogen_x5"
    EH_SENSOR = "eh_sensor"
    REFERENCE_TEMP_VOLTS = "ref_temp_volts"
    REFERENCE_TEMP_DEG_C = "ref_temp_degc"
    RESISTIVITY_TEMP_VOLTS = "resistivity_temp_volts"
    RESISTIVITY_TEMP_DEG_C = "resistivity_temp_degc"
    BATTERY_VOLTAGE = "battery_voltage"


class BarsDataParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure for the
    Satlantic PAR sensor. Overrides the building of values, and the rest comes
    along for free.
    """
    _data_particle_type = DataParticleType.TRHPH_PARSED

    @staticmethod
    def regex():
        """
        Regular expression to match a sample pattern
        @return: regex string
        """
        pattern = SAMPLE_PATTERN

        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(BarsDataParticle.regex())

    def _build_parsed_values(self):
        """
        Take something in the sample format and split it into
        a PAR values (with an appropriate tag)
        
        @throw SampleException If there is a problem with sample creation
        """

        match = BarsDataParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" %
                                  self.raw_data)

        log.debug("Matching Sample Data Particle %r", match.groups())
        res_5 = float(match.group(1))
        res_x1 = float(match.group(2))
        res_x5 = float(match.group(3))
        h_5 = float(match.group(4))
        h_x1 = float(match.group(5))
        h_x5 = float(match.group(6))
        eh = float(match.group(7))
        ref_temp_v = float(match.group(8))
        ref_temp_c = float(match.group(9))
        res_temp_v = float(match.group(10))
        res_temp_c = float(match.group(11))
        batt_v = float(match.group(12))

        result = [{DataParticleKey.VALUE_ID: BarsDataParticleKey.RESISTIVITY_5,
                   DataParticleKey.VALUE: res_5},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.RESISTIVITY_X1,
                   DataParticleKey.VALUE: res_x1},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.RESISTIVITY_X5,
                   DataParticleKey.VALUE: res_x5},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.HYDROGEN_5,
                   DataParticleKey.VALUE: h_5},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.HYDROGEN_X1,
                   DataParticleKey.VALUE: h_x1},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.HYDROGEN_X5,
                   DataParticleKey.VALUE: h_x5},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.EH_SENSOR,
                   DataParticleKey.VALUE: eh},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.REFERENCE_TEMP_VOLTS,
                   DataParticleKey.VALUE: ref_temp_v},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.REFERENCE_TEMP_DEG_C,
                   DataParticleKey.VALUE: ref_temp_c},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.RESISTIVITY_TEMP_VOLTS,
                   DataParticleKey.VALUE: res_temp_v},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.RESISTIVITY_TEMP_DEG_C,
                   DataParticleKey.VALUE: res_temp_c},
                  {DataParticleKey.VALUE_ID: BarsDataParticleKey.BATTERY_VOLTAGE,
                   DataParticleKey.VALUE: batt_v}]

        return result


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
    # Superclass overrides for resource query.
    ########################################################################
    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return Parameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################
    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(MENU, Prompt, NEWLINE, self._driver_event)


###############################################################################
# Protocol
################################################################################

class Protocol(MenuInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses MenuInstrumentProtocol
    """
    __metaclass__ = get_logging_metaclass(log_level='debug')

    def __init__(self, menu, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        MenuInstrumentProtocol.__init__(self, menu, prompts, newline, driver_event)

        # Build protocol state machine.
        self._protocol_fsm = InstrumentFSM(ProtocolState, ProtocolEvent,
                                           ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_discover)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET, self._handler_command_get)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET, self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_AUTOSAMPLE,
                                       self._handler_command_autosample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_DIRECT,
                                       self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)

        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE,
                                       self._handler_autosample_stop)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.SCHEDULED_ACQUIRE_STATUS,
                                       self._handler_autosample_acquire_status)

        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.ENTER,
                                       self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXIT,
                                       self._handler_direct_access_exit)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.STOP_DIRECT,
                                       self._handler_direct_access_stop_direct)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXECUTE_DIRECT,
                                       self._handler_direct_access_execute_direct)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_driver_dict()
        self._build_command_dict()
        self._build_param_dict()

        # Add build handlers for device commands.
        self._add_build_handler(Command.BACK_MENU, self._build_menu_command)
        self._add_build_handler(Command.BLANK, self._build_solo_command)
        self._add_build_handler(Command.START_AUTOSAMPLE, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_PARAM, self._build_menu_command)
        self._add_build_handler(Command.SHOW_PARAM, self._build_menu_command)
        self._add_build_handler(Command.SHOW_STATUS, self._build_menu_command)
        self._add_build_handler(Command.SENSOR_POWER, self._build_menu_command)
        self._add_build_handler(Command.DIRECT_SET, self._build_direct_command)
        self._add_build_handler(Command.CHANGE_CYCLE_TIME, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_VERBOSE, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_METADATA_RESTART, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_METADATA_POWERUP, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_RES_SENSOR_POWER, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_INST_AMP_POWER, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_EH_ISOLATION_AMP_POWER, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_HYDROGEN_POWER, self._build_menu_command)
        self._add_build_handler(Command.CHANGE_REFERENCE_TEMP_POWER, self._build_menu_command)

        # Add response handlers for device commands.
        self._add_response_handler(Command.BACK_MENU, self._parse_menu_change_response)
        self._add_response_handler(Command.BLANK, self._parse_menu_change_response)
        self._add_response_handler(Command.SHOW_PARAM, self._parse_show_param_response)
        self._add_response_handler(Command.SHOW_STATUS, self._parse_show_param_response)
        self._add_response_handler(Command.CHANGE_CYCLE_TIME, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_VERBOSE, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_METADATA_RESTART, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_METADATA_POWERUP, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_RES_SENSOR_POWER, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_INST_AMP_POWER, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_EH_ISOLATION_AMP_POWER, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_HYDROGEN_POWER, self._parse_menu_change_response)
        self._add_response_handler(Command.CHANGE_REFERENCE_TEMP_POWER, self._parse_menu_change_response)
        self._add_response_handler(Command.DIRECT_SET, self._parse_menu_change_response)

        self._add_scheduler_event(ScheduledJob.ACQUIRE_STATUS, ProtocolEvent.SCHEDULED_ACQUIRE_STATUS)

        # Add sample handlers.

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # commands sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

        self._chunker = StringChunker(self.sieve_function)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        """
        return_list = []
        matchers = []

        matchers.append(BarsStatusParticle.regex_compiled())
        matchers.append(BarsDataParticle.regex_compiled())
        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        #log.debug("return_list = %s" % return_list)
        return return_list

    def _go_to_root_menu(self):
        """
        Get back to the root menu.
        """

        # Issue an enter or two off the bat to get out of any display screens
        # and confirm command mode
        try:
            response = self._do_cmd_resp(Command.BLANK, expected_prompt=Prompt.CMD_PROMPT)
            while not str(response).endswith(Prompt.CMD_PROMPT):
                response = self._do_cmd_resp(Command.BLANK,
                                             expected_prompt=Prompt.CMD_PROMPT)
                time.sleep(1)
        except InstrumentTimeoutException:
            raise InstrumentProtocolException("Not able to get valid command prompt. Is instrument in command mode?")

        # When you get a --> prompt, do 9's until you get back to the root
        response = self._do_cmd_resp(Command.BACK_MENU,
                                     expected_prompt=MENU_PROMPTS)
        while not str(response).endswith(Prompt.MAIN_MENU):
            response = self._do_cmd_resp(Command.BACK_MENU,
                                         expected_prompt=MENU_PROMPTS)

    def _filter_capabilities(self, events):
        """
        Define a small filter of the capabilities
        @param events list of events to consider as capabilities
        @retval A list of events that are actually capabilities
        """
        events_out = [x for x in events if Capability.has(x)]
        return events_out

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

    def _handler_discover(self, *args, **kwargs):
        """
        Discover current state by going to the root menu
        @retval (next_state, next_agent_state)
        """

        # Try to break in case we are in auto sample
        self._send_break()

        next_state = ProtocolState.COMMAND
        next_agent_state = ResourceAgentState.IDLE

        return next_state, next_agent_state

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        """
        # Command device to update parameters and send a config change event.
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
        self._update_params()

        # fill the return values from the update
        for param in params:
            if not Parameter.has(param):
                raise InstrumentParameterException("Invalid parameter!")
            result_vals[param] = self._param_dict.get(param)
        result = result_vals

        #log.debug("Get finished, next: %s, result: %s", next_state, result)
        return next_state, result

    def _set_trhph_params(self, params):
        """
        Issue commands to the instrument to set various parameters
        """

        self._go_to_root_menu()
        for (key, val) in params.iteritems():
            if not Parameter.has(key):
                raise InstrumentParameterException()

            # restrict operations to just the read/write parameters
            if key == Parameter.CYCLE_TIME:
                self._navigate(SubMenu.CYCLE_TIME)
                (unit, value) = self._from_seconds(val)

                try:
                    self._do_cmd_resp(Command.DIRECT_SET, unit,
                                      expected_prompt=[Prompt.CYCLE_TIME_SEC_VALUE_PROMPT,
                                                       Prompt.CYCLE_TIME_MIN_VALUE_PROMPT])
                    self._do_cmd_resp(Command.DIRECT_SET, value,
                                      expected_prompt=Prompt.CHANGE_PARAM_MENU)

                except InstrumentParameterException:

                    self._go_to_root_menu()
                    raise InstrumentProtocolException("Could not set cycle time")

                self._go_to_root_menu()

            elif key == Parameter.METADATA_POWERUP:
                self._navigate(SubMenu.METADATA_POWERUP)
                result = self._do_cmd_resp(Command.DIRECT_SET, (1 + int(self._param_dict.get_init_value(key))),
                                           expected_prompt=Prompt.CHANGE_PARAM_MENU)
                if not result:
                    raise InstrumentParameterException("Could not set param %s" % key)

                self._go_to_root_menu()

            elif key == Parameter.METADATA_RESTART:

                self._navigate(SubMenu.METADATA_RESTART)
                result = self._do_cmd_resp(Command.DIRECT_SET, (1 + int(self._param_dict.get_init_value(key))),
                                           expected_prompt=Prompt.CHANGE_PARAM_MENU)
                if not result:
                    raise InstrumentParameterException("Could not set param %s" % key)

                self._go_to_root_menu()

            elif key == Parameter.VERBOSE:

                self._navigate(SubMenu.VERBOSE)
                result = self._do_cmd_resp(Command.DIRECT_SET, self._param_dict.get_init_value(key),
                                           expected_prompt=Prompt.CHANGE_PARAM_MENU)
                if not result:
                    raise InstrumentParameterException("Could not set param %s" % key)

                #need to set value direct because the instrument does not indicate whether it was successful
                #as long as the instrument returns from 'setting' with the command prompt, we assume success
                self._param_dict.set_value(key, self._param_dict.get_init_value(key))

                self._go_to_root_menu()

    def _set_params(self, *args, **kwargs):
        """
        Verify not readonly params and call set_trhph_params to issue commands to the instrument
        to set various parameters
        """
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        # set parameters are only allowed in COMMAND state
        if self.get_current_state() != ProtocolState.COMMAND:
            raise InstrumentProtocolException("Not in command state. Unable to set params")

        self._verify_not_readonly(*args, **kwargs)

        self._set_trhph_params(params)

        # re-sync with param dict
        self._go_to_root_menu()
        self._update_params()

    def _handler_command_set(self, *args, **kwargs):
        """
        Handle setting data from command mode

        @param params Dict of the parameters and values to pass to the state
        @retval return (next state, result)
        @throw InstrumentParameterException For invalid parameter
        """
        next_state = None
        result = None

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        try:
            params = args[0]

        except IndexError:
            raise InstrumentParameterException('_handler_command_set requires a parameter dict.')

        if params is None or (not isinstance(params, dict)):
            raise InstrumentParameterException()

        # Verify parameters are not read only
        self._verify_not_readonly(params, False)

        self._set_trhph_params(params)

        # re-sync with param dict
        self._go_to_root_menu()
        self._update_params()

        return next_state, result

    def _handler_command_autosample(self, *args, **kwargs):
        """
        Start autosample mode
        """

        result = None

        self._navigate(SubMenu.MAIN)
        self._do_cmd_no_resp(Command.START_AUTOSAMPLE)

        next_state = ProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        return next_state, (next_agent_state, result)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Acquire Instrument Status
        @retval return (next state, (next_agent_state, result))
        """

        self._navigate(SubMenu.MAIN)
        self._do_cmd_no_resp(Command.SHOW_STATUS)

        return None, (None, None)

    def _handler_command_start_direct(self):
        """
        @retval return (next state, (next_agent_state, result))
        """

        result = None
        next_state = ProtocolState.DIRECT_ACCESS
        next_agent_state = ResourceAgentState.DIRECT_ACCESS

        return next_state, (next_agent_state, result)

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
        @param data to be sent in direct access
        @retval return (next state, next_agent_state)
        """
        next_state = None
        next_agent_state = None

        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, next_agent_state

    def _handler_direct_access_stop_direct(self):
        """
        @throw InstrumentProtocolException on invalid command
        @retval return (next state, (next_agent_state, result))
        """

        next_state = ProtocolState.COMMAND
        next_agent_state = ResourceAgentState.COMMAND

        return next_state, (next_agent_state, None)

    ########################################################################
    # Autosample handlers
    ########################################################################
    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample mode
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        Acquire instrument's status in autosample state
        @retval return (next state, (next_agent_state, result))
        """

        # Break out of auto sample mode by sending control S to the instrument
        self._send_break()

        # Send the show parameter command to collect instrument's status
        self._navigate(SubMenu.MAIN)
        self._do_cmd_no_resp(Command.SHOW_STATUS)

        # Send the start autosample command to get back to autosample mode once
        # the instrument's status has been collected.
        self._navigate(SubMenu.MAIN)
        self._do_cmd_no_resp(Command.START_AUTOSAMPLE)

        return None, (None, None)

    def _handler_autosample_stop(self):
        """
        Stop autosample mode
        @retval return (next state, (next_agent_state, result))
        """
        next_state = None
        next_agent_state = None
        result = None

        if self._send_break():
            next_state = ProtocolState.COMMAND
            next_agent_state = ResourceAgentState.COMMAND

        return next_state, (next_agent_state, result)

    ########################################################################
    # Command builders
    ########################################################################    
    def _build_solo_command(self, cmd):
        """ Issue a simple command that does NOT require a newline at the end to
        execute. Likely used for control characters or special characters """
        return COMMAND_CHAR[cmd]

    def _build_menu_command(self, cmd):
        """ Pick the right character and add a newline """
        if COMMAND_CHAR[cmd]:
            return COMMAND_CHAR[cmd] + self._newline
        else:
            raise InstrumentProtocolException("Unknown command character for %s" % cmd)

    def _build_direct_command(self, cmd, arg):
        """ Build a command where we just send the argument to the instrument.
        Ignore the command part, we dont need it here as we are already in
        a submenu.
        """
        return "%s%s" % (arg, self._newline)

    ########################################################################
    # Command parsers
    ########################################################################
    def _parse_menu_change_response(self, response, prompt):
        """ Parse a response to a menu change
        
        @param response What was sent back from the command that was sent
        @param prompt The prompt that was returned from the device
        @retval The prompt that was encountered after the change
        """
        log.trace("Parsing menu change response with prompt: %s", prompt)
        return prompt

    def _parse_show_param_response(self, response, prompt):
        """ Parse the show parameter response screen """
        log.trace("Parsing show parameter screen")
        self._param_dict.update_many(response)

    ########################################################################
    # Utilities
    ########################################################################

    def _wakeup(self, timeout, delay=1):
        # Always awake for this instrument!
        pass

    def _got_chunk(self, chunk, timestamp):
        """
        extract samples from a chunk of data
        @param chunk: bytes to parse into a sample.
        """

        if not (self._extract_sample(BarsDataParticle, SAMPLE_REGEX, chunk, timestamp) or
                self._extract_sample(BarsStatusParticle, BarsStatusParticle.regex_compiled(), chunk, timestamp)):
            raise InstrumentProtocolException("Unhandled chunk")

    def _update_params(self):
        """
        Fetch the parameters from the device, and update the param dict.

        """

        old_config = self._param_dict.get_config()
        self._get_config()
        new_config = self._param_dict.get_config()
        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _get_config(self, *args, **kwargs):
        """ Get the entire configuration for the instrument
        
        @param params The parameters and values to set
        Should be a dict of parameters and values
        @throw InstrumentProtocolException On a deeper issue
        """
        # Just need to show the parameter screen...the parser for the command
        # does the update_many()
        self._go_to_root_menu()
        self._navigate(SubMenu.SHOW_PARAM)
        self._go_to_root_menu()

    def _send_break(self, timeout=4):
        """
        Execute an attempts to break out of auto sample (a few if things get garbled).
        For this instrument, it is done with a ^S, a wait for a \r\n, then
        another ^S within 1/2 a second
        @param timeout
        @retval True if 2 ^S chars were sent with a prompt in the middle, False
            if not.
        """
        log.debug("Sending break sequence to instrument...")
        # Timing is an issue, so keep it simple, work directly with the
        # couple chars instead of command/response. Could be done that way
        # though. Just more steps, logic, and delay for such a simple
        # exchange

        for count in range(0, 3):
            self._promptbuf = ""
            try:
                self._connection.send(COMMAND_CHAR[Command.BREAK])
                time.sleep(1)
                (prompt, result) = self._get_raw_response(timeout, expected_prompt=[Prompt.BREAK_ACK,
                                                                                    Prompt.CMD_PROMPT])
                if prompt == Prompt.BREAK_ACK:

                    self._connection.send(COMMAND_CHAR[Command.BREAK])
                    time.sleep(1)

                    self._get_response(timeout, expected_prompt=Prompt.CMD_PROMPT)
                    return True
                elif prompt == Prompt.CMD_PROMPT:
                    return True

            except InstrumentTimeoutException:
                continue

        log.trace("_send_break failing after several attempts")
        return False

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _build_command_dict(self):
        """
        Populate the command dictionary with command.
        """
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name="Start Autosample")
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, display_name="Stop Autosample")
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name="Acquire Status")
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        # Add parameter handlers to parameter dict.
        self._param_dict = ProtocolParameterDict()

        self._param_dict.add(Parameter.CYCLE_TIME,
                             r'(\d+)\s+= Cycle Time \(.*\)\r\n(0|1)\s+= Minutes or Seconds Cycle Time',
                             lambda match: self._to_seconds(int(match.group(1)),
                                                            int(match.group(2))),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Cycle Time",
                             visibility=ParameterDictVisibility.READ_WRITE,
                             startup_param=True,
                             direct_access=True,
                             default_value=20,
                             menu_path_read=SubMenu.SHOW_PARAM,
                             submenu_read=[],
                             menu_path_write=SubMenu.CHANGE_PARAM,
                             units=Units.SECOND,
                             description='Sample interval (15 - 3600), where time greater than 59 is rounded down to '
                                         'the nearest minute.',
                             submenu_write=[["1", Prompt.CYCLE_TIME_PROMPT]])

        self._param_dict.add(Parameter.VERBOSE,
                             r'bogusdatadontmatch',  # Write-only
                             lambda match: None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Verbose",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             init_value=0,
                             description="Enable verbosity with data points (1:on | 0:off)",
                             menu_path_write=SubMenu.CHANGE_PARAM,
                             submenu_write=[["2", Prompt.VERBOSE_PROMPT]])

        self._param_dict.add(Parameter.METADATA_POWERUP,
                             r'(0|1)\s+= Metadata Print Status on Power up',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Metadata on Powerup",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             init_value=0,
                             description="Enable display of metadata at startup (1:on | 0:off)",
                             menu_path_write=SubMenu.CHANGE_PARAM,
                             submenu_write=[["3", Prompt.METADATA_PROMPT]])

        self._param_dict.add(Parameter.METADATA_RESTART,
                             r'(0|1)\s+= Metadata Print Status on Restart Data Collection',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Metadata on Restart",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             init_value=0,
                             description="Enable display of metadata at restart (1:on | 0:off)",
                             menu_path_write=SubMenu.CHANGE_PARAM,
                             submenu_write=[["4", Prompt.METADATA_PROMPT]])

        self._param_dict.add(Parameter.RES_SENSOR_POWER,
                             r'(0|1)\s+= Res Power',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Res Sensor Power",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             startup_param=False,
                             direct_access=False,
                             menu_path_read=SubMenu.SHOW_PARAM,
                             submenu_read=[],
                             description="Enable res sensor power (1:on | 0:off)",
                             menu_path_write=SubMenu.SENSOR_POWER,
                             submenu_write=[["1"]])

        self._param_dict.add(Parameter.INST_AMP_POWER,
                             r'(0|1)\s+= Thermocouple & Hydrogen Amp Power',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Instrumentation Amp Power",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             startup_param=False,
                             direct_access=False,
                             description="Enable instrumentation amp power (1:on | 0:off)",
                             menu_path_read=SubMenu.SHOW_PARAM,
                             submenu_read=[],
                             menu_path_write=SubMenu.SENSOR_POWER,
                             submenu_write=[["2"]])

        self._param_dict.add(Parameter.EH_ISOLATION_AMP_POWER,
                             r'(0|1)\s+= eh Amp Power',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="eH Isolation Amp Power",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             startup_param=False,
                             direct_access=False,
                             description="Enable eH isolation amp power (1:on | 0:off)",
                             menu_path_read=SubMenu.SHOW_PARAM,
                             submenu_read=[],
                             menu_path_write=SubMenu.SENSOR_POWER,
                             submenu_write=[["3"]])

        self._param_dict.add(Parameter.HYDROGEN_POWER,
                             r'(0|1)\s+= Hydrogen Sensor Power',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Hydrogen Sensor Power",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             startup_param=False,
                             direct_access=False,
                             description="Enable hydrogen sensor power (1:on | 0:off)",
                             menu_path_read=SubMenu.SHOW_PARAM,
                             submenu_read=[],
                             menu_path_write=SubMenu.SENSOR_POWER,
                             submenu_write=[["4"]])

        self._param_dict.add(Parameter.REFERENCE_TEMP_POWER,
                             r'(0|1)\s+= Reference Temperature Power',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Reference Temp Power",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             startup_param=False,
                             direct_access=False,
                             description="Enable reference temperature power (1:on | 0:off)",
                             menu_path_read=SubMenu.SHOW_PARAM,
                             submenu_read=[],
                             menu_path_write=SubMenu.SENSOR_POWER,
                             submenu_write=[["5"]])

    @staticmethod
    def _to_seconds(value, unit):
        """
        Converts a number and a unit into seconds. Ie if "4" and "1"
        comes in, it spits out 240
        @param value The int value for some number of minutes or seconds
        @param unit int of 0 or 1 where 0 is seconds, 1 is minutes
        @return Number of seconds.
        """
        if (not isinstance(value, int)) or (not isinstance(unit, int)):
            raise InstrumentProtocolException("Invalid second arguments!")

        if unit == 1:
            return value * 60
        elif unit == 0:
            return value
        else:
            raise InstrumentProtocolException("Invalid Units!")

    @staticmethod
    def _from_seconds(value):
        """
        Converts a number of seconds into a (unit, value) tuple.
        
        @param value The number of seconds to convert
        @retval A tuple of unit and value where the unit is 1 for seconds and 2
            for minutes. If the value is 15-59, units should be returned in
            seconds. If the value is over 59, the units will be returned in
            a number of minutes where the seconds are rounded down to the
            nearest minute.
        """

        if (value < 15) or (value > 3600):
            raise InstrumentParameterException("Invalid seconds value: %s" % value)

        if value < 60:
            return 1, value
        else:
            return 2, value // 60