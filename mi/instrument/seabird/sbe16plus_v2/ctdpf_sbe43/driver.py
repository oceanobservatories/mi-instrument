"""
@package mi.instrument.seabird.sbe16plus_v2.ctdpf_sbe43.driver
@file marine-integrations/mi/instrument/seabird/sbe16plus_v2/ctdpf_sbe43/driver.py
@author Tapana Gupta
@brief Driver for the ctdpf_sbe43
Release notes:

SBE Driver
"""
__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'

import re
import string
import time

from mi.core.log import get_logger

log = get_logger()

from mi.core.util import dict_equal
from mi.core.common import BaseEnum
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.instrument_driver import DriverEvent, ResourceAgentState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import DriverAsyncEvent

from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import SampleException

from xml.dom.minidom import parseString

from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType

from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import ParameterUnit
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import ProtocolState
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19Protocol
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19ConfigurationParticle
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19CalibrationParticle

from mi.instrument.seabird.sbe16plus_v2.driver import Prompt

from mi.instrument.seabird.driver import SeaBirdParticle
from mi.instrument.seabird.driver import SeaBirdInstrumentDriver

from mi.instrument.seabird.driver import NEWLINE
from mi.instrument.seabird.driver import TIMEOUT


# Driver constants
WAKEUP_TIMEOUT = 60
MIN_PUMP_DELAY = 0
MAX_PUMP_DELAY = 600
MIN_AVG_SAMPLES = 1
MAX_AVG_SAMPLES = 32767


class Command(BaseEnum):
    GET_CD = 'GetCD'
    GET_SD = 'GetSD'
    GET_CC = 'GetCC'
    GET_EC = 'GetEC'
    RESET_EC = 'ResetEC'
    GET_HD = 'GetHD'
    START_NOW = 'StartNow'
    STOP = 'Stop'
    TS = 'ts'
    SET = 'set'


class Parameter(DriverParameter):
    """
    Device specific parameters for SBE43.
    """
    DATE_TIME = "DateTime"
    PTYPE = "PType"
    VOLT0 = "Volt0"
    VOLT1 = "Volt1"
    VOLT2 = "Volt2"
    VOLT3 = "Volt3"
    VOLT4 = "Volt4"
    VOLT5 = "Volt5"
    SBE38 = "SBE38"
    WETLABS = "WetLabs"
    GTD = "GTD"
    DUAL_GTD = "DualGTD"
    SBE63 = "SBE63"
    OPTODE = "OPTODE"
    OUTPUT_FORMAT = "OutputFormat"
    NUM_AVG_SAMPLES = "Navg"
    MIN_COND_FREQ = "MinCondFreq"
    PUMP_DELAY = "PumpDelay"
    AUTO_RUN = "AutoRun"
    IGNORE_SWITCH = "IgnoreSwitch"
    LOGGING = "logging"


class ConfirmedParameter(BaseEnum):
    """
    List of all parameters that require confirmation
    i.e. set sent twice to confirm.
    """
    PTYPE = Parameter.PTYPE
    SBE38 = Parameter.SBE38
    GTD = Parameter.GTD
    DUAL_GTD = Parameter.DUAL_GTD
    SBE63 = Parameter.SBE63
    OPTODE = Parameter.OPTODE
    WETLABS = Parameter.WETLABS
    VOLT0 = Parameter.VOLT0
    VOLT1 = Parameter.VOLT1
    VOLT2 = Parameter.VOLT2
    VOLT3 = Parameter.VOLT3
    VOLT4 = Parameter.VOLT4
    VOLT5 = Parameter.VOLT5


class ProtocolEvent(BaseEnum):
    """
    Protocol events for SBE43. Cherry picked from DriverEvent enum.
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    GET_CONFIGURATION = 'PROTOCOL_EVENT_GET_CONFIGURATION'
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS


###############################################################################
# Data Particles
###############################################################################
class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    CTD_PARSED = 'ctdpf_sbe43_sample'
    DEVICE_STATUS = 'ctdpf_sbe43_status'
    DEVICE_CALIBRATION = 'ctdpf_sbe43_calibration_coefficients'
    DEVICE_HARDWARE = 'ctdpf_sbe43_hardware'
    DEVICE_CONFIGURATION = 'ctdpf_sbe43_configuration'


class SBE43DataParticleKey(BaseEnum):
    TEMP = "temperature"
    CONDUCTIVITY = "conductivity"
    PRESSURE = "pressure"
    PRESSURE_TEMP = "pressure_temp"
    VOLT0 = "ext_volt0"


class SBE43DataParticle(SeaBirdParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.

    Sample:
       #04570F0A1E910828FC47BC59F1

    Format:
       #ttttttccccccppppppvvvvvvvv

       Temperature = tttttt
       Conductivity = cccccc
       Pressure = pppppp
       Pressure sensor temperature compensation = vvvv
       First external voltage = vvvv
    """
    _data_particle_type = DataParticleType.CTD_PARSED

    @staticmethod
    def regex():
        """
        Regular expression to match a sample pattern

        Format:
            #ttttttccccccppppppvvvvvvvv
        @return: regex string
        """
        pattern = r'#? *'  # pattern may or may not start with a '
        pattern += r'([0-9A-F]{6})'  # temperature
        pattern += r'([0-9A-F]{6})'  # conductivity
        pattern += r'([0-9A-F]{6})'  # pressure
        pattern += r'([0-9A-F]{4})'  # pressure temp
        pattern += r'([0-9A-F]{4})'  # volt0
        pattern += NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(SBE43DataParticle.regex())

    def _build_parsed_values(self):
        """
        Take something in the autosample/TS format and split it into
        the 5 measurements as stated above

        @throws SampleException If there is a problem with sample creation
        """
        match = SBE43DataParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" %
                                  self.raw_data)

        try:
            temperature = self.hex2value(match.group(1))
            conductivity = self.hex2value(match.group(2))
            pressure = self.hex2value(match.group(3))
            pressure_temp = self.hex2value(match.group(4))
            volt0 = self.hex2value(match.group(5))

        except ValueError:
            raise SampleException("ValueError while converting data: [%s]" %
                                  self.raw_data)

        result = [{DataParticleKey.VALUE_ID: SBE43DataParticleKey.TEMP,
                   DataParticleKey.VALUE: temperature},
                  {DataParticleKey.VALUE_ID: SBE43DataParticleKey.CONDUCTIVITY,
                   DataParticleKey.VALUE: conductivity},
                  {DataParticleKey.VALUE_ID: SBE43DataParticleKey.PRESSURE,
                   DataParticleKey.VALUE: pressure},
                  {DataParticleKey.VALUE_ID: SBE43DataParticleKey.PRESSURE_TEMP,
                   DataParticleKey.VALUE: pressure_temp},
                  {DataParticleKey.VALUE_ID: SBE43DataParticleKey.VOLT0,
                   DataParticleKey.VALUE: volt0}]

        return result


class SBE43StatusParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"

    DATE_TIME = "date_time_string"
    LOGGING_STATE = "logging_status"
    NUMBER_OF_EVENTS = "num_events"

    BATTERY_VOLTAGE_MAIN = "battery_voltage_main"
    BATTERY_VOLTAGE_LITHIUM = "battery_voltage_lithium"
    OPERATIONAL_CURRENT = "operational_current"
    PUMP_CURRENT = "pump_current"
    EXT_V01_CURRENT = "ext_v01_current"

    MEMORY_FREE = "mem_free"
    NUMBER_OF_SAMPLES = "num_samples"
    SAMPLES_FREE = "samples_free"
    SAMPLE_LENGTH = "sample_length"
    PROFILES = "profiles"


# noinspection PyPep8Naming
class SBE43StatusParticle(SeaBirdParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.DEVICE_STATUS

    @staticmethod
    def regex():
        pattern = r'(<StatusData.*?</StatusData>)' + NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(SBE43StatusParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        pattern = r'(<StatusData.*?</StatusData>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        return re.compile(SBE43StatusParticle.resp_regex(), re.DOTALL)

    def _map_param_to_xml_tag(self, parameter_name):
        map_param_to_tag = {SBE43StatusParticleKey.BATTERY_VOLTAGE_MAIN: "vMain",
                            SBE43StatusParticleKey.BATTERY_VOLTAGE_LITHIUM: "vLith",
                            SBE43StatusParticleKey.OPERATIONAL_CURRENT: "iMain",
                            SBE43StatusParticleKey.PUMP_CURRENT: "iPump",
                            SBE43StatusParticleKey.EXT_V01_CURRENT: "iExt01",

                            SBE43StatusParticleKey.MEMORY_FREE: "Bytes",
                            SBE43StatusParticleKey.NUMBER_OF_SAMPLES: "Samples",
                            SBE43StatusParticleKey.SAMPLES_FREE: "SamplesFree",
                            SBE43StatusParticleKey.SAMPLE_LENGTH: "SampleLength",
                            SBE43StatusParticleKey.PROFILES: "Profiles"}
        return map_param_to_tag[parameter_name]

    def _build_parsed_values(self):
        """
        Parse the output of the getSD command
        @throws SampleException If there is a problem with sample creation
        """

        SERIAL_NUMBER = "SerialNumber"
        DATE_TIME = "DateTime"
        LOGGING_STATE = "LoggingState"
        EVENT_SUMMARY = "EventSummary"
        NUMBER_OF_EVENTS = "numEvents"
        POWER = "Power"
        MEMORY_SUMMARY = "MemorySummary"

        # check to make sure there is a correct match before continuing
        match = SBE43StatusParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise SampleException("No regex match of parsed status data: [%s]" %
                                  self.raw_data)

        dom = parseString(self.raw_data)
        root = dom.documentElement
        log.debug("root.tagName = %s", root.tagName)
        serial_number = root.getAttribute(SERIAL_NUMBER)

        date_time = self._extract_xml_element_value(root, DATE_TIME)
        logging_status = self._extract_xml_element_value(root, LOGGING_STATE)
        event_summary = self._extract_xml_elements(root, EVENT_SUMMARY)[0]
        number_of_events = int(event_summary.getAttribute(NUMBER_OF_EVENTS))
        result = [{DataParticleKey.VALUE_ID: SBE43StatusParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number},
                  {DataParticleKey.VALUE_ID: SBE43StatusParticleKey.DATE_TIME,
                   DataParticleKey.VALUE: date_time},
                  {DataParticleKey.VALUE_ID: SBE43StatusParticleKey.LOGGING_STATE,
                   DataParticleKey.VALUE: logging_status},
                  {DataParticleKey.VALUE_ID: SBE43StatusParticleKey.NUMBER_OF_EVENTS,
                   DataParticleKey.VALUE: number_of_events}]

        element = self._extract_xml_elements(root, POWER)[0]
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.BATTERY_VOLTAGE_MAIN))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.BATTERY_VOLTAGE_LITHIUM))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.OPERATIONAL_CURRENT))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.PUMP_CURRENT))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.EXT_V01_CURRENT))

        element = self._extract_xml_elements(root, MEMORY_SUMMARY)[0]
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.MEMORY_FREE, int))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.NUMBER_OF_SAMPLES, int))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.SAMPLES_FREE, int))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.SAMPLE_LENGTH, int))
        result.append(self._get_xml_parameter(element, SBE43StatusParticleKey.PROFILES, int))

        log.debug("Status Particle: %s" % result)

        return result


class SBE43ConfigurationParticle(SBE19ConfigurationParticle):
    """
    This data particle is identical to the corresponding one for CTDPF-Optode, except for the stream
    name, which we specify here
    """
    _data_particle_type = DataParticleType.DEVICE_CONFIGURATION


class SBE43HardwareParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"
    FIRMWARE_VERSION = "firmware_version"
    FIRMWARE_DATE = "firmware_date"
    COMMAND_SET_VERSION = "command_set_version"
    PCB_SERIAL_NUMBER = "pcb_serial_number"
    ASSEMBLY_NUMBER = "assembly_number"
    MANUFACTURE_DATE = "manufacture_date"
    TEMPERATURE_SENSOR_SERIAL_NUMBER = 'temp_sensor_serial_number'
    CONDUCTIVITY_SENSOR_SERIAL_NUMBER = 'cond_sensor_serial_number'
    PRESSURE_SENSOR_TYPE = 'pressure_sensor_type'
    PRESSURE_SENSOR_SERIAL_NUMBER = 'strain_pressure_sensor_serial_number'
    VOLT0_TYPE = 'volt0_type'
    VOLT0_SERIAL_NUMBER = 'volt0_serial_number'


# noinspection PyPep8Naming
class SBE43HardwareParticle(SeaBirdParticle):
    _data_particle_type = DataParticleType.DEVICE_HARDWARE

    @staticmethod
    def regex():
        """
        Regular expression to match a getHD response pattern
        @return: regex string
        """
        pattern = r'(<HardwareData.*?</HardwareData>)' + NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(SBE43HardwareParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        """
        Regular expression to match a getHD response pattern
        @return: regex string
        """
        pattern = r'(<HardwareData.*?</HardwareData>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(SBE43HardwareParticle.resp_regex(), re.DOTALL)

    def _build_parsed_values(self):
        """
        @throws SampleException If there is a problem with sample creation
        """

        SENSOR = "Sensor"
        TYPE = "type"
        ID = "id"
        PCB_SERIAL_NUMBER = "PCBSerialNum"
        ASSEMBLY_NUMBER = "AssemblyNum"
        SERIAL_NUMBER = "SerialNumber"
        FIRMWARE_VERSION = "FirmwareVersion"
        FIRMWARE_DATE = "FirmwareDate"
        COMMAND_SET_VERSION = "CommandSetVersion"
        PCB_ASSEMBLY = "PCBAssembly"
        MANUFACTURE_DATE = "MfgDate"
        INTERNAL_SENSORS = "InternalSensors"
        TEMPERATURE_SENSOR_ID = "Main Temperature"
        CONDUCTIVITY_SENSOR_ID = "Main Conductivity"
        PRESSURE_SENSOR_ID = "Main Pressure"
        EXTERNAL_SENSORS = "ExternalSensors"
        VOLT0 = "volt 0"

        # check to make sure there is a correct match before continuing
        match = SBE43HardwareParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise SampleException("No regex match of parsed hardware data: [%s]" %
                                  self.raw_data)

        dom = parseString(self.raw_data)
        root = dom.documentElement
        log.debug("root.tagName = %s", root.tagName)
        serial_number = root.getAttribute(SERIAL_NUMBER)

        firmware_version = self._extract_xml_element_value(root, FIRMWARE_VERSION)
        firmware_date = self._extract_xml_element_value(root, FIRMWARE_DATE)
        command_set_version = self._extract_xml_element_value(root, COMMAND_SET_VERSION)
        manufacture_date = self._extract_xml_element_value(root, MANUFACTURE_DATE)

        pcb_assembly_elements = self._extract_xml_elements(root, PCB_ASSEMBLY)
        pcb_serial_number = []
        pcb_assembly = []
        for assembly in pcb_assembly_elements:
            pcb_serial_number.append(assembly.getAttribute(PCB_SERIAL_NUMBER))
            pcb_assembly.append(assembly.getAttribute(ASSEMBLY_NUMBER))

        temperature_sensor_serial_number = ""
        conductivity_sensor_serial_number = ""
        pressure_sensor_serial_number = ""
        pressure_sensor_type = ""
        volt0_serial_number = 0
        volt0_type = ""

        internal_sensors_element = self._extract_xml_elements(root, INTERNAL_SENSORS)[0]
        sensors = self._extract_xml_elements(internal_sensors_element, SENSOR)

        for sensor in sensors:
            sensor_id = sensor.getAttribute(ID)
            if sensor_id == TEMPERATURE_SENSOR_ID:
                temperature_sensor_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
            elif sensor_id == CONDUCTIVITY_SENSOR_ID:
                conductivity_sensor_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
            elif sensor_id == PRESSURE_SENSOR_ID:
                pressure_sensor_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
                pressure_sensor_type = self._extract_xml_element_value(sensor, TYPE)

        external_sensors_element = self._extract_xml_elements(root, EXTERNAL_SENSORS)[0]
        sensors = self._extract_xml_elements(external_sensors_element, SENSOR)

        for sensor in sensors:
            sensor_id = sensor.getAttribute(ID)
            if sensor_id == VOLT0:
                volt0_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
                volt0_type = self._extract_xml_element_value(sensor, TYPE)

        result = [{DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.FIRMWARE_VERSION,
                   DataParticleKey.VALUE: firmware_version},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.FIRMWARE_DATE,
                   DataParticleKey.VALUE: firmware_date},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.COMMAND_SET_VERSION,
                   DataParticleKey.VALUE: command_set_version},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.MANUFACTURE_DATE,
                   DataParticleKey.VALUE: manufacture_date},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.PCB_SERIAL_NUMBER,
                   DataParticleKey.VALUE: pcb_serial_number},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.ASSEMBLY_NUMBER,
                   DataParticleKey.VALUE: pcb_assembly},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.TEMPERATURE_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: temperature_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.CONDUCTIVITY_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: conductivity_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.PRESSURE_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: pressure_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.PRESSURE_SENSOR_TYPE,
                   DataParticleKey.VALUE: pressure_sensor_type},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.VOLT0_SERIAL_NUMBER,
                   DataParticleKey.VALUE: volt0_serial_number},
                  {DataParticleKey.VALUE_ID: SBE43HardwareParticleKey.VOLT0_TYPE,
                   DataParticleKey.VALUE: volt0_type}]

        return result


class SBE43CalibrationParticle(SBE19CalibrationParticle):
    """
    This data particle is identical to the corresponding one for CTDPF-Optode, except for the stream
    name, which we specify here
    """
    _data_particle_type = DataParticleType.DEVICE_CALIBRATION


###############################################################################
# Driver
###############################################################################
# noinspection PyMethodMayBeStatic
class InstrumentDriver(SeaBirdInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    def __init__(self, evt_callback):
        """
        InstrumentDriver constructor.
        @param evt_callback Driver process event callback.
        """
        SeaBirdInstrumentDriver.__init__(self, evt_callback)

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
        self._protocol = SBE43Protocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################
class SBE43Protocol(SBE19Protocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """

    def __init__(self, prompts, newline, driver_event):
        """
        SBE43Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The SBE43 newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        # Build SBE19 protocol state machine.
        self._protocol_fsm = InstrumentFSM(ProtocolState, ProtocolEvent,
                                           ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        handlers = {
            ProtocolState.UNKNOWN: [
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.EXIT, self._handler_unknown_exit),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ],
            ProtocolState.COMMAND: [
                (ProtocolEvent.ENTER, self._handler_command_enter),
                (ProtocolEvent.EXIT, self._handler_command_exit),
                (ProtocolEvent.ACQUIRE_SAMPLE, self._handler_command_acquire_sample),
                (ProtocolEvent.GET_CONFIGURATION, self._handler_command_get_configuration),
                (ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample),
                (ProtocolEvent.GET, self._handler_command_get),
                (ProtocolEvent.SET, self._handler_command_set),
                (ProtocolEvent.START_DIRECT, self._handler_command_start_direct),
                (ProtocolEvent.CLOCK_SYNC, self._handler_command_clock_sync_clock),
                (ProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status)
            ],
            ProtocolState.DIRECT_ACCESS: [
                (ProtocolEvent.ENTER, self._handler_direct_access_enter),
                (ProtocolEvent.EXIT, self._handler_direct_access_exit),
                (ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct),
                (ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct)
            ],
            ProtocolState.AUTOSAMPLE: [
                (ProtocolEvent.ENTER, self._handler_autosample_enter),
                (ProtocolEvent.EXIT, self._handler_autosample_exit),
                (ProtocolEvent.GET, self._handler_command_get),
                (ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample),
                (ProtocolEvent.ACQUIRE_STATUS, self._handler_autosample_acquire_status),
                (ProtocolEvent.GET_CONFIGURATION, self._handler_autosample_get_configuration)
            ]
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_driver_dict()
        self._build_command_dict()
        self._build_param_dict()

        self._add_build_handler(Command.GET_CD, self._build_simple_command)
        self._add_build_handler(Command.GET_SD, self._build_simple_command)
        self._add_build_handler(Command.GET_CC, self._build_simple_command)
        self._add_build_handler(Command.GET_EC, self._build_simple_command)
        self._add_build_handler(Command.RESET_EC, self._build_simple_command)
        self._add_build_handler(Command.GET_HD, self._build_simple_command)
        self._add_build_handler(Command.START_NOW, self._build_simple_command)
        self._add_build_handler(Command.STOP, self._build_simple_command)
        self._add_build_handler(Command.TS, self._build_simple_command)

        self._add_build_handler(Command.SET, self._build_set_command)

        # Add build handlers for device commands, only using simple command handler.
        for cmd in Command.list():
            if cmd == Command.SET:
                self._add_build_handler(Command.SET, self._build_set_command)
            else:
                self._add_build_handler(cmd, self._build_simple_command)

        # Add response handlers for device commands.
        # these are here to ensure that correct responses to the commands are received before the next command is sent
        self._add_response_handler(Command.SET, self._parse_set_response)
        self._add_response_handler(Command.GET_SD, self._validate_GetSD_response)
        self._add_response_handler(Command.GET_HD, self._validate_GetHD_response)
        self._add_response_handler(Command.GET_CD, self._validate_GetCD_response)
        self._add_response_handler(Command.GET_CC, self._validate_GetCC_response)
        self._add_response_handler(Command.GET_EC, self._validate_GetEC_response)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        self._chunker = StringChunker(self.sieve_function)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        Over-ride sieve function to handle additional particles.
        """
        matchers = []
        return_list = []

        matchers.append(SBE43DataParticle.regex_compiled())
        matchers.append(SBE43HardwareParticle.regex_compiled())
        matchers.append(SBE43CalibrationParticle.regex_compiled())
        matchers.append(SBE43StatusParticle.regex_compiled())
        matchers.append(SBE43ConfigurationParticle.regex_compiled())

        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, timestamp):
        """
        Over-ride sieve function to handle additional particles.
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        for particle_class in SBE43HardwareParticle, SBE43DataParticle, SBE43CalibrationParticle, \
                              SBE43ConfigurationParticle, SBE43StatusParticle:
            if self._extract_sample(particle_class, particle_class.regex_compiled(), chunk, timestamp):
                return

        raise InstrumentProtocolException("Unhandled chunk %s" % chunk)

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        # check values that the instrument doesn't validate
        # handle special cases for driver specific parameters
        for (key, val) in params.iteritems():
            if key == Parameter.PUMP_DELAY and (val < MIN_PUMP_DELAY or val > MAX_PUMP_DELAY):
                raise InstrumentParameterException("pump delay out of range")
            elif key == Parameter.NUM_AVG_SAMPLES and (val < MIN_AVG_SAMPLES or val > MAX_AVG_SAMPLES):
                raise InstrumentParameterException("num average samples out of range")

        self._verify_not_readonly(*args, **kwargs)

        for (key, val) in params.iteritems():

            old_val = self._param_dict.get(key)
            log.debug("KEY = %r OLD VALUE = %r NEW VALUE = %r", key, old_val, val)

            if old_val != val:
                if key in ConfirmedParameter.list():
                    # We add a write delay here because this command has to be sent
                    # twice, the write delay allows it to process the first command
                    # before it receives the beginning of the second.
                    self._do_cmd_resp(Command.SET, key, val, write_delay=0.2)
                else:
                    self._do_cmd_resp(Command.SET, key, val, **kwargs)

        log.debug("set complete, update params")
        self._update_params()

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @retval (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        (next_agent_state, None) if successful.
        """
        return ProtocolState.AUTOSAMPLE, (ResourceAgentState.STREAMING, None)

    def _start_logging(self, *args, **kwargs):
        """
        Command the instrument to start logging
        @raise: InstrumentProtocolException if failed to start logging
        """
        if not self._is_logging():

            self._do_cmd_no_resp(Command.START_NOW, *args, **kwargs)
            time.sleep(2)

            if not self._is_logging():
                raise InstrumentProtocolException("failed to start logging")

    def _stop_logging(self, *args, **kwargs):
        """
        Command the instrument to stop logging
        @param timeout: how long to wait for a prompt
        @raise: InstrumentTimeoutException if prompt isn't seen
        @raise: InstrumentProtocolException failed to stop logging
        """

        # Issue the stop command.
        # We can get here from handle_unknown_discover, hence it's possible that the current state is unknown
        # handle_unknown_discover checks if we are currently streaming before we get here.
        if self.get_current_state() in [ProtocolState.AUTOSAMPLE, ProtocolState.UNKNOWN]:
            kwargs['timeout'] = TIMEOUT
            self._do_cmd_resp(Command.STOP, *args, **kwargs)

        if self._is_logging(*args, **kwargs):
            raise InstrumentProtocolException("failed to stop logging")

    def _is_logging(self, *args, **kwargs):
        """
        Wake up the instrument and inspect the prompt to determine if we
        are in streaming
        @return: True - instrument logging, False - not logging,
                 None - unknown logging state
        @raise: InstrumentProtocolException if we can't identify the prompt
        """
        response = self._do_cmd_resp(Command.GET_SD, response_regex=SBE43StatusParticle.regex_compiled(), timeout=TIMEOUT)
        for line in response.split(NEWLINE):
            self._param_dict.update(line)

        return self._param_dict.get(Parameter.LOGGING)

    ########################################################################
    # Command handlers.
    ########################################################################
    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary. Wake the device then issue
        display status and display calibration commands. The parameter
        dict will match line output and update itself.
        @throws InstrumentTimeoutException if device cannot be timely woken.
        @throws InstrumentProtocolException if ds/dc misunderstood.
        """
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)

        # Get old param dict config.
        old_config = self._param_dict.get_config()

        # Issue display commands and parse results.
        response = self._do_cmd_resp(Command.GET_SD, response_regex=SBE43StatusParticle.regex_compiled(),
                                     timeout=TIMEOUT)
        for line in response.split(NEWLINE):
            self._param_dict.update(line)

        response = self._do_cmd_resp(Command.GET_CD, response_regex=SBE43ConfigurationParticle.regex_compiled(),
                                     timeout=TIMEOUT)
        for line in response.split(NEWLINE):
            self._param_dict.update(line)

        response = self._do_cmd_resp(Command.GET_HD, response_regex=SBE43HardwareParticle.regex_compiled(),
                                     timeout=TIMEOUT)

        pressure_sensor_match = r"<Sensor id = 'Main Pressure'>.*?<type>(.*?)</type>.*?</Sensor>"
        pressure_sensor_regex = re.compile(pressure_sensor_match.regex(), re.DOTALL)
        match = pressure_sensor_regex.match(response)
        self._param_dict.update(match.group(1), Parameter.PTYPE)

        # Get new param dict config. If it differs from the old config,
        # tell driver superclass to publish a config change event.
        new_config = self._param_dict.get_config()

        log.debug("Old Config: %s", old_config)
        log.debug("New Config: %s", new_config)
        if not dict_equal(new_config, old_config) and self._protocol_fsm.get_current_state() != ProtocolState.UNKNOWN:
            log.debug("parameters updated, sending event")
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        result = []

        result.append(self._do_cmd_resp(Command.GET_SD, response_regex=SBE43StatusParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, response_regex=SBE43HardwareParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, response_regex=SBE43ConfigurationParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, response_regex=SBE43CalibrationParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetEC Response: %s", result)

        # Reset the event counter right after getEC
        self._do_cmd_resp(Command.RESET_EC, timeout=TIMEOUT)

        return None, (None, ''.join(result))

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        Get device status in autosample mode
        """
        result = []

        # When in autosample this command requires two wake-ups to get to the right prompt
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)

        result.append(self._do_cmd_resp(Command.GET_SD, response_regex=SBE43StatusParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, response_regex=SBE43HardwareParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, response_regex=SBE43ConfigurationParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, response_regex=SBE43CalibrationParticle.regex_compiled(),
                                        timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetEC Response: %s", result)

        # Reset the event counter right after getEC
        self._do_cmd_no_resp(Command.RESET_EC)

        return None, (None, ''.join(result))

    def _build_set_command(self, cmd, param, val):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @ retval The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """
        try:
            str_val = self._param_dict.format(param, val)
            set_cmd = '%s=%s%s' % (param, str_val, NEWLINE)

            # Some set commands need to be sent twice to confirm
            if param in ConfirmedParameter.list():
                set_cmd += set_cmd

        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter %s' % param)

        return set_cmd

    ########################################################################
    # response handlers.
    ########################################################################
    def _validate_GetSD_response(self, response, prompt):
        """
        validation handler for GetSD command
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("_validate_GetSD_response: GetSD command encountered error; type='%s' msg='%s'", error[0],
                      error[1])
            raise InstrumentProtocolException('GetSD command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if not SBE43StatusParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetSD_response: GetSD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetSD command not recognized: %s.' % response)

        return response

    def _validate_GetHD_response(self, response, prompt):
        """
        validation handler for GetHD command
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("GetHD command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('GetHD command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if not SBE43HardwareParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetHD_response: GetHD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetHD command not recognized: %s.' % response)

        return response

    def _validate_GetCD_response(self, response, prompt):
        """
        validation handler for GetCD command
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("GetCD command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('GetCD command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if not SBE43ConfigurationParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetCD_response: GetCD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetCD command not recognized: %s.' % response)

        return response

    ########################################################################
    # Private helpers.
    ########################################################################
    def _build_param_dict(self):
        """
        Populate the parameter dictionary with SBE19 parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        # Add parameter handlers to parameter dict.
        self._param_dict.add(Parameter.DATE_TIME,
                             r'DateTime>(.*)</DateTime',
                             lambda match: string.upper(match.group(1)),
                             self._date_time_string_to_numeric,
                             type=ParameterDictType.STRING,
                             display_name="Date/Time",
                             visibility=ParameterDictVisibility.READ_ONLY)
        self._param_dict.add(Parameter.LOGGING,
                             r'LoggingState>(not )?logging</LoggingState',
                             lambda match: False if (match.group(1)) else True,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Is Logging",
                             visibility=ParameterDictVisibility.READ_ONLY)
        self._param_dict.add(Parameter.PTYPE,
                             r"<Sensor id = 'Main Pressure'>.*?<type>(.*?)</type>.*?</Sensor>",
                             self._pressure_sensor_to_int,
                             str,
                             type=ParameterDictType.INT,
                             display_name="Pressure Sensor Type",
                             startup_param=True,
                             direct_access=True,
                             default_value=1,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.VOLT0,
                             r'ExtVolt0>(.*)</ExtVolt0',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 0",
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.VOLT1,
                             r'ExtVolt1>(.*)</ExtVolt1',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 1",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.VOLT2,
                             r'ExtVolt2>(.*)</ExtVolt2',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 2",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.VOLT3,
                             r'ExtVolt3>(.*)</ExtVolt3',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 3",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.VOLT4,
                             r'ExtVolt4>(.*)</ExtVolt4',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 4",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.VOLT5,
                             r'ExtVolt5>(.*)</ExtVolt5',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 5",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.SBE38,
                             r'SBE38>(.*)</SBE38',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="SBE38 Attached",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.WETLABS,
                             r'WETLABS>(.*)</WETLABS',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Enable Wetlabs sensor",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.GTD,
                             r'GTD>(.*)</GTD',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="GTD Attached",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.DUAL_GTD,
                             r'GTD>(.*)</GTD',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Dual GTD Attached",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.SBE63,
                             r'SBE63>(.*)</SBE63',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="SBE63 Attached",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.OPTODE,
                             r'OPTODE>(.*)</OPTODE',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Optode Attached",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.OUTPUT_FORMAT,
                             r'OutputFormat>(.*)</OutputFormat',
                             self._output_format_string_2_int,
                             int,
                             type=ParameterDictType.INT,
                             display_name="Output Format",
                             startup_param=True,
                             direct_access=True,
                             default_value=0,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.NUM_AVG_SAMPLES,
                             r'ScansToAverage>([\d]+)</ScansToAverage>',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Scans To Average",
                             startup_param=True,
                             direct_access=False,
                             default_value=4,
                             visibility=ParameterDictVisibility.READ_WRITE)
        self._param_dict.add(Parameter.MIN_COND_FREQ,
                             r'MinimumCondFreq>([\d]+)</MinimumCondFreq',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Minimum Conductivity Frequency",
                             startup_param=True,
                             direct_access=False,
                             default_value=500,
                             units=ParameterUnit.HERTZ,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.PUMP_DELAY,
                             r'PumpDelay>([\d]+)</PumpDelay',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Pump Delay",
                             startup_param=True,
                             direct_access=False,
                             default_value=60,
                             units=ParameterUnit.SECOND,
                             visibility=ParameterDictVisibility.READ_WRITE)
        self._param_dict.add(Parameter.AUTO_RUN,
                             r'AutoRun>(.*)</AutoRun',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Auto Run",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.IGNORE_SWITCH,
                             r'IgnoreSwitch>(.*)</IgnoreSwitch',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Ignore Switch",
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)
