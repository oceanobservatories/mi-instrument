"""
@package mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver
@file marine-integrations/mi/instrument/seabird/sbe16plus_v2/ctdpf_jb/driver.py
@author Tapana Gupta
@brief Driver for the CTDPF-JB instrument
Release notes:

SBE Driver
"""

__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'

import re
import time

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
from mi.core.common import Units
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker

from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import SampleException

from xml.dom.minidom import parseString

from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType

from mi.instrument.seabird.sbe16plus_v2.driver import SBE16Protocol, SBE16InstrumentDriver, Sbe16plusBaseParticle, NEWLINE, \
    DEFAULT_ENCODER_KEY, TIMEOUT, WAKEUP_TIMEOUT, ScheduledJob, Command, ProtocolState, ProtocolEvent, \
    ConfirmedParameter, CommonParameter
from mi.instrument.seabird.sbe16plus_v2.driver import Prompt


# Driver constants
MIN_PUMP_DELAY = 0
MAX_PUMP_DELAY = 600
MIN_AVG_SAMPLES = 1
MAX_AVG_SAMPLES = 32767


class OptodeCommands(Command):
    SEND_OPTODE = 'sendOptode'


class SendOptodeCommand(BaseEnum):
    GET_ANALOG_OUTPUT = 'get analog output'
    GET_CALPHASE = 'get calphase'
    GET_ENABLE_TEMP = 'get enable temperature'
    GET_ENABLE_TEXT = 'get enable text'
    GET_ENABLE_HUM_COMP = 'get enable humiditycomp'
    GET_ENABLE_AIR_SAT = 'get enable airsaturation'
    GET_ENABLE_RAW_DATA = 'get enable rawdata'
    GET_INTERVAL = 'get interval'
    GET_MODE = 'get mode'


class Parameter(CommonParameter):
    """
    Device specific parameters for SBE19.
    """
    NUM_AVG_SAMPLES = "Navg"
    MIN_COND_FREQ = "MinCondFreq"
    PUMP_DELAY = "PumpDelay"
    AUTO_RUN = "AutoRun"
    IGNORE_SWITCH = "IgnoreSwitch"


###############################################################################
# Data Particles
###############################################################################
class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    CTD_PARSED = 'ctdpf_optode_sample'
    DEVICE_STATUS = 'ctdpf_optode_status'
    DEVICE_CALIBRATION = 'ctdpf_optode_calibration_coefficients'
    DEVICE_HARDWARE = 'ctdpf_optode_hardware'
    DEVICE_CONFIGURATION = 'ctdpf_optode_configuration'
    OPTODE_SETTINGS = 'ctdpf_optode_settings'


class SBE19ConfigurationParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"

    SCANS_TO_AVERAGE = "scans_to_average"
    MIN_COND_FREQ = "min_cond_freq"
    PUMP_DELAY = "pump_delay"
    AUTO_RUN = "auto_run"
    IGNORE_SWITCH = "ignore_switch"

    BATTERY_TYPE = "battery_type"
    BATTERY_CUTOFF = "battery_cutoff"

    EXT_VOLT_0 = "ext_volt_0"
    EXT_VOLT_1 = "ext_volt_1"
    EXT_VOLT_2 = "ext_volt_2"
    EXT_VOLT_3 = "ext_volt_3"
    EXT_VOLT_4 = "ext_volt_4"
    EXT_VOLT_5 = "ext_volt_5"
    SBE38 = "sbe38"
    SBE63 = "sbe63"
    WETLABS = "wetlabs"
    OPTODE = "optode"
    GAS_TENSION_DEVICE = "gas_tension_device"

    ECHO_CHARACTERS = "echo_characters"
    OUTPUT_EXECUTED_TAG = "output_executed_tag"
    OUTPUT_FORMAT = "output_format"


# noinspection PyPep8Naming,PyListCreation
class SBE19ConfigurationParticle(Sbe16plusBaseParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.DEVICE_CONFIGURATION

    @staticmethod
    def regex():
        pattern = r'(<ConfigurationData.*?</ConfigurationData>)' + NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(SBE19ConfigurationParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        pattern = r'(<ConfigurationData.*?</ConfigurationData>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        return re.compile(SBE19ConfigurationParticle.resp_regex(), re.DOTALL)

    def _map_param_to_xml_tag(self, parameter_name):
        map_param_to_tag = {SBE19ConfigurationParticleKey.SCANS_TO_AVERAGE: "ScansToAverage",
                            SBE19ConfigurationParticleKey.MIN_COND_FREQ: "MinimumCondFreq",
                            SBE19ConfigurationParticleKey.PUMP_DELAY: "PumpDelay",
                            SBE19ConfigurationParticleKey.AUTO_RUN: "AutoRun",
                            SBE19ConfigurationParticleKey.IGNORE_SWITCH: "IgnoreSwitch",

                            SBE19ConfigurationParticleKey.BATTERY_TYPE: "Type",
                            SBE19ConfigurationParticleKey.BATTERY_CUTOFF: "CutOff",

                            SBE19ConfigurationParticleKey.EXT_VOLT_0: "ExtVolt0",
                            SBE19ConfigurationParticleKey.EXT_VOLT_1: "ExtVolt1",
                            SBE19ConfigurationParticleKey.EXT_VOLT_2: "ExtVolt2",
                            SBE19ConfigurationParticleKey.EXT_VOLT_3: "ExtVolt3",
                            SBE19ConfigurationParticleKey.EXT_VOLT_4: "ExtVolt4",
                            SBE19ConfigurationParticleKey.EXT_VOLT_5: "ExtVolt5",
                            SBE19ConfigurationParticleKey.SBE38: "SBE38",
                            SBE19ConfigurationParticleKey.SBE63: "SBE63",
                            SBE19ConfigurationParticleKey.WETLABS: "WETLABS",
                            SBE19ConfigurationParticleKey.OPTODE: "OPTODE",
                            SBE19ConfigurationParticleKey.GAS_TENSION_DEVICE: "GTD",

                            SBE19ConfigurationParticleKey.ECHO_CHARACTERS: "EchoCharacters",
                            SBE19ConfigurationParticleKey.OUTPUT_EXECUTED_TAG: "OutputExecutedTag",
                            SBE19ConfigurationParticleKey.OUTPUT_FORMAT: "OutputFormat"}

        return map_param_to_tag[parameter_name]

    def _build_parsed_values(self):
        """
        Parse the output of the getCD command
        @throws SampleException If there is a problem with sample creation
        """

        SERIAL_NUMBER = "SerialNumber"
        PROFILE_MODE = "ProfileMode"
        BATTERY = "Battery"
        DATA_CHANNELS = "DataChannels"

        # check to make sure there is a correct match before continuing
        match = SBE19ConfigurationParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise SampleException("No regex match of parsed configuration data: [%s]" %
                                  self.raw_data)

        dom = parseString(self.raw_data)
        root = dom.documentElement
        log.debug("root.tagName = %s", root.tagName)
        serial_number = root.getAttribute(SERIAL_NUMBER)
        result = [{DataParticleKey.VALUE_ID: SBE19ConfigurationParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number}]

        result.append(self._get_xml_parameter(root, SBE19ConfigurationParticleKey.ECHO_CHARACTERS, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE19ConfigurationParticleKey.OUTPUT_EXECUTED_TAG, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE19ConfigurationParticleKey.OUTPUT_FORMAT, str))

        element = self._extract_xml_elements(root, PROFILE_MODE)[0]
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.SCANS_TO_AVERAGE, int))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.MIN_COND_FREQ, int))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.PUMP_DELAY, int))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.AUTO_RUN, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.IGNORE_SWITCH, self.yesno2bool))

        element = self._extract_xml_elements(root, BATTERY)[0]
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.BATTERY_TYPE, str))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.BATTERY_CUTOFF))

        element = self._extract_xml_elements(root, DATA_CHANNELS)[0]
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.EXT_VOLT_0, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.EXT_VOLT_1, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.EXT_VOLT_2, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.EXT_VOLT_3, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.EXT_VOLT_4, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.EXT_VOLT_5, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.SBE38, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.WETLABS, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.OPTODE, self.yesno2bool))
        result.append(self._get_xml_parameter(element, SBE19ConfigurationParticleKey.SBE63, self.yesno2bool))
        result.append(
            self._get_xml_parameter(element, SBE19ConfigurationParticleKey.GAS_TENSION_DEVICE, self.yesno2bool))

        return result


class SBE19StatusParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"

    DATE_TIME = "date_time_string"
    LOGGING_STATE = "logging_status"
    NUMBER_OF_EVENTS = "num_events"

    BATTERY_VOLTAGE_MAIN = "battery_voltage_main"
    BATTERY_VOLTAGE_LITHIUM = "battery_voltage_lithium"
    OPERATIONAL_CURRENT = "operational_current"
    PUMP_CURRENT = "pump_current"
    EXT_V01_CURRENT = "ext_v01_current"
    SERIAL_CURRENT = "serial_current"

    MEMORY_FREE = "mem_free"
    NUMBER_OF_SAMPLES = "num_samples"
    SAMPLES_FREE = "samples_free"
    SAMPLE_LENGTH = "sample_length"
    PROFILES = "profiles"


# noinspection PyPep8Naming
class SBE19StatusParticle(Sbe16plusBaseParticle):
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
        return re.compile(SBE19StatusParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        pattern = r'(<StatusData.*?</StatusData>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        return re.compile(SBE19StatusParticle.resp_regex(), re.DOTALL)

    def _map_param_to_xml_tag(self, parameter_name):
        map_param_to_tag = {SBE19StatusParticleKey.BATTERY_VOLTAGE_MAIN: "vMain",
                            SBE19StatusParticleKey.BATTERY_VOLTAGE_LITHIUM: "vLith",
                            SBE19StatusParticleKey.OPERATIONAL_CURRENT: "iMain",
                            SBE19StatusParticleKey.PUMP_CURRENT: "iPump",
                            SBE19StatusParticleKey.EXT_V01_CURRENT: "iExt01",
                            SBE19StatusParticleKey.SERIAL_CURRENT: "iSerial",

                            SBE19StatusParticleKey.MEMORY_FREE: "Bytes",
                            SBE19StatusParticleKey.NUMBER_OF_SAMPLES: "Samples",
                            SBE19StatusParticleKey.SAMPLES_FREE: "SamplesFree",
                            SBE19StatusParticleKey.SAMPLE_LENGTH: "SampleLength",
                            SBE19StatusParticleKey.PROFILES: "Profiles"}

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
        match = SBE19StatusParticle.regex_compiled().match(self.raw_data)
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
        result = [{DataParticleKey.VALUE_ID: SBE19StatusParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number},
                  {DataParticleKey.VALUE_ID: SBE19StatusParticleKey.DATE_TIME,
                   DataParticleKey.VALUE: date_time},
                  {DataParticleKey.VALUE_ID: SBE19StatusParticleKey.LOGGING_STATE,
                   DataParticleKey.VALUE: logging_status},
                  {DataParticleKey.VALUE_ID: SBE19StatusParticleKey.NUMBER_OF_EVENTS,
                   DataParticleKey.VALUE: number_of_events}]

        element = self._extract_xml_elements(root, POWER)[0]
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.BATTERY_VOLTAGE_MAIN))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.BATTERY_VOLTAGE_LITHIUM))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.OPERATIONAL_CURRENT))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.PUMP_CURRENT))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.EXT_V01_CURRENT))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.SERIAL_CURRENT))

        element = self._extract_xml_elements(root, MEMORY_SUMMARY)[0]
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.MEMORY_FREE, int))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.NUMBER_OF_SAMPLES, int))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.SAMPLES_FREE, int))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.SAMPLE_LENGTH, int))
        result.append(self._get_xml_parameter(element, SBE19StatusParticleKey.PROFILES, int))

        return result


class SBE19HardwareParticleKey(BaseEnum):
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
    VOLT1_TYPE = 'volt1_type'
    VOLT1_SERIAL_NUMBER = 'volt1_serial_number'


# noinspection PyPep8Naming
class SBE19HardwareParticle(Sbe16plusBaseParticle):
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
        return re.compile(SBE19HardwareParticle.regex(), re.DOTALL)

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
        return re.compile(SBE19HardwareParticle.resp_regex(), re.DOTALL)

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
        VOLT1 = "volt 1"

        # check to make sure there is a correct match before continuing
        match = SBE19HardwareParticle.regex_compiled().match(self.raw_data)
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
        volt1_serial_number = 0
        volt1_type = ""

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
            elif sensor_id == VOLT1:
                volt1_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
                volt1_type = self._extract_xml_element_value(sensor, TYPE)

        result = [{DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.FIRMWARE_VERSION,
                   DataParticleKey.VALUE: firmware_version},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.FIRMWARE_DATE,
                   DataParticleKey.VALUE: firmware_date},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.COMMAND_SET_VERSION,
                   DataParticleKey.VALUE: command_set_version},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.MANUFACTURE_DATE,
                   DataParticleKey.VALUE: manufacture_date},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.PCB_SERIAL_NUMBER,
                   DataParticleKey.VALUE: pcb_serial_number},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.ASSEMBLY_NUMBER,
                   DataParticleKey.VALUE: pcb_assembly},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.TEMPERATURE_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: temperature_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.CONDUCTIVITY_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: conductivity_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.PRESSURE_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: pressure_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.PRESSURE_SENSOR_TYPE,
                   DataParticleKey.VALUE: pressure_sensor_type},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.VOLT0_SERIAL_NUMBER,
                   DataParticleKey.VALUE: volt0_serial_number},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.VOLT0_TYPE,
                   DataParticleKey.VALUE: volt0_type},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.VOLT1_SERIAL_NUMBER,
                   DataParticleKey.VALUE: volt1_serial_number},
                  {DataParticleKey.VALUE_ID: SBE19HardwareParticleKey.VOLT1_TYPE,
                   DataParticleKey.VALUE: volt1_type}]

        return result


class SBE19CalibrationParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"

    TEMP_SENSOR_SERIAL_NUMBER = "temp_sensor_serial_number"
    TEMP_CAL_DATE = "calibration_date_temperature"
    TA0 = "temp_coeff_ta0"
    TA1 = "temp_coeff_ta1"
    TA2 = "temp_coeff_ta2"
    TA3 = "temp_coeff_ta3"
    TOFFSET = "temp_coeff_offset"

    COND_SENSOR_SERIAL_NUMBER = "cond_sensor_serial_number"
    COND_CAL_DATE = "calibration_date_conductivity"
    CONDG = "cond_coeff_cg"
    CONDH = "cond_coeff_ch"
    CONDI = "cond_coeff_ci"
    CONDJ = "cond_coeff_cj"
    CPCOR = "cond_coeff_cpcor"
    CTCOR = "cond_coeff_ctcor"
    CSLOPE = "cond_coeff_cslope"

    PRES_SERIAL_NUMBER = "pressure_sensor_serial_number"
    PRES_CAL_DATE = "calibration_date_pressure"
    PA0 = "press_coeff_pa0"
    PA1 = "press_coeff_pa1"
    PA2 = "press_coeff_pa2"
    PTCA0 = "press_coeff_ptca0"
    PTCA1 = "press_coeff_ptca1"
    PTCA2 = "press_coeff_ptca2"
    PTCB0 = "press_coeff_ptcb0"
    PTCB1 = "press_coeff_ptcb1"
    PTCB2 = "press_coeff_ptcb2"
    PTEMPA0 = "press_coeff_ptempa0"
    PTEMPA1 = "press_coeff_ptempa1"
    PTEMPA2 = "press_coeff_ptempa2"
    POFFSET = "press_coeff_poffset"
    PRES_RANGE = "pressure_sensor_range"

    EXT_VOLT0_OFFSET = "ext_volt0_offset"
    EXT_VOLT0_SLOPE = "ext_volt0_slope"
    EXT_VOLT1_OFFSET = "ext_volt1_offset"
    EXT_VOLT1_SLOPE = "ext_volt1_slope"
    EXT_VOLT2_OFFSET = "ext_volt2_offset"
    EXT_VOLT2_SLOPE = "ext_volt2_slope"
    EXT_VOLT3_OFFSET = "ext_volt3_offset"
    EXT_VOLT3_SLOPE = "ext_volt3_slope"
    EXT_VOLT4_OFFSET = "ext_volt4_offset"
    EXT_VOLT4_SLOPE = "ext_volt4_slope"
    EXT_VOLT5_OFFSET = "ext_volt5_offset"
    EXT_VOLT5_SLOPE = "ext_volt5_slope"

    EXT_FREQ = "ext_freq_sf"


# noinspection PyPep8Naming,PyPep8Naming
class SBE19CalibrationParticle(Sbe16plusBaseParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.DEVICE_CALIBRATION

    @staticmethod
    def regex():
        pattern = r'(<CalibrationCoefficients.*?</CalibrationCoefficients>)' + NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(SBE19CalibrationParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        pattern = r'(<CalibrationCoefficients.*?</CalibrationCoefficients>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        return re.compile(SBE19CalibrationParticle.resp_regex(), re.DOTALL)

    def _map_param_to_xml_tag(self, parameter_name):
        map_param_to_tag = {SBE19CalibrationParticleKey.TEMP_SENSOR_SERIAL_NUMBER: "SerialNum",
                            SBE19CalibrationParticleKey.TEMP_CAL_DATE: "CalDate",
                            SBE19CalibrationParticleKey.TA0: "TA0",
                            SBE19CalibrationParticleKey.TA1: "TA1",
                            SBE19CalibrationParticleKey.TA2: "TA2",
                            SBE19CalibrationParticleKey.TA3: "TA3",
                            SBE19CalibrationParticleKey.TOFFSET: "TOFFSET",

                            SBE19CalibrationParticleKey.COND_SENSOR_SERIAL_NUMBER: "SerialNum",
                            SBE19CalibrationParticleKey.COND_CAL_DATE: "CalDate",
                            SBE19CalibrationParticleKey.CONDG: "G",
                            SBE19CalibrationParticleKey.CONDH: "H",
                            SBE19CalibrationParticleKey.CONDI: "I",
                            SBE19CalibrationParticleKey.CONDJ: "J",
                            SBE19CalibrationParticleKey.CPCOR: "CPCOR",
                            SBE19CalibrationParticleKey.CTCOR: "CTCOR",
                            SBE19CalibrationParticleKey.CSLOPE: "CSLOPE",

                            SBE19CalibrationParticleKey.PRES_SERIAL_NUMBER: "SerialNum",
                            SBE19CalibrationParticleKey.PRES_CAL_DATE: "CalDate",
                            SBE19CalibrationParticleKey.PA0: "PA0",
                            SBE19CalibrationParticleKey.PA1: "PA1",
                            SBE19CalibrationParticleKey.PA2: "PA2",
                            SBE19CalibrationParticleKey.PTCA0: "PTCA0",
                            SBE19CalibrationParticleKey.PTCA1: "PTCA1",
                            SBE19CalibrationParticleKey.PTCA2: "PTCA2",
                            SBE19CalibrationParticleKey.PTCB0: "PTCB0",
                            SBE19CalibrationParticleKey.PTCB1: "PTCB1",
                            SBE19CalibrationParticleKey.PTCB2: "PTCB2",
                            SBE19CalibrationParticleKey.PTEMPA0: "PTEMPA0",
                            SBE19CalibrationParticleKey.PTEMPA1: "PTEMPA1",
                            SBE19CalibrationParticleKey.PTEMPA2: "PTEMPA2",
                            SBE19CalibrationParticleKey.POFFSET: "POFFSET",
                            SBE19CalibrationParticleKey.PRES_RANGE: "PRANGE",

                            SBE19CalibrationParticleKey.EXT_VOLT0_OFFSET: "OFFSET",
                            SBE19CalibrationParticleKey.EXT_VOLT0_SLOPE: "SLOPE",
                            SBE19CalibrationParticleKey.EXT_VOLT1_OFFSET: "OFFSET",
                            SBE19CalibrationParticleKey.EXT_VOLT1_SLOPE: "SLOPE",
                            SBE19CalibrationParticleKey.EXT_VOLT2_OFFSET: "OFFSET",
                            SBE19CalibrationParticleKey.EXT_VOLT2_SLOPE: "SLOPE",
                            SBE19CalibrationParticleKey.EXT_VOLT3_OFFSET: "OFFSET",
                            SBE19CalibrationParticleKey.EXT_VOLT3_SLOPE: "SLOPE",
                            SBE19CalibrationParticleKey.EXT_VOLT4_OFFSET: "OFFSET",
                            SBE19CalibrationParticleKey.EXT_VOLT4_SLOPE: "SLOPE",
                            SBE19CalibrationParticleKey.EXT_VOLT5_OFFSET: "OFFSET",
                            SBE19CalibrationParticleKey.EXT_VOLT5_SLOPE: "SLOPE",

                            SBE19CalibrationParticleKey.EXT_FREQ: "EXTFREQSF"}

        return map_param_to_tag[parameter_name]

    def _build_parsed_values(self):
        """
        Parse the output of the getCC command
        @throws SampleException If there is a problem with sample creation
        """

        SERIAL_NUMBER = "SerialNumber"
        CALIBRATION = "Calibration"
        ID = "id"
        TEMPERATURE_SENSOR_ID = "Main Temperature"
        CONDUCTIVITY_SENSOR_ID = "Main Conductivity"
        PRESSURE_SENSOR_ID = "Main Pressure"
        VOLT0 = "Volt 0"
        VOLT1 = "Volt 1"
        VOLT2 = "Volt 2"
        VOLT3 = "Volt 3"
        VOLT4 = "Volt 4"
        VOLT5 = "Volt 5"
        EXTERNAL_FREQUENCY_CHANNEL = "external frequency channel"

        # check to make sure there is a correct match before continuing
        match = SBE19CalibrationParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise SampleException("No regex match of parsed calibration data: [%s]" %
                                  self.raw_data)

        dom = parseString(self.raw_data)
        root = dom.documentElement
        log.debug("root.tagName = %s", root.tagName)
        serial_number = root.getAttribute(SERIAL_NUMBER)
        result = [{DataParticleKey.VALUE_ID: SBE19CalibrationParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number}]

        calibration_elements = self._extract_xml_elements(root, CALIBRATION)
        for calibration in calibration_elements:
            id_attr = calibration.getAttribute(ID)
            if id_attr == TEMPERATURE_SENSOR_ID:
                result.append(
                    self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.TEMP_SENSOR_SERIAL_NUMBER, str))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.TEMP_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.TA0))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.TA1))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.TA2))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.TA3))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.TOFFSET))
            elif id_attr == CONDUCTIVITY_SENSOR_ID:
                result.append(
                    self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.COND_SENSOR_SERIAL_NUMBER, str))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.COND_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.CONDG))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.CONDH))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.CONDI))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.CONDJ))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.CPCOR))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.CTCOR))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.CSLOPE))
            elif id_attr == PRESSURE_SENSOR_ID:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PRES_SERIAL_NUMBER, str))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PRES_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PA0))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PA1))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PA2))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTCA0))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTCA1))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTCA2))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTCB0))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTCB1))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTCB2))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTEMPA0))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTEMPA1))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PTEMPA2))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.POFFSET))
                result.append(
                    self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.PRES_RANGE, self.float_to_int))
            elif id_attr == VOLT0:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT0_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT0_SLOPE))
            elif id_attr == VOLT1:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT1_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT1_SLOPE))
            elif id_attr == VOLT2:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT2_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT2_SLOPE))
            elif id_attr == VOLT3:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT3_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT3_SLOPE))
            elif id_attr == VOLT4:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT4_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT4_SLOPE))
            elif id_attr == VOLT5:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT5_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_VOLT5_SLOPE))
            elif id_attr == EXTERNAL_FREQUENCY_CHANNEL:
                result.append(self._get_xml_parameter(calibration, SBE19CalibrationParticleKey.EXT_FREQ))

        return result


class SBE19DataParticleKey(BaseEnum):
    TEMP = "temperature"
    CONDUCTIVITY = "conductivity"
    PRESSURE = "pressure"
    PRESSURE_TEMP = "pressure_temp"
    VOLT0 = "oxy_calphase"
    VOLT1 = "oxy_temp"
    OXYGEN = "oxygen"


class SBE19DataParticle(Sbe16plusBaseParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.

    Sample:
       #04570F0A1E910828FC47BC59F199952C64C9

    Format:
       #ttttttccccccppppppvvvvvvvvvvvvoooooo

       Temperature = tttttt
       Conductivity = cccccc
       quartz pressure = pppppp
       quartz pressure temperature compensation = vvvv
       First external voltage = vvvv
       Second external voltage = vvvv
       Oxygen = oooooo
    """
    _data_particle_type = DataParticleType.CTD_PARSED

    @staticmethod
    def regex():
        """
        Regular expression to match a sample pattern
        @return: regex string
        """
        #ttttttccccccppppppvvvvvvvvvvvvoooooo
        pattern = r'#? *'  # patter may or may not start with a '
        pattern += r'([0-9A-F]{6})'  # temperature
        pattern += r'([0-9A-F]{6})'  # conductivity
        pattern += r'([0-9A-F]{6})'  # pressure
        pattern += r'([0-9A-F]{4})'  # pressure temp
        pattern += r'([0-9A-F]{4})'  # volt0
        pattern += r'([0-9A-F]{4})'  # volt1
        pattern += r'([0-9A-F]{6})'  # oxygen
        pattern += NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(SBE19DataParticle.regex())

    def _build_parsed_values(self):
        """
        Take something in the autosample/TS format and split it into
        the 7 measurements as stated above

        @throws SampleException If there is a problem with sample creation
        """
        match = SBE19DataParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" %
                                  self.raw_data)

        try:
            temperature = self.hex2value(match.group(1))
            conductivity = self.hex2value(match.group(2))
            pressure = self.hex2value(match.group(3))
            pressure_temp = self.hex2value(match.group(4))
            volt0 = self.hex2value(match.group(5))
            volt1 = self.hex2value(match.group(6))
            oxygen = self.hex2value(match.group(7))

        except ValueError:
            raise SampleException("ValueError while converting data: [%s]" %
                                  self.raw_data)

        result = [{DataParticleKey.VALUE_ID: SBE19DataParticleKey.TEMP,
                   DataParticleKey.VALUE: temperature},
                  {DataParticleKey.VALUE_ID: SBE19DataParticleKey.CONDUCTIVITY,
                   DataParticleKey.VALUE: conductivity},
                  {DataParticleKey.VALUE_ID: SBE19DataParticleKey.PRESSURE,
                   DataParticleKey.VALUE: pressure},
                  {DataParticleKey.VALUE_ID: SBE19DataParticleKey.PRESSURE_TEMP,
                   DataParticleKey.VALUE: pressure_temp},
                  {DataParticleKey.VALUE_ID: SBE19DataParticleKey.VOLT0,
                   DataParticleKey.VALUE: volt0},
                  {DataParticleKey.VALUE_ID: SBE19DataParticleKey.VOLT1,
                   DataParticleKey.VALUE: volt1},
                  {DataParticleKey.VALUE_ID: SBE19DataParticleKey.OXYGEN,
                   DataParticleKey.VALUE: oxygen}]

        return result


class OptodeSettingsParticleKey(BaseEnum):
    CALPHASE = 'calphase'
    ENABLE_TEMP = 'enable_temperature'
    ENABLE_TEXT = 'enable_text'
    ENABLE_HUM_COMP = 'enable_humiditycomp'
    ENABLE_AIR_SAT = 'enable_airsaturation'
    ENABLE_RAW_DATA = 'enable_rawdata'
    ANALOG_OUTPUT = 'analog_output'
    INTERVAL = 'interval'
    MODE = 'mode'


class OptodeSettingsParticle(Sbe16plusBaseParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.OPTODE_SETTINGS

    @staticmethod
    def regex():
        # pattern for the first sendoptode command
        pattern = r'Optode RX = Analog Output'
        pattern += r'.*?'  # non-greedy match of all the junk between
        pattern += r'Optode RX = Mode[\s]*[\d]+[\s]+[\d]+[\s]+([\w\- \t]+)' + NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(OptodeSettingsParticle.regex(), re.DOTALL)

    def encoders(self):
        return {
            DEFAULT_ENCODER_KEY: str,

            OptodeSettingsParticleKey.CALPHASE: float,
            OptodeSettingsParticleKey.ENABLE_TEMP: self.yesno2bool,
            OptodeSettingsParticleKey.ENABLE_TEXT: self.yesno2bool,
            OptodeSettingsParticleKey.ENABLE_HUM_COMP: self.yesno2bool,
            OptodeSettingsParticleKey.ENABLE_AIR_SAT: self.yesno2bool,
            OptodeSettingsParticleKey.ENABLE_RAW_DATA: self.yesno2bool,
            OptodeSettingsParticleKey.INTERVAL: float}

    # noinspection PyPep8
    def regex_multiline(self):
        return {
            OptodeSettingsParticleKey.ANALOG_OUTPUT: r'Optode RX = Analog Output[\s]*[\d]+[\s]+[\d]+[\s]+([\w]+)',
            OptodeSettingsParticleKey.CALPHASE: r'Optode RX = CalPhase\[Deg][\s]+[\d]+[\s]+[\d]+[\s]+(\d+.\d+)',
            OptodeSettingsParticleKey.ENABLE_TEMP: r'Optode RX = Enable Temperature[\s]+[\d]+[\s]+[\d]+[\s]+(Yes|No)',
            OptodeSettingsParticleKey.ENABLE_TEXT: r'Optode RX = Enable Text[\s]*[\d]+[\s]+[\d]+[\s]+(Yes|No)',
            OptodeSettingsParticleKey.ENABLE_HUM_COMP: r'Optode RX = Enable HumidityComp[\s]*[\d]+[\s]+[\d]+[\s]+(Yes|No)',
            OptodeSettingsParticleKey.ENABLE_AIR_SAT: r'Optode RX = Enable AirSaturation[\s]*[\d]+[\s]+[\d]+[\s]+(Yes|No)',
            OptodeSettingsParticleKey.ENABLE_RAW_DATA: r'Optode RX = Enable Rawdata[\s]*[\d]+[\s]+[\d]+[\s]+(Yes|No)',
            OptodeSettingsParticleKey.INTERVAL: r'Optode RX = Interval[\s]+[\d]+[\s]+[\d]+[\s]+(\d+.\d+)',
            OptodeSettingsParticleKey.MODE: r'Optode RX = Mode[\s]*[\d]+[\s]+[\d]+[\s]+([\w\- \t]+)'}

    def _build_parsed_values(self):
        """
        Parse the output of the SendOptode commands

        @throws SampleException If there is a problem with sample creation
        """
        match = OptodeSettingsParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed optode data: [%s]" %
                                  self.raw_data)

        try:
            return self._get_multiline_values()
        except ValueError as e:
            raise SampleException("ValueError while decoding optode output: [%s]" % e)


###############################################################################
# Driver
###############################################################################
class InstrumentDriver(SBE16InstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################

    # noinspection PyMethodMayBeStatic
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
        self._protocol = SBE19Protocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################
class SBE19Protocol(SBE16Protocol):
    """
    Instrument protocol class for SBE19 Driver
    Subclasses SBE16Protocol
    """

    def __init__(self, prompts, newline, driver_event):
        """
        SBE19Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The SBE19 newline.
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
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ],
            ProtocolState.COMMAND: [
                (ProtocolEvent.ENTER, self._handler_command_enter),
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.ACQUIRE_SAMPLE, self._handler_command_acquire_sample),
                (ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.SET, self._handler_command_set),
                (ProtocolEvent.START_DIRECT, self._handler_command_start_direct),
                (ProtocolEvent.CLOCK_SYNC, self._handler_command_clock_sync_clock),
                (ProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status),
                (ProtocolEvent.SCHEDULED_ACQUIRED_STATUS, self._handler_autosample_acquire_status),
                (ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_command_clock_sync_clock)
            ],
            ProtocolState.DIRECT_ACCESS: [
                (ProtocolEvent.ENTER, self._handler_direct_access_enter),
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct),
                (ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct)
            ],
            ProtocolState.AUTOSAMPLE: [
                (ProtocolEvent.ENTER, self._handler_autosample_enter),
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample),
                (ProtocolEvent.SCHEDULED_ACQUIRED_STATUS, self._handler_autosample_acquire_status),
                (ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_autosample_clock_sync)
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

        # Add build handlers for device commands.
        self._add_build_handler(Command.GET_CD, self._build_simple_command)
        self._add_build_handler(Command.GET_SD, self._build_simple_command)
        self._add_build_handler(Command.GET_CC, self._build_simple_command)
        self._add_build_handler(Command.GET_EC, self._build_simple_command)
        self._add_build_handler(Command.RESET_EC, self._build_simple_command)
        self._add_build_handler(Command.GET_HD, self._build_simple_command)

        self._add_build_handler(Command.STARTNOW, self._build_simple_command)
        self._add_build_handler(Command.STOP, self._build_simple_command)
        self._add_build_handler(Command.TS, self._build_simple_command)
        self._add_build_handler(Command.SET, self._build_set_command)
        self._add_build_handler(OptodeCommands.SEND_OPTODE, self._build_send_optode_command)

        # Add response handlers for device commands.
        # these are here to ensure that correct responses to the commands are received before the next command is sent
        self._add_response_handler(Command.SET, self._parse_set_response)
        self._add_response_handler(Command.GET_SD, self._validate_GetSD_response)
        self._add_response_handler(Command.GET_HD, self._validate_GetHD_response)
        self._add_response_handler(Command.GET_CD, self._validate_GetCD_response)
        self._add_response_handler(Command.GET_CC, self._validate_GetCC_response)
        self._add_response_handler(Command.GET_EC, self._validate_GetEC_response)
        self._add_response_handler(OptodeCommands.SEND_OPTODE, self._validate_SendOptode_response)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        self._chunker = StringChunker(self.sieve_function)

        #Setup schedulable commands
        self._add_scheduler_event(ScheduledJob.ACQUIRE_STATUS, ProtocolEvent.ACQUIRE_STATUS)
        self._add_scheduler_event(ScheduledJob.CLOCK_SYNC, ProtocolEvent.SCHEDULED_CLOCK_SYNC)

    @staticmethod
    def sieve_function(raw_data):
        """ The method that splits samples
        Over-ride sieve function to handle additional particles.
        """
        matchers = []
        return_list = []

        matchers.append(SBE19DataParticle.regex_compiled())
        matchers.append(SBE19HardwareParticle.regex_compiled())
        matchers.append(SBE19CalibrationParticle.regex_compiled())
        matchers.append(SBE19StatusParticle.regex_compiled())
        matchers.append(SBE19ConfigurationParticle.regex_compiled())
        matchers.append(OptodeSettingsParticle.regex_compiled())

        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)
        update_params = False

        # check values that the instrument doesn't validate
        # handle special cases for driver specific parameters
        for (key, val) in params.iteritems():
            if key == Parameter.PUMP_DELAY and (val < MIN_PUMP_DELAY or val > MAX_PUMP_DELAY):
                raise InstrumentParameterException("pump delay out of range")
            elif key == Parameter.NUM_AVG_SAMPLES and (val < MIN_AVG_SAMPLES or val > MAX_AVG_SAMPLES):
                raise InstrumentParameterException("num average samples out of range")

        for (key, val) in params.iteritems():

            old_val = self._param_dict.format(key)
            new_val = self._param_dict.format(key, val)
            log.debug("KEY = %r OLD VALUE = %r NEW VALUE = %r", key, old_val, new_val)

            if old_val != new_val:
                update_params = True
                if ConfirmedParameter.has(key):
                    # We add a write delay here because this command has to be sent
                    # twice, the write delay allows it to process the first command
                    # before it receives the beginning of the second.
                    self._do_cmd_resp(Command.SET, key, val, write_delay=0.2)
                else:
                    self._do_cmd_resp(Command.SET, key, val, **kwargs)

        log.debug("set complete, update params")
        self._update_params()
        if update_params:
            self._update_params()

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        result = []

        result.append(self._do_cmd_resp(Command.GET_SD, response_regex=SBE19StatusParticle.regex_compiled(),
                                   timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, response_regex=SBE19HardwareParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, response_regex=SBE19ConfigurationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, response_regex=SBE19CalibrationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetEC Response: %s", result)

        #Reset the event counter right after getEC
        self._do_cmd_resp(Command.RESET_EC, timeout=TIMEOUT)

        #Now send commands to the Optode to get its status
        #Stop the optode first, need to send the command twice
        stop_command = "stop"
        start_command = "start"
        self._do_cmd_resp(OptodeCommands.SEND_OPTODE, stop_command, timeout=TIMEOUT)
        time.sleep(2)
        self._do_cmd_resp(OptodeCommands.SEND_OPTODE, stop_command, timeout=TIMEOUT)
        time.sleep(3)

        #Send all the 'sendoptode=' commands one by one
        optode_commands = SendOptodeCommand.list()
        for command in optode_commands:
            log.debug("Sending optode command: %s" % command)
            result.append(self._do_cmd_resp(OptodeCommands.SEND_OPTODE, command, timeout=TIMEOUT))
            log.debug("_handler_command_acquire_status: SendOptode Response: %s", result)

        #restart the optode
        self._do_cmd_resp(OptodeCommands.SEND_OPTODE, start_command, timeout=TIMEOUT)

        return None, (None, ''.join(result))

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        result = []

        # When in autosample this command requires two wakeups to get to the right prompt
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)

        result.append(self._do_cmd_resp(Command.GET_SD, response_regex=SBE19StatusParticle.regex_compiled(),
                                   timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, response_regex=SBE19HardwareParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, response_regex=SBE19ConfigurationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, response_regex=SBE19CalibrationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetEC Response: %s", result)

        #Reset the event counter right after getEC
        self._do_cmd_no_resp(Command.RESET_EC)

        return None, (None, ''.join(result))

    def _build_send_optode_command(self, cmd, command):
        """
        Build handler for sendoptode command.
        @param cmd The command to build.
        @param command The optode command.
        @ retval The set command to be sent to the device.

        """
        return "%s=%s%s" % (cmd, command, self._newline)

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

        if not SBE19StatusParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetSD_response: GetSD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetSD command not recognized: %s.' % response)

        self._param_dict.update_many(response)

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

        if not SBE19HardwareParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetHD_response: GetHD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetHD command not recognized: %s.' % response)

        self._param_dict.update_many(response)

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

        if not SBE19ConfigurationParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetCD_response: GetCD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetCD command not recognized: %s.' % response)

        self._param_dict.update_many(response)

        return response

    def _validate_GetCC_response(self, response, prompt):
        """
        validation handler for GetCC command
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("GetCC command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('GetCC command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if not SBE19CalibrationParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetCC_response: GetCC command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetCC command not recognized: %s.' % response)

        return response

    def _validate_GetEC_response(self, response, prompt):
        """
        validation handler for GetEC command
        @param response command response string.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("GetEC command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('GetEC command failure: type="%s" msg="%s"' % (error[0], error[1]))

        return response

    def _validate_SendOptode_response(self, response, prompt):
        """
        validation handler for GetEC command
        @param response command response string.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("SendOptode command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('Send Optode command failure: type="%s" msg="%s"' % (error[0], error[1]))

        return response

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with SBE19 parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        self._build_common_param_dict()

        self._param_dict.add(Parameter.NUM_AVG_SAMPLES,
                             r'ScansToAverage>([\d]+)</ScansToAverage>',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Scans to Average",
                             description="Number of samples to average",
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
                             description="Minimum conductivity frequency to enable pump turn-on.",
                             startup_param=True,
                             direct_access=False,
                             default_value=500,
                             units=Units.HERTZ,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.PUMP_DELAY,
                             r'PumpDelay>([\d]+)</PumpDelay',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Pump Delay",
                             description="Time to wait after minimum conductivity frequency is reached before turning pump on.",
                             startup_param=True,
                             direct_access=False,
                             default_value=60,
                             units=Units.SECOND,
                             visibility=ParameterDictVisibility.READ_WRITE)
        self._param_dict.add(Parameter.AUTO_RUN,
                             r'AutoRun>(.*)</AutoRun',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Auto Run",
                             description="Enable automatic logging when power is applied: (true | false).",
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
                             description="Disable magnetic switch position for starting or stopping logging: (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.OPTODE,
                             r'OPTODE>(.*)</OPTODE',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Optode Attached",
                             description="Enable optode: (true | false)",
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
                             description="Enable external voltage 1: (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)

        self._build_ctd_specific_params()

    def _build_ctd_specific_params(self):
        self._param_dict.add(Parameter.PTYPE,
                             r"<Sensor id = 'Main Pressure'>.*?<type>(.*?)</type>.*?</Sensor>",
                             self._pressure_sensor_to_int,
                             str,
                             type=ParameterDictType.INT,
                             display_name="Pressure Sensor Type",
                             startup_param=True,
                             direct_access=True,
                             default_value=1,
                             description="Sensor type: (1:strain gauge | 3:quartz with temp comp)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             regex_flags=re.DOTALL)

    def _got_chunk(self, chunk, timestamp):
        """
        Over-ride sieve function to handle additional particles.
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if self._extract_sample(SBE19DataParticle, SBE19DataParticle.regex_compiled(), chunk, timestamp):
            self._sampling = True
            return

        for particle_class in SBE19HardwareParticle, \
                              SBE19CalibrationParticle, \
                              SBE19ConfigurationParticle, \
                              SBE19StatusParticle, \
                              OptodeSettingsParticle:
            if self._extract_sample(particle_class, particle_class.regex_compiled(), chunk, timestamp):
                return
