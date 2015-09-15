"""
@package mi.instrument.seabird.sbe26plus_v2.driver
@file mi/instrument/seabird/sbe16plus_v2/driver.py
@author David Everett 
@brief Driver base class for sbe16plus V2 CTD instrument.
"""


__author__ = 'David Everett'
__license__ = 'Apache 2.0'

import time
import re


from mi.core.log import get_logger, get_logging_metaclass
log = get_logger()

from mi.core.common import BaseEnum, Units
from mi.core.util import dict_equal
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverEvent

from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import NotImplementedException
from mi.core.exceptions import SampleException
from xml.dom.minidom import parseString
from mi.core.time_tools import get_timestamp_delayed

WAKEUP_TIMEOUT = 3
NEWLINE = '\r\n'
SBE_EPOCH = 946713600  # Unix time for SBE epoch 2000-01-01 00:00:00
TIMEOUT = 20
DEFAULT_ENCODER_KEY = '__default__'

ERROR_PATTERN = r"<ERROR type='(.*?)' msg='(.*?)'\/>"
ERROR_REGEX   = re.compile(ERROR_PATTERN, re.DOTALL)


class ScheduledJob(BaseEnum):
    ACQUIRE_STATUS = 'acquire_status'
    CLOCK_SYNC = 'clock_sync'


class Command(BaseEnum):
    GET_CD = 'GetCD'
    GET_SD = 'GetSD'
    GET_CC = 'GetCC'
    GET_EC = 'GetEC'
    RESET_EC = 'ResetEC'
    GET_HD = 'GetHD'
    #DS  = 'ds' #Superceded by GetCD and GetSD, do not use!
    #DCAL = 'dcal' #Superceded by GetCC, do not use!
    TS = 'ts'
    STARTNOW = 'StartNow'
    STOP = 'Stop'
    SET = 'set'


class ProtocolState(BaseEnum):
    """
    Protocol states for SBE16. Cherry picked from DriverProtocolState
    enum.
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class ProtocolEvent(BaseEnum):
    """
    Protocol events for SBE16. Cherry picked from DriverEvent enum.
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    SCHEDULED_CLOCK_SYNC = DriverEvent.SCHEDULED_CLOCK_SYNC
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    SCHEDULED_ACQUIRED_STATUS = 'PROTOCOL_EVENT_SCHEDULED_ACQUIRE_STATUS'


class Capability(BaseEnum):
    """
    Capabilities that are exposed to the user (subset of above)
    """
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    DISCOVER = DriverEvent.DISCOVER


class CommonParameter(DriverParameter):
        DATE_TIME = "DateTime"
        PTYPE = "PType"
        VOLT0 = "Volt0"
        VOLT1 = "Volt1"
        VOLT2 = "Volt2"
        VOLT3 = "Volt3"
        VOLT4 = "Volt4"
        VOLT5 = "Volt5"
        SBE38 = "SBE38"
        SBE63 = "SBE63"
        WETLABS = "WetLabs"
        GTD = "GTD"
        DUAL_GTD = "DualGTD"
        OPTODE = "OPTODE"
        OUTPUT_FORMAT = "OutputFormat"
        LOGGING = "logging"


class Parameter(CommonParameter):
    """
    Device parameters for SBE16.
    """
    INTERVAL = 'SampleInterval'
    TXREALTIME = 'TXREALTIME'
    ECHO = "echo"
    OUTPUT_EXEC_TAG = 'OutputExecutedTag'
    PUMP_MODE = "PumpMode"
    NCYCLES = "NCycles"
    BIOWIPER = "Biowiper"
    DELAY_BEFORE_SAMPLE = "DelayBeforeSampling"
    DELAY_AFTER_SAMPLE = "DelayAfterSampling"
    SBE50 = "SBE50"
    SYNCMODE = "SyncMode"
    SYNCWAIT = "SyncWait"


class ConfirmedParameter(BaseEnum):
    """
    List of all parameters that require confirmation
    i.e. set sent twice to confirm.
    """
    PTYPE    = Parameter.PTYPE
    SBE63    = Parameter.SBE63
    SBE38    = Parameter.SBE38
    SBE50    = Parameter.SBE50
    GTD      = Parameter.GTD
    DUAL_GTD = Parameter.DUAL_GTD
    OPTODE   = Parameter.OPTODE
    WETLABS  = Parameter.WETLABS
    VOLT0    = Parameter.VOLT0
    VOLT1    = Parameter.VOLT1
    VOLT2    = Parameter.VOLT2
    VOLT3    = Parameter.VOLT3
    VOLT4    = Parameter.VOLT4
    VOLT5    = Parameter.VOLT5


# Device prompts.
class Prompt(BaseEnum):
    """
    SBE16 io prompts.
    """
    COMMAND = NEWLINE + 'S>'
    BAD_COMMAND = '?cmd S>'
    AUTOSAMPLE =  NEWLINE + 'S>'
    EXECUTED = '<Executed/>'


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    CTD_PARSED = 'ctdbp_cdef_sample'
    DEVICE_STATUS = 'ctdbp_cdef_status'
    DEVICE_CALIBRATION = 'ctdbp_cdef_calibration_coefficients'


class Sbe16plusBaseParticle(DataParticle):
    """
    Overload the base particle to add in some common parsing logic for SBE
    instruments.  Add regex methods to help identify and parse multi-line
    strings.
    """
    @staticmethod
    def regex():
        """
        Return a regex string to use in matching functions.  This can be used
        for parsing too if more complex parsing isn't needed.
        @return: uncompiled regex string
        """
        NotImplementedException()

    @staticmethod
    def regex_compiled():
        """
        Return a regex compiled regex of the regex
        @return: compiled regex
        """
        NotImplementedException()

    def regex_multiline(self):
        """
        return a dictionary containing uncompiled regex used to match patterns
        in SBE multiline results. includes an encoder method.
        @return: dictionary of uncompiled regexs
        """
        NotImplementedException()

    def regex_multiline_compiled(self):
        """
        return a dictionary containing compiled regex used to match patterns
        in SBE multiline results.
        @return: dictionary of compiled regexs
        """
        result = {}
        for (key, regex) in self.regex_multiline().iteritems():
            result[key] = re.compile(regex, re.DOTALL)

        return result

    def encoders(self):
        """
        return a dictionary containing encoder methods for parameters
        a special key 'default' can be used to name the default mechanism
        @return: dictionary containing encoder callbacks
        """
        NotImplementedException()

    def _get_multiline_values(self, split_fun=None):
        """
        return a dictionary containing keys and found values from a
        multiline sample using the multiline regex
        @param: split_fun - function to which splits sample into lines
        @return: dictionary of compiled regexs
        """
        result = []

        if split_fun is None:
            split_fun = self._split_on_newline

        matchers = self.regex_multiline_compiled()
        regexs = self.regex_multiline()

        for line in split_fun(self.raw_data):
            log.trace("Line: %s" % line)
            for key in matchers.keys():
                log.trace("match: %s" % regexs.get(key))
                match = matchers[key].search(line)
                if match:
                    encoder = self._get_encoder(key)
                    if encoder:
                        log.debug("encoding value %s (%s)" % (key, match.group(1)))
                        value = encoder(match.group(1))
                    else:
                        value = match.group(1)

                    log.trace("multiline match %s = %s (%s)" % (key, match.group(1), value))
                    result.append({
                        DataParticleKey.VALUE_ID: key,
                        DataParticleKey.VALUE: value
                    })

        return result

    def _split_on_newline(self, value):
        """
        default split method for multiline regex matches
        @param: value string to split
        @return: list of line split on NEWLINE
        """
        return value.split(NEWLINE)

    def _get_encoder(self, key):
        """
        Get an encoder for a key, if one isn't specified look for a default.
        Can return None for no encoder
        @param: key encoder we are looking for
        @return: dictionary of encoders.
        """
        encoder = self.encoders().get(key)
        if not encoder:
            encoder = self.encoders().get(DEFAULT_ENCODER_KEY)

        return encoder

    def _map_param_to_xml_tag(self, parameter_name):
        """
        @return: a string containing the xml tag name for a parameter
        """
        NotImplementedException()

    def _extract_xml_elements(self, node, tag, raise_exception_if_none_found=True):
        """
        extract elements with tag from an XML node
        @param: node - XML node to look in
        @param: tag - tag of elements to look for
        @param: raise_exception_if_none_found - raise an exception if no element is found
        @return: return list of elements found; empty list if none found
        """
        elements = node.getElementsByTagName(tag)
        if raise_exception_if_none_found and len(elements) == 0:
                raise SampleException("_extract_xml_elements: No %s in input data: [%s]" % (tag, self.raw_data))
        return elements

    def _extract_xml_element_value(self, node, tag, raise_exception_if_none_found=True):
        """
        extract element value that has tag from an XML node
        @param: node - XML node to look in
        @param: tag - tag of elements to look for
        @param: raise_exception_if_none_found - raise an exception if no value is found
        @return: return value of element
        """
        elements = self._extract_xml_elements(node, tag, raise_exception_if_none_found)

        if elements is None:
            return None
        children = elements[0].childNodes
        if len(children) == 0 and raise_exception_if_none_found:
            raise SampleException("_extract_xml_element_value: No value for %s in input data: [%s]" % (tag, self.raw_data))
        return children[0].nodeValue

    def _get_xml_parameter(self, xml_element, parameter_name, dtype=float, raise_exception_if_none_found=True):

        try:
            value = dtype(self._extract_xml_element_value(xml_element, self._map_param_to_xml_tag(parameter_name)))

        except SampleException:
            if raise_exception_if_none_found:
                raise SampleException
            value = None

        return {DataParticleKey.VALUE_ID: parameter_name,
                DataParticleKey.VALUE: value}

    ########################################################################
    # Static helpers.
    ########################################################################
    @staticmethod
    def hex2value(hex_value, divisor=None):
        """
        Convert a SBE hex value to a value.  Some hex values are converted
        from raw counts to volts using a divisor.  If passed the value
        will be calculated, otherwise return an int.
        @param hex_value: string to convert
        @param divisor: conversion value
        @return: int or float of the converted value
        """
        if not isinstance(hex_value, str):
            raise InstrumentParameterException("hex value not a string")

        if divisor is not None and divisor == 0:
            raise InstrumentParameterException("divisor can not be 0")

        value = int(hex_value, 16)
        if divisor is not None:
            return float(value) / divisor
        return value

    @staticmethod
    def yesno2bool(value):
        """
        convert a yes no response to a bool
        @param value: string to convert
        @return: bool
        """
        if not (isinstance(value, str) or isinstance(value, unicode)):
            raise InstrumentParameterException("value not a string")

        if value.lower() == 'no':
            return 0
        elif value.lower() == 'yes':
            return 1

        raise InstrumentParameterException("Could not convert '%s' to bool" % value)

    @staticmethod
    def sbetime2unixtime(value):
        """
        Convert an SBE integer time (epoch 1-1-2000) to unix time
        @param value: sbe integer time
        @return: unix time
        """
        if not isinstance(value, int):
            raise InstrumentParameterException("value not a int")

        return SBE_EPOCH + value

    @staticmethod
    def float_to_int(val):
        return int(float(val))


class SBE16DataParticleKey(BaseEnum):
    TEMP = "temperature"
    CONDUCTIVITY = "conductivity"
    PRESSURE = "pressure"
    PRESSURE_TEMP = "pressure_temp"
    TIME = "ctd_time"


class SBE16DataParticle(Sbe16plusBaseParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.

    Sample:
       #03EC1F0A738A81736187100004000B2CFDC618B859BE

    Format:
       #ttttttccccccppppppvvvvvvvvvvvvssssssss

       Temperature = tttttt = 0A5371 (676721 decimal); temperature A/D counts = 676721
       Conductivity = 1BC722 (1820450 decimal); conductivity frequency = 1820450 / 256 = 7111.133 Hz
       Internally mounted strain gauge pressure = pppppp = 0C14C1 (791745 decimal);
           Strain gauge pressure A/D counts = 791745
       Internally mounted strain gauge temperature compensation = vvvv = 7D82 (32,130 decimal);
           Strain gauge temperature = 32,130 / 13,107 = 2.4514 volts
       First external voltage = vvvv = 0305 (773 decimal); voltage = 773 / 13,107 = 0.0590 volts
       Second external voltage = vvvv = 0594 (1428 decimal); voltage = 1428 / 13,107 = 0.1089 volts
       Time = ssssssss = 0EC4270B (247,736,075 decimal); seconds since January 1, 2000 = 247,736,075
    """
    _data_particle_type = DataParticleType.CTD_PARSED

    @staticmethod
    def regex():
        """
        Regular expression to match a sample pattern
        @return: regex string
        """
        #ttttttccccccppppppvvvvvvvvvvvvssssssss
        pattern = r'#? *'            # patter may or may not start with a '
        pattern += r'([0-9A-F]{6})'  # temperature
        pattern += r'([0-9A-F]{6})'  # conductivity
        pattern += r'([0-9A-F]{6})'  # pressure
        pattern += r'([0-9A-F]{4})'  # pressure temp
        pattern += r'[0-9A-F]*'      # consume extra voltage measurements
        pattern += r'([0-9A-F]{8})'  # time
        pattern += NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(SBE16DataParticle.regex())

    def _build_parsed_values(self):
        """
        Take something in the autosample/TS format and split it into
        C, T, and D values (with appropriate tags)
        
        @throws SampleException If there is a problem with sample creation
        """
        match = SBE16DataParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" %
                                  self.raw_data)
            
        try:
            temperature = self.hex2value(match.group(1))
            conductivity = self.hex2value(match.group(2))
            pressure = self.hex2value(match.group(3))
            pressure_temp = self.hex2value(match.group(4))
            elapse_time = self.hex2value(match.group(5))

            self.set_internal_timestamp(unix_time=self.sbetime2unixtime(elapse_time))
        except ValueError:
            raise SampleException("ValueError while converting data: [%s]" %
                                  self.raw_data)
        
        result = [{DataParticleKey.VALUE_ID: SBE16DataParticleKey.TEMP,
                   DataParticleKey.VALUE: temperature},
                  {DataParticleKey.VALUE_ID: SBE16DataParticleKey.CONDUCTIVITY,
                   DataParticleKey.VALUE: conductivity},
                  {DataParticleKey.VALUE_ID: SBE16DataParticleKey.PRESSURE,
                    DataParticleKey.VALUE: pressure},
                  {DataParticleKey.VALUE_ID: SBE16DataParticleKey.PRESSURE_TEMP,
                   DataParticleKey.VALUE: pressure_temp},
                  {DataParticleKey.VALUE_ID: SBE16DataParticleKey.TIME,
                    DataParticleKey.VALUE: elapse_time}]
        
        return result


class SBE16StatusParticleKey(BaseEnum):
    FIRMWARE_VERSION = "firmware_version"
    SERIAL_NUMBER = "serial_number"
    DATE_TIME = "date_time_string"
    VBATT = "battery_voltage_main"
    VLITH = "battery_voltage_lithium"
    IOPER = "operational_current"
    IPUMP = "pump_current"
    LOGGING_STATUS = "logging_status"
    SAMPLES = "num_samples"
    MEM_FREE = "mem_free"
    SAMPLE_INTERVAL = "sample_interval"
    MEASUREMENTS_PER_SAMPLE = "measurements_per_sample"
    PUMP_MODE = "pump_mode"
    DELAY_BEFORE_SAMPLING = "delay_before_sampling"
    DELAY_AFTER_SAMPLING = "delay_after_sampling"
    TX_REAL_TIME = "tx_real_time"
    BATTERY_CUTOFF = "battery_cutoff"
    PRESSURE_SENSOR = "pressure_sensor_type"
    RANGE = "pressure_sensor_range"
    SBE38 = "sbe38"
    SBE50 = "sbe50"
    WETLABS = "wetlabs"
    OPTODE = "optode"
    GAS_TENSION_DEVICE = "gas_tension_device"
    EXT_VOLT_0 = "ext_volt_0"
    EXT_VOLT_1 = "ext_volt_1"
    EXT_VOLT_2 = "ext_volt_2"
    EXT_VOLT_3 = "ext_volt_3"
    EXT_VOLT_4 = "ext_volt_4"
    EXT_VOLT_5 = "ext_volt_5"
    ECHO_CHARACTERS = "echo_characters"
    OUTPUT_FORMAT = "output_format"
    OUTPUT_SALINITY = "output_salinity"
    OUTPUT_SOUND_VELOCITY = "output_sound_velocity"
    SERIAL_SYNC_MODE = "serial_sync_mode"


class SBE16StatusParticle(Sbe16plusBaseParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.DEVICE_STATUS

    @staticmethod
    def regex():
        pattern = r'(<StatusData.*?</StatusData>).*?(<HardwareData.*?</HardwareData>).*?(<ConfigurationData.*?' \
                  r'</ConfigurationData>)'
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(SBE16StatusParticle.regex(), re.DOTALL)

    def _map_param_to_xml_tag(self, parameter_name):
        map_param_to_tag = {
            #GetSD response
            SBE16StatusParticleKey.DATE_TIME: 'DateTime',
            SBE16StatusParticleKey.VBATT : 'vMain',
            SBE16StatusParticleKey.VLITH : 'vLith',
            SBE16StatusParticleKey.IOPER : 'iMain',
            SBE16StatusParticleKey.IPUMP : 'iPump',
            SBE16StatusParticleKey.LOGGING_STATUS : 'LoggingState',
            SBE16StatusParticleKey.SAMPLES : 'Samples',
            SBE16StatusParticleKey.MEM_FREE : 'SamplesFree',
            #GetHD response
            SBE16StatusParticleKey.FIRMWARE_VERSION: 'FirmwareVersion',
            #GetCD response
            SBE16StatusParticleKey.PUMP_MODE : 'AutoRun',
            SBE16StatusParticleKey.DELAY_BEFORE_SAMPLING : 'PumpDelay',
            SBE16StatusParticleKey.DELAY_AFTER_SAMPLING : 'PumpDelay',
            SBE16StatusParticleKey.SBE38 : 'SBE38',
            SBE16StatusParticleKey.SBE50 : 'SBE50',
            SBE16StatusParticleKey.WETLABS : 'WETLABS',
            SBE16StatusParticleKey.OPTODE : 'OPTODE',
            SBE16StatusParticleKey.GAS_TENSION_DEVICE : 'GTD',
            SBE16StatusParticleKey.EXT_VOLT_0 : 'ExtVolt0',
            SBE16StatusParticleKey.EXT_VOLT_1 : 'ExtVolt1',
            SBE16StatusParticleKey.EXT_VOLT_2 : 'ExtVolt2',
            SBE16StatusParticleKey.EXT_VOLT_3 : 'ExtVolt3',
            SBE16StatusParticleKey.EXT_VOLT_4 : 'ExtVolt4',
            SBE16StatusParticleKey.EXT_VOLT_5 : 'ExtVolt5',
            SBE16StatusParticleKey.ECHO_CHARACTERS : 'EchoCharacters',
            SBE16StatusParticleKey.OUTPUT_FORMAT : 'OutputFormat',

            #not sure where these values are coming from
            SBE16StatusParticleKey.OUTPUT_SALINITY : 'OutputSal',
            SBE16StatusParticleKey.OUTPUT_SOUND_VELOCITY : 'OutputSV',
            SBE16StatusParticleKey.SERIAL_SYNC_MODE : 'SyncMode',
            SBE16StatusParticleKey.RANGE : 'PRange',
            SBE16StatusParticleKey.TX_REAL_TIME : 'TxRealTime',
            SBE16StatusParticleKey.BATTERY_CUTOFF : 'CutOff',
            SBE16StatusParticleKey.PRESSURE_SENSOR : 'type',
            SBE16StatusParticleKey.SAMPLE_INTERVAL : 'SampleInterval',
            SBE16StatusParticleKey.MEASUREMENTS_PER_SAMPLE : 'NCycles',
        }
        return map_param_to_tag[parameter_name]

    def _build_parsed_values(self):
        """
        Take something in the autosample/TS format and split it into
        C, T, and D values (with appropriate tags)
        
        @throws SampleException If there is a problem with sample creation
        """
        match = SBE16StatusParticle.regex_compiled().match(self.raw_data)
        
        if not match:
            raise SampleException("No regex match of parsed status data: [%s]" %
                                  self.raw_data)

        dom = parseString(match.group(1))
        root = dom.documentElement
        serial_number = root.getAttribute("SerialNumber")

        result = [{DataParticleKey.VALUE_ID: SBE16StatusParticleKey.SERIAL_NUMBER, DataParticleKey.VALUE: serial_number}]

        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.DATE_TIME, str))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.VBATT))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.VLITH))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.IOPER))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.IPUMP))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.LOGGING_STATUS, str))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.SAMPLES, int))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.MEM_FREE, int))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.SAMPLE_INTERVAL, int, False)),
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.MEASUREMENTS_PER_SAMPLE, int, False)),

        dom = parseString(match.group(2))
        root = dom.documentElement
        sensors = self._extract_xml_elements(root, "Sensor")
        for sensor in sensors:
            sensor_id = sensor.getAttribute("id")
            log.debug('SENSOR ID %r', sensor_id)
            if sensor_id == "Main Pressure":
                result.append(self._get_xml_parameter(sensor, SBE16StatusParticleKey.PRESSURE_SENSOR, str))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.FIRMWARE_VERSION, str))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.RANGE, int, False))

        dom = parseString(match.group(3))
        root = dom.documentElement
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.PUMP_MODE, str))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.DELAY_BEFORE_SAMPLING))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.DELAY_AFTER_SAMPLING)),
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.SBE38, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.SBE50, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.WETLABS, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.OPTODE, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.GAS_TENSION_DEVICE, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.EXT_VOLT_0, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.EXT_VOLT_1, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.EXT_VOLT_2, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.EXT_VOLT_3, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.EXT_VOLT_4, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.EXT_VOLT_5, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.ECHO_CHARACTERS, self.yesno2bool))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.OUTPUT_FORMAT, str))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.OUTPUT_SALINITY, self.yesno2bool, False))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.OUTPUT_SOUND_VELOCITY, self.yesno2bool, False))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.BATTERY_CUTOFF)),
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.TX_REAL_TIME, self.yesno2bool, False))
        result.append(self._get_xml_parameter(root, SBE16StatusParticleKey.SERIAL_SYNC_MODE, self.yesno2bool, False))

        return result


class SBE16CalibrationParticleKey(BaseEnum):
    FIRMWARE_VERSION = "firmware_version"
    SERIAL_NUMBER = "serial_number"
    DATE_TIME = "date_time_string"
    TEMP_CAL_DATE = "calibration_date_temperature"
    TA0 = "temp_coeff_ta0"
    TA1 = "temp_coeff_ta1"
    TA2 = "temp_coeff_ta2"
    TA3 = "temp_coeff_ta3"
    TOFFSET = "temp_coeff_offset"
    COND_CAL_DATE = "calibration_date_conductivity"
    CONDG = "cond_coeff_cg"
    CONDH = "cond_coeff_ch"
    CONDI = "cond_coeff_ci"
    CONDJ = "cond_coeff_cj"
    CPCOR = "cond_coeff_cpcor"
    CTCOR = "cond_coeff_ctcor"
    CSLOPE = "cond_coeff_cslope"
    PRES_SERIAL_NUMBER = "pressure_sensor_serial_number"
    PRES_RANGE = "pressure_sensor_range"
    PRES_CAL_DATE = "calibration_date_pressure"

    # Quartz
    PC1 = "press_coeff_pc1"
    PC2 = "press_coeff_pc2"
    PC3 = "press_coeff_pc3"
    PD1 = "press_coeff_pd1"
    PD2 = "press_coeff_pd2"
    PT1 = "press_coeff_pt1"
    PT2 = "press_coeff_pt2"
    PT3 = "press_coeff_pt3"
    PT4 = "press_coeff_pt4"
    PSLOPE = "press_coeff_pslope"

    # strain gauge
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


class SBE16CalibrationParticle(Sbe16plusBaseParticle):
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
        return re.compile(SBE16CalibrationParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        pattern = r'(<CalibrationCoefficients.*?</CalibrationCoefficients>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        return re.compile(SBE16CalibrationParticle.resp_regex(), re.DOTALL)

    def _map_param_to_xml_tag(self, parameter_name):
            map_param_to_tag = {
                SBE16CalibrationParticleKey.FIRMWARE_VERSION : "DeviceType",
                SBE16CalibrationParticleKey.SERIAL_NUMBER : "SerialNum",
                SBE16CalibrationParticleKey.DATE_TIME : "CalDate",

                SBE16CalibrationParticleKey.TEMP_CAL_DATE : "CalDate",
                SBE16CalibrationParticleKey.TA0 : "TA0",
                SBE16CalibrationParticleKey.TA1 : "TA1",
                SBE16CalibrationParticleKey.TA2 : "TA2",
                SBE16CalibrationParticleKey.TA3 : "TA3",
                SBE16CalibrationParticleKey.TOFFSET : "TOFFSET",

                SBE16CalibrationParticleKey.COND_CAL_DATE : "CalDate",
                SBE16CalibrationParticleKey.CONDG : "G",
                SBE16CalibrationParticleKey.CONDH : "H",
                SBE16CalibrationParticleKey.CONDI : "I",
                SBE16CalibrationParticleKey.CONDJ : "J",
                SBE16CalibrationParticleKey.CPCOR : "CPCOR",
                SBE16CalibrationParticleKey.CTCOR : "CTCOR",
                SBE16CalibrationParticleKey.CSLOPE : "CSLOPE",

                SBE16CalibrationParticleKey.PRES_SERIAL_NUMBER : "SerialNum",
                SBE16CalibrationParticleKey.PRES_RANGE : r'PRANGE',
                SBE16CalibrationParticleKey.PRES_CAL_DATE : "CalDate",
                SBE16CalibrationParticleKey.PA0 : "PA0",
                SBE16CalibrationParticleKey.PA1 : "PA1",
                SBE16CalibrationParticleKey.PA2 : "PA2",
                SBE16CalibrationParticleKey.PTCA0 : "PTCA0",
                SBE16CalibrationParticleKey.PTCA1 : "PTCA1",
                SBE16CalibrationParticleKey.PTCA2 : "PTCA2",
                SBE16CalibrationParticleKey.PTCB0 : "PTCB0",
                SBE16CalibrationParticleKey.PTCB1 : "PTCB1",
                SBE16CalibrationParticleKey.PTCB2 : "PTCB2",
                SBE16CalibrationParticleKey.PTEMPA0 : "PTEMPA0",
                SBE16CalibrationParticleKey.PTEMPA1 : "PTEMPA1",
                SBE16CalibrationParticleKey.PTEMPA2 : "PTEMPA2",

                # Quartz
                SBE16CalibrationParticleKey.PC1 : "PC1",
                SBE16CalibrationParticleKey.PC2 : "PC2",
                SBE16CalibrationParticleKey.PC3 : "PC3",
                SBE16CalibrationParticleKey.PD1 : "PD1",
                SBE16CalibrationParticleKey.PD2 : "PD2",
                SBE16CalibrationParticleKey.PT1 : "PT1",
                SBE16CalibrationParticleKey.PT2 : "PT2",
                SBE16CalibrationParticleKey.PT3 : "PT3",
                SBE16CalibrationParticleKey.PT4 : "PT4",
                SBE16CalibrationParticleKey.PSLOPE : "PSLOPE",
                SBE16CalibrationParticleKey.POFFSET : "POFFSET",
                SBE16CalibrationParticleKey.EXT_VOLT0_OFFSET : "OFFSET",
                SBE16CalibrationParticleKey.EXT_VOLT0_SLOPE : "SLOPE",
                SBE16CalibrationParticleKey.EXT_VOLT1_OFFSET : "OFFSET",
                SBE16CalibrationParticleKey.EXT_VOLT1_SLOPE : "SLOPE",
                SBE16CalibrationParticleKey.EXT_VOLT2_OFFSET : "OFFSET",
                SBE16CalibrationParticleKey.EXT_VOLT2_SLOPE : "SLOPE",
                SBE16CalibrationParticleKey.EXT_VOLT3_OFFSET : "OFFSET",
                SBE16CalibrationParticleKey.EXT_VOLT3_SLOPE : "SLOPE",
                SBE16CalibrationParticleKey.EXT_VOLT4_OFFSET : "OFFSET",
                SBE16CalibrationParticleKey.EXT_VOLT4_SLOPE : "SLOPE",
                SBE16CalibrationParticleKey.EXT_VOLT5_OFFSET : "OFFSET",
                SBE16CalibrationParticleKey.EXT_VOLT5_SLOPE : "SLOPE",
                SBE16CalibrationParticleKey.EXT_FREQ : "EXTFREQSF"
            }
            return map_param_to_tag[parameter_name]

    def _build_parsed_values(self):
        """
        Parse the output of the calibration command
        @throws SampleException If there is a problem with sample creation
        """
        match = SBE16CalibrationParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise SampleException("No regex match of parsed status data: [%s]" %
                                  self.raw_data)

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

        dom = parseString(self.raw_data)
        root = dom.documentElement

        serial_number = root.getAttribute(SERIAL_NUMBER)
        firmware_version = root.getAttribute("DeviceType")
        result = [{DataParticleKey.VALUE_ID: SBE16CalibrationParticleKey.SERIAL_NUMBER, DataParticleKey.VALUE: serial_number},
                  {DataParticleKey.VALUE_ID: SBE16CalibrationParticleKey.FIRMWARE_VERSION, DataParticleKey.VALUE: firmware_version}]

        calibration_elements = self._extract_xml_elements(root, CALIBRATION)
        for calibration in calibration_elements:
            id_attr = calibration.getAttribute(ID)
            if id_attr == TEMPERATURE_SENSOR_ID:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.DATE_TIME, str))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.TEMP_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.TA0))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.TA1))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.TA2))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.TA3))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.TOFFSET))
            elif id_attr == CONDUCTIVITY_SENSOR_ID:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.COND_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.CONDG))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.CONDH))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.CONDI))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.CONDJ))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.CPCOR))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.CTCOR))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.CSLOPE))
            elif id_attr == PRESSURE_SENSOR_ID:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PRES_SERIAL_NUMBER, str))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PRES_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PA0, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PA1, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PA2, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTCA0, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTCA1, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTCA2, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTCB0, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTCB1, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTCB2, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTEMPA0, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTEMPA1, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PTEMPA2, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.POFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PRES_RANGE, self.float_to_int, False))

                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PC1, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PC2, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PC3, float, False))

                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PD1, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PD2, float, False))

                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PT1, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PT2, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PT3, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PT4, float, False))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.PSLOPE, float, False))

            elif id_attr == VOLT0:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT0_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT0_SLOPE))
            elif id_attr == VOLT1:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT1_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT1_SLOPE))
            elif id_attr == VOLT2:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT2_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT2_SLOPE))
            elif id_attr == VOLT3:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT3_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT3_SLOPE))
            elif id_attr == VOLT4:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT4_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT4_SLOPE))
            elif id_attr == VOLT5:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT5_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_VOLT5_SLOPE))
            elif id_attr == EXTERNAL_FREQUENCY_CHANNEL:
                result.append(self._get_xml_parameter(calibration, SBE16CalibrationParticleKey.EXT_FREQ))

        log.debug('RESULT = %r', result)
        return result


###############################################################################
# Driver
###############################################################################
class SBE16InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass for SBE16 driver.
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

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
        self._protocol = SBE16Protocol(Prompt, NEWLINE, self._driver_event)


###############################################################################
# Seabird Electronics 37-SMP MicroCAT protocol.
###############################################################################
class SBE16Protocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class for SBE16 driver.
    Subclasses SeaBirdProtocol
    """
    __metaclass__ = get_logging_metaclass(log_level='debug')
    _sampling = False

    def __init__(self, prompts, newline, driver_event):
        """
        SBE16Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The SBE16 newline.
        @param driver_event Driver process event callback.
        """
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)
        
        # Build SBE16 protocol state machine.
        self._protocol_fsm = InstrumentFSM(ProtocolState, ProtocolEvent, ProtocolEvent.ENTER, ProtocolEvent.EXIT)

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
                (ProtocolEvent.SCHEDULED_ACQUIRED_STATUS, self._handler_command_acquire_status),
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
        # Add build handlers for device commands, only using simple command handler.
        for cmd in Command.list():
            if cmd == Command.SET:
                self._add_build_handler(Command.SET, self._build_set_command)
                self._add_response_handler(Command.SET, self._parse_set_response)
            else:
                self._add_build_handler(cmd, self._build_simple_command)

        # Add response handlers for device commands.
        self._add_response_handler(Command.GET_SD, self._parse_status_response)
        self._add_response_handler(Command.GET_HD, self._parse_status_response)
        self._add_response_handler(Command.GET_CD, self._parse_status_response)
        self._add_response_handler(Command.GET_CC, self._parse_status_response)
        self._add_response_handler(Command.GET_EC, self._parse_status_response)

        # State state machine in UNKNOWN state. 
        self._protocol_fsm.start(ProtocolState.UNKNOWN)
        
        self._chunker = StringChunker(self.sieve_function)

        self._add_scheduler_event(ScheduledJob.ACQUIRE_STATUS, ProtocolEvent.ACQUIRE_STATUS)
        self._add_scheduler_event(ScheduledJob.CLOCK_SYNC, ProtocolEvent.SCHEDULED_CLOCK_SYNC)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        """
        matchers = []
        return_list = []

        matchers.append(SBE16DataParticle.regex_compiled())
        matchers.append(SBE16StatusParticle.regex_compiled())
        matchers.append(SBE16CalibrationParticle.regex_compiled())

        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _filter_capabilities(self, events):
        return [x for x in events if Capability.has(x)]

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if self._extract_sample(SBE16DataParticle, SBE16DataParticle.regex_compiled(), chunk, timestamp):
            self._sampling = True
        any([
            self._extract_sample(SBE16StatusParticle, SBE16StatusParticle.regex_compiled(), chunk, timestamp),
            self._extract_sample(SBE16CalibrationParticle, SBE16CalibrationParticle.regex_compiled(), chunk, timestamp)])

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
        self._cmd_dict.add(Capability.CLOCK_SYNC, display_name="Synchronize Clock")
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name="Acquire Status")
        self._cmd_dict.add(Capability.ACQUIRE_SAMPLE, display_name="Acquire Sample")
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')

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
        Discover current state; can be COMMAND or AUTOSAMPLE.
        @retval (next_state, next_agent_state), COMMAND or AUTOSAMPLE
        @throws InstrumentProtocolException if the device response does not correspond to
        an expected state.
        """

        #check for a sample particle
        self._sampling = False
        timeout = 2
        end_time = time.time() + timeout
        while time.time() < end_time:
            time.sleep(.1)

        if self._sampling:
            return ProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING

        return ProtocolState.COMMAND, ResourceAgentState.IDLE

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        if self._init_type != InitializationType.NONE:
            self._update_params()

        self._init_params()

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        result = []

        result.append(self._do_cmd_resp(Command.GET_SD, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetEC Response: %s", result)

        # Reset the event counter right after getEC
        self._do_cmd_resp(Command.RESET_EC, timeout=TIMEOUT)

        return None, (None, ''.join(result))

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @retval (next_state, result) tuple, (None, None).
        @throws InstrumentParameterException if missing set parameters, if set parameters not ALL and
        not a dict, or if parameter can't be properly formatted.
        """
        startup = False

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        try:
            params = args[0]
            
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')

        try:
            startup = args[1]
        except IndexError:
            pass
        
        self._set_params(params, startup)

        return None, None

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

        # Pump Mode is the only parameter that is set by the driver
        # that where the input isn't validated by the instrument.  So
        # We will do a quick range check before we start all sets
        for (key, val) in params.iteritems():
            if key == Parameter.PUMP_MODE and val not in [0, 1, 2]:
                raise InstrumentParameterException("pump mode out of range")

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
        if update_params:
            self._update_params()

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        Acquire sample from SBE16.
        @retval next_state, (next_agent_state, result) tuple
        """
        result = self._do_cmd_resp(Command.TS, *args, **kwargs)
        return None, (None, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @retval (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        (next_agent_state, None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        self._start_logging(*args, **kwargs)
        
        return ProtocolState.AUTOSAMPLE, (ResourceAgentState.STREAMING, None)

    def _handler_command_start_direct(self):
        """
        Start direct access
        """
        return ProtocolState.DIRECT_ACCESS, (ResourceAgentState.DIRECT_ACCESS, None)

    def _handler_command_clock_sync_clock(self, *args, **kwargs):
        """
        sync clock close to a second edge 
        @retval (next_state, result) tuple, (None, None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        self._wakeup(timeout=TIMEOUT)
        self._sync_clock(Command.SET, Parameter.DATE_TIME, TIMEOUT, time_format='%m%d%Y%H%M%S')

        return None, (None, None)

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_autosample_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change from
        autosample mode.  For this command we have to move the instrument
        into command mode, do the clock sync, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @retval (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        try:
            # Switch to command mode,
            self._stop_logging(*args, **kwargs)

            # Sync the clock
            self._sync_clock(Command.SET, Parameter.DATE_TIME, TIMEOUT, time_format='%m%d%Y%H%M%S')

        finally:
            # Switch back to streaming
            self._start_logging(*args, **kwargs)

        return None, (None, None)

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        if self._init_type != InitializationType.NONE:
            self._stop_logging()
            self._update_params()
            self._init_params()
            self._start_logging()

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @retval (next_state, result) tuple
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command misunderstood or
        incorrect prompt received.
        """
        self._stop_logging(*args, **kwargs)

        return ProtocolState.COMMAND, (ResourceAgentState.COMMAND, None)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """

        # When in autosample this command requires two wakeups to get to the right prompt
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)

        result = []

        result.append(self._do_cmd_resp(Command.GET_SD, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetEC Response: %s", result)

        # Reset the event counter right after getEC
        self._do_cmd_resp(Command.RESET_EC, timeout=TIMEOUT)

        return None, (None, ''.join(result))
        
    ########################################################################
    # Common handlers.
    ########################################################################
    def _sync_clock(self, command, date_time_param, timeout=TIMEOUT, delay=1, time_format="%m%d%Y%H%M%S"):
        """
        Send the command to the instrument to synchronize the clock
        @param command: command to set6 date time
        @param date_time_param: date time parameter that we want to set
        @param timeout: command timeout
        @param delay: wakeup delay
        @param time_format: time format string for set command
        @raise: InstrumentProtocolException if command fails
        """
        # lets clear out any past data so it doesnt confuse the command
        self._linebuf = ''
        self._promptbuf = ''

        log.debug("Set time format(%s) '%s''", time_format, date_time_param)
        str_val = get_timestamp_delayed(time_format)
        log.debug("Set time value == '%s'", str_val)
        self._do_cmd_resp(command, date_time_param, str_val)

    def _handler_generic_exit(self, *args, **kwargs):
        """
        Exit unknown state.
        """
        pass

    ########################################################################
    # Direct access handlers.
    ########################################################################
    def _handler_direct_access_enter(self, *args, **kwargs):
        """
        Enter direct access state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        self._sent_cmds = []

    def _handler_direct_access_execute_direct(self, data):
        """
        """
        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return None, (None, None)

    def _handler_direct_access_stop_direct(self):
        """
        @throw InstrumentProtocolException on invalid command
        """
        next_state, next_agent_state = self._handler_unknown_discover()
        if next_state == DriverProtocolState.COMMAND:
            next_agent_state = ResourceAgentState.COMMAND

        return next_state, (next_agent_state, None)

    ########################################################################
    # Private helpers.
    ########################################################################
    def _start_logging(self, *args, **kwargs):
        """
        Command the instrument to start logging
        @param timeout: how long to wait for a prompt
        @return: True if successful
        @raise: InstrumentProtocolException if failed to start logging
        """
        self._do_cmd_resp(Command.STARTNOW, *args, **kwargs)

    def _stop_logging(self, *args, **kwargs):
        """
        Command the instrument to stop logging
        @param timeout: how long to wait for a prompt
        @return: True if successful
        @raise: InstrumentTimeoutException if prompt isn't seen
        @raise: InstrumentProtocolException failed to stop logging
        """
        kwargs['timeout'] = TIMEOUT
        self._do_cmd_resp(Command.STOP, *args, **kwargs)

    def _send_wakeup(self):
        """
        Send a newline to attempt to wake the SBE16 device.
        """
        self._connection.send(NEWLINE)
                
    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary. Wake the device then issue
        display status and display calibration commands. The parameter
        dict will match line output and update itself.
        @throws InstrumentTimeoutException if device cannot be timely woken.
        @throws InstrumentProtocolException if ds/dc misunderstood.
        """
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)

        # For some reason when in streaming we require a second wakeup
        self._wakeup(timeout=WAKEUP_TIMEOUT, delay=0.3)

        # Get old param dict config.
        old_config = self._param_dict.get_config()

        # Issue status commands
        self._do_cmd_resp(Command.GET_SD, timeout=TIMEOUT)
        self._do_cmd_resp(Command.GET_CD, timeout=TIMEOUT)
        self._do_cmd_resp(Command.GET_HD, timeout=TIMEOUT)

        # Get new param dict config. If it differs from the old config,
        # tell driver superclass to publish a config change event.
        new_config = self._param_dict.get_config()

        log.debug("Old Config: %s", old_config)
        log.debug("New Config: %s", new_config)
        if not dict_equal(new_config, old_config) and self._protocol_fsm.get_current_state() != ProtocolState.UNKNOWN:
            log.debug("parameters updated, sending event")
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

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
            if param is Parameter.DATE_TIME:
                set_cmd = '%s=%s%s' % (param, val, NEWLINE)
            else:
                str_val = self._param_dict.format(param, val)
                set_cmd = '%s=%s%s' % (param, str_val, NEWLINE)

            # Some set commands need to be sent twice to confirm
            if param in ConfirmedParameter.list():
                set_cmd += set_cmd

        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter %s' % param)
            
        return set_cmd

    def _find_error(self, response):
        """
        Find an error xml message in a response
        @param response command response string.
        @return tuple with type and message, None otherwise
        """
        match = re.search(ERROR_REGEX, response)
        if match:
            return match.group(1), match.group(2)

        return None

    def _parse_set_response(self, response, prompt):
        """
        Parse handler for set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if set command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("Set command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentParameterException('Set command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if prompt not in [Prompt.EXECUTED, Prompt.COMMAND]:
            log.error("Set command encountered error; instrument returned: %s", response)
            raise InstrumentProtocolException('Set command not recognized: %s' % response)

    def _parse_status_response(self, response, prompt):
        """
        Parse handler for status commands.
        @param response command response string.
        @param prompt prompt following command response.        
        @throws InstrumentProtocolException if command misunderstood.
        """
        if prompt not in [Prompt.COMMAND, Prompt.EXECUTED]: 
            raise InstrumentProtocolException('Command not recognized: %s.' % response)

        for line in response.split(NEWLINE):
            self._param_dict.update(line)

        return response

    def _build_common_param_dict(self):

        self._param_dict.add(Parameter.LOGGING,
                             r'LoggingState>(not )?logging</LoggingState',
                             lambda match: False if (match.group(1)) else True,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Logging",
                             description="Enable logging: (true | false)",
                             visibility=ParameterDictVisibility.READ_ONLY)
        self._param_dict.add(Parameter.VOLT0,
                             r'ExtVolt0>(.*)</ExtVolt0',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 0",
                             description="Enable external voltage 0: (true | false)",
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
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.VOLT2,
                             r'ExtVolt2>(.*)</ExtVolt2',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Volt 2",
                             description="Enable external voltage 2: (true | false)",
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
                             description="Enable external voltage 3: (true | false)",
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
                             description="Enable external voltage 4: (true | false)",
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
                             description="Enable external voltage 5: (true | false)",
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
                             description="Enable SBE38: (true | false)",
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
                             description="Enable SBE63: (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.WETLABS,
                             r'WETLABS>(.*)</WETLABS',
                             lambda match: True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Wetlabs Sensor Attached",
                             description="Enable Wetlabs sensor: (true | false)",
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
                             description="Enable GTD: (true | false)",
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
                             description="Enable second GTD: (true | false)",
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
                             description="Format for the instrument output: (0:raw hex | 1:converted hex | 2:raw decimal | "
                                         "3:converted decimal | 4:converted hex for afm | 5:converted xml uvic)",
                             startup_param=True,
                             direct_access=True,
                             default_value=0,
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
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with SBE16 parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        self._build_common_param_dict()

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
        self._param_dict.add(Parameter.ECHO,
                             r'<EchoCharacters>(.*)</EchoCharacters>',
                             lambda match : True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Echo Characters",
                             description="Enable characters to be echoed as typed (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.OUTPUT_EXEC_TAG,
                             r'<OutputExecutedTag>(.*)</OutputExecutedTag>',
                             lambda match : True,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Output Execute Tag",
                             description="Enable display of XML executing and executed tags (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.PUMP_MODE,
                             r'<AutoRun>(.*)</AutoRun>',
                             self._pump_mode_to_int,
                             str,
                             type=ParameterDictType.INT,
                             display_name="Pump Mode",
                             description="Mode: (0:no pump | 1:run pump for 0.5 sec | 2:run pump during sample)",
                             startup_param=True,
                             direct_access=True,
                             default_value=2)
        self._param_dict.add(Parameter.SBE50,
                             r'SBE50>(.*)</SBE50',
                             lambda match : True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="SBE50 Attached",
                             description="Enabled SBE50: (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.DELAY_BEFORE_SAMPLE,
                             r'DelayBeforeSampling>(.*?)</DelayBeforeSampling',
                             lambda match : float(match.group(1)),
                             self._float_to_string,
                             type=ParameterDictType.FLOAT,
                             display_name="Delay Before Sample",
                             description=" Time to wait after switching on external voltages and RS-232 sensors "
                                         "before sampling: (0-600).",
                             startup_param=True,
                             direct_access=True,
                             default_value=0.0,
                             units=Units.SECOND,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.DELAY_AFTER_SAMPLE,
                             r'DelayAfterSample>(.*?)</DelayBeforeSampling',
                             lambda match : float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             display_name="Delay After Sample",
                             description="Time to wait after sampling is completed, before turning off power "
                                         "to external voltages and RS-232 sensors.",
                             startup_param=True,
                             direct_access=True,
                             default_value=0.0,
                             units=Units.SECOND,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.SYNCMODE,
                             r'SyncMode>(dis|en)abled</SyncMode',
                             lambda match : True if match.group(1) == 'en' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Enable Serial Sync",
                             description="Enable serial line sync mode: (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.NCYCLES,
                             r'NCycles>(.*?)</NCycles',
                             lambda match : int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Ncycles",
                             description="Number of measurements to take and average every SampleInterval seconds.",
                             startup_param=True,
                             direct_access=False,
                             default_value=4)
        self._param_dict.add(Parameter.INTERVAL,
                             r'SampleInterval>(.*?)</SampleInterval',
                             lambda match : int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Sample Interval",
                             description="Interval between samples: (10 - 14,400).",
                             startup_param=True,
                             direct_access=False,
                             units=Units.SECOND,
                             default_value=10)
        self._param_dict.add(Parameter.BIOWIPER,
                             r'Biowiper>(.*?)</Biowiper',
                             lambda match : False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Biowiper",
                             description="Enable ECO-FL fluorometer with Bio-Wiper: (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.TXREALTIME,
                             r'TxRealTime>(yes|no)</TxRealTime',
                             lambda match : True if match.group(1) == 'yes' else False,
                             self._true_false_to_string,
                             type=ParameterDictType.BOOL,
                             display_name="Transmit Real-Time",
                             description="Enable real-time data output: (true | false)",
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)

    ########################################################################
    # Static helpers to format set commands.
    ########################################################################
    @staticmethod
    def _pressure_sensor_to_int(match):
        """
        map a pressure sensor string into an int representation
        @param match: regex match
        @return: mode 1, 2, 3 or None for no match
        """
        v = match.group(1)

        log.debug("get pressure type from: %s", v)
        if v == "strain gauge" or v == "strain-0":
            return 1
        elif v == "quartz without temp comp":
            return 2
        elif v == "quartz with temp comp" or v == "quartzTC-0":
            return 3
        else:
            return None

    @staticmethod
    def _pump_mode_to_int(match):
        """
        map a pump mode string into an int representation
        @param match: regex match
        @return: mode 0, 1, 2 or None for no match
        """
        v = match.group(1)

        log.debug("get pump mode from: %s", v)
        if v == "no pump":
            return 0
        elif v == "run pump for 0.5 sec":
            return 1
        elif v == "run pump during sample":
            return 2
        else:
            return None

    @staticmethod
    def _true_false_to_string(v):
        """
        Write a boolean value to string formatted for sbe16 set operations.
        @param v a boolean value.
        @retval A yes/no string formatted for sbe16 set operations.
        @throws InstrumentParameterException if value not a bool.
        """
        
        if not isinstance(v,bool):
            raise InstrumentParameterException('Value %s is not a bool.' % str(v))
        if v:
            return 'y'
        else:
            return 'n'

    @staticmethod
    def _string_to_numeric_date_time_string(date_time_string):
        """
        convert string from "21 AUG 2012  09:51:55" to numeric "mmddyyyyhhmmss"
        """
        return time.strftime("%m%d%Y%H%M%S", time.strptime(date_time_string, "%d %b %Y %H:%M:%S"))

    @staticmethod
    def _output_format_string_2_int(format_string):
        """
        Convert an output format from an string to an int
        @param format_string sbe output format as string or regex match
        @retval int representation of output format
        @raise InstrumentParameterException if format unknown
        """
        if not isinstance(format_string, str):
            format_string = format_string.group(1)

        if format_string.lower() == "raw hex":
            return 0
        elif format_string.lower() == "converted hex":
            return 1
        elif format_string.lower() == "raw decimal":
            return 2
        elif format_string.lower() == "converted decimal":
            return 3
        elif format_string.lower() == "converted hex for afm":
            return 4
        elif format_string.lower() == "converted xml uvic":
            return 5

        raise InstrumentParameterException("output format unknown: %s" % format_string)
