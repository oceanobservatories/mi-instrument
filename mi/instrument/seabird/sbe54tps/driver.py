"""
@package mi.instrument.seabird.sbe54tps.ooicore.driver
@file /Users/unwin/OOI/Workspace/code/marine-integrations/mi/instrument/seabird/sbe54tps/ooicore/driver.py
@author Roger Unwin
@brief Driver for the ooicore
Release notes:

"""


__author__ = 'Roger Unwin'
__license__ = 'Apache 2.0'

import re
import time
from mi.core.log import get_logger

log = get_logger()

from mi.core.util import dict_equal

from mi.core.common import BaseEnum, Units
from mi.core.time_tools import get_timestamp_delayed
from mi.core.time_tools import timegm_to_float

from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.driver_dict import DriverDictKey

from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentException

from mi.core.instrument.data_particle import DataParticle, DataParticleKey, CommonDataParticleType
from mi.core.instrument.chunker import StringChunker

from mi.instrument.seabird.driver import SeaBirdInstrumentDriver
from mi.instrument.seabird.driver import SeaBirdProtocol
from mi.instrument.seabird.driver import NEWLINE
from mi.instrument.seabird.driver import TIMEOUT


GENERIC_PROMPT = r"S>"
LONG_TIMEOUT = 200


class ScheduledJob(BaseEnum):
    ACQUIRE_STATUS = 'acquire_status'
    CONFIGURATION_DATA = "configuration_data"
    STATUS_DATA = "status_data"
    EVENT_COUNTER_DATA = "event_counter"
    HARDWARE_DATA = "hardware_data"
    CLOCK_SYNC = 'clock_sync'


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    PREST_REAL_TIME = 'prest_real_time'
    PREST_REFERENCE_OSCILLATOR = 'prest_reference_oscillator'
    PREST_CONFIGURATION_DATA = 'prest_configuration_data'
    PREST_DEVICE_STATUS = 'prest_device_status'
    PREST_EVENT_COUNTER = 'prest_event_counter'
    PREST_HARDWARE_DATA = 'prest_hardware_data'


# Device specific parameters.
class InstrumentCmds(BaseEnum):
    """
    Instrument Commands
    These are the commands that according to the science profile must be supported.
    """
    # Artificial Constructed Commands for Driver
    SET = "set"

    # Status
    GET_CONFIGURATION_DATA = "GetCD"
    GET_STATUS_DATA = "GetSD"
    GET_EVENT_COUNTER_DATA = "GetEC"
    GET_HARDWARE_DATA = "GetHD"

    # Sampling
    START_LOGGING = "Start"
    STOP_LOGGING = "Stop"
    SAMPLE_REFERENCE_OSCILLATOR = "SampleRefOsc"

    # Diagnostic
    TEST_EEPROM = "TestEeprom"


class ProtocolState(BaseEnum):
    """
    Protocol state enum.
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    OSCILLATOR = "DRIVER_STATE_OSCILLATOR"
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    RECOVER_AUTOSAMPLE = 'PROTOCOL_EVENT_RECOVER_AUTOSAMPLE'
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    SAMPLE_REFERENCE_OSCILLATOR = 'PROTOCOL_EVENT_SAMPLE_REFERENCE_OSCILLATOR'
    TEST_EEPROM = 'PROTOCOL_EVENT_TEST_EEPROM'
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    INIT_PARAMS = DriverEvent.INIT_PARAMS
    SCHEDULED_CLOCK_SYNC = DriverEvent.SCHEDULED_CLOCK_SYNC
    SCHEDULED_ACQUIRE_STATUS = 'PROTOCOL_EVENT_SCHEDULED_ACQUIRE_STATUS'
    ACQUIRE_OSCILLATOR_SAMPLE = 'PROTOCOL_EVENT_ACQUIRE_OSCILLATOR_SAMPLE'


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = ProtocolEvent.CLOCK_SYNC
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    SAMPLE_REFERENCE_OSCILLATOR = ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR
    TEST_EEPROM = ProtocolEvent.TEST_EEPROM
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    DISCOVER = DriverEvent.DISCOVER

# Device specific parameters.
class Parameter(DriverParameter):
    TIME = "time"
    SAMPLE_PERIOD = "sampleperiod"
    ENABLE_ALERTS = "enablealerts"
    BATTERY_TYPE = "batterytype"


# Device prompts.
class Prompt(BaseEnum):
    COMMAND = "<Executed/>\r\nS>"
    AUTOSAMPLE = "<Executed/>\r\n"
    BAD_COMMAND_AUTOSAMPLE = "<Error.*?\r\n<Executed/>\r\n"
    BAD_COMMAND = "<Error.*?\r\n<Executed/>\r\nS>"


######################### PARTICLES #############################
STATUS_DATA_REGEX =r"(<StatusData DeviceType='.*?</StatusData>)"
STATUS_DATA_REGEX_MATCHER = re.compile(STATUS_DATA_REGEX, re.DOTALL)

CONFIGURATION_DATA_REGEX = r"(<ConfigurationData DeviceType=.*?</ConfigurationData>)"
CONFIGURATION_DATA_REGEX_MATCHER = re.compile(CONFIGURATION_DATA_REGEX, re.DOTALL)

EVENT_COUNTER_DATA_REGEX = r"(<EventSummary numEvents='.*?</EventList>)"
EVENT_COUNTER_DATA_REGEX_MATCHER = re.compile(EVENT_COUNTER_DATA_REGEX, re.DOTALL)

HARDWARE_DATA_REGEX = r"(<HardwareData DeviceType='.*?</HardwareData>)"
HARDWARE_DATA_REGEX_MATCHER = re.compile(HARDWARE_DATA_REGEX, re.DOTALL)

SAMPLE_DATA_REGEX = r"<Sample Num='[0-9]+' Type='Pressure'>.*?</Sample>"
SAMPLE_DATA_REGEX_MATCHER = re.compile(SAMPLE_DATA_REGEX, re.DOTALL)

SAMPLE_REF_OSC_REGEX = r"<SetTimeout>.*?</Sample>"
SAMPLE_REF_OSC_MATCHER = re.compile(SAMPLE_REF_OSC_REGEX, re.DOTALL)

ENGINEERING_DATA_REGEX = r"<MainSupplyVoltage>(.*?)</MainSupplyVoltage>"
ENGINEERING_DATA_MATCHER = re.compile(SAMPLE_REF_OSC_REGEX, re.DOTALL)

RECOVER_AUTOSAMPLE_REGEX = "CMD Mode 2 min timeout, returning to ACQ Mode"
RECOVER_AUTOSAMPLE_MATCHER = re.compile(RECOVER_AUTOSAMPLE_REGEX, re.DOTALL)


class SBE54tpsStatusDataParticleKey(BaseEnum):
    DEVICE_TYPE = "device_type"
    SERIAL_NUMBER = "serial_number"
    TIME = "date_time_str"
    EVENT_COUNT = "event_count"
    MAIN_SUPPLY_VOLTAGE = "battery_voltage_main"
    NUMBER_OF_SAMPLES = "sample_number"
    BYTES_USED = "bytes_used"
    BYTES_FREE = "bytes_free"


class SBE54tpsStatusDataParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.PREST_DEVICE_STATUS

    LINE1 = r"<StatusData DeviceType='([^']+)' SerialNumber='(\d+)'>"
    LINE2 = r"<DateTime>([^<]+)</DateTime>"
    LINE3 = r"<EventSummary numEvents='(\d+)'/>"
    LINE4 = r"<MainSupplyVoltage>([.\d]+)</MainSupplyVoltage>"
    LINE5 = r"<Samples>(\d+)</Samples>"
    LINE6 = r"<Bytes>(\d+)</Bytes>"
    LINE7 = r"<BytesFree>(\d+)</BytesFree>"

    def _build_parsed_values(self):
        """
        Take something in the StatusData format and split it into
        values with appropriate tags

        @throws SampleException If there is a problem with sample creation
        """
        values = {
            SBE54tpsStatusDataParticleKey.DEVICE_TYPE: None,
            SBE54tpsStatusDataParticleKey.SERIAL_NUMBER: None,
            SBE54tpsStatusDataParticleKey.TIME: None,
            SBE54tpsStatusDataParticleKey.EVENT_COUNT: None,
            SBE54tpsStatusDataParticleKey.MAIN_SUPPLY_VOLTAGE: None,
            SBE54tpsStatusDataParticleKey.NUMBER_OF_SAMPLES: None,
            SBE54tpsStatusDataParticleKey.BYTES_USED: None,
            SBE54tpsStatusDataParticleKey.BYTES_FREE: None
        }

        matchers = {
            re.compile(self.LINE1): [SBE54tpsStatusDataParticleKey.DEVICE_TYPE,
                                     SBE54tpsStatusDataParticleKey.SERIAL_NUMBER],
            re.compile(self.LINE2): [SBE54tpsStatusDataParticleKey.TIME],
            re.compile(self.LINE3): [SBE54tpsStatusDataParticleKey.EVENT_COUNT],
            re.compile(self.LINE4): [SBE54tpsStatusDataParticleKey.MAIN_SUPPLY_VOLTAGE],
            re.compile(self.LINE5): [SBE54tpsStatusDataParticleKey.NUMBER_OF_SAMPLES],
            re.compile(self.LINE6): [SBE54tpsStatusDataParticleKey.BYTES_USED],
            re.compile(self.LINE7): [SBE54tpsStatusDataParticleKey.BYTES_FREE]
        }

        for line in self.raw_data.split(NEWLINE):
            for matcher, keys in matchers.iteritems():
                match = matcher.match(line)

                if match:
                    for index, key in enumerate(keys):
                        val = match.group(index + 1)

                        if key in [SBE54tpsStatusDataParticleKey.DEVICE_TYPE,
                                   SBE54tpsStatusDataParticleKey.SERIAL_NUMBER]:
                            values[key] = val

                        elif key in [SBE54tpsStatusDataParticleKey.EVENT_COUNT,
                                     SBE54tpsStatusDataParticleKey.NUMBER_OF_SAMPLES,
                                     SBE54tpsStatusDataParticleKey.BYTES_USED,
                                     SBE54tpsStatusDataParticleKey.BYTES_FREE]:
                            values[key] = int(val)

                        elif key in [SBE54tpsStatusDataParticleKey.MAIN_SUPPLY_VOLTAGE]:
                            values[key] = float(val)

                        elif key in [SBE54tpsStatusDataParticleKey.TIME]:
                            values[key] = val
                            py_timestamp = time.strptime(val, "%Y-%m-%dT%H:%M:%S")
                            self.set_internal_timestamp(unix_time=timegm_to_float(py_timestamp))

        result = []
        for key, value in values.iteritems():
            result.append({DataParticleKey.VALUE_ID: key, DataParticleKey.VALUE: value})

        return result


class SBE54tpsConfigurationDataParticleKey(BaseEnum):
    DEVICE_TYPE = "device_type"
    SERIAL_NUMBER = "serial_number"
    ACQ_OSC_CAL_DATE = "calibration_date_acq_crystal"
    FRA0 = "acq_crystal_coeff_fra0"
    FRA1 = "acq_crystal_coeff_fra1"
    FRA2 = "acq_crystal_coeff_fra2"
    FRA3 = "acq_crystal_coeff_fra3"
    PRESSURE_SERIAL_NUM = "pressure_sensor_serial_number"
    PRESSURE_CAL_DATE = "calibration_date_pressure"
    PU0 = "press_coeff_pu0"
    PY1 = "press_coeff_py1"
    PY2 = "press_coeff_py2"
    PY3 = "press_coeff_py3"
    PC1 = "press_coeff_pc1"
    PC2 = "press_coeff_pc2"
    PC3 = "press_coeff_pc3"
    PD1 = "press_coeff_pd1"
    PD2 = "press_coeff_pd2"
    PT1 = "press_coeff_pt1"
    PT2 = "press_coeff_pt2"
    PT3 = "press_coeff_pt3"
    PT4 = "press_coeff_pt4"
    PRESSURE_OFFSET = "press_coeff_poffset"
    PRESSURE_RANGE = "pressure_sensor_range"
    BATTERY_TYPE = "battery_type"
    BAUD_RATE = "baud_rate"
    UPLOAD_TYPE = "upload_type"
    ENABLE_ALERTS = "enable_alerts"
    SAMPLE_PERIOD = "sample_period"


class SBE54tpsConfigurationDataParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.PREST_CONFIGURATION_DATA

    LINE1 = r"<ConfigurationData DeviceType='([^']+)' SerialNumber='(\d+)'>"
    LINE2 = r"<AcqOscCalDate>([0-9\-]+)</AcqOscCalDate>"
    LINE3 = r"<FRA0>([0-9E+-.]+)</FRA0>"
    LINE4 = r"<FRA1>([0-9E+-.]+)</FRA1>"
    LINE5 = r"<FRA2>([0-9E+-.]+)</FRA2>"
    LINE6 = r"<FRA3>([0-9E+-.]+)</FRA3>"
    LINE7 = r"<PressureSerialNum>(\d+)</PressureSerialNum>"
    LINE8 = r"<PressureCalDate>([0-9\-]+)</PressureCalDate>"
    LINE9 = r"<pu0>([0-9E+-.]+)</pu0>"
    LINE10 = r"<py1>([0-9E+-.]+)</py1>"
    LINE11 = r"<py2>([0-9E+-.]+)</py2>"
    LINE12 = r"<py3>([0-9E+-.]+)</py3>"
    LINE13 = r"<pc1>([0-9E+-.]+)</pc1>"
    LINE14 = r"<pc2>([0-9E+-.]+)</pc2>"
    LINE15 = r"<pc3>([0-9E+-.]+)</pc3>"
    LINE16 = r"<pd1>([0-9E+-.]+)</pd1>"
    LINE17 = r"<pd2>([0-9E+-.]+)</pd2>"
    LINE18 = r"<pt1>([0-9E+-.]+)</pt1>"
    LINE19 = r"<pt2>([0-9E+-.]+)</pt2>"
    LINE20 = r"<pt3>([0-9E+-.]+)</pt3>"
    LINE21 = r"<pt4>([0-9E+-.]+)</pt4>"
    LINE22 = r"<poffset>([0-9E+-.]+)</poffset>"
    LINE23 = r"<prange>([0-9E+-.]+)</prange>"
    LINE24 = r"batteryType='(\d+)'"
    LINE25 = r"baudRate='(\d+)'"
    LINE26 = r"enableAlerts='(\d+)'"
    LINE27 = r"uploadType='(\d+)'"
    LINE28 = r"samplePeriod='(\d+)'"

    def _build_parsed_values(self):
        """
        Take something in the StatusData format and split it into
        values with appropriate tags

        @throws SampleException If there is a problem with sample creation
        """

        values = {
            SBE54tpsConfigurationDataParticleKey.DEVICE_TYPE: None,
            SBE54tpsConfigurationDataParticleKey.SERIAL_NUMBER: None,
            SBE54tpsConfigurationDataParticleKey.ACQ_OSC_CAL_DATE: None,
            SBE54tpsConfigurationDataParticleKey.FRA0: None,
            SBE54tpsConfigurationDataParticleKey.FRA1: None,
            SBE54tpsConfigurationDataParticleKey.FRA2: None,
            SBE54tpsConfigurationDataParticleKey.FRA3: None,
            SBE54tpsConfigurationDataParticleKey.PRESSURE_SERIAL_NUM: None,
            SBE54tpsConfigurationDataParticleKey.PRESSURE_CAL_DATE: None,
            SBE54tpsConfigurationDataParticleKey.PU0: None,
            SBE54tpsConfigurationDataParticleKey.PY1: None,
            SBE54tpsConfigurationDataParticleKey.PY2: None,
            SBE54tpsConfigurationDataParticleKey.PY3: None,
            SBE54tpsConfigurationDataParticleKey.PC1: None,
            SBE54tpsConfigurationDataParticleKey.PC2: None,
            SBE54tpsConfigurationDataParticleKey.PC3: None,
            SBE54tpsConfigurationDataParticleKey.PD1: None,
            SBE54tpsConfigurationDataParticleKey.PD2: None,
            SBE54tpsConfigurationDataParticleKey.PT1: None,
            SBE54tpsConfigurationDataParticleKey.PT2: None,
            SBE54tpsConfigurationDataParticleKey.PT3: None,
            SBE54tpsConfigurationDataParticleKey.PT4: None,
            SBE54tpsConfigurationDataParticleKey.PRESSURE_OFFSET: None,
            SBE54tpsConfigurationDataParticleKey.PRESSURE_RANGE: None,
            SBE54tpsConfigurationDataParticleKey.BATTERY_TYPE: None,
            SBE54tpsConfigurationDataParticleKey.BAUD_RATE: None,
            SBE54tpsConfigurationDataParticleKey.ENABLE_ALERTS: None,
            SBE54tpsConfigurationDataParticleKey.UPLOAD_TYPE: None,
            SBE54tpsConfigurationDataParticleKey.SAMPLE_PERIOD: None
        }

        matchers = {
            re.compile(self.LINE1): [SBE54tpsConfigurationDataParticleKey.DEVICE_TYPE,
                                     SBE54tpsConfigurationDataParticleKey.SERIAL_NUMBER],
            re.compile(self.LINE2): [SBE54tpsConfigurationDataParticleKey.ACQ_OSC_CAL_DATE],
            re.compile(self.LINE3): [SBE54tpsConfigurationDataParticleKey.FRA0],
            re.compile(self.LINE4): [SBE54tpsConfigurationDataParticleKey.FRA1],
            re.compile(self.LINE5): [SBE54tpsConfigurationDataParticleKey.FRA2],
            re.compile(self.LINE6): [SBE54tpsConfigurationDataParticleKey.FRA3],
            re.compile(self.LINE7): [SBE54tpsConfigurationDataParticleKey.PRESSURE_SERIAL_NUM],
            re.compile(self.LINE8): [SBE54tpsConfigurationDataParticleKey.PRESSURE_CAL_DATE],
            re.compile(self.LINE9): [SBE54tpsConfigurationDataParticleKey.PU0],
            re.compile(self.LINE10): [SBE54tpsConfigurationDataParticleKey.PY1],
            re.compile(self.LINE11): [SBE54tpsConfigurationDataParticleKey.PY2],
            re.compile(self.LINE12): [SBE54tpsConfigurationDataParticleKey.PY3],
            re.compile(self.LINE13): [SBE54tpsConfigurationDataParticleKey.PC1],
            re.compile(self.LINE14): [SBE54tpsConfigurationDataParticleKey.PC2],
            re.compile(self.LINE15): [SBE54tpsConfigurationDataParticleKey.PC3],
            re.compile(self.LINE16): [SBE54tpsConfigurationDataParticleKey.PD1],
            re.compile(self.LINE17): [SBE54tpsConfigurationDataParticleKey.PD2],
            re.compile(self.LINE18): [SBE54tpsConfigurationDataParticleKey.PT1],
            re.compile(self.LINE19): [SBE54tpsConfigurationDataParticleKey.PT2],
            re.compile(self.LINE20): [SBE54tpsConfigurationDataParticleKey.PT3],
            re.compile(self.LINE21): [SBE54tpsConfigurationDataParticleKey.PT4],
            re.compile(self.LINE22): [SBE54tpsConfigurationDataParticleKey.PRESSURE_OFFSET],
            re.compile(self.LINE23): [SBE54tpsConfigurationDataParticleKey.PRESSURE_RANGE],
            re.compile(self.LINE24): [SBE54tpsConfigurationDataParticleKey.BATTERY_TYPE],
            re.compile(self.LINE25): [SBE54tpsConfigurationDataParticleKey.BAUD_RATE],
            re.compile(self.LINE26): [SBE54tpsConfigurationDataParticleKey.ENABLE_ALERTS],
            re.compile(self.LINE27): [SBE54tpsConfigurationDataParticleKey.UPLOAD_TYPE],
            re.compile(self.LINE28): [SBE54tpsConfigurationDataParticleKey.SAMPLE_PERIOD]
        }

        for line in self.raw_data.split(NEWLINE):
            for matcher, keys in matchers.iteritems():
                match = matcher.match(line)

                if match:
                    for index, key in enumerate(keys):
                        val = match.group(index + 1)

                        if key in [SBE54tpsConfigurationDataParticleKey.DEVICE_TYPE,
                                   SBE54tpsConfigurationDataParticleKey.PRESSURE_CAL_DATE,
                                   SBE54tpsConfigurationDataParticleKey.ACQ_OSC_CAL_DATE,
                                   SBE54tpsConfigurationDataParticleKey.PRESSURE_SERIAL_NUM,
                                   SBE54tpsConfigurationDataParticleKey.SERIAL_NUMBER,]:
                            values[key] = val

                        elif key in [SBE54tpsConfigurationDataParticleKey.BATTERY_TYPE,
                                     SBE54tpsConfigurationDataParticleKey.UPLOAD_TYPE,
                                     SBE54tpsConfigurationDataParticleKey.SAMPLE_PERIOD,
                                     SBE54tpsConfigurationDataParticleKey.BAUD_RATE,
                                     SBE54tpsConfigurationDataParticleKey.ENABLE_ALERTS]:
                            values[key] = int(val)

                        elif key in [SBE54tpsConfigurationDataParticleKey.FRA0,
                                     SBE54tpsConfigurationDataParticleKey.FRA1,
                                     SBE54tpsConfigurationDataParticleKey.FRA2,
                                     SBE54tpsConfigurationDataParticleKey.FRA3,
                                     SBE54tpsConfigurationDataParticleKey.PU0,
                                     SBE54tpsConfigurationDataParticleKey.PY1,
                                     SBE54tpsConfigurationDataParticleKey.PY2,
                                     SBE54tpsConfigurationDataParticleKey.PY3,
                                     SBE54tpsConfigurationDataParticleKey.PC1,
                                     SBE54tpsConfigurationDataParticleKey.PC2,
                                     SBE54tpsConfigurationDataParticleKey.PC3,
                                     SBE54tpsConfigurationDataParticleKey.PD1,
                                     SBE54tpsConfigurationDataParticleKey.PD2,
                                     SBE54tpsConfigurationDataParticleKey.PT1,
                                     SBE54tpsConfigurationDataParticleKey.PT2,
                                     SBE54tpsConfigurationDataParticleKey.PT3,
                                     SBE54tpsConfigurationDataParticleKey.PT4,
                                     SBE54tpsConfigurationDataParticleKey.PRESSURE_OFFSET,
                                     SBE54tpsConfigurationDataParticleKey.PRESSURE_RANGE]:
                            values[key] = float(val)

        result = []
        for key, value in values.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result


class SBE54tpsEventCounterDataParticleKey(BaseEnum):
    NUMBER_EVENTS = "number_events"
    MAX_STACK = "max_stack"
    DEVICE_TYPE = "device_type"
    SERIAL_NUMBER = "serial_number"
    POWER_ON_RESET = "power_on_reset"
    POWER_FAIL_RESET = "power_fail_reset"
    SERIAL_BYTE_ERROR = "serial_byte_error"
    COMMAND_BUFFER_OVERFLOW = "command_buffer_overflow"
    SERIAL_RECEIVE_OVERFLOW = "serial_receive_overflow"
    LOW_BATTERY = "low_battery"
    SIGNAL_ERROR = "signal_error"
    ERROR_10 = "error_10"
    ERROR_12 = "error_12"


class SBE54tpsEventCounterDataParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.PREST_EVENT_COUNTER

    LINE1 = r"<EventSummary numEvents='(\d+)' maxStack='(\d+)'/>"
    LINE2 = r"<EventList DeviceType='([^']+)' SerialNumber='(\d+)'>"
    LINE3 = r"<Event type='PowerOnReset' count='(\d+)'/>"
    LINE4 = r"<Event type='PowerFailReset' count='(\d+)'/>"
    LINE5 = r"<Event type='SerialByteErr' count='(\d+)'/>"
    LINE6 = r"<Event type='CMDBuffOflow' count='(\d+)'/>"
    LINE7 = r"<Event type='SerialRxOflow' count='(\d+)'/>"
    LINE8 = r"<Event type='LowBattery' count='(\d+)'/>"
    LINE9 = r"<Event type='SignalErr' count='(\d+)'/>"
    LINE10 = r"<Event type='Error10' count='(\d+)'/>"
    LINE11 = r"<Event type='Error12' count='(\d+)'/>"

    def _build_parsed_values(self):
        """
        Take something in the StatusData format and split it into
        values with appropriate tags

        @throws SampleException If there is a problem with sample creation
        """
        values = {
            SBE54tpsEventCounterDataParticleKey.NUMBER_EVENTS: None,
            SBE54tpsEventCounterDataParticleKey.MAX_STACK: None,
            SBE54tpsEventCounterDataParticleKey.DEVICE_TYPE: None,
            SBE54tpsEventCounterDataParticleKey.SERIAL_NUMBER: None,
            SBE54tpsEventCounterDataParticleKey.POWER_ON_RESET: None,
            SBE54tpsEventCounterDataParticleKey.POWER_FAIL_RESET: None,
            SBE54tpsEventCounterDataParticleKey.SERIAL_BYTE_ERROR: None,
            SBE54tpsEventCounterDataParticleKey.COMMAND_BUFFER_OVERFLOW: None,
            SBE54tpsEventCounterDataParticleKey.SERIAL_RECEIVE_OVERFLOW: None,
            SBE54tpsEventCounterDataParticleKey.LOW_BATTERY: None,
            SBE54tpsEventCounterDataParticleKey.SIGNAL_ERROR: None,
            SBE54tpsEventCounterDataParticleKey.ERROR_10: None,
            SBE54tpsEventCounterDataParticleKey.ERROR_12: None
        }

        matchers = {
            re.compile(self.LINE1): [SBE54tpsEventCounterDataParticleKey.NUMBER_EVENTS,
                                     SBE54tpsEventCounterDataParticleKey.MAX_STACK],
            re.compile(self.LINE2): [SBE54tpsEventCounterDataParticleKey.DEVICE_TYPE,
                                     SBE54tpsEventCounterDataParticleKey.SERIAL_NUMBER],
            re.compile(self.LINE3): [SBE54tpsEventCounterDataParticleKey.POWER_ON_RESET],
            re.compile(self.LINE4): [SBE54tpsEventCounterDataParticleKey.POWER_FAIL_RESET],
            re.compile(self.LINE5): [SBE54tpsEventCounterDataParticleKey.SERIAL_BYTE_ERROR],
            re.compile(self.LINE6): [SBE54tpsEventCounterDataParticleKey.COMMAND_BUFFER_OVERFLOW],
            re.compile(self.LINE7): [SBE54tpsEventCounterDataParticleKey.SERIAL_RECEIVE_OVERFLOW],
            re.compile(self.LINE8): [SBE54tpsEventCounterDataParticleKey.LOW_BATTERY],
            re.compile(self.LINE9): [SBE54tpsEventCounterDataParticleKey.SIGNAL_ERROR],
            re.compile(self.LINE10): [SBE54tpsEventCounterDataParticleKey.ERROR_10],
            re.compile(self.LINE11): [SBE54tpsEventCounterDataParticleKey.ERROR_12]
        }

        for line in self.raw_data.split(NEWLINE):
            for matcher, keys in matchers.iteritems():
                match = matcher.match(line)

                if match:
                    for index, key in enumerate(keys):
                        val = match.group(index + 1)

                        if key in [SBE54tpsEventCounterDataParticleKey.DEVICE_TYPE,
                                   SBE54tpsEventCounterDataParticleKey.SERIAL_NUMBER]:
                            values[key] = val

                        elif key in [SBE54tpsEventCounterDataParticleKey.NUMBER_EVENTS,
                                     SBE54tpsEventCounterDataParticleKey.MAX_STACK,
                                     SBE54tpsEventCounterDataParticleKey.POWER_ON_RESET,
                                     SBE54tpsEventCounterDataParticleKey.POWER_FAIL_RESET,
                                     SBE54tpsEventCounterDataParticleKey.SERIAL_BYTE_ERROR,
                                     SBE54tpsEventCounterDataParticleKey.COMMAND_BUFFER_OVERFLOW,
                                     SBE54tpsEventCounterDataParticleKey.SERIAL_RECEIVE_OVERFLOW,
                                     SBE54tpsEventCounterDataParticleKey.LOW_BATTERY,
                                     SBE54tpsEventCounterDataParticleKey.SIGNAL_ERROR,
                                     SBE54tpsEventCounterDataParticleKey.ERROR_10,
                                     SBE54tpsEventCounterDataParticleKey.ERROR_12]:
                            values[key] = int(val)

        result = []
        for key, value in values.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result


class SBE54tpsHardwareDataParticleKey(BaseEnum):
    DEVICE_TYPE = "device_type"
    SERIAL_NUMBER = "serial_number"
    MANUFACTURER = "manufacturer"
    FIRMWARE_VERSION = "firmware_version"
    FIRMWARE_DATE = "firmware_date"
    HARDWARE_VERSION = "hardware_version"
    PCB_SERIAL_NUMBER = "pcb_serial_number"
    PCB_TYPE = "pcb_type"
    MANUFACTURE_DATE = "manufacture_date"


class SBE54tpsHardwareDataParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.PREST_HARDWARE_DATA
    
    LINE1 = r"<HardwareData DeviceType='([^']+)' SerialNumber='(\d+)'>"
    LINE2 = r"<Manufacturer>([^<]+)</Manufacturer>"
    LINE3 = r"<FirmwareVersion>([^<]+)</FirmwareVersion>"
    LINE4 = r"<FirmwareDate>([^<]+)</FirmwareDate>"
    LINE5 = r"<HardwareVersion>([^<]+)</HardwareVersion>"
    LINE6 = r"<PCBSerialNum>([^<]+)</PCBSerialNum>"
    LINE7 = r"<PCBType>([^<]+)</PCBType>"
    LINE8 = r"<MfgDate>([^<]+)</MfgDate>"

    def _build_parsed_values(self):
        """
        Take something in the StatusData format and split it into
        values with appropriate tags

        @throws SampleException If there is a problem with sample creation
        """
        arrays = [SBE54tpsHardwareDataParticleKey.HARDWARE_VERSION,
                  SBE54tpsHardwareDataParticleKey.PCB_SERIAL_NUMBER]

        matchers = {
            re.compile(self.LINE1): [SBE54tpsHardwareDataParticleKey.DEVICE_TYPE,
                                     SBE54tpsHardwareDataParticleKey.SERIAL_NUMBER],
            re.compile(self.LINE2): [SBE54tpsHardwareDataParticleKey.MANUFACTURER],
            re.compile(self.LINE3): [SBE54tpsHardwareDataParticleKey.FIRMWARE_VERSION],
            re.compile(self.LINE4): [SBE54tpsHardwareDataParticleKey.FIRMWARE_DATE],
            re.compile(self.LINE5): [SBE54tpsHardwareDataParticleKey.HARDWARE_VERSION],
            re.compile(self.LINE6): [SBE54tpsHardwareDataParticleKey.PCB_SERIAL_NUMBER],
            re.compile(self.LINE7): [SBE54tpsHardwareDataParticleKey.PCB_TYPE],
            re.compile(self.LINE8): [SBE54tpsHardwareDataParticleKey.MANUFACTURE_DATE]
        }

        values = {}
        result = []

        for line in self.raw_data.split(NEWLINE):
            for matcher, keys in matchers.iteritems():
                match = matcher.match(line)

                if match:
                    for index, key in enumerate(keys):
                        val = match.group(index + 1)

                        if key in arrays:
                            values.setdefault(key, []).append(val)
                        else:
                            values[key] = val

        for key, val in values.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: val})

        return result


class SBE54tpsSampleDataParticleKey(BaseEnum):
    SAMPLE_NUMBER = "sample_number"
    SAMPLE_TYPE = "sample_type"
    INST_TIME = "date_time_string"
    PRESSURE = "absolute_pressure"    # psi
    PRESSURE_TEMP = "pressure_temp"


class SBE54tpsSampleDataParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.PREST_REAL_TIME

    LINE1 = r"<Sample Num='(\d+)' Type='([^']+)'>"
    LINE2 = r"<Time>([^<]+)</Time>"
    LINE3 = r"<PressurePSI>([0-9.+-]+)</PressurePSI>"
    LINE4 = r"<PTemp>([0-9.+-]+)</PTemp>"

    def _build_parsed_values(self):
        """
        Take something in the StatusData format and split it into
        values with appropriate tags

        @throws SampleException If there is a problem with sample creation
        """

        values = {
            SBE54tpsSampleDataParticleKey.SAMPLE_NUMBER: None,
            SBE54tpsSampleDataParticleKey.SAMPLE_TYPE: None,
            SBE54tpsSampleDataParticleKey.INST_TIME: None,
            SBE54tpsSampleDataParticleKey.PRESSURE: None,
            SBE54tpsSampleDataParticleKey.PRESSURE_TEMP: None
        }

        matchers = {
            re.compile(self.LINE1): [SBE54tpsSampleDataParticleKey.SAMPLE_NUMBER,
                                     SBE54tpsSampleDataParticleKey.SAMPLE_TYPE],
            re.compile(self.LINE2): [SBE54tpsSampleDataParticleKey.INST_TIME],
            re.compile(self.LINE3): [SBE54tpsSampleDataParticleKey.PRESSURE],
            re.compile(self.LINE4): [SBE54tpsSampleDataParticleKey.PRESSURE_TEMP]
        }

        for line in self.raw_data.split(NEWLINE):
            for matcher, keys in matchers.iteritems():
                match = matcher.match(line)

                if match:
                    for index, key in enumerate(keys):
                        val = match.group(index + 1)

                        if key in [SBE54tpsSampleDataParticleKey.SAMPLE_TYPE]:
                            values[key] = val

                        elif key in [SBE54tpsSampleDataParticleKey.SAMPLE_NUMBER]:
                            values[key] = int(val)

                        elif key in [SBE54tpsSampleDataParticleKey.PRESSURE,
                                     SBE54tpsSampleDataParticleKey.PRESSURE_TEMP]:
                            values[key] = float(val)

                        elif key in [SBE54tpsSampleDataParticleKey.INST_TIME]:
                            # <Time>2012-11-07T12:21:25</Time>
                            # yyyy-mm-ddThh:mm:ss
                            py_timestamp = time.strptime(val, "%Y-%m-%dT%H:%M:%S")
                            self.set_internal_timestamp(unix_time=timegm_to_float(py_timestamp))
                            values[key] = val

        result = []
        for key, value in values.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result


class SBE54tpsSampleRefOscDataParticleKey(BaseEnum):
    SET_TIMEOUT = "set_timeout"
    SET_TIMEOUT_MAX = "set_timeout_max"
    SET_TIMEOUT_ICD = "set_timeout_icd"
    SAMPLE_NUMBER = "sample_number"
    SAMPLE_TYPE = "sample_type"
    SAMPLE_TIMESTAMP = "date_time_string"
    REF_OSC_FREQ = "reference_oscillator_freq"
    PCB_TEMP_RAW = "pcb_thermistor_value"
    REF_ERROR_PPM = "reference_error"


class SBE54tpsSampleRefOscDataParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.PREST_REFERENCE_OSCILLATOR

    LINE1 = r"<SetTimeout>([^<]+)</SetTimeout>"
    LINE2 = r"<SetTimeoutMax>([^<]+)</SetTimeoutMax>"
    LINE3 = r"<SetTimeoutICD>([^<]+)</SetTimeoutICD>"
    LINE4 = r"<Sample Num='([^']+)' Type='([^']+)'>"
    LINE5 = r"<Time>([^<]+)</Time>"
    LINE6 = r"<RefOscFreq>([0-9.+-]+)</RefOscFreq>"
    LINE7 = r"<PCBTempRaw>([0-9.+-]+)</PCBTempRaw>"
    LINE8 = r"<RefErrorPPM>([0-9.+-]+)</RefErrorPPM>"

    def _build_parsed_values(self):
        """
        Take something in the StatusData format and split it into
        values with appropriate tags

        @throws SampleException If there is a problem with sample creation
        """

        values = {
            SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT: None,
            SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_MAX: None,
            SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_ICD: None,
            SBE54tpsSampleRefOscDataParticleKey.SAMPLE_NUMBER: None,
            SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TYPE: None,
            SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TIMESTAMP: None,
            SBE54tpsSampleRefOscDataParticleKey.REF_OSC_FREQ: None,
            SBE54tpsSampleRefOscDataParticleKey.PCB_TEMP_RAW: None,
            SBE54tpsSampleRefOscDataParticleKey.REF_ERROR_PPM: None
        }

        matchers = {
            re.compile(self.LINE1): [SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT],
            re.compile(self.LINE2): [SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_MAX],
            re.compile(self.LINE3): [SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_ICD],
            re.compile(self.LINE4): [SBE54tpsSampleRefOscDataParticleKey.SAMPLE_NUMBER,
                                     SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TYPE],
            re.compile(self.LINE5): [SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TIMESTAMP],
            re.compile(self.LINE6): [SBE54tpsSampleRefOscDataParticleKey.REF_OSC_FREQ],
            re.compile(self.LINE7): [SBE54tpsSampleRefOscDataParticleKey.PCB_TEMP_RAW],
            re.compile(self.LINE8): [SBE54tpsSampleRefOscDataParticleKey.REF_ERROR_PPM]
        }

        for line in self.raw_data.split(NEWLINE):
            for matcher, keys in matchers.iteritems():
                match = matcher.match(line)

                if match:
                    for index, key in enumerate(keys):
                        val = match.group(index + 1)

                        if key in [SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TYPE]:
                            values[key] = val

                        elif key in [SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT,
                                     SBE54tpsSampleRefOscDataParticleKey.SAMPLE_NUMBER,
                                     SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_MAX,
                                     SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_ICD,
                                     SBE54tpsSampleRefOscDataParticleKey.PCB_TEMP_RAW]:
                            if key == SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_MAX and val.lower() == 'off':
                                val = 0
                            values[key] = int(val)

                        elif key in [SBE54tpsSampleRefOscDataParticleKey.REF_OSC_FREQ,
                                     SBE54tpsSampleRefOscDataParticleKey.REF_ERROR_PPM]:
                            values[key] = float(val)

                        elif key in [SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TIMESTAMP]:
                            # <Time>2012-11-07T12:21:25</Time>
                            # yyyy-mm-ddThh:mm:ss
                            values[key] = val
                            py_timestamp = time.strptime(val, "%Y-%m-%dT%H:%M:%S")
                            self.set_internal_timestamp(unix_time=timegm_to_float(py_timestamp))

        result = []
        for key, value in values.iteritems():
            result.append({DataParticleKey.VALUE_ID: key,
                           DataParticleKey.VALUE: value})

        return result
######################################### /PARTICLES #############################


###############################################################################
# Driver
###############################################################################
class SBE54PlusInstrumentDriver(SeaBirdInstrumentDriver):
    """
    SBEInstrumentDriver subclass
    Subclasses Seabird driver with connection state
    machine.
    """

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
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


###############################################################################
# Protocol
################################################################################
class Protocol(SeaBirdProtocol):
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
        SeaBirdProtocol.__init__(self, prompts, newline, driver_event)

        # Build protocol state machine.
        self._protocol_fsm = InstrumentFSM(ProtocolState, ProtocolEvent,
            ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.EXIT, self._handler_unknown_exit)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.START_DIRECT, self._handler_command_start_direct)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXIT, self._handler_command_exit)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.RECOVER_AUTOSAMPLE, self._handler_command_recover_autosample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET, self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.CLOCK_SYNC, self._handler_command_clock_sync)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_command_clock_sync)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SCHEDULED_ACQUIRE_STATUS, self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_DIRECT, self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR, self._handler_command_sample_ref_osc)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.TEST_EEPROM, self._handler_command_test_eeprom)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.INIT_PARAMS, self._handler_command_init_params)

        self._protocol_fsm.add_handler(ProtocolState.OSCILLATOR, ProtocolEvent.ENTER, self._handler_oscillator_enter)
        self._protocol_fsm.add_handler(ProtocolState.OSCILLATOR, ProtocolEvent.ACQUIRE_OSCILLATOR_SAMPLE, self._handler_oscillator_acquire_sample)
        self._protocol_fsm.add_handler(ProtocolState.OSCILLATOR, ProtocolEvent.EXIT, self._handler_oscillator_exit)

        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.SCHEDULED_ACQUIRE_STATUS, self._handler_autosample_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_autosample_clock_sync)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ENTER, self._handler_autosample_enter)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXIT, self._handler_autosample_exit)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.INIT_PARAMS, self._handler_autosample_init_params)

        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.ENTER, self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXIT, self._handler_direct_access_exit)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct)

        # Build dictionaries for driver schema
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        # Add build handlers for device commands.
        self._add_build_handler(InstrumentCmds.SET, self._build_set_command)
        self._add_build_handler(InstrumentCmds.GET_CONFIGURATION_DATA, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.GET_STATUS_DATA, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.GET_EVENT_COUNTER_DATA, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.GET_HARDWARE_DATA, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.START_LOGGING, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.STOP_LOGGING, self._build_simple_command)
        self._add_build_handler(InstrumentCmds.SAMPLE_REFERENCE_OSCILLATOR,  self._build_simple_command)
        self._add_build_handler(InstrumentCmds.TEST_EEPROM, self._build_simple_command)

        # Add response handlers for device commands.
        self._add_response_handler(InstrumentCmds.SET, self._parse_set_response)
        self._add_response_handler(InstrumentCmds.GET_CONFIGURATION_DATA, self._parse_generic_response)
        self._add_response_handler(InstrumentCmds.GET_STATUS_DATA,self._parse_generic_response)
        self._add_response_handler(InstrumentCmds.GET_EVENT_COUNTER_DATA, self._parse_generic_response)
        self._add_response_handler(InstrumentCmds.GET_HARDWARE_DATA, self._parse_generic_response)
        self._add_response_handler(InstrumentCmds.SAMPLE_REFERENCE_OSCILLATOR, self._parse_sample_ref_osc)
        self._add_response_handler(InstrumentCmds.TEST_EEPROM, self._parse_test_eeprom)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        self._add_scheduler_event(ScheduledJob.ACQUIRE_STATUS, ProtocolEvent.ACQUIRE_STATUS)
        self._add_scheduler_event(ScheduledJob.CLOCK_SYNC, ProtocolEvent.SCHEDULED_CLOCK_SYNC)

        # commands sent sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

        self._chunker = StringChunker(Protocol.sieve_function)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        """
        return_list = []

        sieve_matchers = [STATUS_DATA_REGEX_MATCHER,
                          CONFIGURATION_DATA_REGEX_MATCHER,
                          EVENT_COUNTER_DATA_REGEX_MATCHER,
                          HARDWARE_DATA_REGEX_MATCHER,
                          SAMPLE_DATA_REGEX_MATCHER,
                          ENGINEERING_DATA_MATCHER,
                          RECOVER_AUTOSAMPLE_MATCHER]

        for matcher in sieve_matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        # This instrument will automatically put itself back into autosample mode after a couple minutes idle
        # in command mode.  If a message is seen, figure out if an event to needs to be raised to adjust
        # the state machine.
        if RECOVER_AUTOSAMPLE_MATCHER.match(chunk) and self._protocol_fsm.get_current_state() == ProtocolState.COMMAND:
            log.debug("FSM state out of date.  Recovering to autosample!")
            self._async_raise_fsm_event(ProtocolEvent.RECOVER_AUTOSAMPLE)

        if self._extract_sample(SBE54tpsSampleDataParticle, SAMPLE_DATA_REGEX_MATCHER, chunk, timestamp): return
        if self._extract_sample(SBE54tpsStatusDataParticle, STATUS_DATA_REGEX_MATCHER, chunk, timestamp): return
        if self._extract_sample(SBE54tpsConfigurationDataParticle, CONFIGURATION_DATA_REGEX_MATCHER, chunk, timestamp): return
        if self._extract_sample(SBE54tpsEventCounterDataParticle, EVENT_COUNTER_DATA_REGEX_MATCHER, chunk, timestamp): return
        if self._extract_sample(SBE54tpsHardwareDataParticle, HARDWARE_DATA_REGEX_MATCHER, chunk, timestamp): return
        if self._extract_sample(SBE54tpsSampleRefOscDataParticle, SAMPLE_REF_OSC_MATCHER, chunk, timestamp): return

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        events_out = [x for x in events if Capability.has(x)]
        return events_out

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _build_command_dict(self):
        """
        Populate the command dictionary with command.
        """
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name="Acquire Status")
        self._cmd_dict.add(Capability.CLOCK_SYNC, display_name="Synchronize Clock")
        self._cmd_dict.add(Capability.SAMPLE_REFERENCE_OSCILLATOR, display_name="Sample Reference Oscillator")
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name="Start Autosample")
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, display_name="Stop Autosample")
        self._cmd_dict.add(Capability.TEST_EEPROM, display_name="Test EEPROM")
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')

    def _send_wakeup(self):
        pass

    def _wakeup(self, timeout, delay=1):
        pass

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
        Discover current state; always AUTOSAMPLE.
        """
        return ProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING

    def _handler_unknown_exit(self, *args, **kwargs):
        """
        Exit unknown state.
        """
        pass

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Run all Get?? commands.  Concat command results and return
        @param args:
        @param kwargs:
        @return:
        """
        timeout = kwargs.get('timeout', TIMEOUT)

        next_state = None
        next_agent_state = None
        result1 = self._do_cmd_resp(InstrumentCmds.GET_CONFIGURATION_DATA, timeout=timeout)
        result2 = self._do_cmd_resp(InstrumentCmds.GET_STATUS_DATA, timeout=timeout)
        result3 = self._do_cmd_resp(InstrumentCmds.GET_EVENT_COUNTER_DATA, timeout=timeout)
        result4 = self._do_cmd_resp(InstrumentCmds.GET_HARDWARE_DATA, timeout=timeout)

        result = result1 + result2 + result3 + result4

        return next_state, (next_agent_state, result)

    def _handler_command_start_direct(self, *args, **kwargs):
        return ProtocolState.DIRECT_ACCESS, (ResourceAgentState.DIRECT_ACCESS, None)

    def _handler_command_recover_autosample(self, *args, **kwargs):
        """
        Reenter autosample mode.  Used when our data handler detects
        as data sample.
        @retval (next_state, result) tuple, (None, sample dict).
        """
        self._async_agent_state_change(ResourceAgentState.STREAMING)

        return ProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        pass

    def _handler_command_test_eeprom(self, *args, **kwargs):

        kwargs['expected_prompt'] = GENERIC_PROMPT
        kwargs['timeout'] = LONG_TIMEOUT
        result = self._do_cmd_resp(InstrumentCmds.TEST_EEPROM, *args, **kwargs)

        return None, (None, result)

    def _handler_command_sample_ref_osc(self, *args, **kwargs):

        #Transition to a separate state to allow the instrument enough time to acquire a sample
        result = None

        next_state = ProtocolState.OSCILLATOR

        next_agent_state = ResourceAgentState.BUSY
        return next_state, (next_agent_state, result)

    def _handler_oscillator_enter(self, *args, **kwargs):
        """
        Enter the Oscillator state
        """
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        self._async_raise_fsm_event(ProtocolEvent.ACQUIRE_OSCILLATOR_SAMPLE)

    def _handler_oscillator_acquire_sample(self, *args, **kwargs):

        result = None
        kwargs['expected_prompt'] = "</Sample>"
        kwargs['timeout'] = LONG_TIMEOUT

        try:
            result = self._do_cmd_resp(InstrumentCmds.SAMPLE_REFERENCE_OSCILLATOR, *args, **kwargs)
        except InstrumentException as e:
            log.error("Exception occurred when trying to acquire Reference Oscillator Sample: %s" % e)

        return ProtocolState.COMMAND, (ResourceAgentState.COMMAND, result)

    def _handler_oscillator_exit(self, *args, **kwargs):
        """
        Exit the Oscillator state
        """
        pass

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change
        @retval (next_state, result) tuple, (None, (None, )) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        result = self._do_cmd_resp(InstrumentCmds.SET, Parameter.TIME, get_timestamp_delayed("%Y-%m-%dT%H:%M:%S"), **kwargs)

        return None, (None, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @retval (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        kwargs['expected_prompt'] = Prompt.COMMAND
        kwargs['timeout'] = 30
        log.info("SYNCING TIME WITH SENSOR")
        self._do_cmd_resp(InstrumentCmds.SET, Parameter.TIME, get_timestamp_delayed("%Y-%m-%dT%H:%M:%S"), **kwargs)

        # Issue start command and switch to autosample if successful.
        self._do_cmd_no_resp(InstrumentCmds.START_LOGGING, *args, **kwargs)

        return ProtocolState.AUTOSAMPLE, (ResourceAgentState.STREAMING, None)

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
            self._stop_logging()

            # Sync the clock
            result = self._do_cmd_resp(InstrumentCmds.SET, Parameter.TIME, get_timestamp_delayed("%Y-%m-%dT%H:%M:%S"), **kwargs)

        finally:
            # Switch back to streaming
            self._start_logging()

        return None, (None, result)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        Run all status commands.  Concat command results and return
        @param args:
        @param kwargs:
        @return:
        """

        try:
            # Switch to command mode
            self._stop_logging()

            timeout = kwargs.get('timeout', TIMEOUT)
            result1 = self._do_cmd_resp(InstrumentCmds.GET_CONFIGURATION_DATA, timeout=timeout)
            result2 = self._do_cmd_resp(InstrumentCmds.GET_STATUS_DATA, timeout=timeout)
            result3 = self._do_cmd_resp(InstrumentCmds.GET_EVENT_COUNTER_DATA, timeout=timeout)
            result4 = self._do_cmd_resp(InstrumentCmds.GET_HARDWARE_DATA, timeout=timeout)

            result = result1 + result2 + result3 + result4

        finally:
            # Switch back to streaming
            self._start_logging()

        return None, (None, result)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        """
        self._do_cmd_resp(InstrumentCmds.STOP_LOGGING, *args, **kwargs)

        return ProtocolState.COMMAND, (ResourceAgentState.COMMAND, None)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
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
        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return None, (None, None)

    def _handler_direct_access_stop_direct(self):
        """
        @throw InstrumentProtocolException on invalid command
        """
        return ProtocolState.AUTOSAMPLE, (ResourceAgentState.STREAMING, None)

    def _handler_direct_access_exit(self, *args, **kwargs):
        """
        Exit direct access state.
        """
        pass

    ########################################################################
    # Response Parsers
    ########################################################################
    def _parse_set_response(self, response, prompt):
        """
        Parse handler for set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if set command misunderstood.
        """
        if 'Error' in response:
            raise InstrumentParameterException('Protocol._parse_set_response : Set command not recognized: %s' % response)

    def _parse_generic_response(self, response, prompt):

        response = response.replace("S>" + NEWLINE, "")
        response = response.replace("<Executed/>" + NEWLINE, "")
        response = response.replace("S>", "")

        return response


    def _parse_test_eeprom(self, response, prompt):
        """
        @return: True or False
        """
        if prompt != GENERIC_PROMPT:
            raise InstrumentProtocolException('TEST_EEPROM command not recognized: %s' % response)

        if "PASSED" in response:
            return True
        return False

    def _parse_sample_ref_osc(self, response, prompt):

        if not SAMPLE_REF_OSC_MATCHER.search(response):
            log.error("Unexpected reply received from instrument in response to sample reference oscillator command.")

        return response

    ########################################################################
    # Response Parsers
    ########################################################################
    def _build_set_command(self, cmd, param, val):
        """
        Build handler for set commands. SETparam=val followed by newline.
        String val constructed by param dict formatting function.  <--- needs a better/clearer way
        @param param the parameter key to set.
        @param val the parameter value to set.
        @ retval The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """
        try:
            str_val = self._param_dict.format(param, val)
            if str_val is None:
                raise InstrumentParameterException("Driver PARAM was None!!!!")
            set_cmd = 'set%s=%s%s' % (param, str_val, NEWLINE)
        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter %s' % param)

        return set_cmd

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)

        for (key, val) in params.iteritems():
            log.debug("KEY = " + str(key) + " VALUE = " + str(val))
            self._do_cmd_resp(InstrumentCmds.SET, key, val, **kwargs)

        self._update_params()

    def apply_startup_params(self):

        log.debug("CURRENT STATE: %s", self.get_current_state())
        if (self.get_current_state() != DriverProtocolState.COMMAND and
                    self.get_current_state() != DriverProtocolState.AUTOSAMPLE):
            raise InstrumentProtocolException("Not in command or autosample state. Unable to apply startup params")

        # If we are in streaming mode and our configuration on the
        # instrument matches what we think it should be then we
        # don't need to do anything.
        if self._instrument_config_dirty():
            self._apply_params()


    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary. Wake the device then issue
        display status and display calibration commands. The parameter
        dict will match line output and update itself.
        @throws InstrumentTimeoutException if device cannot be timely woken.
        @throws InstrumentProtocolException if ds/dc misunderstood.
        """
        # Get old param dict config.
        old_config = self._param_dict.get_config()

        # Issue display commands and parse results.
        timeout = kwargs.get('timeout', TIMEOUT)

        log.debug("Run status command: %s" % InstrumentCmds.GET_STATUS_DATA)
        response = self._do_cmd_resp(InstrumentCmds.GET_STATUS_DATA, timeout=timeout)
        for line in response.split(NEWLINE):
            self._param_dict.update(line)
        log.debug("status command response: %r" % response)

        log.debug("Run configure command: %s" % InstrumentCmds.GET_CONFIGURATION_DATA)
        response = self._do_cmd_resp(InstrumentCmds.GET_CONFIGURATION_DATA, timeout=timeout)
        for line in response.split(NEWLINE):
            self._param_dict.update(line)
        log.debug("configure command response: %r" % response)

        # Get new param dict config. If it differs from the old config,
        # tell driver superclass to publish a config change event.
        new_config = self._param_dict.get_config()
        log.debug("new_config: %s == old_config: %s" % (new_config, old_config))
        if not dict_equal(old_config, new_config, ignore_keys=Parameter.TIME):
            log.debug("configuration has changed.  Send driver event")
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)


    ########################################################################
    # Private helpers.
    ########################################################################
    def _start_logging(self, timeout=TIMEOUT):
        """
        Command the instrument to start logging
        @param timeout: how long to wait for a prompt
        @return: True if successful
        @raise: InstrumentProtocolException if failed to start logging
        """
        self._do_cmd_no_resp(InstrumentCmds.START_LOGGING)
        return True

    def _stop_logging(self):

        self._do_cmd_resp(InstrumentCmds.STOP_LOGGING)
        return True

    @staticmethod
    def _bool_to_int_string(v):
        # return a string of 1 or 0 to indicate true/false
        if v:
            return "1"
        return "0"

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        self._param_dict.add(Parameter.TIME,
                             SBE54tpsStatusDataParticle.LINE2,
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             expiration=0,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name="Instrument Time",
                             description="Timestamp of last clock sync.",
                             units="Y-M-DTH:M:S")

        self._param_dict.add(Parameter.SAMPLE_PERIOD,
                             SBE54tpsConfigurationDataParticle.LINE28,
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Sample Period",
                             description="Duration of each pressure measurement.",
                             units=Units.SECOND,
                             default_value=15,
                             startup_param=True,
                             direct_access=True)

        self._param_dict.add(Parameter.BATTERY_TYPE,
                             SBE54tpsConfigurationDataParticle.LINE24,
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Battery Type",
                             description="Battery type: (0:lithium | 1:alkaline) ",
                             default_value=1,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True)

        self._param_dict.add(Parameter.ENABLE_ALERTS,
                             SBE54tpsConfigurationDataParticle.LINE26,
                             lambda match: bool(int(match.group(1))),
                             self._bool_to_int_string,
                             type=ParameterDictType.BOOL,
                             display_name="Enable Alerts",
                             description="Enable output of alerts (true | false)",
                             default_value=1,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True)
