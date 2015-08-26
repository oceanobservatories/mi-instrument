"""
@package mi.instrument.rbr.xr-420_thermistor_24.ooicore.driver
@file /Users/Bill/WorkSpace/marine-integrations/mi/instrument/rbr/xr-420_thermistor_24/ooicore/driver.py
@author Bill Bollenbacher
@brief Driver for the RBR Thermistor String (24 thermistors)
Release notes:

initial release
"""

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'


import time
import calendar
import re
import ntplib
import struct

from mi.core.common import BaseEnum, Units
from mi.core.time_tools import get_timestamp_delayed
from mi.core.time_tools import timegm_to_float
from mi.core.instrument.instrument_driver import DriverParameter

from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.exceptions import InstrumentTimeoutException, InstrumentParameterException, InstrumentProtocolException, \
    SampleException, InstrumentCommandException
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticle, DataParticleKey, CommonDataParticleType
from mi.core.instrument.instrument_driver import ResourceAgentState

from mi.core.log import get_logger, get_logging_metaclass
log = get_logger()

SAMPLE_DATA_PATTERN = (r'TIM (\d+)' +           # timestamp
                       '\s+(-*\d+\.\d+)' +      # Channel 1
                       '\s+(-*\d+\.\d+)' +      # Channel 2
                       '\s+(-*\d+\.\d+)' +      # Channel 3
                       '\s+(-*\d+\.\d+)' +      # Channel 4
                       '\s+(-*\d+\.\d+)' +      # Channel 5
                       '\s+(-*\d+\.\d+)' +      # Channel 6
                       '\s+(-*\d+\.\d+)' +      # Channel 7
                       '\s+(-*\d+\.\d+)' +      # Channel 8
                       '\s+(-*\d+\.\d+)' +      # Channel 9
                       '\s+(-*\d+\.\d+)' +      # Channel 10
                       '\s+(-*\d+\.\d+)' +      # Channel 11
                       '\s+(-*\d+\.\d+)' +      # Channel 12
                       '\s+(-*\d+\.\d+)' +      # Channel 13
                       '\s+(-*\d+\.\d+)' +      # Channel 14
                       '\s+(-*\d+\.\d+)' +      # Channel 15
                       '\s+(-*\d+\.\d+)' +      # Channel 16
                       '\s+(-*\d+\.\d+)' +      # Channel 17
                       '\s+(-*\d+\.\d+)' +      # Channel 18
                       '\s+(-*\d+\.\d+)' +      # Channel 19
                       '\s+(-*\d+\.\d+)' +      # Channel 20
                       '\s+(-*\d+\.\d+)' +      # Channel 21
                       '\s+(-*\d+\.\d+)' +      # Channel 22
                       '\s+(-*\d+\.\d+)' +      # Channel 23
                       '\s+(-*\d+\.\d+)' +      # Channel 24
                       '\s+BV: (-*\d+\.\d+)' +  # battery voltage
                       '\s+SN: (\d+) FET')      # serial number


SAMPLE_DATA_REGEX = re.compile(SAMPLE_DATA_PATTERN)


class ScheduledJob(BaseEnum):
    ACQUIRE_STATUS = 'acquire_status'
    CLOCK_SYNC = 'clock_sync'


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    SAMPLE = 'tmpsf_sample'
    ENGINEERING = 'tmpsf_engineering'


INSTRUMENT_NEWLINE = '\r\n'
WRITE_DELAY = 0

# default timeout.
INSTRUMENT_TIMEOUT = 5


# Device responses.
class InstrumentResponses(BaseEnum):
    """
    XR-420 responses.
    """
    GET_STATUS = 'Logger status '
    GET_IDENTIFICATION = 'RBR XR-420 '
    GET_LOGGER_DATE_AND_TIME = 'CTD\r\n'
    GET_SAMPLE_INTERVAL = 'CSP\r\n'
    GET_START_DATE_AND_TIME = 'CST\r\n'
    GET_END_DATE_AND_TIME = 'CET\r\n'
    GET_BATTERY_VOLTAGE = 'BAT\r\n'
    GET_CHANNEL_CALIBRATION = 'CAL\r\n'
    GET_ADVANCED_FUNCTIONS = 'STC\r\n'
    UNKNOWN_COMMAND = '? Unknown command \r\n'
    START_SAMPLING = 'Logger started in mode '

        
class InstrumentCmds(BaseEnum):   
    GET_IDENTIFICATION = 'A'
    GET_LOGGER_DATE_AND_TIME = 'B'
    GET_SAMPLE_INTERVAL = 'C'
    GET_START_DATE_AND_TIME = 'D'
    GET_END_DATE_AND_TIME = 'E'
    GET_STATUS = 'T'
    GET_CHANNEL_CALIBRATION = 'Z'
    GET_BATTERY_VOLTAGE = '!D'
    SET_LOGGER_DATE_AND_TIME = 'J'
    SET_SAMPLE_INTERVAL = 'K'
    SET_START_DATE_AND_TIME = 'L'
    SET_END_DATE_AND_TIME = 'M'
    TAKE_SAMPLE_IMMEDIATELY = 'F'
    RESET_SAMPLING_ERASE_FLASH = 'N'
    START_SAMPLING = 'P'
    STOP_SAMPLING = '!9'
    SUSPEND_SAMPLING = '!S'
    RESUME_SAMPLING = '!R'
    SET_ADVANCED_FUNCTIONS = '!1'
    GET_ADVANCED_FUNCTIONS = '!2'


class ProtocolStates(BaseEnum):
    """
    Protocol states for XR-420. Cherry picked from DriverProtocolState enum.
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS

    
class ProtocolEvent(BaseEnum):
    """
    Protocol events for XR-420. Cherry picked from DriverEvent enum.
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
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    SCHEDULED_CLOCK_SYNC = DriverEvent.SCHEDULED_CLOCK_SYNC
    SCHEDULED_ACQUIRE_STATUS = 'PROTOCOL_EVENT_SCHEDULED_ACQUIRE_STATUS'


class Capability(BaseEnum):
    """
    Capabilities that are exposed to the user (subset of above)
    """
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = ProtocolEvent.CLOCK_SYNC
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    DISCOVER = DriverEvent.DISCOVER


# Device specific parameters.
class InstrumentParameters(DriverParameter):
    """
    Device parameters for XR-420.
    """
    # main menu parameters
    IDENTIFICATION = 'identification'
    LOGGER_DATE_AND_TIME = 'logger_date_and_time'
    SAMPLE_INTERVAL = 'sample_interval'
    START_DATE_AND_TIME = 'start_date_and_time'
    END_DATE_AND_TIME = 'end_date_and_time'
    STATUS = 'status'
    BATTERY_VOLTAGE = 'battery_voltage'
    POWER_ALWAYS_ON = 'power_always_on'
    SIX_HZ_PROFILING_MODE = 'six_hz_profiling_mode'
    OUTPUT_INCLUDES_SERIAL_NUMBER = 'output_includes_serial_number'
    OUTPUT_INCLUDES_BATTERY_VOLTAGE = 'output_includes_battery_voltage'
    SAMPLING_LED = 'sampling_led'
    ENGINEERING_UNITS_OUTPUT = 'engineering_units_output'
    AUTO_RUN = 'auto_run'
    INHIBIT_DATA_STORAGE = 'inhibit_data_storage'
    CALIBRATION_COEFFICIENTS_CHANNEL_1 = 'calibration_coefficients_channel_1'
    CALIBRATION_COEFFICIENTS_CHANNEL_2 = 'calibration_coefficients_channel_2'
    CALIBRATION_COEFFICIENTS_CHANNEL_3 = 'calibration_coefficients_channel_3'
    CALIBRATION_COEFFICIENTS_CHANNEL_4 = 'calibration_coefficients_channel_4'
    CALIBRATION_COEFFICIENTS_CHANNEL_5 = 'calibration_coefficients_channel_5'
    CALIBRATION_COEFFICIENTS_CHANNEL_6 = 'calibration_coefficients_channel_6'
    CALIBRATION_COEFFICIENTS_CHANNEL_7 = 'calibration_coefficients_channel_7'
    CALIBRATION_COEFFICIENTS_CHANNEL_8 = 'calibration_coefficients_channel_8'
    CALIBRATION_COEFFICIENTS_CHANNEL_9 = 'calibration_coefficients_channel_9'
    CALIBRATION_COEFFICIENTS_CHANNEL_10 = 'calibration_coefficients_channel_10'
    CALIBRATION_COEFFICIENTS_CHANNEL_11 = 'calibration_coefficients_channel_11'
    CALIBRATION_COEFFICIENTS_CHANNEL_12 = 'calibration_coefficients_channel_12'
    CALIBRATION_COEFFICIENTS_CHANNEL_13 = 'calibration_coefficients_channel_13'
    CALIBRATION_COEFFICIENTS_CHANNEL_14 = 'calibration_coefficients_channel_14'
    CALIBRATION_COEFFICIENTS_CHANNEL_15 = 'calibration_coefficients_channel_15'
    CALIBRATION_COEFFICIENTS_CHANNEL_16 = 'calibration_coefficients_channel_16'
    CALIBRATION_COEFFICIENTS_CHANNEL_17 = 'calibration_coefficients_channel_17'
    CALIBRATION_COEFFICIENTS_CHANNEL_18 = 'calibration_coefficients_channel_18'
    CALIBRATION_COEFFICIENTS_CHANNEL_19 = 'calibration_coefficients_channel_19'
    CALIBRATION_COEFFICIENTS_CHANNEL_20 = 'calibration_coefficients_channel_20'
    CALIBRATION_COEFFICIENTS_CHANNEL_21 = 'calibration_coefficients_channel_21'
    CALIBRATION_COEFFICIENTS_CHANNEL_22 = 'calibration_coefficients_channel_22'
    CALIBRATION_COEFFICIENTS_CHANNEL_23 = 'calibration_coefficients_channel_23'
    CALIBRATION_COEFFICIENTS_CHANNEL_24 = 'calibration_coefficients_channel_24'


class Status(BaseEnum):
    NOT_ENABLED_FOR_SAMPLING = 0x00
    ENABLED_SAMPLING_NOT_STARTED = 0x01
    STARTED_SAMPLING = 0x02
    STOPPED_SAMPLING = 0x04
    TEMPORARILY_SUSPENDED_SAMPLING = 0x05
    HIGH_SPEED_PROFILING_MODE = 0x06
    ERASING_DATA_MEMORY = 0x7F
    DATA_MEMORY_ERASE_FAILED = 0x80
    PASSED_END_TIME = 0x01
    RCVD_STOP_COMMAND = 0x02
    DATA_MEMORY_FULL = 0x03
    CONFIGURATION_ERROR = 0x05


class AdvancedFunctionsParameters(BaseEnum):
    POWER_ALWAYS_ON                 = InstrumentParameters.POWER_ALWAYS_ON
    SIX_HZ_PROFILING_MODE           = InstrumentParameters.SIX_HZ_PROFILING_MODE
    OUTPUT_INCLUDES_SERIAL_NUMBER   = InstrumentParameters.OUTPUT_INCLUDES_SERIAL_NUMBER
    OUTPUT_INCLUDES_BATTERY_VOLTAGE = InstrumentParameters.OUTPUT_INCLUDES_BATTERY_VOLTAGE
    SAMPLING_LED                    = InstrumentParameters.SAMPLING_LED
    ENGINEERING_UNITS_OUTPUT        = InstrumentParameters.ENGINEERING_UNITS_OUTPUT
    AUTO_RUN                        = InstrumentParameters.AUTO_RUN
    INHIBIT_DATA_STORAGE            = InstrumentParameters.INHIBIT_DATA_STORAGE


class AdvancedFunctionsBits(BaseEnum):
    power_always_on = 0x8000
    six_hz_profiling_mode = 0x4000
    output_includes_serial_number = 0x20
    output_includes_battery_voltage = 0x10
    sampling_led = 0x8
    engineering_units_output = 0x4
    auto_run = 0x2
    inhibit_data_storage = 0x1


###############################################################################
#   Driver for XR-420 Thermistor
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):

    """
    Instrument driver class for XR-420 driver.
    Uses CommandResponseInstrumentProtocol to communicate with the device
    """

    def __init__(self, evt_callback, refdes):
        SingleConnectionInstrumentDriver.__init__(self, evt_callback, refdes)
        # replace the driver's discover handler with one that applies the startup values after discovery
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, 
                                         DriverEvent.DISCOVER, 
                                         self._handler_connected_discover)
    
    def _handler_connected_discover(self, event, *args, **kwargs):
        # Redefine discover handler so that we can apply startup params after we discover. 
        # For this instrument the driver puts the instrument into command mode during discover.
        result = SingleConnectionInstrumentDriver._handler_connected_protocol_event(self, event, *args, **kwargs)
        self.apply_startup_params()
        return result

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = InstrumentProtocol(InstrumentResponses, INSTRUMENT_NEWLINE, self._driver_event)


###############################################################################
# Data particles
###############################################################################
class XR_420SampleDataParticleKey(BaseEnum):
    TIMESTAMP = "timestamp"
    TEMPERATURE01 = "temperature01"
    TEMPERATURE02 = "temperature02"
    TEMPERATURE03 = "temperature03"
    TEMPERATURE04 = "temperature04"
    TEMPERATURE05 = "temperature05"
    TEMPERATURE06 = "temperature06"
    TEMPERATURE07 = "temperature07"
    TEMPERATURE08 = "temperature08"
    TEMPERATURE09 = "temperature09"
    TEMPERATURE10 = "temperature10"
    TEMPERATURE11 = "temperature11"
    TEMPERATURE12 = "temperature12"
    TEMPERATURE13 = "temperature13"
    TEMPERATURE14 = "temperature14"
    TEMPERATURE15 = "temperature15"
    TEMPERATURE16 = "temperature16"
    TEMPERATURE17 = "temperature17"
    TEMPERATURE18 = "temperature18"
    TEMPERATURE19 = "temperature19"
    TEMPERATURE20 = "temperature20"
    TEMPERATURE21 = "temperature21"
    TEMPERATURE22 = "temperature22"
    TEMPERATURE23 = "temperature23"
    TEMPERATURE24 = "temperature24"
    BATTERY_VOLTAGE = "battery_voltage"
    SERIAL_NUMBER = "serial_number"

                
class XR_420SampleDataParticle(DataParticle):
    """
    Class for parsing sample data into a data particle structure for the XR-420 sensor. 
    """
    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take something in the data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        temps = []

        match = SAMPLE_DATA_REGEX.match(self.raw_data)

        if not match:
            raise SampleException("XR_420SampleDataParticle: No regex match of parsed sample data: [%r]", self.raw_data)
        
        log.debug('_build_parsed_values: match=%s', match.group(0))
                
        try:
            log.debug('_build_parsed_values: group(1)=%s', match.group(1))
            timestamp = time.strptime(match.group(1), "%y%m%d%H%M%S")
            log.debug("_build_parsed_values: ts=%s", timestamp)
            self.set_internal_timestamp(unix_time=timegm_to_float(timestamp))
            ntp_timestamp = ntplib.system_to_ntp_time(timegm_to_float(timestamp))

            for i in range(2, 26):
                temps.append(float(match.group(i)))

            battery_voltage = float(match.group(26))
            serial_number = match.group(27)
            
        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%r]" % (ex, self.raw_data))
                     
        result = [{DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TIMESTAMP,
                   DataParticleKey.VALUE: ntp_timestamp},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE01,
                   DataParticleKey.VALUE: temps[0]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE02,
                   DataParticleKey.VALUE: temps[1]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE03,
                   DataParticleKey.VALUE: temps[2]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE04,
                   DataParticleKey.VALUE: temps[3]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE05,
                   DataParticleKey.VALUE: temps[4]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE06,
                   DataParticleKey.VALUE: temps[5]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE07,
                   DataParticleKey.VALUE: temps[6]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE08,
                   DataParticleKey.VALUE: temps[7]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE09,
                   DataParticleKey.VALUE: temps[8]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE10,
                   DataParticleKey.VALUE: temps[9]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE11,
                   DataParticleKey.VALUE: temps[10]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE12,
                   DataParticleKey.VALUE: temps[11]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE13,
                   DataParticleKey.VALUE: temps[12]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE14,
                   DataParticleKey.VALUE: temps[13]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE15,
                   DataParticleKey.VALUE: temps[14]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE16,
                   DataParticleKey.VALUE: temps[15]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE17,
                   DataParticleKey.VALUE: temps[16]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE18,
                   DataParticleKey.VALUE: temps[17]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE19,
                   DataParticleKey.VALUE: temps[18]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE20,
                   DataParticleKey.VALUE: temps[19]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE21,
                   DataParticleKey.VALUE: temps[20]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE22,
                   DataParticleKey.VALUE: temps[21]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE23,
                   DataParticleKey.VALUE: temps[22]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.TEMPERATURE24,
                   DataParticleKey.VALUE: temps[23]},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.BATTERY_VOLTAGE,
                   DataParticleKey.VALUE: battery_voltage},
                  {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number}]
 
        log.debug('XR_420SampleDataParticle: particle=%r', result)
        return result


class XR_420EngineeringDataParticleKey(BaseEnum):
    CALIBRATION_COEFFICIENTS = 'tmpsf_cal_coeffs'
    BATTERY_VOLTAGE = 'battery_voltage'

CALIBRATION_COEFFICIENTS_PARAMETERS = [
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_1,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_2,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_3,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_4,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_5,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_6,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_7,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_8,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_9,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_10,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_11,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_12,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_13,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_14,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_15,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_16,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_17,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_18,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_19,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_20,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_21,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_22,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_23,
    InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_24
]


class XR_420EngineeringDataParticle(DataParticle):
    """
    Class for constructing engineering data into an engineering particle structure for the XR-420 sensor.
    The raw_data variable in the DataParticle base class needs to be initialized to a reference to
    a dictionary that contains the status parameters.
    """
    _data_particle_type = DataParticleType.ENGINEERING

    def _build_parsed_values(self):
        """
        Build the status particle from a dictionary of parameters adding the appropriate tags.
        NOTE: raw_data references a dictionary with the status parameters, not a line of input
        @throws SampleException If there is a problem with particle creation
        """
        result = []

        if not isinstance(self.raw_data, dict):
            raise SampleException("Error: raw_data is not a dictionary")

        log.debug('XR_420EngineeringDataParticle: raw_data=%r', self.raw_data)

        voltage = self.raw_data.get(InstrumentParameters.BATTERY_VOLTAGE)
        if voltage is not None:
            result.append(
                {DataParticleKey.VALUE_ID: XR_420SampleDataParticleKey.BATTERY_VOLTAGE,
                 DataParticleKey.VALUE: voltage}
            )
        else:
            raise SampleException("XR_420EngineeringDataParticle: missing battery voltage")

        cals = []
        for param in CALIBRATION_COEFFICIENTS_PARAMETERS:
            value = self.raw_data.get(param)
            if value is not None:
                cals.append(value)
            else:
                raise SampleException("XR_420EngineeringDataParticle: missing calibration")

        result.append({DataParticleKey.VALUE_ID: XR_420EngineeringDataParticleKey.CALIBRATION_COEFFICIENTS,
                       DataParticleKey.VALUE: cals})

        log.debug('XR_420EngineeringDataParticle: particle=%r', result)
        return result


###############################################################################
#   Protocol for XR-420
###############################################################################
class InstrumentProtocol(CommandResponseInstrumentProtocol):
    """
    This protocol implements a simple command-response interaction for the XR-420 instrument.  
    """
    __metaclass__ = get_logging_metaclass(log_level='debug')

    def __init__(self, prompts, newline, driver_event):
        """
        """
        self.write_delay = WRITE_DELAY
        self._last_data_timestamp = None
        self.eoln = INSTRUMENT_NEWLINE
        self.advanced_functions_bits = AdvancedFunctionsBits.dict()
        
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)
                
        self._protocol_fsm = InstrumentFSM(ProtocolStates, 
                                           ProtocolEvent, 
                                           ProtocolEvent.ENTER,
                                           ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(ProtocolStates.UNKNOWN, ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolStates.UNKNOWN, ProtocolEvent.EXIT, self._handler_unknown_exit)
        self._protocol_fsm.add_handler(ProtocolStates.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)
        self._protocol_fsm.add_handler(ProtocolStates.UNKNOWN, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.EXIT, self._handler_command_exit)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.SET, self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.START_DIRECT, self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.CLOCK_SYNC, self._handler_clock_sync)
        self._protocol_fsm.add_handler(ProtocolStates.COMMAND, ProtocolEvent.ACQUIRE_STATUS, self._handler_acquire_status)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.ENTER, self._handler_autosample_enter)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.EXIT, self._handler_autosample_exit)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.SCHEDULED_ACQUIRE_STATUS, self._handler_acquire_status)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_clock_sync)
        self._protocol_fsm.add_handler(ProtocolStates.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.ENTER, self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.EXIT, self._handler_direct_access_exit)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct)
        self._protocol_fsm.add_handler(ProtocolStates.DIRECT_ACCESS, ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct)

        # Set state machine in UNKNOWN state. 
        self._protocol_fsm.start(ProtocolStates.UNKNOWN)

        self._build_command_handlers()
 
        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        # create chunker for processing instrument samples.
        self._chunker = StringChunker(InstrumentProtocol.chunker_sieve_function)

        self.initialize_scheduler()

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
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes. 
        """
        log.debug("_got_chunk: detected structure = <%s>", structure)
        self._extract_sample(XR_420SampleDataParticle, SAMPLE_DATA_REGEX, structure, timestamp)

    ########################################################################
    # overridden methods from base class.
    ########################################################################
    def _get_response(self, timeout=10, expected_prompt=None):
        """
        overridden to find expected prompt anywhere in buffer
        Get a response from the instrument, but be a bit loose with what we
        find. Leave some room for white space around prompts and not try to
        match that just in case we are off by a little whitespace or not quite
        at the end of a line.
        
        @todo Consider cases with no prompt
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
            if isinstance(expected_prompt, str):
                prompt_list = [expected_prompt]
            else:
                prompt_list = expected_prompt

        while True:
            for item in prompt_list:
                if item in self._promptbuf:
                    return item, self._linebuf
                else:
                    time.sleep(.1)

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in InstrumentProtocol._get_response()")

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        overridden to retrieve the expected response from the build handler
        Perform a command-response on the device.
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup and command timeout.
        @retval resp_result The (possibly parsed) response result.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """

        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', 10)
        expected_prompt = kwargs.get('expected_prompt', None)
        write_delay = kwargs.get('write_delay', 0)
        
        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException('Cannot build command: %s' % cmd)

        (cmd_line, expected_response) = build_handler(command=cmd, **kwargs)
        if expected_prompt is None:
            expected_prompt = expected_response
            
        # Wakeup the device, pass up exception if timeout
        self._wakeup()
        
        # Clear line and prompt buffers for result.

        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        log.debug('_do_cmd_resp: cmd=%r, timeout=%s, write_delay=%s, expected_prompt=%s,',
                  repr(cmd_line), timeout, write_delay, expected_prompt)

        if write_delay == 0:
            self._connection.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection.send(char)
                time.sleep(write_delay)

        # Wait for the prompt, prepare result and return, timeout exception
        (prompt, result) = self._get_response(timeout, expected_prompt=expected_prompt)

        resp_handler = self._response_handlers.get((self.get_current_state(), cmd), None) or \
                       self._response_handlers.get(cmd, None)
        resp_result = None
        if resp_handler:
            resp_result = resp_handler(result, prompt, **kwargs)

        return resp_result

    def _wakeup(self, *args):
        """
        overridden to find longest matching prompt anywhere in the buffer and to be
        more responsive with its use of sleep()
        Clear buffers and send a wakeup command to the instrument
        @param timeout The timeout to wake the device.
        @param delay The time to wait between consecutive wakeups.
        @throw InstrumentTimeoutException if the device could not be woken.
        """
        
        timeout = 5
        response_delay = 1
        
        # Clear the prompt buffer.
        self._promptbuf = ''
        
        # Grab start time for overall timeout.
        start_time = time.time()
        
        while True:
            # Send 'get status' command.
            log.debug('_wakeup: sending <%r>' % InstrumentCmds.GET_STATUS)
            self._connection.send(InstrumentCmds.GET_STATUS)
            # Grab send time for response timeout.
            send_time = time.time()

            while True:
                time.sleep(.1)

                # look for response
                if InstrumentResponses.GET_STATUS in self._promptbuf:
                    log.debug('_wakeup got prompt: %r' % repr(InstrumentResponses.GET_STATUS))
                    return InstrumentResponses.GET_STATUS

                time_now = time.time()
                # check for overall timeout
                if time_now > start_time + timeout:
                    raise InstrumentTimeoutException("Timeout waiting for instrument to wakeup")
                # check for retry timeout
                if time_now > send_time + response_delay:
                    break

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
    
    def _handler_unknown_exit(self, *args, **kwargs):
        """
        Exit unknown state.
        """
        pass

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE.  
        @retval (next_state, result), (ProtocolStates.COMMAND or ProtocolStates.AUTOSAMPLE, None) if successful.
        @raise InstrumentProtocolException if we fail to discover our state
        """
        try:
            self._wakeup()
        except InstrumentTimeoutException:
            # didn't get status response, so indicate that there is trouble with the instrument
            raise InstrumentProtocolException('Failed to discover instrument state. Unable to wake up instrument.')
        
        match = re.search('Logger status (\d{2})', self._promptbuf)
        if match is not None:
            # got status response, so determine what mode the instrument is in
            status = int(match.group(1), 16)
            log.debug("_handler_unknown_discover: parsed=%s, status=%d" % (match.group(1), status))

            if status in [Status.STARTED_SAMPLING,
                          Status.TEMPORARILY_SUSPENDED_SAMPLING,
                          Status.HIGH_SPEED_PROFILING_MODE]:
                next_state = ProtocolStates.AUTOSAMPLE
                result = ResourceAgentState.STREAMING
            else:
                next_state = ProtocolStates.COMMAND
                result = ResourceAgentState.IDLE
        else:
            raise InstrumentProtocolException('Failed to discover instrument state. prompt mismatch.')
                    
        return next_state, result

    ########################################################################
    # State Command handlers.
    ########################################################################
    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        # Command device to update parameters and send a config change event if needed.
        if self._init_type != InitializationType.NONE:
            self._update_params()
            self._init_params()

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
            
    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        pass

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @retval None, None
        """
        self._verify_not_readonly(*args, **kwargs)
        self._set_params(*args, **kwargs)

        return None, None

    def _set_params(self, *args, **kwargs):

        try:
            params_to_set = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')
        else:
            if not isinstance(params_to_set, dict):
                raise InstrumentParameterException('Set parameters not a dict.')

        self._verify_not_readonly(*args, **kwargs)

        update_params = False
        set_advance_params = False

        for (key, val) in params_to_set.iteritems():
            try:
                if key in AdvancedFunctionsParameters.list():
                    old_val = self._param_dict.get(key)
                    if old_val != val:
                        self._param_dict.set_value(key, val)
                        set_advance_params = True
                else:
                    old_val = self._param_dict.format(key)
                    new_val = self._param_dict.format(key, params_to_set[key])

                    if old_val != new_val:
                        update_params = True
                        command = self._param_dict.get_submenu_write(key)
                        log.debug('set_params: cmd=%s, name=%s, value=%s', command, key, val)
                        self._do_cmd_no_resp(command, key, val, timeout=5)
            except Exception:
                log.debug('Unknown driver parameter %s', key)
                raise InstrumentParameterException('Unknown driver parameter %s' % key)

        if set_advance_params:
            command = self._param_dict.get_submenu_write(AdvancedFunctionsParameters.POWER_ALWAYS_ON)
            self._do_cmd_no_resp(command, AdvancedFunctionsParameters.POWER_ALWAYS_ON,
                                 self._param_dict.get(AdvancedFunctionsParameters.POWER_ALWAYS_ON), timeout=5)

        if update_params:
            self._update_params()

        self._check_for_set_failures(params_to_set)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @retval (next_state, result) tuple, (ProtocolStates.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """

        # these calls will return if reset is successful or raise an exception otherwise
        self._reset_instrument()
        self._start_sampling()

        return ProtocolStates.AUTOSAMPLE, (ResourceAgentState.STREAMING, None)

    def _handler_command_start_direct(self):
        
        return ProtocolStates.DIRECT_ACCESS, (ResourceAgentState.DIRECT_ACCESS, None)

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        if self._init_type != InitializationType.NONE:
            self._reset_instrument()
            self._update_params()
            self._init_params()
            self._start_sampling()

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
    
    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """
        pass

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @retval (next_state, result) tuple, (ProtocolStates.COMMAND,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command misunderstood or
        incorrect prompt received.
        """

        # this call will return if reset is successful or raise an exception otherwise
        self._reset_instrument()

        return ProtocolStates.COMMAND, (ResourceAgentState.COMMAND, None)
        
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
        self._do_cmd_direct(data)
                        
        return None, None

    def _handler_direct_access_stop_direct(self):
        return ProtocolStates.COMMAND, (ResourceAgentState.COMMAND, None)

    ########################################################################
    # general handlers.
    ########################################################################
    def _handler_clock_sync(self, *args, **kwargs):
        """
        sync clock close to a second edge 
        @retval (next_state, result) tuple, (None, None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        if self.get_current_state() == ProtocolStates.AUTOSAMPLE:
            try:
                self._reset_instrument()
            except InstrumentCommandException:
                log.error('Unable to break out of sampling. Will not set clock!')

        # get time in ION format so command builder method can convert it correctly
        str_time = get_timestamp_delayed("%d %b %Y %H:%M:%S")
        log.debug("_handler_command_clock_sync: time set to %s", str_time)
        # set the time
        command = self._param_dict.get_submenu_write(InstrumentParameters.LOGGER_DATE_AND_TIME)
        self._do_cmd_no_resp(command, InstrumentParameters.LOGGER_DATE_AND_TIME, str_time, timeout=5)
        # get the time to update the parameter value
        command = self._param_dict.get_submenu_read(InstrumentParameters.LOGGER_DATE_AND_TIME)
        self._do_cmd_resp(command)

        if self.get_current_state() == ProtocolStates.AUTOSAMPLE:
            try:
                self._start_sampling()
            except InstrumentCommandException:
                log.error('Unable to go back to sampling, changing state to COMMAND')
                return ProtocolStates.COMMAND, (ResourceAgentState.COMMAND, None)

        return None, (None, None)

    def _handler_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        # update battery voltage value
        command = self._param_dict.get_submenu_read(InstrumentParameters.BATTERY_VOLTAGE)
        self._do_cmd_resp(command, name=InstrumentParameters.BATTERY_VOLTAGE)

        params = [InstrumentParameters.BATTERY_VOLTAGE] + CALIBRATION_COEFFICIENTS_PARAMETERS
        status_params = {}
        for name in params:
            status_params[name] = self._param_dict.get(name)
            log.debug("Add parameter %s: %r", name, status_params[name])

        # Create status data particle, but pass in a reference to the dictionary just created as first parameter instead of the 'line'.
        # The status data particle class will use the 'raw_data' variable as a reference to a dictionary object to get
        # access to parameter values (see the Mavs4EngineeringDataParticle class).
        particle = XR_420EngineeringDataParticle(status_params, preferred_timestamp=DataParticleKey.DRIVER_TIMESTAMP)
        status = particle.generate()

        self._driver_event(DriverAsyncEvent.SAMPLE, status)
        return None, (None, None)

    ########################################################################
    # Private helpers.
    ########################################################################
    def _check_for_set_failures(self, params_to_check):
            device_parameters = self._param_dict.get_config()
            for key in params_to_check.keys():
                log.debug("Verify set, key: %s", key)
                if params_to_check[key] != device_parameters[key]:
                    msg = "SET FAILURE: %s is %s and should have been set to %s" % (key, device_parameters[key], params_to_check[key])
                    log.debug("_check_for_set_failures: %s", msg)
                    raise InstrumentParameterException(msg)

    def _start_sampling(self):
        
        # now start sampling
        status_response = self._do_cmd_resp(InstrumentCmds.START_SAMPLING)
        log.debug('_start_sampling: status=%s', status_response)
        status_as_int = int(status_response, 16)
        if status_as_int not in [Status.ENABLED_SAMPLING_NOT_STARTED, Status.STARTED_SAMPLING]:
            raise InstrumentCommandException("_handler_command_start_autosample: " +
                                             "Failed to start sampling, status=%s" 
                                             % status_response)
            
    def _reset_instrument(self):

        ENABLING_SEQUENCE = '!U01N'
        ERASE_TIMEOUT = 60
        # Issue reset command and return if successful.
        for i in range(2):
            # Wakeup the device, pass up exception if timeout    
            self._wakeup()
            # Send 'reset sampling' command.
            log.debug('_reset_instrument: sending <%r>' % ENABLING_SEQUENCE)
            self._connection.send(ENABLING_SEQUENCE)
            time.sleep(1)
            log.debug('_reset_instrument: sending <%r>' % InstrumentCmds.RESET_SAMPLING_ERASE_FLASH)
            self._connection.send(InstrumentCmds.RESET_SAMPLING_ERASE_FLASH)
            starttime = time.time()
            # Erasing flash memory and resetting sampling mode
            while True:
                self._do_cmd_resp(InstrumentCmds.GET_STATUS)
                status_as_int = int(self._param_dict.get(InstrumentParameters.STATUS), 16)
                log.debug('_reset_instrument: status=%x', status_as_int)
                if status_as_int == Status.NOT_ENABLED_FOR_SAMPLING:
                    # instrument is reset and ready
                    return
                elif status_as_int == Status.ERASING_DATA_MEMORY:
                    # instrument is still busy
                    time.sleep(1)
                elif status_as_int == Status.DATA_MEMORY_ERASE_FAILED:
                    # serious instrument failure
                    raise InstrumentCommandException("_reset_instrument: " +
                                                     "SERIOUS FAILURE to reset instrument! status=%s" 
                                                     % Status.DATA_MEMORY_ERASE_FAILED)
                if time.time() > starttime + ERASE_TIMEOUT:
                    break
        raise InstrumentCommandException("_reset_instrument: " +
                                         "Failed to reset instrument after 2 tries of %d seconds each, status=%s" 
                                         % (ERASE_TIMEOUT, self._param_dict.get(InstrumentParameters.STATUS)))
            
    def _convert_battery_voltage(self, reported_battery_voltage):
        battery_voltage = int(reported_battery_voltage, 16)
        battery_voltage *= .0816485
        battery_voltage += .25417
        return battery_voltage

    def _convert_xr_420_date_and_time(self, reported_date_and_time):
        """
        convert string from XR-420 "yymmddhhmmss to ION "21 AUG 2012  09:51:55"
        """
        return time.strftime("%d %b %Y %H:%M:%S", time.strptime(reported_date_and_time, "%y%m%d%H%M%S"))

    def _convert_ion_date_time(self, ion_date_time_string):
        """
        convert string from ION "21 AUG 2012  09:51:55" to XR-420 "yymmddhhmmss"
        """
        return time.strftime("%y%m%d%H%M%S", time.strptime(ion_date_time_string, "%d %b %Y %H:%M:%S"))
    
    def _convert_xr_420_time(self, reported_time):
        """
        convert string from XR-420 "hhmmss to ION "09:51:55"
        """
        return time.strftime("%H:%M:%S", time.strptime(reported_time, "%H%M%S"))

    def _convert_ion_time(self, ion_date_time_string):
        """
        convert string from ION "09:51:55" to XR-420 "hhmmss"
        """
        return time.strftime("%H%M%S", time.strptime(ion_date_time_string, "%H:%M:%S"))
    
    def _convert_calibration(self, calibration_string):
        """
        convert calibration string from 32 hex byte values to 4 floating point values
        """
        if len(calibration_string) != 64:
            raise InstrumentParameterException('_convert_calibration: calibration response is not 64 characters in length.')
        float_list = []
        for index in range(4):
            bytes_in_hex = calibration_string[0:16]
            calibration_string = calibration_string[16:]
            bytes_in_hex = bytes_in_hex.decode('hex')
            float_value = struct.unpack('<d', bytes_in_hex)
            float_list.append(float_value[0])
        return float_list
    
    def _check_bit_value(self, value):
        if value in [0, 1]:
            log.debug('_check_bit_value: value <%s> is binary', value)
            return value
        else:
            log.debug('_check_bit_value: value <%s> is not binary - raising exception', value)
            raise InstrumentParameterException('not a binary value.')

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary. 
        """
        # Get old param dict config.
        old_config = self._param_dict.get_config()
        
        for key in InstrumentParameters.list():
            if key != InstrumentParameters.ALL:
                command = self._param_dict.get_submenu_read(key)
                response = self._do_cmd_resp(command, name=key)

        # Get new param dict config. If it differs from the old config,
        # tell driver superclass to publish a config change event.
        new_config = self._param_dict.get_config()
        log.debug("Old Configuration: %s", old_config)
        log.debug("New Configuration: %s", new_config)
        if new_config != old_config:
            log.debug("Configuration change detected!")
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, False)

    def _build_command_dict(self):
        """
        Populate the command dictionary with command.
        """
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name="Acquire Status")
        self._cmd_dict.add(Capability.CLOCK_SYNC, display_name='Synchronize Clock')
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name="Start Autosample")
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, display_name="Stop Autosample")
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')


    def _build_param_dict(self):
        """
        Populate the parameter dictionary with XR-420 parameters.
        For each parameter key add value formatting function for set commands.
        """
        # The parameter dictionary.
        self._param_dict = ProtocolParameterDict()
        
        # Add parameter handlers to parameter dictionary for instrument configuration parameters.
        self._param_dict.add(InstrumentParameters.STATUS,
                             r'Logger status (.*)\r\n', 
                             lambda match : match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Logger Status",
                             description="Current logging status of the instrument.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_STATUS)

        self._param_dict.add(InstrumentParameters.IDENTIFICATION,
                             r'(RBR XR-420 .*)\r\n', 
                             lambda match : match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Identification",
                             description="Instrument identification.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_IDENTIFICATION)

        self._param_dict.add(InstrumentParameters.LOGGER_DATE_AND_TIME,
                             r'(\d{12})CTD\r\n', 
                             lambda match : self._convert_xr_420_date_and_time(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Date/Time",
                             description="Timestamp of last get/set time.",
                             units="D M Y H:M:S",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_LOGGER_DATE_AND_TIME,
                             submenu_write=InstrumentCmds.SET_LOGGER_DATE_AND_TIME)

        self._param_dict.add(InstrumentParameters.SAMPLE_INTERVAL,
                             r'(\d{6})CSP\r\n', 
                             lambda match : self._convert_xr_420_time(match.group(1)),
                             str,
                             startup_param=True,
                             default_value='00:00:12',             # 12 seconds
                             type=ParameterDictType.STRING,
                             display_name="Sample Interval",
                             description="Sampling interval between samples taken.",
                             units="HH:MM:SS",
                             submenu_read=InstrumentCmds.GET_SAMPLE_INTERVAL,
                             submenu_write=InstrumentCmds.SET_SAMPLE_INTERVAL)

        self._param_dict.add(InstrumentParameters.START_DATE_AND_TIME,
                             r'(\d{12})CST\r\n', 
                             lambda match : self._convert_xr_420_date_and_time(match.group(1)),
                             str,
                             startup_param=True,
                             default_value='01 Jan 2000 00:00:00',
                             direct_access=False,
                             type=ParameterDictType.STRING,
                             display_name="Start Date and Time",
                             description="Date/time at which the logger starts sampling.",
                             units="D M Y H:M:S",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_START_DATE_AND_TIME,
                             submenu_write=InstrumentCmds.SET_START_DATE_AND_TIME)

        self._param_dict.add(InstrumentParameters.END_DATE_AND_TIME,
                             r'(\d{12})CET\r\n', 
                             lambda match : self._convert_xr_420_date_and_time(match.group(1)),
                             str,
                             default_value='01 Jan 2050 00:00:00',
                             startup_param=True,
                             direct_access=False,
                             type=ParameterDictType.STRING,
                             display_name="End Date and Time",
                             description="Date/time at which the logger stops sampling.",
                             units="D M Y H:M:S",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_END_DATE_AND_TIME,
                             submenu_write=InstrumentCmds.SET_END_DATE_AND_TIME)

        self._param_dict.add(InstrumentParameters.BATTERY_VOLTAGE,
                             r'(\w{2})BAT\r\n', 
                             lambda match : self._convert_battery_voltage(match.group(1)),
                             self._float_to_string,
                             type=ParameterDictType.FLOAT,
                             display_name="Battery Voltage",
                             description="Battery voltage of the instrument.",
                             units=Units.VOLT,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_BATTERY_VOLTAGE)

        self._param_dict.add(InstrumentParameters.POWER_ALWAYS_ON,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=1,          # 1 = True
                             type=ParameterDictType.INT,
                             display_name="Power Always On",
                             description="Enable instrument sleeping between samples: (1:True | 0:False)",
                             startup_param=True,
                             direct_access=False,
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        # Not available using this logger
        self._param_dict.add(InstrumentParameters.SIX_HZ_PROFILING_MODE,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=0,          # 0 = False
                             type=ParameterDictType.INT,
                             display_name="6Hz Profiling Mode",
                             description="Enable profiling mode: (1:True | 0:False)",
                             startup_param=True,
                             direct_access=False,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        self._param_dict.add(InstrumentParameters.OUTPUT_INCLUDES_SERIAL_NUMBER,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=1,          # 1 = True
                             startup_param=True,
                             direct_access=True,
                             type=ParameterDictType.INT,
                             display_name="Output Includes Serial Number",
                             description="Enable serial number in output: (1:True | 0:False)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        self._param_dict.add(InstrumentParameters.OUTPUT_INCLUDES_BATTERY_VOLTAGE,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=1,          # 1 = True
                             startup_param=True,
                             direct_access=True,
                             type=ParameterDictType.INT,
                             display_name="Output Includes Battery Voltage",
                             description="Enable battery voltage in output: (1:True | 0:False)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        self._param_dict.add(InstrumentParameters.SAMPLING_LED,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=0,          # 0 = False
                             startup_param=True,
                             direct_access=False,
                             type=ParameterDictType.INT,
                             display_name="Sampling LED",
                             description="Enable sampling LED: (1:True | 0:False)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        self._param_dict.add(InstrumentParameters.ENGINEERING_UNITS_OUTPUT,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=1,          # 1 = True
                             startup_param=True,
                             direct_access=True,
                             type=ParameterDictType.INT,
                             display_name="Engineering Units Output",
                             description="Enable engineering units in output: (1:True | 0:False)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        self._param_dict.add(InstrumentParameters.AUTO_RUN,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=1,          # 1 = True
                             startup_param=True,
                             direct_access=False,
                             type=ParameterDictType.INT,
                             display_name="Auto Run",
                             description="Enable instrument to restart in sampling mode after power cycle: (1:True | 0:False)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        self._param_dict.add(InstrumentParameters.INHIBIT_DATA_STORAGE,
                             r'$^',
                             lambda value : self._check_bit_value(value),
                             None,
                             default_value=1,          # 1 = True
                             startup_param=True,
                             direct_access=False,
                             type=ParameterDictType.INT,
                             display_name="Inhibit Data Storage",
                             description="Disable data storage on instrument: (1:True | 0:False)",
                             submenu_read=InstrumentCmds.GET_ADVANCED_FUNCTIONS,
                             submenu_write=InstrumentCmds.SET_ADVANCED_FUNCTIONS)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_1,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 1",
                             description="Current calibrations for channel 1.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_2,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 2",
                             description="Current calibrations for channel 2.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_3,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 3",
                             description="Current calibrations for channel 3.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_4,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 4",
                             description="Current calibrations for channel 4.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_5,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 5",
                             description="Current calibrations for channel 5.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_6,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 6",
                             description="Current calibrations for channel 6.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_7,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 7",
                             description="Current calibrations for channel 7.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_8,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 8",
                             description="Current calibrations for channel 8.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_9,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 9",
                             description="Current calibrations for channel 9.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_10,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 10",
                             description="Current calibrations for channel 10.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_11,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 11",
                             description="Current calibrations for channel 11.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_12,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 12",
                             description="Current calibrations for channel 12.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_13,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 13",
                             description="Current calibrations for channel 13.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_14,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 14",
                             description="Current calibrations for channel 14.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_15,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 15",
                             description="Current calibrations for channel 15.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_16,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 16",
                             description="Current calibrations for channel 16.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_17,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 17",
                             description="Current calibrations for channel 17.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_18,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 18",
                             description="Current calibrations for channel 18.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_19,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 19",
                             description="Current calibrations for channel 19.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_20,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 20",
                             description="Current calibrations for channel 20.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_21,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 21",
                             description="Current calibrations for channel 21.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_22,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 22",
                             description="Current calibrations for channel 22.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_23,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 23",
                             description="Current calibrations for channel 23.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

        self._param_dict.add(InstrumentParameters.CALIBRATION_COEFFICIENTS_CHANNEL_24,
                             r'(\w{64})CAL\r\n', 
                             lambda match : self._convert_calibration(match.group(1)),
                             str,
                             type=ParameterDictType.LIST,
                             display_name="Calibration Coefficients Channel 24",
                             description="Current calibrations for channel 24.",
                             visibility=ParameterDictVisibility.READ_ONLY,
                             submenu_read=InstrumentCmds.GET_CHANNEL_CALIBRATION)

    def _build_command_handlers(self):
        
        # Add build handlers for device get commands.
        self._add_build_handler(InstrumentCmds.GET_STATUS, self._build_get_status_command)
        self._add_build_handler(InstrumentCmds.GET_IDENTIFICATION, self._build_get_identification_command)
        self._add_build_handler(InstrumentCmds.GET_LOGGER_DATE_AND_TIME, self._build_get_logger_date_and_time_command)
        self._add_build_handler(InstrumentCmds.GET_SAMPLE_INTERVAL, self._build_get_sample_interval_command)
        self._add_build_handler(InstrumentCmds.GET_START_DATE_AND_TIME, self._build_get_start_date_and_time_command)
        self._add_build_handler(InstrumentCmds.GET_END_DATE_AND_TIME, self._build_get_end_date_and_time_command)
        self._add_build_handler(InstrumentCmds.GET_BATTERY_VOLTAGE, self._build_get_battery_voltage_command)
        self._add_build_handler(InstrumentCmds.GET_CHANNEL_CALIBRATION, self._build_get_channel_calibration_command)
        self._add_build_handler(InstrumentCmds.GET_ADVANCED_FUNCTIONS, self._build_get_advanced_functions_command)
        self._add_build_handler(InstrumentCmds.START_SAMPLING, self._build_start_sampling_command)
        
        # Add build handlers for device set commands.
        self._add_build_handler(InstrumentCmds.SET_LOGGER_DATE_AND_TIME, self._build_set_date_time_command)
        self._add_build_handler(InstrumentCmds.SET_START_DATE_AND_TIME, self._build_set_date_time_command)
        self._add_build_handler(InstrumentCmds.SET_END_DATE_AND_TIME, self._build_set_date_time_command)
        self._add_build_handler(InstrumentCmds.SET_SAMPLE_INTERVAL, self._build_set_time_command)
        self._add_build_handler(InstrumentCmds.SET_ADVANCED_FUNCTIONS, self._build_set_advanced_functions_command)

        # Add response handlers for device get commands.
        self._add_response_handler(InstrumentCmds.GET_STATUS, self._parse_status_response)
        self._add_response_handler(InstrumentCmds.GET_IDENTIFICATION, self._parse_identification_response)
        self._add_response_handler(InstrumentCmds.GET_LOGGER_DATE_AND_TIME, self._parse_logger_date_and_time_response)
        self._add_response_handler(InstrumentCmds.GET_SAMPLE_INTERVAL, self._parse_sample_interval_response)
        self._add_response_handler(InstrumentCmds.GET_START_DATE_AND_TIME, self._parse_start_date_and_time_response)
        self._add_response_handler(InstrumentCmds.GET_END_DATE_AND_TIME, self._parse_end_date_and_time_response)
        self._add_response_handler(InstrumentCmds.GET_BATTERY_VOLTAGE, self._parse_battery_voltage_response)
        self._add_response_handler(InstrumentCmds.GET_CHANNEL_CALIBRATION, self._parse_channel_calibration_response)
        self._add_response_handler(InstrumentCmds.GET_ADVANCED_FUNCTIONS, self._parse_advanced_functions_response)
        self._add_response_handler(InstrumentCmds.START_SAMPLING, self._parse_start_sampling_response)
   
##################################################################################################
# set command handlers
##################################################################################################
    def _build_set_date_time_command(self, cmd, *args):
        try:
            [name, value] = args
            time_str = self._convert_ion_date_time(value)
            command = cmd + time_str
            return command
        except Exception as ex:
            raise InstrumentParameterException('_build_set_date_time_command: %r.' % ex)

    def _build_set_time_command(self, cmd, *args):
        try:
            [name, value] = args
            time_str = self._convert_ion_time(value)
            command = cmd + time_str
            return command
        except Exception as ex:
            raise InstrumentParameterException('_build_set_time_command: %r.' % ex)
        
    def _build_set_advanced_functions_command(self, cmd, *args):
        try:
            value = 0
            for name in AdvancedFunctionsParameters.list():
                if self._param_dict.get(name) == 1:
                    value = value | self.advanced_functions_bits[name]
                log.debug("_build_set_advanced_functions_command: value=%x, a_f[%s]=%x", value, name, self.advanced_functions_bits[name])
            value *= 0x10000
            value_str = '%08x' % value
            command = cmd + value_str
            return command
        except Exception as ex:
            raise InstrumentParameterException('_build_set_advanced_functions_command: %r.' % ex)

##################################################################################################
# get command handlers
##################################################################################################
    def _build_get_status_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_status_command requires a command.')
        return cmd_name, InstrumentResponses.GET_STATUS
    
    def _build_get_identification_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_identification_command requires a command.')
        return cmd_name, InstrumentResponses.GET_IDENTIFICATION
    
    def _build_get_logger_date_and_time_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_logger_date_and_time_command requires a command.')
        return cmd_name, InstrumentResponses.GET_LOGGER_DATE_AND_TIME
    
    def _build_get_sample_interval_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_sample_interval_command requires a command.')
        return cmd_name, InstrumentResponses.GET_SAMPLE_INTERVAL
    
    def _build_get_start_date_and_time_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_start_date_and_time_command requires a command.')
        return cmd_name, InstrumentResponses.GET_START_DATE_AND_TIME
    
    def _build_get_end_date_and_time_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_end_date_and_time_command requires a command.')
        return cmd_name, InstrumentResponses.GET_END_DATE_AND_TIME
    
    def _build_get_battery_voltage_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_battery_voltage_command requires a command.')
        return cmd_name, InstrumentResponses.GET_BATTERY_VOLTAGE
    
    def _build_get_channel_calibration_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_channel_calibration_command requires a command.')
        param_name = kwargs.get('name', None)
        if param_name is None:
            raise InstrumentParameterException('_build_get_channel_calibration_command requires a parameter name.')
        channel_number = '%02X' % int(param_name.split('_')[-1])
        cmd = cmd_name + channel_number
        return cmd, InstrumentResponses.GET_CHANNEL_CALIBRATION
    
    def _build_get_advanced_functions_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_get_advanced_functions_command requires a command.')
        return cmd_name, InstrumentResponses.GET_ADVANCED_FUNCTIONS
    
    def _build_start_sampling_command(self, **kwargs):
        cmd_name = kwargs.get('command', None)
        if cmd_name is None:
            raise InstrumentParameterException('_build_start_sampling_command requires a command.')
        return cmd_name, InstrumentResponses.START_SAMPLING
    
##################################################################################################
# response handlers
##################################################################################################
    def _parse_status_response(self, response, prompt, **kwargs):
        if InstrumentResponses.GET_STATUS in response:
            self._param_dict.update(response)
        else:
            raise InstrumentParameterException('Get status response not correct: %r.' % response)
               
    def _parse_identification_response(self, response, prompt, **kwargs):
        if InstrumentResponses.GET_IDENTIFICATION in response:
            self._param_dict.update(response)
        else:
            raise InstrumentParameterException('Get identification response not correct: %r.' % response)
               
    def _parse_logger_date_and_time_response(self, response, prompt, **kwargs):
        if InstrumentResponses.GET_LOGGER_DATE_AND_TIME in response:
            self._param_dict.update(response)
        else:
            raise InstrumentParameterException('Get logger date and time response not correct: %r.' % response)
               
    def _parse_sample_interval_response(self, response, prompt, **kwargs):
        if InstrumentResponses.GET_SAMPLE_INTERVAL in response:
            self._param_dict.update(response)
        else:
            raise InstrumentParameterException('Get sample interval response not correct: %r.' % response)

    def _parse_start_date_and_time_response(self, response, prompt, **kwargs):
        if InstrumentResponses.GET_START_DATE_AND_TIME in response:
            self._param_dict.update(response)
        else:
            raise InstrumentParameterException('Get start date and time response not correct: %r.' % response)

    def _parse_end_date_and_time_response(self, response, prompt, **kwargs):
        if InstrumentResponses.GET_END_DATE_AND_TIME in response:
            self._param_dict.update(response)
        else:
            raise InstrumentParameterException('Get end date and time response not correct: %r.' % response)

    def _parse_battery_voltage_response(self, response, prompt, **kwargs):
        if InstrumentResponses.GET_BATTERY_VOLTAGE in response:
            self._param_dict.update(response)
        else:
            raise InstrumentParameterException('Get battery voltage response not correct: %r.' % response)

    def _parse_channel_calibration_response(self, response, prompt, **kwargs):
        param_name = kwargs.get('name', None)
        if param_name is None:
            raise InstrumentParameterException('_parse_channel_calibration_response requires a parameter name.')
        if InstrumentResponses.GET_CHANNEL_CALIBRATION in response:
            self._param_dict.update(response, param_name)
        else:
            raise InstrumentParameterException('Get channel calibration response not correct: %r.' % response)

    def _get_bit_value(self, name, value, **kwargs):
        bit_value = value & self.advanced_functions_bits[name]
        log.debug("_get_bit_value: value=%x, a_f[%s]=%x, bit_value=%d", value, name, self.advanced_functions_bits[name], bit_value)
        return 0 if bit_value == 0 else 1
    
    def _parse_advanced_functions_response(self, response, prompt, **kwargs):
        match = re.search('([0-9A-F]{4})[0-9A-F]{4}STC', response)
        if match is not None:
            hex_value = int(match.group(1), 16)
            log.debug("_parse_advanced_functions_response: hex_str=%s, hex_value=%x", match.group(1), hex_value)
            for name in AdvancedFunctionsParameters.list():
                self._param_dict.set_value(name, self._get_bit_value(name, hex_value))
        else:
            raise InstrumentParameterException('Get advanced functions response not correct: %r.' % response)
  
    def _parse_start_sampling_response(self, response, prompt, **kwargs):
        match = re.search('Logger started in mode (\d{2})', response)
        if match is not None:
            return match.group(1)
        else:
            raise InstrumentParameterException('Start sampling response not correct: %r.' % response)
