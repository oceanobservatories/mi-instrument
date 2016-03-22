"""
@package mi.instrument.kut.ek60.ooicore.driver
@file /mi/instrument/kut/ek60/ooicore/driver.py
@author Richard Han
@brief Driver for the ooicore
Release notes:
This Driver supports the Kongsberg UnderWater Technology's EK60 Instrument.
"""


import ftplib
import json
import tempfile
import urllib2
import yaml
import time

from mi.core.common import BaseEnum
from mi.core.exceptions import InstrumentParameterException, InstrumentException, SampleException
from mi.core.exceptions import InstrumentConnectionException
from mi.core.instrument.data_particle import DataParticle, CommonDataParticleType, DataParticleKey
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.protocol_param_dict import ParameterDictType

from mi.core.log import get_logger
from mi.core.log import get_logging_metaclass
log = get_logger()

__author__ = 'Richard Han & Craig Risien'
__license__ = 'Apache 2.0'


# newline.
NEWLINE = '\r\n'

# Default Instrument's IP Address
DEFAULT_HOST = "128.193.64.201"
YAML_FILE_NAME = "driver_schedule.yaml"
DEFAULT_PORT = "80"

USER_NAME = "ooi"
PASSWORD = "994ef22"

STATUS_TIMEOUT = 10

DEFAULT_CONFIG = {
    'file_prefix':    "Driver DEFAULT CONFIG_PREFIX",
    'file_path':      "DEFAULT_FILE_PATH",  # relative to filesystem_root/data
    'max_file_size':   288,  # 50MB in bytes:  50 * 1024 * 1024
    'intervals': [{
        'name': "default",
        'type': "constant",
        'start_at': "00:00",
        'duration': "00:15:00",
        'repeat_every': "01:00",
        'stop_repeating_at': "23:55",
        'interval': 1000,
        'max_range': 80,
        'frequency': {
            38000: {
                'mode': 'active',
                'power': 100,
                'pulse_length': 256
            },
            120000: {
                'mode': 'active',
                'power': 100,
                'pulse_length': 64
            },
            200000: {
                'mode': 'active',
                'power': 120,
                'pulse_length': 64
            }
        }
    }]
}


###
#    Driver Constant Definitions
###
# String constants
CONNECTED = "connected"
CURRENT_RAW_FILENAME = "current_raw_filename"
CURRENT_RAW_FILESIZE = "current_raw_filesize"
CURRENT_RUNNING_INTERVAL = "current_running_interval"
CURRENT_UTC_TIME = "current_utc_time"
DURATION = "duration"
ER60_CHANNELS = "er60_channels"
ER60_STATUS = "er60_status"
EXECUTABLE = "executable"
FILE_PATH = "file_path"
FILE_PREFIX = "file_prefix"
FREQUENCY = "frequency"
FREQ_120K = "120000"
FREQ_200K = "200000"
FREQ_38K = "38000"
FS_ROOT = "fs_root"
GPTS_ENABLED = "gpts_enabled"
HOST = "host"
INTERVAL = "interval"
INTERVALS = "intervals"
RAW_OUTPUT = "raw_output"
MAX_FILE_SIZE = "max_file_size"
MAX_RANGE = "max_range"
MODE = "mode"
NAME = "name"
NEXT_SCHEDULED_INTERVAL = "next_scheduled_interval"
PID = "pid"
PORT = "port"
POWER = "power"
PULSE_LENGTH = "pulse_length"
SAMPLE_INTERVAL = "sample_interval"
SAMPLE_RANGE = "sample_range"
SAVE_INDEX = "save_index"
SAVE_BOTTOM = "save_bottom"
SAVE_RAW = "save_raw"
SCHEDULE = "schedule"
SCHEDULE_FILENAME = "schedule_filename"
SCHEDULED_INTERVALS_REMAINING = "scheduled_intervals_remaining"
START_AT = "start_at"
STOP_REPEATING_AT = "stop_repeating_at"
TYPE = "type"


class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver
    """
    RAW = CommonDataParticleType.RAW
    ZPLSC_STATUS = 'zplsc_status'


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
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
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET
    DISCOVER = ProtocolEvent.DISCOVER


class Parameter(DriverParameter):
    """
    Device specific parameters.
    """
    SCHEDULE = "schedule"
    FTP_IP_ADDRESS = "ftp_ip_address"
    FTP_USERNAME = "ftp_username"
    FTP_PASSWORD = "ftp_password"
    FTP_PORT = "ftp_port"


class Prompt(BaseEnum):
    """
    Device i/o prompts..
    """


class Command(BaseEnum):
    """
    Instrument command strings
    """
    ACQUIRE_STATUS = 'acquire_status'
    START_AUTOSAMPLE = 'start_autosample'
    STOP_AUTOSAMPLE = 'stop_autosample'
    GET = 'get_param'
    SET = 'set_param'


###############################################################################
# Data Particles
###############################################################################
class ZPLSCStatusParticleKey(BaseEnum):
    ZPLSC_CONNECTED = "zplsc_connected"                                      # Connected to a running ER 60 instance
    ZPLSC_ACTIVE_38K_MODE = "zplsc_active_38k_mode"                          # 38K Transducer transmit mode
    ZPLSC_ACTIVE_38K_POWER = "zplsc_active_38k_power"                        # 38K Transducer transmit power in W
    ZPLSC_ACTIVE_38K_PULSE_LENGTH = "zplsc_active_38k_pulse_length"          # 38K Transducer transmit pulse length in seconds
    ZPLSC_ACTIVE_38K_SAMPLE_INTERVAL = "zplsc_active_38k_sample_interval"    # Sample interval in seconds
    ZPLSC_ACTIVE_120K_MODE = "zplsc_active_120k_mode"                        # 120K Transducer transmit mode
    ZPLSC_ACTIVE_120K_POWER = "zplsc_active_120k_power"                      # 120K Transducer transmit power in W
    ZPLSC_ACTIVE_120K_PULSE_LENGTH = "zplsc_active_120k_pulse_length"        # 120K Transducer Transmit pulse length in seconds
    ZPLSC_ACTIVE_120K_SAMPLE_INTERVAL = "zplsc_active_120k_sample_interval"  # 120K Sample Interval
    ZPLSC_ACTIVE_200K_MODE = "zplsc_active_200k_mode"                        # 200K Transducer transmit mode
    ZPLSC_ACTIVE_200K_POWER = "zplsc_active_200k_power"                      # 200K Transducer transmit power in W
    ZPLSC_ACTIVE_200K_PULSE_LENGTH = "zplsc_active_200k_pulse_length"        # 200K Transducer transmit pulse length in seconds
    ZPLSC_ACTIVE_200K_SAMPLE_INTERVAL = "zplsc_active_200k_sample_interval"  # 200K Transducer sample interval
    ZPLSC_CURRENT_UTC_TIME = "zplsc_current_utc_time"                        # Current UTC Time
    ZPLSC_EXECUTABLE = "zplsc_executable"                                    # Executable used to launch ER60
    ZPLSC_FS_ROOT = "zplsc_fs_root"                                          # Root directory where data/logs/configs are stored
    ZPLSC_NEXT_SCHEDULED_INTERVAL = "zplsc_next_scheduled_interval"          # UTC time of next scheduled interval
    ZPLSC_HOST = "zplsc_host"                                                # Host IP Address
    ZPLSC_PID = "zplsc_pid"                                                  # PID of running ER60 process
    ZPLSC_PORT = "zplsc_port"                                                # Host port number
    ZPLSC_CURRENT_RAW_FILENAME = "zplsc_current_raw_filename"                # File name of the current .raw file
    ZPLSC_CURRENT_RAW_FILESIZE = "zplsc_current_raw_filesize"                # File size of current .raw file
    ZPLSC_FILE_PATH = "zplsc_file_path"                                      # File storage path
    ZPLSC_FILE_PREFIX = "zplsc_file_prefix"                                  # Current file prefix
    ZPLSC_MAX_FILE_SIZE = "zplsc_max_file_size"                              # Maximum file size
    ZPLSC_SAMPLE_RANGE = "zplsc_sample_range"                                # Recording range
    ZPLSC_SAVE_BOTTOM = "zplsc_save_bottom"                                  # Save bottom file
    ZPLSC_SAVE_INDEX = "zplsc_save_index"                                    # Save index file
    ZPLSC_SAVE_RAW = "zplsc_save_raw"                                        # Save raw file
    ZPLSC_SCHEDULED_INTERVALS_REMAINING = "zplsc_scheduled_intervals_remaining"  # Number of intervals remaining in running schedule
    ZPLSC_GPTS_ENABLED = "zplsc_gpts_enabled"                                # GPTs enabled
    ZPLSC_SCHEDULE_FILENAME = "zplsc_schedule_filename"                      # Filename for .yaml schedule file


class ZPLSCStatusParticle(DataParticle):
    """
    Routines for parsing raw data into a status particle structure. Override
    the building of values, and the rest should come along for free.

    Sample:
    {'connected': True,
     'er60_channels': {'GPT  38 kHz 00907207b7b1 6-1 OOI.38|200': {'frequency': 38000,
                                                                   'mode': 'active',
                                                                   'power': 100.0,
                                                                   'pulse_length': 0.000256,
                                                                   'sample_interval': 6.4e-05},
                       'GPT 120 kHz 00907207b7dc 1-1 ES120-7CD': {'frequency': 120000,
                                                                  'mode': 'active',
                                                                  'power': 100.0,
                                                                  'pulse_length': 6.4e-05,
                                                                  'sample_interval': 1.6e-05},
                       'GPT 200 kHz 00907207b7b1 6-2 OOI38|200': {'frequency': 200000,
                                                                  'mode': 'active',
                                                                  'power': 120.0,
                                                                  'pulse_length': 6.4e-05,
                                                                  'sample_interval': 1.6e-05}},
     'er60_status': {'current_running_interval': None,
                     'current_utc_time': '2014-07-08 22:34:18.667000',
                     'executable': 'c:/users/ooi/desktop/er60.lnk',
                     'fs_root': 'D:/',
                     'host': '157.237.15.100',
                     'next_scheduled_interval': None,
                     'pid': 1864,
                     'port': 56635,
                     'raw_output': {'current_raw_filename': 'OOI-D20140707-T214500.raw',
                                    'current_raw_filesize': None,
                                    'file_path': 'D:\\data\\QCT_1',
                                    'file_prefix': 'OOI',
                                    'max_file_size': 52428800,
                                    'sample_range': 220.0,
                                    'save_bottom': True,
                                    'save_index': True,
                                    'save_raw': True},
                     'scheduled_intervals_remaining': 0},
     'gpts_enabled': False,
     'schedule': {},
     'schedule_filename': 'qct_configuration_example_1.yaml'}

    """
    __metaclass__ = get_logging_metaclass(log_level='trace')

    _data_particle_type = DataParticleType.ZPLSC_STATUS

    def _encode_value(self, name, value, encoding_function):
        """
        Encode a value using the encoding function, if it fails store the error in a queue
        Override to handle None values.
        """
        encoded_val = None

        if value is not None:
            try:
                encoded_val = encoding_function(value)
            except Exception:
                log.error("Data particle error encoding. Name:%s Value:%s", name, value)
                self._encoding_errors.append({name: value})
        return {DataParticleKey.VALUE_ID: name,
                DataParticleKey.VALUE: encoded_val}

    def _build_parsed_values(self):
        """
        Parse ZPLSC Status response and return the ZPLSC Status particles
        @throws SampleException If there is a problem with sample
        """
        try:
            log.debug("status raw_data = %s", self.raw_data)
            config = self.raw_data

            if not isinstance(config, dict):
                raise SampleException("ZPLSC status data is not a dictionary" % self.raw_data)

            active_200k_mode = None
            active_200k_power = None
            active_200k_pulse_length = None
            active_200k_sample_interval = None
            active_120k_mode = None
            active_120k_power = None
            active_120k_pulse_length = None
            active_120k_sample_interval = None
            active_38k_mode = None
            active_38k_power = None
            active_38k_pulse_length = None
            active_38k_sample_interval = None

            connected = config.get(CONNECTED)
            er60_channels = config.get(ER60_CHANNELS)
            if er60_channels is not None:
                for key in er60_channels:
                    if '200 kHz' in key:
                        active_200k_mode = er60_channels[key].get(MODE)
                        active_200k_power = er60_channels[key].get(POWER)
                        active_200k_pulse_length = er60_channels[key].get(PULSE_LENGTH)
                        active_200k_sample_interval = er60_channels[key].get(SAMPLE_INTERVAL)
                    elif '120 kHz' in key:
                        active_120k_mode = er60_channels[key].get(MODE)
                        active_120k_power = er60_channels[key].get(POWER)
                        active_120k_pulse_length = er60_channels[key].get(PULSE_LENGTH)
                        active_120k_sample_interval = er60_channels[key].get(SAMPLE_INTERVAL)
                    elif '38 kHz' in key:
                        active_38k_mode = er60_channels[key].get(MODE)
                        active_38k_power = er60_channels[key].get(POWER)
                        active_38k_pulse_length = er60_channels[key].get(PULSE_LENGTH)
                        active_38k_sample_interval = er60_channels[key].get(SAMPLE_INTERVAL)

            current_utc_time = None
            executable = None
            fs_root = None
            next_scheduled_interval = 'None'
            host = None
            pid = '0'
            port = None
            current_raw_filename = None
            current_raw_filesize = 0
            file_path = None
            file_prefix = None
            max_file_size = None
            sample_range = None
            save_bottom = None
            save_index = None
            save_raw = None
            scheduled_intervals_remaining = None

            er60_status = config.get(ER60_STATUS)
            if er60_status is not None:
                current_utc_time = er60_status.get(CURRENT_UTC_TIME)
                executable = er60_status.get(EXECUTABLE)
                fs_root = er60_status.get(FS_ROOT)

                if er60_status.get(NEXT_SCHEDULED_INTERVAL) is not None:
                    next_scheduled_interval = er60_status.get(NEXT_SCHEDULED_INTERVAL)

                host = er60_status.get(HOST)
                if er60_status.get(PID) is not None:
                    pid = er60_status.get(PID)

                port = er60_status.get(PORT)
                raw_output = er60_status.get(RAW_OUTPUT)

                if raw_output is not None:
                    current_raw_filename = raw_output.get(CURRENT_RAW_FILENAME)

                    if raw_output.get(CURRENT_RAW_FILESIZE) is not None:
                        current_raw_filesize = raw_output.get(CURRENT_RAW_FILESIZE)

                    file_path = raw_output.get(FILE_PATH)
                    file_prefix = raw_output.get(FILE_PREFIX)
                    max_file_size = raw_output.get(MAX_FILE_SIZE)
                    sample_range = raw_output.get(SAMPLE_RANGE)
                    save_bottom = raw_output.get(SAVE_BOTTOM)
                    save_index = raw_output.get(SAVE_INDEX)
                    save_raw = raw_output.get(SAVE_RAW)

                scheduled_intervals_remaining = er60_status.get(SCHEDULED_INTERVALS_REMAINING)
            gpts_enabled = config.get(GPTS_ENABLED)
            schedule_filename = config.get(SCHEDULE_FILENAME)

        except KeyError:
            raise SampleException("ValueError while converting ZPLSC Status: [%s]" % self.raw_data)

        result = [
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_CONNECTED, connected, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_MODE, active_200k_mode, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_POWER, active_200k_power, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_PULSE_LENGTH, active_200k_pulse_length, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_SAMPLE_INTERVAL, active_200k_sample_interval, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_MODE, active_120k_mode, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_POWER, active_120k_power, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_PULSE_LENGTH, active_120k_pulse_length, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_SAMPLE_INTERVAL, active_120k_sample_interval, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_MODE, active_38k_mode, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_POWER, active_38k_power, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_PULSE_LENGTH, active_38k_pulse_length, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_SAMPLE_INTERVAL, active_38k_sample_interval, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_CURRENT_UTC_TIME, current_utc_time, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_EXECUTABLE, executable, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_FS_ROOT, fs_root, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_NEXT_SCHEDULED_INTERVAL, next_scheduled_interval, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_HOST, host, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_PID, pid, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_PORT, port, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILENAME, current_raw_filename, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILESIZE, current_raw_filesize, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_FILE_PATH, file_path, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_FILE_PREFIX, file_prefix, str),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_MAX_FILE_SIZE, max_file_size, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_SAMPLE_RANGE, sample_range, float),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_SAVE_BOTTOM, save_bottom, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_SAVE_INDEX, save_index, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_SAVE_RAW, save_raw, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_SCHEDULED_INTERVALS_REMAINING, scheduled_intervals_remaining, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_GPTS_ENABLED, gpts_enabled, int),
            self._encode_value(ZPLSCStatusParticleKey.ZPLSC_SCHEDULE_FILENAME, schedule_filename, str)
        ]

        log.debug("build_parsed_value: %s", result)

        return result


###############################################################################
# Driver
###############################################################################

class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state machine.
    """

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

    __metaclass__ = get_logging_metaclass(log_level='trace')

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
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_AUTOSAMPLE, self._handler_command_autosample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET, self._handler_command_get)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET, self._handler_command_set)

        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GET, self._handler_command_get)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_driver_dict()
        self._build_command_dict()
        self._build_param_dict()

        # Add sample handlers.

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # commands sent sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """

        self._param_dict.add(Parameter.SCHEDULE,
                             r'schedule:\s+(.*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Schedule",
                             description="Large block of text used to create the .yaml file defining the sampling schedule.",
                             startup_param=True,
                             default_value=yaml.dump(DEFAULT_CONFIG, default_flow_style=False))

        self._param_dict.add(Parameter.FTP_IP_ADDRESS,
                             r'ftp address:\s+(\d\d\d\d\.\d\d\d\d\.\d\d\d\d\.\d\d\d)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="FTP IP Address",
                             description="IP address the driver uses to connect to the instrument FTP server.",
                             startup_param=True,
                             default_value=DEFAULT_HOST)

        self._param_dict.add(Parameter.FTP_USERNAME,
                             r'username:(.*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="FTP User Name",
                             description="Username used to connect to the FTP server.",
                             startup_param=True,
                             default_value=USER_NAME)

        self._param_dict.add(Parameter.FTP_PASSWORD,
                             r'password:(.*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="FTP Password",
                             description="Password used to connect to the FTP server.",
                             startup_param=True,
                             default_value=PASSWORD)

        self._param_dict.add(Parameter.FTP_PORT,
                             r'port:(.*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="FTP Port",
                             description="Location on the OOI infrastructure where .raw files and echogram images will be stored.",
                             startup_param=True,
                             default_value=DEFAULT_PORT)

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


    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

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
        @retval next_state, (next_state, result)
        """
        next_state = ProtocolState.COMMAND
        result = []

        # Try to get the status to check if the instrument is alive
        host = self._param_dict.get_config_value(Parameter.FTP_IP_ADDRESS)
        port = self._param_dict.get_config_value(Parameter.FTP_PORT)
        response = self._url_request(host, port, '/status.json')

        if response is None:
            error_msg = "_handler_unknown_discover: Unable to connect to host: %s" % host
            log.error(error_msg)
            raise InstrumentConnectionException(error_msg)

        return next_state, (next_state, result)

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        self._init_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        pass

    def _handler_command_get(self, *args, **kwargs):
        """
        Get parameters while in the command state.
        @param params List of the parameters to pass to the state
        @retval returns (next_state, result) where result is a dict {}. No
            agent state changes happening with Get, so no next_agent_state
        @throw InstrumentParameterException for invalid parameter
        """
        result_vals = {}

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        try:
            params = args[0]

        except IndexError:
            raise InstrumentParameterException('_handler_command_get requires a parameter dict.')

        if Parameter.ALL in params:
            log.debug("Parameter ALL in params")
            params = Parameter.list()
            params.remove(Parameter.ALL)

        log.debug("_handler_command_get: params = %s", params)

        if params is None or not isinstance(params, list):
            raise InstrumentParameterException("GET parameter list not a list!")

        # fill the return values from the update
        for param in params:
            if not Parameter.has(param):
                raise InstrumentParameterException("Invalid parameter!")
            result_vals[param] = self._param_dict.get(param)
            self._param_dict.get_config_value(param)
        result = result_vals

        log.debug("Get finished, next_state: %s, result: %s", None, result)
        return None, result

    def _handler_command_set(self, *args, **kwargs):
        """
        Set parameter
        @retval next state, result
        """
        startup = False

        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('_handler_command_set: command requires a parameter dict.')

        try:
            startup = args[1]
        except IndexError:
            pass

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')

        # For each key, val in the params, set the param dictionary.
        old_config = self._param_dict.get_config()
        self._set_params(params, startup)

        new_config = self._param_dict.get_config()
        if old_config != new_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        return None, None

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        # verify param is not readonly param
        self._verify_not_readonly(*args, **kwargs)

        for key, val in params.iteritems():
            log.debug("KEY = %s VALUE = %s", key, val)
            self._param_dict.set_value(key, val)
            if key == Parameter.SCHEDULE:
                self._ftp_schedule_file()

                # Load the schedule file
                host = self._param_dict.get(Parameter.FTP_IP_ADDRESS)
                port = self._param_dict.get_config_value(Parameter.FTP_PORT)
                log.debug("_set_params: stop the current schedule file")
                self._url_request(host, port, '/stop_schedule', data={})
                log.debug("_set_params: upload driver YAML file to host %s", host)
                res = self._url_request(host, port, '/load_schedule', data=json.dumps({'filename': YAML_FILE_NAME}))
                log.debug("_set_params: result from load = %s", res)

        log.debug("set complete, update params")

    def _ftp_schedule_file(self):
        """
        Construct a YAML schedule file and
        ftp the file to the Instrument server
        """
        # Create a temporary file and write the schedule YAML information to the file
        try:
            config_file = tempfile.TemporaryFile()
            log.debug("temporary file created")

            if config_file is None or not isinstance(config_file, file):
                raise InstrumentException("config_file is not a temp file!")

            config_file.write(self._param_dict.get(Parameter.SCHEDULE))
            config_file.seek(0)
            log.debug("finished writing config file:\n%r", self._param_dict.get(Parameter.SCHEDULE))

        except Exception as e:
            log.error("Create schedule YAML file exception: %s", e)
            raise e

        #  FTP the schedule file to the ZPLSC server
        host = ''

        try:
            log.debug("Create a ftp session")
            host = self._param_dict.get_config_value(Parameter.FTP_IP_ADDRESS)
            log.debug("Got host ip address %s", host)

            ftp_session = ftplib.FTP()
            ftp_session.connect(host)
            ftp_session.login(USER_NAME, PASSWORD)
            log.debug("ftp session was created...")

            ftp_session.set_pasv(False)
            ftp_session.cwd("config")

            ftp_session.storlines('STOR ' + YAML_FILE_NAME, config_file)
            files = ftp_session.dir()

            log.debug("*** Config yaml file sent: %s", files)

            ftp_session.quit()
            config_file.close()

        except (ftplib.socket.error, ftplib.socket.gaierror), e:
            log.error("ERROR: cannot reach FTP Host %s: %s ", host, e)
            raise InstrumentException("ERROR: cannot reach FTP Host %s " % host)

        log.debug("*** FTP %s to ftp host %s successfully", YAML_FILE_NAME, host)

    def _url_request(self, host, port, page, data=None):
        """
        Loads a schedule file previously uploaded to the instrument and sets it as
        the active instrument configuration
        """
        result = None
        url = "https://%s:%d/%s" % (host, port, page)

        try:
            if data is not None:
                log.debug("Request data: %s", data)
                req = urllib2.Request(url, data=data, headers={'Content-Type': 'application/json'})
            else:
                log.debug("No request data")
                req = urllib2.Request(url)

            log.debug("Request url: %s", req.__dict__)

            f = urllib2.urlopen(req, timeout=10)
            res = f.read()
            f.close()
        except urllib2.HTTPError as e:
            log.error("Failed to open url %s. %s", url, e)
            return result
        except urllib2.URLError as e:
            log.error("Failed to open url %s. %s", url, e)
            return result

        try:
            result = json.loads(res)
        except ValueError:
            log.error("Request from url %s is not in valid json format, returned: %s.", url, res)

        return result

    def _handler_command_autosample(self, *args, **kwargs):
        """
        Start autosample mode
        """
        next_state = ProtocolState.AUTOSAMPLE
        result = []

        # FTP the driver schedule file to the instrument server
        self._ftp_schedule_file()

        # Stop the current running schedule file just in case one is running and
        # load the driver schedule file
        host = self._param_dict.get(Parameter.FTP_IP_ADDRESS)
        port = self._param_dict.get_config_value(Parameter.FTP_PORT)
        log.debug("_handler_command_autosample: stop the current schedule file")
        self._url_request(host, port, '/stop_schedule', data={})

        log.debug("_handler_command_autosample: upload driver YAML file to host %s", host)
        res = self._url_request(host, port, '/load_schedule', data=json.dumps({'filename': YAML_FILE_NAME}))

        log.debug(" result from load = %s", res)
        if res.get('result') != 'OK':
            raise InstrumentException('_handler_command_autosample: Load Instrument Schedule File Error.')

        res = self._url_request(host, port, '/start_schedule', data={})
        if res.get('result') != 'OK':
            raise InstrumentException('_handler_command_autosample: Start Schedule File Error.')

        return next_state, (next_state, result)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Acquire status from the instrument
        """
        next_state = None
        timeout = time.time() + STATUS_TIMEOUT

        host = self._param_dict.get_config_value(Parameter.FTP_IP_ADDRESS)
        port = self._param_dict.get_config_value(Parameter.FTP_PORT)
        response = self._url_request(host, port, '/status.json')

        if response:
            log.debug("_handler_command_acquire_status: response from status = %r", response)
            particle = ZPLSCStatusParticle(response, port_timestamp=self._param_dict.get_current_timestamp())

            parsed_sample = particle.generate()
            self._particle_dict[particle.data_particle_type()] = parsed_sample

            self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)
        else:
            log.error("_handler_command_acquire_status: Failed to acquire status from instrument.")

        particles = self.wait_for_particles([DataParticleType.ZPLSC_STATUS], timeout)

        return next_state, (next_state, particles)

    ########################################################################
    # Autosample handlers
    ########################################################################
    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample mode
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_stop(self):
        """
        Stop autosample mode
        """
        next_state = ProtocolState.COMMAND
        result = []

        host = self._param_dict.get_config_value(Parameter.FTP_IP_ADDRESS)
        port = self._param_dict.get_config_value(Parameter.FTP_PORT)
        log.debug("_handler_autosample_stop: stop the current schedule file")
        res = self._url_request(host, port, '/stop_schedule', data={})
        log.debug("handler_autosample_stop: stop schedule returns %r", res)

        return next_state, (next_state, result)
