"""
@package mi.instrument.teledyne.workhorse.driver
@file marine-integrations/mi/instrument/teledyne/workhorse/driver.py
@author Sung Ahn
@brief Driver for the Teledyne Workhorse class instruments
Release notes:

Generic Driver for ADCPS-K, ADCPS-I, ADCPT-B and ADCPT-DE
"""
import time
import struct
import re
from contextlib import contextmanager

from mi.core.log import get_logger
from mi.core.common import Units, Prefixes
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.chunker import StringChunker
from mi.core.common import BaseEnum
from mi.core.time_tools import get_timestamp_delayed
from mi.core.exceptions import InstrumentParameterException, InstrumentTimeoutException, InstrumentException, \
    SampleException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.driver_scheduler import DriverSchedulerConfigKey
from mi.core.driver_scheduler import TriggerType
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.util import dict_equal

from mi.instrument.teledyne.workhorse.pd0_parser import AdcpPd0Record
from mi.instrument.teledyne.workhorse.particles import \
    AdcpCompassCalibrationDataParticle, AdcpSystemConfigurationDataParticle, AdcpAncillarySystemDataParticle, \
    AdcpTransmitPathParticle, AdcpPd0ConfigParticle, AdcpPd0EngineeringParticle, \
    Pd0BeamParticle, Pd0CoordinateTransformType, Pd0EarthParticle, WorkhorseDataParticleType

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

log = get_logger()

# default timeout.
TIMEOUT = 20

# newline.
NEWLINE = '\n'

DEFAULT_CMD_TIMEOUT = 20
DEFAULT_WRITE_DELAY = 0

ZERO_TIME_INTERVAL = '00:00:00'

BASE_YEAR = 2000


class WorkhorsePrompt(BaseEnum):
    """
    Device i/o prompts..
    """
    COMMAND = '\r\n>'
    BREAK = 'BREAK'
    SAMPLING = 'CS\r\n'


class WorkhorseEngineeringParameter(BaseEnum):
    # Engineering parameters for the scheduled commands
    CLOCK_SYNCH_INTERVAL = 'clockSynchInterval'
    GET_STATUS_INTERVAL = 'getStatusInterval'


class WorkhorseParameter(DriverParameter):
    """
    Device parameters
    """
    #
    # set-able parameters
    #
    SERIAL_DATA_OUT = 'CD'          # 000 000 000 Serial Data Out (Vel;Cor;Amp PG;St;P0 P1;P2;P3)
    SERIAL_FLOW_CONTROL = 'CF'      # Flow Control
    BANNER = 'CH'                   # Banner
    INSTRUMENT_ID = 'CI'            # Int 0-255
    SLEEP_ENABLE = 'CL'             # SLEEP Enable
    SAVE_NVRAM_TO_RECORDER = 'CN'   # Save NVRAM to RECORD
    POLLED_MODE = 'CP'              # Polled Mode
    XMIT_POWER = 'CQ'               # 0=Low, 255=High
    LATENCY_TRIGGER = 'CX'          # Latency Trigger

    HEADING_ALIGNMENT = 'EA'        # Heading Alignment
    HEADING_BIAS = 'EB'             # Heading Bias
    SPEED_OF_SOUND = 'EC'           # 1500  Speed Of Sound (m/s)
    TRANSDUCER_DEPTH = 'ED'         # Transducer Depth
    PITCH = 'EP'                    # Pitch
    ROLL = 'ER'                     # Roll
    SALINITY = 'ES'                 # 35 (0-40 pp thousand)
    COORDINATE_TRANSFORMATION = 'EX'
    SENSOR_SOURCE = 'EZ'            # Sensor Source (C;D;H;P;R;S;T)

    DATA_STREAM_SELECTION = 'PD'    # Data Stream selection

    # VADCP parameters
    SYNC_PING_ENSEMBLE = 'SA'
    RDS3_MODE_SEL = 'SM'            # 0=off, 1=master, 2=slave
    SLAVE_TIMEOUT = 'ST'
    SYNCH_DELAY = 'SW'

    ENSEMBLE_PER_BURST = 'TC'       # Ensemble per Burst
    TIME_PER_ENSEMBLE = 'TE'        # 01:00:00.00 (hrs:min:sec.sec/100)
    TIME_OF_FIRST_PING = 'TG'       # ****/**/**,**:**:** (CCYY/MM/DD,hh:mm:ss)
    TIME_PER_PING = 'TP'            # 00:00.20  (min:sec.sec/100)
    TIME = 'TT'                     # 2013/02/26,05:28:23 (CCYY/MM/DD,hh:mm:ss)
    BUFFERED_OUTPUT_PERIOD = 'TX'   # Buffered Output Period

    FALSE_TARGET_THRESHOLD = 'WA'   # 255,001 (Max)(0-255),Start Bin # <--------- TRICKY.... COMPLEX TYPE
    BANDWIDTH_CONTROL = 'WB'        # Bandwidth Control (0=Wid,1=Nar)
    CORRELATION_THRESHOLD = 'WC'    # 064  Correlation Threshold
    SERIAL_OUT_FW_SWITCHES = 'WD'   # 111100000  Data Out (Vel;Cor;Amp PG;St;P0 P1;P2;P3)
    ERROR_VELOCITY_THRESHOLD = 'WE'  # 5000  Error Velocity Threshold (0-5000 mm/s)
    BLANK_AFTER_TRANSMIT = 'WF'     # 0088  Blank After Transmit (cm)
    CLIP_DATA_PAST_BOTTOM = 'WI'    # 0 Clip Data Past Bottom (0=OFF,1=ON)
    RECEIVER_GAIN_SELECT = 'WJ'     # 1  Rcvr Gain Select (0=Low,1=High)
    NUMBER_OF_DEPTH_CELLS = 'WN'    # Number of depth cells (1-255)
    PINGS_PER_ENSEMBLE = 'WP'       # Pings per Ensemble (0-16384)
    SAMPLE_AMBIENT_SOUND = 'WQ'     # Sample Ambient sound
    DEPTH_CELL_SIZE = 'WS'          # 0800  Depth Cell Size (cm)
    TRANSMIT_LENGTH = 'WT'          # 0000 Transmit Length 0 to 3200(cm) 0 = Bin Length
    PING_WEIGHT = 'WU'              # 0 Ping Weighting (0=Box,1=Triangle)
    AMBIGUITY_VELOCITY = 'WV'       # 175 Mode 1 Ambiguity Vel (cm/s radial)

    # Engineering parameters for the scheduled commands
    CLOCK_SYNCH_INTERVAL = WorkhorseEngineeringParameter.CLOCK_SYNCH_INTERVAL
    GET_STATUS_INTERVAL = WorkhorseEngineeringParameter.GET_STATUS_INTERVAL


class WorkhorseInstrumentCmds(BaseEnum):
    """
    Device specific commands
    Represents the commands the driver implements and the string that
    must be sent to the instrument to execute the command.
    """
    # Instrument Commands
    OUTPUT_CALIBRATION_DATA = 'AC'
    START_LOGGING = 'CS'
    GET_SYSTEM_CONFIGURATION = 'PS0'
    RUN_TEST_200 = 'PT200'
    OUTPUT_PT2 = 'PT2'
    OUTPUT_PT4 = 'PT4'
    # Engineering commands
    SET = 'set'
    GET = 'get'


class WorkhorseProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class WorkhorseProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    # Scheduled events
    SCHEDULED_CLOCK_SYNC = 'PROTOCOL_EVENT_SCHEDULED_CLOCK_SYNC'
    SCHEDULED_GET_STATUS = 'PROTOCOL_EVENT_SCHEDULED_GET_STATUS'

    # Recovery
    RECOVER_AUTOSAMPLE = 'PROTOCOL_EVENT_RECOVER_AUTOSAMPLE'

    # Base events
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    RUN_TEST = DriverEvent.RUN_TEST
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    DISCOVER = DriverEvent.DISCOVER
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT


class WorkhorseCapability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_AUTOSAMPLE = WorkhorseProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = WorkhorseProtocolEvent.STOP_AUTOSAMPLE
    CLOCK_SYNC = WorkhorseProtocolEvent.CLOCK_SYNC
    RUN_TEST = WorkhorseProtocolEvent.RUN_TEST
    ACQUIRE_STATUS = WorkhorseProtocolEvent.ACQUIRE_STATUS
    GET = WorkhorseProtocolEvent.GET
    SET = WorkhorseProtocolEvent.SET
    START_DIRECT = WorkhorseProtocolEvent.START_DIRECT
    STOP_DIRECT = WorkhorseProtocolEvent.STOP_DIRECT
    DISCOVER = WorkhorseProtocolEvent.DISCOVER


class WorkhorseScheduledJob(BaseEnum):
    CLOCK_SYNC = 'clock_sync'
    GET_CONFIGURATION = 'acquire_configuration'


class WorkhorseADCPUnits(Units):
    PPTHOUSAND = 'ppt'


parameter_regexes = {
    WorkhorseParameter.SERIAL_DATA_OUT: r'CD = (\d\d\d \d\d\d \d\d\d) \-+ Serial Data Out ',
    WorkhorseParameter.SERIAL_FLOW_CONTROL: r'CF = (\d+) \-+ Flow Ctrl ',
    WorkhorseParameter.BANNER: r'CH = (\d) \-+ Suppress Banner',
    WorkhorseParameter.INSTRUMENT_ID: r'CI = (\d+) \-+ Instrument ID ',
    WorkhorseParameter.SLEEP_ENABLE: r'CL = (\d) \-+ Sleep Enable',
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: r'CN = (\d) \-+ Save NVRAM to recorder',
    WorkhorseParameter.POLLED_MODE: r'CP = (\d) \-+ PolledMode ',
    WorkhorseParameter.XMIT_POWER: r'CQ = (\d+) \-+ Xmt Power ',
    WorkhorseParameter.LATENCY_TRIGGER: r'CX = (\d) \-+ Trigger Enable ',
    WorkhorseParameter.HEADING_ALIGNMENT: r'EA = ([+\-]\d+) \-+ Heading Alignment',
    WorkhorseParameter.HEADING_BIAS: r'EB = ([+\-]\d+) \-+ Heading Bias',
    WorkhorseParameter.SPEED_OF_SOUND: r'EC = (\d+) \-+ Speed Of Sound',
    WorkhorseParameter.TRANSDUCER_DEPTH: r'ED = (\d+) \-+ Transducer Depth ',
    WorkhorseParameter.PITCH: r'EP = ([+\-\d]+) \-+ Tilt 1 Sensor ',
    WorkhorseParameter.ROLL: r'ER = ([+\-\d]+) \-+ Tilt 2 Sensor ',
    WorkhorseParameter.SALINITY: r'ES = (\d+) \-+ Salinity ',
    WorkhorseParameter.COORDINATE_TRANSFORMATION: r'EX = (\d+) \-+ Coord Transform ',
    WorkhorseParameter.SENSOR_SOURCE: r'EZ = (\d+) \-+ Sensor Source ',
    WorkhorseParameter.DATA_STREAM_SELECTION: r'PD = (\d+) \-+ Data Stream Select',
    WorkhorseParameter.ENSEMBLE_PER_BURST: r'TC (\d+) \-+ Ensembles Per Burst',
    WorkhorseParameter.TIME_PER_ENSEMBLE: r'TE (\d\d:\d\d:\d\d.\d\d) \-+ Time per Ensemble ',
    WorkhorseParameter.TIME_OF_FIRST_PING: r'TG (..../../..,..:..:..) - Time of First Ping ',
    WorkhorseParameter.TIME_PER_PING: r'TP (\d\d:\d\d.\d\d) \-+ Time per Ping',
    WorkhorseParameter.TIME: r'TT (\d\d\d\d/\d\d/\d\d,\d\d:\d\d:\d\d) \- Time Set ',
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: r'TX (\d\d:\d\d:\d\d) \-+ Buffer Output Period:',
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: r'WA (\d+,\d+) \-+ False Target Threshold ',
    WorkhorseParameter.BANDWIDTH_CONTROL: r'WB (\d) \-+ Bandwidth Control ',
    WorkhorseParameter.CORRELATION_THRESHOLD: r'WC (\d+) \-+ Correlation Threshold',
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: r'WD ([\d ]+) \-+ Data Out ',
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: r'WE (\d+) \-+ Error Velocity Threshold',
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: r'WF (\d+) \-+ Blank After Transmit',
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: r'WI (\d) \-+ Clip Data Past Bottom',
    WorkhorseParameter.RECEIVER_GAIN_SELECT: r'WJ (\d) \-+ Rcvr Gain Select',
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: r'WN (\d+) \-+ Number of depth cells',
    WorkhorseParameter.PINGS_PER_ENSEMBLE: r'WP (\d+) \-+ Pings per Ensemble ',
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: r'WQ (\d) \-+ Sample Ambient Sound',
    WorkhorseParameter.DEPTH_CELL_SIZE: r'WS (\d+) \-+ Depth Cell Size',
    WorkhorseParameter.TRANSMIT_LENGTH: r'WT (\d+) \-+ Transmit Length ',
    WorkhorseParameter.PING_WEIGHT: r'WU (\d) \-+ Ping Weighting ',
    WorkhorseParameter.AMBIGUITY_VELOCITY: r'WV (\d+) \-+ Mode 1 Ambiguity Vel ',
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: r'BOGUS',
    WorkhorseParameter.GET_STATUS_INTERVAL: r'BOGUS',
    WorkhorseParameter.SYNC_PING_ENSEMBLE: r'SA = (\d+) \-+ Synch Before',
    WorkhorseParameter.RDS3_MODE_SEL: r'SM = (\d+) \-+ Mode Select',
    WorkhorseParameter.SLAVE_TIMEOUT: r'ST = (\d+) \-+ Slave Timeout',
    WorkhorseParameter.SYNCH_DELAY: r'SW = (\d+) \-+ Synch Delay',
}

parameter_extractors = {
    WorkhorseParameter.SERIAL_DATA_OUT: lambda match: match.group(1),
    WorkhorseParameter.SERIAL_FLOW_CONTROL: lambda match: match.group(1),
    WorkhorseParameter.BANNER: lambda match: bool(int(match.group(1))),
    WorkhorseParameter.INSTRUMENT_ID: lambda match: int(match.group(1)),
    WorkhorseParameter.SLEEP_ENABLE: lambda match: int(match.group(1)),
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: lambda match: bool(int(match.group(1))),
    WorkhorseParameter.POLLED_MODE: lambda match: bool(int(match.group(1))),
    WorkhorseParameter.XMIT_POWER: lambda match: int(match.group(1)),
    WorkhorseParameter.LATENCY_TRIGGER: lambda match: bool(int(match.group(1))),
    WorkhorseParameter.HEADING_ALIGNMENT: lambda match: int(match.group(1)),
    WorkhorseParameter.HEADING_BIAS: lambda match: int(match.group(1)),
    WorkhorseParameter.SPEED_OF_SOUND: lambda match: int(match.group(1)),
    WorkhorseParameter.TRANSDUCER_DEPTH: lambda match: int(match.group(1)),
    WorkhorseParameter.PITCH: lambda match: int(match.group(1)),
    WorkhorseParameter.ROLL: lambda match: int(match.group(1)),
    WorkhorseParameter.SALINITY: lambda match: int(match.group(1)),
    WorkhorseParameter.COORDINATE_TRANSFORMATION: lambda match: match.group(1),
    WorkhorseParameter.SENSOR_SOURCE: lambda match: match.group(1),
    WorkhorseParameter.DATA_STREAM_SELECTION: lambda match: int(match.group(1)),
    WorkhorseParameter.ENSEMBLE_PER_BURST: lambda match: int(match.group(1)),
    WorkhorseParameter.TIME_PER_ENSEMBLE: lambda match: match.group(1),
    WorkhorseParameter.TIME_OF_FIRST_PING: lambda match: match.group(1),
    WorkhorseParameter.TIME_PER_PING: lambda match: match.group(1),
    WorkhorseParameter.TIME: lambda match: match.group(1) + " UTC",
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: lambda match: match.group(1),
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: lambda match: match.group(1),
    WorkhorseParameter.BANDWIDTH_CONTROL: lambda match: int(match.group(1)),
    WorkhorseParameter.CORRELATION_THRESHOLD: lambda match: int(match.group(1)),
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: lambda match: match.group(1),
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: lambda match: int(match.group(1)),
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: lambda match: int(match.group(1)),
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: lambda match: bool(int(match.group(1))),
    WorkhorseParameter.RECEIVER_GAIN_SELECT: lambda match: int(match.group(1)),
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: lambda match: int(match.group(1)),
    WorkhorseParameter.PINGS_PER_ENSEMBLE: lambda match: int(match.group(1)),
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: lambda match: bool(int(match.group(1))),
    WorkhorseParameter.DEPTH_CELL_SIZE: lambda match: int(match.group(1)),
    WorkhorseParameter.TRANSMIT_LENGTH: lambda match: int(match.group(1)),
    WorkhorseParameter.PING_WEIGHT: lambda match: int(match.group(1)),
    WorkhorseParameter.AMBIGUITY_VELOCITY: lambda match: int(match.group(1)),
    WorkhorseParameter.SYNC_PING_ENSEMBLE: lambda match: str(match.group(1)),
    WorkhorseParameter.RDS3_MODE_SEL: lambda match: int(match.group(1)),
    WorkhorseParameter.SLAVE_TIMEOUT: lambda match: int(match.group(1)),
    WorkhorseParameter.SYNCH_DELAY: lambda match: int(match.group(1)),
}

parameter_formatters = {
    WorkhorseParameter.SERIAL_DATA_OUT: str,
    WorkhorseParameter.SERIAL_FLOW_CONTROL: str,
    WorkhorseParameter.BANNER: int,
    WorkhorseParameter.INSTRUMENT_ID: str,
    WorkhorseParameter.SLEEP_ENABLE: int,
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: int,
    WorkhorseParameter.POLLED_MODE: int,
    WorkhorseParameter.XMIT_POWER: str,
    WorkhorseParameter.LATENCY_TRIGGER: int,
    WorkhorseParameter.HEADING_ALIGNMENT: str,
    WorkhorseParameter.HEADING_BIAS: str,
    WorkhorseParameter.SPEED_OF_SOUND: str,
    WorkhorseParameter.TRANSDUCER_DEPTH: str,
    WorkhorseParameter.PITCH: str,
    WorkhorseParameter.ROLL: str,
    WorkhorseParameter.SALINITY: str,
    WorkhorseParameter.COORDINATE_TRANSFORMATION: str,
    WorkhorseParameter.SENSOR_SOURCE: str,
    WorkhorseParameter.DATA_STREAM_SELECTION: str,
    WorkhorseParameter.ENSEMBLE_PER_BURST: str,
    WorkhorseParameter.TIME_PER_ENSEMBLE: str,
    WorkhorseParameter.TIME_OF_FIRST_PING: str,
    WorkhorseParameter.TIME_PER_PING: str,
    WorkhorseParameter.TIME: str,
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: str,
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: str,
    WorkhorseParameter.BANDWIDTH_CONTROL: str,
    WorkhorseParameter.CORRELATION_THRESHOLD: str,
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: str,
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: str,
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: str,
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: int,
    WorkhorseParameter.RECEIVER_GAIN_SELECT: str,
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: str,
    WorkhorseParameter.PINGS_PER_ENSEMBLE: str,
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: int,
    WorkhorseParameter.DEPTH_CELL_SIZE: str,
    WorkhorseParameter.TRANSMIT_LENGTH: str,
    WorkhorseParameter.PING_WEIGHT: str,
    WorkhorseParameter.AMBIGUITY_VELOCITY: str,
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: str,
    WorkhorseParameter.GET_STATUS_INTERVAL: str,
    WorkhorseParameter.SYNC_PING_ENSEMBLE: str,
    WorkhorseParameter.RDS3_MODE_SEL: str,
    WorkhorseParameter.SLAVE_TIMEOUT: str,
    WorkhorseParameter.SYNCH_DELAY: str,
}

parameter_types = {
    WorkhorseParameter.SERIAL_DATA_OUT: ParameterDictType.STRING,
    WorkhorseParameter.SERIAL_FLOW_CONTROL: ParameterDictType.STRING,
    WorkhorseParameter.BANNER: ParameterDictType.BOOL,
    WorkhorseParameter.INSTRUMENT_ID: ParameterDictType.INT,
    WorkhorseParameter.SLEEP_ENABLE: ParameterDictType.INT,
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: ParameterDictType.BOOL,
    WorkhorseParameter.POLLED_MODE: ParameterDictType.BOOL,
    WorkhorseParameter.XMIT_POWER: ParameterDictType.INT,
    WorkhorseParameter.LATENCY_TRIGGER: ParameterDictType.BOOL,
    WorkhorseParameter.HEADING_ALIGNMENT: ParameterDictType.INT,
    WorkhorseParameter.HEADING_BIAS: ParameterDictType.INT,
    WorkhorseParameter.SPEED_OF_SOUND: ParameterDictType.INT,
    WorkhorseParameter.TRANSDUCER_DEPTH: ParameterDictType.INT,
    WorkhorseParameter.PITCH: ParameterDictType.INT,
    WorkhorseParameter.ROLL: ParameterDictType.INT,
    WorkhorseParameter.SALINITY: ParameterDictType.INT,
    WorkhorseParameter.COORDINATE_TRANSFORMATION: ParameterDictType.STRING,
    WorkhorseParameter.SENSOR_SOURCE: ParameterDictType.STRING,
    WorkhorseParameter.DATA_STREAM_SELECTION: ParameterDictType.INT,
    WorkhorseParameter.ENSEMBLE_PER_BURST: ParameterDictType.INT,
    WorkhorseParameter.TIME_PER_ENSEMBLE: ParameterDictType.STRING,
    WorkhorseParameter.TIME_OF_FIRST_PING: ParameterDictType.STRING,
    WorkhorseParameter.TIME_PER_PING: ParameterDictType.STRING,
    WorkhorseParameter.TIME: ParameterDictType.STRING,
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: ParameterDictType.STRING,
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: ParameterDictType.STRING,
    WorkhorseParameter.BANDWIDTH_CONTROL: ParameterDictType.INT,
    WorkhorseParameter.CORRELATION_THRESHOLD: ParameterDictType.INT,
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: ParameterDictType.STRING,
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: ParameterDictType.INT,
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: ParameterDictType.INT,
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: ParameterDictType.BOOL,
    WorkhorseParameter.RECEIVER_GAIN_SELECT: ParameterDictType.INT,
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: ParameterDictType.INT,
    WorkhorseParameter.PINGS_PER_ENSEMBLE: ParameterDictType.INT,
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: ParameterDictType.BOOL,
    WorkhorseParameter.DEPTH_CELL_SIZE: ParameterDictType.INT,
    WorkhorseParameter.TRANSMIT_LENGTH: ParameterDictType.INT,
    WorkhorseParameter.PING_WEIGHT: ParameterDictType.INT,
    WorkhorseParameter.AMBIGUITY_VELOCITY: ParameterDictType.INT,
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: ParameterDictType.STRING,
    WorkhorseParameter.GET_STATUS_INTERVAL: ParameterDictType.STRING,
    WorkhorseParameter.SYNC_PING_ENSEMBLE: ParameterDictType.STRING,
    WorkhorseParameter.RDS3_MODE_SEL: ParameterDictType.INT,
    WorkhorseParameter.SLAVE_TIMEOUT: ParameterDictType.INT,
    WorkhorseParameter.SYNCH_DELAY: ParameterDictType.INT,
}

parameter_names = {
    WorkhorseParameter.SERIAL_DATA_OUT: "Serial Data Out",
    WorkhorseParameter.SERIAL_FLOW_CONTROL: "Serial Flow Control",
    WorkhorseParameter.BANNER: "Banner",
    WorkhorseParameter.INSTRUMENT_ID: "Instrument ID",
    WorkhorseParameter.SLEEP_ENABLE: "Sleep Enable",
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: "Save NVRAM to Recorder",
    WorkhorseParameter.POLLED_MODE: "Polled Mode",
    WorkhorseParameter.XMIT_POWER: "Transmit Power",
    WorkhorseParameter.LATENCY_TRIGGER: "Latency trigger",
    WorkhorseParameter.HEADING_ALIGNMENT: "Heading Alignment",
    WorkhorseParameter.HEADING_BIAS: "Heading Bias",
    WorkhorseParameter.SPEED_OF_SOUND: 'Speed of Sound',
    WorkhorseParameter.TRANSDUCER_DEPTH: 'Transducer Depth',
    WorkhorseParameter.PITCH: 'Pitch',
    WorkhorseParameter.ROLL: 'Roll',
    WorkhorseParameter.SALINITY: 'Salinity',
    WorkhorseParameter.COORDINATE_TRANSFORMATION: 'Coordinate Transformation',
    WorkhorseParameter.SENSOR_SOURCE: 'Sensor Source',
    WorkhorseParameter.DATA_STREAM_SELECTION: 'Data Stream Selection',
    WorkhorseParameter.ENSEMBLE_PER_BURST: 'Ensemble per Burst',
    WorkhorseParameter.TIME_PER_ENSEMBLE: 'Time per Ensemble',
    WorkhorseParameter.TIME_OF_FIRST_PING: 'Time of First Ping',
    WorkhorseParameter.TIME_PER_PING: 'Time per Ping',
    WorkhorseParameter.TIME: 'Time',
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: 'Buffered Output Period',
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: 'False Target Threshold',
    WorkhorseParameter.BANDWIDTH_CONTROL: 'Bandwidth Control',
    WorkhorseParameter.CORRELATION_THRESHOLD: 'Correlation Threshold',
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: 'Serial Out FW Switches',
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: 'Error Velocity Threshold',
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: 'Blank After Transmit',
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: 'Clip Data Past Bottom',
    WorkhorseParameter.RECEIVER_GAIN_SELECT: 'Receiver Gain Select',
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: 'Number of Depth Cells',
    WorkhorseParameter.PINGS_PER_ENSEMBLE: 'Pings Per Ensemble',
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: 'Sample Ambient Sound',
    WorkhorseParameter.DEPTH_CELL_SIZE: 'Depth Cell Size',
    WorkhorseParameter.TRANSMIT_LENGTH: 'Transmit Length',
    WorkhorseParameter.PING_WEIGHT: 'Ping Weight',
    WorkhorseParameter.AMBIGUITY_VELOCITY: 'Ambiguity Velocity',
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: 'Clock Sync Interval',
    WorkhorseParameter.GET_STATUS_INTERVAL: 'Acquire Status Interval',
    WorkhorseParameter.SYNC_PING_ENSEMBLE: 'Sync Ping Ensemble',
    WorkhorseParameter.RDS3_MODE_SEL: 'RDS3 Mode Selection',
    WorkhorseParameter.SLAVE_TIMEOUT: 'Slave Timeout',
    WorkhorseParameter.SYNCH_DELAY: 'Sync Delay'
}

parameter_descriptions = {
    WorkhorseParameter.SERIAL_DATA_OUT: 'Firmware switches for serial data types collected by the ADCP. See manual for usage.',
    WorkhorseParameter.SERIAL_FLOW_CONTROL: 'Sets various ADCP dta flow-control parameters. See manual for firmware switches.',
    WorkhorseParameter.BANNER: 'Enable suppressing the banner: (true | false)',
    WorkhorseParameter.SLEEP_ENABLE: 'Enable sleeping between pings: (true | false)',
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: 'Disable saving NVRAM to recorder at the end of a deployment: (true | false)',
    WorkhorseParameter.POLLED_MODE: 'Enable ADCP to be polled for data: (true | false)',
    WorkhorseParameter.XMIT_POWER: 'Allow transmit power to be set high or low: (0 - 255)',
    WorkhorseParameter.LATENCY_TRIGGER: 'Enable the low latency trigger input: (true | false)',
    WorkhorseParameter.TIME_PER_ENSEMBLE: 'Minimum interval between data collection cycles.',
    WorkhorseParameter.TIME_OF_FIRST_PING: 'Time ADCP wakes up to start data collection.',
    WorkhorseParameter.TIME_PER_PING: 'Minimum time between pings.',
    WorkhorseParameter.TIME: 'Time of internal real-time clock from last clock sync.',
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: 'Minimum interval between buffered data outputs.',
    WorkhorseParameter.BANDWIDTH_CONTROL: 'Profiling mode 1 bandwidth: (0:Wide | 1:Narrow)',
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: 'Enable flagging of velocity data as bad (true | false)',
    WorkhorseParameter.RECEIVER_GAIN_SELECT: 'Receiver gain: (0:reduce receiver gain by 40 dB | 1:normal receiver gain)',
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: 'Enable ambient sound samples (true | false)',
    WorkhorseParameter.PING_WEIGHT: 'Ensemble weighting method: (0:Box | 1:Triangle)',
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: 'Interval to schedule clock synchronization.',
    WorkhorseParameter.GET_STATUS_INTERVAL: 'Interval to schedule acquire status.',
    WorkhorseParameter.INSTRUMENT_ID: "Identification of the ADCP: (0 - 255)",
    WorkhorseParameter.HEADING_ALIGNMENT: "Correction for physical misalignment between Beam 3 and the heading reference: (-17999 to 18000)",
    WorkhorseParameter.HEADING_BIAS: "Correction for electrical/magnetic bias between heading value and heading reference: (-17999 to 18000)",
    WorkhorseParameter.SPEED_OF_SOUND: 'Speed of sound value used for ADCP data processing: (1400 - 1600)',
    WorkhorseParameter.TRANSDUCER_DEPTH: 'Measurement from sea level to transducer faces: (0 - 65535)',
    WorkhorseParameter.PITCH: 'Pitch/tilt 1 angle: (-6000 - 6000)',
    WorkhorseParameter.ROLL: 'Roll/tilt 2 angle: (-6000 - 6000)',
    WorkhorseParameter.SALINITY: 'Salinity of the water: (0 - 40)',
    WorkhorseParameter.COORDINATE_TRANSFORMATION: 'Firmware switches for velocity and percent-good data. See manual for usage.',
    WorkhorseParameter.SENSOR_SOURCE: 'Firmware switches to use data from manual settings or from an associated sensor. See manual for usage.',
    WorkhorseParameter.DATA_STREAM_SELECTION: 'Type of ensemble output data structure: (0 - 18)',
    WorkhorseParameter.ENSEMBLE_PER_BURST: 'Number of ensembles per burst: (0 - 65535)',
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: 'False target threshold and starting bin (000-255,000-255)',
    WorkhorseParameter.CORRELATION_THRESHOLD: 'Minimum threshold of water-track data that must meet correlation criteria: (0 - 255)',
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: 'Firmware switches for data types collected by the ADCP. See manual for usage.',
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: 'Maximum error velocity for good water-current data: (0 - 5000)',
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: 'Moves location of first depth cell away from transducer head: (0 - 9999)',
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: 'Number of depth cells over which the ADCP collects data: (1 - 255)',
    WorkhorseParameter.PINGS_PER_ENSEMBLE: 'Number of pings to average in each data ensemble: (0 - 16384)',
    WorkhorseParameter.DEPTH_CELL_SIZE: 'Volume of water for one measurement cell: (40 - 3200)',
    WorkhorseParameter.TRANSMIT_LENGTH: 'Transmit length different from the depth cell length: (0 - 3200)',
    WorkhorseParameter.AMBIGUITY_VELOCITY: 'Radial ambiguity velocity: (2 - 700)',

    #VADCP Params
    WorkhorseParameter.SYNC_PING_ENSEMBLE: 'Firmware switches for synchronization pulse. See manual for usage.',
    WorkhorseParameter.RDS3_MODE_SEL: 'RDS3 Mode: (0:Off | 1:RDS3 master | 2:RDS3 slave | 3: NEMO)',
    WorkhorseParameter.SLAVE_TIMEOUT: 'Wait time to hear a synch pulse before slave proceeds: (0 - 10800)',
    WorkhorseParameter.SYNCH_DELAY: 'Wait time after sending a pulse: (0 - 65535)'
}

parameter_ranges = {
    WorkhorseParameter.BANNER: {'True': True, 'False': False},
    WorkhorseParameter.SLEEP_ENABLE: {'True': 1, 'False': 0},
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: {'True': True, 'False': False},
    WorkhorseParameter.POLLED_MODE: {'True': True, 'False': False},
    WorkhorseParameter.XMIT_POWER: (0, 255),
    WorkhorseParameter.LATENCY_TRIGGER: {'True': True, 'False': False},
    WorkhorseParameter.BANDWIDTH_CONTROL: {'Wide': 0, 'Narrow': 1},
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: {'True': True, 'False': False},
    WorkhorseParameter.RECEIVER_GAIN_SELECT: {'-40 dB Receiver Gain': 0, 'Normal': 1},
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: {'True': True, 'False': False},
    WorkhorseParameter.PING_WEIGHT: {'Box': 0, 'Triangle': 1},
    WorkhorseParameter.INSTRUMENT_ID: (0, 255),
    WorkhorseParameter.HEADING_ALIGNMENT: (-17999, 18000),
    WorkhorseParameter.HEADING_BIAS: (-17999, 18000),
    WorkhorseParameter.SPEED_OF_SOUND: (1400, 1600),
    WorkhorseParameter.TRANSDUCER_DEPTH: (0, 65535),
    WorkhorseParameter.PITCH: (-6000, 6000),
    WorkhorseParameter.ROLL: (-6000, 6000),
    WorkhorseParameter.SALINITY: (0, 40),
    WorkhorseParameter.DATA_STREAM_SELECTION: (0, 18),
    WorkhorseParameter.ENSEMBLE_PER_BURST: (0, 65535),
    WorkhorseParameter.CORRELATION_THRESHOLD: (0, 255),
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: (0, 5000),
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: (0, 9999),
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: (1, 255),
    WorkhorseParameter.PINGS_PER_ENSEMBLE: (0, 16384),
    WorkhorseParameter.DEPTH_CELL_SIZE: (40, 3200),
    WorkhorseParameter.TRANSMIT_LENGTH: (0, 3200),
    WorkhorseParameter.AMBIGUITY_VELOCITY: (2, 480),

    #VADCP Params
    WorkhorseParameter.RDS3_MODE_SEL: {'Off': 0, 'RDS3 master': 1, 'RDS3 slave': 2, 'NEMO': 3},
    WorkhorseParameter.SLAVE_TIMEOUT: (0, 10800),
    WorkhorseParameter.SYNCH_DELAY: (0, 65535)
}

parameter_units = {
    WorkhorseParameter.HEADING_ALIGNMENT: Prefixes.CENTI + Units.DEGREE_PLANE_ANGLE,
    WorkhorseParameter.HEADING_BIAS: Prefixes.CENTI + Units.DEGREE_PLANE_ANGLE,
    WorkhorseParameter.SPEED_OF_SOUND: Units.METER + '/' + Units.SECOND,
    WorkhorseParameter.TRANSDUCER_DEPTH: Prefixes.DECI + Units.METER,
    WorkhorseParameter.PITCH: Prefixes.CENTI + Units.DEGREE_PLANE_ANGLE,
    WorkhorseParameter.ROLL: Prefixes.CENTI + Units.DEGREE_PLANE_ANGLE,
    WorkhorseParameter.SALINITY: 'ppt',
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: Units.CENTIMETER,
    WorkhorseParameter.DEPTH_CELL_SIZE: Units.CENTIMETER,
    WorkhorseParameter.TRANSMIT_LENGTH: Units.CENTIMETER,
    WorkhorseParameter.AMBIGUITY_VELOCITY: Units.CENTIMETER + '/' + Units.SECOND,
    WorkhorseParameter.TIME_PER_ENSEMBLE: 'hh:mm:ss:ff',
    WorkhorseParameter.TIME_OF_FIRST_PING: 'yy/mm/dd,hh:mm:ss',
    WorkhorseParameter.TIME_PER_PING: 'mm:ss:ff',
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: 'hh:mm:ss',
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: 'nnn,bbb',
    WorkhorseParameter.CORRELATION_THRESHOLD: Units.COUNTS,
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: Units.MILLIMETER + '/' + Units.SECOND,
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: 'hh:mm:ss',
    WorkhorseParameter.GET_STATUS_INTERVAL: 'hh:mm:ss',
    WorkhorseParameter.TIME: 'yyyy/mm/dd,hh:mm:ss',

    #VADCP Params
    WorkhorseParameter.SYNCH_DELAY: '0.1 '+ Units.MILLISECOND,
    WorkhorseParameter.SLAVE_TIMEOUT: Units.SECOND,
}

parameter_startup = {
    WorkhorseParameter.SERIAL_DATA_OUT: True,
    WorkhorseParameter.SERIAL_FLOW_CONTROL: True,
    WorkhorseParameter.BANNER: True,
    WorkhorseParameter.INSTRUMENT_ID: True,
    WorkhorseParameter.SLEEP_ENABLE: True,
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: True,
    WorkhorseParameter.POLLED_MODE: True,
    WorkhorseParameter.XMIT_POWER: True,
    WorkhorseParameter.LATENCY_TRIGGER: True,
    WorkhorseParameter.HEADING_ALIGNMENT: True,
    WorkhorseParameter.HEADING_BIAS: True,
    WorkhorseParameter.SPEED_OF_SOUND: True,
    WorkhorseParameter.TRANSDUCER_DEPTH: True,
    WorkhorseParameter.PITCH: True,
    WorkhorseParameter.ROLL: True,
    WorkhorseParameter.SALINITY: True,
    WorkhorseParameter.COORDINATE_TRANSFORMATION: True,
    WorkhorseParameter.SENSOR_SOURCE: True,
    WorkhorseParameter.DATA_STREAM_SELECTION: True,
    WorkhorseParameter.ENSEMBLE_PER_BURST: True,
    WorkhorseParameter.TIME_PER_ENSEMBLE: True,
    WorkhorseParameter.TIME_OF_FIRST_PING: False,
    WorkhorseParameter.TIME_PER_PING: True,
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: True,
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: True,
    WorkhorseParameter.BANDWIDTH_CONTROL: True,
    WorkhorseParameter.CORRELATION_THRESHOLD: True,
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: True,
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: True,
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: True,
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: True,
    WorkhorseParameter.RECEIVER_GAIN_SELECT: True,
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: True,
    WorkhorseParameter.PINGS_PER_ENSEMBLE: True,
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: True,
    WorkhorseParameter.DEPTH_CELL_SIZE: True,
    WorkhorseParameter.TRANSMIT_LENGTH: True,
    WorkhorseParameter.PING_WEIGHT: True,
    WorkhorseParameter.AMBIGUITY_VELOCITY: True,
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: True,
    WorkhorseParameter.GET_STATUS_INTERVAL: True,
    WorkhorseParameter.SYNC_PING_ENSEMBLE: True,
    WorkhorseParameter.RDS3_MODE_SEL: True,
    WorkhorseParameter.SLAVE_TIMEOUT: True,
    WorkhorseParameter.SYNCH_DELAY: True,
}

parameter_direct = {
    WorkhorseParameter.SERIAL_DATA_OUT: True,
    WorkhorseParameter.SERIAL_FLOW_CONTROL: True,
    WorkhorseParameter.BANNER: True,
    WorkhorseParameter.INSTRUMENT_ID: True,
    WorkhorseParameter.SLEEP_ENABLE: True,
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: True,
    WorkhorseParameter.POLLED_MODE: True,
    WorkhorseParameter.XMIT_POWER: True,
    WorkhorseParameter.LATENCY_TRIGGER: True,
    WorkhorseParameter.HEADING_ALIGNMENT: True,
    WorkhorseParameter.HEADING_BIAS: True,
    WorkhorseParameter.SPEED_OF_SOUND: True,
    WorkhorseParameter.TRANSDUCER_DEPTH: True,
    WorkhorseParameter.PITCH: True,
    WorkhorseParameter.ROLL: True,
    WorkhorseParameter.SALINITY: True,
    WorkhorseParameter.COORDINATE_TRANSFORMATION: True,
    WorkhorseParameter.SENSOR_SOURCE: True,
    WorkhorseParameter.DATA_STREAM_SELECTION: True,
    WorkhorseParameter.ENSEMBLE_PER_BURST: True,
    WorkhorseParameter.TIME_PER_ENSEMBLE: True,
    WorkhorseParameter.TIME_OF_FIRST_PING: False,
    WorkhorseParameter.TIME_PER_PING: True,
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: True,
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: True,
    WorkhorseParameter.BANDWIDTH_CONTROL: True,
    WorkhorseParameter.CORRELATION_THRESHOLD: True,
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: True,
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: True,
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: True,
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: True,
    WorkhorseParameter.RECEIVER_GAIN_SELECT: True,
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: True,
    WorkhorseParameter.PINGS_PER_ENSEMBLE: True,
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: True,
    WorkhorseParameter.DEPTH_CELL_SIZE: True,
    WorkhorseParameter.TRANSMIT_LENGTH: True,
    WorkhorseParameter.PING_WEIGHT: True,
    WorkhorseParameter.AMBIGUITY_VELOCITY: True,
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: False,
    WorkhorseParameter.GET_STATUS_INTERVAL: False,
    WorkhorseParameter.SYNC_PING_ENSEMBLE: True,
    WorkhorseParameter.RDS3_MODE_SEL: True,
    WorkhorseParameter.SLAVE_TIMEOUT: True,
    WorkhorseParameter.SYNCH_DELAY: True,
}

parameter_visibility = {
    WorkhorseParameter.SERIAL_DATA_OUT: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SERIAL_FLOW_CONTROL: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.BANNER: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.INSTRUMENT_ID: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SLEEP_ENABLE: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.POLLED_MODE: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.XMIT_POWER: ParameterDictVisibility.READ_WRITE,
    WorkhorseParameter.LATENCY_TRIGGER: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.HEADING_ALIGNMENT: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.HEADING_BIAS: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.COORDINATE_TRANSFORMATION: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.DATA_STREAM_SELECTION: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.ENSEMBLE_PER_BURST: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.TIME_OF_FIRST_PING: ParameterDictVisibility.READ_ONLY,
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SYNC_PING_ENSEMBLE: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.RDS3_MODE_SEL: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SLAVE_TIMEOUT: ParameterDictVisibility.IMMUTABLE,
    WorkhorseParameter.SYNCH_DELAY: ParameterDictVisibility.IMMUTABLE,
}

parameter_defaults = {
    WorkhorseParameter.SERIAL_DATA_OUT: '000 000 000',
    WorkhorseParameter.SERIAL_FLOW_CONTROL: '11110',
    WorkhorseParameter.BANNER: False,
    WorkhorseParameter.INSTRUMENT_ID: 0,
    WorkhorseParameter.SLEEP_ENABLE: 0,
    WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: True,
    WorkhorseParameter.POLLED_MODE: False,
    WorkhorseParameter.XMIT_POWER: 255,
    WorkhorseParameter.LATENCY_TRIGGER: False,
    WorkhorseParameter.HEADING_ALIGNMENT: 0,
    WorkhorseParameter.HEADING_BIAS: 0,
    WorkhorseParameter.SPEED_OF_SOUND: 1485,
    WorkhorseParameter.TRANSDUCER_DEPTH: 8000,
    WorkhorseParameter.PITCH: 0,
    WorkhorseParameter.ROLL: 0,
    WorkhorseParameter.SALINITY: 35,
    WorkhorseParameter.COORDINATE_TRANSFORMATION: '00111',
    WorkhorseParameter.SENSOR_SOURCE: '1111101',
    WorkhorseParameter.DATA_STREAM_SELECTION: 0,
    WorkhorseParameter.ENSEMBLE_PER_BURST: 0,
    WorkhorseParameter.TIME_PER_ENSEMBLE: '00:00:00.00',
    WorkhorseParameter.TIME_PER_PING: '00:01.00',
    WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: '00:00:00',
    WorkhorseParameter.FALSE_TARGET_THRESHOLD: '050,001',
    WorkhorseParameter.BANDWIDTH_CONTROL: 0,
    WorkhorseParameter.CORRELATION_THRESHOLD: 64,
    WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: '111100000',
    WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: 2000,
    WorkhorseParameter.BLANK_AFTER_TRANSMIT: 704,
    WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: False,
    WorkhorseParameter.RECEIVER_GAIN_SELECT: 1,
    WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: 100,
    WorkhorseParameter.PINGS_PER_ENSEMBLE: 1,
    WorkhorseParameter.SAMPLE_AMBIENT_SOUND: False,
    WorkhorseParameter.DEPTH_CELL_SIZE: 800,
    WorkhorseParameter.TRANSMIT_LENGTH: 0,
    WorkhorseParameter.PING_WEIGHT: 0,
    WorkhorseParameter.AMBIGUITY_VELOCITY: 175,
    WorkhorseParameter.CLOCK_SYNCH_INTERVAL: '00:00:00',
    WorkhorseParameter.GET_STATUS_INTERVAL: '00:00:00',
    WorkhorseParameter.SYNC_PING_ENSEMBLE: '001',
    WorkhorseParameter.RDS3_MODE_SEL: 0,
    WorkhorseParameter.SLAVE_TIMEOUT: 0,
    WorkhorseParameter.SYNCH_DELAY: 0,
}


#
# Particle Regex's'
#
ADCP_PD0_PARSED_REGEX = r'\x7f\x7f(..)'
ADCP_PD0_PARSED_REGEX_MATCHER = re.compile(ADCP_PD0_PARSED_REGEX, re.DOTALL)
ADCP_SYSTEM_CONFIGURATION_REGEX = r'Instrument S/N.*?>'
ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER = re.compile(ADCP_SYSTEM_CONFIGURATION_REGEX, re.DOTALL)
ADCP_COMPASS_CALIBRATION_REGEX = r'ACTIVE FLUXGATE CALIBRATION MATRICES in NVRAM.*?>'
ADCP_COMPASS_CALIBRATION_REGEX_MATCHER = re.compile(ADCP_COMPASS_CALIBRATION_REGEX, re.DOTALL)
ADCP_ANCILLARY_SYSTEM_DATA_REGEX = r'Ambient +Temperature.*?>'
ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER = re.compile(ADCP_ANCILLARY_SYSTEM_DATA_REGEX, re.DOTALL)
ADCP_TRANSMIT_PATH_REGEX = r'IXMT +=.*?>'
ADCP_TRANSMIT_PATH_REGEX_MATCHER = re.compile(ADCP_TRANSMIT_PATH_REGEX, re.DOTALL)


# noinspection PyUnusedLocal
class WorkhorseProtocol(CommandResponseInstrumentProtocol):
    """
    Specialization for this version of the workhorse driver
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

        # Build Workhorse protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(WorkhorseProtocolState, WorkhorseProtocolEvent,
                                           DriverEvent.ENTER, DriverEvent.EXIT)

        handlers = {
            WorkhorseProtocolState.UNKNOWN: (
                (WorkhorseProtocolEvent.ENTER, self._handler_unknown_enter),
                (WorkhorseProtocolEvent.EXIT, self._handler_unknown_exit),
                (WorkhorseProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ),
            WorkhorseProtocolState.COMMAND: (
                (WorkhorseProtocolEvent.ENTER, self._handler_command_enter),
                (WorkhorseProtocolEvent.EXIT, self._handler_command_exit),
                (WorkhorseProtocolEvent.GET, self._handler_get),
                (WorkhorseProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample),
                (WorkhorseProtocolEvent.SET, self._handler_command_set),
                (WorkhorseProtocolEvent.CLOCK_SYNC, self._handler_command_clock_sync),
                (WorkhorseProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_command_clock_sync),
                (WorkhorseProtocolEvent.SCHEDULED_GET_STATUS, self._handler_command_acquire_status),
                (WorkhorseProtocolEvent.START_DIRECT, self._handler_command_start_direct),
                (WorkhorseProtocolEvent.RUN_TEST, self._handler_command_run_test_200),
                (WorkhorseProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status),
                (WorkhorseProtocolEvent.RECOVER_AUTOSAMPLE, self._handler_command_recover_autosample),
            ),
            WorkhorseProtocolState.AUTOSAMPLE: (
                (WorkhorseProtocolEvent.ENTER, self._handler_autosample_enter),
                (WorkhorseProtocolEvent.EXIT, self._handler_autosample_exit),
                (WorkhorseProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample),
                (WorkhorseProtocolEvent.GET, self._handler_get),
                (WorkhorseProtocolEvent.SCHEDULED_CLOCK_SYNC, self._handler_autosample_clock_sync),
                (WorkhorseProtocolEvent.SCHEDULED_GET_STATUS, self._handler_autosample_acquire_status),
            ),
            WorkhorseProtocolState.DIRECT_ACCESS: (
                (WorkhorseProtocolEvent.ENTER, self._handler_direct_access_enter),
                (WorkhorseProtocolEvent.EXIT, self._handler_direct_access_exit),
                (WorkhorseProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct),
                (WorkhorseProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct),
            )
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Build dictionaries for driver schema
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        # Add build handlers for device commands.
        self._add_build_handler(WorkhorseInstrumentCmds.OUTPUT_CALIBRATION_DATA, self._build_simple_command)
        self._add_build_handler(WorkhorseInstrumentCmds.START_LOGGING, self._build_simple_command)
        self._add_build_handler(WorkhorseInstrumentCmds.GET_SYSTEM_CONFIGURATION, self._build_simple_command)
        self._add_build_handler(WorkhorseInstrumentCmds.RUN_TEST_200, self._build_simple_command)
        self._add_build_handler(WorkhorseInstrumentCmds.SET, self._build_set_command)
        self._add_build_handler(WorkhorseInstrumentCmds.GET, self._build_get_command)
        self._add_build_handler(WorkhorseInstrumentCmds.OUTPUT_PT2, self._build_simple_command)
        self._add_build_handler(WorkhorseInstrumentCmds.OUTPUT_PT4, self._build_simple_command)

        # Add response handlers
        self._add_response_handler(WorkhorseInstrumentCmds.OUTPUT_CALIBRATION_DATA, self._response_passthrough)
        self._add_response_handler(WorkhorseInstrumentCmds.GET_SYSTEM_CONFIGURATION, self._response_passthrough)
        self._add_response_handler(WorkhorseInstrumentCmds.RUN_TEST_200, self._response_passthrough)
        self._add_response_handler(WorkhorseInstrumentCmds.SET, self._parse_set_response)
        self._add_response_handler(WorkhorseInstrumentCmds.GET, self._parse_get_response)
        self._add_response_handler(WorkhorseInstrumentCmds.OUTPUT_PT2, self._response_passthrough)
        self._add_response_handler(WorkhorseInstrumentCmds.OUTPUT_PT4, self._response_passthrough)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(WorkhorseProtocolState.UNKNOWN)

        # commands sent sent to device to be
        # filtered in responses for telnet DA
        self._sent_cmds = []
        self.disable_autosample_recover = False
        self._chunker = StringChunker(self.sieve_function)
        self.initialize_scheduler()

        # dictionary to store last transmitted metadata particle values
        # so we can not send updates when nothing changed
        self._last_values = {}

    def _build_param_dict(self):
        for param in parameter_regexes:
            self._param_dict.add(param,
                                 parameter_regexes.get(param),
                                 parameter_extractors.get(param),
                                 parameter_formatters.get(param),
                                 type=parameter_types.get(param),
                                 display_name=parameter_names.get(param),
                                 description=parameter_descriptions.get(param),
                                 range=parameter_ranges.get(param),
                                 startup_param=parameter_startup.get(param, False),
                                 direct_access=parameter_direct.get(param, False),
                                 visibility=parameter_visibility.get(param, ParameterDictVisibility.READ_WRITE),
                                 default_value=parameter_defaults.get(param),
                                 units=parameter_units.get(param))

        self._param_dict.set_default(WorkhorseParameter.CLOCK_SYNCH_INTERVAL)
        self._param_dict.set_default(WorkhorseParameter.GET_STATUS_INTERVAL)

    @staticmethod
    def sieve_function(raw_data):
        """
        Chunker sieve method to help the chunker identify chunks.
        @returns a list of chunks identified, if any.
        The chunks are all the same type.
        """
        sieve_matchers = [ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                          ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                          ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                          ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                          ADCP_PD0_PARSED_REGEX_MATCHER]

        return_list = []

        for matcher in sieve_matchers:
            if matcher == ADCP_PD0_PARSED_REGEX_MATCHER:
                #
                # Have to cope with variable length binary records...
                # lets grab the length, then write a proper query to
                # snag it.
                #
                matcher2 = re.compile(r'\x7f\x7f(..)', re.DOTALL)
                for match in matcher2.finditer(raw_data):
                    length = struct.unpack('<H', match.group(1))[0]
                    end_index = match.start() + length
                    # read the checksum and compute our own
                    # if they match we have a PD0 record
                    if len(raw_data) > end_index + 1:
                        checksum = struct.unpack_from('<H', raw_data, end_index)[0]
                        calculated = sum(bytearray(raw_data[match.start():end_index])) & 0xffff
                        if checksum == calculated:
                            # include the checksum in our match... (2 bytes)
                            return_list.append((match.start(), end_index + 2))
            else:
                for match in matcher.finditer(raw_data):
                    return_list.append((match.start(), match.end()))

        return return_list

    def _build_command_dict(self):
        """
        Build command dictionary
        """
        self._cmd_dict.add(WorkhorseCapability.START_AUTOSAMPLE,
                           display_name="Start Autosample",
                           description="Place the instrument into autosample mode")
        self._cmd_dict.add(WorkhorseCapability.STOP_AUTOSAMPLE,
                           display_name="Stop Autosample",
                           description="Exit autosample mode and return to command mode")
        self._cmd_dict.add(WorkhorseCapability.CLOCK_SYNC,
                           display_name="Synchronize Clock")
        self._cmd_dict.add(WorkhorseCapability.RUN_TEST,
                           display_name="Run Test 200")
        self._cmd_dict.add(WorkhorseCapability.ACQUIRE_STATUS, timeout=30,
                           display_name="Acquire Status")
        self._cmd_dict.add(WorkhorseCapability.DISCOVER,
                           display_name="Discover State")
        self._cmd_dict.add(WorkhorseCapability.DISCOVER, display_name='Discover')

    # #######################################################################
    # Private helpers.
    # #######################################################################
    def _changed(self, particle):
        stream = particle.get('stream_name')
        values = particle.get('values')
        last_values = self._last_values.get(stream)
        if values == last_values:
            return False

        self._last_values[stream] = values
        return True

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """
        if ADCP_PD0_PARSED_REGEX_MATCHER.match(chunk):
            pd0 = AdcpPd0Record(chunk)
            transform = pd0.coord_transform.coord_transform
            if transform == Pd0CoordinateTransformType.BEAM:
                science = Pd0BeamParticle(pd0, port_timestamp=timestamp).generate()
            elif transform == Pd0CoordinateTransformType.EARTH:
                science = Pd0EarthParticle(pd0, port_timestamp=timestamp).generate()
            else:
                raise SampleException('Received unknown coordinate transform type: %s' % transform)

            # generate the particles

            config = AdcpPd0ConfigParticle(pd0, port_timestamp=timestamp).generate()
            engineering = AdcpPd0EngineeringParticle(pd0, port_timestamp=timestamp).generate()

            out_particles = [science]
            for particle in [config, engineering]:
                if self._changed(particle):
                    out_particles.append(particle)

            for particle in out_particles:
                self._driver_event(DriverAsyncEvent.SAMPLE, particle)

            if self.get_current_state() == WorkhorseProtocolState.COMMAND:
                self._async_raise_fsm_event(WorkhorseProtocolEvent.RECOVER_AUTOSAMPLE)
            log.debug("_got_chunk - successful match for AdcpPd0ParsedDataParticle")

        elif self._extract_sample(AdcpCompassCalibrationDataParticle,
                                  ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.debug("_got_chunk - successful match for AdcpCompassCalibrationDataParticle")

        elif self._extract_sample(AdcpSystemConfigurationDataParticle,
                                  ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.debug("_got_chunk - successful match for AdcpSystemConfigurationDataParticle")

        elif self._extract_sample(AdcpAncillarySystemDataParticle,
                                  ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.trace("_got_chunk - successful match for AdcpAncillarySystemDataParticle")

        elif self._extract_sample(AdcpTransmitPathParticle,
                                  ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                                  chunk,
                                  timestamp):
            log.trace("_got_chunk - successful match for AdcpTransmitPathParticle")

    def _send_break_cmd(self, delay):
        """
        Send a BREAK to attempt to wake the device.
        """
        self._connection.send_break(delay)

    def stop_scheduled_job(self, schedule_job):
        """
        Remove the scheduled job
        @param schedule_job scheduling job.
        """
        if self._scheduler is not None:
            try:
                self._remove_scheduler(schedule_job)
            except KeyError:
                log.warn("_remove_scheduler could not find %s", schedule_job)

    def start_scheduled_job(self, param, schedule_job, protocol_event):
        """
        Add a scheduled job
        """
        self.stop_scheduled_job(schedule_job)
        val = self._param_dict.get(param)

        try:
            hours, minutes, seconds = [int(x) for x in val.split(':', 2)]
        except ValueError:
            raise InstrumentParameterException('Bad schedule string! Expected format HH:MM:SS, received %r' % val)

        if sum((hours, minutes, seconds)) > 0:
            config = {
                DriverConfigKey.SCHEDULER: {
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
            self.set_init_params(config)
            self._add_scheduler_event(schedule_job, protocol_event)

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if WorkhorseCapability.has(x)]

    def _sync_clock(self, command, date_time_param, timeout=TIMEOUT, delay=1, time_format="%Y/%m/%d,%H:%M:%S"):
        """
        Send the command to the instrument to synchronize the clock
        @param command set command
        @param date_time_param: date time parameter that we want to set
        @param timeout: command timeout
        @param delay: wakeup delay
        @param time_format: time format string for set command
        """
        str_val = get_timestamp_delayed(time_format)
        self._do_cmd_direct(date_time_param + str_val + NEWLINE)
        time.sleep(1)
        self._get_response(TIMEOUT)

    # #######################################################################
    # Startup parameter handlers
    ########################################################################

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary.
        """
        # see if we passed in a list of parameters to query
        # if not, use the whole parameter list
        parameters = kwargs.get('params')
        if parameters is None or WorkhorseParameter.ALL in parameters:
            parameters = WorkhorseParameter.list()
        # filter out the engineering parameters and ALL
        parameters = [p for p in parameters if not WorkhorseEngineeringParameter.has(p) and p != WorkhorseParameter.ALL]

        # Get old param dict config.
        old_config = self._param_dict.get_config()

        if parameters:
            # Clear out the line buffer / prompt buffer
            # Send ALL get commands sequentially, then grab them all at once
            self._linebuf = ''
            self._promptbuf = ''
            command = ''.join(['%s?%s' % (p, NEWLINE) for p in parameters])
            self._do_cmd_direct(command)
            regex = re.compile(r'(%s.*?%s.*?>)' % (parameters[0], parameters[-1]), re.DOTALL)
            resp = self._get_response(response_regex=regex)
            self._param_dict.update_many(resp)

        new_config = self._param_dict.get_config()

        # Check if there is any changes. Ignore TT
        if not dict_equal(new_config, old_config, ['TT']) or kwargs.get('force'):
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        self._verify_not_readonly(*args, **kwargs)
        params = args[0]
        changed = []

        old_config = self._param_dict.get_config()

        commands = []
        for key, val in params.iteritems():
            if WorkhorseEngineeringParameter.has(key):
                continue
            if val != old_config.get(key):
                changed.append(key)
                commands.append(self._build_set_command(WorkhorseInstrumentCmds.SET, key, val))

        if commands:
            # we are going to send the concatenation of all our set commands
            self._linebuf = ''
            self._do_cmd_direct(''.join(commands))
            # we'll need to build a regular expression to retrieve all of the responses
            # including any possible errors
            if len(commands) == 1:
                regex = re.compile(r'(%s.*?)\r\n>' % commands[-1].strip(), re.DOTALL)
            else:
                regex = re.compile(r'(%s.*?%s.*?)\r\n>' % (commands[0].strip(), commands[-1].strip()), re.DOTALL)
            response = self._get_response(response_regex=regex)
            self._parse_set_response(response[0], None)

        # Handle engineering parameters
        force = False

        if WorkhorseParameter.CLOCK_SYNCH_INTERVAL in params:
            if (params[WorkhorseParameter.CLOCK_SYNCH_INTERVAL] != self._param_dict.get(
                    WorkhorseParameter.CLOCK_SYNCH_INTERVAL)):
                self._param_dict.set_value(WorkhorseParameter.CLOCK_SYNCH_INTERVAL,
                                           params[WorkhorseParameter.CLOCK_SYNCH_INTERVAL])
                self.start_scheduled_job(WorkhorseParameter.CLOCK_SYNCH_INTERVAL, WorkhorseScheduledJob.CLOCK_SYNC,
                                         WorkhorseProtocolEvent.SCHEDULED_CLOCK_SYNC)
                force = True

        if WorkhorseParameter.GET_STATUS_INTERVAL in params:
            if (params[WorkhorseParameter.GET_STATUS_INTERVAL] != self._param_dict.get(
                    WorkhorseParameter.GET_STATUS_INTERVAL)):
                self._param_dict.set_value(WorkhorseParameter.GET_STATUS_INTERVAL,
                                           params[WorkhorseParameter.GET_STATUS_INTERVAL])
                self.start_scheduled_job(WorkhorseParameter.GET_STATUS_INTERVAL,
                                         WorkhorseScheduledJob.GET_CONFIGURATION,
                                         WorkhorseProtocolEvent.SCHEDULED_GET_STATUS)
                force = True

        self._update_params(params=changed, force=force)
        return None

    def _send_break(self, duration=1000):
        """
        Send a BREAK to attempt to wake the device.
        """
        self._linebuf = ''
        self._promptbuf = ''
        self._send_break_cmd(duration)
        self._get_response(expected_prompt=WorkhorsePrompt.BREAK)

    def _send_wakeup(self):
        """
        Send a newline to attempt to wake the device.
        """
        self._connection.send(NEWLINE)

    def _start_logging(self, timeout=TIMEOUT):
        """
        Command the instrument to start logging
        @param timeout: how long to wait for a prompt
        @throws: InstrumentProtocolException if failed to start logging
        """
        self._do_cmd_resp(WorkhorseInstrumentCmds.START_LOGGING, timeout=timeout)

    def _stop_logging(self):
        self._send_break()

    def _discover(self):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE or UNKNOWN.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentStateException if the device response does not correspond to
        an expected state.
        """
        next_state = None

        try:
            self._wakeup(3)
            next_state = WorkhorseProtocolState.COMMAND
        except InstrumentTimeoutException:
            # TODO - should verify that a particle is being received (e.g. wait_for_particle for 1 sec, otherwise throw exception
            next_state = WorkhorseProtocolState.AUTOSAMPLE

        return next_state

    def _run_test(self, *args, **kwargs):
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = WorkhorsePrompt.COMMAND
        return self._do_cmd_resp(WorkhorseInstrumentCmds.RUN_TEST_200, *args, **kwargs)

    @contextmanager
    def _pause_logging(self):
        self._send_break()
        try:
            yield
        finally:
            self._start_logging()

    ########################################################################
    # UNKNOWN handlers.
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

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE.
        """
        next_state = self._discover()
        result = []

        return next_state, (next_state, result)

    ########################################################################
    # COMMAND handlers.
    ########################################################################

    def _handler_command_run_test_200(self, *args, **kwargs):
        next_state = None
        result = self._run_test(*args, **kwargs)
        return next_state, (next_state, [result])

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        log.debug('_handler_command_enter: init_type: %r', self._init_type)
        if self._init_type != InitializationType.NONE:
            self._update_params()
            self._init_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        """
        next_state = WorkhorseProtocolState.AUTOSAMPLE
        result = []

        # Issue start command and switch to autosample if successful.
        try:
            self._sync_clock(WorkhorseInstrumentCmds.SET, WorkhorseParameter.TIME)
            self._start_logging()

        except InstrumentException:
            self._stop_logging()
            raise

        return next_state, (next_state, result)

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : params dict.
        @throws InstrumentParameterException if missing set parameters, if set parameters not ALL and
        not a dict, or if parameter can't be properly formatted.
        @throws InstrumentTimeoutException if device cannot be woken for set command.
        @throws InstrumentProtocolException if set command could not be built or misunderstood.
        """
        next_state = None
        result = []
        startup = False

        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('_handler_command_set Set command requires a parameter dict.')

        try:
            startup = args[1]
        except IndexError:
            pass

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')

        self._set_params(params, startup)
        return next_state, result

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change
        """
        next_state = None
        result = []

        timeout = kwargs.get('timeout', TIMEOUT)
        self._sync_clock(WorkhorseInstrumentCmds.SET, WorkhorseParameter.TIME, timeout)
        return next_state, (next_state, result)

    def _do_acquire_status(self, *args, **kwargs):
        self._do_cmd_resp(WorkhorseInstrumentCmds.GET_SYSTEM_CONFIGURATION, *args, **kwargs),
        self._do_cmd_resp(WorkhorseInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs),
        self._do_cmd_resp(WorkhorseInstrumentCmds.OUTPUT_PT2, *args, **kwargs),
        self._do_cmd_resp(WorkhorseInstrumentCmds.OUTPUT_PT4, *args, **kwargs)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        execute a get status
        @return next_state, next_state, result) if successful.
        @throws InstrumentProtocolException from _do_cmd_resp.
        """
        next_state = None

        self._do_acquire_status(*args, **kwargs)

        result = self.wait_for_particles([WorkhorseDataParticleType.ADCP_SYSTEM_CONFIGURATION,
                                          WorkhorseDataParticleType.ADCP_COMPASS_CALIBRATION,
                                          WorkhorseDataParticleType.ADCP_ANCILLARY_SYSTEM_DATA,
                                          WorkhorseDataParticleType.ADCP_TRANSMIT_PATH])

        return next_state, (next_state, result)

    def _handler_command_start_direct(self, *args, **kwargs):
        next_state = WorkhorseProtocolState.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    def _handler_command_recover_autosample(self):
        next_state = WorkhorseProtocolState.AUTOSAMPLE
        result = []
        return next_state, (next_state, result)

    ######################################################
    # AUTOSAMPLE handlers
    ######################################################

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        if self._init_type != InitializationType.NONE:
            with self._pause_logging():
                self._update_params()
                self._init_params()

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        incorrect prompt received.
        """
        next_state = WorkhorseProtocolState.COMMAND
        result = []
        self._stop_logging()
        return next_state, (next_state, result)

    def _handler_autosample_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change from
        autosample mode.  For this command we have to move the instrument
        into command mode, do the clock sync, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        next_state = None
        result = []

        with self._pause_logging():
            self._handler_command_clock_sync()

        return next_state, (next_state, result)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        execute a get status on the leading edge of a second change
        @throws InstrumentProtocolException from _do_cmd_resp
        """
        next_state = None
        result = []

        with self._pause_logging():
            self._handler_command_acquire_status(*args, **kwargs)

        return next_state, (next_state, result)

    ######################################################
    # DIRECT_ACCESS handlers
    ######################################################

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
        self._stop_logging()

    def _handler_direct_access_execute_direct(self, data):
        next_state = None
        result = []
        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)
        return next_state, (next_state, result)

    def _handler_direct_access_stop_direct(self):
        next_state = WorkhorseProtocolState.COMMAND
        result = []
        return next_state, (next_state, result)

    ########################################################################
    # build handlers.
    ########################################################################

    def _build_set_command(self, cmd, param, val):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """
        try:
            str_val = self._param_dict.format(param, val)
            set_cmd = '%s%s%s' % (param, str_val, NEWLINE)
        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter. %s' % param)
        except ValueError:
            raise InstrumentParameterException('Cannot format parameter value: %s %s' % (param, val))

        return set_cmd

    def _build_get_command(self, cmd, param, **kwargs):
        """
        param? followed by newline.
        @param cmd get command
        @param param the parameter key to get.
        @return The get command to be sent to the device.
        """
        self.get_param = param
        get_cmd = param + '?' + NEWLINE
        return get_cmd

    ########################################################################
    # response handlers.
    ########################################################################

    def _response_passthrough(self, response, prompt):
        """
        Return the output from the calibration request base 64 encoded
        """
        return response

    def _parse_get_response(self, response, prompt):
        if 'ERR' in response:
            raise InstrumentProtocolException(
                'Protocol._parse_get_response : Get command not recognized: %s' % response)

        self._param_dict.update(response)
        return response

    def _parse_set_response(self, response, prompt):
        """
        Parse handler for set command.
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if set command misunderstood.
        """
        if 'ERR' in response:
            raise InstrumentParameterException('Error setting parameter: %s' % response)
        return response


def create_playback_protocol(callback):
    return WorkhorseProtocol(None, None, callback)