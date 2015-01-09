"""
@package mi.instrument.KML.driver
@file marine-integrations/mi/instrument/KML/driver.py
@author Sung Ahn
@brief Driver for the KML family
Release notes:
"""
import struct

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import re
import base64
import time
import functools

import sys
from mi.core.common import BaseEnum
from mi.core.time import get_timestamp_delayed

from mi.core.exceptions import InstrumentParameterException, NotImplementedException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentTimeoutException

from mi.core.log import get_logger
log = get_logger()
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver, DriverConnectionState
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.driver_scheduler import DriverSchedulerConfigKey
from mi.core.driver_scheduler import TriggerType
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.util import dict_equal


# default timeout.
TIMEOUT = 20

# newline.
NEWLINE = '\r\n'

DEFAULT_CMD_TIMEOUT = 20
DEFAULT_WRITE_DELAY = 0

ZERO_TIME_INTERVAL = '00:00:00'


class KMLPrompt(BaseEnum):
    """
    Device i/o prompts..
    """
    END = '>'


class ParameterIndex(BaseEnum):
    SET = 0
    GET = 1
    Start = 2
    LENGTH = 3
    DEFAULT_DATA = 4
    DISPLAY_NAME = 5
    DESCRIPTION = 6
    KEY = 7
    D_DEFAULT = 8

class KMLParameter(DriverParameter):
    """
    Device parameters
    """
    #
    # set-able parameters
    #
    """
    set <\x16:NT:\x05\x00\x00157.237.237.101\x00>
    Byte1 = Interval in second (5 seconds)
    Byte2 & 3 = 16 bit Integer Port Nr (if 0 use default port 123)
    Byte 4 to end = Server name in ASCII with \ 0 end of string
    Default is 5 seconds, 0 for port #123, server name = 157.237.237.104,
    ReadOnly

    get <\x03:GN:>
    GN + Variable number of bytes.
    Byte1 = Interval in seconds
    Byte2 & 3 = 16 bit Integer Port Nr
    Byte 4 to end = Server name in ASCII with \0 end of string

    """
    NTP_SETTING =('NT', '<\x03:GN:>', 1, None, '\x00\x00157.237.237.104\x00', 'NTP Setting',
                  'interval(in second), NTP port, NTP Server name', 'NTP_SETTING', 255)

    """
    set <\x04:CL:\x00>
    variable number of bytes representing \0 terminated ASCII string.
    (\0 only) indicates files are saved in the default location on the camera

    get <\x03:FL:>
    Byte1 to end = Network location as an ASCII string with \0 end of string. Send only \0 character to set default
    """
    NETWORK_DRIVE_LOCATION = ('CL','<\x04:CL:\x00>', 1, None, '\x00',
                              'Network Drive Location','\x00 for local default location', 'NETWORK_DRIVE_LOCATION', 0)


    """
    set <\x04:RM:\x01>
    1 byte with value \x01 or \x02
    0x1 = Replace oldest image on the disk
    0x2 = return NAK (or stop capture) when disk is full.
    Default is 0x1 and it is ReadOnly :set <0x04:RM:0x01>

    get <0x03:GM:>
    GM + 1 byte with value 0x1 or 0x2 0x1 = Replace oldest image on the disk
    0x2 = return NAK (or stop capture) when disk is full.
    """
    WHEN_DISK_IS_FULL = ('RM', '<\x03:GM:>', 1, 1, '\x01', 'When Disk Is Full',
                         '1 = Replace oldest image on the disk, 2 = return ERROR when disk is full',
                         'WHEN_DISK_IS_FULL', 1)

    """
    Camera Mode

    set <\x04:SV:\x09>
    1 byte:
    0x00 = None (off)
    0x09 = Stream
    0x0A = Framing
    0x0B = Focus
    Default is 0x09 <0x04:SV:0x09>

    get <0x03:GV:>
    GV + 1 byte:
    0x00 = None
    0x09 = Stream
    0x0A = Framing
    0x0B = Area of Interest
    """

    CAMERA_MODE = ('SV', '<\x03:GV:>', 1, 1, '\x09', 'Camera Mode',
                   '0 = None (off), 9 = Stream, 10 = Framing, 11 = Focus', 'CAMERA_MODE', 9)

    """
    set <\x04:FR:\x1E>
    1 Byte with value between 1 and 30. If the requested frame rate cannot be achieved, the maximum rate will be used
    Default is 0x1E : set <0x04:FR:0x1E>

    get <0x03:GR:>
    GR + 1 Byte with value between 1 and 30.
    If the requested frame rate cannot be achieved, the maximum rate will be used.
    """
    FRAME_RATE = ('FR', '<\x03:GR:>', 1, 1, '\x1E', 'Frame Rate', 'From 1 to 30 frames in second', 'FRAME_RATE', 30)

    """
    set <\x04:SD:\x01>
    1 Byte with a dividing value of 0x1, 0x2, 0x4 and 0x8.
    0x1 = Full resolution
    0x2 = 1/2 Full resolution etc
    Default is 0x1 : set <0x04:SD:0x01>,

    get <0x03:GD:>
    GD + 1 Byte with a dividing value of 0x1, 0x2, 0x4 and 0x8.
    """
    IMAGE_RESOLUTION = ('SD', '<\x03:GD:>', 1, 1, '\x01',
                        'Image Resolution','1 = Full resolution, 2 = half Full resolution', 'IMAGE_RESOLUTION', 1)

    """
    set <\x04:CD:\x64>
    1 Byte with value between 0x01 and 0x64. (decimal 1 - 100)
    0x64 = Minimum data loss
    0x01 = Maximum data loss
    Default is 0x64 : set <0x04:CD:0x64>

    get <0x03:GI:>
    GI + 1 Byte with value between 0x01 and 0x64. (decimal 1 - 100) 0x64 = Minimum data loss 0x01 = Maximum data loss
    """
    COMPRESSION_RATIO = ('CD', '<\x03:GI:>',1, 1, '\x64', 'Compression Ratio',
                         '100 = Minimum data loss, 1 = Maximum data loss', 'COMPRESSION_RATIO', 100)

    """
    set <\x05:ET:\xFF\xFF>
    2 bytes.
    Byte1 = Value (starting value)
    Byte2 = Exponent (Number of zeros to add)
    Default is 0xFF 0xFF (auto shutter mode) : set <0x05:ET:0xFF0xFF>

    get <0x03:EG:>
    EG + 2 bytes
    Byte1 = Value (starting value)
    Byte2 = Multiplier (Number of zeros to add)
    e.g.. If Byte1 = 25 and byte2 = 3 then exposure time will be 25000 Max value allowed is 60000000 microseconds
    (if both bytes are set to 0xFF, the camera is in auto shutter mode)
    """
    SHUTTER_SPEED = ('ET', '<\x03:EG:>', 1, 2, '\xFF\xFF', 'Shutter Speed',
                     'Byte1 = Value (starting value), Byte2 = Exponent (Number of zeros to add)', 'SHUTTER_SPEED',
                     '255:255')

    """
    get <\x04:GS:\xFF>
    byte Value 0x01 to 0x20 sets a static value and 0xFF sets auto gain.
    In automatic gain control, the camera will attempt to adjust the gain to give the optimal exposure.
    Default is 0xFF : set <\x04:GS:\xFF>

    get <0x03:GG:>
    GG + 1 byte
    Value 0x01 to 0x20 for a static value and 0xFF for auto GAIN
    """
    CAMERA_GAIN = ('GS', '<\x03:GG:>',1,1, '\xFF', 'Camera Gain','From 1 to 32 and 255 sets auto gain',
                  'CAMERA_GAIN', 255)

    """
    set <\x05:BF:\x03\x32>
    Byte 1 is lamp to control: 0x01 = Lamp1 0x02 = Lamp2 0x03 = Both Lamps
    Byte 2 is brightness between 0x00 and 0x64
    Default is 0x03 0x32

    set <0x03:PF:>
    PF + 2 bytes
    1st byte for lamp 1
    2nd byte for lamp 2. For each lamp, MSB indicates On/Off
    """
    LAMP_BRIGHTNESS = ('BF', '<\x03:PF:>', 1, 2, '\x03\x32','Lamp Brightness',
                       'Byte 1 is lamp to control: 1 = Lamp1, 2 = Lamp2, 3 = Both Lamps, Byte 2 is brightness between 0 and 100',
                       'LAMP_BRIGHTNESS', '3:50')

    """
    Set <\x04:FX:\x00>
    Set focus speed
    1 byte between 0x00 and 0x0F
    Default is 0x00 : set <\x04:FX:\x00>

    No get focus speed
    ???set <0x03:FP:>
    ???FP + 1 byte between \x00 and \xC8
    """
    FOCUS_SPEED = ('FX', None, 1, 1, '\x00', 'Focus Speed','between 0 and 15', 'FOCUS_SPEED', 0)

    """
    set <\x04:ZX:\x00>
    Set zoom speed.
    1 byte between 0x00 and 0x0F
    Default is 0x00 : set <\x04:ZX:\x00>

    ???get <0x03:ZP:>
    ???ZP + 1 byte between 0x00 and 0xC8
    """
    ZOOM_SPEED = ('ZX', None, 1, 1, '\x00', 'Zoom Speed', 'between 0 and 15', 'ZOOM_SPEED', 0)

    """
    Set <\x04:IG:\x08>
    Iris_Position
    1 byte between 0x00 and 0x0F
    default is 0x08 <0x04:IG:0x08>

    IP + 1 byte between 0x00
    get <0x03:IP>
    """
    IRIS_POSITION = ('IG', '<\x03:IP:>', 1,1, '\x08', 'Iris Position', 'between 0 and 15', 'IRIS_POSITION', 8)

    """
    Zoom Position
    set <\x04:ZG:\x64>

    1 byte between 0x00 and 0xC8 (200 Zoom positions)
    Default value is <0x04:ZG:0x64>
    ZP + 1 byte between 0x00 and 0xC8
    get <0x03:ZP:>
    """
    ZOOM_POSITION = ('ZG', '<\x03:ZP:>', 1, 1, '\x64', 'Zoom Position', 'between 0 and 200', 'ZOOM_POSITION', 100)

    """
    Pan Speed
    1 byte between 0x00 and 0x64
    Default is 0x32 : set <0x04:DS:0x32>

    ???? No get pan speed
    """
    PAN_SPEED = ('DS', None, None, None, '\x32', 'Pan Speed', 'between 0 and 100', 'PAN_SPEED', 50)

    """
    Set tilt speed
    1 byte between 0x00 and 0x64
    Default is 0x32 : <0x04:TA:0x32>
    """
    TILT_SPEED = ('TA', None, None, None, '\x32', 'TILT Speed','between 0 and 100', 'TILT_SPEED', 50)

    """
    Enable or disable the soft end stops of the pan and tilt device
    1 byte:0x00 = Disable 0x01 = Enable
    Default is 0x01 : <0x04:ES:0x01>

    get <0x03:AS:>
    AS + 7 bytes
    Byte1 = Tilt Position hundreds
    Byte2 = Tilt Position tens
    Byte3 = Tilt Position units
    Byte4 = Pan Position hundreds
    Byte5 = Pan Position tens Byte6 = Pan Position units
    Byte7 = End stops enable (0x1 = enabled, 0x0 = disabled)
    Bytes 1 to 6 are ASCII characters between 0x30 and 0x39
    """
    SOFT_END_STOPS = ('ES', '<\x03:AS:>', 7, 1, '\x01', 'Soft End Stops','0 = Disable, 1 = Enable',
                      'SOFT_END_STOPS', 1)

    """
    3 Bytes representing a three letter string containing the required pan location.
    Byte1 = Hundreds of degrees
    Byte2 = Tens of degrees
    Byte 3 = Units of degrees
    (e.g.. 90 = 0x30, 0x37, 0x35 or 360 = 0x33, 0x36, 0x30)
    Default is 90 degree : <0x06:PP:0x30 0x37 0x35>

    get <0x03:AS:>
    AS + 7 bytes
    Byte1 = Tilt Position hundreds
    Byte2 = Tilt Position tens
    Byte3 = Tilt Position units
    Byte4 = Pan Position hundreds
    Byte5 = Pan Position tens Byte6 = Pan Position units
    Byte7 = End stops enable (0x1 = enabled, 0x0 = disabled)
    Bytes 1 to 6 are ASCII characters between 0x30 and 0x39
    """
    PAN_POSITION = ('PP', '<\x03:AS:>', 4, 3,'\x30\x37\x35','Pan Position',
                    'Byte1 = Hundreds of degrees, Byte2 = Tens of degrees, Byte 3 = Units of degrees',
                    'PAN_POSITION', 90)

    """
    3 Bytes representing a three letter string containing the required tilt location.
    Byte1 = Hundreds of degrees
    Byte2 = Tens of degrees
    Byte 3 = Units of degrees
    (e.g.. 90 = 0x30, 0x37, 0x35 or 360 = 0x33, 0x36, 0x30)

    get <0x03:AS:>
    AS + 7 bytes
    Byte1 = Tilt Position hundreds
    Byte2 = Tilt Position tens
    Byte3 = Tilt Position units
    Byte4 = Pan Position hundreds
    Byte5 = Pan Position tens Byte6 = Pan Position units
    Byte6 = Pan Position Pan Position units
    Byte7 = End stops enable (0x1 = enabled, 0x0 = disabled)
    Bytes 1 to 6 are ASCII characters between 0x30 and 0x39
    """
    TILT_POSITION = ('TP', '<\x03:AS:>',1, 3, '\x30\x37\x35', 'Tilt Position',
                    'Byte1 = Hundreds of degrees, Byte2 = Tens of degrees, Byte 3 = Units of degrees',
                    'TILT_POSITION', 90)

    """
    set <\x04:FG:\x64>
    1 byte between 0x00 and 0xC8

    get <\x03:FP:>
    """
    FOCUS_POSITION = ('FG', '<\x03:FP:>', 1, 1, '\x64', 'Focus Position', 'between 0 and 200', 'FOCUS_POSITION', 100)

    # Engineering parameters for the scheduled commands
    SAMPLE_INTERVAL = (None, None, None, None, '00:00:30', 'Sample Interval',
                       'hh:mm:ss', 'SAMPLE_INTERVAL', '00:00:30')
    ACQUIRE_STATUS_INTERVAL = (None, None, None, None, '00:00:00', 'Acquire Status Interval',
                               'hh:mm:ss', 'ACQUIRE_STATUS_INTERVAL', '00:00:00')
    VIDEO_FORWARDING = (None, None, None, None, False, 'Video Forwarding Flag',
                        'True - Turn on Video, False - Turn off video', 'VIDEO_FORWARDING', False)
    VIDEO_FORWARDING_TIMEOUT = (None, None, None, None, '01:00:00', 'video forwarding timeout',
                                'hh:mm:ss', 'VIDEO_FORWARDING_TIMEOUT', '01:00:00')
    PRESET_NUMBER = (None, None, None, None, 1, 'Preset number', 'preset number (1- 15)', 'PRESET_NUMBER', 1)
    AUTO_CAPTURE_DURATION = (None, None, None, None, '00:00:03', 'Auto Capture Duration', 'hh:mm:ss, 1 to 5 Seconds',
                             'AUTO_CAPTURE_DURATION', '00:00:03')

class KMLParameter_display(DriverParameter):
    ACQUIRE_STATUS_INTERVAL =  KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]
    AUTO_CAPTURE_DURATION = KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]
    CAMERA_GAIN = KMLParameter.CAMERA_GAIN[ParameterIndex.KEY]
    CAMERA_MODE = KMLParameter.CAMERA_MODE[ParameterIndex.KEY]
    COMPRESSION_RATIO = KMLParameter.COMPRESSION_RATIO[ParameterIndex.KEY]
    FOCUS_POSITION = KMLParameter.FOCUS_POSITION[ParameterIndex.KEY]
    FOCUS_SPEED = KMLParameter.FOCUS_SPEED[ParameterIndex.KEY]
    FRAME_RATE = KMLParameter.FRAME_RATE[ParameterIndex.KEY]
    IMAGE_RESOLUTION = KMLParameter.IMAGE_RESOLUTION[ParameterIndex.KEY]
    IRIS_POSITION = KMLParameter.IRIS_POSITION[ParameterIndex.KEY]

    LAMP_BRIGHTNESS = KMLParameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]
    NETWORK_DRIVE_LOCATION = KMLParameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY]
    NTP_SETTING = KMLParameter.NTP_SETTING[ParameterIndex.KEY]
    PAN_POSITION = KMLParameter.PAN_POSITION[ParameterIndex.KEY]
    PAN_SPEED = KMLParameter.PAN_SPEED[ParameterIndex.KEY]
    PRESET_NUMBER = KMLParameter.PRESET_NUMBER[ParameterIndex.KEY]
    SAMPLE_INTERVAL = KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY]
    SHUTTER_SPEED = KMLParameter.SHUTTER_SPEED[ParameterIndex.KEY]
    SOFT_END_STOPS = KMLParameter.SOFT_END_STOPS[ParameterIndex.KEY]
    TILT_POSITION = KMLParameter.TILT_POSITION[ParameterIndex.KEY]
    TILT_SPEED = KMLParameter.TILT_SPEED[ParameterIndex.KEY]

    VIDEO_FORWARDING = KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY]
    VIDEO_FORWARDING_TIMEOUT = KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]
    WHEN_DISK_IS_FULL = KMLParameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY]
    ZOOM_POSITION = KMLParameter.ZOOM_POSITION[ParameterIndex.KEY]
    ZOOM_SPEED = KMLParameter.ZOOM_SPEED[ParameterIndex.KEY]


class KMLInstrumentCmds(BaseEnum):
    """
    Device specific commands
    Represents the commands the driver implements and the string that
    must be sent to the instrument to execute the command.
    """

    START_CAPTURE = 'SP'
    STOP_CAPTURE = 'SR'

    TAKE_SNAPSHOT = 'CI'

    START_FOCUS_NEAR = 'FN'
    START_FOCUS_FAR = 'FF'
    STOP_FOCUS = 'FS'

    START_ZOOM_OUT = 'ZW'
    START_ZOOM_IN = 'ZT'
    STOP_ZOOM = 'ZS'

    INCREASE_IRIS = 'II'
    DECREASE_IRIS = 'ID'

    START_PAN_LEFT = 'PL'
    START_PAN_RIGHT = 'PR'
    STOP_PAN = 'PS'

    START_TILT_UP = 'TU'
    START_TILT_DOWN = 'TD'
    STOP_TILT = 'TS'

    GO_TO_PRESET = 'XG'

    TILE_UP_SOFT = 'UT'
    TILE_DOWN_SOFT = 'DT'
    PAN_LEFT_SOFT = 'AW'
    PAN_RIGHT_SOFT = 'CW'
    SET_PRESET = 'XS'

    LAMP_ON = 'OF'
    LAMP_OFF = 'NF'

    LASER_ON = 'OL'
    LASER_OFF = 'NL'

    GET_DISK_USAGE = 'GC'
    HEALTH_REQUEST = 'HS'

    GET = 'get'
    SET = 'set'


class CAMDSProtocolState(DriverProtocolState):
    """
    Base states for driver protocols. Subclassed for specific driver
    protocols.
    """


class KMLProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = CAMDSProtocolState.UNKNOWN
    COMMAND = CAMDSProtocolState.COMMAND
    AUTOSAMPLE = CAMDSProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = CAMDSProtocolState.DIRECT_ACCESS


class KMLProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    INIT_PARAMS = DriverEvent.INIT_PARAMS
    DISCOVER = DriverEvent.DISCOVER

    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT

    GET = DriverEvent.GET
    SET = DriverEvent.SET

    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT

    PING_DRIVER = DriverEvent.PING_DRIVER

    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE

    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE

    LASER_1_ON = "DRIVER_EVENT_LASER_1_ON"
    LASER_2_ON = "DRIVER_EVENT_LASER_2_ON"
    LASER_BOTH_ON = "DRIVER_EVENT_LASER_BOTH_ON"
    LASER_1_OFF = "DRIVER_EVENT_LASER_1_OFF"
    LASER_2_OFF = "DRIVER_EVENT_LASER_2_OFF"
    LASER_BOTH_OFF = "DRIVER_EVENT_LASER_BOTH_OFF"

    LAMP_ON = "DRIVER_EVENT_LAMP_ON"
    LAMP_OFF = "DRIVER_EVENT_LAMP_OFF"
    SET_PRESET = "DRIVER_EVENT_SET_PRESET"
    GOTO_PRESET = "DRIVER_EVENT_GOTO_PRESET"

    EXECUTE_AUTO_CAPTURE = 'DRIVER_EVENT_EXECUTE_AUTO_CAPTURE'
    STOP_CAPTURE = 'DRIVER_EVENT_STOP_CAPTURE'

    STOP_FORWARD = 'DRIVER_EVENT_STOP_FORWARD'


class KMLCapability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE

    ACQUIRE_STATUS = KMLProtocolEvent.ACQUIRE_STATUS
    ACQUIRE_SAMPLE = KMLProtocolEvent.ACQUIRE_SAMPLE

    EXECUTE_AUTO_CAPTURE = KMLProtocolEvent.EXECUTE_AUTO_CAPTURE
    STOP_CAPTURE = KMLProtocolEvent.STOP_CAPTURE

    LASER_1_ON = KMLProtocolEvent.LASER_1_ON
    LASER_2_ON = KMLProtocolEvent.LASER_2_ON
    LASER_BOTH_ON = KMLProtocolEvent.LASER_BOTH_ON
    LASER_1_OFF = KMLProtocolEvent.LASER_1_OFF
    LASER_2_OFF = KMLProtocolEvent.LASER_2_OFF
    LASER_BOTH_OFF = KMLProtocolEvent.LASER_BOTH_OFF

    LAMP_ON = KMLProtocolEvent.LAMP_ON
    LAMP_OFF = KMLProtocolEvent.LAMP_OFF

    SET_PRESET = KMLProtocolEvent.SET_PRESET
    GOTO_PRESET = KMLProtocolEvent.GOTO_PRESET


class KMLScheduledJob(BaseEnum):
    SAMPLE = 'sample'
    VIDEO_FORWARDING = "video forwarding"
    STATUS = "status"
    STOP_CAPTURE = "stop capturing"


class KMLInstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver Family SubClass
    """

    def __init__(self, evt_callback):
        """
        Driver constructor.
        @param evt_callback Driver process event callback.
        """
        # Construct superclass.
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)


# noinspection PyMethodMayBeStatic
class KMLProtocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol Family SubClass
    """

    #'ACK' reply from the instrument, indicating command successfully processed
    ACK = '\x06'

    #'NAK' reply from the instrument, indicating bad command sent to the instrument
    NAK = '\x15'

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """

        self.last_wakeup = 0
        self.video_forwarding_flag = False

        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        self.last_wakeup = 0

        # Build ADCPT protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(KMLProtocolState, KMLProtocolEvent,
                                           KMLProtocolEvent.ENTER, KMLProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(KMLProtocolState.UNKNOWN, KMLProtocolEvent.ENTER,
                                       self._handler_unknown_enter)
        self._protocol_fsm.add_handler(KMLProtocolState.UNKNOWN, KMLProtocolEvent.EXIT,
                                       self._handler_unknown_exit)
        self._protocol_fsm.add_handler(KMLProtocolState.UNKNOWN, KMLProtocolEvent.DISCOVER,
                                       self._handler_unknown_discover)

        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.ENTER,
                                       self._handler_command_enter)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.EXIT,
                                       self._handler_command_exit)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.INIT_PARAMS,
                                       self._handler_command_init_params)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.GET,
                                       self._handler_get)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.START_AUTOSAMPLE,
                                       self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.SET,
                                       self._handler_command_set)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.START_DIRECT,
                                       self._handler_command_start_direct)

        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.ACQUIRE_SAMPLE,
                                       self._handler_command_acquire_sample)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LAMP_ON,
                                       self._handler_command_lamp_on)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LAMP_OFF,
                                       self._handler_command_lamp_off)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LASER_1_ON,
                                       self._handler_command_laser1_on)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LASER_2_ON,
                                       self._handler_command_laser2_on)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LASER_BOTH_ON,
                                       self._handler_command_laser_both_on)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LASER_1_OFF,
                                       self._handler_command_laser1_off)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LASER_2_OFF,
                                       self._handler_command_laser2_off)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.LASER_BOTH_OFF,
                                      self._handler_command_laser_both_off)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.SET_PRESET,
                                       self._handler_command_set_preset)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.GOTO_PRESET,
                                       self._handler_command_goto_preset)
        # self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.START_CAPTURE,
        #                                self._handler_command_start_capture)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.STOP_CAPTURE,
                                       self._handler_command_stop_capture)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.EXECUTE_AUTO_CAPTURE,
                                       self._handler_command_start_capture)
        self._protocol_fsm.add_handler(KMLProtocolState.COMMAND, KMLProtocolEvent.STOP_FORWARD,
                                       self._handler_command_stop_forward)

        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.ENTER,
                                       self._handler_autosample_enter)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.EXIT,
                                       self._handler_autosample_exit)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.INIT_PARAMS,
                                       self._handler_autosample_init_params)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.STOP_AUTOSAMPLE,
                                       self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.GET,
                                       self._handler_get)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.SET,
                                       self._handler_command_set)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.ACQUIRE_SAMPLE,
                                       self._handler_command_acquire_sample)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LAMP_ON,
                                       self._handler_command_lamp_on)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LAMP_OFF,
                                       self._handler_command_lamp_off)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LASER_1_ON,
                                       self._handler_command_laser1_on)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LASER_2_ON,
                                       self._handler_command_laser2_on)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LASER_BOTH_ON,
                                       self._handler_command_laser_both_on)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LASER_1_OFF,
                                       self._handler_command_laser1_off)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LASER_2_OFF,
                                       self._handler_command_laser2_off)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.LASER_BOTH_OFF,
                                      self._handler_command_laser_both_off)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.SET_PRESET,
                                       self._handler_command_set_preset)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.GOTO_PRESET,
                                       self._handler_command_goto_preset)
        # self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.START_CAPTURE,
        #                                self._handler_command_start_capture)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.STOP_CAPTURE,
                                       self._handler_command_stop_capture)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.EXECUTE_AUTO_CAPTURE,
                                       self._handler_command_start_capture)
        self._protocol_fsm.add_handler(KMLProtocolState.AUTOSAMPLE, KMLProtocolEvent.STOP_FORWARD,
                                       self._handler_command_stop_forward)

        self._protocol_fsm.add_handler(KMLProtocolState.DIRECT_ACCESS, KMLProtocolEvent.ENTER,
                                       self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(KMLProtocolState.DIRECT_ACCESS, KMLProtocolEvent.EXIT,
                                       self._handler_direct_access_exit)
        self._protocol_fsm.add_handler(KMLProtocolState.DIRECT_ACCESS, KMLProtocolEvent.EXECUTE_DIRECT,
                                       self._handler_direct_access_execute_direct)
        self._protocol_fsm.add_handler(KMLProtocolState.DIRECT_ACCESS, KMLProtocolEvent.STOP_DIRECT,
                                       self._handler_direct_access_stop_direct)

        # Build dictionaries for driver schema
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        ##################

        ##########
        # Add build handlers for device commands.

        self._add_build_handler(KMLInstrumentCmds.SET, self._build_set_command)
        self._add_build_handler(KMLInstrumentCmds.GET, self._build_get_command)

        self._add_build_handler(KMLInstrumentCmds.START_CAPTURE, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.STOP_CAPTURE, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.TAKE_SNAPSHOT, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.START_FOCUS_NEAR, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.START_FOCUS_FAR, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.STOP_FOCUS, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.START_ZOOM_OUT, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.START_ZOOM_IN, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.STOP_ZOOM, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.INCREASE_IRIS, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.DECREASE_IRIS, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.GO_TO_PRESET, self.build_preset_command)
        self._add_build_handler(KMLInstrumentCmds.SET_PRESET, self.build_preset_command)

        self._add_build_handler(KMLInstrumentCmds.START_PAN_LEFT, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.START_PAN_RIGHT, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.STOP_PAN, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.START_TILT_UP, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.START_TILT_DOWN, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.STOP_TILT, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.TILE_UP_SOFT, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.TILE_DOWN_SOFT, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.PAN_LEFT_SOFT, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.PAN_RIGHT_SOFT, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.LAMP_ON, self.build_simple_command)
        self._add_build_handler(KMLInstrumentCmds.LAMP_OFF, self.build_simple_command)

        self._add_build_handler(KMLInstrumentCmds.LASER_ON, self.build_laser_command)
        self._add_build_handler(KMLInstrumentCmds.LASER_OFF, self.build_laser_command)

        self._add_build_handler(KMLInstrumentCmds.GET_DISK_USAGE, self.build_status_command)
        self._add_build_handler(KMLInstrumentCmds.HEALTH_REQUEST, self.build_status_command)

        # add response_handlers
        self._add_response_handler(KMLInstrumentCmds.SET, self._parse_set_response)
        self._add_response_handler(KMLInstrumentCmds.GET, self._parse_get_response)

        self._add_response_handler(KMLInstrumentCmds.SET_PRESET, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.GO_TO_PRESET, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.LAMP_OFF, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.LAMP_ON, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.LASER_OFF, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.LASER_ON, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.PAN_LEFT_SOFT, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.PAN_RIGHT_SOFT, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.DECREASE_IRIS, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_CAPTURE, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_FOCUS_FAR, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_FOCUS_NEAR, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_PAN_RIGHT, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_PAN_LEFT, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_TILT_DOWN, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_TILT_UP, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.TILE_UP_SOFT, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.TILE_DOWN_SOFT, self._parse_simple_response)

        #Generate data particle
        self._add_response_handler(KMLInstrumentCmds.GET_DISK_USAGE, self._parse_simple_response)

        #Generate data particle
        self._add_response_handler(KMLInstrumentCmds.HEALTH_REQUEST, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_ZOOM_IN, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.START_ZOOM_OUT, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.INCREASE_IRIS, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.TAKE_SNAPSHOT, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.STOP_ZOOM, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.STOP_CAPTURE, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.STOP_FOCUS, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.STOP_PAN, self._parse_simple_response)
        self._add_response_handler(KMLInstrumentCmds.STOP_TILT, self._parse_simple_response)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(KMLProtocolState.UNKNOWN)

        # commands sent sent to device to be
        # filtered in responses for telnet DA
        self._sent_cmds = []

        self.disable_autosample_recover = False

    def convertDecToHex(self, int_value):
        """
        Convert decimal to hex
        """
        encoded = format(int_value, 'x')
        length = len(encoded)
        encoded = encoded.zfill(length+length%2)
        return encoded.decode('hex')

    def build_simple_command(self, cmd):

        command = '<\x03:%s:>' % cmd
        log.debug("Built simple command: %s" % command)
        return command

    def build_camds_5byte_command(self, cmd, *args):
        """
        Builder for 5-byte cam commands

        @param cmd The command to build
        @param args Unused arguments
        @retval Returns string ready for sending to instrument
        """
        data1 = struct.pack('!b', args[0])
        data2 = struct.pack('!b', args[1])
        return "<\x05:%s:%s%s>" % (cmd, data1, data2)

    def stop_scheduled_job(self, schedule_job):
        """
        Remove the scheduled job
        @param schedule_job scheduling job.
        """
        log.debug("Attempting to remove the scheduler")
        if self._scheduler is not None:
            try:
                self._remove_scheduler(schedule_job)
                log.debug("successfully removed scheduler")
            except KeyError:
                log.debug("_remove_scheduler could not find %s", schedule_job)

    def start_scheduled_job(self, param, schedule_job, protocol_event):
        """
        Add a scheduled job
        """
        self.stop_scheduled_job(schedule_job)

        interval = self._param_dict.get(param).split(':')
        hours = interval[0]
        minutes = interval[1]
        seconds = interval[2]
        log.debug("Setting scheduled interval to: %s %s %s", hours, minutes, seconds)

        if hours == '00' and minutes == '00' and seconds == '00':
            # if interval is all zeroed, then stop scheduling jobs
            self.stop_scheduled_job(schedule_job)
        else:
            config = {DriverConfigKey.SCHEDULER: {
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

    def _build_param_dict(self):
        """
        It will be implemented in its child
        @throw NotImplementedException
        """
        raise NotImplementedException('Not implemented.')

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if KMLCapability.has(x)]

    # #######################################################################
    # Startup parameter handlers
    ########################################################################
    def apply_startup_params(self):
        """
        Apply all startup parameters.  First we check the instrument to see
        if we need to set the parameters.  If they are they are set
        correctly then we don't do anything.

        If we need to set parameters then we might need to transition to
        command first.  Then we will transition back when complete.

        @throws: InstrumentProtocolException if not in command or streaming
        """
        # Let's give it a try in unknown state

        if (self.get_current_state() != KMLProtocolState.COMMAND and
                    self.get_current_state() != KMLProtocolState.AUTOSAMPLE):
            raise InstrumentProtocolException("Not in command or autosample state. Unable to apply startup params")

        # If we are in streaming mode and our configuration on the
        # instrument matches what we think it should be then we
        # don't need to do anything.

        if not self._instrument_config_dirty():
            return True

        error = None

        try:
            self._apply_params()

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            log.error("EXCEPTION WAS " + str(e))
            error = e

        if error:
            raise error

    def _apply_params(self):
        """
        apply startup parameters to the instrument.
        @throws: InstrumentProtocolException if in wrong mode.
        """
        log.debug("IN _apply_params")
        config = self.get_startup_config()
        del config['WHEN_DISK_IS_FULL']
        del config['NTP_SETTING']

        # Pass true to _set_params so we know these are startup values
        self._set_params(config, True)

    def _getattr_key(self, attr):
        return getattr(KMLParameter, attr)

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary.
        """
        log.debug("in _update_params")

        error = None
        results = None

        try:
            # Get old param dict config.
            old_config = self._param_dict.get_config()

            cmds = self._get_params()
            cmds.remove('__class__')

            results = ""
            for attr in sorted(cmds):

                if attr not in [ KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                                 KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                 KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                 KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                 KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                                 KMLParameter.PRESET_NUMBER[ParameterIndex.KEY],
                                 KMLParameter.NTP_SETTING[ParameterIndex.KEY],
                                 KMLParameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY],
                                 # TODO: CAMERA_GAIN shouldn't be in this list
                                 # waiting to sort out instrument issues regarding this parameter
                                 KMLParameter.CAMERA_GAIN[ParameterIndex.KEY],
                                 KMLParameter.FOCUS_SPEED[ParameterIndex.KEY],
                                 KMLParameter.PAN_SPEED[ParameterIndex.KEY],
                                 KMLParameter.TILT_SPEED[ParameterIndex.KEY],
                                 KMLParameter.ZOOM_SPEED[ParameterIndex.KEY],
                                 'ALL']:
                    if attr.startswith('__'):
                        return
                    if attr in ['dict', 'has', 'list', 'ALL']:
                        return
                    time.sleep(2)

                    key = self._getattr_key(attr)
                    result = self._do_cmd_resp(KMLInstrumentCmds.GET, key, **kwargs)
                    results += result + NEWLINE

            new_config = self._param_dict.get_config()

            if not dict_equal(new_config, old_config):
                self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            log.error("EXCEPTION in _update_params WAS " + str(e))
            error = e

        if error:
            raise error

        return results

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        log.trace("in _set_params")

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        result = None
        try:
            params = args[0]
            log.error("Sung _set_params params %r", params)
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)

        for key, val in params.iteritems():
            log.DEBUG("In _set_params, %s, %s", key, val)
            if key not in [ KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                            KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                            KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                            KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                            KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                            KMLParameter.PRESET_NUMBER[ParameterIndex.KEY],
                            KMLParameter.NTP_SETTING[ParameterIndex.KEY],
                            KMLParameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY],
                            'ALL'
                            ]:

                time.sleep(2)
                result = self._do_cmd_resp(KMLInstrumentCmds.SET, key, val, **kwargs)

                #Make sure the instrument has sufficient time to process this command
                if key in [KMLParameter.CAMERA_MODE[ParameterIndex.KEY],
                           KMLParameter.IMAGE_RESOLUTION[ParameterIndex.KEY]]:
                    log.error("Just set Camera parameters, sleeping for 15 seconds")
                    time.sleep(15)

        self._update_params()

        return result

    def _instrument_config_dirty(self):
        """
        Read the startup config and compare that to what the instrument
        is configured too.  If they differ then return True
        @return: True if the startup config doesn't match the instrument
        @throws: InstrumentParameterException
        """
        log.trace("in _instrument_config_dirty")

        startup_params = self._param_dict.get_startup_list()
        log.trace("Startup Parameters: %s" % startup_params)

        for param in startup_params:

            if self._param_dict.get(param) != self._param_dict.get_config_value(param):
                log.trace("DIRTY: %s %s != %s" % (
                    param, self._param_dict.get(param), self._param_dict.get_config_value(param)))
                return True

        log.trace("Clean instrument config")
        return False

    def _sanitize(self, s):
        s = s.replace('\xb3', '_')
        s = s.replace('\xbf', '_')
        s = s.replace('\xc0', '_')
        s = s.replace('\xd9', '_')
        s = s.replace('\xda', '_')
        s = s.replace('\xf8', '_')

        return s

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        # Command device to initialize parameters and send a config change event.
        self._protocol_fsm.on_event(KMLProtocolEvent.INIT_PARAMS)

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        self.stop_scheduled_job(KMLScheduledJob.SAMPLE)

        status_interval = self._param_dict.get(KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY])
        if status_interval != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                     KMLScheduledJob.STATUS,
                                     KMLProtocolEvent.ACQUIRE_STATUS)

        # start scheduled event for get_status only if the interval is not "00:00:00
        self.video_forwarding_flag = self._param_dict.get(KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY])
        self.forwarding_time = self._param_dict.get(KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY])
        if self.video_forwarding_flag == True:
            if self.forwarding_time != ZERO_TIME_INTERVAL:
                self.start_scheduled_job(KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                             KMLScheduledJob.VIDEO_FORWARDING,
                                             KMLProtocolEvent.STOP_FORWARD)


    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """

        log.debug("Entered handler_command_exit")
        self.stop_scheduled_job(KMLScheduledJob.STOP_CAPTURE)
        self.stop_scheduled_job(KMLScheduledJob.STATUS)
        self.stop_scheduled_job(KMLScheduledJob.VIDEO_FORWARDING)
        log.debug("Exit handler_command_exit")

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

    ######################################################
    #                                                    #
    ######################################################

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE.
        @return protocol_state, agent_state if successful
        """
        protocol_state, agent_state = self._discover()
        if protocol_state == KMLProtocolState.COMMAND:
            agent_state = ResourceAgentState.IDLE

        return protocol_state, agent_state

    ######################################################
    #                                                    #
    ######################################################
    def _handler_command_init_params(self, *args, **kwargs):
        """
        initialize parameters
        """
        next_state = None
        result = None

        self._init_params()
        return next_state, result

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        self._protocol_fsm.on_event(KMLProtocolEvent.INIT_PARAMS)
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """

    def _handler_autosample_init_params(self, *args, **kwargs):
        """
        initialize parameters.  For this instrument we need to
        put the instrument into command mode, apply the changes
        then put it back.
        """
        log.debug("in _handler_autosample_init_params")
        next_state = None
        result = None
        error = None

        try:
            self._init_params()

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            error = e

        if error:
            log.error("Error in init_param in handler_autosample_init_params: %s", error)
            raise error

        return next_state, result

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @return next_state, (next_agent_state, result) if successful.
        """
        result = None
        kwargs['timeout'] = 30

        log.error("Inside _handler_command_start_autosample")

        # first stop scheduled sampling
        self.stop_scheduled_job(KMLScheduledJob.SAMPLE)

        # start scheduled event for Sampling only if the interval is not "00:00:00
        sample_interval = self._param_dict.get(KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY])

        log.error("Sample Interval is %s" % sample_interval)

        if sample_interval != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY], KMLScheduledJob.SAMPLE,
                                     KMLProtocolEvent.ACQUIRE_SAMPLE)

        next_state = KMLProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        return next_state, (next_agent_state, result)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return  next_state, (next_agent_state, result) if successful.
        incorrect prompt received.
        """
        result = None

        # Wake up the device, continuing until autosample prompt seen.
        timeout = kwargs.get('timeout', TIMEOUT)

        next_state = KMLProtocolState.COMMAND
        next_agent_state = ResourceAgentState.COMMAND

        self.stop_scheduled_job(KMLScheduledJob.SAMPLE)

        return next_state, (next_agent_state, result)

    def _handler_recover_autosample(self, *args, **kwargs):
        """
        Reenter autosample mode.  Used when our data handler detects
        as data sample.
        @return next_state, next_agent_state
        """
        next_state = KMLProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        self._async_agent_state_change(ResourceAgentState.STREAMING)

        return next_state, next_agent_state

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @return (next_state, result) tuple, (None, None).
        @throws InstrumentParameterException if missing set parameters, if set parameters not ALL and
        not a dict, or if parameter can't be properly formatted.
        @throws InstrumentTimeoutException if device cannot be woken for set command.
        @throws InstrumentProtocolException if set command could not be built or misunderstood.
        """
        log.trace("IN _handler_command_set")
        next_state = None
        startup = False
        changed = False

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

        # For each key, val in the dict, issue set command to device.
        # Raise if the command not understood.

        # Handle engineering parameters
        if KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY] in params:
            if (params[KMLParameter.SAMPLE_INTERVAL] != self._param_dict.get(
                    KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY])):
                self._param_dict.set_value(KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                                           params[KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY]])
                if params[KMLParameter.SAMPLE_INTERVAL[ParameterIndex.KEY]] == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job( KMLScheduledJob.SAMPLE)
                changed = True

        if KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY] in params:
            if (params[KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]] != self._param_dict.get(
                    KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY])):
                self._param_dict.set_value(KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                           params[KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]])
                if params[KMLParameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]] == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job( KMLScheduledJob.STATUS)
                changed = True

        if KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY] in params:
            if (params[KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]] != self._param_dict.get(
                    KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY])):
                self._param_dict.set_value(KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                           params[KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]])

                self.forwarding_time = params[KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]]
                if self.video_forwarding_flag == True:
                    if self.forwarding_time != ZERO_TIME_INTERVAL:
                        self.start_scheduled_job(KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                                 KMLScheduledJob.VIDEO_FORWARDING,
                                                 KMLProtocolEvent.STOP_FORWARD)
                if self.forwarding_time == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job( KMLScheduledJob.VIDEO_FORWARDING)
                changed = True

        if KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY] in params:
            if (params[KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY]] != self._param_dict.get(
                    KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY])):
                self._param_dict.set_value(KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                                           params[KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY]])
                self.video_forwarding_flag = params[KMLParameter.VIDEO_FORWARDING[ParameterIndex.KEY]]

                if self.video_forwarding_flag == True:
                    if self.forwarding_time != ZERO_TIME_INTERVAL:
                        self.start_scheduled_job(KMLParameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                                 KMLScheduledJob.VIDEO_FORWARDING,
                                                 KMLProtocolEvent.STOP_FORWARD)
                changed = True

        if KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY] in params:
            if (params[KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]] != self._param_dict.get(
                    KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])):
                self._param_dict.set_value(KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                           params[KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]])
                changed = True

        if KMLParameter.PRESET_NUMBER[ParameterIndex.KEY] in params:
            if (params[KMLParameter.PRESET_NUMBER[ParameterIndex.KEY]] != self._param_dict.get(
                    KMLParameter.PRESET_NUMBER[ParameterIndex.KEY])):
                self._param_dict.set_value(KMLParameter.PRESET_NUMBER[ParameterIndex.KEY],
                                           params[KMLParameter.PRESET_NUMBER[ParameterIndex.KEY]])
                changed = True

        if changed:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        log.debug("In handler_command_set: about to call set_params")
        result = self._set_params(params, startup)

        return next_state, result

    def _handler_command_start_direct(self, *args, **kwargs):
        result = None

        next_state = KMLProtocolState.DIRECT_ACCESS
        next_agent_state = ResourceAgentState.DIRECT_ACCESS
        return next_state, (next_agent_state, result)

    def _handler_capture_start(self, *args, **kwargs):
        result = None

        kwargs['timeout'] = 30
        result = self._do_cmd_resp(KMLInstrumentCmds.START_CAPTURE, *args, **kwargs)

        next_state = None
        next_agent_state = None
        return next_state, (next_agent_state, result)

    def _handler_capture_stop(self, *args, **kwargs):
        """
        @reval next_state, (next_agent_state, result)
        """

        result = None
        kwargs['timeout'] = 30

        result = self._do_cmd_resp(KMLInstrumentCmds.STOP_CAPTURE, *args, **kwargs)

        # Wake up the device, continuing until autosample prompt seen.
        timeout = kwargs.get('timeout', TIMEOUT)

        (next_state, next_agent_state) = self._discover()

        return next_state, (next_agent_state, result)

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

    def _handler_direct_access_execute_direct(self, data):
        next_state = None
        result = None
        next_agent_state = None
        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, (next_agent_state, result)

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        Take a snapshot
        """
        log.error("IN _handler_command_acquire_sample")
        next_state = None

        kwargs['timeout'] = 30

        # Before taking a snapshot, update parameters
        self._update_params()

        log.error("Acquire Sample: about to take a snapshot")

        try:
            self._do_cmd_resp(KMLInstrumentCmds.TAKE_SNAPSHOT, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        log.error("Acquire Sample: Captured snapshot!")

        time.sleep(.5)

        return next_state, (None, None)

    def _handler_autosample_acquire_sample(self, *args, **kwargs):
        """
        Take a snapshot
        """
        log.error("IN _handler_autosample_acquire_sample")
        next_state = None

        kwargs['timeout'] = 30

        # Before taking a snapshot, update parameters
        self._update_params()

        log.error("Acquire Sample: about to take a snapshot")

        try:
            self._do_cmd_resp(KMLInstrumentCmds.TAKE_SNAPSHOT, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        log.error("Acquire Sample: Captured snapshot!")

        time.sleep(.5)

        return next_state, (None, None)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Take a snapshot
        """
        log.error("ACQUIRE_STATUS: IN _handler_command_acquire_status")
        next_state = None

        kwargs['timeout'] = 2
        #kwargs['expected_prompt'] = KMLPrompt.COMMAND

        # Execute the following commands
        #  GET_DISK_USAGE = 'GC'
        #  HEALTH_REQUEST  = 'HS'
        try:
            self._do_cmd_resp(KMLInstrumentCmds.GET_DISK_USAGE, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        log.error("ACQUIRE_STATUS: Executed GET_DISK_USAGE")

        time.sleep(.5)
        try:
            self._do_cmd_resp(KMLInstrumentCmds.HEALTH_REQUEST, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        log.error("ACQUIRE_STATUS: Executed HEALTH_REQUEST")

        return next_state, (None, None)

    def _handler_command_lamp_on(self, *args, **kwargs):
        """
        Take a snapshot
        """
        log.debug("IN _handler_command_lamp_on")
        next_state = None

        kwargs['timeout'] = 30

        try:
            self._do_cmd_resp(KMLInstrumentCmds.LAMP_ON, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_lamp_off(self, *args, **kwargs):
        """
        Take a snapshot
        """
        log.debug("IN _handler_command_lamp_off")
        next_state = None

        kwargs['timeout'] = 30

        try:
            self._do_cmd_resp(KMLInstrumentCmds.LAMP_OFF, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_laser(self, command, light, *args, **kwargs):

        """
        Command the laser
        """
        log.debug("IN _handler_command_laser_common")
        next_state = None

        kwargs['timeout'] = 2

        try:
            self._do_cmd_resp(command, light, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_laser1_on(self, *args, **kwargs):
        """
        Turn laser 1 on
        """
        log.debug("IN _handler_command_laser1_on")

        return self._handler_command_laser(KMLInstrumentCmds.LASER_ON, '\x01', *args, **kwargs)

    def _handler_command_laser1_off(self, *args, **kwargs):
        """
        Turn laser 1 off
        """
        log.debug("IN _handler_command_laser1_off")

        return self._handler_command_laser(KMLInstrumentCmds.LASER_OFF, '\x01', *args, **kwargs)

    def _handler_command_laser2_on(self, *args, **kwargs):
        """
        Turn laser 2 on
        """
        log.debug("IN _handler_command_laser2_on")

        return self._handler_command_laser(KMLInstrumentCmds.LASER_ON, '\x02', *args, **kwargs)

    def _handler_command_laser2_off(self, *args, **kwargs):
        """
        Turn laser 2 off
        """
        log.debug("IN _handler_command_laser2_off")

        return self._handler_command_laser(KMLInstrumentCmds.LASER_OFF, '\x02', *args, **kwargs)

    def _handler_command_laser_both_on(self, *args, **kwargs):
        """
        Turn both lasers on
        """
        log.debug("IN _handler_command_laser_both_on")

        return self._handler_command_laser(KMLInstrumentCmds.LASER_ON, '\x03', *args, **kwargs)

    def _handler_command_laser_both_off(self, *args, **kwargs):
        """
        Turn both lasers off
        """
        log.debug("IN _handler_command_laser_both_off")

        return self._handler_command_laser(KMLInstrumentCmds.LASER_OFF, '\x03', *args, **kwargs)

    def _handler_command_set_preset(self, *args, **kwargs):
        """
        Take a snapshot
        """
        log.debug("IN _handler_command_set_preset")
        next_state = None

        kwargs['timeout'] = 2

        # Execute the following commands
        #  GET_DISK_USAGE = 'GC'
        #  HEALTH_REQUEST  = 'HS'
        pd = self._param_dict.get_all()
        result = []
        preset_number = 1

        for key, value in pd.items():
            if key == KMLParameter.PRESET_NUMBER[ParameterIndex.KEY]:
                preset_number = value

        try:
            self._do_cmd_resp(KMLInstrumentCmds.SET_PRESET, preset_number, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_start_capture (self, *args, **kwargs):

        log.debug("IN _handler_command_start_capture")
        next_state = None

        kwargs['timeout'] = 2

        # Before taking a snapshot, update parameters
        self._update_params()

        capturing_duration = self._param_dict.get(KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])

        if capturing_duration != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(KMLParameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                     KMLScheduledJob.STOP_CAPTURE,
                                     KMLProtocolEvent.STOP_CAPTURE)

        try:
            self._do_cmd_resp(KMLInstrumentCmds.START_CAPTURE, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

    def _handler_command_stop_capture (self, *args, **kwargs):

        log.debug("IN _handler_command_stop_capture")
        next_state = None

        kwargs['timeout'] = 2

        self.stop_scheduled_job(KMLScheduledJob.STOP_CAPTURE)

        try:
            self._do_cmd_resp(KMLInstrumentCmds.STOP_CAPTURE, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp() stop capture' + str(e))

    def _handler_command_stop_forward (self, *args, **kwargs):

        log.debug("IN _handler_command_stop_forward")
        next_state = None

        kwargs['timeout'] = 2

        self.stop_scheduled_job(KMLScheduledJob.VIDEO_FORWARDING)
        self.video_forwarding_flag = False

    def _handler_command_goto_preset(self, *args, **kwargs):
        """
        Take a snapshot
        """
        log.debug("IN _handler_command_goto_preset")
        next_state = None

        kwargs['timeout'] = 2

        # Execute the following commands
        #  GET_DISK_USAGE = 'GC'
        #  HEALTH_REQUEST  = 'HS'
        pd = self._param_dict.get_all()

        preset_number = 1
        for key, value in pd.items():
            if key == KMLParameter.PRESET_NUMBER[ParameterIndex.KEY]:
                preset_number = value

        try:
            self._do_cmd_resp(KMLInstrumentCmds.GO_TO_PRESET, preset_number, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_no_resp()' + str(e))

        return next_state, (None, None)

    def _discover(self):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE or UNKNOWN.
        @return (next_protocol_state, next_agent_state)
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentStateException if the device response does not correspond to
        an expected state.
        """
        if self._scheduler is not None:
            return KMLProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING
        return KMLProtocolState.COMMAND, ResourceAgentState.COMMAND

    def _handler_direct_access_stop_direct(self):
        """
        @reval next_state, (next_agent_state, result)
        """
        result = None
        (next_state, next_agent_state) = self._discover()

        return next_state, (next_agent_state, result)

    def _handler_command_restore_factory_params(self):
        """
        """

    def _get_param_tuple(self, param):
        list_param = self._get_params()
        results = ""
        for attr in sorted(list_param):
            if attr[ParameterIndex.KEY] is param:
                return attr
        return None

    def _get_response_(self, response):
        #<size:command:data>
        #throw InstrumentProtocolException

        # make sure that the response is right format
        log.error("Sung get_response %s", response)
        if '<' in response:
            log.error("Sung get_response2 %s", response)
            if response[0] == '<':
                log.error("Sung get_response3 %s", response)
                if response[len(response)-1] == '>':
                    log.error("Sung get_response4 %s", response)
                    if ':' in response:
                        log.error("Sung get_response5 %s", response)
                        response.replace('<','')
                        response.replace('>','')
                        log.error("Sung get_response6 %s", response)
                        return response.split(':')
        # Not valid response
        raise InstrumentProtocolException('Not valid instrument response %s' % response)

    def _parse_get_disk_usage_response(self, response, prompt):

        resopnse_striped = '%r' % response.strip()
        #check the size of the response
        if len(resopnse_striped) != 12:
            raise InstrumentParameterException('Size of the get_disk_usage is not 12 ' + self.get_param
                                               + '  ' + resopnse_striped + ' ' + self.get_cmd)
        if resopnse_striped[0] != '<':
            raise InstrumentParameterException('Failed to cmd a response for lookup of ' + self.get_param
                                               + '  ' + resopnse_striped + ' ' + self.get_cmd)
        if resopnse_striped[len(resopnse_striped) -1] != '>':
            raise InstrumentParameterException('Failed to cmd a response for lookup of ' + self.get_param
                                               + '  ' + resopnse_striped + ' ' + self.get_cmd)
        if resopnse_striped[3] == self.NAK:
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : Set dis usage command not recognized')

    def _build_get_command(self, cmd, param, **kwargs):
        """
        param=val followed by newline.
        @param cmd get command
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The get command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """

        log.debug("Sung build_command %r", param)
        param_tuple = param
        self.get_param = param[ParameterIndex.KEY]
        self.get_param_dict = param
        self.get_cmd = cmd

        log.error("Sung build_command %r", param_tuple[ParameterIndex.GET])
        return param_tuple[ParameterIndex.GET]

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

        self.get_param = param
        self.get_cmd = cmd

        try:

            val = str(val)

            if param in [KMLParameter.PAN_POSITION[ParameterIndex.KEY],
                                                              KMLParameter.TILT_POSITION[ParameterIndex.KEY]]:

                if len(val) == 1:
                    val = str(ord('0')) + ':' + str(ord('0')) + ':' + str(ord(val))
                elif len(val) == 2:
                    val = str(ord('0')) + ':' + str(ord(val[0])) + ':' + str(ord(val[1]))
                elif len(val) == 3:
                    val = str(ord(val[0])) + ':' + str(ord(val[1])) + ':' + str(ord(val[2]))

                else:
                    raise InstrumentParameterException('The input cannot be more than 3 bytes. %s' % param)

            input_data = val.split(':')

            converted = ''

            input_data_size = 0

            for x in input_data:

                input_data_size += 1

                converted = converted + self.convertDecToHex(int(x))

            data_size = input_data_size + 3

            if param == KMLParameter.NTP_SETTING[ParameterIndex.KEY]:
                converted = converted + KMLParameter.NTP_SETTING[ParameterIndex.DEFAULT_DATA]
                data_size = len(converted) + 3

            param_tuple = self._get_param(param)

            set_cmd = '<%s:%s:%s>' % (self.convertDecToHex(data_size), param_tuple[ParameterIndex.SET], converted)

            log.debug("Build_set_command: %r", set_cmd)

        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter. %s' % param)

        return set_cmd

    def build_status_command(self, cmd):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """

        self.get_cmd = cmd

        command = '<\x03:%s:>' % cmd
        return command

    def build_laser_command(self, cmd, data):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """

        self.get_cmd = cmd

        command = '<\x04:%s:%s>' % (cmd, data)

        log.error("In build_laser_command, cmd is: %r", command)

        return command

    def build_preset_command(self, cmd, data):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """

        self.get_cmd = cmd

        command = '<\x04:%s:%s>' % (cmd, self.convertDecToHex(data))
        return command


    def _parse_set_response(self, response, prompt):

        log.error("Sung SET RESPONSE = " + repr(response))

        #Make sure the response is the right format
        resopnse_striped = '%s' % response.strip()

        if resopnse_striped[0] != '<':
            log.error("Sung SET RESPONSE, < is okay ")
            raise InstrumentParameterException('Failed to set a response for lookup of <' + self.get_param
                                               + '  ' + resopnse_striped)
        if resopnse_striped[len(resopnse_striped) -1] != '>':
            log.error("Sung SET RESPONSE, > is okay ")
            raise InstrumentParameterException('Failed to set a response for lookup of > ' + self.get_param
                                               + '  ' + resopnse_striped)
        if resopnse_striped[3] == self.NAK:
            reason = self.CAMDS_failure_message(resopnse_striped[5])
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : Set command not recognized: ' + reason)

        return response

    def _parse_get_response(self, response, prompt):
        log.debug("GET RESPONSE = " + repr(response))

        #Make sure the response is the right format
        response_striped = response.strip()
        if response_striped[0] != '<':
            raise InstrumentParameterException('Failed to get a response for lookup of <')

        if response_striped[len(response_striped) - 1] != '>':
            raise InstrumentParameterException('Failed to get a response for lookup of >')

        if response_striped[3] == self.NAK:
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : get command not recognized')

        log.debug("GET RESPONSE : Response for %r is: %s" % (self.get_param, response_striped))

        if response_striped[3] == self.ACK:

            #parse out parameter value first

            if self.get_param[ParameterIndex.GET] is None:
                # No response data to process
                return

            if self.get_param[ParameterIndex.LENGTH] is None:
                # Not fixed size of the response data
                # get the size of the responding data
                log.debug("GET RESPONSE : get Length is None")
                raw_value = response_striped[self.get_param_dict[ParameterIndex.Start] + 6:
                                             len(response_striped) - 2]

                log.debug("GET RESPONSE : response raw : %r", raw_value)

                if self.get_param[ParameterIndex.KEY] == KMLParameter.NTP_SETTING[ParameterIndex.KEY]:
                    self._param_dict.update(ord(raw_value[0]), target_params = self.get_param)
                if self.get_param[ParameterIndex.KEY] == KMLParameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY]:
                    self._param_dict.update(raw_value.trim(), target_params = self.get_param)

            else:

                # The input data is ended with '\x00'
                if self.get_param_dict[ParameterIndex.LENGTH] == None:
                    raw_value = response_striped[self.get_param_dict[ParameterIndex.Start] + 6:
                                                 len(response_striped)-1]

                else:
                    raw_value = response_striped[self.get_param_dict[ParameterIndex.Start] + 6:
                                                 self.get_param_dict[ParameterIndex.Start] +
                                                 self.get_param_dict[ParameterIndex.LENGTH] + 6]

                if len(raw_value) == 1:

                    log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], ord(raw_value)))
                    self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], ord(raw_value))

                else:

                    if self.get_param_dict[ParameterIndex.KEY] in [KMLParameter.PAN_POSITION[ParameterIndex.KEY],
                                                              KMLParameter.TILT_POSITION[ParameterIndex.KEY]]:

                        log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], int(raw_value)))
                        self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], int(raw_value))

                    elif self.get_param_dict[ParameterIndex.KEY] in [KMLParameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]]:
                        if len(raw_value) == 2:
                            lamp1_brightness = ord(raw_value[0])
                            lamp2_brightness = ord(raw_value[1])
                            brightness = (lamp1_brightness + lamp2_brightness) / 2
                            param_val = '3:' + str(brightness)
                            log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], param_val))
                            self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], param_val)

                    elif self.get_param_dict[ParameterIndex.KEY] in [KMLParameter.SHUTTER_SPEED[ParameterIndex.KEY]]:
                        if len(raw_value) == 2:
                            first = ord(raw_value[0])
                            multiplier = ord(raw_value[1])
                            param_val = '' + str(first) + ':' + str(multiplier)
                            log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], param_val))
                            self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], param_val)

        self.get_count = 0

        new_param_value = self._param_dict.get(self.get_param_dict[ParameterIndex.KEY])
        log.error("Param Dict Value for %s was set to %s" % (self.get_param_dict[ParameterIndex.KEY], new_param_value))

        return response

    def _parse_simple_response(self, response, prompt):
        log.trace("GET RESPONSE = " + repr(response))

        #Make sure the response is the right format
        resopnse_striped = '%s' % response.strip()
        if resopnse_striped[0] != '<':
            raise InstrumentParameterException('Failed to get a response for lookup of ' + self.get_param
                                               + '  ' + resopnse_striped)
        if resopnse_striped[len(resopnse_striped) -1] != '>':
            raise InstrumentParameterException('Failed to get a response for lookup of ' + self.get_param
                                               + '  ' + resopnse_striped)
        if resopnse_striped[3] == self.NAK:
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : get command not recognized')

        self.get_count = 0
        return response

    def CAMDS_failure_message(self, error_code):
        """
        Struct a error message based on error code
            0x00 - Undefined
            0x01 - Command not recognised
            0x02 - Invalid Command Structure
            0x03 - Command Timed out
            0x04 - Command cannot be processed because the camera is in an incorrect state.
            0x05 - Invalid data values
            0x06 - Camera Busy Processing
        """
        if error_code == '\x00':
            return "Undefined"
        if error_code == '\x01':
            return "Command not recognized"
        if error_code == '\x02':
            return "Invalid Command Structure"
        if error_code == '\x03':
            return "Command Timed out"
        if error_code == '\x04':
            return "Command cannot be processed because the camera is in an incorrect state"
        if error_code == '\x05':
            return "Invalid data values"
        if error_code == '\x06':
            return "Camera Busy Processing"
        return "Unknown"

    def _get_params(self):

        return KMLParameter.list()

    def _get_param(self, key):
        log.error("Sung _get_param %s", key)
        param_dict = KMLParameter.dict()
        return param_dict.get(key)

