"""
@package mi.instrument.KML.CAM.driver
@file mi-instrument/mi/instrument/kml/cam/driver.py
@author Sung Ahn
@brief Driver for the CAMDS

"""

import time
import re
import struct

from mi.core.common import BaseEnum
from mi.core.exceptions import InstrumentParameterException, NotImplementedException
from mi.core.exceptions import InstrumentProtocolException

from mi.core.instrument.data_particle import DataParticle, RawDataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.driver_dict import DriverDictKey

from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol

from mi.core.instrument.port_agent_client import PortAgentClient

from mi.core.driver_scheduler import DriverSchedulerConfigKey
from mi.core.driver_scheduler import TriggerType

from mi.core.log import get_logger

log = get_logger()
from mi.core.log import get_logging_metaclass

from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import ConfigMetadataKey

# default timeout.
TIMEOUT = 20

METALOGGER = get_logging_metaclass()

# newline.
NEWLINE = '\r\n'

DEFAULT_CMD_TIMEOUT = 20
DEFAULT_WRITE_DELAY = 0

ZERO_TIME_INTERVAL = '00:00:00'
MIN_SAMPLE_INTERVAL = 35
RE_PATTERN = type(re.compile(""))

DEFAULT_DICT_TIMEOUT = 30


# Particle Regex's'
CAMDS_DISK_STATUS_MATCHER = r'<\x0B:\x06:GC.+?>'
CAMDS_DISK_STATUS_MATCHER_COM = re.compile(CAMDS_DISK_STATUS_MATCHER, re.DOTALL)
CAMDS_HEALTH_STATUS_MATCHER = r'<\x07:\x06:HS.+?>'
CAMDS_HEALTH_STATUS_MATCHER_COM = re.compile(CAMDS_HEALTH_STATUS_MATCHER, re.DOTALL)
CAMDS_SNAPSHOT_MATCHER = r'<\x04:\x06:CI>'
CAMDS_SNAPSHOT_MATCHER_COM = re.compile(CAMDS_SNAPSHOT_MATCHER, re.DOTALL)
CAMDS_START_CAPTURING = r'<\x04:\x06:SP>'
CAMDS_START_CAPTURING_COM = re.compile(CAMDS_START_CAPTURING, re.DOTALL)
CAMDS_STOP_CAPTURING = r'<\x04:\x06:SR>'
CAMDS_STOP_CAPTURING_COM = re.compile(CAMDS_STOP_CAPTURING, re.DOTALL)


class CAMDSPrompt(BaseEnum):
    """
    Device i/o prompts..
    """
    END = '>'
    COMMAND = '<\x03:\x15:\x02>'


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

class CAMDSConnections(BaseEnum):
    """
    The protocol needs to have 2 connections
    """
    DRIVER = 'Driver'

class StreamPortAgentClient(PortAgentClient):
    """
    Wrap PortAgentClient for Video stream
    """
    def __init__(self, host, port, cmd_port, delim=None):
        PortAgentClient.__init__(self, host, port, cmd_port, delim=None)
        self.info = "This is portAgentClient for Video Stream"


################################################################################
# Data Particles
################################################################################
class DataParticleType(BaseEnum):
    """
    Stream types of data particles
    """
    RAW = CommonDataParticleType.RAW

    CAMDS_VIDEO = "camds_video"
    CAMDS_HEALTH_STATUS = "camds_health_status"
    CAMDS_DISK_STATUS = "camds_disk_status"
    CAMDS_IMAGE_METADATA = "camds_image_metadata"

# keys for video stream
class CAMDS_VIDEO_KEY(BaseEnum):
    """
    Video stream data key
    """
    CAMDS_VIDEO_BINARY = "raw"


# Data particle for PT4 command
class CAMDS_VIDEO(RawDataParticle):
    """
    cam video stream data particle
    """
    _data_particle_type = DataParticleType.CAMDS_VIDEO


# HS command
class CAMDS_HEALTH_STATUS_KEY(BaseEnum):
    """
    cam health status keys
    """
    temp = "camds_temp"
    humidity = "camds_humidity"
    error = "camds_error"


# Data particle for HS
class CAMDS_HEALTH_STATUS(DataParticle):
    """
    cam health status data particle
    """
    _data_particle_type = DataParticleType.CAMDS_HEALTH_STATUS

    TEMP_INDEX = 7
    HUMIDITY_INDEX = 8
    ERROR_INDEX = 9


    def _build_parsed_values(self):

        response_stripped = self.raw_data

        #check the size of the response
        if len(response_stripped) != 11:
            log.error("Health status size should be 11 %r" + response_stripped)
            return
        if response_stripped[0] != '<':
            log.error("Health status is not correctly formatted %r" + response_stripped)
            return
        if response_stripped[-1] != '>':
            log.error("Health status is not correctly formatted %r" + response_stripped)
            return

        int_bytes = bytearray(response_stripped)

        parsed_sample = [{DataParticleKey.VALUE_ID: CAMDS_HEALTH_STATUS_KEY.temp,
                          DataParticleKey.VALUE: int_bytes[self.TEMP_INDEX]},
                        {DataParticleKey.VALUE_ID: CAMDS_HEALTH_STATUS_KEY.humidity,
                          DataParticleKey.VALUE: int_bytes[self.HUMIDITY_INDEX]},
                         {DataParticleKey.VALUE_ID: CAMDS_HEALTH_STATUS_KEY.error,
                          DataParticleKey.VALUE: int_bytes[self.ERROR_INDEX]}]

        log.debug("CAMDS_HEALTH_STATUS: Finished building particle: %s" % parsed_sample)

        return parsed_sample


# GC command
class CAMDS_DISK_STATUS_KEY(BaseEnum):
    """
    cam disk status keys
    """
    size = "camds_disk_size"
    disk_remaining = "camds_disk_remaining"
    image_remaining = "camds_images_remaining"
    image_on_disk = "camds_images_on_disk"

# Data particle for GC command
class CAMDS_DISK_STATUS(DataParticle):
    """
    cam disk status data particle
    """
    _data_particle_type = DataParticleType.CAMDS_DISK_STATUS

    BYTE_1_INDEX = 7
    BYTE_2_INDEX = 8
    BYTE_3_INDEX = 9

    def _build_parsed_values(self):

        response_stripped = self.raw_data.strip()

          #check the size of the response
        if len(response_stripped) != 15:
            log.error("Disk status size should be 15 %r" + response_stripped)
            return
        if response_stripped[0] != '<':
            log.error("Disk status is not correctly formatted %r" + response_stripped)
            return
        if response_stripped[-1] != '>':
            log.error("Disk status is not correctly formatted %r" + response_stripped)
            return

        int_bytes = bytearray(response_stripped)

        byte1 = int_bytes[self.BYTE_1_INDEX]
        byte2 = int_bytes[self.BYTE_2_INDEX]
        byte3 = int_bytes[self.BYTE_3_INDEX]

        available_disk = byte1 * pow(10, byte2)
        available_disk_percent = byte3

        temp = struct.unpack('!h', response_stripped[10] + response_stripped[11])
        images_remaining = temp[0]
        temp = struct.unpack('!h', response_stripped[12] + response_stripped[13])
        images_on_disk = temp[0]

        parsed_sample = [{DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.size,
                          DataParticleKey.VALUE: available_disk},
                         {DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.disk_remaining,
                          DataParticleKey.VALUE: available_disk_percent},
                         {DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.image_remaining,
                          DataParticleKey.VALUE: images_remaining},
                         {DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.image_on_disk,
                          DataParticleKey.VALUE: images_on_disk}]

        log.debug("CAMDS_DISK_STATUS: Finished building particle: %s" % parsed_sample)

        return parsed_sample

# Data particle for CAMDS Image Metadata
class CAMDS_IMAGE_METADATA(DataParticle):
    """
    camds_image_metadata particle
    """
    _data_particle_type = DataParticleType.CAMDS_IMAGE_METADATA

    def _build_parsed_values(self):
        # Initialize

        result = []

        log.debug("CAMDS_IMAGE_METADATA: Building data particle...")

        param_dict = self.raw_data.get_all()

        log.debug("Param Dict: %s" % param_dict)

        result.append({DataParticleKey.VALUE_ID: "camds_pan_position",
                           DataParticleKey.VALUE: param_dict.get(Parameter.PAN_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_tilt_position",
                           DataParticleKey.VALUE: param_dict.get(Parameter.TILT_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_focus_position",
                           DataParticleKey.VALUE: param_dict.get(Parameter.FOCUS_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_zoom_position",
                           DataParticleKey.VALUE: param_dict.get(Parameter.ZOOM_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_iris_position",
                           DataParticleKey.VALUE: param_dict.get(Parameter.IRIS_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_resolution",
                           DataParticleKey.VALUE: param_dict.get(Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY])})

        brightness_param = param_dict.get(Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY])

        try:
            brightness = int(brightness_param.split(':')[1])
            result.append({DataParticleKey.VALUE_ID: "camds_brightness", DataParticleKey.VALUE: brightness})

        except ValueError:
            log.error("Error building camds_image_metadata particle: Brightness value is not an Integer")

        except IndexError:
            log.error("Error building camds_image_metadata particle: Brightness parameter incorrectly "
                      "formatted: %s" % brightness_param)

        #TODO: Instrument has issues returning CAMERA_GAIN, set to default value for now
        # result.append({DataParticleKey.VALUE_ID: "camds_gain",
        #            DataParticleKey.VALUE: param_dict.get(Parameter.CAMERA_GAIN[ParameterIndex.KEY])})
        gain_val = 255
        result.append({DataParticleKey.VALUE_ID: "camds_gain", DataParticleKey.VALUE: gain_val})

        log.debug("CAMDS_IMAGE_METADATA: Finished building particle: %s" % result)

        return result


class Parameter(DriverParameter):
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
    SAMPLE_INTERVAL = (None, None, None, None, '00:30:00', 'Sample Interval',
                       'hh:mm:ss', 'SAMPLE_INTERVAL', '00:30:00')
    ACQUIRE_STATUS_INTERVAL = (None, None, None, None, '00:00:00', 'Acquire Status Interval',
                               'hh:mm:ss', 'ACQUIRE_STATUS_INTERVAL', '00:00:00')
    VIDEO_FORWARDING = (None, None, None, None, False, 'Video Forwarding Flag',
                        'True - Turn on Video, False - Turn off video', 'VIDEO_FORWARDING', False)
    VIDEO_FORWARDING_TIMEOUT = (None, None, None, None, '01:00:00', 'video forwarding timeout',
                                'hh:mm:ss', 'VIDEO_FORWARDING_TIMEOUT', '01:00:00')
    PRESET_NUMBER = (None, None, None, None, 1, 'Preset number', 'preset number (1- 15)', 'PRESET_NUMBER', 1)
    AUTO_CAPTURE_DURATION = (None, None, None, None, '00:00:03', 'Auto Capture Duration', 'hh:mm:ss, 1 to 5 Seconds',
                             'AUTO_CAPTURE_DURATION', '00:00:03')


class ProtocolEvent(BaseEnum):
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


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE

    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    ACQUIRE_SAMPLE = ProtocolEvent.ACQUIRE_SAMPLE

    EXECUTE_AUTO_CAPTURE = ProtocolEvent.EXECUTE_AUTO_CAPTURE
    STOP_CAPTURE = ProtocolEvent.STOP_CAPTURE

    LASER_1_ON = ProtocolEvent.LASER_1_ON
    LASER_2_ON = ProtocolEvent.LASER_2_ON
    LASER_BOTH_ON = ProtocolEvent.LASER_BOTH_ON
    LASER_1_OFF = ProtocolEvent.LASER_1_OFF
    LASER_2_OFF = ProtocolEvent.LASER_2_OFF
    LASER_BOTH_OFF = ProtocolEvent.LASER_BOTH_OFF

    LAMP_ON = ProtocolEvent.LAMP_ON
    LAMP_OFF = ProtocolEvent.LAMP_OFF

    SET_PRESET = ProtocolEvent.SET_PRESET
    GOTO_PRESET = ProtocolEvent.GOTO_PRESET


class ScheduledJob(BaseEnum):
    SAMPLE = 'sample'
    VIDEO_FORWARDING = "video forwarding"
    STATUS = "status"
    STOP_CAPTURE = "stop capturing"


class ProtocolState(DriverProtocolState):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class InstrumentCmds(BaseEnum):
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

###############################################################################
# Driver
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass for cam driver.
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """
    #__metaclass__ = METALOGGER

    def __init__(self, evt_callback):
        """
        Driver constructor.
        @param evt_callback Driver process event callback.
        """
        # Construct superclass.
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)


    # #######################################################################
    # Protocol builder.
    # #######################################################################

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = CAMDSProtocol(CAMDSPrompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################
class CAMDSProtocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """
    #__metaclass__ = METALOGGER

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

        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        self.last_wakeup = 0

        # commands sent sent to device to be
        # filtered in responses for telnet DA
        self._sent_cmds = []

        self.video_forwarding_flag = False
        self.disable_autosample_recover = False

        self.initialize_scheduler()

        self._connection = None
        self._chunker = StringChunker(self.sieve_function)

        self.protocol_fsm_add_handlers()

        # Build dictionaries for driver schema
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        self.add_build_command_handlers()
        self.add_response_handlers()

        # Set state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # commands sent sent to device to be
        # filtered in responses for telnet DA
        self._sent_cmds = []

        self.disable_autosample_recover = False

    @staticmethod
    def sieve_function(raw_data):
        """
        Chunker sieve method to help the chunker identify chunks.
        @returns a list of chunks identified, if any.
        The chunks are all the same type.
        """

        sieve_matchers = [CAMDS_SNAPSHOT_MATCHER_COM,
                          CAMDS_DISK_STATUS_MATCHER_COM,
                          CAMDS_HEALTH_STATUS_MATCHER_COM,
                          CAMDS_START_CAPTURING_COM]

        return_list = []
        log.debug('Sieve function raw data %r' % raw_data)
        for matcher in sieve_matchers:

            for match in matcher.finditer(raw_data):
                log.debug('Sieve function match %s' % match)

                return_list.append((match.start(), match.end()))

        return return_list

    def protocol_fsm_add_handlers(self):
        """
        Add event handlers to Protocol Finite State Machine
        """

        # Build CAMDS protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(ProtocolState, ProtocolEvent,
                                           ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # Add event handlers to protocol state machine.
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.ENTER,
                                       self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.EXIT,
                                       self._handler_unknown_exit)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER,
                                       self._handler_unknown_discover)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER,
                                       self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXIT,
                                       self._handler_command_exit)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.INIT_PARAMS,
                                       self._handler_command_init_params)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET,
                                       self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_AUTOSAMPLE,
                                       self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET,
                                       self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_DIRECT,
                                       self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_SAMPLE,
                                       self._handler_command_acquire_sample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LAMP_ON,
                                       self._handler_command_lamp_on)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LAMP_OFF,
                                       self._handler_command_lamp_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASER_1_ON,
                                       self._handler_command_laser1_on)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASER_2_ON,
                                       self._handler_command_laser2_on)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASER_BOTH_ON,
                                       self._handler_command_laser_both_on)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASER_1_OFF,
                                       self._handler_command_laser1_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASER_2_OFF,
                                       self._handler_command_laser2_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASER_BOTH_OFF,
                                      self._handler_command_laser_both_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET_PRESET,
                                       self._handler_command_set_preset)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GOTO_PRESET,
                                       self._handler_command_goto_preset)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.STOP_CAPTURE,
                                       self._handler_command_stop_capture)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXECUTE_AUTO_CAPTURE,
                                       self._handler_command_start_capture)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.STOP_FORWARD,
                                       self._handler_command_stop_forward)

        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ENTER,
                                       self._handler_autosample_enter)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXIT,
                                       self._handler_autosample_exit)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.INIT_PARAMS,
                                       self._handler_autosample_init_params)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE,
                                       self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GET,
                                       self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.SET,
                                       self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ACQUIRE_SAMPLE,
                                       self._handler_command_acquire_sample)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LAMP_ON,
                                       self._handler_command_lamp_on)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LAMP_OFF,
                                       self._handler_command_lamp_off)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASER_1_ON,
                                       self._handler_command_laser1_on)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASER_2_ON,
                                       self._handler_command_laser2_on)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASER_BOTH_ON,
                                       self._handler_command_laser_both_on)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASER_1_OFF,
                                       self._handler_command_laser1_off)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASER_2_OFF,
                                       self._handler_command_laser2_off)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASER_BOTH_OFF,
                                      self._handler_command_laser_both_off)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.SET_PRESET,
                                       self._handler_command_set_preset)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GOTO_PRESET,
                                       self._handler_command_goto_preset)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_CAPTURE,
                                       self._handler_command_stop_capture)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXECUTE_AUTO_CAPTURE,
                                       self._handler_command_start_capture)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_FORWARD,
                                       self._handler_command_stop_forward)

        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.ENTER,
                                       self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXIT,
                                       self._handler_direct_access_exit)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXECUTE_DIRECT,
                                       self._handler_direct_access_execute_direct)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.STOP_DIRECT,
                                       self._handler_direct_access_stop_direct)

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with kml parameters.
        For each parameter key, add match stirng, match lambda function,
        and value formatting function for set commands.
        """
        self._param_dict.add(Parameter.NTP_SETTING[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.NTP_SETTING[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.NTP_SETTING[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=True,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             default_value=Parameter.NTP_SETTING[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=False,
                             default_value=Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.WHEN_DISK_IS_FULL[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.WHEN_DISK_IS_FULL[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=True,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             default_value=Parameter.WHEN_DISK_IS_FULL[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.CAMERA_MODE[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.CAMERA_MODE[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.CAMERA_MODE[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.CAMERA_MODE[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.FRAME_RATE[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.FRAME_RATE[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.FRAME_RATE[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.FRAME_RATE[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.IMAGE_RESOLUTION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.IMAGE_RESOLUTION[ParameterIndex.DESCRIPTION],
                             direct_access=True,
                             startup_param=True,
                             default_value=Parameter.IMAGE_RESOLUTION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.COMPRESSION_RATIO[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.COMPRESSION_RATIO[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.COMPRESSION_RATIO[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.COMPRESSION_RATIO[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.SHUTTER_SPEED[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.SHUTTER_SPEED[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.SHUTTER_SPEED[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.SHUTTER_SPEED[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.CAMERA_GAIN[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.CAMERA_GAIN[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.CAMERA_GAIN[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.CAMERA_GAIN[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.LAMP_BRIGHTNESS[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.LAMP_BRIGHTNESS[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.LAMP_BRIGHTNESS[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.FOCUS_SPEED[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.FOCUS_SPEED[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.FOCUS_SPEED[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.FOCUS_SPEED[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.FOCUS_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.FOCUS_POSITION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.FOCUS_POSITION[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.FOCUS_POSITION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.ZOOM_SPEED[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             #lambda value: '%+06d' % value,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.ZOOM_SPEED[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.ZOOM_SPEED[ParameterIndex.DESCRIPTION],
                             direct_access=True,
                             startup_param=True,
                             default_value=Parameter.ZOOM_SPEED[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.IRIS_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.IRIS_POSITION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.IRIS_POSITION[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.IRIS_POSITION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.ZOOM_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.ZOOM_POSITION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.ZOOM_POSITION[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.ZOOM_POSITION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.PAN_SPEED[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.PAN_SPEED[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.PAN_SPEED[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.PAN_SPEED[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.TILT_SPEED[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.TILT_SPEED[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.TILT_SPEED[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.TILT_SPEED[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.SOFT_END_STOPS[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.SOFT_END_STOPS[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.SOFT_END_STOPS[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.SOFT_END_STOPS[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.PAN_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str, # format before sending sensror
                             type=ParameterDictType.STRING, # meta data
                             display_name=Parameter.PAN_POSITION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.PAN_POSITION[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.PAN_POSITION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.TILT_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.TILT_POSITION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.TILT_POSITION[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.TILT_POSITION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.SAMPLE_INTERVAL[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.SAMPLE_INTERVAL[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=False,
                             default_value=Parameter.SAMPLE_INTERVAL[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=False,
                             default_value=Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.D_DEFAULT])


        self._param_dict.add(Parameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.VIDEO_FORWARDING[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.VIDEO_FORWARDING[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=False,
                             default_value=Parameter.VIDEO_FORWARDING[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=False,
                             default_value=Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.PRESET_NUMBER[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.PRESET_NUMBER[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.PRESET_NUMBER[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=False,
                             default_value=Parameter.PRESET_NUMBER[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                             r'NOT USED',
                             lambda match: bool(int(match.group(1))),
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.DISPLAY_NAME],
                             value_description=Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.DESCRIPTION],
                             startup_param=True,
                             direct_access=False,
                             default_value=Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.D_DEFAULT])

        self._param_dict.set_default(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY])
        self._param_dict.set_default(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY])
        self._param_dict.set_default(Parameter.VIDEO_FORWARDING[ParameterIndex.KEY])
        self._param_dict.set_default(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY])
        self._param_dict.set_default(Parameter.PRESET_NUMBER[ParameterIndex.KEY])
        self._param_dict.set_default(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])

    def _build_command_dict(self):
        """
        Build command dictionary
        """
        self._cmd_dict.add(Capability.START_AUTOSAMPLE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Start Autosample",
                           description="Place the instrument into autosample mode")
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Stop Autosample",
                           description="Exit autosample mode and return to command mode")
        self._cmd_dict.add(Capability.EXECUTE_AUTO_CAPTURE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Auto Capture",
                           description="Capture images for default duration")
        self._cmd_dict.add(Capability.ACQUIRE_STATUS,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Acquire Status",
                           description="Get disk usage and check health")
        self._cmd_dict.add(Capability.ACQUIRE_SAMPLE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Acquire Sample",
                           description="Take a snapshot")
        self._cmd_dict.add(Capability.GOTO_PRESET,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Goto Preset",
                           description="Go to the preset number")
        self._cmd_dict.add(Capability.SET_PRESET,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Set Preset",
                           description="Set the preset number")
        self._cmd_dict.add(Capability.LAMP_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="lamp off",
                           description="Turn off the lamp")
        self._cmd_dict.add(Capability.LAMP_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="lamp on",
                           description="Turn on the lamp")
        self._cmd_dict.add(Capability.LASER_1_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 1  off",
                           description="Turn off the laser #1")
        self._cmd_dict.add(Capability.LASER_2_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 2 off",
                           description="Turn off the laser #2")
        self._cmd_dict.add(Capability.LASER_BOTH_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser off",
                           description="Turn off the all laser")
        self._cmd_dict.add(Capability.LASER_1_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 1  on",
                           description="Turn on the laser #1")
        self._cmd_dict.add(Capability.LASER_2_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 2 on",
                           description="Turn on the laser #2")
        self._cmd_dict.add(Capability.LASER_BOTH_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser on",
                           description="Turn on the all laser")
        self._cmd_dict.add(Capability.STOP_CAPTURE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Stop Capture",
                           description="Stop Capture")

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    ########################################################################
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

        if (self.get_current_state() != ProtocolState.COMMAND and
                    self.get_current_state() != ProtocolState.AUTOSAMPLE):
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
        return getattr(Parameter, attr)

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
                if attr not in [ Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                                 Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                 Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                 Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                 Parameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                                 Parameter.PRESET_NUMBER[ParameterIndex.KEY],
                                 Parameter.NTP_SETTING[ParameterIndex.KEY],
                                 Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY],
                                 # TODO: CAMERA_GAIN shouldn't be in this list
                                 # waiting to sort out instrument issues regarding this parameter
                                 Parameter.CAMERA_GAIN[ParameterIndex.KEY],
                                 Parameter.FOCUS_SPEED[ParameterIndex.KEY],
                                 Parameter.PAN_SPEED[ParameterIndex.KEY],
                                 Parameter.TILT_SPEED[ParameterIndex.KEY],
                                 Parameter.ZOOM_SPEED[ParameterIndex.KEY],
                                 'ALL']:
                    if attr.startswith('__'):
                        return
                    if attr in ['dict', 'has', 'list', 'ALL']:
                        return
                    time.sleep(2)

                    key = self._getattr_key(attr)
                    result = self._do_cmd_resp(InstrumentCmds.GET, key, **kwargs)
                    results += result + NEWLINE

            new_config = self._param_dict.get_config()

            if new_config != old_config:
                self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            log.error("EXCEPTION in _update_params WAS " + str(e))
            error = e

        if error:
            raise error

        return results

    def _update_metadata_params(self, *args, **kwargs):
        """
        Update parameters specific to the camds_image_metadata particle.
        """

        error = None
        results = None

        try:
            # Get old param dict config.
            old_config = self._param_dict.get_config()

            cmds = self._get_params()
            cmds.remove('__class__')

            results = ""
            for attr in sorted(cmds):

                if attr in [ Parameter.PAN_POSITION[ParameterIndex.KEY],
                             Parameter.TILT_POSITION[ParameterIndex.KEY],
                             Parameter.FOCUS_POSITION[ParameterIndex.KEY],
                             Parameter.ZOOM_POSITION[ParameterIndex.KEY],
                             Parameter.IRIS_POSITION[ParameterIndex.KEY],
                             # TODO: CAMERA_GAIN needs to be in this list
                             # waiting to sort out instrument issues regarding this parameter
                             #Parameter.CAMERA_GAIN[ParameterIndex.KEY],
                             Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY],
                             Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]]:

                    key = self._getattr_key(attr)
                    result = self._do_cmd_resp(InstrumentCmds.GET, key, **kwargs)
                    results += result + NEWLINE

            new_config = self._param_dict.get_config()

            if new_config != old_config:
                self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            log.error("EXCEPTION in _update_metadata_params WAS " + str(e))
            error = e

        if error:
            raise error

        return results

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        result = None
        try:
            params = args[0]
            log.debug("_set_params: params %r", params)
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)

        for key, val in params.iteritems():
            log.debug("In _set_params, %s, %s", key, val)
            if key not in [ Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                            Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                            Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                            Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                            Parameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                            Parameter.PRESET_NUMBER[ParameterIndex.KEY],
                            Parameter.NTP_SETTING[ParameterIndex.KEY],
                            Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY],
                            'ALL']:

                time.sleep(2)
                result = self._do_cmd_resp(InstrumentCmds.SET, key, val, **kwargs)

                #Make sure the instrument has sufficient time to process this command
                if key in [Parameter.CAMERA_MODE[ParameterIndex.KEY],
                           Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY]]:
                    log.error("Just set Camera parameters, sleeping for 15 seconds")
                    time.sleep(15)

        self._update_params()

        return result

    def _instrument_config_dirty(self):
        """
        Read the startup config and compare that to what the instrument
        is configured too.  If they differ then return True
        @return: True if the startup config doesn't match the instrument
        """
        startup_params = self._param_dict.get_startup_list()
        log.debug("Startup Parameters: %s" % startup_params)

        for param in startup_params:

            # These are the only params we get back from the instrument. The param_dict doesn't
            # have values for other params
            if param in [Parameter.CAMERA_MODE[ParameterIndex.KEY],
                         # TODO: CAMERA_GAIN needs to be in this list
                         # waiting to sort out instrument issues regarding this parameter
                         #KMLParameter.CAMERA_GAIN[ParameterIndex.KEY],
                         Parameter.COMPRESSION_RATIO[ParameterIndex.KEY],
                         Parameter.FOCUS_POSITION[ParameterIndex.KEY],
                         Parameter.FRAME_RATE[ParameterIndex.KEY],
                         Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY],
                         Parameter.IRIS_POSITION[ParameterIndex.KEY],
                         Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY],
                         Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY],
                         Parameter.PAN_POSITION[ParameterIndex.KEY],
                         Parameter.SHUTTER_SPEED[ParameterIndex.KEY],
                         Parameter.SOFT_END_STOPS[ParameterIndex.KEY],
                         Parameter.TILT_POSITION[ParameterIndex.KEY],
                         Parameter.ZOOM_POSITION[ParameterIndex.KEY],
                         ]:
                if self._param_dict.get(param) != self._param_dict.get_config_value(param):
                    log.debug("Instrument config DIRTY: %s %s != %s" % (
                        param, self._param_dict.get(param), self._param_dict.get_config_value(param)))
                    return True

        log.debug("Instrument config clean.")
        return False

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

    def _build_get_command(self, cmd, param, **kwargs):
        """
        param=val followed by newline.
        @param cmd get command
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The get command to be sent to the device.
        """

        log.debug("build_get_command %r", param)
        param_tuple = param
        self.get_param = param[ParameterIndex.KEY]
        self.get_param_dict = param
        self.get_cmd = cmd

        log.debug("build_get_command %r", param_tuple[ParameterIndex.GET])
        return param_tuple[ParameterIndex.GET]

    def _build_set_command(self, cmd, param, val):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The set command to be sent to the device.
        @throws InstrumentParameterException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """

        self.get_param = param
        self.get_cmd = cmd

        try:

            val = str(val)

            if param in [Parameter.PAN_POSITION[ParameterIndex.KEY],
                                                              Parameter.TILT_POSITION[ParameterIndex.KEY]]:

                if len(val) == 1:
                    val = str(ord('0')) + ':' + str(ord('0')) + ':' + str(ord(val))
                elif len(val) == 2:
                    val = str(ord('0')) + ':' + str(ord(val[0])) + ':' + str(ord(val[1]))
                elif len(val) == 3:
                    val = str(ord(val[0])) + ':' + str(ord(val[1])) + ':' + str(ord(val[2]))

                else:
                    raise InstrumentParameterException('The desired value for %s cannot be more than 3 bytes: %s'
                                                       % (param, val))

            input_data = val.split(':')

            converted = ''

            input_data_size = 0

            for x in input_data:

                input_data_size += 1

                converted = converted + chr(int(x))

            data_size = input_data_size + 3

            if param == Parameter.NTP_SETTING[ParameterIndex.KEY]:
                converted = converted + Parameter.NTP_SETTING[ParameterIndex.DEFAULT_DATA]
                data_size = len(converted) + 3

            param_tuple = self._get_param(param)

            set_cmd = '<%s:%s:%s>' % (chr(data_size), param_tuple[ParameterIndex.SET], converted)

            log.debug("Build_set_command: %r", set_cmd)

        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter. %s' % param)

        return set_cmd

    def build_status_command(self, cmd):
        """
        Build handler for status command.
        @param cmd the command.
        """

        self.get_cmd = cmd

        command = '<\x03:%s:>' % cmd
        return command

    def build_laser_command(self, cmd, data):
        """
        Build handler for laser command.
        @param cmd the command.
        @param data the data value.
        @return The command to be sent to the device.
        """

        self.get_cmd = cmd

        command = '<\x04:%s:%s>' % (cmd, data)

        log.debug("build_laser_command, cmd is: %r", command)

        return command

    def build_preset_command(self, cmd, data):
        """
        Build handler for preset command.
        @param cmd the command.
        @param data the data value.
        @return The command to be sent to the device.
        """

        self.get_cmd = cmd

        command = '<\x04:%s:%s>' % (cmd, chr(data))
        return command

    def _parse_set_response(self, response, prompt):

        log.error("SET RESPONSE = %r" % response)

        #Make sure the response is the right format
        response_stripped = response.strip()

        if response_stripped[0] != '<' or response_stripped[-1] != '>':
            raise InstrumentParameterException('Received invalid response from the Instrument when trying to set '
                                               + self.get_param + ': ' + response_stripped)

        if response_stripped[3] == self.NAK:
            reason = self.CAMDS_failure_message(response_stripped[5])
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : Set command not recognized: ' + reason)

        return response

    def _parse_get_response(self, response, prompt):
        log.debug("GET RESPONSE = " + repr(response))

        start_index = 6

        #Make sure the response is the right format
        response_stripped = response.strip()
        if response_stripped[0] != '<' or response_stripped[-1] != '>':
            raise InstrumentParameterException('Received invalid response from the Instrument upon sending'
                                               ' get command: ' + response_stripped)

        if response_stripped[3] == self.NAK:
            raise InstrumentProtocolException(
                'Protocol._parse_get_response : get command not recognized')

        log.debug("GET RESPONSE : Response for %r is: %s" % (self.get_param, response_stripped))

        #parse out parameter value first

        if self.get_param[ParameterIndex.GET] is None:
            # No response data to process
            return

        if self.get_param[ParameterIndex.LENGTH] is None:
            # Not fixed size of the response data
            # get the size of the responding data
            log.debug("GET RESPONSE : get Length is None")
            raw_value = response_stripped[self.get_param_dict[ParameterIndex.Start] + start_index: -2]

            log.debug("GET RESPONSE : response raw : %r", raw_value)

            if self.get_param[ParameterIndex.KEY] == Parameter.NTP_SETTING[ParameterIndex.KEY]:
                self._param_dict.update(ord(raw_value[0]), target_params = self.get_param)
            if self.get_param[ParameterIndex.KEY] == Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY]:
                self._param_dict.update(raw_value.trim(), target_params = self.get_param)

        else:

            # The input data is ended with '\x00'
            if self.get_param_dict[ParameterIndex.LENGTH] is None:
                raw_value = response_stripped[self.get_param_dict[ParameterIndex.Start] + start_index: -1]

            else:
                raw_value = response_stripped[self.get_param_dict[ParameterIndex.Start] + start_index:
                                             self.get_param_dict[ParameterIndex.Start] +
                                             self.get_param_dict[ParameterIndex.LENGTH] + start_index]

            if len(raw_value) == 1:

                log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], ord(raw_value)))
                self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], ord(raw_value))

            else:

                if self.get_param_dict[ParameterIndex.KEY] in [Parameter.PAN_POSITION[ParameterIndex.KEY],
                                                          Parameter.TILT_POSITION[ParameterIndex.KEY]]:

                    log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], int(raw_value)))
                    self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], int(raw_value))

                elif self.get_param_dict[ParameterIndex.KEY] in [Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]]:
                    if len(raw_value) == 2:
                        lamp1_brightness = ord(raw_value[0])
                        lamp2_brightness = ord(raw_value[1])
                        brightness = (lamp1_brightness + lamp2_brightness) / 2
                        param_val = '3:' + str(brightness)
                        log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], param_val))
                        self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], param_val)

                elif self.get_param_dict[ParameterIndex.KEY] in [Parameter.SHUTTER_SPEED[ParameterIndex.KEY]]:
                    if len(raw_value) == 2:
                        first = ord(raw_value[0])
                        multiplier = ord(raw_value[1])
                        param_val = '' + str(first) + ':' + str(multiplier)
                        log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_dict[ParameterIndex.KEY], param_val))
                        self._param_dict.set_value(self.get_param_dict[ParameterIndex.KEY], param_val)

        new_param_value = self._param_dict.get(self.get_param_dict[ParameterIndex.KEY])
        log.debug("Param Dict Value for %s was set to %s" % (self.get_param_dict[ParameterIndex.KEY], new_param_value))

        self.get_count = 0

        return response

    def _parse_simple_response(self, response, prompt):

        #Make sure the response is the right format
        response_stripped = '%s' % response.strip()
        if response_stripped[0] != '<' or response_stripped[-1] != '>':
            raise InstrumentParameterException('Received invalid response from instrument: ' + response_stripped)
        if response_stripped[3] == self.NAK:
            raise InstrumentProtocolException(
                'Protocol._parse_simple_response : command not recognized')

        self.get_count = 0
        return response


    def CAMDS_failure_message(self, error_code):
        """
        Create an error message based on error code
        """
        error_codes = {
            '\x00': 'Undefined',
            '\x01': 'Command not recognized',
            '\x02': 'Invalid Command Structure',
            '\x03': 'Command Timed out',
            '\x04': 'Command cannot be processed because the camera is in an incorrect state',
            '\x05': 'Invalid data values',
            '\x06': 'Camera Busy Processing',

        }

        return error_codes.get(error_code, 'Unknown')

    def _get_params(self):

        return Parameter.list()

    def _get_param(self, key):
        log.debug("_get_param: %s", key)
        return getattr(Parameter, key)

    ###############################################################
    # Unknown State handlers
    ###############################################################
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
        @return protocol_state, agent_state if successful
        """
        protocol_state, agent_state = self._discover()
        if protocol_state == ProtocolState.COMMAND:
            agent_state = ResourceAgentState.IDLE

        return protocol_state, agent_state


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

        # enforce a minimum sample interval of 35 seconds to prevent instrument from choking
        if hours == '00' and minutes == '00' and seconds != '00':
            if int(seconds) < MIN_SAMPLE_INTERVAL:
                seconds = str(MIN_SAMPLE_INTERVAL)

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

    ####################################################################################
    # Command State handlers
    ####################################################################################
    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        # Command device to initialize parameters and send a config change event.
        self._protocol_fsm.on_event(ProtocolEvent.INIT_PARAMS)

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        status_interval = self._param_dict.get(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY])
        if status_interval != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                     ScheduledJob.STATUS,
                                     ProtocolEvent.ACQUIRE_STATUS)

        # start scheduled event for get_status only if the interval is not "00:00:00
        self.video_forwarding_flag = self._param_dict.get(Parameter.VIDEO_FORWARDING[ParameterIndex.KEY])
        self.forwarding_time = self._param_dict.get(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY])
        if self.video_forwarding_flag:
            if self.forwarding_time != ZERO_TIME_INTERVAL:
                self.start_scheduled_job(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                             ScheduledJob.VIDEO_FORWARDING,
                                             ProtocolEvent.STOP_FORWARD)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        self.stop_scheduled_job(ScheduledJob.STOP_CAPTURE)
        self.stop_scheduled_job(ScheduledJob.STATUS)
        self.stop_scheduled_job(ScheduledJob.VIDEO_FORWARDING)

    def _handler_command_init_params(self, *args, **kwargs):
        """
        initialize parameters
        """
        next_state = None
        result = None

        self._init_params()
        return next_state, result

    def _handler_command_start_direct(self, *args, **kwargs):
        result = None

        next_state = ProtocolState.DIRECT_ACCESS
        next_agent_state = ResourceAgentState.DIRECT_ACCESS
        return next_state, (next_agent_state, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @return next_state, (next_agent_state, result) if successful.
        """
        result = None
        kwargs['timeout'] = 30

        # first stop scheduled sampling
        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        # start scheduled event for Sampling only if the interval is not "00:00:00
        sample_interval = self._param_dict.get(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY])

        log.debug("Sample Interval is %s" % sample_interval)

        if sample_interval != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY], ScheduledJob.SAMPLE,
                                     ProtocolEvent.ACQUIRE_SAMPLE)

        next_state = ProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        return next_state, (next_agent_state, result)

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        Acquire Sample
        """
        log.debug("IN _handler_command_acquire_sample")
        next_state = None

        kwargs['timeout'] = 30

        # Before taking a snapshot, update parameters
        self._update_metadata_params()

        log.debug("Acquire Sample: about to take a snapshot")

        try:
            self._do_cmd_resp(InstrumentCmds.TAKE_SNAPSHOT, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

        log.debug("Acquire Sample: Captured snapshot!")

        time.sleep(.5)

        return next_state, (None, None)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Acquire status
        """
        next_state = None

        kwargs['timeout'] = 2

        # Execute the following commands
        #  GET_DISK_USAGE = 'GC'
        #  HEALTH_REQUEST  = 'HS'

        log.debug("ACQUIRE_STATUS: executing status commands...")
        try:
            self._do_cmd_resp(InstrumentCmds.GET_DISK_USAGE, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

        log.debug("ACQUIRE_STATUS: Executed GET_DISK_USAGE")

        time.sleep(.5)
        try:
            self._do_cmd_resp(InstrumentCmds.HEALTH_REQUEST, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

        log.debug("ACQUIRE_STATUS: Executed HEALTH_REQUEST")

        return next_state, (None, None)

    def _handler_command_lamp_on(self, *args, **kwargs):
        """
        Turn the instrument lamp on
        """
        next_state = None

        kwargs['timeout'] = 30

        try:
            self._do_cmd_resp(InstrumentCmds.LAMP_ON, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_lamp_off(self, *args, **kwargs):
        """
        Turn the instrument lamp off
        """
        next_state = None

        kwargs['timeout'] = 30

        try:
            self._do_cmd_resp(InstrumentCmds.LAMP_OFF, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_laser(self, command, light, *args, **kwargs):

        """
        Command the laser
        """
        next_state = None

        kwargs['timeout'] = 2

        try:
            self._do_cmd_resp(command, light, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_laser1_on(self, *args, **kwargs):
        """
        Turn laser 1 on
        """
        return self._handler_command_laser(InstrumentCmds.LASER_ON, '\x01', *args, **kwargs)

    def _handler_command_laser1_off(self, *args, **kwargs):
        """
        Turn laser 1 off
        """
        return self._handler_command_laser(InstrumentCmds.LASER_OFF, '\x01', *args, **kwargs)

    def _handler_command_laser2_on(self, *args, **kwargs):
        """
        Turn laser 2 on
        """
        return self._handler_command_laser(InstrumentCmds.LASER_ON, '\x02', *args, **kwargs)

    def _handler_command_laser2_off(self, *args, **kwargs):
        """
        Turn laser 2 off
        """
        return self._handler_command_laser(InstrumentCmds.LASER_OFF, '\x02', *args, **kwargs)

    def _handler_command_laser_both_on(self, *args, **kwargs):
        """
        Turn both lasers on
        """
        return self._handler_command_laser(InstrumentCmds.LASER_ON, '\x03', *args, **kwargs)

    def _handler_command_laser_both_off(self, *args, **kwargs):
        """
        Turn both lasers off
        """
        return self._handler_command_laser(InstrumentCmds.LASER_OFF, '\x03', *args, **kwargs)

    def _handler_command_set_preset(self, *args, **kwargs):
        """
        Set preset position
        """
        next_state = None

        kwargs['timeout'] = 2

        # Execute the following commands
        #  GET_DISK_USAGE = 'GC'
        #  HEALTH_REQUEST  = 'HS'
        pd = self._param_dict.get_all()
        result = []
        preset_number = 1

        for key, value in pd.items():
            if key == Parameter.PRESET_NUMBER[ParameterIndex.KEY]:
                preset_number = value

        try:
            self._do_cmd_resp(InstrumentCmds.SET_PRESET, preset_number, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

        return next_state, (None, None)

    def _handler_command_start_capture (self, *args, **kwargs):
        """
        Start Auto Capture
        """
        next_state = None

        kwargs['timeout'] = 2

        # Before performing capture, update parameters
        self._update_metadata_params()

        capturing_duration = self._param_dict.get(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])

        if capturing_duration != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                     ScheduledJob.STOP_CAPTURE,
                                     ProtocolEvent.STOP_CAPTURE)
        else:
            log.error("Capturing Duration set to 0: Not Performing Capture.")

        try:
            self._do_cmd_resp(InstrumentCmds.START_CAPTURE, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp()' + str(e))

    def _handler_command_stop_capture (self, *args, **kwargs):
        """
        Stop Auto capture
        """
        next_state = None

        kwargs['timeout'] = 2

        self.stop_scheduled_job(ScheduledJob.STOP_CAPTURE)

        try:
            self._do_cmd_resp(InstrumentCmds.STOP_CAPTURE, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentProtocolException in _do_cmd_resp(): Unable to stop capture' + str(e))

    def _handler_command_stop_forward (self, *args, **kwargs):
        """
        Stop Video Forwarding
        """
        next_state = None

        kwargs['timeout'] = 2

        self.stop_scheduled_job(ScheduledJob.VIDEO_FORWARDING)
        self.video_forwarding_flag = False

    def _handler_command_goto_preset(self, *args, **kwargs):
        """
        Go to the preset position
        """
        next_state = None

        kwargs['timeout'] = 2

        # Execute the following commands
        #  GET_DISK_USAGE = 'GC'
        #  HEALTH_REQUEST  = 'HS'
        pd = self._param_dict.get_all()

        preset_number = 1
        for key, value in pd.items():
            if key == Parameter.PRESET_NUMBER[ParameterIndex.KEY]:
                preset_number = value

        try:
            self._do_cmd_resp(InstrumentCmds.GO_TO_PRESET, preset_number, *args, **kwargs)

        except Exception as e:
            raise InstrumentParameterException(
                'InstrumentParameterException in _do_cmd_resp(): Unable to go to Preset' + str(e))

        return next_state, (None, None)

    def _discover(self):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE or UNKNOWN.
        @return (next_protocol_state, next_agent_state)
        """
        if self._scheduler is not None:
            return ProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING
        return ProtocolState.COMMAND, ResourceAgentState.COMMAND


    ###################################################################################
    # Direct Access State handlers
    ###################################################################################
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

    def _handler_direct_access_stop_direct(self):
        """
        @reval next_state, (next_agent_state, result)
        """
        result = None
        (next_state, next_agent_state) = self._discover()

        return next_state, (next_agent_state, result)

    def _get_param_tuple(self, param):
        list_param = self._get_params()
        results = ""
        for attr in sorted(list_param):
            if attr[ParameterIndex.KEY] is param:
                return attr
        return None

    def _get_response_(self, response):
        """
        <size:command:data>
        @throws InstrumentProtocolException
        """

        # make sure that the response is right format
        log.debug("_get_response, Response is: %s", response)
        if '<' in response:
            if response[0] == '<':
                if response[-1] == '>':
                    if ':' in response:
                        response.replace('<','')
                        response.replace('>','')

                        return response.split(':')
        # Not valid response
        raise InstrumentProtocolException('Not valid instrument response %s' % response)


    ################################################################################
    # Autosample state handlers
    ################################################################################
    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        self._protocol_fsm.on_event(ProtocolEvent.INIT_PARAMS)
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

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return  next_state, (next_agent_state, result) if successful.
        incorrect prompt received.
        """
        result = None

        # Wake up the device, continuing until autosample prompt seen.
        timeout = kwargs.get('timeout', TIMEOUT)

        next_state = ProtocolState.COMMAND
        next_agent_state = ResourceAgentState.COMMAND

        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        return next_state, (next_agent_state, result)

    def _handler_recover_autosample(self, *args, **kwargs):
        """
        Reenter autosample mode.  Used when our data handler detects
        as data sample.
        @return next_state, next_agent_state
        """
        next_state = ProtocolState.AUTOSAMPLE
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
        if Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY] in params:
            if (params[Parameter.SAMPLE_INTERVAL] != self._param_dict.get(
                    Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY])):
                self._param_dict.set_value(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                                           params[Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]])
                if params[Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]] == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job( ScheduledJob.SAMPLE)
                changed = True

        if Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY] in params:
            if (params[Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]] != self._param_dict.get(
                    Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY])):
                self._param_dict.set_value(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                           params[Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]])
                if params[Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]] == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job(ScheduledJob.STATUS)
                changed = True

        if Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY] in params:
            if (params[Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]] != self._param_dict.get(
                    Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY])):
                self._param_dict.set_value(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                           params[Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]])

                self.forwarding_time = params[Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]]
                if self.video_forwarding_flag == True:
                    if self.forwarding_time != ZERO_TIME_INTERVAL:
                        self.start_scheduled_job(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                                 ScheduledJob.VIDEO_FORWARDING,
                                                 ProtocolEvent.STOP_FORWARD)
                if self.forwarding_time == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job(ScheduledJob.VIDEO_FORWARDING)
                changed = True

        if Parameter.VIDEO_FORWARDING[ParameterIndex.KEY] in params:
            if (params[Parameter.VIDEO_FORWARDING[ParameterIndex.KEY]] != self._param_dict.get(
                    Parameter.VIDEO_FORWARDING[ParameterIndex.KEY])):
                self._param_dict.set_value(Parameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                                           params[Parameter.VIDEO_FORWARDING[ParameterIndex.KEY]])
                self.video_forwarding_flag = params[Parameter.VIDEO_FORWARDING[ParameterIndex.KEY]]

                if self.video_forwarding_flag == True:
                    if self.forwarding_time != ZERO_TIME_INTERVAL:
                        self.start_scheduled_job(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                                 ScheduledJob.VIDEO_FORWARDING,
                                                 ProtocolEvent.STOP_FORWARD)
                changed = True

        if Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY] in params:
            if (params[Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]] != self._param_dict.get(
                    Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])):
                self._param_dict.set_value(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                           params[Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]])
                changed = True

        if Parameter.PRESET_NUMBER[ParameterIndex.KEY] in params:
            if (params[Parameter.PRESET_NUMBER[ParameterIndex.KEY]] != self._param_dict.get(
                    Parameter.PRESET_NUMBER[ParameterIndex.KEY])):
                self._param_dict.set_value(Parameter.PRESET_NUMBER[ParameterIndex.KEY],
                                           params[Parameter.PRESET_NUMBER[ParameterIndex.KEY]])
                changed = True

        if changed:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        result = self._set_params(params, startup)

        return next_state, result


    def _handler_capture_start(self, *args, **kwargs):
        result = None

        kwargs['timeout'] = 30
        result = self._do_cmd_resp(InstrumentCmds.START_CAPTURE, *args, **kwargs)

        next_state = None
        next_agent_state = None
        return next_state, (next_agent_state, result)

    def _handler_capture_stop(self, *args, **kwargs):
        """
        @reval next_state, (next_agent_state, result)
        """

        result = None
        kwargs['timeout'] = 30

        result = self._do_cmd_resp(InstrumentCmds.STOP_CAPTURE, *args, **kwargs)

        # Wake up the device, continuing until autosample prompt seen.
        timeout = kwargs.get('timeout', TIMEOUT)

        (next_state, next_agent_state) = self._discover()

        return next_state, (next_agent_state, result)

    def add_build_command_handlers(self):
        """
        Add build handlers for device commands.
        """

        self._add_build_handler(InstrumentCmds.SET, self._build_set_command)
        self._add_build_handler(InstrumentCmds.GET, self._build_get_command)

        self._add_build_handler(InstrumentCmds.START_CAPTURE, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.STOP_CAPTURE, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.TAKE_SNAPSHOT, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.START_FOCUS_NEAR, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.START_FOCUS_FAR, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.STOP_FOCUS, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.START_ZOOM_OUT, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.START_ZOOM_IN, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.STOP_ZOOM, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.INCREASE_IRIS, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.DECREASE_IRIS, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.GO_TO_PRESET, self.build_preset_command)
        self._add_build_handler(InstrumentCmds.SET_PRESET, self.build_preset_command)

        self._add_build_handler(InstrumentCmds.START_PAN_LEFT, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.START_PAN_RIGHT, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.STOP_PAN, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.START_TILT_UP, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.START_TILT_DOWN, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.STOP_TILT, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.TILE_UP_SOFT, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.TILE_DOWN_SOFT, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.PAN_LEFT_SOFT, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.PAN_RIGHT_SOFT, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.LAMP_ON, self.build_simple_command)
        self._add_build_handler(InstrumentCmds.LAMP_OFF, self.build_simple_command)

        self._add_build_handler(InstrumentCmds.LASER_ON, self.build_laser_command)
        self._add_build_handler(InstrumentCmds.LASER_OFF, self.build_laser_command)

        self._add_build_handler(InstrumentCmds.GET_DISK_USAGE, self.build_status_command)
        self._add_build_handler(InstrumentCmds.HEALTH_REQUEST, self.build_status_command)

    def add_response_handlers(self):
        """
        Add response_handlers
        """
        # add response_handlers
        self._add_response_handler(InstrumentCmds.SET, self._parse_set_response)
        self._add_response_handler(InstrumentCmds.GET, self._parse_get_response)

        self._add_response_handler(InstrumentCmds.SET_PRESET, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.GO_TO_PRESET, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.LAMP_OFF, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.LAMP_ON, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.LASER_OFF, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.LASER_ON, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.PAN_LEFT_SOFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.PAN_RIGHT_SOFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.DECREASE_IRIS, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_CAPTURE, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_FOCUS_FAR, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_FOCUS_NEAR, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_PAN_RIGHT, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_PAN_LEFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_TILT_DOWN, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_TILT_UP, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.TILE_UP_SOFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.TILE_DOWN_SOFT, self._parse_simple_response)

        #Generate data particle
        self._add_response_handler(InstrumentCmds.GET_DISK_USAGE, self._parse_simple_response)

        #Generate data particle
        self._add_response_handler(InstrumentCmds.HEALTH_REQUEST, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_ZOOM_IN, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.START_ZOOM_OUT, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.INCREASE_IRIS, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.TAKE_SNAPSHOT, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.STOP_ZOOM, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.STOP_CAPTURE, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.STOP_FOCUS, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.STOP_PAN, self._parse_simple_response)
        self._add_response_handler(InstrumentCmds.STOP_TILT, self._parse_simple_response)


    def get_config_metadata_dict(self):
        """
        Return a list of metadata about the protocol's driver support,
        command formats, and parameter formats. The format should be easily
        JSONifyable (as will happen in the driver on the way out to the agent)
        @return A python dict that represents the metadata
        @see https://confluence.oceanobservatories.org/display/syseng/
                   CIAD+MI+SV+Instrument+Driver-Agent+parameter+and+command+metadata+exchange
        """
        return_dict = {}
        return_dict[ConfigMetadataKey.DRIVER] = self._driver_dict.generate_dict()
        return_dict[ConfigMetadataKey.COMMANDS] = self._cmd_dict.generate_dict()
        return_dict[ConfigMetadataKey.PARAMETERS] = self._param_dict.generate_dict()

        return return_dict


    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """

        if (self._extract_sample(CAMDS_DISK_STATUS,
                                 CAMDS_DISK_STATUS_MATCHER_COM,
                                 chunk,
                                 timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_DISK_STATUS")

        elif (self._extract_sample(CAMDS_HEALTH_STATUS,
                                   CAMDS_HEALTH_STATUS_MATCHER_COM,
                                   chunk,
                                   timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_HEALTH_STATUS")

        elif (self._extract_sample(CAMDS_IMAGE_METADATA,
                                   CAMDS_SNAPSHOT_MATCHER_COM,
                                   chunk,
                                   timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_IMAGE_METADATA(Snapshot)")

        elif (self._extract_sample(CAMDS_IMAGE_METADATA,
                                   CAMDS_START_CAPTURING_COM,
                                   chunk,
                                   timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_IMAGE_METADATA(Start Capturing)")

        elif (self._extract_sample(CAMDS_IMAGE_METADATA,
                                   CAMDS_STOP_CAPTURING_COM,
                                   chunk,
                                   timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_IMAGE_METADATA(Stop Capturing")

    def _extract_sample(self, particle_class, regex, line, timestamp, publish=True):
        """
        Override this method as we have special cases for CAMDS
        Extract sample from a response line if present and publish
        parsed particle

        @param particle_class The class to instantiate for this specific
            data particle. Parameterizing this allows for simple, standard
            behavior from this routine
        @param regex The regular expression that matches a data sample
        @param line string to match for sample.
        @param timestamp port agent timestamp to include with the particle
        @param publish boolean to publish samples (default True). If True,
               two different events are published: one to notify raw data and
               the other to notify parsed data.

        @retval dict of dicts {'parsed': parsed_sample, 'raw': raw_sample} if
                the line can be parsed for a sample. Otherwise, None.
        @todo Figure out how the agent wants the results for a single poll
            and return them that way from here
        """

        if regex.match(line):

            particle = None

            # special case for the CAMDS image metadata particle - need to pass in param_dict
            if particle_class is CAMDS_IMAGE_METADATA:
                particle = particle_class(self._param_dict, port_timestamp=timestamp)
            else:
                particle = particle_class(line, port_timestamp=timestamp)

            parsed_sample = particle.generate()

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample

    def _get_params(self):
        return dir(Parameter)

    def _getattr_key(self, attr):
        return getattr(Parameter, attr)

    def _has_parameter(self, param):
        return Parameter.has(param)

    def _send_wakeup(self):
        """
        Send a wakeup to the device. Overridden by device specific
        subclasses.
        """
        self._connection.send(NEWLINE)
