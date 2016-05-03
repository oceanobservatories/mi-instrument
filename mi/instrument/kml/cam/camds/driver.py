"""
@package mi.instrument.KML.CAM.driver
@file mi-instrument/mi/instrument/kml/cam/driver.py
@author Sung Ahn
@brief Driver for the CAMDS

"""

import time
import re
import os
import struct

from threading import Timer

from mi.core.common import BaseEnum
from mi.core.common import Units
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import SampleException

from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import DataParticleValue
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
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType

from mi.core.driver_scheduler import DriverSchedulerConfigKey
from mi.core.driver_scheduler import TriggerType

from mi.core.log import get_logger

log = get_logger()
from mi.core.log import get_logging_metaclass

from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.chunker import StringChunker

# default timeout.
TIMEOUT = 20

METALOGGER = get_logging_metaclass()

# newline.
NEWLINE = '\r\n'

DEFAULT_CMD_TIMEOUT = 20
DEFAULT_WRITE_DELAY = 0

# Time taken by the camera to 'recover' from taking a single image, in seconds
CAMERA_RECOVERY_TIME = 30

ZERO_TIME_INTERVAL = '00:00:00'

# Enforce  maximum duration for auto capture
MAX_AUTO_CAPTURE_DURATION = 5

RE_PATTERN = type(re.compile(""))

DEFAULT_DICT_TIMEOUT = 30

DEFAULT_PRESET_POSITION = 0

DEFAULT_USER_PRESET_POSITION = 1

# 'NAK' reply from the instrument, indicating bad command sent to the instrument
NAK = '\x15'

# Minimum length of a response form the CAMDS Instrument
MIN_RESPONSE_LENGTH = 7

# Regex to extract image date from the file name
IMG_REGEX = '.*_(\d{4})(\d{2})(\d{2})T.*png'
IMG_PATTERN = re.compile(IMG_REGEX, re.DOTALL)

# Particle Regex's'
CAMDS_DISK_STATUS_MATCHER = r'<\x0B:\x06:GC.+?>'
CAMDS_DISK_STATUS_MATCHER_COM = re.compile(CAMDS_DISK_STATUS_MATCHER, re.DOTALL)
CAMDS_HEALTH_STATUS_MATCHER = r'<\x07:\x06:HS.+?>'
CAMDS_HEALTH_STATUS_MATCHER_COM = re.compile(CAMDS_HEALTH_STATUS_MATCHER, re.DOTALL)
CAMDS_IMAGE_FILE_MATCHER = r'New Image:(.+\.png)'
CAMDS_IMAGE_FILE_MATCHER_COM = re.compile(CAMDS_IMAGE_FILE_MATCHER, re.DOTALL)


def camds_failure_message(error_code):
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


def validate_response(response):
    if len(response) < MIN_RESPONSE_LENGTH:
        return False
    if not all([response.startswith('<'), response.endswith('>')]):
        return False

    if response[3] == NAK:
        reason = camds_failure_message(response[5])
        raise InstrumentProtocolException(
            'NAK received from instrument: ' + reason)

    return True


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


# HS command
class CamdsHealthStatusKey(BaseEnum):
    """
    cam health status keys
    """
    temp = "camds_temp"
    humidity = "camds_humidity"
    error = "camds_error"


# Data particle for HS
class CamdsHealthStatus(DataParticle):
    """
    cam health status data particle
    """
    _data_particle_type = DataParticleType.CAMDS_HEALTH_STATUS

    TEMP_INDEX = 7
    HUMIDITY_INDEX = 8
    ERROR_INDEX = 9

    def _build_parsed_values(self):
        # check the response
        if not validate_response(self.raw_data):
            log.error("Invalid response received for Health status request: %r" + self.raw_data)
            return

        int_bytes = bytearray(self.raw_data)

        parsed_sample = [{DataParticleKey.VALUE_ID: CamdsHealthStatusKey.temp,
                          DataParticleKey.VALUE: int_bytes[self.TEMP_INDEX]},
                         {DataParticleKey.VALUE_ID: CamdsHealthStatusKey.humidity,
                          DataParticleKey.VALUE: int_bytes[self.HUMIDITY_INDEX]},
                         {DataParticleKey.VALUE_ID: CamdsHealthStatusKey.error,
                          DataParticleKey.VALUE: int_bytes[self.ERROR_INDEX]}]

        log.debug("CAMDS_HEALTH_STATUS: Finished building particle: %s" % parsed_sample)

        return parsed_sample


# GC command
class CamdsDiskStatusKey(BaseEnum):
    """
    cam disk status keys
    """
    size = "camds_disk_size"
    disk_remaining = "camds_disk_remaining"
    image_remaining = "camds_images_remaining"
    image_on_disk = "camds_images_on_disk"


# Data particle for GC command
class CamdsDiskStatus(DataParticle):
    """
    cam disk status data particle
    """
    _data_particle_type = DataParticleType.CAMDS_DISK_STATUS

    def _build_parsed_values(self):
        response_stripped = self.raw_data.strip()

        # check the response
        if not validate_response(response_stripped):
            log.error("Invalid response received for Disk status request: %r" + response_stripped)
            return

        byte1, byte2, byte3, images_remaining, images_on_disk = struct.unpack('!3B2H', response_stripped[7:14])

        available_disk = byte1 * pow(10, byte2)
        available_disk_percent = byte3

        parsed_sample = [{DataParticleKey.VALUE_ID: CamdsDiskStatusKey.size,
                          DataParticleKey.VALUE: available_disk},
                         {DataParticleKey.VALUE_ID: CamdsDiskStatusKey.disk_remaining,
                          DataParticleKey.VALUE: available_disk_percent},
                         {DataParticleKey.VALUE_ID: CamdsDiskStatusKey.image_remaining,
                          DataParticleKey.VALUE: images_remaining},
                         {DataParticleKey.VALUE_ID: CamdsDiskStatusKey.image_on_disk,
                          DataParticleKey.VALUE: images_on_disk}]

        log.debug("CAMDS_DISK_STATUS: Finished building particle: %s" % parsed_sample)

        return parsed_sample


# Data particle for CAMDS Image Metadata
class CamdsImageMetadata(DataParticle):
    """
    camds_image_metadata particle
    """
    _data_particle_type = DataParticleType.CAMDS_IMAGE_METADATA

    def __init__(self, raw_data,
                 img_filename,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        # Construct particle superclass.
        DataParticle.__init__(self, raw_data, port_timestamp, internal_timestamp, preferred_timestamp,
                              quality_flag, new_sequence)

        self._image_filename = img_filename

    def _build_parsed_values(self):
        # Initialize

        result = []

        log.debug("CAMDS_IMAGE_METADATA: Building data particle...")

        param_dict = self.raw_data.get_all()

        log.debug("Param Dict: %s" % param_dict)

        match = IMG_PATTERN.match(self._image_filename)

        if not match:
            raise SampleException("No regex match for image filename: %s" % self._image_filename)

        year = match.group(1)
        month = match.group(2)
        day = match.group(3)

        image_path = os.path.join(year, month, day, self._image_filename)

        result.append({DataParticleKey.VALUE_ID: "filepath",
                       DataParticleKey.VALUE: image_path})
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
        result.append({DataParticleKey.VALUE_ID: "camds_brightness",
                       DataParticleKey.VALUE: param_dict.get(Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_gain",
                       DataParticleKey.VALUE: param_dict.get(Parameter.CAMERA_GAIN[ParameterIndex.KEY])})

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
    NTP_SETTING = ('NT', '<\x03:GN:>', 4, None, '\x00\x00157.237.237.104\x00', 'NTP Setting',
                   'NTP server connection data.', 'NTP_SETTING', None)

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
                         'How to handle full disk (Overwrite Oldest Image: 1 | Prevent Capture: 2)',
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
                   'Capture mode of the camera (None:0 | Stream:9 | Framing:10 | Focus:11)', 'CAMERA_MODE', 9)

    """
    set <\x04:FR:\x1E>
    1 Byte with value between 1 and 30. If the requested frame rate cannot be achieved, the maximum rate will be used
    Default is 0x1E : set <0x04:FR:0x1E>

    get <0x03:GR:>
    GR + 1 Byte with value between 1 and 30.
    If the requested frame rate cannot be achieved, the maximum rate will be used.
    """
    FRAME_RATE = ('FR', '<\x03:GR:>', 1, 1, '\x1E', 'Frame Rate', 'Capture rate in frames per second (1-30)', 'FRAME_RATE', 30)

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
                        'Image Resolution', 'Resolution of streamed images (Full Resolution:1 | Half Resolution:2 | Quarter Resolution:4 | Eighth Resolution:8)', 'IMAGE_RESOLUTION', 1)

    """
    set <\x04:CD:\x64>
    1 Byte with value between 0x01 and 0x64. (decimal 1 - 100)
    0x64 = Minimum data loss
    0x01 = Maximum data loss
    Default is 0x64 : set <0x04:CD:0x64>

    get <0x03:GI:>
    GI + 1 Byte with value between 0x01 and 0x64. (decimal 1 - 100) 0x64 = Minimum data loss 0x01 = Maximum data loss
    """
    COMPRESSION_RATIO = ('CD', '<\x03:GI:>', 1, 1, '\x64', 'Compression Ratio',
                         'Compression ratio of streamed images (1-100)', 'COMPRESSION_RATIO', 100)

    """
    get <\x04:GS:\xFF>
    byte Value 0x01 to 0x20 sets a static value and 0xFF sets auto gain.
    In automatic gain control, the camera will attempt to adjust the gain to give the optimal exposure.
    Default is 0xFF : set <\x04:GS:\xFF>

    get <0x03:GG:>
    GG + 1 byte
    Value 0x01 to 0x20 for a static value and 0xFF for auto GAIN
    """
    CAMERA_GAIN = ('GS', '<\x03:GG:>', 1, 1, '\xFF', 'Camera Gain', 'Gain of the camera (1-32 | Auto:255)',
                   'CAMERA_GAIN', None)

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
    LAMP_BRIGHTNESS = ('BF', '<\x03:PF:>', 1, 2, '\x03\x32', 'Lamp Brightness',
                       'Brightness level for lamps (0-100)', 'LAMP_BRIGHTNESS', None)

    """
    Set <\x04:IG:\x08>
    Iris_Position
    1 byte between 0x00 and 0x0F
    default is 0x08 <0x04:IG:0x08>

    IP + 1 byte between 0x00
    get <0x03:IP>
    """
    IRIS_POSITION = ('IG', '<\x03:IP:>', 1, 1, '\x08', 'Iris Position', 'Position of iris (0-15)', 'IRIS_POSITION', None)

    """
    Zoom Position
    set <\x04:ZG:\x64>

    1 byte between 0x00 and 0xC8 (200 Zoom positions)
    Default value is <0x04:ZG:0x64>
    ZP + 1 byte between 0x00 and 0xC8
    get <0x03:ZP:>
    """
    ZOOM_POSITION = ('ZG', '<\x03:ZP:>', 1, 1, '\x64', 'Zoom Position', 'Position of zoom (0-200)', 'ZOOM_POSITION', None)

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
    PAN_POSITION = ('PP', '<\x03:AS:>', 4, 3, '\x30\x37\x35', 'Pan Position',
                    'Position to pan to (0-360)',
                    'PAN_POSITION', None)

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
    TILT_POSITION = ('TP', '<\x03:AS:>', 1, 3, '\x30\x37\x35', 'Tilt Position',
                     'Position to tilt to (0-360)',
                     'TILT_POSITION', None)

    """
    set <\x04:FG:\x64>
    1 byte between 0x00 and 0xC8

    get <\x03:FP:>
    """
    FOCUS_POSITION = ('FG', '<\x03:FP:>', 1, 1, '\x64', 'Focus Position', 'Position of focus (0-200)', 'FOCUS_POSITION', None)

    # Engineering parameters for the scheduled commands
    SAMPLE_INTERVAL = (None, None, None, None, '00:30:00', 'Sample Interval',
                       'Time to wait between time-lapsed samples.', 'SAMPLE_INTERVAL', '00:30:00')
    ACQUIRE_STATUS_INTERVAL = (None, None, None, None, '00:00:00', 'Acquire Status Interval',
                               'Time to wait between acquiring status.', 'ACQUIRE_STATUS_INTERVAL', '00:00:00')
    VIDEO_FORWARDING = (None, None, None, None, 'N', 'Video Forwarding Flag',
                        'Enable streaming live video (Yes:Y | No:N)', 'VIDEO_FORWARDING', 'N')
    VIDEO_FORWARDING_TIMEOUT = (None, None, None, None, '01:00:00', 'Video Forwarding Timeout',
                                'Length of time to stream live video.', 'VIDEO_FORWARDING_TIMEOUT', '01:00:00')
    PRESET_NUMBER = (None, None, None, None, 1, 'Preset number', 'Preset number to set/go to (1-15)', 'PRESET_NUMBER', 1)
    AUTO_CAPTURE_DURATION = (None, None, None, None, 3, 'Auto Capture Duration',
                             'How long to run auto capture mode. Zero indicates snapshot (0-5)',
                             'AUTO_CAPTURE_DURATION', 3)


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

    START_RECOVER = "DRIVER_EVENT_START_RECOVER"
    RECOVER_COMPLETE = "DRIVER_EVENT_RECOVER_COMPLETE"

    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE

    LASERS_ON = "DRIVER_EVENT_LASERS_ON"
    LASERS_OFF = "DRIVER_EVENT_LASERS_OFF"

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

    LASERS_ON = ProtocolEvent.LASERS_ON
    LASERS_OFF = ProtocolEvent.LASERS_OFF

    LAMP_ON = ProtocolEvent.LAMP_ON
    LAMP_OFF = ProtocolEvent.LAMP_OFF

    GOTO_PRESET = ProtocolEvent.GOTO_PRESET

    GET = DriverEvent.GET
    SET = DriverEvent.SET

    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT

    DISCOVER = DriverEvent.DISCOVER


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
    RECOVERY = "DRIVER_STATE_RECOVERY"
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class InstrumentCommands(BaseEnum):
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

    LASERS_ON = 'OL'
    LASERS_OFF = 'NL'

    GET_DISK_USAGE = 'GC'
    HEALTH_REQUEST = 'HS'

    GET = 'get'
    SET = 'set'


class InstrumentCommandNames(BaseEnum):
    """
    Display Names for each command - to be used in Direct Access for button labels
    """
    START_CAPTURE = 'Start Capture'
    STOP_CAPTURE = 'Stop Capture'

    TAKE_SNAPSHOT = 'Take Snapshot'

    START_FOCUS_NEAR = 'Start Focus Near'
    START_FOCUS_FAR = 'Start Focus Far'
    STOP_FOCUS = 'Stop Focus'

    START_ZOOM_OUT = 'Start Zoom Out'
    START_ZOOM_IN = 'Start Zoom In'
    STOP_ZOOM = 'Stop Zoom'

    INCREASE_IRIS = 'Increase Iris'
    DECREASE_IRIS = 'Decreate Iris'

    START_PAN_LEFT = 'Start Pan Left'
    START_PAN_RIGHT = 'Start Pan Right'
    STOP_PAN = 'Stop Pan'

    START_TILT_UP = 'Start Tilt Up'
    START_TILT_DOWN = 'Start Tilt Up'
    STOP_TILT = 'Stop Tilt'

    GO_TO_PRESET = 'Goto Preset>'

    TILE_UP_SOFT = 'Tile Up'
    TILE_DOWN_SOFT = 'Tile Down'
    PAN_LEFT_SOFT = 'Pan Left'
    PAN_RIGHT_SOFT = 'Pan Right'
    SET_PRESET = 'Set Preset'

    LAMP_ON = 'Lamp On'
    LAMP_OFF = 'Lamp Off'

    LASERS_ON = 'Lasers On'
    LASERS_OFF = 'Lasers Off'

    GET_DISK_USAGE = 'Get Disk Usage'
    HEALTH_REQUEST = 'Health'

    GET = 'Get>'
    SET = 'Set>'


###############################################################################
# Driver
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass for cam driver.
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """
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
# noinspection PyUnusedLocal,PyMethodMayBeStatic
class CAMDSProtocol(CommandResponseInstrumentProtocol):
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

        self._direct_commands['Newline'] = self._newline
        command_dict = InstrumentCommands.dict()
        label_dict = InstrumentCommandNames.dict()
        for key in command_dict:
            label = label_dict.get(key)
            command = command_dict[key]
            builder = self._build_handlers.get(command, None)
            if builder in [self._build_simple_command, self.build_status_command]:
                command = builder(command)
                self._direct_commands[label] = command
            if builder is self.build_laser_command:
                command = '<\x04:%s:' % command

    @staticmethod
    def sieve_function(raw_data):
        """
        Chunker sieve method to help the chunker identify chunks.
        @returns a list of chunks identified, if any.
        The chunks are all the same type.
        """

        sieve_matchers = [CAMDS_IMAGE_FILE_MATCHER_COM,
                          CAMDS_DISK_STATUS_MATCHER_COM,
                          CAMDS_HEALTH_STATUS_MATCHER_COM]

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
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASERS_ON,
                                       self._handler_command_lasers_on)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.LASERS_OFF,
                                       self._handler_command_lasers_off)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET_PRESET,
                                       self._handler_command_set_preset)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GOTO_PRESET,
                                       self._handler_command_goto_preset)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.STOP_CAPTURE,
                                       self._handler_command_stop_capture)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXECUTE_AUTO_CAPTURE,
                                       self._handler_command_start_capture)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_RECOVER,
                                       self._handler_command_start_recovery)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.STOP_FORWARD,
                                       self._handler_command_stop_forward)

        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ENTER,
                                       self._handler_autosample_enter)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXIT,
                                       self._handler_autosample_exit)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE,
                                       self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GET,
                                       self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.SET,
                                       self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ACQUIRE_SAMPLE,
                                       self._handler_autosample_acquire_sample)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LAMP_ON,
                                       self._handler_command_lamp_on)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LAMP_OFF,
                                       self._handler_command_lamp_off)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASERS_ON,
                                       self._handler_command_lasers_on)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.LASERS_OFF,
                                       self._handler_command_lasers_off)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.SET_PRESET,
                                       self._handler_command_set_preset)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GOTO_PRESET,
                                       self._handler_command_goto_preset)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_CAPTURE,
                                       self._handler_autosample_stop_capture)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXECUTE_AUTO_CAPTURE,
                                       self._handler_autosample_start_capture)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.START_RECOVER,
                                       self._handler_command_start_recovery)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_FORWARD,
                                       self._handler_command_stop_forward)

        self._protocol_fsm.add_handler(ProtocolState.RECOVERY, ProtocolEvent.ENTER,
                                       self._handler_recovery_enter)
        self._protocol_fsm.add_handler(ProtocolState.RECOVERY, ProtocolEvent.EXIT,
                                       self._handler_recovery_exit)
        self._protocol_fsm.add_handler(ProtocolState.RECOVERY, ProtocolEvent.RECOVER_COMPLETE,
                                       self._handler_recovery_complete)

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
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.NTP_SETTING[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.NTP_SETTING[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=True,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.WHEN_DISK_IS_FULL[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.WHEN_DISK_IS_FULL[ParameterIndex.DESCRIPTION],
                             range={'Overwrite Oldest Image': 1, 'Prevent Capture': 2},
                             startup_param=False,
                             direct_access=True,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.CAMERA_MODE[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.CAMERA_MODE[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.CAMERA_MODE[ParameterIndex.DESCRIPTION],
                             range={'None': 0, 'Stream': 9, 'Framing': 10, 'Focus': 11},
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.CAMERA_MODE[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.FRAME_RATE[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.FRAME_RATE[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.FRAME_RATE[ParameterIndex.DESCRIPTION],
                             range=(1, 30),
                             startup_param=True,
                             direct_access=True,
                             units=Units.FRAMES_PER_SECOND,
                             default_value=Parameter.FRAME_RATE[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.IMAGE_RESOLUTION[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.IMAGE_RESOLUTION[ParameterIndex.DESCRIPTION],
                             range={'Full Resolution': 1, 'Half Resolution': 2, 'Quarter Resolution': 4,
                                    'Eighth Resolution': 8},
                             direct_access=True,
                             startup_param=True,
                             default_value=Parameter.IMAGE_RESOLUTION[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.COMPRESSION_RATIO[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.COMPRESSION_RATIO[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.COMPRESSION_RATIO[ParameterIndex.DESCRIPTION],
                             range=(1, 100),
                             startup_param=True,
                             direct_access=True,
                             default_value=Parameter.COMPRESSION_RATIO[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.CAMERA_GAIN[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.CAMERA_GAIN[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.CAMERA_GAIN[ParameterIndex.DESCRIPTION],
                             # range is 1 to 32 & 255 (auto). However, the value sometimes comes back as zero from the
                             # instrument
                             range=(1, 255),
                             startup_param=False,
                             direct_access=False)

        self._param_dict.add(Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.LAMP_BRIGHTNESS[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.LAMP_BRIGHTNESS[ParameterIndex.DESCRIPTION],
                             range=(0, 100),
                             startup_param=False,
                             direct_access=False)

        self._param_dict.add(Parameter.FOCUS_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.FOCUS_POSITION[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.FOCUS_POSITION[ParameterIndex.DESCRIPTION],
                             range=(0, 200),
                             startup_param=False,
                             direct_access=False)

        self._param_dict.add(Parameter.IRIS_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.IRIS_POSITION[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.IRIS_POSITION[ParameterIndex.DESCRIPTION],
                             range=(0, 15),
                             startup_param=False,
                             direct_access=False)

        self._param_dict.add(Parameter.ZOOM_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.ZOOM_POSITION[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.ZOOM_POSITION[ParameterIndex.DESCRIPTION],
                             range=(0, 200),
                             startup_param=False,
                             direct_access=False)

        self._param_dict.add(Parameter.PAN_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.PAN_POSITION[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.PAN_POSITION[ParameterIndex.DESCRIPTION],
                             range=(0, 360),
                             startup_param=False,
                             direct_access=False,
                             units=Units.DEGREE_PLANE_ANGLE,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.TILT_POSITION[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.TILT_POSITION[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.TILT_POSITION[ParameterIndex.DESCRIPTION],
                             range=(0, 360),
                             startup_param=False,
                             direct_access=False,
                             units=Units.DEGREE_PLANE_ANGLE,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.SAMPLE_INTERVAL[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.SAMPLE_INTERVAL[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=False,
                             units= "HH:MM:SS",
                             default_value=Parameter.SAMPLE_INTERVAL[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=False,
                             units= "HH:MM:SS",
                             default_value=Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.VIDEO_FORWARDING[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.VIDEO_FORWARDING[ParameterIndex.DESCRIPTION],
                             range={'Yes': 'Y', 'No': 'N'},
                             startup_param=False,
                             direct_access=False,
                             default_value=Parameter.VIDEO_FORWARDING[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name=Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=False,
                             units= "HH:MM:SS",
                             default_value=Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.PRESET_NUMBER[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.PRESET_NUMBER[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.PRESET_NUMBER[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=False,
                             range=(1, 15),
                             default_value=Parameter.PRESET_NUMBER[ParameterIndex.D_DEFAULT])

        self._param_dict.add(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                             r'NOT USED',
                             None,
                             int,
                             type=ParameterDictType.INT,
                             display_name=Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.DISPLAY_NAME],
                             description=Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.DESCRIPTION],
                             startup_param=False,
                             direct_access=False,
                             range=(0, 5),
                             units=Units.SECOND,
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
        self._cmd_dict.add(Capability.LAMP_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Lamp Off",
                           description="Turn off the lamp")
        self._cmd_dict.add(Capability.LAMP_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Lamp On",
                           description="Turn on the lamp")
        self._cmd_dict.add(Capability.LASERS_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Lasers Off",
                           description="Turn off the lasers")
        self._cmd_dict.add(Capability.LASERS_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Lasers On",
                           description="Turn on the lasers")
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, False)

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary.
        """
        # Get old param dict config.
        old_config = self._param_dict.get_config()

        params = Parameter.list()

        results = ""

        for param in params:

            # These are engineering params
            if param[ParameterIndex.KEY] not in [Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                                                 Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                                                 Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                                 Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                                 Parameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                                                 Parameter.PRESET_NUMBER[ParameterIndex.KEY],
                                                 'ALL']:

                if param in ['DRIVER_PARAMETER_ALL']:
                    continue

                result = self._do_cmd_resp(InstrumentCommands.GET, param, **kwargs)
                results += result + NEWLINE

        new_config = self._param_dict.get_config()

        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        return results

    def _update_metadata_params(self):
        """
        Update parameters specific to the camds_image_metadata particle.
        Also get the frame rate here, as it's needed to calculate how
        long the driver will sleep after capture.
        """

        error = None
        results = None

        try:
            # Get old param dict config.
            old_config = self._param_dict.get_config()

            params = Parameter.list()

            results = ""

            for param in params:

                if param[ParameterIndex.KEY] in [Parameter.PAN_POSITION[ParameterIndex.KEY],
                                                 Parameter.TILT_POSITION[ParameterIndex.KEY],
                                                 Parameter.FOCUS_POSITION[ParameterIndex.KEY],
                                                 Parameter.ZOOM_POSITION[ParameterIndex.KEY],
                                                 Parameter.IRIS_POSITION[ParameterIndex.KEY],
                                                 Parameter.FRAME_RATE[ParameterIndex.KEY],
                                                 Parameter.CAMERA_GAIN[ParameterIndex.KEY],
                                                 Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY],
                                                 Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]]:
                    result = self._do_cmd_resp(InstrumentCommands.GET, param)
                    results += result + NEWLINE

            new_config = self._param_dict.get_config()

            if new_config != old_config:
                self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        # Catch all errors then rethrow the error
        except Exception as e:
            log.error("EXCEPTION in _update_metadata_params WAS " + str(e))
            error = e

        if error:
            raise error

        return results

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters. The UI values are set in the _update_params call
        """

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        result = None
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        # Before sets are executed, we must first validate certain params that cannot be validated by the UI
        for key, val in params.iteritems():

            # CAMERA_GAIN must be an integer between 1 and 32, or equal to 255 (auto gain)
            if key == Parameter.CAMERA_GAIN[ParameterIndex.KEY]:
                val = params[Parameter.CAMERA_GAIN[ParameterIndex.KEY]]
                if val == 255 or (0 < val < 33):
                    val = chr(val)
                else:
                    raise InstrumentParameterException('The desired value for CAMERA_GAIN must be an integer '
                                                'either equal to 255 or between 1 and 32: %s' % val)

            # Time interval params must have a valid range of 00:00:00 - 99:59:59
            elif key in {Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                            Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                            Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]}:

                val = params[key]
                valid_value_regex = r'^\d{2}:[0-5]\d:[0-5]\d$'
                range_checker = re.compile(valid_value_regex)

                if not range_checker.match(val):
                    raise InstrumentParameterException("Invalid time string value for %s. Format is HH:MM:SS "
                                                       "with a range of 00:00:00-99:59:59" % key)

        self._verify_not_readonly(*args, **kwargs)

        for key, val in params.iteritems():
            log.debug("In _set_params, %s, %s", key, val)

            # These are driver specific parameters. They are not set on the instrument.
            if key not in [Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                           Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY],
                           Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                           Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                           Parameter.VIDEO_FORWARDING[ParameterIndex.KEY],
                           Parameter.PRESET_NUMBER[ParameterIndex.KEY],
                           Parameter.NTP_SETTING[ParameterIndex.KEY],
                           Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY]]:

                result = self._do_cmd_resp(InstrumentCommands.SET, key, val, **kwargs)
                time.sleep(2)

                # The instrument needs extra time to process these commands
                if key in [Parameter.CAMERA_MODE[ParameterIndex.KEY],
                           Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY]]:
                    log.debug("Just set Camera parameters, sleeping for 25 seconds")
                    time.sleep(25)
                elif key in [Parameter.IRIS_POSITION[ParameterIndex.KEY],
                             Parameter.FOCUS_POSITION[ParameterIndex.KEY],
                             Parameter.ZOOM_POSITION[ParameterIndex.KEY]]:
                    log.debug("Just set Camera parameters, sleeping for 10 seconds")
                    time.sleep(10)

        self._update_params()

    def _instrument_config_dirty(self):
        """
        Read the startup config and compare that to what the instrument
        is configured too.  If they differ then return True
        @return: True if the startup config doesn't match the instrument
        """
        startup_params = self._param_dict.get_startup_list()
        log.debug("Startup Parameters: %s" % startup_params)

        for param in startup_params:

            if param in [Parameter.CAMERA_MODE[ParameterIndex.KEY],
                         Parameter.CAMERA_GAIN[ParameterIndex.KEY],
                         Parameter.COMPRESSION_RATIO[ParameterIndex.KEY],
                         Parameter.FOCUS_POSITION[ParameterIndex.KEY],
                         Parameter.FRAME_RATE[ParameterIndex.KEY],
                         Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY],
                         Parameter.IRIS_POSITION[ParameterIndex.KEY],
                         Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY],
                         Parameter.PAN_POSITION[ParameterIndex.KEY],
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
        return command

    def _build_get_command(self, cmd, param, **kwargs):
        """
        param=val followed by newline.
        @param cmd get command
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The get command to be sent to the device.
        """
        self.get_param_tuple = param

        return param[ParameterIndex.GET]

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

        log.debug("Building set command for %r with value %r" % (param, val))

        try:

            if isinstance(val, basestring):
                val = ''.join(chr(int(x)) for x in val.split(':'))

            else:
                if param == Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]:
                    # Set both lamps to an equal value by setting first byte to \x03 which indicates to the instrument
                    # to apply the given value to both lamps
                    val = ''.join( (chr(3), chr(val)) )

                else:
                    val = chr(val)

            data_size = len(val) + 3
            param_tuple = getattr(Parameter, param)

            set_cmd = '<%s:%s:%s>' % (chr(data_size), param_tuple[ParameterIndex.SET], val)
            log.debug("Set command: %r" % set_cmd)

        except KeyError:
                raise InstrumentParameterException('Unknown driver parameter. %s' % param)


        return set_cmd

    def build_status_command(self, cmd):
        """
        Build handler for status command.
        @param cmd the command.
        """
        command = '<\x03:%s:>' % cmd
        return command

    def build_laser_command(self, cmd, data):
        """
        Build handler for laser command.
        @param cmd the command.
        @param data the data value.
        @return The command to be sent to the device.
        """
        command = '<\x04:%s:%s>' % (cmd, data)

        return command

    def build_preset_command(self, cmd, data):
        """
        Build handler for preset command.
        @param cmd the command.
        @param data the data value.
        @return The command to be sent to the device.
        """
        command = '<\x04:%s:%s>' % (cmd, chr(data))
        return command

    def _parse_set_response(self, response, prompt):
        log.debug("SET RESPONSE = %r" % response)

        # Make sure the response is the right format
        response_stripped = response.strip()

        if not validate_response(response_stripped):
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : Set command not recognized. Response is ' + response)

        return response

    def _parse_get_response(self, response, prompt):
        log.debug("GET RESPONSE = " + repr(response))

        start_index = 6

        # Make sure the response is the right format
        response_stripped = response.strip()

        if not validate_response(response_stripped):
            raise InstrumentProtocolException(
                'Protocol._parse_set_response : Get command not recognized. Response is ' + response)

        log.debug("GET RESPONSE : Response for %r is: %s" % (self.get_param_tuple[ParameterIndex.KEY], response_stripped))

        # parse out parameter value first
        if self.get_param_tuple[ParameterIndex.GET] is None:
            # No response data to process
            return

        if self.get_param_tuple[ParameterIndex.LENGTH] is None:
            # There is no guaranteed field size, so substring from the start index to (length-2)
            # Examples are NTP_SETTING
            log.debug("GET RESPONSE : get Length is None")
            raw_value = response_stripped[self.get_param_tuple[ParameterIndex.Start] + start_index: -2]
            log.debug("GET RESPONSE : response raw : %r", raw_value)
            self._param_dict.set_value(self.get_param_tuple[ParameterIndex.KEY], raw_value.strip())

        else:
            start = self.get_param_tuple[ParameterIndex.Start] + start_index
            stop = start + self.get_param_tuple[ParameterIndex.LENGTH]
            raw_value = response_stripped[start:stop]

            if len(raw_value) == 1:
                # Returned value is a one byte value that need to be converted to a numerical value
                log.debug("About to update Parameter %s in param_dict to %s" % (self.get_param_tuple[ParameterIndex.KEY], ord(raw_value)))
                self._param_dict.set_value(self.get_param_tuple[ParameterIndex.KEY], ord(raw_value))

            else:

                if self.get_param_tuple[ParameterIndex.KEY] in [Parameter.PAN_POSITION[ParameterIndex.KEY],
                                                               Parameter.TILT_POSITION[ParameterIndex.KEY]]:

                    log.debug("About to update Parameter %s in param_dict to %s" %
                              (self.get_param_tuple[ParameterIndex.KEY], int(raw_value)))
                    self._param_dict.set_value(self.get_param_tuple[ParameterIndex.KEY], int(raw_value))

                elif self.get_param_tuple[ParameterIndex.KEY] in [Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]]:

                    total_value = 0
                    for curr_value in raw_value:
                        # There should be two bytes, which contain the brightness value of each lamp respectively,
                        # indicating the brightness of each lamp

                        curr_value = ord(curr_value)

                        if curr_value > 127:
                            # If the received lamp brightness value is greater than 127, then that means that the
                            # most significant bit of the value is "1" (128), which indicates that the lamps are
                            # turned on. If it's less than 128, then the lamps are turned off and the brightness
                            # level can be retrieved without any extra work

                            # Convert the received lamp brightness value to binary
                            bin_val = bin(curr_value)

                            if len(bin_val) == 10:
                                # A valid lamp_brightness value received from the instrument, with lamps turned on
                                # and a brightness value between 0 and 100, should have a binary string length of 10
                                # (most significant bit will be a "1"). Trim the first three characters ('0b1') of
                                # the binary string, and use the last 7 least significant bits to get the actual
                                # brightness value.
                                curr_value = int( bin_val[3:] , 2)
                            else:
                                # Received an invalid number; probably a number too large for any proper value for
                                # the lamp brightness param. Should never happen, but...
                                raise InstrumentParameterException("Received an invalid Lamp Brightness value"
                                                                   "from the instrument: %r" % curr_value)

                        elif curr_value < 0 or (100 < curr_value < 128):
                            # Handle negative values and values between 101 and 127. This should never happen, but...
                            raise InstrumentParameterException("Received an invalid Lamp Brightness value"
                                                                   "from the instrument: %r" % curr_value)

                        total_value += curr_value

                    log.debug("lamp brightness total_value: %r" % total_value)
                    brightness = (total_value/2)

                    log.debug("About to update Parameter %s in param_dict to %s" %
                              (self.get_param_tuple[ParameterIndex.KEY], brightness))
                    self._param_dict.set_value(self.get_param_tuple[ParameterIndex.KEY], brightness)


        new_param_value = self._param_dict.get(self.get_param_tuple[ParameterIndex.KEY])
        log.debug("Param Dict Value for %s was set to %s" % (self.get_param_tuple[ParameterIndex.KEY], new_param_value))

        self.get_count = 0

        return response

    def _parse_simple_response(self, response, prompt):

        # Make sure the response is the right format
        response_stripped = response.strip()

        if not validate_response(response_stripped):
            raise InstrumentProtocolException(
                'Protocol._parse_simple_response : command not recognized. Response is ' + response)

        self.get_count = 0
        return response

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
        """
        result = []
        next_state = self._discover()
        return next_state, (next_state, result)

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
        :param param:
        :param schedule_job:
        :param protocol_event:
        :type param: object
        """
        self.stop_scheduled_job(schedule_job)

        hours = 0
        minutes = 0
        seconds = 0

        if param == Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]:
            seconds = self._param_dict.get(param)

            if seconds > MAX_AUTO_CAPTURE_DURATION:
                log.error("Capture duration is greater than maximum permissible value. Not performing capture.")
                raise InstrumentParameterException('Capture duration is greater than maximum permissible value. Not performing capture.')

        else:
            (hours, minutes, seconds) = (int(val) for val in self._param_dict.get(param).split(':'))

            # make sure the sample interval is never less than the instrument recovery time
            # otherwise we'll be trying to collect samples faster than the instrument can process them
            if param == Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]:
                interval_secs = hours * 3600 + minutes * 60 + seconds
                recovery_time = self._calculate_recovery_time()
                if interval_secs < recovery_time:
                    hours = recovery_time / 3600
                    recovery_time %= 3600
                    minutes = recovery_time / 60
                    seconds = recovery_time % 60

        log.debug("Setting scheduled interval for %s to %02d:%02d:%02d" % (param, hours, minutes, seconds))

        if hours == 0 and minutes == 0 and seconds == 0:
            # if interval is all zeroed, then stop scheduling jobs
            self.stop_scheduled_job(schedule_job)
        else:
            config = {DriverConfigKey.SCHEDULER: {
                schedule_job: {
                    DriverSchedulerConfigKey.TRIGGER: {
                        DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.INTERVAL,
                        DriverSchedulerConfigKey.HOURS: hours,
                        DriverSchedulerConfigKey.MINUTES: minutes,
                        DriverSchedulerConfigKey.SECONDS: seconds
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
        if self._init_type != InitializationType.NONE:
            self._update_params()

        # Command device to initialize parameters
        self._init_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        # start scheduled event for get_status only if the interval is not "00:00:00
        status_interval = self._param_dict.get(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY])
        if status_interval != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY],
                                     ScheduledJob.STATUS,
                                     ProtocolEvent.ACQUIRE_STATUS)

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

    def _handler_command_start_direct(self, *args, **kwargs):
        next_state = ProtocolState.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    def _handler_command_start_recovery(self, *args, **kwargs):
        next_state = ProtocolState.RECOVERY
        result = []
        return next_state, (next_state, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @return next_state, (next_state, result) if successful.
        """
        next_state = ProtocolState.AUTOSAMPLE
        result = []
        kwargs['timeout'] = 30

        # first stop scheduled sampling
        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        capture_duration = self._param_dict.get(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])

        # If the capture duration is set to 0, schedule an event to take a snapshot at the sample interval,
        # Otherwise schedule an event to capture a series of images for the capture duration, at the sample interval
        if capture_duration == 0:
            self.start_scheduled_job(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY], ScheduledJob.SAMPLE,
                                     ProtocolEvent.ACQUIRE_SAMPLE)
        else:
            self.start_scheduled_job(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY], ScheduledJob.SAMPLE,
                                     ProtocolEvent.EXECUTE_AUTO_CAPTURE)

        return next_state, (next_state, result)

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        Acquire Sample
        """
        next_state = ProtocolState.RECOVERY
        result = []

        kwargs['timeout'] = 30

        # Before taking a snapshot, update parameters
        self._update_metadata_params()

        log.debug("Acquire Sample: about to take a snapshot")

        self._do_cmd_resp(InstrumentCommands.TAKE_SNAPSHOT, *args, **kwargs)

        log.debug("Acquire Sample: Captured snapshot!")

        # Camera needs time to recover after taking a snapshot
        self._do_recover(CAMERA_RECOVERY_TIME)

        return next_state, (next_state, result)

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Acquire status
        """
        timeout = time.time() + TIMEOUT

        next_state = None

        kwargs['timeout'] = 2

        # Execute the following commands
        #  GET_DISK_USAGE = 'GC'
        #  HEALTH_REQUEST  = 'HS'

        log.debug("ACQUIRE_STATUS: executing status commands...")
        self._do_cmd_resp(InstrumentCommands.GET_DISK_USAGE, *args, **kwargs)

        log.debug("ACQUIRE_STATUS: Executed GET_DISK_USAGE")

        self._do_cmd_resp(InstrumentCommands.HEALTH_REQUEST, *args, **kwargs)

        log.debug("ACQUIRE_STATUS: Executed HEALTH_REQUEST")

        particles = self.wait_for_particles([DataParticleType.CAMDS_DISK_STATUS,
                                             DataParticleType.CAMDS_HEALTH_STATUS], timeout)

        return next_state, (next_state, particles)

    def _handler_command_lamp_on(self, *args, **kwargs):
        """
        Turn the instrument lamp on
        """
        next_state = None
        result = []

        kwargs['timeout'] = 30

        self._do_cmd_resp(InstrumentCommands.LAMP_ON, *args, **kwargs)

        return next_state, (next_state, result)

    def _handler_command_lamp_off(self, *args, **kwargs):
        """
        Turn the instrument lamp off
        """
        next_state = None
        result = []

        kwargs['timeout'] = 30

        self._do_cmd_resp(InstrumentCommands.LAMP_OFF, *args, **kwargs)

        return next_state, (next_state, result)

    def _handler_command_lasers_on(self, *args, **kwargs):
        """
        Turn the lasers on
        """
        next_state = None
        result = []

        kwargs['timeout'] = 2

        self._do_cmd_resp(InstrumentCommands.LASERS_ON, '\x03', **kwargs)

        return next_state, (next_state, result)

    def _handler_command_lasers_off(self, *args, **kwargs):
        """
        Turn the lasers off
        """
        next_state = None
        result = []

        kwargs['timeout'] = 2

        self._do_cmd_resp(InstrumentCommands.LASERS_OFF, '\x03', **kwargs)

        return next_state, (next_state, result)

    def _handler_command_set_preset(self, *args, **kwargs):
        """
        Set preset position
        """
        next_state = None
        result = []

        kwargs['timeout'] = 2

        pd = self._param_dict.get_all()

        # set default preset position
        preset_number = DEFAULT_USER_PRESET_POSITION

        for key, value in pd.iteritems():
            if key == Parameter.PRESET_NUMBER[ParameterIndex.KEY]:
                preset_number = value

        self._do_cmd_resp(InstrumentCommands.SET_PRESET, preset_number, *args, **kwargs)

        return next_state, (next_state, result)

    def _handler_command_start_capture(self, *args, **kwargs):
        """
        Start Auto Capture
        """
        next_state = None
        result = []

        kwargs['timeout'] = 2

        capturing_duration = self._param_dict.get(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])

        if capturing_duration == 0:
            # If duration = 0, then just take a single snapshot
            self._handler_command_acquire_sample(*args, **kwargs)
        elif 0 < capturing_duration < 6:
            # Before performing capture, update parameters
            self._update_metadata_params()

            self.start_scheduled_job(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY],
                                     ScheduledJob.STOP_CAPTURE,
                                     ProtocolEvent.STOP_CAPTURE)
            self._do_cmd_resp(InstrumentCommands.START_CAPTURE, *args, **kwargs)
        else:
            log.error("Capturing Duration %s out of range: Not Performing Capture." % capturing_duration)

        return next_state, (next_state, result)

    def _handler_command_stop_capture(self, *args, **kwargs):
        """
        Stop Auto capture
        """
        next_state = None
        result = []

        kwargs['timeout'] = 2

        self.stop_scheduled_job(ScheduledJob.STOP_CAPTURE)

        self._do_cmd_resp(InstrumentCommands.STOP_CAPTURE, *args, **kwargs)

        # Camera needs time to recover after capturing images
        self._do_recover(self._calculate_recovery_time())

        return next_state, (next_state, result)

    def _handler_command_stop_forward(self, *args, **kwargs):
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
        result = []

        kwargs['timeout'] = 2

        # set to default user preset position
        preset_number = DEFAULT_USER_PRESET_POSITION

        # Check if the user set a preset position, if so, make the camera go to that position
        pd = self._param_dict.get_all()

        for key, value in pd.items():
            if key == Parameter.PRESET_NUMBER[ParameterIndex.KEY]:
                preset_number = value

        log.debug("Commanding camera to go to preset position %s " % preset_number)

        self._do_cmd_resp(InstrumentCommands.GO_TO_PRESET, preset_number, *args, **kwargs)

        return next_state, (next_state, result)

    def _discover(self):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE or UNKNOWN.
        @return next_state, (next_state, result)
        """
        protocol_state = ProtocolState.COMMAND
        result = []

        log.debug("trying to discover state...")

        if self._scheduler_callback is not None:
            if self._scheduler_callback.get(ScheduledJob.SAMPLE):
                protocol_state = ProtocolState.AUTOSAMPLE

        return protocol_state

    def _calculate_recovery_time(self):
        """
        Calculates the camera recovery time per sample taken in autosample mode
        @return: the recovery time in seconds
        """
        # driver won't accept any commands after we stop capture, so sleep for a while.
        # Assuming ~ 30 s recovery time per image, multiply capture duration in seconds
        # by frame_rate*30 to get sleep time in seconds

        # first get the capture duration in seconds...
        duration_seconds = self._param_dict.get(Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY])

        if duration_seconds > MAX_AUTO_CAPTURE_DURATION:
            duration_seconds = MAX_AUTO_CAPTURE_DURATION

        frame_rate = int(self._param_dict.get(Parameter.FRAME_RATE[ParameterIndex.KEY]))
        log.debug("Recovery time: Frame rate: %s, Capture duration: %s" % (frame_rate, duration_seconds))

        # Capture duration of 0 means we take a single image
        if duration_seconds == 0:
            recovery_time = CAMERA_RECOVERY_TIME
        else:
            recovery_time = duration_seconds + (duration_seconds * frame_rate * CAMERA_RECOVERY_TIME)

        return recovery_time

    def _do_recover(self, recovery_time):

        # start timer here
        log.debug("Starting timer for %s seconds" % recovery_time)
        Timer(recovery_time, self._recovery_timer_expired, [self._protocol_fsm.get_current_state()]).start()

        # Transiton to the Recovery State until the timer expires
        self._async_raise_fsm_event(ProtocolEvent.START_RECOVER)

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
        result = []

        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, (next_state, result)

    def _handler_direct_access_stop_direct(self):
        """
        @reval next_state, (next_state, result)
        """
        next_state = self._discover()
        result = []

        return next_state, (next_state, result)

    ################################################################################
    # Autosample state handlers
    ################################################################################
    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        self._init_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """

    def _handler_autosample_acquire_sample(self, *args, **kwargs):
        """
        Acquire Sample
        """
        next_state = ProtocolState.RECOVERY
        result = []

        kwargs['timeout'] = 30

        # First, go to the user defined preset position
        self._handler_command_goto_preset()

        # Before taking a snapshot, update parameters
        self._update_metadata_params()

        log.debug("Acquire Sample: about to take a snapshot")

        self._do_cmd_resp(InstrumentCommands.TAKE_SNAPSHOT, *args, **kwargs)

        log.debug("Acquire Sample: Captured snapshot!")

        # Camera needs time to recover after taking a snapshot
        self._do_recover(CAMERA_RECOVERY_TIME)

        return next_state, (next_state, result)

    def _handler_autosample_start_capture(self, *args, **kwargs):
        """
        Start Auto Capture
        """
        # First, go to the user defined preset position
        self._handler_command_goto_preset()

        self._handler_command_start_capture(*args, **kwargs)

    def _handler_autosample_stop_capture(self, *args, **kwargs):
        """
        Stop Auto capture
        """
        self._handler_command_stop_capture(*args, **kwargs)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return  next_state, (next_state, result) if successful.
        incorrect prompt received.
        """
        next_state = ProtocolState.COMMAND
        result = []

        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        return next_state, (next_state, result)

    def _handler_recovery_enter(self, *args, **kwargs):
        """
        Enter the Recovery state
        """
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_recovery_exit(self, *args, **kwargs):
        """
        Exit recovery state.
        """

    def _handler_recovery_complete(self, *args, **kwargs):
        """
        Protocol method to transition back to the previous state once recovery is complete
        """
        next_state = args[0]
        result = []

        log.debug("Recovery complete, returning to %s" % next_state)

        # Recovery complete, return back to the previous state
        if next_state == ProtocolState.AUTOSAMPLE:
            # return camera back to default position
            log.debug("time to return camera to default position")
            self._do_cmd_resp(InstrumentCommands.GO_TO_PRESET, DEFAULT_PRESET_POSITION, **kwargs)
            next_agent_state = ResourceAgentState.STREAMING
        else:
            next_agent_state = ResourceAgentState.COMMAND

        self._async_agent_state_change(next_agent_state)

        return next_state, (next_state, result)

    def _recovery_timer_expired(self, *args, **kwargs):
        """
        Callback method for when the timer for the recovery state expires
        """
        next_state = args[0]

        self._async_raise_fsm_event(ProtocolEvent.RECOVER_COMPLETE, next_state)

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
            if (params[Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]] != self._param_dict.get(
                    Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY])):
                self._param_dict.set_value(Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY],
                                           params[Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]])
                if params[Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]] == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job(ScheduledJob.SAMPLE)
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
                if self.video_forwarding_flag is True:
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

                if self.video_forwarding_flag is True:
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

    def add_build_command_handlers(self):
        """
        Add build handlers for device commands.
        """

        self._add_build_handler(InstrumentCommands.SET, self._build_set_command)
        self._add_build_handler(InstrumentCommands.GET, self._build_get_command)

        self._add_build_handler(InstrumentCommands.START_CAPTURE, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.STOP_CAPTURE, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.TAKE_SNAPSHOT, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.START_FOCUS_NEAR, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.START_FOCUS_FAR, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.STOP_FOCUS, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.START_ZOOM_OUT, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.START_ZOOM_IN, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.STOP_ZOOM, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.INCREASE_IRIS, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.DECREASE_IRIS, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.GO_TO_PRESET, self.build_preset_command)
        self._add_build_handler(InstrumentCommands.SET_PRESET, self.build_preset_command)

        self._add_build_handler(InstrumentCommands.START_PAN_LEFT, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.START_PAN_RIGHT, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.STOP_PAN, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.START_TILT_UP, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.START_TILT_DOWN, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.STOP_TILT, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.TILE_UP_SOFT, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.TILE_DOWN_SOFT, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.PAN_LEFT_SOFT, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.PAN_RIGHT_SOFT, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.LAMP_ON, self.build_simple_command)
        self._add_build_handler(InstrumentCommands.LAMP_OFF, self.build_simple_command)

        self._add_build_handler(InstrumentCommands.LASERS_ON, self.build_laser_command)
        self._add_build_handler(InstrumentCommands.LASERS_OFF, self.build_laser_command)

        self._add_build_handler(InstrumentCommands.GET_DISK_USAGE, self.build_status_command)
        self._add_build_handler(InstrumentCommands.HEALTH_REQUEST, self.build_status_command)

    def add_response_handlers(self):
        """
        Add response_handlers
        """
        # add response_handlers
        self._add_response_handler(InstrumentCommands.SET, self._parse_set_response)
        self._add_response_handler(InstrumentCommands.GET, self._parse_get_response)

        self._add_response_handler(InstrumentCommands.SET_PRESET, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.GO_TO_PRESET, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.LAMP_OFF, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.LAMP_ON, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.LASERS_OFF, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.LASERS_ON, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.PAN_LEFT_SOFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.PAN_RIGHT_SOFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.DECREASE_IRIS, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_CAPTURE, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_FOCUS_FAR, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_FOCUS_NEAR, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_PAN_RIGHT, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_PAN_LEFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_TILT_DOWN, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_TILT_UP, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.TILE_UP_SOFT, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.TILE_DOWN_SOFT, self._parse_simple_response)

        # Generate data particle
        self._add_response_handler(InstrumentCommands.GET_DISK_USAGE, self._parse_simple_response)

        # Generate data particle
        self._add_response_handler(InstrumentCommands.HEALTH_REQUEST, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_ZOOM_IN, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.START_ZOOM_OUT, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.INCREASE_IRIS, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.TAKE_SNAPSHOT, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.STOP_ZOOM, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.STOP_CAPTURE, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.STOP_FOCUS, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.STOP_PAN, self._parse_simple_response)
        self._add_response_handler(InstrumentCommands.STOP_TILT, self._parse_simple_response)

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """

        if (self._extract_sample(CamdsDiskStatus,
                                 CAMDS_DISK_STATUS_MATCHER_COM,
                                 chunk,
                                 timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_DISK_STATUS")

        elif (self._extract_sample(CamdsHealthStatus,
                                   CAMDS_HEALTH_STATUS_MATCHER_COM,
                                   chunk,
                                   timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_HEALTH_STATUS")

        elif (self._extract_metadata_sample(CamdsImageMetadata,
                                            CAMDS_IMAGE_FILE_MATCHER_COM,
                                            chunk,
                                            timestamp)):
            log.debug("_got_chunk - successful match for CAMDS_IMAGE_METADATA")

    def _extract_metadata_sample(self, particle_class, regex, line, timestamp, publish=True):
        """
        Special case for extract_sample - camds_image_metadata particle
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
        """

        match = regex.match(line)
        if match:
            # special case for the CAMDS image metadata particle - need to pass in param_dict
            particle = None
            if regex == CAMDS_IMAGE_FILE_MATCHER_COM:
                # this is the image filename handed over by the port agent
                img_filename = match.group(1)
                particle = particle_class(self._param_dict, img_filename, port_timestamp=timestamp)

            if particle is None:
                return

            parsed_sample = particle.generate()

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample

    def _wakeup(self, timeout, delay=1):
        """
        This driver does not require a wakeup
        """
