"""
@package mi.instrument.subc_control.onecam.ooicore.driver
@file marine-integrations/mi/instrument/subc_control/onecam/ooicore/driver.py
@author Tapana Gupta
@brief Driver for the CAMHD instrument
"""

import re
import json
import time
import subprocess

from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.protocol_param_dict import ParameterDictType

from mi.core.log import get_logger
from mi.core.common import BaseEnum, Units
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentDataException
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker
from mi.core.driver_scheduler import DriverSchedulerConfigKey
from mi.core.driver_scheduler import TriggerType

__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'

log = get_logger()

# newline.
NEWLINE = '\n'

# default timeout.
TIMEOUT = 10

ZERO_TIME_INTERVAL = '00:00:00'

# The IP Address of the Decode Computer
DEFAULT_ENDPOINT = '128.95.97.233'

# Regexes for command responses
CAMHD_RESPONSE_REGEX = r'([A-Z]+)(.*)' + NEWLINE
CAMHD_RESPONSE_PATTERN = re.compile(CAMHD_RESPONSE_REGEX, re.DOTALL)

START_RESPONSE_REGEX = r'(STARTED|ERROR)(.*)' + NEWLINE
START_RESPONSE_PATTERN = re.compile(START_RESPONSE_REGEX, re.DOTALL)

STOP_RESPONSE_REGEX = r'(STOPPED|ERROR)(.*)' + NEWLINE
STOP_RESPONSE_PATTERN = re.compile(STOP_RESPONSE_REGEX, re.DOTALL)

LOOKAT_GET_RESPONSE_REGEX = r'(LOOKINGAT|ERROR)(.*)' + NEWLINE
LOOKAT_GET_RESPONSE_PATTERN = re.compile(LOOKAT_GET_RESPONSE_REGEX, re.DOTALL)

LOOKAT_SET_RESPONSE_REGEX = r'(STOPPED.*heading|ERROR)(.*)' + NEWLINE
LOOKAT_SET_RESPONSE_PATTERN = re.compile(LOOKAT_SET_RESPONSE_REGEX, re.DOTALL)

LIGHTS_RESPONSE_REGEX = r'(LIGHTS|ERROR)(.*)' + NEWLINE
LIGHTS_RESPONSE_PATTERN = re.compile(LIGHTS_RESPONSE_REGEX, re.DOTALL)

CAMERA_RESPONSE_REGEX = r'(CAMERA|ERROR)(.*)' + NEWLINE
CAMERA_RESPONSE_PATTERN = re.compile(CAMERA_RESPONSE_REGEX, re.DOTALL)

ADREAD_RESPONSE_REGEX = r'(ADVAL|ERROR)(.*)' + NEWLINE
ADREAD_RESPONSE_PATTERN = re.compile(ADREAD_RESPONSE_REGEX, re.DOTALL)

# data particle matchers

# lookat, lights, camera always get executed sequentially.
# This Regex will save us from generating too many particles. Since 'CAMERA' is the last command to get
# executed, all other params will have been updated before. Hence it's the perfect time to capture
# param values and generate the metadata particle.
CAMHD_METADATA_REGEX = r'(CAMERA)(.*)' + NEWLINE
CAMHD_METADATA_MATCHER = re.compile(CAMHD_METADATA_REGEX, re.DOTALL)

CAMHD_STATUS_REGEX = r'(ADVAL)(.*)' + NEWLINE
CAMHD_STATUS_MATCHER = re.compile(CAMHD_STATUS_REGEX, re.DOTALL)

START_ARCHIVE_COMMAND = 'curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml"' \
                        ' -d "<group_id>27</group_id>" http://209.124.182.238/api/live_events/4/start_output_group'

STOP_ARCHIVE_COMMAND = 'curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml"' \
                       ' -d "<group_id>27</group_id>" http://209.124.182.238/api/live_events/4/stop_output_group'

ADREAD_DATA_POSITION = 2


class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver
    """
    RAW = CommonDataParticleType.RAW
    STREAMING_STATUS = 'camhd_streaming_status'
    ADREAD_STATUS = 'camhd_adread_status'


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
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
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE

    GET_STATUS_STREAMING = 'DRIVER_EVENT_GET_STATUS_STREAMING'
    START_STREAMING = 'DRIVER_EVENT_START_STREAMING'
    STOP_STREAMING = 'DRIVER_EVENT_STOP_STREAMING'


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS

    GET_STATUS_STREAMING = ProtocolEvent.GET_STATUS_STREAMING
    START_STREAMING = ProtocolEvent.START_STREAMING
    STOP_STREAMING = ProtocolEvent.STOP_STREAMING


class Command(BaseEnum):
    """
    CAMHD Instrument command strings
    """
    GET = 'get'
    SET = 'set'
    START = 'START'
    STOP = 'STOP'
    LOOKAT = 'LOOKAT'
    LIGHTS = 'LIGHTS'
    CAMERA = 'CAMERA'
    ADREAD = 'ADREAD'


class Parameter(DriverParameter):
    """
    Device specific parameters for CAMHD.

    """
    ENDPOINT = 'Endpoint'
    PAN_POSITION = 'Pan_Position'
    TILT_POSITION = 'Tilt_Position'
    PAN_TILT_SPEED = 'Pan_Tilt_Speed'
    HEADING = 'Heading'
    PITCH = 'Pitch'
    LIGHT_1_LEVEL = 'Light_1_Level'
    LIGHT_2_LEVEL = 'Light_2_Level'
    ZOOM_LEVEL = 'Zoom_Level'
    LASERS_STATE = 'Lasers_State'
    SAMPLE_INTERVAL = 'Sample_Interval'
    STATUS_INTERVAL = 'Acquire_Status_Interval'
    AUTO_CAPTURE_DURATION = 'Auto_Capture_Duration'


class ParameterUnit(BaseEnum):
    TIME_INTERVAL = 'HH:MM:SS'


class ScheduledJob(BaseEnum):
    """
    Scheduled Jobs for CAMHD
    """
    SAMPLE = 'sample'
    STOP_CAPTURE = "stop capturing"
    ACQUIRE_STATUS = "acquire_status"


class Prompt(BaseEnum):
    """
    Device i/o prompts..
    """
    # No prompts here. Stays empty.


###############################################################################
# Data Particles
###############################################################################

class CAMHDStreamingStatusParticleKey(BaseEnum):
    PAN_POSITION = 'camhd_pan_position'
    TILT_POSITION = 'camhd_tilt_position'
    HEADING = 'camhd_heading'
    PITCH = 'camhd_pitch'
    LIGHT1_INTENSITY = 'camhd_light1_intensity'
    LIGHT2_INTENSITY = 'camhd_light2_intensity'
    ZOOM = 'camhd_zoom'
    LASER = 'camhd_laser'


class CAMHDStreamingStatusParticle(DataParticle):
    """
    CAMHD Streaming Status particle
    """
    _data_particle_type = DataParticleType.STREAMING_STATUS

    def _build_parsed_values(self):
        # Initialize

        result = []

        param_dict = self.raw_data.get_all()
        log.debug("Streaming status - Param Dict: %s" % param_dict)

        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.PAN_POSITION,
                       DataParticleKey.VALUE: param_dict.get(Parameter.PAN_POSITION)})
        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.TILT_POSITION,
                       DataParticleKey.VALUE: param_dict.get(Parameter.TILT_POSITION)})
        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.HEADING,
                       DataParticleKey.VALUE: param_dict.get(Parameter.HEADING)})
        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.PITCH,
                       DataParticleKey.VALUE: param_dict.get(Parameter.PITCH)})
        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.LIGHT1_INTENSITY,
                       DataParticleKey.VALUE: param_dict.get(Parameter.LIGHT_1_LEVEL)})
        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.LIGHT2_INTENSITY,
                       DataParticleKey.VALUE: param_dict.get(Parameter.LIGHT_2_LEVEL)})
        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.ZOOM,
                       DataParticleKey.VALUE: param_dict.get(Parameter.ZOOM_LEVEL)})
        result.append({DataParticleKey.VALUE_ID: CAMHDStreamingStatusParticleKey.LASER,
                       DataParticleKey.VALUE: param_dict.get(Parameter.LASERS_STATE)})

        log.debug("STREAMING_STATUS: Finished building particle: %s" % result)

        return result


class CAMHDAdreadStatusParticleKey(BaseEnum):
    CHANNEL_NAME = 'camhd_channel_name'
    CHANNEL_VALUE = 'camhd_channel_value'
    VALUE_UNITS = 'camhd_value_units'


class CAMHDSAdreadStatusParticle(DataParticle):
    """
    CAMHD ADREAD Status particle
    """
    _data_particle_type = DataParticleType.ADREAD_STATUS

    def _build_parsed_values(self):

        result = []

        log.debug("Raw Data: %s" % self.raw_data)

        match = CAMHD_STATUS_MATCHER.match(self.raw_data)

        if not match:
            raise InstrumentDataException("Incorrectly formatted data received in response to "
                                          "ADREAD. Reply from instrument: %s" % self.raw_data)

        try:
            values_dict = json.loads(match.group(ADREAD_DATA_POSITION))
        except ValueError:
            raise InstrumentDataException("Data received in response to ADREAD is not a"
                                          " dictionary. Reply from instrument: %s" % self.raw_data)

        timestamp = values_dict.get('time')

        if timestamp:
            self.set_internal_timestamp(timestamp)

        # Get the list of channels
        channel_list = values_dict.get('data')

        if not channel_list:
            raise InstrumentDataException("Missing data in response to ADREAD - channel information "
                                          " missing. Reply from instrument: %s" % self.raw_data)

        log.debug("No. of ADREAD channels: %s" % len(channel_list))

        (channel_names, channel_values, value_units) = ([], [], [])

        # Each channel in the channel list is a data dictionary
        for channel in channel_list:

            # the channel dictionary must have exactly 3 keys
            if len(channel) != 3:
                log.error("Incomplete data received in ADREAD reply from instrument.")
                raise InstrumentDataException("Incomplete data received in ADREAD "
                                              "reply from instrument: %s" % self.raw_data)

            # Populate the data particle arrays
            channel_names.append(channel.get('name'))
            channel_values.append(channel.get('val'))
            value_units.append(channel.get('units'))

        result.append({DataParticleKey.VALUE_ID: CAMHDAdreadStatusParticleKey.CHANNEL_NAME,
                       DataParticleKey.VALUE: channel_names})
        result.append({DataParticleKey.VALUE_ID: CAMHDAdreadStatusParticleKey.CHANNEL_VALUE,
                       DataParticleKey.VALUE: channel_values})
        result.append({DataParticleKey.VALUE_ID: CAMHDAdreadStatusParticleKey.VALUE_UNITS,
                       DataParticleKey.VALUE: value_units})

        log.debug("ADREAD STATUS: Finished building particle: %s" % result)

        return result


###############################################################################
# Driver
###############################################################################

class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Protocol builder.
    ########################################################################

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = CAMHDProtocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################

class CAMHDProtocol(CommandResponseInstrumentProtocol):
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

        # Build protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(ProtocolState, ProtocolEvent, ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # keep track of whether the camera is streaming video
        self._streaming = False

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.EXIT, self._handler_unknown_exit)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)

        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXIT, self._handler_command_exit)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_DIRECT,
                                       self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET, self._handler_command_get)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET, self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_STREAMING,
                                       self._handler_command_start_streaming)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.STOP_STREAMING,
                                       self._handler_command_stop_streaming)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET_STATUS_STREAMING,
                                       self._handler_command_get_status_streaming)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_AUTOSAMPLE,
                                       self._handler_command_start_autosample)

        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ENTER,
                                       self._handler_autosample_enter)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXIT,
                                       self._handler_autosample_exit)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE,
                                       self._handler_autosample_stop_autosample)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GET,
                                       self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ACQUIRE_SAMPLE,
                                       self._handler_autosample_acquire_sample)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_STREAMING,
                                       self._handler_autosample_stop_streaming)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GET_STATUS_STREAMING,
                                       self._handler_command_get_status_streaming)

        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.ENTER,
                                       self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXIT,
                                       self._handler_direct_access_exit)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS,
                                       ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS,
                                       ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_driver_dict()
        self._build_command_dict()
        self._build_param_dict()

        self.initialize_scheduler()

        # Add build handlers for device commands.
        self._add_build_handler(Command.SET, self._build_set_command)
        self._add_build_handler(Command.START, self._build_start_command)
        self._add_build_handler(Command.STOP, self._build_simple_command)
        self._add_build_handler(Command.LOOKAT, self._build_simple_command)
        self._add_build_handler(Command.LIGHTS, self._build_simple_command)
        self._add_build_handler(Command.CAMERA, self._build_simple_command)
        self._add_build_handler(Command.ADREAD, self._build_simple_command)

        # Add response handlers for device commands.
        self._add_response_handler(Command.SET, self._parse_set_response)
        self._add_response_handler(Command.START, self._parse_start_response)
        self._add_response_handler(Command.STOP, self._parse_stop_response)
        self._add_response_handler(Command.LOOKAT, self._parse_get_response)
        self._add_response_handler(Command.LIGHTS, self._parse_get_response)
        self._add_response_handler(Command.CAMERA, self._parse_get_response)
        self._add_response_handler(Command.ADREAD, self._parse_adread_response)

        # Set state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # commands sent sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

        self._chunker = StringChunker(self.sieve_function)

    # We override this as the instrument has no prompts
    def _wakeup(self, timeout, delay=1):
        pass

    @staticmethod
    def sieve_function(raw_data):
        """
        Chunker sieve method to help the chunker identify chunks.
        :param raw_data: raw instrument data
        @returns a list of chunks identified, if any.
        The chunks are all the same type.
        """

        sieve_matchers = [CAMHD_METADATA_MATCHER,
                          CAMHD_STATUS_MATCHER]

        return_list = []
        log.debug('Sieve function raw data %r' % raw_data)
        for matcher in sieve_matchers:

            for match in matcher.finditer(raw_data):
                log.debug('Sieve function match')

                return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """

        if (self._extract_sample(CAMHDSAdreadStatusParticle,
                                 CAMHD_STATUS_MATCHER,
                                 chunk,
                                 timestamp)):
            log.debug("_got_chunk - successful match for CAMHD ADREAD Status")

        elif self._extract_metadata_sample(CAMHDStreamingStatusParticle,
                                           CAMHD_METADATA_MATCHER,
                                           chunk,
                                           timestamp):
            log.debug("_got_chunk - successful match for CAMHD Streaming Status")

    def _extract_metadata_sample(self, particle_class, regex, line, timestamp, publish=True):
        """
        Special case for extract_sample - camhd_streaming_status particle
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

        if regex.match(line):

            # Update the param dict with values obtained from the instrument
            self._param_dict.update(line)

            # special case for the CAMHD streaming status particle - need to pass in param_dict
            particle = particle_class(self._param_dict, port_timestamp=timestamp)

            parsed_sample = particle.generate()

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample

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

        self._cmd_dict.add(Capability.GET_STATUS_STREAMING, display_name="Get Status_Streaming")
        self._cmd_dict.add(Capability.START_STREAMING, display_name="Start Streaming")
        self._cmd_dict.add(Capability.STOP_STREAMING, display_name="Stop Streaming")

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match stirng, match lambda function,
        and value formatting function for set commands.
        """

        # TODO: leave this regex here for now - we've only tested against a simulator
        # the real instrument might give us floats, then we'll need this
        # FLOAT_REGEX = r'((?:[+-]?[0-9]|[1-9][0-9])+\.[0-9]+)'

        int_regex = r'([+-]?[0-9]+)'

        # Add parameter handlers to parameter dict.
        self._param_dict.add(Parameter.ENDPOINT,
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Endpoint",
                             description='IP address of the system running the UltraGrid receiver process.',
                             startup_param=False,
                             direct_access=False,
                             default_value=DEFAULT_ENDPOINT,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.PAN_POSITION,
                             r'"pan": ' + int_regex,
                             lambda match: float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             display_name="Pan",
                             range=(45, 315),
                             description='Camera pan position: (45 - 315)',
                             startup_param=False,
                             direct_access=False,
                             default_value=180.0,
                             units=Units.DEGREE_PLANE_ANGLE,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.TILT_POSITION,
                             r'"tilt": ' + int_regex,
                             lambda match: float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             display_name="Tilt",
                             description='Camera tilt position: (50 - 140)',
                             range=(50, 140),
                             startup_param=False,
                             direct_access=False,
                             default_value=90.0,
                             units=Units.DEGREE_PLANE_ANGLE,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.PAN_TILT_SPEED,
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.FLOAT,
                             display_name="Speed",
                             range=(0.5, 40),
                             description='Pan-Tilt speed, in 0.5 deg/s increments: (0.5 - 40)',
                             startup_param=False,
                             direct_access=False,
                             default_value=10.0,
                             units=Units.DEGREE_PLANE_ANGLE_PER_SECOND,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.HEADING,
                             r'"heading": ' + int_regex,
                             lambda match: float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             display_name="Heading",
                             range=(0, 360),
                             description='Heading relative to magnetic North: (0 - 360)',
                             startup_param=False,
                             direct_access=False,
                             default_value=0.0,
                             units=Units.DEGREE_PLANE_ANGLE,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.PITCH,
                             r'"pitch": ' + int_regex,
                             lambda match: float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             display_name="Pitch",
                             range=(-90, 90),
                             description='Gravity referenced pitch angle. Negative values are up, '
                                         'positive values are down: (-90 - 90)',
                             startup_param=False,
                             direct_access=False,
                             default_value=0.0,
                             units=Units.DEGREE_PLANE_ANGLE,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.LIGHT_1_LEVEL,
                             r'"intensity": \[([\d]+), ([\d]+)\]',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Light 1 Level",
                             range=(0, 100),
                             description='Relative intensity of light 1: (0 - 100)',
                             startup_param=False,
                             direct_access=False,
                             default_value=50,
                             units=Units.PERCENT,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.LIGHT_2_LEVEL,
                             r'"intensity": \[([\d]+), ([\d]+)\]',
                             lambda match: int(match.group(2)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Light 2 Level",
                             range=(0, 100),
                             description='Relative intensity of light 2: (0 - 100)',
                             startup_param=False,
                             direct_access=False,
                             default_value=50,
                             units=Units.PERCENT,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.ZOOM_LEVEL,
                             r'"zoom": ' + int_regex,
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             display_name="Zoom Level",
                             range=(0, 7),
                             description='Zoom level in steps relative to the current setting: (+/- integer value)',
                             startup_param=False,
                             direct_access=False,
                             default_value=0,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.LASERS_STATE,
                             r'"laser": "(on|off)"',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Lasers State",
                             range={'On': 'on', 'Off': 'off'},
                             description='Lasers state: (on | off)',
                             startup_param=False,
                             direct_access=False,
                             default_value='off',
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.SAMPLE_INTERVAL,
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Sample Interval",
                             description='Time to wait between taking time-lapsed samples.',
                             startup_param=False,
                             direct_access=False,
                             default_value='00:30:00',
                             units=ParameterUnit.TIME_INTERVAL,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.STATUS_INTERVAL,
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Acquire Status Interval",
                             description='Driver parameter used for acquire status schedule.',
                             startup_param=False,
                             direct_access=False,
                             default_value='00:00:00',
                             units=ParameterUnit.TIME_INTERVAL,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.add(Parameter.AUTO_CAPTURE_DURATION,
                             r'NOT USED',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Auto Capture Duration",
                             description='Duration for which streaming video will be captured before '
                                         'it is stopped by the driver.',
                             startup_param=False,
                             direct_access=False,
                             default_value='00:05:00',
                             units=ParameterUnit.TIME_INTERVAL,
                             visibility=ParameterDictVisibility.READ_WRITE)

        self._param_dict.set_default(Parameter.SAMPLE_INTERVAL)
        self._param_dict.set_default(Parameter.STATUS_INTERVAL)
        self._param_dict.set_default(Parameter.ENDPOINT)
        self._param_dict.set_default(Parameter.PAN_TILT_SPEED)
        self._param_dict.set_default(Parameter.AUTO_CAPTURE_DURATION)

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary.
        """

        log.debug("Inside _update_params")

        was_streaming = True

        # Start streaming if not already started, otherwise instrument won't
        # respond to 'Get' commands
        if not self._streaming:
            was_streaming = False
            self._handler_start_streaming()

        # Get old param dict config.
        old_config = self._param_dict.get_config()

        # Send LOOKAT, LIGHTS and CAMERA with no arguments, in order to get parameter values

        log.debug("Sending LOOKAT...")

        # Get Pan, Tilt, Heading and Pitch
        self._do_cmd_resp(Command.LOOKAT, response_regex=LOOKAT_GET_RESPONSE_PATTERN)

        log.debug("Sending LIGHTS...")

        # Get Intensity for both Lights
        self._do_cmd_resp(Command.LIGHTS, response_regex=LIGHTS_RESPONSE_PATTERN)

        log.debug("Sending CAMERA...")

        # Get Zoom value and Lasers state
        self._do_cmd_resp(Command.CAMERA, response_regex=CAMERA_RESPONSE_PATTERN)

        # Return streaming back to original state
        if not was_streaming:
            self._handler_command_stop_streaming()

        new_config = self._param_dict.get_config()

        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    ########################################################################
    # Build handlers.
    ########################################################################

    def _build_camhd_command(self, cmd, args_dict):
        """
        Helper function that actually puts together the command together with
        arguments, in a format that the port agent will recognize.
        @param cmd the command to format.
        @param args_dict the argument dictionary
        @retval The command to be sent to the device.
        """

        log.debug("_build_camhd_command, for %s" % cmd)

        # JSON encode argument dictionary
        args_encoded = json.dumps(args_dict)

        # Return a JSON encoded list containing the command and argument dictionary,
        # followed by a newline.
        command = json.dumps([cmd, args_encoded]) + NEWLINE

        log.debug("Built CAMHD command: %s" % command)

        return command

    def _build_simple_command(self, cmd, *args):
        """
        Build handler for basic commands.
        @param cmd the simple command to format.
        @retval The command to be sent to the device.
        """

        log.debug("_build_simple_command, for %s" % cmd)

        return self._build_camhd_command(cmd, {})

    def _build_start_command(self, cmd):
        """
        Build handler for CAMHD start command.
        @param cmd the get command to format.
        @retval The command to be sent to the device.
        """

        log.debug("_build_start_command, for %s" % cmd)

        return self._build_camhd_command(cmd, {'endpoint': DEFAULT_ENDPOINT})

    def _build_set_command(self, cmd, param):
        """
        Build handler for CAMHD set commands: LOOKAT, LIGHTS and CAMERA
        @param cmd the set command.
        @param param the parameter to be set.
        @retval The command to be sent to the device.
        """
        cmd_args = {}

        # Setting the Pan/Tilt/Speed values involves sending a 'LOOKAT' msg to the instrument.
        # Here we construct a 'LOOKAT' message.
        if param == Parameter.PAN_POSITION or param == Parameter.TILT_POSITION or param == Parameter.PAN_TILT_SPEED:
            cmd = Command.LOOKAT

            # first get values from the dictionary
            pan_val = self._param_dict.get(Parameter.PAN_POSITION)
            tilt_val = self._param_dict.get(Parameter.TILT_POSITION)

            # speed is optional
            if param == Parameter.PAN_TILT_SPEED:
                speed_val = self._param_dict.get(Parameter.PAN_TILT_SPEED)
            else:
                speed_val = None

            cmd_args['pan'] = pan_val
            cmd_args['tilt'] = tilt_val
            if speed_val:
                cmd_args['speed'] = speed_val

        # Setting the light levels involves sending a 'LIGHTS' to the instrument.
        # Here we construct a 'LIGHTS' message.
        elif param == Parameter.LIGHT_1_LEVEL or param == Parameter.LIGHT_2_LEVEL:
            cmd = Command.LIGHTS

            # first get values from the dictionary
            light1_val = self._param_dict.get(Parameter.LIGHT_1_LEVEL)
            light2_val = self._param_dict.get(Parameter.LIGHT_2_LEVEL)

            cmd_args['intensity'] = [light1_val, light2_val]

        # Setting the Zoom level/Laser state involves sending a 'CAMERA' msg to the instrument.
        # Here we construct a 'CAMERA' message.
        elif param == Parameter.ZOOM_LEVEL or param == Parameter.LASERS_STATE:
            cmd = Command.CAMERA

            # first get values from the dictionary
            zoom_val = self._param_dict.get(Parameter.ZOOM_LEVEL)
            lasers_val = self._param_dict.get(Parameter.LASERS_STATE)

            cmd_args['zoom'] = zoom_val
            cmd_args['laser'] = lasers_val

        return self._build_camhd_command(cmd, cmd_args)

    ########################################################################
    # Response handlers.
    ########################################################################

    def _parse_start_response(self, response, prompt):
        """
        Response handler for start command.
        @param response command response string.
        @param prompt prompt following command response.
        """

        log.debug("START RESPONSE = %s" % response)

        if response.startswith("ERROR"):
            log.error("Instrument returned error in response to START Command: %s" % response)
            raise InstrumentProtocolException(
                'Protocol._parse_start_response: Instrument returned: ' + response)

        self._streaming = True

        return response

    def _parse_stop_response(self, response, prompt):
        """
        Response handler for stop command.
        @param response command response string.
        @param prompt prompt following command response.
        """

        log.debug("STOP RESPONSE = %s" % response)

        if response.startswith("ERROR"):
            log.error("Instrument returned error in response to STOP Command: %s" % response)

        self._streaming = False

        return response

    def _parse_adread_response(self, response, prompt):
        """
        Response handler for adread command.
        @param response command response string.
        @param prompt prompt following command response.
        """

        log.debug("ADREAD RESPONSE = %s" % response)

        if response.startswith("ERROR"):
            log.error("Instrument returned error in response to ADREAD Command: %s" % response)
            raise InstrumentProtocolException(
                'Protocol._parse_adread_response: Instrument returned: ' + response)

        return response

    def _parse_get_response(self, response, prompt):
        """
        Response handler for get commands.
        @param response command response string.
        @param prompt prompt following command response.
        """

        log.debug("GET RESPONSE = %s" % response)

        # Use the built-in update method to allow extraction of parameter values
        # using the pre-defined regex for that parameter.
        if response.startswith("ERROR"):
            log.error("Instrument returned error in response to GET Command: %s" % response)
            raise InstrumentProtocolException(
                'Protocol._parse_get_response: Instrument returned: ' + response)

        # Update the param dict with values obtained from the instrument
        self._param_dict.update(response)

        return response

    def _parse_set_response(self, response, prompt):
        """
        Parse handler for set commands.
        @param response command response string.
        @param prompt prompt following command response.
        """

        log.debug("SET RESPONSE = %s" % response)

        if response.startswith("ERROR"):
            log.error("Instrument returned error in response to SET Command: %s" % response)
            raise InstrumentProtocolException(
                'Protocol._parse_set_response: Instrument returned: ' + response)

        return response

    def _discover(self):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE or UNKNOWN.
        @return (next_protocol_state, next_protocol_state)
        """
        next_state = ProtocolState.COMMAND

        if self._scheduler_callback is not None:
            if self._scheduler_callback.get(ScheduledJob.SAMPLE):
                next_state = ProtocolState.AUTOSAMPLE

        return next_state

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

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE.
        """
        next_state = self._discover()
        result = []
        return next_state, (next_state, result)

    ########################################################################
    # Command handlers.
    ########################################################################

    def start_scheduled_job(self, param, schedule_job, protocol_event):
        """
        Add a scheduled job
        :param param: if set to 'Auto_Capture_Duration', use the configured duration
        :param schedule_job: currently scheduled job identifier
        :param protocol_event: protocol event to use for new job
        """
        self.stop_scheduled_job(schedule_job)

        (hours, minutes, seconds) = (int(val) for val in self._param_dict.get(param).split(':'))

        # Video Capture Duration must be less than Autosample Interval
        if param == Parameter.AUTO_CAPTURE_DURATION:
            capture_interval = self.get_interval_seconds(param)
            sample_interval = self.get_interval_seconds(Parameter.SAMPLE_INTERVAL)

            if capture_interval >= sample_interval:
                log.error("Video Capture Duration must be less than Autosample Interval. Not performing capture.")
                raise InstrumentParameterException('Video Capture Duration must be less than Autosample Interval.')

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

    def get_interval_seconds(self, param):
        """
        Helper to get Interval in seconds from a string time value
        :param param: interval in HH:MM:SS format
        """
        (hours, minutes, seconds) = (int(val) for val in self._param_dict.get(param).split(':'))
        return hours * 3600 + minutes * 60 + seconds

    def stop_scheduled_job(self, schedule_job):
        """
        Remove the scheduled job
        :param schedule_job: scheduling job
        """
        log.debug("Attempting to remove the scheduler")
        if self._scheduler is not None:
            try:
                self._remove_scheduler(schedule_job)
                log.debug("successfully removed scheduler")
            except KeyError:
                log.debug("_remove_scheduler could not find %s", schedule_job)

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """

        # Update parameters - get values from Instrument
        # No startup parameters to apply here, simply get Instrument values
        # stop streaming, just to make sure we get a fresh start
        self._handler_command_stop_streaming()

        self._update_params()

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        # start scheduled event for get_status only if the interval is not "00:00:00
        status_interval = self._param_dict.get(Parameter.STATUS_INTERVAL)

        if status_interval != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(Parameter.STATUS_INTERVAL,
                                     ScheduledJob.ACQUIRE_STATUS,
                                     ProtocolEvent.ACQUIRE_STATUS)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """
        self.stop_scheduled_job(ScheduledJob.ACQUIRE_STATUS)

    def _handler_command_get(self, *args, **kwargs):
        """
        Get parameters while in the command state.
        Call _handler_get in the base class, which calls _update_params.
        @param args[0] list of parameters to retrieve, or DriverParameter.ALL.
        @retval returns (next_state, result) where result is a dict {}.
        """
        next_state, result = self._handler_get(*args, **kwargs)
        # TODO - match return signature for other handlers - return next_state (next_state, result)
        return next_state, result

    def _handler_command_set(self, *args, **kwargs):

        next_state = None
        changed = False

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')

        # Handle engineering parameters
        if Parameter.SAMPLE_INTERVAL in params:
            if params[Parameter.SAMPLE_INTERVAL] != self._param_dict.get(Parameter.SAMPLE_INTERVAL):
                self._param_dict.set_value(Parameter.SAMPLE_INTERVAL, params[Parameter.SAMPLE_INTERVAL])
                if params[Parameter.SAMPLE_INTERVAL] == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job(ScheduledJob.SAMPLE)
                changed = True

        if Parameter.STATUS_INTERVAL in params:
            if params[Parameter.STATUS_INTERVAL] != self._param_dict.get(Parameter.STATUS_INTERVAL):
                self._param_dict.set_value(Parameter.STATUS_INTERVAL, params[Parameter.STATUS_INTERVAL])
                if params[Parameter.STATUS_INTERVAL] == ZERO_TIME_INTERVAL:
                    self.stop_scheduled_job(ScheduledJob.ACQUIRE_STATUS)
                changed = True

        if Parameter.AUTO_CAPTURE_DURATION in params:
            if params[Parameter.AUTO_CAPTURE_DURATION] != self._param_dict.get(Parameter.AUTO_CAPTURE_DURATION):
                self._param_dict.set_value(Parameter.AUTO_CAPTURE_DURATION, params[Parameter.AUTO_CAPTURE_DURATION])
                changed = True

        if changed:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        result = self._set_params(params)

        return next_state, result

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        result = None
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)

        (lookat, lights, camera) = (False, False, False)

        filtered_params = dict(params)

        # first pass - update the param dict - this is needed, for example:
        # If the user is updating both pan and tilt, the 'lookat' message will have a stale
        # value for either pan or tilt if we don't update the param_dict right now.
        # We use param dict values to construct the lookat message (and others).
        # Also, we need to make sure we don't send the lookat command twice in the above example.
        # Here we remove either pan/tilt from the list of params to be updated. This makes sure
        # we send the lookat only once, but with the user provided values for both pan and tilt.
        for key, val in params.iteritems():

            # These are driver specific parameters. They are not set on the instrument.
            if key in [Parameter.SAMPLE_INTERVAL, Parameter.STATUS_INTERVAL, Parameter.AUTO_CAPTURE_DURATION]:
                filtered_params.pop(key)
            else:

                # First, start streaming, if we aren't already. We need to be streaming
                # to set Instrument Parameters
                if not self._streaming:
                    self._handler_start_streaming(**kwargs)

                # First perform range check for parameters
                if key == Parameter.PAN_POSITION:
                    if not isinstance(val, int) or val < 45 or val > 315:
                        raise InstrumentParameterException('The desired value for %s must be an integer'
                                                           ' between 45 and 315: %s' % (key, val))
                elif key == Parameter.TILT_POSITION:
                    if not isinstance(val, int) or val < 50 or val > 140:
                        raise InstrumentParameterException('The desired value for %s must be an integer'
                                                           ' between 50 and 140: %s' % (key, val))
                elif key == Parameter.PAN_TILT_SPEED:
                    speed_val = self._param_dict.get(Parameter.PAN_TILT_SPEED)
                    if not isinstance(speed_val, float) or speed_val < 0.5 or speed_val > 40.0:
                        raise InstrumentParameterException('The desired value for %s must be a value'
                                                           ' between 0.5 and 40.0: %s' % (key, val))

                elif key == Parameter.LIGHT_1_LEVEL or key == Parameter.LIGHT_2_LEVEL:
                    if not isinstance(val, int) or val < 0 or val > 100:
                        raise InstrumentParameterException('The desired value for %s must be an integer'
                                                           ' between 0 and 100: %s' % (key, val))

                elif key == Parameter.ZOOM_LEVEL:
                    if not isinstance(val, int) or val < -6 or val > 6:
                        raise InstrumentParameterException('The desired value for %s must be an integer'
                                                           ' between -6 and 6: %s' % (key, val))
                elif key == Parameter.LASERS_STATE:
                    if val not in['on', 'off']:
                        raise InstrumentParameterException('The desired value for %s must be on/off'
                                                           ': %s' % (key, val))

                log.debug("In _set_params, update param dict: %s, %s", key, val)

                # update the param dict
                self._param_dict.set_value(key, val)

            # Remove "same-command" parameters from the filtered list
            if key in [Parameter.PAN_POSITION, Parameter.TILT_POSITION, Parameter.PAN_TILT_SPEED]:
                if lookat:
                    filtered_params.pop(key)
                lookat = True
            elif key in [Parameter.LIGHT_1_LEVEL, Parameter.LIGHT_2_LEVEL]:
                if lights:
                    filtered_params.pop(key)
                lights = True
            elif key in [Parameter.ZOOM_LEVEL, Parameter.LASERS_STATE]:
                if camera:
                    filtered_params.pop(key)
                camera = True

        resp_regex = CAMHD_RESPONSE_PATTERN

        # second pass: now build individual 'set' commands for each param
        for key, val in filtered_params.iteritems():
            log.debug("In _set_params, setting %s to %s", key, val)

            if key in [Parameter.PAN_POSITION, Parameter.TILT_POSITION, Parameter.PAN_TILT_SPEED]:
                resp_regex = LOOKAT_SET_RESPONSE_PATTERN
            elif key in [Parameter.LIGHT_1_LEVEL, Parameter.LIGHT_2_LEVEL]:
                resp_regex = LIGHTS_RESPONSE_PATTERN
            elif key in [Parameter.ZOOM_LEVEL, Parameter.LASERS_STATE]:
                resp_regex = CAMERA_RESPONSE_PATTERN

            result = self._do_cmd_resp(Command.SET, key, response_regex=resp_regex)

        # Set complete, now update params
        self._update_params()

        return result

    def _handler_command_acquire_status(self):
        """
        Acquire status
        """
        timeout = time.time() + TIMEOUT

        next_state = None

        resp = self._do_cmd_resp(Command.ADREAD, response_regex=ADREAD_RESPONSE_PATTERN)

        if resp.startswith('ERROR'):
            log.error("Unable to Acquire Status. In response to ADREAD command, Instrument returned: %s" % resp)

        particles = self.wait_for_particles([DataParticleType.ADREAD_STATUS], timeout)

        return next_state, (next_state, particles)

    def _handler_command_start_streaming(self, **kwargs):
        """
        Start streaming video in command mode.
        """
        next_state = None
        result = []

        self._handler_start_streaming(**kwargs)

        return next_state, (next_state, result)

    def _handler_start_streaming(self, **kwargs):
        """
        Start streaming video.
        """
        kwargs['timeout'] = 30
        kwargs['response_regex'] = START_RESPONSE_PATTERN

        # First, start the archive recording on elemental
        subprocess.call(START_ARCHIVE_COMMAND, shell=True)

        # Next, send command to instrument to start streaming
        resp = self._do_cmd_resp(Command.START, **kwargs)

        if resp.startswith('ERROR'):
            log.error("Unable to Start Streaming. In response to START command, Instrument returned: %s" % resp)

    def _handler_command_stop_streaming(self):
        """
        Stop streaming video.
        """
        next_state = None

        # Send command to instrument to stop streaming
        resp = self._do_cmd_resp(Command.STOP, response_regex=STOP_RESPONSE_PATTERN)

        # Next, stop the archive recording on elemental
        subprocess.call(STOP_ARCHIVE_COMMAND, shell=True)

        if resp.startswith('ERROR'):
            log.warn("Unable to Stop Streaming. In response to STOP command, Instrument returned: %s" % resp)

        return next_state, (next_state, [resp])

    def _handler_command_get_status_streaming(self):
        """
        Get video streaming status.
        """

        if not self._streaming:
            log.error("Unable to get Streaming Status: Video streaming must be turned on first.")
            raise InstrumentProtocolException("Unable to get Streaming Status:"
                                              " Video streaming must be turned on first.")

        # Call update params to send LOOKAT, LIGHTS, CAMERA to instrument.
        # Responses to the above commands trigger publication of the camhd_streaming_status
        # particle, which is the desired output of this event.
        self._update_params()

    def _handler_command_start_direct(self, *args, **kwargs):
        """
        Enter direct access mode.
        """
        next_state = ProtocolState.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        """
        next_state = ProtocolState.AUTOSAMPLE
        result = []

        # first stop scheduled sampling
        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        # if we are streaming, stop streaming
        if self._streaming:
            self._handler_command_stop_streaming()

        # Schedule an event to capture streaming video for the capture duration, at the sample interval
        self.start_scheduled_job(Parameter.SAMPLE_INTERVAL, ScheduledJob.SAMPLE, ProtocolEvent.ACQUIRE_SAMPLE)

        return next_state, (next_state, result)

    ########################################################################
    # Autosample handlers.
    ########################################################################

    def _handler_autosample_enter(self):
        """
        Enter autosample state.
        """

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self):
        """
        Exit autosample state.
        """

    def _handler_autosample_acquire_sample(self, *args, **kwargs):
        """
        Acquire Sample
        """
        next_state = None

        # Schedule event to execute STOP when the capture time expires
        capturing_duration = self._param_dict.get(Parameter.AUTO_CAPTURE_DURATION)

        if capturing_duration != ZERO_TIME_INTERVAL:
            self.start_scheduled_job(Parameter.AUTO_CAPTURE_DURATION,
                                     ScheduledJob.STOP_CAPTURE,
                                     ProtocolEvent.STOP_STREAMING)
        else:
            log.error("Capturing Duration set to 0: Not Performing Capture.")

        # First, start streaming
        self._handler_start_streaming(**kwargs)

        # Set pan/tilt, lights, camera to user defined position

        # This sends the 'LOOKAT' command to the instrument with user set values for Pan/Tilt
        # populated from the param dict
        self._do_cmd_resp(Command.SET, Parameter.PAN_POSITION, response_regex=LOOKAT_SET_RESPONSE_PATTERN)

        # This sends the 'LIGHTS' command to the instrument with user set values for Light Intensity
        # populated from the param dict
        self._do_cmd_resp(Command.SET, Parameter.LIGHT_1_LEVEL, response_regex=LIGHTS_RESPONSE_PATTERN)

        # This sends the 'CAMERA' command to the instrument with user set values for Zoom/Lasers state
        # populated from the param dict
        resp = self._do_cmd_resp(Command.SET, Parameter.ZOOM_LEVEL, response_regex=CAMERA_RESPONSE_PATTERN)

        return next_state, (next_state, [resp])

    def _handler_autosample_stop_streaming(self):
        """
        Stop streaming video.
        """
        # Remove the job that was scheduled to stop streaming
        self._remove_scheduler(ScheduledJob.STOP_CAPTURE)

        next_state = None

        # Send command to instrument to stop streaming
        resp = self._do_cmd_resp(Command.STOP, response_regex=STOP_RESPONSE_PATTERN)

        # Next, stop the archive recording on elemental
        subprocess.call(STOP_ARCHIVE_COMMAND, shell=True)

        if resp.startswith('ERROR'):
            log.warn("Unable to Stop Streaming. In response to STOP command, Instrument returned: %s" % resp)

        return next_state, (next_state, [resp])

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return  next_state, (next_state, result) if successful.
        incorrect prompt received.
        """
        next_state = ProtocolState.COMMAND
        result = []

        # If currently streaming, send STOP to instrument
        if self._streaming:
            self._handler_command_stop_streaming()

        self.stop_scheduled_job(ScheduledJob.SAMPLE)

        return next_state, (next_state, result)

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

    def _handler_direct_access_execute_direct(self, data):
        """
        Execute Direct Access
        """
        next_state = None
        result = []

        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, (next_state, result)

    def _handler_direct_access_stop_direct(self):
        """
        Stop Direct Access
        """
        next_state = self._discover()
        result = []

        return next_state, (next_state, result)