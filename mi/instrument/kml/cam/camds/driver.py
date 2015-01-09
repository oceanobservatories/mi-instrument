"""
@package mi.instrument.KML.CAMDS.driver
@file marine-integrations/mi/instrument/KLM/CAMDS/driver.py
@author Sung Ahn
@brief Driver for the CAMDS

"""

import time
import re
from mi.instrument.kml.driver import KMLScheduledJob, ParameterIndex
from mi.instrument.kml.driver import KMLCapability
from mi.instrument.kml.driver import KMLInstrumentCmds
from mi.instrument.kml.driver import KMLProtocolState
from mi.instrument.kml.driver import KMLPrompt
from mi.instrument.kml.driver import KMLProtocol
from mi.instrument.kml.driver import KMLInstrumentDriver
from mi.instrument.kml.driver import KMLParameter
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.common import BaseEnum

from mi.core.instrument.port_agent_client import PortAgentClient
from mi.instrument.kml.particles import CAMDS_IMAGE_METADATA, \
    CAMDS_SNAPSHOT_MATCHER_COM, CAMDS_STOP_CAPTURING_COM,\
    CAMDS_START_CAPTURING_COM

from mi.core.log import get_logger

log = get_logger()
from mi.core.log import get_logging_metaclass

from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.instrument.kml.driver import KMLProtocolEvent
from mi.core.instrument.chunker import StringChunker
from mi.instrument.kml.particles import CAMDS_HEALTH_STATUS, CAMDS_DISK_STATUS, \
                                        CAMDS_HEALTH_STATUS_MATCHER_COM, CAMDS_DISK_STATUS_MATCHER_COM
from mi.core.instrument.instrument_driver import ConfigMetadataKey

# default timeout.
TIMEOUT = 20

#METALOGGER = get_logging_metaclass()

# newline.
NEWLINE = '\r\n'

DEFAULT_CMD_TIMEOUT = 20
DEFAULT_WRITE_DELAY = 0

ZERO_TIME_INTERVAL = '00:00:00'
RE_PATTERN = type(re.compile(""))

DEFAULT_DICT_TIMEOUT = 30


# ##############################################################################
# Driver
# ##############################################################################

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

class CAMDSInstrumentDriver(KMLInstrumentDriver):
    """
    InstrumentDriver subclass for cam driver.
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    #__metaclass__ = METALOGGER

    def __init__(self, evt_callback):
        """
        InstrumentDriver constructor.
        @param evt_callback Driver process event callback.
        """
        # Construct superclass.
        KMLInstrumentDriver.__init__(self, evt_callback)


    # #######################################################################
    # Protocol builder.
    # #######################################################################

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = CAMDSProtocol(KMLPrompt, NEWLINE, self._driver_event)


# ##########################################################################
# Protocol
# ##########################################################################

class CAMDSProtocol(KMLProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """
    #__metaclass__ = METALOGGER

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

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """

        # Construct protocol superclass.
        KMLProtocol.__init__(self, prompts, newline, driver_event)

        self._connection = None

        self._chunker = StringChunker(self.sieve_function)


    def _build_command_dict(self):
        """
        Build command dictionary
        """
        self._cmd_dict.add(KMLCapability.START_AUTOSAMPLE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Start Autosample",
                           description="Place the instrument into autosample mode")
        self._cmd_dict.add(KMLCapability.STOP_AUTOSAMPLE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Stop Autosample",
                           description="Exit autosample mode and return to command mode")
        self._cmd_dict.add(KMLCapability.EXECUTE_AUTO_CAPTURE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Auto Capture",
                           description="Capture images for default duration")
        self._cmd_dict.add(KMLCapability.ACQUIRE_STATUS,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Acquire Status",
                           description="Get disk usage and check health")
        self._cmd_dict.add(KMLCapability.ACQUIRE_SAMPLE,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Acquire Sample",
                           description="Take a snapshot")
        self._cmd_dict.add(KMLCapability.GOTO_PRESET,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Goto Preset",
                           description="Go to the preset number")
        self._cmd_dict.add(KMLCapability.SET_PRESET,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Set Preset",
                           description="Set the preset number")
        self._cmd_dict.add(KMLCapability.LAMP_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="lamp off",
                           description="Turn off the lamp")
        self._cmd_dict.add(KMLCapability.LAMP_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="lamp on",
                           description="Turn on the lamp")
        self._cmd_dict.add(KMLCapability.LASER_1_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 1  off",
                           description="Turn off the laser #1")
        self._cmd_dict.add(KMLCapability.LASER_2_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 2 off",
                           description="Turn off the laser #2")
        self._cmd_dict.add(KMLCapability.LASER_BOTH_OFF,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser off",
                           description="Turn off the all laser")
        self._cmd_dict.add(KMLCapability.LASER_1_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 1  on",
                           description="Turn on the laser #1")
        self._cmd_dict.add(KMLCapability.LASER_2_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser 2 on",
                           description="Turn on the laser #2")
        self._cmd_dict.add(KMLCapability.LASER_BOTH_ON,
                           timeout=DEFAULT_DICT_TIMEOUT,
                           display_name="Laser on",
                           description="Turn on the all laser")

    # #######################################################################
    # Private helpers.
    # #######################################################################


    def got_raw(self, port_agent_packet):
        """
        Called by the port agent client when raw data is available, such as data
        sent by the driver to the instrument, the instrument responses,etc.
        """
        self.publish_raw(port_agent_packet)

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

        sample = None
        if regex.match(line):

            particle = None

            # special case for the CAMDS image metadata particle - need to pass in param_dict
            if particle_class is CAMDS_IMAGE_METADATA:
                log.error("EXTRACT_SAMPLE: Particle is CAMDS_IMAGE_METADATA!!!!!")
                particle = particle_class(self._param_dict, port_timestamp=timestamp)
            else:
                particle = particle_class(line, port_timestamp=timestamp)

            parsed_sample = particle.generate()

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample

    def _get_params(self):
        return dir(KMLParameter)

    def _getattr_key(self, attr):
        return getattr(KMLParameter, attr)

    def _has_parameter(self, param):
        return KMLParameter.has(param)

    def _sanitize(self, s):
        s = s.replace('\xb3', '_')
        s = s.replace('\xbf', '_')
        s = s.replace('\xc0', '_')
        s = s.replace('\xd9', '_')
        s = s.replace('\xda', '_')
        s = s.replace('\xf8', '_')

        return s

    def _send_wakeup(self):
        """
        Send a wakeup to the device. Overridden by device specific
        subclasses.
        """
        self._connection.send(NEWLINE)

    def _wakeup(self, timeout, delay=1):
        """
        Clear buffers and send a wakeup command to the instrument
        @param timeout The timeout to wake the device.
        @param delay The time to wait between consecutive wakeups.
        @throw InstrumentTimeoutException if the device could not be woken.
        """
        # Clear the prompt buffer.
        log.debug("clearing promptbuf: %s", self._promptbuf)
        self._promptbuf = ''

        # Grab time for timeout.
        starttime = time.time()

        while True:
            # Send a line return and wait a sec.
            log.debug('Sending wakeup. timeout=%s', timeout)
            self._send_wakeup()
            time.sleep(delay)

            log.debug("Prompts: %s", self._get_prompts())

            for item in self._get_prompts():
                log.debug("buffer: %s", self._promptbuf)
                log.debug("find prompt: %s", item)

                index = self._promptbuf.find(item)
                log.debug("Got prompt (index: %s): %s ", index, repr(self._promptbuf))
                if index >= 0:
                    log.debug('wakeup got prompt: %s', repr(item))
                    return item
            log.debug("Searched for all prompts")

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in _wakeup()")

    def _get_prompts(self):
        """
        Return a list of prompts order from longest to shortest.  The
        assumption is the longer is more specific.
        @return: list of prompts orders by length.
        """
        if isinstance(self._prompts, list):
            prompts = self._prompts
        else:
            prompts = self._prompts.list()

        prompts.sort(lambda x, y: cmp(len(y), len(x)))

        return prompts

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Perform a command-response on the device.
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param write_delay kwarg for the amount of delay in seconds to pause
        between each character. If none supplied, the DEFAULT_WRITE_DELAY
        value will be used.
        @param timeout optional wakeup and command timeout via kwargs.
        @param expected_prompt kwarg offering a specific prompt to look for
        other than the ones in the protocol class itself.
        @param response_regex kwarg with a compiled regex for the response to
        match. Groups that match will be returned as a string.
        Cannot be supplied with expected_prompt. May be helpful for
        instruments that do not have a prompt.
        @retval resp_result The (possibly parsed) response result including the
        first instance of the prompt matched. If a regex was used, the prompt
        will be an empty string and the response will be the joined collection
        of matched groups.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """

        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', DEFAULT_CMD_TIMEOUT)
        expected_prompt = kwargs.get('expected_prompt', None)
        response_regex = kwargs.get('response_regex', None)
        write_delay = kwargs.get('write_delay', DEFAULT_WRITE_DELAY)

        if response_regex and not isinstance(response_regex, RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException('Cannot build command: %s' % cmd)

        cmd_line = build_handler(cmd, *args)

        # Wakeup the device, pass up exception if timeout
        self._wakeup(timeout)

        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        log.debug('_do_cmd_resp: %s, timeout=%s, write_delay=%s, expected_prompt=%s, response_regex=%s',
                        repr(cmd_line), timeout, write_delay, expected_prompt, response_regex)

        if (write_delay == 0):
            self._connection.send(cmd_line)

        else:
            for char in cmd_line:
                self._connection.send(char)
                time.sleep(write_delay)

        # Wait for the prompt, prepare result and return, timeout exception
        if response_regex:
            prompt = ""
            result_tuple = self._get_response(timeout,
                                              response_regex=response_regex,
                                              expected_prompt=expected_prompt)
            result = "".join(result_tuple)
        else:
            (prompt, result) = self._get_response(timeout,
                                                  expected_prompt=expected_prompt)

        resp_handler = self._response_handlers.get((self.get_current_state(), cmd), None) or \
            self._response_handlers.get(cmd, None)

        resp_result = None
        if resp_handler:
            resp_result = resp_handler(result, prompt)

        return resp_result

    # def _do_cmd_no_resp(self, cmd, *args, **kwargs):
    #     """
    #     Issue a command to the instrument after a wake up and clearing of
    #     buffers. No response is handled as a result of the command.
    #
    #     @param cmd The command to execute.
    #     @param args positional arguments to pass to the build handler.
    #     @param timeout=timeout optional wakeup timeout.
    #     @raises InstrumentTimeoutException if the response did not occur in time.
    #     @raises InstrumentProtocolException if command could not be built.
    #     """
    #
    #     timeout = kwargs.get('timeout', DEFAULT_CMD_TIMEOUT)
    #     write_delay = kwargs.get('write_delay', DEFAULT_WRITE_DELAY)
    #
    #     build_handler = self._build_handlers.get(cmd, None)
    #     if not build_handler:
    #         log.error('_do_cmd_no_resp: no handler for command: %s' % (cmd))
    #         raise InstrumentProtocolException(error_code=InstErrorCode.BAD_DRIVER_COMMAND)
    #     cmd_line = build_handler(cmd, *args)
    #
    #     # Wakeup the device, timeout exception as needed
    #     prompt = self._wakeup(timeout)
    #
    #     # Clear line and prompt buffers for result.
    #
    #     self._linebuf = ''
    #     self._promptbuf = ''
    #
    #     # Send command.
    #     log.debug('_do_cmd_no_resp: %s, timeout=%s' % (repr(cmd_line), timeout))
    #     if (write_delay == 0):
    #         self._connection.send(cmd_line)
    #     else:
    #         for char in cmd_line:
    #             self._connection.send(char)
    #             time.sleep(write_delay)

    def _do_cmd_direct(self, cmd):
        """
        Issue an untranslated command to the instrument. No response is handled
        as a result of the command.

        @param cmd The high level command to issue
        """

        # Send command.
        log.debug('_do_cmd_direct: <%s>' % cmd)
        self._connection.send(cmd)


class Prompt(KMLPrompt):
    """
    Device i/o prompts..
    """
    COMMAND = '<\x03:\x15:\x02>'


class Parameter(KMLParameter):
    """
    Device parameters
    """
    #
    # set-able parameters
    #


class ProtocolEvent(KMLProtocolEvent):
    """
    Protocol events
    """


class Capability(KMLCapability):
    """
    Protocol events that should be exposed to users (subset of above).
    """


class ScheduledJob(KMLScheduledJob):
    """
    Create ScheduledJob from KMLScheduledJob
    """


class InstrumentCmds(KMLInstrumentCmds):
    """
    Device specific commands
    Represents the commands the driver implements and the string that
    must be sent to the instrument to execute the command.
    """


class ProtocolState(KMLProtocolState):
    """
    Instrument protocol states
    """


class InstrumentDriver(CAMDSInstrumentDriver):
    """
    Specialization for this version of the cam driver
    """

    #__metaclass__ = METALOGGER

    def __init__(self, evt_callback):
        """
        InstrumentDriver constructor.
        @param evt_callback Driver process event callback.
        """
        # Construct superclass.
        CAMDSInstrumentDriver.__init__(self, evt_callback)

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)
        log.debug("self._protocol = " + repr(self._protocol))


class Protocol(CAMDSProtocol):
    """
    Specialization for this version of the cam driver
    """

    #__metaclass__ = METALOGGER

    def __init__(self, prompts, newline, driver_event):
        log.debug("IN Protocol.__init__")
        CAMDSProtocol.__init__(self, prompts, newline, driver_event)
        self.initialize_scheduler()

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
                             #default_value=None)

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
                             #default_value=None)
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

