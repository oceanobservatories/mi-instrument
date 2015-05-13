"""
@package mi.instrument.teledyne.workhorse.vadcp.driver
@file marine-integrations/mi/instrument/teledyne/workhorse/vadcp/driver.py
@author Sung Ahn
@brief Driver for the VADCP
"""

import base64
import time
from mi.instrument.teledyne.driver import TIMEOUT
from mi.instrument.teledyne.particles import ADCP_COMPASS_CALIBRATION_DataParticle, \
    ADCP_SYSTEM_CONFIGURATION_DataParticle, ADCP_ANCILLARY_SYSTEM_DATA_PARTICLE, ADCP_TRANSMIT_PATH_PARTICLE, \
    ADCP_PD0_PARSED_DataParticle, ADCP_COMPASS_CALIBRATION_REGEX_MATCHER, \
    ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER, ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER, ADCP_TRANSMIT_PATH_REGEX_MATCHER, \
    ADCP_PD0_PARSED_REGEX_MATCHER

from mi.instrument.teledyne.workhorse.driver import WorkhorseInstrumentDriver
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocol

from mi.instrument.teledyne.driver import TeledyneScheduledJob
from mi.instrument.teledyne.driver import TeledyneInstrumentCmds
from mi.instrument.teledyne.driver import TeledyneProtocolState
from mi.instrument.teledyne.driver import TeledynePrompt

from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()

from mi.core.util import dict_equal
from mi.core.time_tools import get_timestamp_delayed
from mi.core.common import InstErrorCode
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.instrument.teledyne.workhorse.driver import WorkhorseParameter
from mi.instrument.teledyne.workhorse.adcp.driver import ADCPUnits
from mi.instrument.teledyne.workhorse.adcp.driver import ADCPDescription
from mi.instrument.teledyne.driver import TeledyneProtocolEvent
from mi.core.instrument.port_agent_client import PortAgentClient
from mi.core.exceptions import InstrumentParameterException
from mi.core.instrument.instrument_driver import DriverConnectionState, DriverConfigKey
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import ResourceAgentEvent
from mi.core.exceptions import InstrumentConnectionException
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.data_particle import RawDataParticle
from mi.core.common import BaseEnum
from mi.core.instrument.instrument_protocol import InitializationType
from mi.core.exceptions import InstrumentParameterExpirationException
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.instrument.teledyne.driver import TeledyneParameter
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import ConfigMetadataKey

# newline.
NEWLINE = '\r\n'


class SlaveProtocol(BaseEnum):
    """
    The protocol needs to have 2 connections, 4Beam(Master) and 5thBeam(Slave)
    """
    FOURBEAM = '4Beam'
    FIFTHBEAM = '5thBeam'


class Parameter(WorkhorseParameter):
    """
    Device parameters
    """
    #
    # set-able parameters
    #
    SYNC_PING_ENSEMBLE = 'SA'
    RDS3_MODE_SEL = 'SM'  # 0=off, 1=master, 2=slave
    SLAVE_TIMEOUT = 'ST'
    SYNCH_DELAY = 'SW'


class TeledyneParameter2(DriverParameter):
    """
    Device parameters for the possible secondary instrument
    """
    #
    # set-able parameters
    #
    SERIAL_DATA_OUT = 'CD_5th'  # 000 000 000 Serial Data Out (Vel;Cor;Amp PG;St;P0 P1;P2;P3)
    INSTRUMENT_ID = 'CI_5th'  # Int 0-255
    XMIT_POWER = 'CQ_5th'  # 0=Low, 255=High
    SPEED_OF_SOUND = 'EC_5th'  # 1500  Speed Of Sound (m/s)
    SALINITY = 'ES_5th'  # 35 (0-40 pp thousand)
    COORDINATE_TRANSFORMATION = 'EX_5th'  #
    SENSOR_SOURCE = 'EZ_5th'  # Sensor Source (C;D;H;P;R;S;T)
    TIME_PER_ENSEMBLE = 'TE_5th'  # 01:00:00.00 (hrs:min:sec.sec/100)
    TIME_OF_FIRST_PING = 'TG_5th'  # ****/**/**,**:**:** (CCYY/MM/DD,hh:mm:ss)
    TIME = 'TT_5th'  # 2013/02/26,05:28:23 (CCYY/MM/DD,hh:mm:ss)
    FALSE_TARGET_THRESHOLD = 'WA_5th'  # 255,001 (Max)(0-255),Start Bin # <--------- TRICKY.... COMPLEX TYPE
    BANDWIDTH_CONTROL = 'WB_5th'  # Bandwidth Control (0=Wid,1=Nar)
    CORRELATION_THRESHOLD = 'WC_5th'  # 064  Correlation Threshold
    SERIAL_OUT_FW_SWITCHES = 'WD_5th'  # 111100000  Data Out (Vel;Cor;Amp PG;St;P0 P1;P2;P3)
    ERROR_VELOCITY_THRESHOLD = 'WE_5th'  # 5000  Error Velocity Threshold (0-5000 mm/s)
    BLANK_AFTER_TRANSMIT = 'WF_5th'  # 0088  Blank After Transmit (cm)
    CLIP_DATA_PAST_BOTTOM = 'WI_5th'  # 0 Clip Data Past Bottom (0=OFF,1=ON)
    RECEIVER_GAIN_SELECT = 'WJ_5th'  # 1  Rcvr Gain Select (0=Low,1=High)
    NUMBER_OF_DEPTH_CELLS = 'WN_5th'  # Number of depth cells (1-255)
    PINGS_PER_ENSEMBLE = 'WP_5th'  # Pings per Ensemble (0-16384)
    DEPTH_CELL_SIZE = 'WS_5th'  # 0800  Depth Cell Size (cm)
    TRANSMIT_LENGTH = 'WT_5th'  # 0000 Transmit Length 0 to 3200(cm) 0 = Bin Length
    PING_WEIGHT = 'WU_5th'  # 0 Ping Weighting (0=Box,1=Triangle)
    AMBIGUITY_VELOCITY = 'WV_5th'  # 175 Mode 1 Ambiguity Vel (cm/s radial)

    #
    # Workhorse parameters
    #
    SERIAL_FLOW_CONTROL = 'CF_5th'  # Flow Control
    BANNER = 'CH_5th'  # Banner
    SLEEP_ENABLE = 'CL_5th'  # SLEEP Enable
    SAVE_NVRAM_TO_RECORDER = 'CN_5th'  # Save NVRAM to RECORD
    POLLED_MODE = 'CP_5th'  # Polled Mode
    PITCH = 'EP_5th'  # Pitch
    ROLL = 'ER_5th'  # Roll

    LATENCY_TRIGGER = 'CX_5th'  # Latency Trigger
    HEADING_ALIGNMENT = 'EA_5th'  # Heading Alignment
    HEADING_BIAS = 'EB_5th'  # Heading Bias
    DATA_STREAM_SELECTION = 'PD_5th'  # Data Stream selection
    ENSEMBLE_PER_BURST = 'TC_5th'  # Ensemble per Burst
    BUFFERED_OUTPUT_PERIOD = 'TX_5th'  # Buffered Output Period
    SAMPLE_AMBIENT_SOUND = 'WQ_5th'  # Sample Ambient sound
    TRANSDUCER_DEPTH = 'ED_5th'  # Transducer Depth


class Parameter2(TeledyneParameter2):
    """
    Device parameters
    """
    SYNC_PING_ENSEMBLE = 'SA_5th'
    RDS3_MODE_SEL = 'SM_5th'  # 0=off, 1=master, 2=slave
    SLAVE_TIMEOUT = 'ST_5th'
    SYNCH_DELAY = 'SW_5th'


class InstrumentCmds(TeledyneInstrumentCmds):
    """
    Device specific commands
    Represents the commands the driver implements and the string that
    must be sent to the instrument to execute the command.
    """
    GET2 = 'get2'
    SET2 = 'set2'


class RawDataParticle_5thbeam(RawDataParticle):
    _data_particle_type = "raw_5thbeam"


class VADCP_COMPASS_CALIBRATION_DataParticle(ADCP_COMPASS_CALIBRATION_DataParticle):
    _data_particle_type = "vadcp_5thbeam_compass_calibration"


class VADCP_4BEAM_SYSTEM_CONFIGURATION_DataParticle(ADCP_SYSTEM_CONFIGURATION_DataParticle):
    _data_particle_type = "vadcp_4beam_system_configuration"
    _master = True


class VADCP_PD0_BEAM_PARSED_DataParticle(ADCP_PD0_PARSED_DataParticle):
    _data_particle_type = "vadcp_pd0_beam_parsed"
    _master = True


class VADCP_5THBEAM_SYSTEM_CONFIGURATION_DataParticle(ADCP_SYSTEM_CONFIGURATION_DataParticle):
    _data_particle_type = "vadcp_5thbeam_system_configuration"
    _slave = True
    _offset = 6


class VADCP_ANCILLARY_SYSTEM_DATA_PARTICLE(ADCP_ANCILLARY_SYSTEM_DATA_PARTICLE):
    _data_particle_type = "vadcp_ancillary_system_data"


class VADCP_TRANSMIT_PATH_PARTICLE(ADCP_TRANSMIT_PATH_PARTICLE):
    _data_particle_type = "vadcp_transmit_path"


class VADCP_PD0_PARSED_DataParticle(ADCP_PD0_PARSED_DataParticle):
    _data_particle_type = "VADCP"
    _slave = True


class AdcpPortAgentClient(PortAgentClient):
    def __init__(self, host, port, cmd_port, delim=None):
        PortAgentClient.__init__(self, host, port, cmd_port, delim=None)
        self.info = "This is portAgentClient for VADCP"


class InstrumentDriver(WorkhorseInstrumentDriver):
    """
    Specialization for this version of the workhorse VADCP driver
    """
    __metaclass__ =  get_logging_metaclass(log_level='debug')

    def __init__(self, evt_callback):
        """
        InstrumentDriver constructor.
        @param evt_callback Driver process event callback.
        """
        WorkhorseInstrumentDriver.__init__(self, evt_callback)

        # multiple portAgentClient
        self._connection = {}

    def apply_startup_params(self):
        """
        Apply the startup values previously stored in the protocol to
        the running config of the live instrument. The startup values are the
        values that are (1) marked as startup parameters and are (2) the "best"
        value to use at startup. Preference is given to the previously-set init
        value, then the default value, then the currently used value.

        This default implementation simply pushes the logic down into the protocol
        for processing should the action be better accomplished down there.

        The driver writer can decide to overload this method in the derived
        driver class and apply startup parameters in the driver (likely calling
        some get and set methods for the resource). If the driver does not
        implement an apply_startup_params() method in the driver, this method
        will call into the protocol. Deriving protocol classes are expected to
        implement an apply_startup_params() method lest they get the exception
        from the base InstrumentProtocol implementation.
        """
        # Apply startup params for Master and Slave
        self._protocol.apply_startup_params()
        self._protocol.apply_startup_params2()

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(TeledynePrompt, NEWLINE, self._driver_event)

    def _handler_disconnected_connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger and
        construct and initialize a protocol FSM for device interaction.
        @return (next_state, result) tuple, (DriverConnectionState.CONNECTED, None) if successful.
        @raises InstrumentConnectionException if the attempt to connect failed.
        """
        self._build_protocol()

        # for Master first
        try:
            self._connection[SlaveProtocol.FOURBEAM].init_comms(self._protocol.got_data,
                                                                 self._protocol.got_raw,
                                                                 self._got_config,
                                                                 self._got_exception,
                                                                 self._lost_connection_callback)
            self._protocol._connection_4Beam = self._connection[SlaveProtocol.FOURBEAM]
        except InstrumentConnectionException as e:
            log.error("Connection Exception Beam 1-4: %s", e)
            log.error("Instrument Driver remaining in disconnected state.")
            raise

        # for Slave
        try:
            self._connection[SlaveProtocol.FIFTHBEAM].init_comms(self._protocol.got_data2,
                                                                  self._protocol.got_raw2,
                                                                  self._got_config,
                                                                  self._got_exception,
                                                                  self._lost_connection_callback)
            self._protocol._connection_5thBeam = self._connection[SlaveProtocol.FIFTHBEAM]

        except InstrumentConnectionException as e:
            log.error("Connection Exception Beam 5: %s", e)
            log.error("Instrument Driver remaining in disconnected state.")
            raise

        return DriverConnectionState.CONNECTED, None

    def _handler_connected_disconnect(self, *args, **kwargs):
        """
        Disconnect to the device via port agent / logger and destroy the protocol FSM.
        @return (next_state, result) tuple, (DriverConnectionState.DISCONNECTED, None) if successful.
        """
        for connection in self._connection.values():
            connection.stop_comms()
        self._protocol = None
        return DriverConnectionState.DISCONNECTED, None

    def _handler_connected_connection_lost(self, *args, **kwargs):
        """
        The device connection was lost. Stop comms, destroy protocol FSM and revert to disconnected state.
        @return (next_state, result) tuple, (DriverConnectionState.DISCONNECTED, None).
        """
        for connection in self._connection.values():
            connection.stop_comms()
        self._protocol = None

        # Send async agent state change event.
        log.info("_handler_connected_connection_lost: sending LOST_CONNECTION event, moving to DISCONNECTED state.")
        self._driver_event(DriverAsyncEvent.AGENT_EVENT,
                           ResourceAgentEvent.LOST_CONNECTION)

        return DriverConnectionState.DISCONNECTED, None

    def _build_connection(self, all_configs):
        """
        Constructs and returns a Connection object according to the given
        configuration. The connection object is a LoggerClient instance in
        this base class. Subclasses can overwrite this operation as needed.
        The value returned by this operation is assigned to self._connection
        and also to self._protocol._connection upon entering in the
        DriverConnectionState.CONNECTED state.

        @param all_configs configuration dict
        @returns a dictionary of Connection instances, which will be assigned to self._connection
        @throws InstrumentParameterException Invalid configuration.
        """
        connections = {}
        for name, config in all_configs.items():
            if not isinstance(config, dict):
                continue
            if 'mock_port_agent' in config:
                mock_port_agent = config['mock_port_agent']
                # check for validity here...
                if mock_port_agent is not None:
                    connections[name] = mock_port_agent
            else:
                try:
                    addr = config['addr']
                    port = config['port']
                    cmd_port = config.get('cmd_port')

                    if isinstance(addr, str) and isinstance(port, int) and len(addr) > 0:
                        connections[name] = AdcpPortAgentClient(addr, port, cmd_port)
                    else:
                        raise InstrumentParameterException('Invalid comms config dict in build_connections.')

                except (TypeError, KeyError):
                    raise InstrumentParameterException('Invalid comms config dict..')
        return connections


class Protocol(WorkhorseProtocol):
    # There is only one protocol and only one state machine for VADCP.
    # The handlers of the state machine will invoke both 4Beam(master) and 5th beam(slave) instruments
    # There will be trailing '2' when the methods are used for the slave instrument
    DEFAULT_CMD_TIMEOUT = 20
    DEFAULT_WRITE_DELAY = 0

    def _get_params(self):
        return dir(Parameter)

    def _getattr_key(self, attr):
        return getattr(Parameter, attr)

    def _has_parameter(self, param):
        return Parameter.has(param)

    def _has_parameter2(self, param):
        return Parameter2.has(param)

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """

        WorkhorseProtocol.__init__(self, prompts, newline, driver_event)

        self._add_build_handler(InstrumentCmds.SET2, self._build_set_command2)
        self._add_build_handler(InstrumentCmds.GET2, self._build_get_command)

        self._add_response_handler(InstrumentCmds.SET2, self._parse_set_response)
        self._add_response_handler(InstrumentCmds.GET2, self._parse_get_response2)

        self._connection_4Beam = None
        self._connection_5thBeam = None

        # Line buffer for input from device.
        self._linebuf2 = ''

        # Short buffer to look for prompts from device in command-response
        # mode.
        self._promptbuf2 = ''

        # The parameter, command, and driver dictionaries.
        self._param_dict2 = ProtocolParameterDict()
        self._build_param_dict2()
        self._chunker2 = StringChunker(WorkhorseProtocol.sieve_function)

    def set_init_params(self, config):
        """
        Set the initialization parameters to the given values in the protocol parameter dictionary.
        @param config The parameter_name/value to set in the initialization fields of the parameter dictionary
        @raise InstrumentParameterException If the config cannot be set
        """
        if not isinstance(config, dict):
            raise InstrumentParameterException("Invalid init config format")

        self._startup_config = config
        param_config = config.get(DriverConfigKey.PARAMETERS)
        if param_config:
            for name in param_config.keys():
                log.trace("Setting init value for %s to %s", name, param_config[name])
                if name.find('_') != -1:  # Found
                    self._param_dict2.set_init_value(name, param_config[name])
                else:
                    self._param_dict.set_init_value(name, param_config[name])

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
        return_dict[ConfigMetadataKey.PARAMETERS].update(self._param_dict2.generate_dict())

        return return_dict

    def _build_set_command2(self, cmd, param, val):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @ return The set command to be sent to the device.
        @throws InstrumentProtocolException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """
        try:
            str_val = self._param_dict2.format(param + "_5th", val)

            set_cmd = '%s%s' % (param, str_val)
            set_cmd =+ NEWLINE
        except KeyError:
            raise InstrumentParameterException('Unknown driver parameter from _build_set_command2 %s' % param)

        return set_cmd

    def _parse_get_response2(self, response, prompt):
        if prompt == TeledynePrompt.ERR:
            raise InstrumentProtocolException(
                'Protocol._parse_get_response : Set command not recognized: %s' % response)

        while (not response.endswith('\r\n>\r\n>')) or ('?' not in response):
            (prompt, response) = self._get_raw_response2(30, TeledynePrompt.COMMAND)
            time.sleep(.05)  # was 1

        self._param_dict2.update(response)

        for line in response.split(NEWLINE):
            self._param_dict2.update(line)
            if not "?" in line and ">" != line:
                response = line

        if self.get_param not in response:
            raise InstrumentParameterException('Failed to get a response for lookup of ' + self.get_param)

        self.get_count = 0
        return response

    def _get_raw_response2(self, timeout=10, expected_prompt=None):
        """
        Get a response from the instrument, but don't trim whitespace. Used in
        times when the whitespace is what we are looking for.

        @param timeout The timeout in seconds
        @param expected_prompt Only consider the specific expected prompt as
        presented by this string
        @throw InstrumentProtocolException on timeout
        """
        # Grab time for timeout and wait for prompt.
        strip_chars = "\t "

        starttime = time.time()
        if expected_prompt is None:
            prompt_list = self._get_prompts()
        else:
            if isinstance(expected_prompt, str):
                prompt_list = [expected_prompt]
            else:
                prompt_list = expected_prompt

        while True:
            for item in prompt_list:
                if self._promptbuf2.rstrip(strip_chars).endswith(item.rstrip(strip_chars)):
                    return item, self._linebuf2
                else:
                    time.sleep(.1)

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in InstrumentProtocol._get_raw_response()")

    def _do_cmd_direct(self, cmd):
        """
        Issue an untranslated command to the instrument. No response is handled
        as a result of the command.

        @param cmd The high level command to issue
        """

        # Send command.
        if cmd.find('::') != -1:  # Found
            cmd_split = cmd.split('::', 1)
            instrument = cmd_split[0].strip()
            if instrument == "master":
                self._connection_4Beam.send(NEWLINE + cmd_split[1])
            if instrument == "slave":
                self._connection_5thBeam.send(NEWLINE + cmd_split[1])
        else:
            self._connection_4Beam.send(cmd)
            self._connection_5thBeam.send(cmd)

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
        @return resp_result The (possibly parsed) response result including the
        first instance of the prompt matched. If a regex was used, the prompt
        will be an empty string and the response will be the joined collection
        of matched groups.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """

        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', self.DEFAULT_CMD_TIMEOUT)
        expected_prompt = kwargs.get('expected_prompt', None)
        response_regex = kwargs.get('response_regex', None)
        write_delay = kwargs.get('write_delay', self.DEFAULT_WRITE_DELAY)

        if response_regex and not isinstance(response_regex, self.RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException('Cannot build command: %s' % cmd)

        cmd_line = build_handler(cmd, *args)
        # Wakeup the device, pass up exception if timeout
        self._wakeup_connection(self._connection_4Beam, self._promptbuf)

        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        log.trace('_do_cmd_resp: %s, timeout=%s, write_delay=%s, expected_prompt=%s, response_regex=%s',
                  repr(cmd_line), timeout, write_delay, expected_prompt, response_regex)

        if write_delay == 0:
            self._connection_4Beam.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection_4Beam.send(char)
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

    def _do_cmd_resp2(self, cmd, *args, **kwargs):
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
        @return resp_result The (possibly parsed) response result including the
        first instance of the prompt matched. If a regex was used, the prompt
        will be an empty string and the response will be the joined collection
        of matched groups.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """

        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', self.DEFAULT_CMD_TIMEOUT)
        expected_prompt = kwargs.get('expected_prompt', None)
        response_regex = kwargs.get('response_regex', None)
        write_delay = kwargs.get('write_delay', self.DEFAULT_WRITE_DELAY)
        if response_regex and not isinstance(response_regex, self.RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)

        if not build_handler:
            raise InstrumentProtocolException('Cannot build command: %s' % cmd)
        cmd_line = build_handler(cmd, *args)
        # Wakeup the device, pass up exception if timeout
        self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)

        # Clear line and prompt buffers for result.
        self._linebuf2 = ''
        self._promptbuf2 = ''

        # Send command.
        log.trace('_do_cmd_resp2: %s, timeout=%s, write_delay=%s, expected_prompt=%s, response_regex=%s',
                  repr(cmd_line), timeout, write_delay, expected_prompt, response_regex)
        if write_delay == 0:
            self._connection_5thBeam.send(cmd_line)

        else:
            for char in cmd_line:
                self._connection_5thBeam.send(char)
                time.sleep(write_delay)

        # Wait for the prompt, prepare result and return, timeout exception
        if response_regex:
            prompt = ""
            result_tuple = self._get_response2(timeout,
                                               response_regex=response_regex,
                                               expected_prompt=expected_prompt)
            result = "".join(result_tuple)
        else:
            (prompt, result) = self._get_response2(timeout,
                                                   expected_prompt=expected_prompt)

        resp_handler = self._response_handlers.get((self.get_current_state(), cmd), None) or \
                       self._response_handlers.get(cmd, None)
        resp_result = None
        if resp_handler:
            resp_result = resp_handler(result, prompt)

        return resp_result

    def _get_response2(self, timeout=10, expected_prompt=None, response_regex=None):
        """
        Get a response from the instrument, but be a bit loose with what we
        find. Leave some room for white space around prompts and not try to
        match that just in case we are off by a little whitespace or not quite
        at the end of a line.

        @todo Consider cases with no prompt
        @param timeout The timeout in seconds
        @param expected_prompt Only consider the specific expected prompt as
        presented by this string
        @param response_regex Look for a response value that matches the
        supplied compiled regex pattern. Groups that match will be returned as a
        string. Cannot be used with expected prompt. None
        will be returned as a prompt with this match. If a regex is supplied,
        internal the prompt list will be ignored.
        @return Regex search result tuple (as MatchObject.groups() would return
        if a response_regex is supplied. A tuple of (prompt, response) if a
        prompt is looked for.
        @throw InstrumentProtocolException if both regex and expected prompt are
        passed in or regex is not a compiled pattern.
        @throw InstrumentTimeoutException on timeout
        """
        # Grab time for timeout and wait for prompt.
        starttime = time.time()

        if response_regex and not isinstance(response_regex, self.RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        if response_regex:
            prompt_list = []

        if expected_prompt is None:
            prompt_list = self._get_prompts()
        else:
            if isinstance(expected_prompt, str):
                prompt_list = [expected_prompt]
            else:
                prompt_list = expected_prompt

        while True:
            if response_regex:
                match = response_regex.search(self._linebuf2)
                if match:
                    return match.groups()
                else:
                    time.sleep(.1)
            else:
                for item in prompt_list:
                    index = self._promptbuf2.find(item)
                    if index >= 0:
                        result = self._promptbuf2[0:index + len(item)]
                        return item, result
                    else:
                        time.sleep(.1)

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in InstrumentProtocol._get_response()")

    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        Issue a command to the instrument after a wake up and clearing of
        buffers. No response is handled as a result of the command.

        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup timeout.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built.
        """

        timeout = kwargs.get('timeout', self.DEFAULT_CMD_TIMEOUT)
        write_delay = kwargs.get('write_delay', self.DEFAULT_WRITE_DELAY)

        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            log.error('_do_cmd_no_resp: no handler for command: %s' % cmd)
            raise InstrumentProtocolException(error_code=InstErrorCode.BAD_DRIVER_COMMAND)
        cmd_line = build_handler(cmd, *args)

        # Wakeup the device, timeout exception as needed
        self._wakeup_connection(self._connection_4Beam, self._promptbuf)

        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        log.trace('_do_cmd_no_resp: %s, timeout=%s' % (repr(cmd_line), timeout))
        if write_delay == 0:
            self._connection_4Beam.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection_4Beam.send(char)
                time.sleep(write_delay)

    def _do_cmd_no_resp2(self, cmd, *args, **kwargs):
        """
        Issue a command to the instrument after a wake up and clearing of
        buffers. No response is handled as a result of the command.

        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup timeout.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built.
        """

        timeout = kwargs.get('timeout', self.DEFAULT_CMD_TIMEOUT)
        write_delay = kwargs.get('write_delay', self.DEFAULT_WRITE_DELAY)

        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            log.error('_do_cmd_no_resp: no handler for command: %s' % cmd)
            raise InstrumentProtocolException(error_code=InstErrorCode.BAD_DRIVER_COMMAND)
        cmd_line = build_handler(cmd, *args)

        # Wakeup the device, timeout exception as needed
        self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)

        # Clear line and prompt buffers for result.
        self._linebuf2 = ''
        self._promptbuf2 = ''

        # Send command.
        log.trace('_do_cmd_no_resp2: %s, timeout=%s' % (repr(cmd_line), timeout))
        if write_delay == 0:
            self._connection_5thBeam.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection_5thBeam.send(char)
                time.sleep(write_delay)

    def got_data2(self, port_agent_packet):
        """
        Called by the instrument connection when data is available.
        Append line and prompt buffers.

        @param port_agent_packet is port agent stream.

        Also add data to the chunker and when received call got_chunk
        to publish results.
        """

        data_length = port_agent_packet.get_data_length()
        data = port_agent_packet.get_data()
        timestamp = port_agent_packet.get_timestamp()

        if data_length > 0:
            if self.get_current_state() == DriverProtocolState.DIRECT_ACCESS:
                self._driver_event(DriverAsyncEvent.DIRECT_ACCESS, data)

            self.add_to_buffer2(data)

            self._chunker2.add_chunk(data, timestamp)
            (timestamp, chunk) = self._chunker2.get_next_data()
            while chunk:
                self._got_chunk2(chunk, timestamp)
                (timestamp, chunk) = self._chunker2.get_next_data()

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """
        self._extract_sample(ADCP_COMPASS_CALIBRATION_DataParticle,
                             ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VADCP_PD0_BEAM_PARSED_DataParticle,
                             ADCP_PD0_PARSED_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VADCP_4BEAM_SYSTEM_CONFIGURATION_DataParticle,
                             ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(ADCP_ANCILLARY_SYSTEM_DATA_PARTICLE,
                             ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(ADCP_TRANSMIT_PATH_PARTICLE,
                             ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                             chunk,
                             timestamp)

    def _got_chunk2(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """
        self._extract_sample(VADCP_COMPASS_CALIBRATION_DataParticle,
                             ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VADCP_PD0_PARSED_DataParticle,
                             ADCP_PD0_PARSED_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VADCP_5THBEAM_SYSTEM_CONFIGURATION_DataParticle,
                             ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VADCP_ANCILLARY_SYSTEM_DATA_PARTICLE,
                             ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VADCP_TRANSMIT_PATH_PARTICLE,
                             ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                             chunk,
                             timestamp)

    def got_raw2(self, port_agent_packet):
        """
        Called by the port agent client when raw data is available, such as data
        sent by the driver to the instrument, the instrument responses,etc.
        """
        self.publish_raw2(port_agent_packet)

    def publish_raw2(self, port_agent_packet):
        """
        Publish raw data
        @param: port_agent_packet port agent packet containing raw
        """
        particle = RawDataParticle_5thbeam(port_agent_packet.get_as_dict(),
                                           port_timestamp=port_agent_packet.get_timestamp())

        parsed_sample = particle.generate()
        if self._driver_event:
            self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

    def add_to_buffer2(self, data):
        """
        Add a chunk of data to the internal data buffers
        @param data: bytes to add to the buffer
        """
        self._linebuf2 += data
        self._promptbuf2 += data
        self._last_data_timestamp2 = time.time()

    def _wakeup_until2(self, timeout, desired_prompt, delay=1, no_tries=5):
        """
        Continue waking device until a specific prompt appears or a number
        of tries has occurred. Desired prompt must be in the instrument's
        prompt list.
        @param timeout The timeout to wake the device.
        @desired_prompt Continue waking until this prompt is seen.
        @delay Time to wake between consecutive wakeups.
        @no_tries Maximum number of wakeup tries to see desired prompt.
        @raises InstrumentTimeoutException if device could not be woken.
        @raises InstrumentProtocolException if the desired prompt is not seen in the
        maximum number of attempts.
        """

        count = 0
        while count < no_tries:
            prompt = self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)
            if prompt == desired_prompt:
                break
            time.sleep(delay)
            count += 1
            if count >= no_tries:
                raise InstrumentProtocolException('Incorrect prompt.')

    def _send_break(self, duration=3000):
        """
        Send a BREAK to attempt to wake the device.
        """
        self._promptbuf = ''
        self._linebuf = ''
        self._send_break_cmd(duration, self._connection_4Beam)
        break_message = "[BREAK Wakeup A]"

        found = False
        timeout = 30
        count = 0
        while not found:
            count += 1
            if break_message in self._linebuf:
                    found = True
            if count > (timeout * 10):
                if not found:
                    raise InstrumentTimeoutException("NO BREAK RESPONSE.")
            time.sleep(0.1)
        self._chunker._clean_buffer(len(self._chunker.raw_chunk_list))
        self._promptbuf = ''
        self._linebuf = ''
        return True

    def _send_break2(self, duration=3000):
        """
        Send a BREAK to attempt to wake the device.
        """
        self._promptbuf2 = ''
        self._linebuf2 = ''
        self._send_break_cmd(duration, self._connection_5thBeam)
        break_message = "[BREAK Wakeup A]"

        found = False
        timeout = 30
        count = 0
        while not found:
            count += 1
            if break_message in self._linebuf2:
                found = True
            if count > (timeout * 10):
                if not found:
                    raise InstrumentTimeoutException("NO BREAK RESPONSE2.")
            time.sleep(0.1)
        self._chunker2._clean_buffer(len(self._chunker2.raw_chunk_list))
        self._promptbuf2 = ''
        self._linebuf2 = ''
        return True


    def _send_wakeup(self, connection):
        connection.send(NEWLINE)
        connection.send(NEWLINE)

    def _send_break_cmd(self, delay=1000, connection=None):
        """
        Send a BREAK to attempt to wake the device.
        """
        connection.send_break(delay)

    def _sync_clock(self, command, date_time_param, timeout=TIMEOUT, delay=1, time_format="%d %b %Y %H:%M:%S"):
        """
        Send the command to the instrument to synchronize  the clock
        @param date_time_param: date time parameter that we want to set
        @param timeout: command timeout
        @param delay: wakeup delay
        @param time_format: time format string for set command
        @return: true if the command is successful
        @throws: InstrumentProtocolException if command fails
        """

        # lets clear out any past data so it doesnt confuse the command
        self._linebuf = ''
        self._promptbuf = ''

        self._linebuf2 = ''
        self._promptbuf2 = ''

        self._wakeup_connection(self._connection_4Beam, self._promptbuf)
        self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)

        str_val = get_timestamp_delayed(time_format)
        self._do_cmd_direct(date_time_param + str_val)

        time.sleep(1)

        self._get_response(TIMEOUT)
        self._get_response2(TIMEOUT)

    def _instrument_config_dirty2(self):
        """
        Read the startup config and compare that to what the instrument
        is configured too.  If they differ then return True
        @return: True if the startup config doesn't match the instrument
        @throws: InstrumentParameterException
        """

        startup_params2 = self._param_dict2.get_startup_list()
        log.trace("Startup Parameters 5th beam: %s" % startup_params2)

        for param in startup_params2:
            split_param = param.split('_', 1)
            _param = split_param[0]
            if not self._has_parameter2(param):
                raise InstrumentParameterException("Parameters are unknown")

            if self._param_dict2.get(param) != self._param_dict2.get_config_value(param):
                log.trace("DIRTY: %s %s != %s" % (
                    param, self._param_dict2.get(param), self._param_dict2.get_config_value(param)))
                return True

        return False

    # for Slave
    def _update_params2(self, *args, **kwargs):
        """
        Update the parameter dictionary.
        """
        _error = None
        logging = self._is_logging2()
        results = None

        try:
            if logging:
                # Switch to command mode,
                self._stop_logging2()

            # ##
            # Get old param dict config.
            old_config = self._param_dict2.get_config()
            kwargs['expected_prompt'] = TeledynePrompt.COMMAND

            cmds = self._get_params()
            results = ""
            for attr in sorted(cmds):
                if attr not in ['dict', 'has', 'list', 'ALL', 'GET_STATUS_INTERVAL', 'CLOCK_SYNCH_INTERVAL']:

                    if not attr.startswith("_"):
                        key = self._getattr_key(attr)
                        key_split = key.split('_', 1)

                        result = self._do_cmd_resp2(InstrumentCmds.GET2, key_split[0], **kwargs)
                        results += result + NEWLINE

            new_config = self._param_dict2.get_config()

            if not dict_equal(new_config, old_config, ['TT', 'TT_5th']):
                self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        # Catch all errors, go back to streaming, then rethrow the error
        except Exception as e:
            log.error("EXCEPTION WAS " + str(e))
            _error = e

        # Switch back to streaming
        if logging:
            my_state = self._protocol_fsm.get_current_state()
            log.trace("current_state = %s calling start_logging", my_state)
            self._start_logging2()

        if _error:
            raise _error

        return results

    # for Slave
    def _set_params2(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        result = None
        startup = False
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        try:
            startup = args[1]
        except IndexError:
            pass

        self._verify_not_readonly2(*args, **kwargs)
        for (key, val) in params.iteritems():
            if key.find('_') != -1:  # Found
                if key not in [TeledyneParameter.CLOCK_SYNCH_INTERVAL, TeledyneParameter.GET_STATUS_INTERVAL]:
                    key_split = key.split('_', 1)
                    result = self._do_cmd_resp2(InstrumentCmds.SET2, key_split[0], val, **kwargs)

        # Handle engineering parameters
        changed = False

        if TeledyneParameter.CLOCK_SYNCH_INTERVAL in params:
            if (params[TeledyneParameter.CLOCK_SYNCH_INTERVAL] != self._param_dict.get(
                    TeledyneParameter.CLOCK_SYNCH_INTERVAL)):
                self._param_dict.set_value(TeledyneParameter.CLOCK_SYNCH_INTERVAL,
                                           params[TeledyneParameter.CLOCK_SYNCH_INTERVAL])
                self.start_scheduled_job(TeledyneParameter.CLOCK_SYNCH_INTERVAL, TeledyneScheduledJob.CLOCK_SYNC,
                                         TeledyneProtocolEvent.SCHEDULED_CLOCK_SYNC)
                changed = True

        if TeledyneParameter.GET_STATUS_INTERVAL in params:
            if (params[TeledyneParameter.GET_STATUS_INTERVAL] != self._param_dict.get(
                    TeledyneParameter.GET_STATUS_INTERVAL)):
                self._param_dict.set_value(TeledyneParameter.GET_STATUS_INTERVAL,
                                           params[TeledyneParameter.GET_STATUS_INTERVAL])
                self.start_scheduled_job(TeledyneParameter.GET_STATUS_INTERVAL,
                                         TeledyneScheduledJob.GET_CONFIGURATION,
                                         TeledyneProtocolEvent.SCHEDULED_GET_STATUS)
                changed = True
        if changed:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        self._update_params2()
        return result

    # for Slave
    def apply_startup_params2(self):
        """
        Apply all startup parameters.  First we check the instrument to see
        if we need to set the parameters.  If they are they are set
        correctly then we don't do anything.

        If we need to set parameters then we might need to transition to
        command first.  Then we will transition back when complete.

        @throws: InstrumentProtocolException if not in command or streaming
        """
        # Let's give it a try in unknown state
        if (self.get_current_state() != TeledyneProtocolState.COMMAND and
                    self.get_current_state() != TeledyneProtocolState.AUTOSAMPLE):
            raise InstrumentProtocolException("Not in command or autosample state. Unable to apply startup params")

        logging = self._is_logging2()
        # If we are in streaming mode and our configuration on the
        # instrument matches what we think it should be then we
        # don't need to do anything.

        if not self._instrument_config_dirty2():
            return True

        error_status = None

        try:
            if logging:
                # Switch to command mode,
                self._stop_logging2()
            self._apply_params2()

        # Catch all errors, go back to streaming, then rethrow the error
        except Exception as e:
            log.error("EXCEPTION WAS " + str(e))
            error_status = e

        # Switch back to streaming
        if logging:
            self._start_logging2()

        if error_status:
            raise error_status

    def _apply_params2(self):
        """
        apply startup parameters to the instrument.
        @throws: InstrumentProtocolException if in wrong mode.
        """
        config = self.get_startup_config2()
        # Pass true to _set_params so we know these are startup values
        self._set_params2(config, True)

    def get_startup_config2(self):
        """
        Gets the startup configuration for the instrument. The parameters
        returned are marked as startup, and the values are the best as chosen
        from the initialization, default, and current parameters.

        @return The dict of parameter_name/values (override this method if it
            is more involved for a specific instrument) that should be set at
            a higher level.

        @raise InstrumentProtocolException if a startup parameter doesn't
               have a init or default value
        """
        return_dict = {}
        start_list = self._param_dict2.get_keys()
        log.trace("Startup list 2: %s", start_list)
        assert isinstance(start_list, list)

        for param in start_list:
            result = self._param_dict2.get_config_value(param)
            if result is not None:
                return_dict[param] = result
            elif self._param_dict2.is_startup_param(param):
                raise InstrumentProtocolException("Required startup value not specified: %s" % param)

        log.trace("Applying startup config: %s", return_dict)
        return return_dict

    def _init_params(self):
        """
        Initialize parameters based on initialization type.  If we actually
        do some initialization (either startup or DA) after we are done
        set the init type to None so we don't initialize again.
        @raises InstrumentProtocolException if the init_type isn't set or it
                                            is unknown
        """
        if self._init_type == InitializationType.STARTUP:
            log.debug("_init_params: Apply Startup Config")
            self.apply_startup_params()
            self.apply_startup_params2()
            self._init_type = InitializationType.NONE
        elif self._init_type == InitializationType.DIRECTACCESS:
            log.debug("_init_params: Apply DA Config")
            self.apply_direct_access_params()
            self.apply_direct_access_params2()
            self._init_type = InitializationType.NONE
        elif self._init_type == InitializationType.NONE:
            log.debug("_init_params: No initialization required")
        elif self._init_type is None:
            raise InstrumentProtocolException("initialization type not set")
        else:
            raise InstrumentProtocolException("Unknown initialization type: %s" % self._init_type)

    def _is_logging2(self, timeout=TIMEOUT):
        """
        Poll the instrument to see if we are in logging mode.  Return True
        if we are, False if not.
        @param: timeout - Command timeout
        @return: True - instrument logging, False - not logging
        """

        self._linebuf2 = ""
        self._promptbuf2 = ""

        prompt = self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)
        if TeledynePrompt.COMMAND == prompt:
            logging = False
            log.trace("COMMAND MODE!")
        else:
            logging = True
            log.trace("AUTOSAMPLE MODE!")

        return logging


    def _is_logging(self, timeout=TIMEOUT):
        """
        Poll the instrument to see if we are in logging mode.  Return True
        if we are, False if not.
        @param: timeout - Command timeout
        @return: True - instrument logging, False - not logging
        """

        self._linebuf = ""
        self._promptbuf = ""

        prompt = self._wakeup_connection(self._connection_4Beam, self._promptbuf)
        if TeledynePrompt.COMMAND == prompt:
            logging = False
        else:
            logging = True

        return logging

    def _discover(self):
        """
        Discover current state; will always discover to COMMAND.
        @return (next_protocol_state, next_agent_state)
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentStateException if the device response does not correspond to
        an expected state.
        """

        #stop autosample if we are in it, send break commands to both ports
        self._send_break(1000)
        self._send_break2(1000)

        return TeledyneProtocolState.COMMAND, ResourceAgentState.COMMAND

    def _start_logging2(self, timeout=TIMEOUT):
        """
        Command the instrument to start logging
        @param timeout: how long to wait for a prompt
        @throws: InstrumentProtocolException if failed to start logging
        """
        if self._is_logging2():
            log.trace("ALREADY LOGGING2")
            return True
        log.trace("SENDING START LOGGING2")
        self._do_cmd_no_resp2(TeledyneInstrumentCmds.START_LOGGING, timeout=timeout)

    def _stop_logging(self, timeout=TIMEOUT):
        self._stop_logging1()
        self._stop_logging2()

    def _stop_logging2(self, timeout=TIMEOUT):
        """
        Command the instrument to stop logging
        @param timeout: how long to wait for a prompt
        @throws: InstrumentTimeoutException if prompt isn't seen
        @throws: InstrumentProtocolException failed to stop logging
        """
        # Issue the stop command.

        # Send break twice, as sometimes the driver ack's the first one then
        # forgets to actually break.

        self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)
        self._send_break2(duration=3000)
        time.sleep(2)
        # Prompt device until command prompt is seen.
        timeout = 3
        self._wakeup_until2(timeout, TeledynePrompt.COMMAND)

        # set logging to false, as we just got a prompt after a break

        if self._is_logging2(timeout):
            raise InstrumentProtocolException("failed to stop logging")

    def _stop_logging1(self, timeout=TIMEOUT):
        """
        Command the instrument to stop logging
        @param timeout: how long to wait for a prompt
        @throws: InstrumentTimeoutException if prompt isn't seen
        @throws: InstrumentProtocolException failed to stop logging
        """
        # Issue the stop command.

        # Send break twice, as sometimes the driver ack's the first one then
        self._wakeup_connection(self._connection_4Beam, self._promptbuf)
        self._send_break(duration=3000)
        time.sleep(2)

        # Prompt device until command prompt is seen.
        timeout = 3
        self._wakeup_until(timeout, TeledynePrompt.COMMAND)
        # set logging to false, as we just got a prompt after a break

        if self._is_logging(timeout):
            log.error("FAILED TO STOP LOGGING in _stop_logging")
            raise InstrumentProtocolException("failed to stop logging")

    def _verify_not_readonly2(self, params_to_set, startup=False):
        """
        Verify that the parameters we are attempting to set in upstream methods
        are not readonly.  A parameter is considered read only if it is characterized
        as read-only or immutable.  However, if the startup flag is passed in as true
        then immutable will be considered settable.
        @param params_to_set: dictionary containing parameters to set
        @param startup: startup flag, if set don't verify visibility
        @return: True if we aren't violating visibility
        @raise: InstrumentParameterException if we violate visibility
        """
        if not isinstance(params_to_set, dict):
            raise InstrumentParameterException('parameters not a dict.')

        readonly_params = self._param_dict2.get_visibility_list(ParameterDictVisibility.READ_ONLY)
        if not startup:
            readonly_params += self._param_dict2.get_visibility_list(ParameterDictVisibility.IMMUTABLE)

        not_settable = []
        for (key, val) in params_to_set.iteritems():
            if key in readonly_params:
                not_settable.append(key)
        if len(not_settable) > 0:
            raise InstrumentParameterException("Attempt to set read only parameter(s) (%s)" % not_settable)

        return True

    def _get_param_list(self, *args, **kwargs):
        """
        returns a list of parameters based on the list passed in.  If the
        list contains and ALL parameters request then the list will contain
        all parameters.  Otherwise the original list will be returned. Also
        check the list for unknown parameters
        @param args[0] list of parameters to inspect
        @return: list of parameters.
        @raises: InstrumentParameterException when the wrong param type is passed
        in or an unknown parameter is in the list
        """
        try:
            param_list = args[0]
        except IndexError:
            raise InstrumentParameterException('Parameter required, none specified')

        if isinstance(param_list, str):
            param_list = [param_list]
        elif not isinstance(param_list, (list, tuple)):
            raise InstrumentParameterException("Expected a list, tuple or a string")

        # Verify all parameters are known parameters
        bad_params = []
        new_param_list = []
        known_params = self._param_dict.get_keys() + [DriverParameter.ALL]
        for param in param_list:
            if param.find('_') == -1:  # Not Found
                if param not in [DriverParameter.ALL]:
                    new_param_list.append(param)
                if param not in known_params:
                    bad_params.append(param)

        if len(bad_params):
            raise InstrumentParameterException("Unknown parameters: %s" % bad_params)

        if DriverParameter.ALL in param_list:
            return self._param_dict.get_keys()
        else:
            return new_param_list

    def _get_param_list2(self, *args, **kwargs):
        """
        returns a list of parameters based on the list passed in.  If the
        list contains and ALL parameters request then the list will contain
        all parameters.  Otherwise the original list will be returned. Also
        check the list for unknown parameters
        @param args[0] list of parameters to inspect
        @return: list of parameters.
        @raises: InstrumentParameterException when the wrong param type is passed
        in or an unknown parameter is in the list
        """
        try:
            param_list = args[0]
        except IndexError:
            raise InstrumentParameterException('Parameter required, none specified')

        if isinstance(param_list, str):
            param_list = [param_list]
        elif not isinstance(param_list, (list, tuple)):
            raise InstrumentParameterException("Expected a list, tuple or a string")
        # Verify all parameters are known parameters
        bad_params = []
        new_param_list = []
        known_params = self._param_dict2.get_keys() + [DriverParameter.ALL]
        for param in param_list:
            if param.find('_') != -1:  # Found
                new_param_list.append(param)
                if param not in known_params:
                    bad_params.append(param)

        if len(bad_params):
            raise InstrumentParameterException("Unknown parameters2: %s" % bad_params)

        if DriverParameter.ALL in param_list:
            return self._param_dict2.get_keys()
        else:
            return new_param_list

    def _get_param_result2(self, param_list, expire_time):
        """
        return a dictionary of the parameters and values
        @param expire_time: baseline time for expiration calculation
        @return: dictionary of values
        @throws InstrumentParameterException if missing or invalid parameter
        @throws InstrumentParameterExpirationException if value is expired.
        """
        result = {}

        for param in param_list:
            val = self._param_dict2.get(param, expire_time)
            result[param] = val

        return result

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with VADCP parameters for master instrument.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """

        self._param_dict.add(Parameter.SERIAL_DATA_OUT,
                             r'CD = (\d\d\d \d\d\d \d\d\d) \-+ Serial Data Out ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Serial data out",
                             value_description=ADCPDescription.SERIALDATAOUT,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value='000 000 000')

        self._param_dict.add(Parameter.SERIAL_FLOW_CONTROL,
                             r'CF = (\d+) \-+ Flow Ctrl ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Serial flow control",
                             value_description=ADCPDescription.FLOWCONTROL,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value='11110')

        self._param_dict.add(Parameter.BANNER,
                             r'CH = (\d) \-+ Suppress Banner',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Banner",
                             value_description=ADCPDescription.TRUEON,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=False)

        self._param_dict.add(Parameter.INSTRUMENT_ID,
                             r'CI = (\d+) \-+ Instrument ID ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Instrument id",
                             direct_access=True,
                             startup_param=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=0)

        self._param_dict.add(Parameter.SLEEP_ENABLE,
                             r'CL = (\d) \-+ Sleep Enable',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Sleep enable",
                             value_description=ADCPDescription.SLEEP,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=0)

        self._param_dict.add(Parameter.SAVE_NVRAM_TO_RECORDER,
                             r'CN = (\d) \-+ Save NVRAM to recorder',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Save nvram to recorder",
                             value_description=ADCPDescription.TRUEOFF,
                             startup_param=True,
                             default_value=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE)

        self._param_dict.add(Parameter.POLLED_MODE,
                             r'CP = (\d) \-+ PolledMode ',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Polled mode",
                             value_description=ADCPDescription.TRUEON,
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value=False)

        self._param_dict.add(Parameter.XMIT_POWER,
                             r'CQ = (\d+) \-+ Xmt Power ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Xmit power",
                             value_description=ADCPDescription.XMTPOWER,
                             startup_param=True,
                             direct_access=True,
                             default_value=255)

        self._param_dict.add(Parameter.LATENCY_TRIGGER,
                             r'CX = (\d) \-+ Trigger Enable ',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="latency trigger",
                             value_description=ADCPDescription.TRUEON,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=False)

        self._param_dict.add(Parameter.HEADING_ALIGNMENT,
                             r'EA = ([+-]\d+) \-+ Heading Alignment',
                             lambda match: int(match.group(1)),
                             lambda value: '%+06d' % value,
                             type=ParameterDictType.INT,
                             display_name="Heading alignment",
                             units=ADCPUnits.CDEGREE,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             direct_access=True,
                             startup_param=True,
                             default_value=+00000)

        self._param_dict.add(Parameter.HEADING_BIAS,
                             r'EB = ([+-]\d+) \-+ Heading Bias',
                             lambda match: int(match.group(1)),
                             lambda value: '%+06d' % value,
                             type=ParameterDictType.INT,
                             display_name="Heading Bias",
                             units=ADCPUnits.CDEGREE,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=+00000)

        self._param_dict.add(Parameter.SPEED_OF_SOUND,
                             r'EC = (\d+) \-+ Speed Of Sound',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Speed of sound",
                             units=ADCPUnits.MPERS,
                             startup_param=True,
                             direct_access=True,
                             default_value=1485)

        self._param_dict.add(Parameter.TRANSDUCER_DEPTH,
                             r'ED = (\d+) \-+ Transducer Depth ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Transducer Depth",
                             units=ADCPUnits.DM,
                             startup_param=True,
                             direct_access=True,
                             default_value=2000)

        self._param_dict.add(Parameter.PITCH,
                             r'EP = ([+-]\d+) \-+ Tilt 1 Sensor ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Pitch",
                             units=ADCPUnits.CDEGREE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.ROLL,
                             r'ER = ([\+\-]\d+) \-+ Tilt 2 Sensor ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Roll",
                             units=ADCPUnits.CDEGREE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.SALINITY,
                             r'ES = (\d+) \-+ Salinity ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Salinity",
                             units=ADCPUnits.PPTHOUSAND,
                             startup_param=True,
                             direct_access=True,
                             default_value=35)

        self._param_dict.add(Parameter.COORDINATE_TRANSFORMATION,
                             r'EX = (\d+) \-+ Coord Transform ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Coordinate transformation",
                             startup_param=True,
                             direct_access=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             default_value='00111')

        self._param_dict.add(Parameter.SENSOR_SOURCE,
                             r'EZ = (\d+) \-+ Sensor Source ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Sensor source",
                             startup_param=True,
                             direct_access=True,
                             default_value='1111101')

        self._param_dict.add(Parameter.DATA_STREAM_SELECTION,
                             r'PD = (\d+) \-+ Data Stream Select',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Data Stream Selection",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.SYNC_PING_ENSEMBLE,
                             r'SA = (\d+) \-+ Synch Before',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Synch ping ensemble",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value='001')

        self._param_dict.add(Parameter.RDS3_MODE_SEL,
                             r'SM = (\d+) \-+ Mode Select',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="RDS3 mode selection",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=1)

        self._param_dict.add(Parameter.SYNCH_DELAY,
                             r'SW = (\d+) \-+ Synch Delay',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Synch Delay",
                             units=ADCPUnits.TENTHMILLISECOND,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=100)

        self._param_dict.add(Parameter.ENSEMBLE_PER_BURST,
                             r'TC (\d+) \-+ Ensembles Per Burst',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Ensemble per burst",
                             units=ADCPUnits.ENSEMBLEPERBURST,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.TIME_PER_ENSEMBLE,
                             r'TE (\d\d:\d\d:\d\d.\d\d) \-+ Time per Ensemble ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time per ensemble",
                             value_description=ADCPDescription.INTERVALTIMEHundredth,
                             startup_param=True,
                             direct_access=True,
                             default_value='00:00:00.00')

        self._param_dict.add(Parameter.TIME_OF_FIRST_PING,
                             r'TG (..../../..,..:..:..) - Time of First Ping ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time of first ping",
                             value_description=ADCPDescription.DATETIME,
                             startup_param=False,
                             direct_access=False,
                             visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict.add(Parameter.TIME_PER_PING,
                             r'TP (\d\d:\d\d.\d\d) \-+ Time per Ping',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time per ping",
                             value_description=ADCPDescription.PINGTIME,
                             startup_param=True,
                             direct_access=True,
                             default_value='00:01.00')

        self._param_dict.add(Parameter.TIME,
                             r'TT (\d\d\d\d/\d\d/\d\d,\d\d:\d\d:\d\d) \- Time Set ',
                             lambda match: str(match.group(1) + " UTC"),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Time",
                             value_description=ADCPDescription.SETTIME,
                             expiration=86400)  # expire once per day 60 * 60 * 24

        self._param_dict.add(Parameter.BUFFERED_OUTPUT_PERIOD,
                             r'TX (\d\d:\d\d:\d\d) \-+ Buffer Output Period:',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Buffered output period",
                             value_description=ADCPDescription.INTERVALTIME,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value='00:00:00')

        self._param_dict.add(Parameter.FALSE_TARGET_THRESHOLD,
                             r'WA (\d+,\d+) \-+ False Target Threshold ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="False target threshold",
                             startup_param=True,
                             direct_access=True,
                             default_value='050,001')

        self._param_dict.add(Parameter.BANDWIDTH_CONTROL,
                             r'WB (\d) \-+ Bandwidth Control ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Bandwidth control",
                             value_description="0=Wid,1=Nar",
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.CORRELATION_THRESHOLD,
                             r'WC (\d+) \-+ Correlation Threshold',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Correlation threshold",
                             startup_param=True,
                             direct_access=True,
                             default_value=64)

        self._param_dict.add(Parameter.SERIAL_OUT_FW_SWITCHES,
                             r'WD ([\d ]+) \-+ Data Out ',
                             lambda match: str(match.group(1)),
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Serial out fw switches",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value='111100000')

        self._param_dict.add(Parameter.ERROR_VELOCITY_THRESHOLD,
                             r'WE (\d+) \-+ Error Velocity Threshold',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Error velocity threshold",
                             units=ADCPUnits.MPERS,
                             startup_param=True,
                             direct_access=True,
                             default_value=2000)

        self._param_dict.add(Parameter.BLANK_AFTER_TRANSMIT,
                             r'WF (\d+) \-+ Blank After Transmit',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Blank after transmit",
                             units=ADCPUnits.CENTIMETER,
                             startup_param=True,
                             direct_access=True,
                             default_value=88)

        self._param_dict.add(Parameter.CLIP_DATA_PAST_BOTTOM,
                             r'WI (\d) \-+ Clip Data Past Bottom',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Clip data past bottom",
                             value_description=ADCPDescription.TRUEON,
                             startup_param=True,
                             direct_access=True,
                             default_value=False)

        self._param_dict.add(Parameter.RECEIVER_GAIN_SELECT,
                             r'WJ (\d) \-+ Rcvr Gain Select \(0=Low,1=High\)',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Receiver gain select",
                             value_description="0=Low,1=High",
                             startup_param=True,
                             direct_access=True,
                             default_value=1)

        self._param_dict.add(Parameter.NUMBER_OF_DEPTH_CELLS,
                             r'WN (\d+) \-+ Number of depth cells',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Number of depth cells",
                             startup_param=True,
                             direct_access=True,
                             default_value=220)

        self._param_dict.add(Parameter.PINGS_PER_ENSEMBLE,
                             r'WP (\d+) \-+ Pings per Ensemble ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Pings per ensemble",
                             startup_param=True,
                             direct_access=True,
                             default_value=1)

        self._param_dict.add(Parameter.SAMPLE_AMBIENT_SOUND,
                             r'WQ (\d) \-+ Sample Ambient Sound',
                             lambda match: bool(int(match.group(1))),
                             int,
                             type=ParameterDictType.BOOL,
                             display_name="Sample ambient sound",
                             value_description=ADCPDescription.TRUEON,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             direct_access=True,
                             default_value=False)

        self._param_dict.add(Parameter.DEPTH_CELL_SIZE,
                             r'WS (\d+) \-+ Depth Cell Size \(cm\)',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Depth cell size",
                             units=ADCPUnits.CENTIMETER,
                             startup_param=True,
                             direct_access=True,
                             default_value=100)

        self._param_dict.add(Parameter.TRANSMIT_LENGTH,
                             r'WT (\d+) \-+ Transmit Length ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Transmit length",
                             units=ADCPUnits.CENTIMETER,
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.PING_WEIGHT,
                             r'WU (\d) \-+ Ping Weighting ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Ping weight",
                             value_description="0=Box,1=Triangle",
                             startup_param=True,
                             direct_access=True,
                             default_value=0)

        self._param_dict.add(Parameter.AMBIGUITY_VELOCITY,
                             r'WV (\d+) \-+ Mode 1 Ambiguity Vel ',
                             lambda match: int(match.group(1)),
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             display_name="Ambiguity velocity",
                             units=ADCPUnits.CMPERSRADIAL,
                             startup_param=True,
                             direct_access=True,
                             default_value=175)

        # Engineering parameters for scheduling
        self._param_dict.add(Parameter.CLOCK_SYNCH_INTERVAL,
                             r'BOGUS',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Clock synch interval",
                             value_description=ADCPDescription.INTERVALTIME,
                             startup_param=True,
                             direct_access=False,
                             default_value="00:00:00")

        # Engineering parameters for scheduling
        self._param_dict.add(Parameter.GET_STATUS_INTERVAL,
                             r'BOGUS',
                             None,
                             str,
                             type=ParameterDictType.STRING,
                             display_name="Get status interval",
                             value_description=ADCPDescription.INTERVALTIME,
                             startup_param=True,
                             direct_access=False,
                             default_value="00:00:00")

        self._param_dict.set_default(Parameter.CLOCK_SYNCH_INTERVAL)
        self._param_dict.set_default(Parameter.GET_STATUS_INTERVAL)

    def _build_param_dict2(self):
        """
        Populate the parameter dictionary with VADCP parameters for slave instrument.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """

        self._param_dict2.add(Parameter2.SERIAL_DATA_OUT,
                              r'CD = (\d\d\d \d\d\d \d\d\d) \-+ Serial Data Out ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Serial data out for 5th beam",
                              value_description=ADCPDescription.SERIALDATAOUT,
                              startup_param=True,
                              direct_access=True,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              default_value='000 000 000')

        self._param_dict2.add(Parameter2.SERIAL_FLOW_CONTROL,
                              r'CF = (\d+) \-+ Flow Ctrl ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Serial flow control for 5th beam",
                              value_description=ADCPDescription.FLOWCONTROL,
                              startup_param=True,
                              direct_access=True,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              default_value='11110')

        self._param_dict2.add(Parameter2.BANNER,
                              r'CH = (\d) \-+ Suppress Banner',
                              lambda match: bool(int(match.group(1))),
                              int,
                              type=ParameterDictType.BOOL,
                              display_name="Banner for 5th beam",
                              value_description=ADCPDescription.TRUEON,
                              startup_param=True,
                              direct_access=True,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              default_value=False)

        self._param_dict2.add(Parameter2.INSTRUMENT_ID,
                              r'CI = (\d+) \-+ Instrument ID ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Instrument id for 5th beam",
                              direct_access=True,
                              startup_param=True,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              default_value=0)

        self._param_dict2.add(Parameter2.SLEEP_ENABLE,
                              r'CL = (\d) \-+ Sleep Enable',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Sleep enable for 5th beam",
                              value_description=ADCPDescription.SLEEP,
                              startup_param=True,
                              direct_access=True,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              default_value=0)

        self._param_dict2.add(Parameter2.SAVE_NVRAM_TO_RECORDER,
                              r'CN = (\d) \-+ Save NVRAM to recorder',
                              lambda match: bool(int(match.group(1))),
                              int,
                              type=ParameterDictType.BOOL,
                              display_name="Save nvram to recorder for 5th beam",
                              value_description=ADCPDescription.TRUEOFF,
                              startup_param=True,
                              default_value=True,
                              direct_access=True,
                              visibility=ParameterDictVisibility.IMMUTABLE)

        self._param_dict2.add(Parameter2.POLLED_MODE,
                              r'CP = (\d) \-+ PolledMode ',
                              lambda match: bool(int(match.group(1))),
                              int,
                              type=ParameterDictType.BOOL,
                              display_name="Polled mode for 5th beam",
                              value_description=ADCPDescription.TRUEON,
                              startup_param=True,
                              direct_access=True,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              default_value=False)

        self._param_dict2.add(Parameter2.XMIT_POWER,
                              r'CQ = (\d+) \-+ Xmt Power ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Xmit power for 5th beam",
                              value_description=ADCPDescription.XMTPOWER,
                              startup_param=True,
                              direct_access=True,
                              default_value=255)

        self._param_dict2.add(Parameter2.LATENCY_TRIGGER,
                              r'CX = (\d) \-+ Trigger Enable ',
                              lambda match: bool(int(match.group(1))),
                              int,
                              type=ParameterDictType.BOOL,
                              display_name="latency trigger for 5th beam",
                              value_description=ADCPDescription.TRUEON,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=False)

        self._param_dict2.add(Parameter2.HEADING_ALIGNMENT,
                              r'EA = ([+-]\d+) \-+ Heading Alignment',
                              lambda match: int(match.group(1)),
                              lambda value: '%+06d' % value,
                              type=ParameterDictType.INT,
                              display_name="Heading alignment for 5th beam",
                              units=ADCPUnits.CDEGREE,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              direct_access=True,
                              startup_param=True,
                              default_value=+00000)

        self._param_dict2.add(Parameter2.HEADING_BIAS,
                              r'EB = ([+-]\d+) \-+ Heading Bias',
                              lambda match: int(match.group(1)),
                              lambda value: '%+06d' % value,
                             type=ParameterDictType.INT,
                              display_name="Heading Bias for 5th beam",
                              units=ADCPUnits.CDEGREE,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=+00000)

        self._param_dict2.add(Parameter2.SPEED_OF_SOUND,
                              r'EC = (\d+) \-+ Speed Of Sound',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Speed of sound for 5th beam",
                              units=ADCPUnits.MPERS,
                              startup_param=True,
                              direct_access=True,
                              default_value=1485)

        self._param_dict2.add(Parameter2.TRANSDUCER_DEPTH,
                              r'ED = (\d+) \-+ Transducer Depth ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Transducer Depth for 5th beam",
                              units=ADCPUnits.DM,
                              startup_param=True,
                              direct_access=True,
                              default_value=2000)

        self._param_dict2.add(Parameter2.PITCH,
                              r'EP = ([+-]\d+) \-+ Tilt 1 Sensor ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Pitch for 5th beam",
                              units=ADCPUnits.CDEGREE,
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.ROLL,
                              r'ER = ([\+\-]\d+) \-+ Tilt 2 Sensor ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Roll for 5th beam",
                              units=ADCPUnits.CDEGREE,
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.SALINITY,
                              r'ES = (\d+) \-+ Salinity ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Salinity for 5th beam",
                              units=ADCPUnits.PPTHOUSAND,
                              startup_param=True,
                              direct_access=True,
                              default_value=35)

        self._param_dict2.add(Parameter2.COORDINATE_TRANSFORMATION,
                              r'EX = (\d+) \-+ Coord Transform ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Coordinate transformation for 5th beam",
                              startup_param=True,
                              direct_access=True,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              default_value='00111')

        self._param_dict2.add(Parameter2.SENSOR_SOURCE,
                              r'EZ = (\d+) \-+ Sensor Source ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Sensor source for 5th beam",
                              startup_param=True,
                              direct_access=True,
                              default_value='1111101')

        self._param_dict2.add(Parameter2.DATA_STREAM_SELECTION,
                              r'PD = (\d+) \-+ Data Stream Select',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Data Stream Selection for 5th beam",
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.SYNC_PING_ENSEMBLE,
                              r'SA = (\d+) \-+ Synch Before',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Synch ping ensemble for 5th beam",
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value='001')

        self._param_dict2.add(Parameter2.RDS3_MODE_SEL,
                              r'SM = (\d+) \-+ Mode Select',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="RDS3 mode selection for 5th beam",
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=2)

        self._param_dict2.add(Parameter2.SLAVE_TIMEOUT,
                              r'ST = (\d+) \-+ Slave Timeout',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Slave timeout for 5th beam",
                              units=ADCPUnits.SECOND,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.SYNCH_DELAY,
                              r'SW = (\d+) \-+ Synch Delay',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Synch delay for 5th beam",
                              units=ADCPUnits.TENTHMILLISECOND,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.ENSEMBLE_PER_BURST,
                              r'TC (\d+) \-+ Ensembles Per Burst',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Ensemble per burst for 5th beam",
                              units=ADCPUnits.ENSEMBLEPERBURST,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.TIME_PER_ENSEMBLE,
                              r'TE (\d\d:\d\d:\d\d.\d\d) \-+ Time per Ensemble ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Time per ensemble for 5th beam",
                              value_description=ADCPDescription.INTERVALTIMEHundredth,
                              startup_param=True,
                              direct_access=True,
                              default_value='00:00:00.00')

        self._param_dict2.add(Parameter2.TIME_OF_FIRST_PING,
                              r'TG (..../../..,..:..:..) - Time of First Ping ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Time of first ping for 5th beam",
                              value_description=ADCPDescription.DATETIME,
                              startup_param=False,
                              direct_access=False,
                              visibility=ParameterDictVisibility.READ_ONLY)

        self._param_dict2.add(Parameter2.TIME,
                              r'TT (\d\d\d\d/\d\d/\d\d,\d\d:\d\d:\d\d) \- Time Set ',
                              lambda match: str(match.group(1) + " UTC"),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Time for 5th beam",
                              value_description=ADCPDescription.SETTIME,
                              expiration=86400)  # expire once per day 60 * 60 * 24

        self._param_dict2.add(Parameter2.BUFFERED_OUTPUT_PERIOD,
                              r'TX (\d\d:\d\d:\d\d) \-+ Buffer Output Period:',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Buffered output period for 5th beam",
                              value_description=ADCPDescription.INTERVALTIME,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value='00:00:00')

        self._param_dict2.add(Parameter2.FALSE_TARGET_THRESHOLD,
                              r'WA (\d+,\d+) \-+ False Target Threshold ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="False target threshold for 5th beam",
                              startup_param=True,
                              direct_access=True,
                              default_value='050,001')

        self._param_dict2.add(Parameter2.BANDWIDTH_CONTROL,
                              r'WB (\d) \-+ Bandwidth Control ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Bandwidth control for 5th beam",
                              value_description="Wid,1=Nar",
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.CORRELATION_THRESHOLD,
                              r'WC (\d+) \-+ Correlation Threshold',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Correlation threshold for 5th beam",
                              startup_param=True,
                              direct_access=True,
                              default_value=64)

        self._param_dict2.add(Parameter2.SERIAL_OUT_FW_SWITCHES,
                              r'WD ([\d ]+) \-+ Data Out ',
                              lambda match: str(match.group(1)),
                              str,
                              type=ParameterDictType.STRING,
                              display_name="Serial out fw switches for 5th beam",
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value='111100000')

        self._param_dict2.add(Parameter2.ERROR_VELOCITY_THRESHOLD,
                              r'WE (\d+) \-+ Error Velocity Threshold',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Error velocity threshold for 5th beam",
                              units=ADCPUnits.MPERS,
                              startup_param=True,
                              direct_access=True,
                              default_value=2000)

        self._param_dict2.add(Parameter2.BLANK_AFTER_TRANSMIT,
                              r'WF (\d+) \-+ Blank After Transmit',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Blank after transmit for 5th beam",
                              units=ADCPUnits.CENTIMETER,
                              startup_param=True,
                              direct_access=True,
                              default_value=83)

        self._param_dict2.add(Parameter2.CLIP_DATA_PAST_BOTTOM,
                              r'WI (\d) \-+ Clip Data Past Bottom',
                              lambda match: bool(int(match.group(1))),
                              int,
                              type=ParameterDictType.BOOL,
                              display_name="Clip data past bottom for 5th beam",
                              value_description=ADCPDescription.TRUEON,
                              startup_param=True,
                              direct_access=True,
                              default_value=False)

        self._param_dict2.add(Parameter2.RECEIVER_GAIN_SELECT,
                              r'WJ (\d) \-+ Rcvr Gain Select \(0=Low,1=High\)',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Receiver gain select for 5th beam",
                              value_description="0=Low,1=High",
                              startup_param=True,
                              direct_access=True,
                              default_value=1)

        self._param_dict2.add(Parameter2.NUMBER_OF_DEPTH_CELLS,
                              r'WN (\d+) \-+ Number of depth cells',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Number of depth cells for 5th beam",
                              startup_param=True,
                              direct_access=True,
                              default_value=220)

        self._param_dict2.add(Parameter2.PINGS_PER_ENSEMBLE,
                              r'WP (\d+) \-+ Pings per Ensemble ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Pings per ensemble for 5th beam",
                              startup_param=True,
                              direct_access=True,
                              default_value=1)

        self._param_dict2.add(Parameter2.SAMPLE_AMBIENT_SOUND,
                              r'WQ (\d) \-+ Sample Ambient Sound',
                              lambda match: bool(int(match.group(1))),
                              int,
                              type=ParameterDictType.BOOL,
                              display_name="Sample ambient sound for 5th beam",
                              value_description=ADCPDescription.TRUEON,
                              visibility=ParameterDictVisibility.IMMUTABLE,
                              startup_param=True,
                              direct_access=True,
                              default_value=False)

        self._param_dict2.add(Parameter2.DEPTH_CELL_SIZE,
                              r'WS (\d+) \-+ Depth Cell Size \(cm\)',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Depth cell size for 5th beam",
                              units=ADCPUnits.CENTIMETER,
                              startup_param=True,
                              direct_access=True,
                              default_value=94)

        self._param_dict2.add(Parameter2.TRANSMIT_LENGTH,
                              r'WT (\d+) \-+ Transmit Length ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Transmit length for 5th beam",
                              units=ADCPUnits.CENTIMETER,
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.PING_WEIGHT,
                              r'WU (\d) \-+ Ping Weighting ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Ping weight for 5th beam",
                              value_description="0=Box,1=Triangle",
                              startup_param=True,
                              direct_access=True,
                              default_value=0)

        self._param_dict2.add(Parameter2.AMBIGUITY_VELOCITY,
                              r'WV (\d+) \-+ Mode 1 Ambiguity Vel ',
                              lambda match: int(match.group(1)),
                              self._int_to_string,
                              type=ParameterDictType.INT,
                              display_name="Ambiguity velocity for 5th beam",
                              units=ADCPUnits.CMPERSRADIAL,
                              startup_param=True,
                              direct_access=True,
                              default_value=175)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @return (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        kwargs['timeout'] = 70

        log.info("SYNCING TIME WITH SENSOR.")
        self._do_cmd_resp(TeledyneInstrumentCmds.SET, TeledyneParameter.TIME,
                          get_timestamp_delayed("%Y/%m/%d, %H:%M:%S"), **kwargs)
        self._do_cmd_resp2(TeledyneInstrumentCmds.SET, TeledyneParameter.TIME,
                           get_timestamp_delayed("%Y/%m/%d, %H:%M:%S"), **kwargs)

        # Save setup to nvram and switch to autosample if successful.
        self._do_cmd_resp(TeledyneInstrumentCmds.SAVE_SETUP_TO_RAM, *args, **kwargs)
        self._do_cmd_resp2(TeledyneInstrumentCmds.SAVE_SETUP_TO_RAM, *args, **kwargs)

        # # Issue start command and switch to autosample if successful.
        self._do_cmd_no_resp(TeledyneInstrumentCmds.START_LOGGING, timeout=TIMEOUT)
        self._do_cmd_no_resp2(TeledyneInstrumentCmds.START_LOGGING, timeout=TIMEOUT)

        return TeledyneProtocolState.AUTOSAMPLE, (ResourceAgentState.STREAMING, None)

    def _handler_get(self, *args, **kwargs):
        """
        Get device parameters from the parameter dict.
        @param args[0] list of parameters to retrieve, or DriverParameter.ALL.
        @throws InstrumentParameterException if missing or invalid parameter.
        """

        result = None
        result2 = None
        error_status = None
        error_status2 = None

        # Grab a baseline time for calculating expiration time.  It is assumed
        # that all data if valid if acquired after this time.
        expire_time = self._param_dict.get_current_timestamp()
        log.trace("expire_time = " + str(expire_time))
        # build a list of parameters we need to get
        param_list = self._get_param_list(*args, **kwargs)
        param_list2 = self._get_param_list2(*args, **kwargs)

        # for master
        try:
            # Take a first pass at getting parameters.  If they are
            # expired an exception will be raised.
            result = self._get_param_result(param_list, expire_time)
        except InstrumentParameterExpirationException as e:
            # In the second pass we need to update parameters, it is assumed
            # that _update_params does everything required to refresh all
            # parameters or at least those that would expire.

            log.trace("in _handler_command_get Parameter expired, refreshing, %s", e)
            if self._is_logging():
                log.trace("I am logging")
                try:
                    # Switch to command mode,
                    self._stop_logging1()
                    self._update_params()
                    # Take a second pass at getting values, this time is should
                    # have all fresh values.
                    log.trace("Fetching parameters for the second time")
                    result = self._get_param_result(param_list, expire_time)
                # Catch all errors, go back to streaming, then rethrow the error
                except Exception as e:
                    error_status = e

                finally:
                    # Switch back to streaming
                    self._start_logging()

                if error_status:
                    raise error_status
            else:
                self._update_params()
                # Take a second pass at getting values, this time is should
                # have all fresh values.
                log.trace("Fetching parameters for the second time")
                result = self._get_param_result(param_list, expire_time)
        # for slave
        try:
            # Take a first pass at getting parameters.  If they are
            # expired an exception will be raised.
            result2 = self._get_param_result2(param_list2, expire_time)
        except InstrumentParameterExpirationException as e:
            # In the second pass we need to update parameters, it is assumed
            # that _update_params does everything required to refresh all
            # parameters or at least those that would expire.

            log.trace("in _handler_command_get Parameter expired, refreshing, %s", e)

            if self._is_logging2():
                log.trace("I am logging2")
                try:
                    # Switch to command mode,
                    self._stop_logging2()

                    self._update_params2()
                    # Take a second pass at getting values, this time is should
                    # have all fresh values.
                    log.trace("Fetching parameters for the second time")
                    result2 = self._get_param_result2(param_list, expire_time)
                # Catch all errors, go back to streaming, then rethrow the error
                except Exception as e:
                    error_status2 = e

                finally:
                    # Switch back to streaming
                    self._start_logging2()

                if error_status2:
                    raise error_status2
            else:
                log.trace("I am not logging")
                self._update_params2()
                # Take a second pass at getting values, this time is should
                # have all fresh values.
                log.trace("Fetching parameters for the second time")
                result2 = self._get_param_result2(param_list2, expire_time)

        # combine the two results
        result.update(result2)
        return None, result

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change
        @return (next_state, result) tuple, (None, (None, )) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        timeout = kwargs.get('timeout', TIMEOUT)

        self._wakeup_connection(self._connection_4Beam, self._promptbuf)
        self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)

        self._sync_clock(TeledyneInstrumentCmds.SET, TeledyneParameter.TIME, timeout, time_format="%Y/%m/%d,%H:%M:%S")
        return None, (None, None)

    def _handler_command_get_calibration(self, *args, **kwargs):
        """
        @param args:
        @param kwargs:
        @return:
        """
        kwargs['timeout'] = 180

        output = self._do_cmd_resp(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
        output2 = self._do_cmd_resp2(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
        result = self._sanitize(base64.b64decode(output))
        result2 = self._sanitize(base64.b64decode(output2))
        result_combined = result + result2
        return None, (None, result_combined)

    def _handler_command_save_setup_to_ram(self, *args, **kwargs):
        """
        save setup to ram.
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.SAVE_SETUP_TO_RAM, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.SAVE_SETUP_TO_RAM, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined

    def _handler_command_get_configuration(self, *args, **kwargs):
        """
        @param args:
        @param kwargs:
        @return:
        """
        kwargs['timeout'] = 180  # long time to get params.
        output = self._do_cmd_resp(TeledyneInstrumentCmds.GET_SYSTEM_CONFIGURATION, *args, **kwargs)
        output2 = self._do_cmd_resp2(TeledyneInstrumentCmds.GET_SYSTEM_CONFIGURATION, *args, **kwargs)
        result = self._sanitize(base64.b64decode(output))
        result2 = self._sanitize(base64.b64decode(output2))
        result_combined = result + result2
        return None, (None, {'result': result_combined})

    def _handler_command_run_test_200(self, *args, **kwargs):
        """
        run test PT200
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.RUN_TEST_200, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.RUN_TEST_200, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined

    def _handler_command_factory_sets(self, *args, **kwargs):
        """
        run Factory set
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.FACTORY_SETS, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.FACTORY_SETS, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined

    def _handler_command_user_sets(self, *args, **kwargs):
        """
        run user set
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.USER_SETS, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.USER_SETS, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined

    def _handler_command_clear_error_status_word(self, *args, **kwargs):
        """
        clear the error status word
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.CLEAR_ERROR_STATUS_WORD, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.CLEAR_ERROR_STATUS_WORD, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined

    def _handler_command_acquire_error_status_word(self, *args, **kwargs):
        """
        read the error status word
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.DISPLAY_ERROR_STATUS_WORD, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.DISPLAY_ERROR_STATUS_WORD, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined
        # return (next_state, result)

    def _handler_command_display_fault_log(self, *args, **kwargs):
        """
        display the error log.
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.GET_FAULT_LOG, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.GET_FAULT_LOG, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined

    def _handler_command_clear_fault_log(self, *args, **kwargs):
        """
        clear the error log.
        """
        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND
        result = self._do_cmd_resp(TeledyneInstrumentCmds.CLEAR_FAULT_LOG, *args, **kwargs)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.CLEAR_FAULT_LOG, *args, **kwargs)
        result_combined = result + result2
        return None, result_combined

    def _handler_autosample_init_params(self, *args, **kwargs):
        """
        initialize parameters.  For this instrument we need to
        put the instrument into command mode, apply the changes
        then put it back.
        """
        if self._init_type != InitializationType.NONE:

            error_status = None

            try:
                log.trace("stopping logging without checking")
                self._stop_logging()
                self._init_params()

            except Exception as e:
                error_status = e

            finally:
                # Switch back to streaming
                log.trace("starting logging")
                self._start_logging()
                self._start_logging2()

            if error_status:
                log.error("Error in _handler_autosample_init_params: %s", error_status)
                raise error_status

        return None, None

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return (next_state, result) tuple, (ProtocolState.COMMAND,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command misunderstood or
        incorrect prompt received.
        """

        # Wake up the device, continuing until autosample prompt seen.
        timeout = kwargs.get('timeout', TIMEOUT)

        self._send_break(duration=1000)
        self._send_break2(duration=1000)
        self._send_break(duration=1000)
        self._send_break2(duration=1000)

        #self._stop_logging(timeout)

        return TeledyneProtocolState.COMMAND, (ResourceAgentState.COMMAND, None)

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
        if TeledyneParameter.CLOCK_SYNCH_INTERVAL in params:
            if (params[TeledyneParameter.CLOCK_SYNCH_INTERVAL] != self._param_dict.get(
                    TeledyneParameter.CLOCK_SYNCH_INTERVAL)):
                self._param_dict.set_value(TeledyneParameter.CLOCK_SYNCH_INTERVAL,
                                           params[TeledyneParameter.CLOCK_SYNCH_INTERVAL])
                self.start_scheduled_job(TeledyneParameter.CLOCK_SYNCH_INTERVAL, TeledyneScheduledJob.CLOCK_SYNC,
                                         TeledyneProtocolEvent.SCHEDULED_CLOCK_SYNC)
                changed = True

        if TeledyneParameter.GET_STATUS_INTERVAL in params:
            if (params[TeledyneParameter.GET_STATUS_INTERVAL] != self._param_dict.get(
                    TeledyneParameter.GET_STATUS_INTERVAL)):
                self._param_dict.set_value(TeledyneParameter.GET_STATUS_INTERVAL,
                                           params[TeledyneParameter.GET_STATUS_INTERVAL])
                self.start_scheduled_job(TeledyneParameter.GET_STATUS_INTERVAL,
                                         TeledyneScheduledJob.GET_CONFIGURATION,
                                         TeledyneProtocolEvent.SCHEDULED_GET_STATUS)
                changed = True
        if changed:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        self._set_params(params, startup)
        self._set_params2(params, startup)

        return None, None

    def _handler_autosample_clock_sync(self, *args, **kwargs):
        """
        execute a clock sync on the leading edge of a second change from
        autosample mode.  For this command we have to move the instrument
        into command mode, do the clock sync, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @return (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        error_status = None

        logging = False
        logging2 = False

        self._promptbuf = ""
        self._linebuf = ""

        self._promptbuf2 = ""
        self._linebuf2 = ""

        if self._is_logging():
            logging = True
            # Switch to command mode,
            self._stop_logging()

        if self._is_logging2():
            logging2 = True
            # Switch to command mode,
            self._stop_logging2()

        try:
            # Sync the clock
            timeout = kwargs.get('timeout', TIMEOUT)

            self._sync_clock(TeledyneInstrumentCmds.SET, TeledyneParameter.TIME, timeout,
                             time_format="%Y/%m/%d,%H:%M:%S")

        # Catch all error so we can put ourself back into
        # streaming.  Then rethrow the error
        except Exception as e:
            error_status = e

        finally:
            # Switch back to streaming
            if logging:
                self._start_logging()
            if logging2:
                self._start_logging2()

        if error_status:
            raise error_status

        return None, (None, None)

    def _handler_command_get_status(self, *args, **kwargs):
        """
        execute a get status on the leading edge of a second change
        @return next_state, (next_agent_state, result) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        try:
            self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
            self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)

            self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_PT2, *args, **kwargs)
            self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_PT2, *args, **kwargs)

            self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_PT4, *args, **kwargs)
            self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_PT4, *args, **kwargs)

        except Exception as e:
            log.error("Exception on Executing do_cmd_no_resp() %s", e)
            raise e

        return None, (None, None)

    def _handler_autosample_get_calibration(self, *args, **kwargs):
        """
        execute a get calibration from autosample mode.
        For this command we have to move the instrument
        into command mode, get calibration, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @return next_state, (next_agent_state, result) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """
        error_status = None
        output = ""
        output2 = ""

        try:
            # Switch to command mode,
            self._stop_logging(*args, **kwargs)
            self._stop_logging2(*args, **kwargs)

            kwargs['timeout'] = 180
            output = self._do_cmd_resp(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
            output2 = self._do_cmd_resp2(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            error_status = e

        finally:
            # Switch back to streaming
            self._start_logging()
            self._start_logging2()

        if error_status:
            raise error_status

        result = self._sanitize(base64.b64decode(output))
        result2 = self._sanitize(base64.b64decode(output2))
        result_combined = result + ", " + result2

        return None, (None, result_combined)

    def _handler_autosample_get_status(self, *args, **kwargs):
        """
        execute a get status on the leading edge of a second change
        @return (next_state, result) tuple, (None, (None, )) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """

        logging = False
        logging2 = False

        # for master
        if self._is_logging():
            logging = True
            # Switch to command mode,
            self._stop_logging()

        # for slave
        if self._is_logging2():
            logging2 = True
            # Switch to command mode,
            self._stop_logging2()

        try:
            # for master
            self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
            self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_PT2, *args, **kwargs)
            self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_PT4, *args, **kwargs)

            # for slave
            self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
            self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_PT2, *args, **kwargs)
            self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_PT4, *args, **kwargs)

        except Exception as e:
            log.error("Unknown driver parameter on _do_cmd_no_resp in handle_autosample_get_status. Exception thrown")
            raise InstrumentParameterException(
                'Unknown driver parameter on _do_cmd_no_resp in handle_autosample_get_status. Exception :' + str(e))

        finally:
            # Switch back to streaming
            if logging:
                self._start_logging()
            if logging2:
                self._start_logging2()

        return None, (None, None)

    def _handler_autosample_get_configuration(self, *args, **kwargs):
        """
        execute a get configuration from autosample mode.
        For this command we have to move the instrument
        into command mode, get configuration, then switch back.  If an
        exception is thrown we will try to get ourselves back into
        streaming and then raise that exception.
        @return (next_state, result) tuple, (ProtocolState.AUTOSAMPLE,
        None) if successful.
        @throws InstrumentTimeoutException if device cannot be woken for command.
        @throws InstrumentProtocolException if command could not be built or misunderstood.
        """

        next_state = None
        next_agent_state = None
        error_status = None

        output = ""
        output2 = ""

        try:
            # Switch to command mode,
            self._stop_logging(*args, **kwargs)
            self._stop_logging2(*args, **kwargs)

            # Sync the clock
            output = self._do_cmd_resp(TeledyneInstrumentCmds.GET_SYSTEM_CONFIGURATION, *args, **kwargs)
            output2 = self._do_cmd_resp2(TeledyneInstrumentCmds.GET_SYSTEM_CONFIGURATION, *args, **kwargs)

        # Catch all error so we can put ourselves back into
        # streaming.  Then rethrow the error
        except Exception as e:
            error_status = e

        finally:
            # Switch back to streaming
            self._start_logging()
            self._start_logging2()

        if error_status:
            raise error_status

        result = self._sanitize(base64.b64decode(output))
        result2 = self._sanitize(base64.b64decode(output2))
        result_combined = result + result2

        return next_state, (next_agent_state, result_combined)

    def _handler_direct_access_exit(self, *args, **kwargs):
        """
        Exit direct access state.
        """
        self._send_break()
        self._send_break2()

        result = self._do_cmd_resp(TeledyneInstrumentCmds.GET, TeledyneParameter.TIME_OF_FIRST_PING)
        result2 = self._do_cmd_resp2(TeledyneInstrumentCmds.GET, TeledyneParameter.TIME_OF_FIRST_PING)
        if "****/**/**,**:**:**" not in result:
            log.error("TG not allowed to be set. sending a break to clear it.")

            self._send_break()

        if "****/**/**,**:**:**" not in result2:
            log.error("TG not allowed to be set. sending a break2 to clear it.")

            self._send_break2()

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        Handle acquire status by executing AC, PT2 and PT4
        """
        next_state = None

        kwargs['timeout'] = 70
        kwargs['expected_prompt'] = TeledynePrompt.COMMAND

        self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
        self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_PT2, *args, **kwargs)
        self._do_cmd_no_resp(TeledyneInstrumentCmds.OUTPUT_PT4, *args, **kwargs)

        self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_CALIBRATION_DATA, *args, **kwargs)
        self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_PT2, *args, **kwargs)
        self._do_cmd_no_resp2(TeledyneInstrumentCmds.OUTPUT_PT4, *args, **kwargs)

        return next_state, None

    def _wakeup(self, timeout=3, delay=1):
        """
        Overridden method.
        Send a wakeup to both connections
        """
        self._wakeup_connection(self._connection_4Beam, self._promptbuf)
        self._wakeup_connection(self._connection_5thBeam, self._promptbuf2)

    def _wakeup_connection(self, connection=None, promptbuf=None):
        """
        Clear buffers and sends a wake-up command to the instrument
        """

        promptbuf = ''
        timeout = 3

        starttime = time.time()
        endtime = starttime + float(timeout)

        log.trace('Sending wakeup. timeout=%s' % timeout)
        self._send_wakeup(connection)
        while time.time() < endtime:
            time.sleep(0.05)
            for item in self._get_prompts():
                index = promptbuf.find(item)
                if index >= 0:
                    log.trace('!!!!wakeup got prompt: %s' % repr(item))
                    return item
        return None