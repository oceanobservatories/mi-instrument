from contextlib import contextmanager
import copy
import functools
import time
import re

from mi.core.log import get_logger
log = get_logger()

from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import RawDataParticle
from mi.core.instrument.instrument_protocol import RE_PATTERN, DEFAULT_CMD_TIMEOUT, DEFAULT_WRITE_DELAY
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility, ProtocolParameterDict
from mi.core.time_tools import get_timestamp_delayed
from mi.core.util import dict_equal
from mi.core.common import BaseEnum, InstErrorCode
from mi.core.exceptions import InstrumentConnectionException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.exceptions import InstrumentException
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.instrument.instrument_driver import ResourceAgentEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.port_agent_client import PortAgentClient

from mi.instrument.teledyne.workhorse.particles import AdcpCompassCalibrationDataParticle
from mi.instrument.teledyne.workhorse.particles import AdcpPd0ParsedDataParticle
from mi.instrument.teledyne.workhorse.particles import AdcpAncillarySystemDataParticle
from mi.instrument.teledyne.workhorse.particles import AdcpTransmitPathParticle
from mi.instrument.teledyne.workhorse.particles import AdcpSystemConfigurationDataParticle

from mi.instrument.teledyne.workhorse.driver import WorkhorseParameter
from mi.instrument.teledyne.workhorse.driver import WorkhorsePrompt
from mi.instrument.teledyne.workhorse.driver import NEWLINE
from mi.instrument.teledyne.workhorse.driver import parameter_regexes
from mi.instrument.teledyne.workhorse.driver import parameter_extractors
from mi.instrument.teledyne.workhorse.driver import parameter_formatters
from mi.instrument.teledyne.workhorse.driver import parameter_defaults
from mi.instrument.teledyne.workhorse.driver import parameter_types
from mi.instrument.teledyne.workhorse.driver import parameter_names
from mi.instrument.teledyne.workhorse.driver import parameter_descriptions
from mi.instrument.teledyne.workhorse.driver import parameter_startup
from mi.instrument.teledyne.workhorse.driver import parameter_direct
from mi.instrument.teledyne.workhorse.driver import parameter_visibility
from mi.instrument.teledyne.workhorse.driver import parameter_units, WorkhorseProtocol
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocolState
from mi.instrument.teledyne.workhorse.driver import WorkhorseInstrumentCmds
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocolEvent
from mi.instrument.teledyne.workhorse.driver import ADCP_COMPASS_CALIBRATION_REGEX_MATCHER
from mi.instrument.teledyne.workhorse.driver import ADCP_PD0_PARSED_REGEX_MATCHER
from mi.instrument.teledyne.workhorse.driver import ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER
from mi.instrument.teledyne.workhorse.driver import ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER
from mi.instrument.teledyne.workhorse.driver import ADCP_TRANSMIT_PATH_REGEX_MATCHER
from mi.instrument.teledyne.workhorse.driver import WorkhorseEngineeringParameter
from mi.instrument.teledyne.workhorse.driver import TIMEOUT
from mi.instrument.teledyne.workhorse.driver import WorkhorseScheduledJob

master_parameter_defaults = copy.deepcopy(parameter_defaults)
slave_parameter_defaults = copy.deepcopy(parameter_defaults)

master_parameter_defaults[WorkhorseParameter.TRANSDUCER_DEPTH] = 2000
master_parameter_defaults[WorkhorseParameter.RDS3_MODE_SEL] = 1
master_parameter_defaults[WorkhorseParameter.SYNCH_DELAY] = 100
master_parameter_defaults[WorkhorseParameter.BLANK_AFTER_TRANSMIT] = 88
master_parameter_defaults[WorkhorseParameter.NUMBER_OF_DEPTH_CELLS] = 220
master_parameter_defaults[WorkhorseParameter.DEPTH_CELL_SIZE] = 100
master_parameter_defaults[WorkhorseParameter.TIME_PER_PING] = '00:01.00'

slave_parameter_defaults[WorkhorseParameter.TRANSDUCER_DEPTH] = 2000
slave_parameter_defaults[WorkhorseParameter.RDS3_MODE_SEL] = 2
slave_parameter_defaults[WorkhorseParameter.SYNCH_DELAY] = 100
slave_parameter_defaults[WorkhorseParameter.BLANK_AFTER_TRANSMIT] = 83
slave_parameter_defaults[WorkhorseParameter.NUMBER_OF_DEPTH_CELLS] = 220
slave_parameter_defaults[WorkhorseParameter.DEPTH_CELL_SIZE] = 94
slave_parameter_defaults[WorkhorseParameter.TIME_PER_PING] = '00:00.00'


class SlaveProtocol(BaseEnum):
    """
    The protocol needs to have 2 connections, 4Beam(Master) and 5thBeam(Slave)
    """
    FOURBEAM = '4Beam'
    FIFTHBEAM = '5thBeam'


class RawDataParticle5(RawDataParticle):
    _data_particle_type = "raw_5thbeam"


class VadcpCompassCalibrationDataParticle(AdcpCompassCalibrationDataParticle):
    _data_particle_type = "vadcp_5thbeam_compass_calibration"


class VadcpSystemConfigurationDataParticle(AdcpSystemConfigurationDataParticle):
    _data_particle_type = "vadcp_4beam_system_configuration"
    _master = True


class VadcpPd0BeamParsedDataParticle(AdcpPd0ParsedDataParticle):
    _data_particle_type = "vadcp_pd0_beam_parsed"
    _master = True


class VadcpSystemConfigurationDataParticle5(AdcpSystemConfigurationDataParticle):
    _data_particle_type = "vadcp_5thbeam_system_configuration"
    _slave = True
    _offset = 6


class VadcpAncillarySystemDataParticle(AdcpAncillarySystemDataParticle):
    _data_particle_type = "vadcp_ancillary_system_data"


class VadcpTransmitPathParticle(AdcpTransmitPathParticle):
    _data_particle_type = "vadcp_transmit_path"


class VadcpPd0SlaveDataParticle(AdcpPd0ParsedDataParticle):
    _data_particle_type = "VADCP"
    _slave = True


class InstrumentDriver(SingleConnectionInstrumentDriver):
    def __init__(self, evt_callback, refdes):
        """
        InstrumentDriver constructor.
        @param evt_callback Driver process event callback.
        """
        SingleConnectionInstrumentDriver.__init__(self, evt_callback, refdes)

        # multiple portAgentClient
        self._connection = {}

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(WorkhorsePrompt, NEWLINE, self._driver_event,
                                  connections=[SlaveProtocol.FOURBEAM, SlaveProtocol.FIFTHBEAM])

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
            got_data = functools.partial(self._protocol.got_data, connection=SlaveProtocol.FOURBEAM)
            got_raw = functools.partial(self._protocol.got_raw, connection=SlaveProtocol.FOURBEAM)
            self._connection[SlaveProtocol.FOURBEAM].init_comms(got_data,
                                                                got_raw,
                                                                self._got_config,
                                                                self._got_exception,
                                                                self._lost_connection_callback)
            self._protocol.connections[SlaveProtocol.FOURBEAM] = self._connection[SlaveProtocol.FOURBEAM]
        except InstrumentConnectionException as e:
            log.error("Connection Exception Beam 1-4: %s", e)
            log.error("Instrument Driver remaining in disconnected state.")
            raise

        # for Slave
        try:
            got_data = functools.partial(self._protocol.got_data, connection=SlaveProtocol.FIFTHBEAM)
            got_raw = functools.partial(self._protocol.got_raw, connection=SlaveProtocol.FIFTHBEAM)
            self._connection[SlaveProtocol.FIFTHBEAM].init_comms(got_data,
                                                                 got_raw,
                                                                 self._got_config,
                                                                 self._got_exception,
                                                                 self._lost_connection_callback)
            self._protocol.connections[SlaveProtocol.FIFTHBEAM] = self._connection[SlaveProtocol.FIFTHBEAM]

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
                        connections[name] = PortAgentClient(addr, port, cmd_port)
                    else:
                        raise InstrumentParameterException('Invalid comms config dict in build_connections.')

                except (TypeError, KeyError) as e:
                    raise InstrumentParameterException('Invalid comms config dict.. %r' % e)
        return connections


class Protocol(WorkhorseProtocol):
    def __init__(self, prompts, newline, driver_event, connections=None):
        """
        Constructor.
        @param prompts Enum class containing possible device prompts used for
        command response logic.
        @param newline The device newline.
        @driver_event The callback for asynchronous driver events.
        """
        if not type(connections) is list:
            raise InstrumentProtocolException('Unable to instantiate multi connection protocol without connection list')
        self._param_dict2 = ProtocolParameterDict()

        # Construct superclass.
        WorkhorseProtocol.__init__(self, prompts, newline, driver_event)

        # Create multiple connection versions of the pieces of protocol involving data to/from the instrument
        self._linebuf = {connection: '' for connection in connections}
        self._promptbuf = {connection: '' for connection in connections}
        self._last_data_timestamp = {connection: None for connection in connections}
        self.connections = {connection: None for connection in connections}
        self.chunkers = {connection: StringChunker(self.sieve_function) for connection in connections}

    def _get_response(self, timeout=10, expected_prompt=None, response_regex=None, connection=None):
        """
        Overridden to handle multiple port agent connections
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')
        # Grab time for timeout and wait for prompt.
        end_time = time.time() + timeout

        if response_regex and not isinstance(response_regex, RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        if expected_prompt is None:
            prompt_list = self._get_prompts()
        else:
            if isinstance(expected_prompt, str):
                prompt_list = [expected_prompt]
            else:
                prompt_list = expected_prompt

        if response_regex is None:
            pattern = None
        else:
            pattern = response_regex.pattern

        log.debug('_get_response: timeout=%s, prompt_list=%s, expected_prompt=%r, response_regex=%r, promptbuf=%r',
                  timeout, prompt_list, expected_prompt, pattern, self._promptbuf)
        while time.time() < end_time:
            if response_regex:
                # noinspection PyArgumentList
                match = response_regex.search(self._linebuf[connection])
                if match:
                    return match.groups()
            else:
                for item in prompt_list:
                    index = self._promptbuf[connection].find(item)
                    if index >= 0:
                        result = self._promptbuf[connection][0:index + len(item)]
                        return item, result

            time.sleep(.1)

        raise InstrumentTimeoutException("in InstrumentProtocol._get_response()")

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Overridden to handle multiple port agent connections
        """
        connection = kwargs.get('connection')
        if connection is None:
            raise InstrumentProtocolException('_do_cmd_resp: no connection supplied!')

        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', DEFAULT_CMD_TIMEOUT)
        expected_prompt = kwargs.get('expected_prompt', None)
        response_regex = kwargs.get('response_regex', None)

        if response_regex and not isinstance(response_regex, RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        self._do_cmd_no_resp(cmd, *args, **kwargs)

        # Wait for the prompt, prepare result and return, timeout exception
        if response_regex:
            prompt = ""
            result_tuple = self._get_response(timeout,
                                              connection=connection,
                                              response_regex=response_regex,
                                              expected_prompt=expected_prompt)
            result = "".join(result_tuple)
        else:
            (prompt, result) = self._get_response(timeout,
                                                  connection=connection,
                                                  expected_prompt=expected_prompt)

        resp_handler = self._response_handlers.get((self.get_current_state(), cmd),
                                                   self._response_handlers.get(cmd, None))
        resp_result = None
        if callable(resp_handler):
            resp_result = resp_handler(result, prompt)

        return resp_result

    def _send_data(self, data, write_delay=0, connection=None):
        if connection is None:
            raise InstrumentProtocolException('_send_data: no connection supplied!')

        if write_delay == 0:
            self.connections[connection].send(data)
        else:
            for char in data:
                self.connections[connection].send(char)
                time.sleep(write_delay)

    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        Overridden to handle multiple port agent connections
        """
        connection = kwargs.get('connection')
        if connection is None:
            raise InstrumentProtocolException('_do_cmd_no_resp: no connection supplied! %r %r %r' % (cmd, args, kwargs))

        timeout = kwargs.get('timeout', DEFAULT_CMD_TIMEOUT)
        write_delay = kwargs.get('write_delay', DEFAULT_WRITE_DELAY)

        build_handler = self._build_handlers.get(cmd, None)
        if not callable(build_handler):
            log.error('_do_cmd_no_resp: no handler for command: %s' % cmd)
            raise InstrumentProtocolException(error_code=InstErrorCode.BAD_DRIVER_COMMAND)
        cmd_line = build_handler(cmd, *args)

        # Wakeup the device, timeout exception as needed
        self._wakeup(timeout, connection=connection)

        # Clear line and prompt buffers for result, then send command.
        self._linebuf[connection] = ''
        self._promptbuf[connection] = ''
        self._send_data(cmd_line, write_delay, connection=connection)

    def _do_cmd_direct(self, cmd, connection=None):
        """
        Issue an untranslated command to the instrument. No response is handled
        as a result of the command.

        @param cmd The high level command to issue
        """
        # Send command.
        self._send_data(cmd, connection=connection)

    ########################################################################
    # Incoming data (for parsing) callback.
    ########################################################################
    def got_data(self, port_agent_packet, connection=None):
        """
        Called by the instrument connection when data is available.
        Append line and prompt buffers.

        Also add data to the chunker and when received call got_chunk
        to publish results.
        """
        if connection is None:
            raise InstrumentProtocolException('got_data: no connection supplied!')

        data_length = port_agent_packet.get_data_length()
        data = port_agent_packet.get_data()
        timestamp = port_agent_packet.get_timestamp()

        log.debug("Got Data: %r %r", connection, data)
        log.debug("Add Port Agent Timestamp: %r %s", connection, timestamp)

        if data_length > 0:
            if self.get_current_state() == DriverProtocolState.DIRECT_ACCESS:
                self._driver_event(DriverAsyncEvent.DIRECT_ACCESS, data)

            self.add_to_buffer(data, connection=connection)

            self.chunkers[connection].add_chunk(data, timestamp)
            (timestamp, chunk) = self.chunkers[connection].get_next_data()
            while chunk:
                self._got_chunk(chunk, timestamp, connection=connection)
                (timestamp, chunk) = self.chunkers[connection].get_next_data()

    ########################################################################
    # Incoming raw data callback.
    ########################################################################
    def got_raw(self, port_agent_packet, connection=None):
        """
        Called by the port agent client when raw data is available, such as data
        sent by the driver to the instrument, the instrument responses,etc.
        """
        self.publish_raw(port_agent_packet, connection)

    def publish_raw(self, port_agent_packet, connection=None):
        """
        Publish raw data
        @param: port_agent_packet port agent packet containing raw
        """
        if connection == SlaveProtocol.FOURBEAM:
            particle_class = RawDataParticle
        else:
            particle_class = RawDataParticle5
        particle = particle_class(port_agent_packet.get_as_dict(),
                                  port_timestamp=port_agent_packet.get_timestamp())

        if self._driver_event:
            self._driver_event(DriverAsyncEvent.SAMPLE, particle.generate())

    def add_to_buffer(self, data, connection=None):
        """
        Add a chunk of data to the internal data buffers
        buffers implemented as lifo ring buffer
        @param data: bytes to add to the buffer
        """
        # Update the line and prompt buffers.
        self._linebuf[connection] += data
        self._promptbuf[connection] += data
        self._last_data_timestamp[connection] = time.time()

        # If our buffer exceeds the max allowable size then drop the leading
        # characters on the floor.
        if len(self._linebuf[connection]) > self._max_buffer_size():
            self._linebuf[connection] = self._linebuf[connection][self._max_buffer_size() * -1:]

        # If our buffer exceeds the max allowable size then drop the leading
        # characters on the floor.
        if len(self._promptbuf[connection]) > self._max_buffer_size():
            self._promptbuf[connection] = self._promptbuf[connection][self._max_buffer_size() * -1:]

        log.debug("LINE BUF: %r", self._linebuf[connection][-50:])
        log.debug("PROMPT BUF: %r", self._promptbuf[connection][-50:])

    ########################################################################
    # Wakeup helpers.
    ########################################################################

    def _send_wakeup(self, connection=None):
        """
        Send a wakeup to the device. Overridden by device specific
        subclasses.
        """
        self.connections[connection].send(NEWLINE)

    def _wakeup(self, timeout, delay=1, connection=None):
        """
        Clear buffers and send a wakeup command to the instrument
        @param timeout The timeout to wake the device.
        @param delay The time to wait between consecutive wakeups.
        @throw InstrumentTimeoutException if the device could not be woken.
        """
        if connection is None:
            raise InstrumentProtocolException('_wakeup: no connection supplied!')

        # Clear the prompt buffer.
        log.trace("clearing promptbuf: %r", self._promptbuf)
        self._promptbuf[connection] = ''

        # Grab time for timeout.
        starttime = time.time()

        while True:
            # Send a line return and wait a sec.
            log.trace('Sending wakeup. timeout=%s', timeout)
            self._send_wakeup(connection=connection)
            time.sleep(delay)

            log.trace("Prompts: %s", self._get_prompts())

            for item in self._get_prompts():
                log.trace("buffer: %r", self._promptbuf[connection])
                log.trace("find prompt: %r", item)
                index = self._promptbuf[connection].find(item)
                log.trace("Got prompt (index: %s): %r ", index, self._promptbuf[connection])
                if index >= 0:
                    log.trace('wakeup got prompt: %r', item)
                    return item
            log.trace("Searched for all prompts")

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in _wakeup()")

    def _build_param_dict(self):
        # We're going to build two complete sets of ADCP parameters here
        # one set for the master instrument and one for the slave
        for param in parameter_regexes:
            self._param_dict.add(param,
                                 parameter_regexes.get(param),
                                 parameter_extractors.get(param),
                                 parameter_formatters.get(param),
                                 type=parameter_types.get(param),
                                 display_name=parameter_names.get(param),
                                 value_description=parameter_descriptions.get(param),
                                 startup_param=parameter_startup.get(param, False),
                                 direct_access=parameter_direct.get(param, False),
                                 visibility=parameter_visibility.get(param, ParameterDictVisibility.READ_WRITE),
                                 default_value=master_parameter_defaults.get(param),
                                 units=parameter_units.get(param))

        for param in parameter_regexes:
            # Scheduled events are handled by the master
            if WorkhorseEngineeringParameter.has(param):
                continue
            self._param_dict.add(param + '_5th',
                                 r'DONTMATCHMEIMNOTREAL!',
                                 parameter_extractors.get(param),
                                 parameter_formatters.get(param),
                                 type=parameter_types.get(param),
                                 display_name=parameter_names.get(param),
                                 value_description=parameter_descriptions.get(param),
                                 startup_param=parameter_startup.get(param, False),
                                 direct_access=parameter_direct.get(param, False),
                                 visibility=parameter_visibility.get(param, ParameterDictVisibility.READ_WRITE),
                                 default_value=slave_parameter_defaults.get(param),
                                 units=parameter_units.get(param))

        self._param_dict.set_default(WorkhorseParameter.CLOCK_SYNCH_INTERVAL)
        self._param_dict.set_default(WorkhorseParameter.GET_STATUS_INTERVAL)

        # now we're going to build a whole 'nother param dict for the slave parameters
        # that contain regex values so we can fill them in easily...
        for param in parameter_regexes:
            # Scheduled events are handled by the master
            if WorkhorseEngineeringParameter.has(param):
                continue
            self._param_dict2.add(param + '_5th',
                                  parameter_regexes.get(param),
                                  parameter_extractors.get(param),
                                  parameter_formatters.get(param))

    # #######################################################################
    # Private helpers.
    # #######################################################################
    def _got_chunk(self, chunk, timestamp, connection=None):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """
        if connection == SlaveProtocol.FIFTHBEAM:
            if self._extract_sample(VadcpCompassCalibrationDataParticle,
                                    ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                # if self.get_current_state() == WorkhorseProtocolState.COMMAND:
                #     self._async_raise_fsm_event(WorkhorseProtocolEvent.RECOVER_AUTOSAMPLE)
                return

            if self._extract_sample(VadcpPd0SlaveDataParticle,
                                    ADCP_PD0_PARSED_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

            if self._extract_sample(VadcpSystemConfigurationDataParticle5,
                                    ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

            if self._extract_sample(VadcpAncillarySystemDataParticle,
                                    ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

            if self._extract_sample(VadcpTransmitPathParticle,
                                    ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

        elif connection == SlaveProtocol.FOURBEAM:
            if self._extract_sample(AdcpCompassCalibrationDataParticle,
                                    ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                # if self.get_current_state() == WorkhorseProtocolState.COMMAND:
                #     self._async_raise_fsm_event(WorkhorseProtocolEvent.RECOVER_AUTOSAMPLE)
                return

            if self._extract_sample(VadcpPd0BeamParsedDataParticle,
                                    ADCP_PD0_PARSED_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

            if self._extract_sample(VadcpSystemConfigurationDataParticle,
                                    ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

            if self._extract_sample(AdcpAncillarySystemDataParticle,
                                    ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

            if self._extract_sample(AdcpTransmitPathParticle,
                                    ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                                    chunk,
                                    timestamp):
                return

    def _send_break_cmd(self, delay, connection=None):
        """
        Send a BREAK to attempt to wake the device.
        """
        self.connections[connection].send_break(delay)

    def _sync_clock(self, command, date_time_param, timeout=TIMEOUT, delay=1, time_format="%Y/%m/%d,%H:%M:%S"):
        """
        Send the command to the instrument to synchronize the clock
        @param command set command
        @param date_time_param: date time parameter that we want to set
        @param timeout: command timeout
        @param delay: wakeup delay
        @param time_format: time format string for set command
        """
        log.info("SYNCING TIME WITH SENSOR.")
        for connection in self.connections:
            self._do_cmd_resp(command, date_time_param, get_timestamp_delayed("%Y/%m/%d, %H:%M:%S"),
                              timeout=timeout, connection=connection)

    # #######################################################################
    # Startup parameter handlers
    ########################################################################

    def _get_params(self, parameters, connection):
        command = NEWLINE.join(['%s?' % p for p in parameters]) + NEWLINE

        if len(parameters) > 1:
            regex = re.compile(r'(%s.*?%s.*?>)' % (parameters[0], parameters[-1]), re.DOTALL)
        else:
            regex = re.compile(r'(%s.*?>)' % parameters[0], re.DOTALL)

        self._linebuf[connection] = ''
        self._promptbuf[connection] = ''
        self._do_cmd_direct(command, connection=connection)
        return self._get_response(response_regex=regex, connection=connection)

    def _update_params(self, *args, **kwargs):
        """
        Update the parameter dictionary.
        """
        # see if we passed in a list of parameters to query
        # if not, use the whole parameter list
        parameters = kwargs.get('params')
        if parameters is None or WorkhorseParameter.ALL in parameters:
            parameters = self._param_dict.get_keys()
        # filter out the engineering parameters and ALL
        parameters = [p for p in parameters if not WorkhorseEngineeringParameter.has(p) and p != WorkhorseParameter.ALL]

        # Get old param dict config.
        old_config = self._param_dict.get_config()

        if parameters:
            # MASTER
            master_params = [p for p in parameters if '_5th' not in p]
            if master_params:
                resp = self._get_params(master_params, SlaveProtocol.FOURBEAM)
                self._param_dict.update_many(resp)

            # SLAVE
            slave_params = [p.replace('_5th', '') for p in parameters if '_5th' in p]
            if slave_params:
                resp = self._get_params(slave_params, SlaveProtocol.FIFTHBEAM)
                self._param_dict2.update_many(resp)
                for key, value in self._param_dict2.get_all().iteritems():
                    self._param_dict.set_value(key, value)

        new_config = self._param_dict.get_config()

        # Check if there is any changes. Ignore TT
        if not dict_equal(new_config, old_config, ['TT']) or kwargs.get('force'):
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _execute_set_params(self, commands, connection):
        if commands:
            # we are going to send the concatenation of all our set commands
            self._linebuf[connection] = ''
            self._do_cmd_direct(''.join(commands), connection=connection)
            # we'll need to build a regular expression to retrieve all of the responses
            # including any possible errors
            if len(commands) == 1:
                regex = re.compile(r'(%s.*?)\r\n>' % commands[-1].strip(), re.DOTALL)
            else:
                regex = re.compile(r'(%s.*?%s.*?)\r\n>' % (commands[0].strip(), commands[-1].strip()), re.DOTALL)
            response = self._get_response(response_regex=regex, connection=connection)
            self._parse_set_response(response[0], None)

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        """
        self._verify_not_readonly(*args, **kwargs)
        params = args[0]
        changed = []

        old_config = self._param_dict.get_config()

        master_commands = []
        slave_commands = []
        for key, val in params.iteritems():
            if WorkhorseEngineeringParameter.has(key):
                continue
            if val != old_config.get(key):
                changed.append(key)
                if '_5th' in key:
                    slave_commands.append(self._build_set_command(
                        WorkhorseInstrumentCmds.SET, key.replace('_5th', ''), val))
                else:
                    master_commands.append(self._build_set_command(WorkhorseInstrumentCmds.SET, key, val))

        self._execute_set_params(master_commands, connection=SlaveProtocol.FOURBEAM)
        self._execute_set_params(slave_commands, connection=SlaveProtocol.FIFTHBEAM)

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

    def _send_break(self, duration=1000, connection=None):
        """
        Send a BREAK to attempt to wake the device.
        """
        self._linebuf[connection] = ''
        self._promptbuf[connection] = ''
        self._send_break_cmd(duration, connection=connection)
        self._get_response(expected_prompt=WorkhorsePrompt.BREAK, connection=connection)

    def _start_logging(self, timeout=TIMEOUT, connection=None):
        """
        Command the instrument to start logging
        @param timeout: how long to wait for a prompt
        @throws: InstrumentProtocolException if failed to start logging
        """
        try:
            start = WorkhorseInstrumentCmds.START_LOGGING
            # start the slave first, it collects on signal from master
            self._do_cmd_resp(start, timeout=timeout, connection=SlaveProtocol.FIFTHBEAM)
            self._do_cmd_resp(start, timeout=timeout, connection=SlaveProtocol.FOURBEAM)
        except InstrumentException:
            self._stop_logging()
            raise

    def _stop_logging(self):
        # stop the master first (slave only collects on signal from master)
        self._send_break(connection=SlaveProtocol.FOURBEAM)
        self._send_break(connection=SlaveProtocol.FIFTHBEAM)

    def _discover(self, connection=None):
        """
        Discover current state; can be COMMAND or AUTOSAMPLE or UNKNOWN.
        @return (next_protocol_state, next_agent_state)
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentStateException if the device response does not correspond to
        an expected state.
        """
        states = set()
        command = (WorkhorseProtocolState.COMMAND, ResourceAgentState.COMMAND)
        auto = (WorkhorseProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING)
        for connection in self.connections:
            try:
                self._wakeup(3, connection=connection)
                states.add(command)
            except InstrumentException:
                states.add(auto)

        if len(states) == 1:
            # states match, return this state
            return states.pop()

        # states don't match
        self._stop_logging()
        return command

    def _run_test(self, *args, **kwargs):
        kwargs['timeout'] = 30
        kwargs['expected_prompt'] = WorkhorsePrompt.COMMAND
        result = []
        for connection in self.connections:
            result.append(connection)
            kwargs['connection'] = connection
            result.append(self._do_cmd_resp(WorkhorseInstrumentCmds.RUN_TEST_200, *args, **kwargs))
        return NEWLINE.join(result)

    @contextmanager
    def _pause_logging(self):
        try:
            self._stop_logging()
            yield
        finally:
            self._start_logging()

    ########################################################################
    # COMMAND handlers.
    ########################################################################

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        execute a get status
        @return next_state, (next_agent_state, result) if successful.
        @throws InstrumentProtocolException from _do_cmd_resp.
        """
        a = super(Protocol, self)._handler_command_acquire_status(connection=SlaveProtocol.FOURBEAM)
        b = super(Protocol, self)._handler_command_acquire_status(connection=SlaveProtocol.FIFTHBEAM)
        return None, (None, a[1][1] + b[1][1])

    def _handler_command_recover_autosample(self):
        log.info('PD0 sample detected in COMMAND, not allowed in VADCP. Sending break')
        self._stop_logging()

    ######################################################
    # DIRECT_ACCESS handlers
    ######################################################

    def _handler_direct_access_execute_direct(self, data):
        next_state = None
        result = None
        next_agent_state = None
        self._do_cmd_direct(data)

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)
        return next_state, (next_agent_state, result)
