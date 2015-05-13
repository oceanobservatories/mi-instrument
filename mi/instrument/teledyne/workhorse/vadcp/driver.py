import copy
from mi.core.instrument.data_particle import RawDataParticle
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility

from mi.core.log import get_logger, get_logging_metaclass
from mi.instrument.teledyne.workhorse.particles import AdcpCompassCalibrationDataParticle, AdcpPd0ParsedDataParticle, \
    AdcpAncillarySystemDataParticle, AdcpTransmitPathParticle
from mi.instrument.teledyne.workhorse.particles import AdcpSystemConfigurationDataParticle

log = get_logger()

from mi.core.common import BaseEnum
from mi.core.exceptions import InstrumentConnectionException, InstrumentParameterException, InstrumentProtocolException, \
    InstrumentTimeoutException
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver, DriverConnectionState, \
    ResourceAgentEvent, DriverAsyncEvent, ConfigMetadataKey, DriverParameter, DriverConfigKey, ResourceAgentState
from mi.core.instrument.port_agent_client import PortAgentClient
from mi.instrument.teledyne.workhorse.driver import WorkhorseParameter, parameter_defaults, WorkhorsePrompt, NEWLINE, \
    parameter_regexes, parameter_extractors, parameter_formatters, parameter_types, parameter_names, \
    parameter_descriptions, parameter_startup, parameter_direct, parameter_visibility, parameter_units, \
    WorkhorseProtocol, WorkhorseProtocolState, WorkhorseInstrumentCmds, WorkhorseProtocolEvent, \
    ADCP_COMPASS_CALIBRATION_REGEX_MATCHER, ADCP_PD0_PARSED_REGEX_MATCHER, ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER, \
    ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER, ADCP_TRANSMIT_PATH_REGEX_MATCHER, WorkhorseEngineeringParameter

master_parameter_defaults = copy.deepcopy(parameter_defaults)
slave_parameter_defaults = copy.deepcopy(parameter_defaults)

master_parameter_defaults[WorkhorseParameter.TRANSDUCER_DEPTH] = 2000
master_parameter_defaults[WorkhorseParameter.RDS3_MODE_SEL] = 1
master_parameter_defaults[WorkhorseParameter.SYNCH_DELAY] = 100
master_parameter_defaults[WorkhorseParameter.BLANK_AFTER_TRANSMIT] = 88
master_parameter_defaults[WorkhorseParameter.NUMBER_OF_DEPTH_CELLS] = 220
master_parameter_defaults[WorkhorseParameter.DEPTH_CELL_SIZE] = 100

slave_parameter_defaults[WorkhorseParameter.TRANSDUCER_DEPTH] = 2000
slave_parameter_defaults[WorkhorseParameter.RDS3_MODE_SEL] = 2
slave_parameter_defaults[WorkhorseParameter.SYNCH_DELAY] = 100
slave_parameter_defaults[WorkhorseParameter.BLANK_AFTER_TRANSMIT] = 83
slave_parameter_defaults[WorkhorseParameter.NUMBER_OF_DEPTH_CELLS] = 220
slave_parameter_defaults[WorkhorseParameter.DEPTH_CELL_SIZE] = 94


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


class VadcpPd0ParsedDataParticle(AdcpPd0ParsedDataParticle):
    _data_particle_type = "VADCP"
    _slave = True


class VadcpInstrumentDriver(SingleConnectionInstrumentDriver):
    __metaclass__ = get_logging_metaclass(log_level='trace')

    def __init__(self, evt_callback):
        """
        InstrumentDriver constructor.
        @param evt_callback Driver process event callback.
        """
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)

        # multiple portAgentClient
        self._connection = {}

    def apply_startup_params(self):
        self._protocol.apply_startup_params()
        self._protocol.apply_startup_params2()

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = VadcpMasterInstrumentProtocol(WorkhorsePrompt, NEWLINE, self._driver_event)

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
            self._protocol._connection = self._connection[SlaveProtocol.FOURBEAM]
        except InstrumentConnectionException as e:
            log.error("Connection Exception Beam 1-4: %s", e)
            log.error("Instrument Driver remaining in disconnected state.")
            raise

        # for Slave
        try:
            self._connection[SlaveProtocol.FIFTHBEAM].init_comms(self._protocol._slave.got_data,
                                                                 self._protocol._slave.got_raw,
                                                                 self._got_config,
                                                                 self._got_exception,
                                                                 self._lost_connection_callback)
            self._protocol._slave._connection = self._connection[SlaveProtocol.FIFTHBEAM]

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

                except (TypeError, KeyError):
                    raise InstrumentParameterException('Invalid comms config dict..')
        return connections


class VadcpSlaveInstrumentProtocol(WorkhorseProtocol):
    def _build_param_dict(self):
        for param in parameter_regexes:
            # Scheduled events are handled by the master
            if WorkhorseEngineeringParameter.has(param):
                continue
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
                                 default_value=slave_parameter_defaults.get(param),
                                 units=parameter_units.get(param))

    def publish_raw(self, port_agent_packet):
        """
        Publish raw data
        @param: port_agent_packet port agent packet containing raw
        """
        particle = RawDataParticle5(port_agent_packet.get_as_dict(),
                                    port_timestamp=port_agent_packet.get_timestamp())

        parsed_sample = particle.generate()
        if self._driver_event:
            self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """
        self._extract_sample(VadcpCompassCalibrationDataParticle,
                             ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VadcpPd0ParsedDataParticle,
                             ADCP_PD0_PARSED_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VadcpSystemConfigurationDataParticle5,
                             ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VadcpAncillarySystemDataParticle,
                             ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VadcpTransmitPathParticle,
                             ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                             chunk,
                             timestamp)

    def _handler_command_recover_autosample(self):
        log.error('PD0 sample detected in COMMAND in VADCP (unallowed) sending break')
        self._send_break()
        return None, (None, None)


class VadcpMasterInstrumentProtocol(WorkhorseProtocol):
    def __init__(self, prompts, newline, driver_event):
        super(VadcpMasterInstrumentProtocol, self).__init__(prompts, newline, driver_event)
        self._slave = VadcpSlaveInstrumentProtocol(WorkhorsePrompt, NEWLINE, self._driver_event)

    def _build_param_dict(self):
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

        self._param_dict.set_default(WorkhorseParameter.CLOCK_SYNCH_INTERVAL)
        self._param_dict.set_default(WorkhorseParameter.GET_STATUS_INTERVAL)

    def get_config_metadata_dict(self):
        """
        Return a list of metadata about the protocol's driver support,
        command formats, and parameter formats. The format should be easily
        JSONifyable (as will happen in the driver on the way out to the agent)
        @retval A python dict that represents the metadata
        @see https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+Instrument+Driver-Agent+parameter+and+command+metadata+exchange
        """
        return_dict = super(VadcpMasterInstrumentProtocol, self).get_config_metadata_dict()
        for k, v in self._slave._param_dict.generate_dict().iteritems():
            return_dict[ConfigMetadataKey.PARAMETERS][k + '_5th'] = v

        return return_dict

    def _get_param_list(self, *args, **kwargs):
        try:
            param_list = args[0]
        except IndexError:
            raise InstrumentParameterException('Parameter required, none specified')

        if(isinstance(param_list, str)):
            param_list = [param_list]
        elif(not isinstance(param_list, (list, tuple))):
            raise InstrumentParameterException("Expected a list, tuple or a string")

        if DriverParameter.ALL in param_list:
            return self._param_dict.get_keys(), ['%s_5th' % x for x in self._param_dict.get_keys()]

        master_params = []
        slave_params = []

        for param in param_list:
            if '5th' in param:
                slave_params.append(param.replace('_5th', ''))
            else:
                master_params.append(param)

        for param in set(master_params + slave_params):
            if param not in self._param_dict:
                raise InstrumentParameterException('Unknown parameter: %s' % param)

        return master_params, slave_params

    def _handler_get(self, *args, **kwargs):
        # build a list of parameters we need to get
        master_params, slave_params = self._get_param_list(*args, **kwargs)
        expire_time = self._param_dict.get_current_timestamp()
        try:
            master_result = self._get_param_result(master_params, expire_time)
            slave_result = self._slave._get_param_result(slave_params, expire_time)
        except InstrumentParameterException:
            self._update_params()
            master_result = self._get_param_result(master_params, expire_time)
            slave_result = self._slave._get_param_result(slave_params, expire_time)

        for k, v in slave_result.iteritems():
            master_result['%s_5th' % k] = v

        return master_result

    def get_resource_capabilities(self, current_state=True):
        """
        """
        res_cmds = self._protocol_fsm.get_events(current_state)
        res_cmds = self._filter_capabilities(res_cmds)
        res_params = self._param_dict.get_keys() + [x.replace('_5th', '') for x in self._slave._param_dict.get_keys()]

        return [res_cmds, res_params]

    def set_init_params(self, config):
        """
        Set the initialization parameters to the given values in the protocol
        parameter dictionary.
        @param config The parameter_name/value to set in the initialization
            fields of the parameter dictionary
        @raise InstrumentParameterException If the config cannot be set
        """
        if not isinstance(config, dict):
            raise InstrumentParameterException("Invalid init config format")

        self._startup_config = config

        param_config = config.get(DriverConfigKey.PARAMETERS)
        if param_config:
            for name in param_config.keys():
                log.debug("Setting init value for %s to %s", name, param_config[name])
                if '_5th' in name:
                    self._slave._param_dict.set_init_value(name.replace('_5th', ''), param_config[name])
                else:
                    self._param_dict.set_init_value(name, param_config[name])

    def get_startup_config(self):
        return_dict = {}
        start_list = self._param_dict.get_keys()
        start_list_slave = self._slave._param_dict.get_keys()

        for param in start_list:
            result = self._param_dict.get_config_value(param)
            if result is not None:
                return_dict[param] = result
            elif self._param_dict.is_startup_param(param):
                raise InstrumentProtocolException("Required startup value not specified: %s" % param)

        for param in start_list_slave:
            name = param + '_5th'
            result = self._slave._param_dict.get_config_value(param)
            if result is not None:
                return_dict[name] = result
            elif self._slave._param_dict.is_startup_param(param):
                raise InstrumentProtocolException("Required startup value not specified: %s" % param)

        return return_dict

    def get_cached_config(self):
        master_config = self._param_dict.get_config()
        slave_config = self._slave._param_dict.get_config()

        for k, v in slave_config.iteritems():
            master_config[k + '_5th'] = v

        return master_config

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.
        Pass it to extract_sample with the appropriate particle
        objects and REGEXes.
        """
        self._extract_sample(AdcpCompassCalibrationDataParticle,
                             ADCP_COMPASS_CALIBRATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VadcpPd0BeamParsedDataParticle,
                             ADCP_PD0_PARSED_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(VadcpSystemConfigurationDataParticle,
                             ADCP_SYSTEM_CONFIGURATION_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(AdcpAncillarySystemDataParticle,
                             ADCP_ANCILLARY_SYSTEM_DATA_REGEX_MATCHER,
                             chunk,
                             timestamp)

        self._extract_sample(AdcpTransmitPathParticle,
                             ADCP_TRANSMIT_PATH_REGEX_MATCHER,
                             chunk,
                             timestamp)

    def _split_params(self, parameters, filter_engineering=False):
        if type(parameters) is list:
            master_p = [p for p in parameters if '_5th' not in p]
            slave_p = [p.replace('_5th', '') for p in parameters if '_5th' in p]

            if filter_engineering:
                # filter out the engineering parameters and ALL
                master_p = [p for p in master_p if not WorkhorseEngineeringParameter.has(p)]
                slave_p = [p for p in slave_p if not WorkhorseEngineeringParameter.has(p)]

            return master_p, slave_p

        elif type(parameters) is dict:
            master_p = {k:v for k,v in parameters.iteritems() if '_5th' not in k}
            slave_p = {k.replace('_5th', ''):v for k,v in parameters.iteritems() if '_5th' in k}

            if filter_engineering:
                # filter out the engineering parameters and ALL
                master_p = {k:v for k,v in master_p.iteritems() if not WorkhorseEngineeringParameter.has(k)}
                slave_p = {k:v for k,v in slave_p.iteritems() if not WorkhorseEngineeringParameter.has(k)}

            return master_p, slave_p

    def _update_params(self, *args, **kwargs):

        # see if we passed in a list of parameters to query
        # if not, use the whole parameter list
        parameters = kwargs.get('params')
        if parameters is None or WorkhorseParameter.ALL in parameters:
            parameters = WorkhorseParameter.list()

        master_p, slave_p = self._split_params(parameters, filter_engineering=True)
        super(VadcpMasterInstrumentProtocol, self)._update_params(master_p, *args, **kwargs)
        self._slave._update_params(slave_p, *args, **kwargs).iteritems()

    def _set_params(self, *args, **kwargs):
        parameters = args[0]
        master_p, slave_p = self._split_params(parameters)

        super(VadcpMasterInstrumentProtocol, self)._set_params(master_p, *args, **kwargs)
        self._slave._set_params(master_p, *args, **kwargs)

    ########################################################################
    # UNKNOWN handlers.
    ########################################################################

    def _handler_unknown_discover(self, *args, **kwargs):
        protocol_state, agent_state = self._discover()

        slave_protocol_state, slave_agent_state = self._slave._discover()

        # states match, go to that state
        if protocol_state == slave_protocol_state:
            if protocol_state == WorkhorseProtocolState.COMMAND:
                agent_state = ResourceAgentState.IDLE
            return protocol_state, agent_state

        # state mismatch, bring master and slave to COMMAND
        self._send_break()
        self._slave._send_break()

        return WorkhorseProtocolState.COMMAND, ResourceAgentState.IDLE

    ########################################################################
    # COMMAND handlers.
    ########################################################################

    def _handler_command_run_test_200(self, *args, **kwargs):
        result = super(VadcpMasterInstrumentProtocol, self)._handler_command_run_test_200(*args, **kwargs)[1][1]
        result += self._slave._handler_command_run_test_200(*args, **kwargs)[1][1]
        return None, (None, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @return next_state, (next_agent_state, result) if successful.
        """
        try:
            self._slave._protocol_fsm.on_event(WorkhorseProtocolEvent.START_AUTOSAMPLE)
            super(VadcpMasterInstrumentProtocol, self)._handler_command_start_autosample(*args, **kwargs)

        except InstrumentTimeoutException:
            self._send_break()
            self._slave._send_break()

            if self._slave.get_current_state() == WorkhorseProtocolState.AUTOSAMPLE:
                self._slave._protocol_fsm.on_event(WorkhorseProtocolEvent.STOP_AUTOSAMPLE)

            raise

        next_state = WorkhorseProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        return next_state, (next_agent_state, None)

    def _handler_command_clock_sync(self, *args, **kwargs):
        super(VadcpMasterInstrumentProtocol, self)._handler_command_clock_sync(*args, **kwargs)
        self._slave._handler_command_clock_sync(*args, **kwargs)
        return None, (None, None)

    def _handler_command_acquire_status(self, *args, **kwargs):
        super(VadcpMasterInstrumentProtocol, self)._handler_command_acquire_status(*args, **kwargs)
        self._slave._handler_command_acquire_status(*args, **kwargs)

    def _handler_command_start_direct(self, *args, **kwargs):
        self._slave._protocol_fsm.on_event(WorkhorseProtocolEvent.START_DIRECT)
        next_state = WorkhorseProtocolState.DIRECT_ACCESS
        next_agent_state = ResourceAgentState.DIRECT_ACCESS
        return next_state, (next_agent_state, None)

    def _handler_command_recover_autosample(self):
        log.error('PD0 sample detected in COMMAND in VADCP (unallowed) sending break')
        self._send_break()
        return None, (None, None)

    ######################################################
    # AUTOSAMPLE handlers
    ######################################################

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        self._slave._protocol_fsm.on_event(WorkhorseProtocolEvent.STOP_AUTOSAMPLE)
        return super(VadcpMasterInstrumentProtocol, self)._handler_autosample_stop_autosample(*args, **kwargs)

    def _handler_autosample_clock_sync(self, *args, **kwargs):
        self._slave._handler_autosample_clock_sync(*args, **kwargs)
        return super(VadcpMasterInstrumentProtocol, self)._handler_autosample_clock_sync(*args, **kwargs)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        self._slave._handler_autosample_acquire_status(*args, **kwargs)
        return super(VadcpMasterInstrumentProtocol, self)._handler_autosample_acquire_status(*args, **kwargs)

    def _handler_direct_access_execute_direct(self, data):
        try:
            target, message = data.split(':', 1)
            if target not in ['45']:
                raise ValueError()

            if target == '4':
                super(VadcpMasterInstrumentProtocol, self)._handler_direct_access_execute_direct(message)
            else:
                self._slave._handler_direct_access_execute_direct(message)
        except ValueError:
            raise InstrumentProtocolException('Direct access commands for VADCP must be preceded by 4: or 5:')
