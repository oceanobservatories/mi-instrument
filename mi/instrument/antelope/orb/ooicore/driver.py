import ntplib
import cPickle as pickle
from threading import Lock

from mi.core.driver_scheduler import DriverSchedulerConfigKey, TriggerType
from mi.core.exceptions import InstrumentProtocolException, InstrumentParameterException
from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.port_agent_client import PortAgentPacket
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility, ParameterDictType
from mi.core.log import get_logger
from mi.instrument.antelope.orb.ooicore.packet_log import PacketLog, GapException


log = get_logger()

from mi.core.common import BaseEnum, Units
from mi.core.persistent_store import PersistentStoreDict

from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver, DriverConfigKey
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import ResourceAgentState

from mi.core.instrument.instrument_protocol import InstrumentProtocol
from mi.core.instrument.instrument_fsm import ThreadSafeFSM


ORBOLDEST = -13


class ProtocolState(BaseEnum):
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    STOPPING = 'DRIVER_STATE_STOPPING'
    WRITE_ERROR = 'DRIVER_STATE_WRITE_ERROR'


class ProtocolEvent(BaseEnum):
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    DISCOVER = DriverEvent.DISCOVER
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    FLUSH = 'PROTOCOL_EVENT_FLUSH'
    CLEAR_WRITE_ERROR = 'PROTOCOL_EVENT_CLEAR_WRITE_ERROR'


class Capability(BaseEnum):
    DISCOVER = DriverEvent.DISCOVER
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    CLEAR_WRITE_ERROR = ProtocolEvent.CLEAR_WRITE_ERROR


class Parameter(BaseEnum):
    REFDES = 'refdes'
    SOURCE_REGEX = 'source_regex'
    START_PKTID = 'start_pktid'
    FLUSH_INTERVAL = 'flush_interval'
    DB_ADDR = 'database_address'
    DB_PORT = 'database_port'
    FILE_LOCATION = 'file_location'


class ScheduledJob(BaseEnum):
    FLUSH = 'flush'


class AntelopeDataParticles(BaseEnum):
    METADATA = 'antelope_metadata'


class AntelopeMetadataParticleKey(BaseEnum):
    NET = 'antelope_network'
    STATION = 'antelope_station'
    LOCATION = 'antelope_location'
    CHANNEL = 'antelope_channel'
    START = 'antelope_starttime'
    END = 'antelope_endtime'
    RATE = 'antelope_sampling_rate'
    NSAMPS = 'antelope_num_samples'
    FILENAME = 'filepath'
    UUID = 'uuid'


class AntelopeMetadataParticle(DataParticle):
    _data_particle_type = AntelopeDataParticles.METADATA

    def __init__(self, raw_data, **kwargs):
        super(AntelopeMetadataParticle, self).__init__(raw_data, **kwargs)
        self.set_internal_timestamp(unix_time=raw_data.header.starttime)

    def _build_parsed_values(self):
        header = self.raw_data.header
        pk = AntelopeMetadataParticleKey
        return [
            self._encode_value(pk.NET, header.net, str),
            self._encode_value(pk.STATION, header.station, str),
            self._encode_value(pk.LOCATION, header.location, str),
            self._encode_value(pk.CHANNEL, header.channel, str),
            self._encode_value(pk.START, ntplib.system_to_ntp_time(header.starttime), float),
            self._encode_value(pk.END, ntplib.system_to_ntp_time(header.endtime), float),
            self._encode_value(pk.RATE, header.rate, int),
            self._encode_value(pk.NSAMPS, header.num_samples, int),
            self._encode_value(pk.FILENAME, self.raw_data.relname, str),
            self._encode_value(pk.UUID, self.raw_data.bin_uuid, str),
        ]


class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    Generic antelope instrument driver
    """
    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(self._driver_event)


# noinspection PyMethodMayBeStatic,PyUnusedLocal
class Protocol(InstrumentProtocol):
    def __init__(self, driver_event):
        super(Protocol, self).__init__(driver_event)
        self._protocol_fsm = ThreadSafeFSM(ProtocolState, ProtocolEvent,
                                           ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        handlers = {
            ProtocolState.UNKNOWN: (
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.EXIT, self._handler_unknown_exit),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ),
            ProtocolState.COMMAND: (
                (ProtocolEvent.ENTER, self._handler_command_enter),
                (ProtocolEvent.EXIT, self._handler_command_exit),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.SET, self._handler_set),
                (ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample),
            ),
            ProtocolState.AUTOSAMPLE: (
                (ProtocolEvent.ENTER, self._handler_autosample_enter),
                (ProtocolEvent.EXIT, self._handler_autosample_exit),
                (ProtocolEvent.GET, self._handler_get),
                (ProtocolEvent.FLUSH, self._flush),
                (ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample),
            ),
            ProtocolState.STOPPING: (
                (ProtocolEvent.ENTER, self._handler_stopping_enter),
                (ProtocolEvent.EXIT, self._handler_stopping_exit),
                (ProtocolEvent.FLUSH, self._flush),
            ),
            ProtocolState.WRITE_ERROR: (
                (ProtocolEvent.ENTER, self._handler_write_error_enter),
                (ProtocolEvent.EXIT, self._handler_write_error_exit),
                (ProtocolEvent.CLEAR_WRITE_ERROR, self._handler_clear_write_error),
            )}

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Build dictionaries for driver schema
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)
        self._logs = {}
        self._filled_logs = []
        self._pickle_cache = []

        # persistent store, cannot initialize until startup config has been applied
        # since we need the address for postgres
        self._persistent_store = None

        # lock for flush actions to prevent writing or altering the data files
        # during flush
        self._lock = Lock()
        self._pktid = 0

    def _filter_capabilities(self, events):
        """
        Filter a list of events to only include valid capabilities
        @param events: list of events to be filtered
        @return: list of filtered events
        """
        return [x for x in events if Capability.has(x)]

    def _build_command_dict(self):
        """
        Populate the command dictionary with commands.
        """
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name="Start Autosample")
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, display_name="Stop Autosample")
        self._cmd_dict.add(Capability.GET, display_name="Get")
        self._cmd_dict.add(Capability.SET, display_name="Set")
        self._cmd_dict.add(Capability.DISCOVER, display_name="Discover")
        self._cmd_dict.add(Capability.CLEAR_WRITE_ERROR, display_name="Clear Write Error")

    def _build_param_dict(self):
        self._param_dict.add(Parameter.REFDES,
                             'NA',
                             str,
                             str,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             display_name='Reference Designator',
                             description='Reference Designator for this driver',
                             type=ParameterDictType.STRING)
        self._param_dict.add(Parameter.SOURCE_REGEX,
                             'NA',
                             str,
                             str,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             display_name='Source Filter Regex',
                             description='Filter sources to be processed from the ORB',
                             type=ParameterDictType.STRING,
                             value_description='Regular expression')
        self._param_dict.add(Parameter.FLUSH_INTERVAL,
                             'NA',
                             str,
                             str,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             display_name='Flush Interval',
                             description='Interval after which all records are flushed to disk',
                             type=ParameterDictType.INT,
                             value_description='Interval, in seconds',
                             units=Units.SECOND)
        self._param_dict.add(Parameter.DB_ADDR,
                             'NA',
                             str,
                             str,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             default_value='localhost',
                             display_name='Database Address',
                             description='Postgres database IP address or hostname',
                             type=ParameterDictType.STRING,
                             value_description='IP address or hostname')
        self._param_dict.add(Parameter.DB_PORT,
                             'NA',
                             str,
                             str,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             default_value=5432,
                             display_name='Database Port',
                             description='Postgres database port number',
                             type=ParameterDictType.INT,
                             value_description='Integer port number (default 5432)')
        self._param_dict.add(Parameter.FILE_LOCATION,
                             'NA',
                             str,
                             str,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             startup_param=True,
                             default_value="./antelope_data",
                             display_name='File Location',
                             description='Root file path of the packet data files',
                             type=ParameterDictType.STRING,
                             value_description='String representing the packet data root file path')

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, False)

    def _build_persistent_dict(self):
        name = 'antelope'
        refdes = self._param_dict.get(Parameter.REFDES)
        host = self._param_dict.get(Parameter.DB_ADDR)
        port = self._param_dict.get(Parameter.DB_PORT)

        self._persistent_store = PersistentStoreDict(name, refdes, host=host, port=port)
        if 'pktid' not in self._persistent_store:
            self._persistent_store['pktid'] = ORBOLDEST

    def _handler_set(self, *args, **kwargs):
        pass

    def _update_params(self, *args, **kwargs):
        pass

    def _set_params(self, *args, **kwargs):
        """
        Set various parameters
        @param args: arglist, should contain a dictionary of parameters/values to be set
        """
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)

        old_config = self._param_dict.get_config()

        # all constraints met or no constraints exist, set the values
        for key, value in params.iteritems():
            self._param_dict.set_value(key, value)

        new_config = self._param_dict.get_config()

        if not old_config == new_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        # Set the base directory for the packet data file location.
        PacketLog.base_dir = self._param_dict.get(Parameter.FILE_LOCATION)

    def _flush(self):
        log.info('flush')
        particles = []
        with self._lock:
            log.info('got lock')

            # On the last flush, close all the bins.
            last_flush = self.get_current_state() == ProtocolState.STOPPING
            if last_flush:
                self._filled_logs.extend(self._logs.values())
                self._logs = {}

            for _log in self._logs.itervalues():
                try:
                    _log.flush()
                except InstrumentProtocolException as ex:
                    # Ensure the current logs are clear to prevent residual data from being flushed.
                    self._driver_event(DriverAsyncEvent.ERROR, ex)
                    self._logs = {}
                    self._filled_logs = []
                    return ProtocolState.WRITE_ERROR, (ProtocolState.WRITE_ERROR, None)

                particles.append(AntelopeMetadataParticle(_log, preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP))

            for _log in self._filled_logs:
                try:
                    _log.flush()
                except InstrumentProtocolException as ex:
                    # Ensure the current logs are clear to prevent residual data from being flushed.
                    self._driver_event(DriverAsyncEvent.ERROR, ex)
                    self._logs = {}
                    self._filled_logs = []
                    return ProtocolState.WRITE_ERROR, (ProtocolState.WRITE_ERROR, None)

                particles.append(AntelopeMetadataParticle(_log, preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP))
                _log.data = []

            self._filled_logs = []
            log.info('updating persistent store')
            self._persistent_store['pktid'] = self._pktid

        for particle in particles:
            self._driver_event(DriverAsyncEvent.SAMPLE, particle.generate())

        if last_flush:
            self.stop_scheduled_job(ScheduledJob.FLUSH)
            return ProtocolState.COMMAND, (ProtocolState.COMMAND, None)

        return None, (None, None)

    # noinspection PyProtectedMember
    def _orbstart(self):
        self._connection._command_port_agent('orbselect %s' % self._param_dict.get(Parameter.SOURCE_REGEX))
        self._connection._command_port_agent('orbseek %s' % self._persistent_store['pktid'])
        self._connection._command_port_agent('orbstart')

    # noinspection PyProtectedMember
    def _orbstop(self):
        self._connection._command_port_agent('orbstop')

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
            seconds = int(val)
        except ValueError:
            raise InstrumentParameterException('Bad interval. Cannot parse %r as integer' % val)

        if seconds > 0:
            config = {
                DriverConfigKey.SCHEDULER: {
                    schedule_job: {
                        DriverSchedulerConfigKey.TRIGGER: {
                            DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.INTERVAL,
                            DriverSchedulerConfigKey.SECONDS: seconds
                        }
                    }
                }
            }
            self.set_init_params(config)
            self._add_scheduler_event(schedule_job, protocol_event)

    def got_data(self, port_agent_packet):
        data_length = port_agent_packet.get_data_length()
        data_type = port_agent_packet.get_header_type()

        if data_type == PortAgentPacket.PICKLED_FROM_INSTRUMENT:
            self._pickle_cache.append(port_agent_packet.get_data())
            # this is the max size (65535) minus the header size (16)
            # any packet of this length will be followed by one or more packets
            # with additional data. Keep accumulating packets until we have
            # the complete data, then unpickle.
            if data_length != 65519:
                data = pickle.loads(''.join(self._pickle_cache))
                self._pickle_cache = []
                self._bin_data(data)
        else:
            raise InstrumentProtocolException('Received unpickled data from port agent')

    def got_raw(self, port_agent_packet):
        pass

    def _get_bin(self, packet):
        rate_map = {
            1: 86400,       # 1 day
            8: 86400,       # 1 day
            40: 86400,      # 1 day
            200: 86400,     # 1 day
            64000: 60 * 5,  # 5 minutes
            256000: 60,     # 1 minute
        }
        start_time = packet['time']
        rate = packet['samprate']
        bin_size = rate_map.get(rate, 60)
        bin_value = int(start_time/bin_size)
        bin_start = bin_value * bin_size
        bin_end = (bin_value + 1) * bin_size

        return bin_start, bin_end

    def _bin_data(self, packet):
        key = '%s.%s.%s.%s' % (packet['net'], packet.get('location', ''),
                               packet.get('sta', ''), packet['chan'])
        start, end = self._get_bin(packet)

        with self._lock:
            self._pktid = packet['pktid']

            if key not in self._logs:
                self._logs[key] = PacketLog.from_packet(packet, end, self._param_dict.get(Parameter.REFDES))

            try:
                while True:
                    packet = self._logs[key].add_packet(packet)
                    if packet is None:
                        break
                    # residual, we need a new bin
                    # log is complete, move to holding list until next flush
                    self._filled_logs.append(self._logs[key])
                    del self._logs[key]
                    # create the new log...
                    start, end = self._get_bin(packet)
                    self._logs[key] = PacketLog.from_packet(packet, end, self._param_dict.get(Parameter.REFDES))

            except GapException:
                # non-contiguous data detected, close this log and open a new one
                self._filled_logs.append(self._logs[key])
                del self._logs[key]
                # create the new log
                self._logs[key] = PacketLog.from_packet(packet, end, self._param_dict.get(Parameter.REFDES))
                self._logs[key].add_packet(packet)

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
        Discover current state; always COMMAND.
        @return protocol_state, protocol_state
        """
        return ProtocolState.COMMAND, ProtocolState.COMMAND

    ########################################################################
    # COMMAND handlers.
    ########################################################################

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        @throws InstrumentTimeoutException if the device cannot be woken.
        @throws InstrumentProtocolException if the update commands and not recognized.
        """
        self._init_params()
        # We can't build the persistent dict until parameters are applied, so build it here
        if self._persistent_store is None:
            self._build_persistent_dict()
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_exit(self, *args, **kwargs):
        """
        Exit command state.
        """

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        Switch into autosample mode.
        @return next_state, (next_state, result) if successful.
        """
        result = None

        # Ensure the current logs are clear to prevent residual data from being flushed.
        self._logs = {}
        self._filled_logs = []

        self._orbstart()
        next_state = ProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        return next_state, (next_state, result)

    ######################################################
    # AUTOSAMPLE handlers
    ######################################################

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        self.start_scheduled_job(Parameter.FLUSH_INTERVAL, ScheduledJob.FLUSH, ProtocolEvent.FLUSH)
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        """
        Exit autosample state.
        """
        self._orbstop()

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return  next_state, (next_state, result) if successful.
        """
        self._orbstop()

        result = None
        next_state = ProtocolState.STOPPING
        next_agent_state = None

        return next_state, (next_state, result)

    ######################################################
    # STOPPING handlers
    ######################################################

    def _handler_stopping_enter(self, *args, **kwargs):
        """
        Enter stopping state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_stopping_exit(self, *args, **kwargs):
        """
        Exit stopping state.
        Stop the scheduled flush job and schedule flush one more time and
        indicate that it is the last flush before stopping auto sampling.
        """
        pass

    ######################################################
    # WRITE_ERROR handlers
    ######################################################

    def _handler_write_error_enter(self, *args, **kwargs):
        """
        Enter write error state.
        """
        self.stop_scheduled_job(ScheduledJob.FLUSH)

        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_write_error_exit(self, *args, **kwargs):
        """
        Exit write error state.
        """
        pass

    def _handler_clear_write_error(self, *args, **kwargs):
        """
        Clear the WRITE_ERROR state by transitioning to the COMMAND state.
        @return next_state, (next_state, result)
        """
        return ProtocolState.COMMAND, (ProtocolState.COMMAND, None)
