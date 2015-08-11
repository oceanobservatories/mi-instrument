import cPickle as pickle
import time
from threading import Lock, Thread
from mi.core.driver_scheduler import DriverSchedulerConfigKey, TriggerType
from mi.core.exceptions import InstrumentProtocolException, InstrumentParameterException
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.driver_dict import DriverDictKey

from mi.core.instrument.port_agent_client import PortAgentPacket
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility, ParameterDictType
from mi.core.log import get_logger, get_logging_metaclass
from mi.instrument.antelope.orb.ooicore.packet_log import PacketLog, GapException

log = get_logger()
meta = get_logging_metaclass('info')

from mi.core.common import BaseEnum, Units
from mi.core.persistent_store import PersistentStoreDict

from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver, DriverConfigKey
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import ResourceAgentState

from mi.core.instrument.instrument_protocol import InstrumentProtocol
from mi.core.instrument_fsm import ThreadSafeFSM


ORBOLDEST = -13


class ProtocolState(BaseEnum):
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE


class ProtocolEvent(BaseEnum):
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    DISCOVER = DriverEvent.DISCOVER
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    FLUSH = 'PROTOCOL_EVENT_FLUSH'


class Capability(BaseEnum):
    DISCOVER = DriverEvent.DISCOVER
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    GET = DriverEvent.GET
    SET = DriverEvent.SET


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
    NET = 'network'
    STATION = 'station'
    LOCATION = 'location'
    CHANNEL = 'channel'
    START = 'starttime'
    END = 'endtime'
    RATE = 'sampling_rate'
    NSAMPS = 'num_samples'
    FILENAME = 'filename'
    OPEN = 'open'


class AntelopeMetadataParticle(DataParticle):
    _data_particle_type = AntelopeDataParticles.METADATA

    def __init__(self, raw_data, is_open, **kwargs):
        super(AntelopeMetadataParticle, self).__init__(raw_data, **kwargs)
        self.is_open = is_open

    def _build_parsed_values(self):
        header = self.raw_data.header
        pk = AntelopeMetadataParticleKey
        return [
            self._encode_value(pk.NET, header.net, str),
            self._encode_value(pk.STATION, header.station, str),
            self._encode_value(pk.LOCATION, header.location, str),
            self._encode_value(pk.CHANNEL, header.channel, str),
            self._encode_value(pk.START, header.starttime, str),
            self._encode_value(pk.END, header.endtime, str),
            self._encode_value(pk.RATE, header.rate, float),
            self._encode_value(pk.NSAMPS, header.num_samples, int),
            self._encode_value(pk.FILENAME, self.raw_data.filename, str),
            self._encode_value(pk.OPEN, self.is_open, int),
        ]


class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    Generic antelope instrument driver
    """
    # __metaclass__ = meta

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(self._driver_event)


class Protocol(InstrumentProtocol):
    #__metaclass__ = meta

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
            )}

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Build dictionaries for driver schema
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        # Set the base directory for the packet data file location.
        PacketLog.file_location(self._param_dict.get(Parameter.FILE_LOCATION))

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
                             default_value="./",
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
        if not 'pktid' in self._persistent_store:
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

    def _flush(self, close_all=False):
        log.info('flush')
        particles = []
        with self._lock:
            log.info('got lock')
            if close_all:
                self._filled_logs.extend(self._logs.values())
                self._logs = {}

            for _log in self._logs.itervalues():
                log.info('flushing incomplete')
                _log.flush()
                particles.append(AntelopeMetadataParticle(_log, True))

            for _log in self._filled_logs:
                log.info('flushing complete')
                _log.flush()
                particles.append(AntelopeMetadataParticle(_log, False))
                _log.data = []

            self._filled_logs = []
            log.info('updating persistent store')
            self._persistent_store['pktid'] = self._pktid

        for particle in particles:
            self._driver_event(DriverAsyncEvent.SAMPLE, particle.generate())

        return None, None

    def _orbstart(self):
        self._connection._command_port_agent('orbselect %s' % self._param_dict.get(Parameter.SOURCE_REGEX))
        self._connection._command_port_agent('orbseek %s' % self._persistent_store['pktid'])
        self._connection._command_port_agent('orbstart')

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
            1: 86400 * 7,   # 1 week
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
        return bin_value * bin_size, (bin_value + 1) * bin_size

    def _bin_data(self, packet):
        key = '%s.%s.%s.%s' % (packet['net'], packet.get('location', ''),
                               packet.get('sta', ''), packet['chan'])
        start, end = self._get_bin(packet)
        self._pktid = packet['pktid']

        with self._lock:
            if key not in self._logs:
                self._logs[key] = PacketLog.from_packet(packet, end)

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
                    self._logs[key] = PacketLog.from_packet(packet, end)

            except GapException:
                # non-contiguous data detected, close this log and open a new one
                self._filled_logs.append(self._logs[key])
                del self._logs[key]
                # create the new log
                self._logs[key] = PacketLog.from_packet(packet, end)
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
        @return protocol_state, agent_state
        """
        return ProtocolState.COMMAND, ResourceAgentState.IDLE

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
        @return next_state, (next_agent_state, result) if successful.
        """
        result = None

        self._orbstart()
        next_state = ProtocolState.AUTOSAMPLE
        next_agent_state = ResourceAgentState.STREAMING

        return next_state, (next_agent_state, result)

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
        self.stop_scheduled_job(ScheduledJob.FLUSH)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample and switch back to command mode.
        @return  next_state, (next_agent_state, result) if successful.
        incorrect prompt received.
        """
        result = None

        self._orbstop()
        self._flush(True)
        next_state = ProtocolState.COMMAND
        next_agent_state = ResourceAgentState.COMMAND

        return next_state, (next_agent_state, result)
