
"""
@package mi.instrument.noaa.ooicore.driver
@file marine-integrations/mi/instrument/noaa/ooicore/driver.py
@author Pete Cable
@brief virtual driver, generates simulated particle data
Release notes:
"""
from collections import namedtuple
import functools
import random
import sqlite3
import string
import time

import ntplib

from mi.core.common import BaseEnum
from mi.core.driver_scheduler import DriverSchedulerConfigKey, TriggerType
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.instrument_driver import DriverEvent, DriverConfigKey
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.exceptions import InstrumentParameterException, SampleException
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
import mi.core.log


__author__ = 'Pete Cable'
__license__ = 'Apache 2.0'

log = mi.core.log.get_logger()
META_LOGGER = mi.core.log.get_logging_metaclass('trace')
NEWLINE = '\n'

# Preload helper items

PARAMDICT_SELECT = """
SELECT id, scenario, name, parameter_ids, temporal_parameter
FROM parameterdictionary
WHERE id like 'DICT%'
"""

PARAMDEF_SELECT = """
SELECT id, scenario, name, hid, parameter_type, value_encoding, unit_of_measure, fill_value, display_name,
        precision, parameter_function_id, parameter_function_map, data_product_identifier
FROM parameterdefs
WHERE id like 'PD%'
"""

ParameterDef = namedtuple('ParameterDef',
                          'id, scenario, name, hid, parameter_type, value_encoding, units, fill_value, '
                          'display_name, precision, parameter_function_id, parameter_function_map, dpi')
ParameterDictionary = namedtuple('ParameterDictionary', 'id, scenario, name, parameter_ids, temporal_parameter')


def load_paramdefs(conn):
    log.debug('Loading Parameter Definitions')
    c = conn.cursor()
    c.execute(PARAMDEF_SELECT)
    params = map(ParameterDef._make, c.fetchall())
    return {x.id: x for x in params}


def load_paramdicts(conn):
    log.debug('Loading Parameter Dictionary')
    c = conn.cursor()
    c.execute(PARAMDICT_SELECT)
    params = map(ParameterDictionary._make, c.fetchall())
    return {p.name: p for p in params}


class Parameter(BaseEnum):
    pass


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE


class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    DISCOVER = DriverEvent.DISCOVER
    VERY_LONG_COMMAND = 'VERY_LONG_COMMAND'


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    VERY_LONG_COMMAND = ProtocolEvent.VERY_LONG_COMMAND


class Prompt(BaseEnum):
    """
    Instrument responses (basic)
    """


class VirtualParticle(DataParticle):
    __metaclass__ = META_LOGGER
    _parameters = None
    _streams = None
    _ignore = 'PD7,PD10,PD11,PD12,PD16,PD863'.split(',')
    INT8_MIN = -2**7
    INT8_MAX = 2**7
    UINT8_MAX = 2**8

    INT16_MIN = -2**15
    INT16_MAX = 2**15
    UINT16_MAX = 2**16

    INT32_MIN = -2**31
    INT32_MAX = 2**31
    UINT32_MAX = 2**32

    INT64_MIN = -2**63
    INT64_MAX = 2**63
    UINT64_MAX = 2**64

    def _load_streams(self):
        conn = sqlite3.connect('preload.db')
        self._parameters = load_paramdefs(conn)
        self._streams = load_paramdicts(conn)

    @staticmethod
    def random_string(size):
        return ''.join(random.sample(string.ascii_letters, size))

    def _build_parsed_values(self):
        """
        @throws SampleException If there is a problem with sample creation
        """
        if self._parameters is None or self._streams is None:
            self._load_streams()

        if not self.raw_data in self._streams:
            raise SampleException('Unknown stream')

        self._data_particle_type = self.raw_data

        parameters = self._streams.get(self.raw_data).parameter_ids.split(',')

        values = []
        for param in parameters:
            if param in self._ignore:
                continue
            p = self._parameters.get(param)

            if p.parameter_type == 'function':
                continue

            log.debug('Generating random data for param: %s name: %s', param, p.name)

            val = None
            if p.value_encoding in ['str', 'string']:
                val = self.random_string(20)
            elif p.value_encoding == 'int8':
                val = random.randint(self.INT8_MIN, self.INT8_MAX)
            elif p.value_encoding == 'int16':
                val = random.randint(self.INT16_MIN, self.INT16_MAX)
            elif p.value_encoding == 'int32':
                val = random.randint(self.INT32_MIN, self.INT32_MAX)
            elif p.value_encoding == 'int64':
                val = random.randint(self.INT64_MIN, self.INT64_MAX)
            elif p.value_encoding == 'uint8':
                val = random.randint(0, self.UINT8_MAX)
            elif p.value_encoding == 'uint16':
                val = random.randint(0, self.UINT16_MAX)
            elif p.value_encoding == 'uint32':
                val = random.randint(0, self.UINT32_MAX)
            elif p.value_encoding == 'uint64':
                val = random.randint(0, self.UINT64_MAX)
            elif p.value_encoding in ['float32', 'float64']:
                val = random.random()
            else:
                log.debug('Unhandled parameter value encoding: %s', p)
            if val is not None:
                if 'array' in p.parameter_type and not p.value_encoding in ['str', 'string']:
                    val = [val] * 2
                values.append({'value_id': p.name, 'value': val})

        return values


class PortAgentClientStub(object):
    pass


###############################################################################
# Driver
###############################################################################

# noinspection PyMethodMayBeStatic
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state machine.
    """

    def __init__(self, evt_callback):
        """
        Driver constructor.
        @param evt_callback Driver process event callback.
        """
        #Construct superclass.
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################
    def get_resource_params(self):
        """
        Return list of device parameters available.
        @return List of parameters
        """
        return Parameter.list()

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(BaseEnum, NEWLINE, self._driver_event)

    def _handler_unconfigured_configure(self, *args, **kwargs):
        """
        Configure driver for device comms.
        @param args[0] Communiations config dictionary.
        @retval (next_state, result) tuple, (DriverConnectionState.DISCONNECTED,
        None) if successful, (None, None) otherwise.
        @raises InstrumentParameterException if missing or invalid param dict.
        """
        result = None
        self._connection = PortAgentClientStub()
        next_state = DriverConnectionState.DISCONNECTED

        return next_state, result

    ########################################################################
    # Disconnected handlers.
    ########################################################################

    def _handler_disconnected_configure(self, *args, **kwargs):
        """
        Configure driver for device comms.
        @param args[0] Communiations config dictionary.
        @retval (next_state, result) tuple, (None, None).
        @raises InstrumentParameterException if missing or invalid param dict.
        """
        result = None
        self._connection = PortAgentClientStub()
        next_state = DriverConnectionState.DISCONNECTED

        return next_state, result

    def _handler_disconnected_connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger and
        construct and intialize a protocol FSM for device interaction.
        @retval (next_state, result) tuple, (DriverConnectionState.CONNECTED,
        None) if successful.
        @raises InstrumentConnectionException if the attempt to connect failed.
        """
        self._build_protocol()
        self._protocol._connection = self._connection
        next_state = DriverConnectionState.CONNECTED

        return next_state, None


###########################################################################
# Protocol
###########################################################################

# noinspection PyUnusedLocal,PyMethodMayBeStatic
class Protocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """
    __metaclass__ = META_LOGGER

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

        # Add event handlers for protocol state machine.
        handlers = {
            ProtocolState.UNKNOWN: [
                (ProtocolEvent.ENTER, self._handler_generic_enter),
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ],
            ProtocolState.AUTOSAMPLE: [
                (ProtocolEvent.ENTER, self._handler_autosample_enter),
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.GET, self._handler_command_get),
                (ProtocolEvent.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample),
                (ProtocolEvent.VERY_LONG_COMMAND, self._very_long_command),
            ],
            ProtocolState.COMMAND: [
                (ProtocolEvent.ENTER, self._handler_command_enter),
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.GET, self._handler_command_get),
                (ProtocolEvent.SET, self._handler_command_set),
                (ProtocolEvent.START_AUTOSAMPLE, self._handler_command_start_autosample),
                (ProtocolEvent.VERY_LONG_COMMAND, self._very_long_command),
            ],
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Construct the metadata dictionaries
        self._build_command_dict()
        self._build_driver_dict()

        # Start state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # set up scheduled event handling
        self.initialize_scheduler()
        self._schedulers = []

    def _generate_particle(self, stream_name, count=1):
        # we're faking it anyway, send these as fast as we can...
        # the overall rate will be close enough
        for x in range(count):
            particle = VirtualParticle(stream_name, port_timestamp=ntplib.system_to_ntp_time(time.time()))
            self._driver_event(DriverAsyncEvent.SAMPLE, particle.generate())

    def _create_scheduler(self, stream_name, rate):
        job_name = stream_name

        if rate > 1:
            interval = 1
        else:
            interval = 1 / rate

        config = {
            DriverConfigKey.SCHEDULER: {
                job_name: {
                    DriverSchedulerConfigKey.TRIGGER: {
                        DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.INTERVAL,
                        DriverSchedulerConfigKey.SECONDS: interval
                    }
                }
            }
        }
        self.set_init_params(config)
        self._schedulers.append(stream_name)

        if rate > 1:
            self._add_scheduler(stream_name, functools.partial(self._generate_particle, stream_name, count=rate))
        else:
            self._add_scheduler(stream_name, functools.partial(self._generate_particle, stream_name))

    def _delete_all_schedulers(self):
        for name in self._schedulers:
            try:
                self._remove_scheduler(name)
            except:
                pass

    def _got_chunk(self, chunk, ts):
        """
        Process chunk output by the chunker.  Generate samples and (possibly) react
        @param chunk: data
        @param ts: ntp timestamp
        @return sample
        @throws InstrumentProtocolException
        """
        return

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

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, False)

    def _update_params(self, *args, **kwargs):
        """
        Update the param dictionary based on instrument response
        """

    def _set_params(self, *args, **kwargs):
        if len(args) < 1:
            raise InstrumentParameterException('Set command requires a parameter dict.')
        params = args[0]

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')

        self._param_dict = ProtocolParameterDict()

        for param in params:
            log.info('Creating new parameter: %s', param)
            self._param_dict.add(param, '', None, None)
            self._param_dict.set_value(param, params[param])

        return None, None

    def set_init_params(self, config):
        if not isinstance(config, dict):
            raise InstrumentParameterException("Invalid init config format")

        self._startup_config = config

        param_config = config.get(DriverConfigKey.PARAMETERS)
        if param_config:
            for name in param_config.keys():
                self._param_dict.add(name, '', None, None)
                log.debug("Setting init value for %s to %s", name, param_config[name])
                self._param_dict.set_init_value(name, param_config[name])

    def _very_long_command(self, *args, **kwargs):
        return None, time.sleep(30)

    ########################################################################
    # Unknown handlers.
    ########################################################################

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Process discover event
        @return next_state, next_agent_state
        """
        return ProtocolState.COMMAND, ResourceAgentState.IDLE

    ########################################################################
    # Autosample handlers.
    ########################################################################

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        self._init_params()

        for stream_name in self._param_dict.get_keys():
            self._create_scheduler(stream_name, self._param_dict.get(stream_name))

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        Stop autosample
        @return next_state, (next_agent_state, result)
        """
        self._delete_all_schedulers()
        return ProtocolState.COMMAND, (ResourceAgentState.COMMAND, None)

    ########################################################################
    # Command handlers.
    ########################################################################

    def _handler_command_enter(self, *args, **kwargs):
        """
        Enter command state.
        """
        self._init_params()
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_get(self, *args, **kwargs):
        """
        Process GET event
        @return next_state, result
        """
        return self._handler_get(*args, **kwargs)

    def _handler_command_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @return (next_state, result)
        @throws InstrumentParameterException
        """
        return self._set_params(*args, **kwargs)

    def _handler_command_start_autosample(self):
        """
        Start autosample
        @return next_state, (next_agent_state, result)
        """
        return ProtocolState.AUTOSAMPLE, (ResourceAgentState.STREAMING, None)

    ########################################################################
    # Generic handlers.
    ########################################################################

    def _handler_generic_enter(self, *args, **kwargs):
        """
        Generic enter state handler
        """
        # Tell driver superclass to send a state change event.
        # Superclass will query the state.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_generic_exit(self, *args, **kwargs):
        """
        Generic exit state handler
        """
