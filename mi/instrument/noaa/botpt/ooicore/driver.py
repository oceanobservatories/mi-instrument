"""
@package mi.instrument.noaa.ooicore.driver
@file marine-integrations/mi/instrument/noaa/ooicore/driver.py
@author Pete Cable
@brief BOTPT
Release notes:
"""

import re
import time
import datetime

import ntplib
from mi.core.driver_scheduler import DriverSchedulerConfigKey, TriggerType
from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility, ParameterDictType
from mi.core.common import BaseEnum, Units, Prefixes
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentDataException
from mi.core.instrument.driver_dict import DriverDictKey
import mi.instrument.noaa.botpt.ooicore.particles as particles
import mi.core.log


__author__ = 'Pete Cable'
__license__ = 'Apache 2.0'

log = mi.core.log.get_logger()
META_LOGGER = mi.core.log.get_logging_metaclass('trace')


###
#    Driver Constant Definitions
###

NEWLINE = '\n'

LILY_STRING = 'LILY,'
NANO_STRING = 'NANO,'
IRIS_STRING = 'IRIS,'
SYST_STRING = 'SYST,'

LILY_COMMAND = '*9900XY'
IRIS_COMMAND = LILY_COMMAND
NANO_COMMAND = '*0100'

NANO_RATE_RESPONSE = '*0001TH'

MAX_BUFFER_SIZE = 2 ** 16

STATUS_TIMEOUT = 30


class ScheduledJob(BaseEnum):
    """
    Instrument scheduled jobs
    """
    LEVELING_TIMEOUT = 'botpt_leveling_timeout'
    NANO_TIME_SYNC = 'botpt_nano_time_sync'
    ACQUIRE_STATUS = 'botpt_acquire_status'


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN

    # BOTPT initiates sampling at discovery. However, the UI commands still need to be available for execution
    # as BOTPT can handle commands while sampling. As such, even though sampling is started, the state will
    # remain COMMAND so that the UI will allow command execution. There is currently a tracking issue against the UI
    # (Redmine 10283) to allow the driver to control what capabilities are available. When that issue is resolved,
    # this will need to be changed from DriverProtocolState.COMMAND to DriverProtocolState.AUTOSAMPLE. Currently,
    # this just allows us to fake out the UI
    AUTOSAMPLE = DriverProtocolState.COMMAND

    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    START_DIRECT = DriverEvent.START_DIRECT
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    START_LEVELING = 'PROTOCOL_EVENT_START_LEVELING'
    STOP_LEVELING = 'PROTOCOL_EVENT_STOP_LEVELING'
    NANO_TIME_SYNC = 'PROTOCOL_EVENT_NANO_TIME_SYNC'
    LEVELING_TIMEOUT = 'PROTOCOL_EVENT_LEVELING_TIMEOUT'


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    START_DIRECT = ProtocolEvent.START_DIRECT
    STOP_DIRECT = ProtocolEvent.STOP_DIRECT
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    START_LEVELING = ProtocolEvent.START_LEVELING
    STOP_LEVELING = ProtocolEvent.STOP_LEVELING
    DISCOVER = ProtocolEvent.DISCOVER


class Parameter(DriverParameter):
    """
    Device specific parameters.
    """
    AUTO_RELEVEL = 'auto_relevel'  # Auto-relevel mode
    XTILT_TRIGGER = 'xtilt_relevel_trigger'
    YTILT_TRIGGER = 'ytilt_relevel_trigger'
    LEVELING_TIMEOUT = 'relevel_timeout'
    LEVELING_FAILED = 'leveling_failed'
    OUTPUT_RATE = 'output_rate_hz'
    LILY_LEVELING = 'lily_leveling'

    @classmethod
    def reverse_dict(cls):
        return dict((v, k) for k, v in cls.dict().iteritems())


class ParameterConstraint(BaseEnum):
    """
    Constraints for parameters
    (type, min, max)
    """
    XTILT_TRIGGER = (float, 0, 330)
    YTILT_TRIGGER = (float, 0, 330)
    LEVELING_TIMEOUT = (int, 60, 6000)
    OUTPUT_RATE = (int, 1, 40)
    AUTO_RELEVEL = (bool, None, None)


class InstrumentCommands(BaseEnum):
    """
    Instrument Commands
    """
    LILY_ON = LILY_STRING + LILY_COMMAND + 'C2'  # turns on continuous data
    LILY_OFF = LILY_STRING + LILY_COMMAND + 'C-OFF'  # turns off continuous data
    LILY_DUMP1 = LILY_STRING + LILY_COMMAND + '-DUMP-SETTINGS'  # outputs current settings
    LILY_DUMP2 = LILY_STRING + LILY_COMMAND + '-DUMP2'  # outputs current extended settings
    LILY_START_LEVELING = LILY_STRING + LILY_COMMAND + '-LEVEL,1'  # starts leveling
    LILY_STOP_LEVELING = LILY_STRING + LILY_COMMAND + '-LEVEL,0'  # stops leveling
    NANO_ON = NANO_STRING + NANO_COMMAND + 'E4'  # turns on continuous data
    NANO_OFF = NANO_STRING + NANO_COMMAND + 'E3'  # turns off continuous data
    NANO_DUMP1 = NANO_STRING + NANO_COMMAND + 'IF'  # outputs current settings
    NANO_SET_TIME = NANO_STRING + 'TS'  # requests the SBC to update the NANO time
    NANO_SET_RATE = NANO_STRING + '*0100EW*0100TH='  # sets the sample rate in Hz
    IRIS_ON = IRIS_STRING + IRIS_COMMAND + 'C2'  # turns on continuous data
    IRIS_OFF = IRIS_STRING + IRIS_COMMAND + 'C-OFF'  # turns off continuous data
    IRIS_DUMP1 = IRIS_STRING + IRIS_COMMAND + '-DUMP-SETTINGS'  # outputs current settings
    IRIS_DUMP2 = IRIS_STRING + IRIS_COMMAND + '-DUMP2'  # outputs current extended settings
    SYST_DUMP1 = SYST_STRING + '1'


class InstrumentCommandNames(BaseEnum):
    """
    Instrument Commands
    """
    LILY_ON = 'Lily On'
    LILY_OFF = 'Lily Off'
    LILY_DUMP1 = 'Lily Settings'
    LILY_DUMP2 = 'Lily Extended Settings'
    LILY_START_LEVELING = 'Start Leveling'
    LILY_STOP_LEVELING = 'Stop Leveling'
    NANO_ON = 'Nano On'
    NANO_OFF = 'Nano Off'
    NANO_DUMP1 = 'Nano Settings'
    NANO_SET_TIME = 'Set Time>'
    NANO_SET_RATE = 'Set Rate>'
    IRIS_ON = 'Iris On'
    IRIS_OFF = 'Iris Off'
    IRIS_DUMP1 = 'Iris Settings'
    IRIS_DUMP2 = 'Iris Extended Settings'
    SYST_DUMP1 = 'System Settings'


class Prompt(BaseEnum):
    """
    Instrument responses (basic)
    """
    LILY_ON = LILY_COMMAND + 'C2'
    LILY_OFF = LILY_COMMAND + 'C-OFF'
    IRIS_ON = IRIS_COMMAND + 'C2'
    IRIS_OFF = IRIS_COMMAND + 'C-OFF'
    LILY_START_LEVELING = LILY_COMMAND + '-LEVEL,1'
    LILY_STOP_LEVELING = LILY_COMMAND + '-LEVEL,0'


###############################################################################
# Driver
###############################################################################

# noinspection PyMethodMayBeStatic
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state machine.
    """

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
                (ProtocolEvent.GET, self._handler_autosample_get),
                (ProtocolEvent.SET, self._handler_autosample_set),
                (ProtocolEvent.START_AUTOSAMPLE, self._handler_start_autosample),
                (ProtocolEvent.ACQUIRE_STATUS, self._handler_acquire_status),
                (ProtocolEvent.START_LEVELING, self._handler_start_leveling),
                (ProtocolEvent.STOP_LEVELING, self._handler_stop_leveling),
                (ProtocolEvent.START_DIRECT, self._handler_autosample_start_direct),
                (ProtocolEvent.NANO_TIME_SYNC, self._handler_time_sync),
                (ProtocolEvent.LEVELING_TIMEOUT, self._handler_leveling_timeout),
            ],
            ProtocolState.DIRECT_ACCESS: [
                (ProtocolEvent.ENTER, self._handler_direct_access_enter),
                (ProtocolEvent.EXIT, self._handler_generic_exit),
                (ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct),
                (ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct),
            ],
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Construct the metadata dictionaries
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        # Add build handlers for device commands.
        for command in InstrumentCommands.list():
            if command in [InstrumentCommands.NANO_SET_RATE]:
                self._add_build_handler(command, self._build_command_with_value)
            else:
                self._add_build_handler(command, self._build_simple_command)

        # # Add response handlers for device commands.
        for command in InstrumentCommands.list():
            self._add_response_handler(command, self._generic_response_handler)

        # Start state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # commands sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

        # create chunker
        self._chunker = StringChunker(Protocol.sieve_function)

        self._last_data_timestamp = 0
        self.has_pps = True

        # set up scheduled event handling
        self.initialize_scheduler()
        self._add_scheduler_event(ScheduledJob.ACQUIRE_STATUS, ProtocolEvent.ACQUIRE_STATUS)
        self._add_scheduler_event(ScheduledJob.NANO_TIME_SYNC, ProtocolEvent.NANO_TIME_SYNC)

        self._direct_commands['Wakeup'] = NEWLINE
        self._direct_commands['Newline'] = NEWLINE
        self._direct_commands['Heater Duration>']='HEAT,'
        command_dict = InstrumentCommands.dict()
        label_dict = InstrumentCommandNames.dict()
        for key in command_dict:
            label = label_dict.get(key)
            command = command_dict[key]
            builder = self._build_handlers.get(command, None)
            if builder in [self._build_simple_command]:
                command = builder(command)
                self._direct_commands[label] = command


    @staticmethod
    def sieve_function(raw_data):
        """
        Sort data in the chunker...
        @param raw_data: Data to be searched for samples
        @return: list of (start,end) tuples
        """
        matchers = []
        return_list = []

        matchers.append(particles.HeatSampleParticle.regex_compiled())
        matchers.append(particles.IrisSampleParticle.regex_compiled())
        matchers.append(particles.NanoSampleParticle.regex_compiled())
        matchers.append(particles.LilySampleParticle.regex_compiled())
        matchers.append(particles.LilyLevelingParticle.regex_compiled())

        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, ts):
        """
        Process chunk output by the chunker.  Generate samples and (possibly) react
        @param chunk: data
        @param ts: ntp timestamp
        @return sample
        @throws InstrumentProtocolException
        """
        possible_particles = [
            (particles.LilySampleParticle, self._check_for_autolevel),
            (particles.LilyLevelingParticle, self._check_completed_leveling),
            (particles.HeatSampleParticle, None),
            (particles.IrisSampleParticle, None),
            (particles.NanoSampleParticle, self._check_pps_sync),
        ]

        for particle_type, func in possible_particles:
            sample = self._extract_sample(particle_type, particle_type.regex_compiled(), chunk, ts)
            if sample:
                if func and self.get_current_state() != ProtocolState.UNKNOWN:
                    func(sample)
                return sample

        raise InstrumentProtocolException(u'unhandled chunk received by _got_chunk: [{0!r:s}]'.format(chunk))

    def _extract_sample(self, particle_class, regex, line, timestamp, publish=True):
        """
        Overridden to set the quality flag for LILY particles that are out of range.
        @param particle_class: Class type for particle
        @param regex: regular expression to verify data
        @param line: data
        @param timestamp: ntp timestamp
        @param publish: boolean to indicate if sample should be published
        @return: extracted sample
        """
        if regex.match(line):
            if particle_class == particles.LilySampleParticle and self._param_dict.get(Parameter.LEVELING_FAILED):
                particle = particle_class(line, port_timestamp=timestamp, quality_flag=DataParticleValue.OUT_OF_RANGE)
            else:
                particle = particle_class(line, port_timestamp=timestamp)
            parsed_sample = particle.generate()

            # Add an entry to the particle dictionary, with the particle class as the key
            self._particle_dict[particle.data_particle_type()] = parsed_sample

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample

    def _filter_capabilities(self, events):
        """
        Filter a list of events to only include valid capabilities
        @param events: list of events to be filtered
        @return: list of filtered events
        """
        leveling = self._param_dict.get(Parameter.LILY_LEVELING)

        capabilities = []
        for x in events:
            if Capability.has(x):
                if x is Capability.START_LEVELING and leveling:
                    continue
                if x is Capability.STOP_LEVELING and not leveling:
                    continue
                capabilities.append(x)
        return capabilities

    def _build_command_dict(self):
        """
        Populate the command dictionary with commands.
        """
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name="Acquire Status")
        self._cmd_dict.add(Capability.START_LEVELING, display_name="Start LILY Leveling")
        self._cmd_dict.add(Capability.STOP_LEVELING, display_name="Stop LILY Leveling")
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """
        my_regex = 'Not used'
        ro, rw = ParameterDictVisibility.READ_ONLY, ParameterDictVisibility.READ_WRITE
        _bool, _float, _int = ParameterDictType.BOOL, ParameterDictType.FLOAT, ParameterDictType.INT

        parameters = {
            Parameter.AUTO_RELEVEL: {
                'type': _bool,
                'display_name': 'Automatic Releveling Enabled',
                'description': 'Enable LILY re-leveling automatically: (true | false)',
                'range': {'True': True, 'False': False},
                'visibility': rw,
                'startup_param': True,
            },
            Parameter.XTILT_TRIGGER: {
                'type': _float,
                'display_name': 'X-tilt Releveling Trigger',
                'description': 'The X-tilt value that must be exceeded before LILY auto releveling occurs: (1-330)',
                'range': (1, 330),
                'units': Prefixes.MICRO + Units.RADIAN,
                'visibility': rw,
                'startup_param': True,
            },
            Parameter.YTILT_TRIGGER: {
                'type': _float,
                'display_name': 'Y-tilt Releveling Trigger',
                'description': 'The Y-tilt value that must be exceeded before LILY auto releveling occurs: (1-330)',
                'range': (1, 330),
                'units': Prefixes.MICRO + Units.RADIAN,
                'visibility': rw,
                'startup_param': True,
            },
            Parameter.LEVELING_TIMEOUT: {
                'type': _int,
                'display_name': 'LILY Leveling Timeout',
                'description': 'Leveling timeout: (1-32767)',
                'range': (60, 6000),
                'units': Units.SECOND,
                'visibility': rw,
                'startup_param': True,
            },
            Parameter.OUTPUT_RATE: {
                'type': _int,
                'display_name': 'NANO Output Rate',
                'description': 'Sample rate: (1, 40)',
                'range': (1, 40),
                'units': Units.HERTZ,
                'visibility': rw,
                'startup_param': True,
            },
            Parameter.LILY_LEVELING: {
                'type': _bool,
                'display_name': 'Lily Leveling',
                'description': 'Indicates if LILY leveling is occurring: (true | false)',
                'range': {'True': True, 'False': False},
                'value': False,
                'visibility': ro,
            },
            Parameter.LEVELING_FAILED: {
                'type': _bool,
                'display_name': 'LILY Leveling Failed',
                'description': 'Indicates if LILY leveling failed: (true | false)',
                'range': {'True': True, 'False': False},
                'value': False,
                'visibility': ro,
            },
        }
        for param in parameters:
            self._param_dict.add(param, my_regex, None, None, **parameters[param])

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, False)

    def _build_command_with_value(self, cmd, value):
        """
        Build a simple command with one value specified
        @param cmd: instrument command
        @param value: value to be sent
        @return: command string
        """
        return '%s%d%s' % (cmd, value, NEWLINE)

    def _verify_set_values(self, params):
        """
        Verify supplied values are in range, if applicable
        @param params: Dictionary of Parameter:value pairs to be verified
        @throws InstrumentParameterException
        """
        constraints = ParameterConstraint.dict()
        parameters = Parameter.reverse_dict()

        # step through the list of parameters
        for key, val in params.iteritems():
            # verify this parameter exists
            if not Parameter.has(key):
                raise InstrumentParameterException('Received invalid parameter in SET: %s' % key)
            # if constraint exists, verify we have not violated it
            constraint_key = parameters.get(key)
            if constraint_key in constraints:
                var_type, minimum, maximum = constraints[constraint_key]
                constraint_string = 'Parameter: %s Value: %s Type: %s Minimum: %s Maximum: %s' % \
                                    (key, val, var_type, minimum, maximum)
                log.debug('SET CONSTRAINT: %s', constraint_string)

                # attempt to cast val to target type
                try:
                    val = var_type(val)
                except ValueError:
                    raise InstrumentParameterException('Type mismatch: %s' % constraint_string)

                # now, verify we are within min/max
                if not var_type == bool:
                    if val < minimum or val > maximum:
                        raise InstrumentParameterException('Out of range: %s' % constraint_string)

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters
        @param args: arglist, should contain a dictionary of parameters/values to be set
        """
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_set_values(params)
        self._verify_not_readonly(*args, **kwargs)

        # if setting the output rate, get the current rate from the instrument first...
        if Parameter.OUTPUT_RATE in params:
            self._update_params()

        old_config = self._param_dict.get_config()

        # all constraints met or no constraints exist, set the values
        for key, value in params.iteritems():
            self._param_dict.set_value(key, value)

        new_config = self._param_dict.get_config()

        if not old_config == new_config:
            log.debug('Config change: %r %r', old_config, new_config)
            if old_config[Parameter.OUTPUT_RATE] is not None:
                if int(old_config[Parameter.OUTPUT_RATE]) != int(new_config[Parameter.OUTPUT_RATE]):
                    self._do_cmd_no_resp(InstrumentCommands.NANO_SET_RATE, int(new_config[Parameter.OUTPUT_RATE]))
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _update_params(self, *args, **kwargs):
        """
        Update the param dictionary based on instrument response
        """
        result, _ = self._do_cmd_resp(InstrumentCommands.NANO_DUMP1,
                                      response_regex=particles.NanoStatusParticle.regex_compiled())
        rate = int(re.search(r'NANO,\*TH:(\d+)', result).group(1))
        self._param_dict.set_value(Parameter.OUTPUT_RATE, rate)

    def _wakeup(self, timeout, delay=1):
        """
        Overriding _wakeup; does not apply to this instrument
        """

    def add_to_buffer(self, data):
        """
        Overriding base class to reduce logging due to NANO high data rate
        @param data: data to be added to buffers
        """
        # Update the line and prompt buffers.
        self._linebuf += data
        self._promptbuf += data
        self._last_data_timestamp = time.time()

        # If our buffer exceeds the max allowable size then drop the leading
        # characters on the floor.
        max_size = self._max_buffer_size()
        if len(self._linebuf) > max_size:
            self._linebuf = self._linebuf[max_size * -1:]

        # If our buffer exceeds the max allowable size then drop the leading
        # characters on the floor.
        if len(self._promptbuf) > max_size:
            self._promptbuf = self._linebuf[max_size * -1:]

    def _max_buffer_size(self):
        """
        Overriding base class to increase max buffer size
        @return int max_buffer_size
        """
        return MAX_BUFFER_SIZE

    def _remove_leveling_timeout(self):
        """
        Clean up the leveling timer
        """
        try:
            self._remove_scheduler(ScheduledJob.LEVELING_TIMEOUT)
        except KeyError:
            log.debug('Unable to remove LEVELING_TIMEOUT scheduled job, job does not exist.')

    def _schedule_leveling_timeout(self):
        """
        Set up a leveling timer to make sure we don't stay in leveling state forever if something goes wrong
        """
        self._remove_leveling_timeout()
        dt = datetime.datetime.now() + datetime.timedelta(seconds=self._param_dict.get(Parameter.LEVELING_TIMEOUT))
        job_name = ScheduledJob.LEVELING_TIMEOUT
        config = {
            DriverConfigKey.SCHEDULER: {
                job_name: {
                    DriverSchedulerConfigKey.TRIGGER: {
                        DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.ABSOLUTE,
                        DriverSchedulerConfigKey.DATE: dt
                    },
                }
            }
        }

        self.set_init_params(config)
        self._add_scheduler_event(ScheduledJob.LEVELING_TIMEOUT, ProtocolEvent.LEVELING_TIMEOUT)

    def _generic_response_handler(self, resp, prompt):
        """
        Pass through response handler
        @param resp: response
        @param prompt: prompt
        @return: (response, prompt)
        """
        return resp, prompt

    def _particle_to_dict(self, sample):
        """
        Convert a particle to a dictionary of value_id:value
        @param sample: particle to be parsed
        @return: dictionary representing the particle
        """
        sample_dict = {}
        values = sample.get(DataParticleKey.VALUES, [])
        for each in values:
            sample_dict[each[DataParticleKey.VALUE_ID]] = each[DataParticleKey.VALUE]
        return sample_dict

    def _check_for_autolevel(self, sample):
        """
        Check this sample, kick off a leveling event if out of range
        @param sample: sample to be checked
        """
        if self._param_dict.get(Parameter.AUTO_RELEVEL) and self.get_current_state() == ProtocolState.AUTOSAMPLE:
            # Find the current X and Y tilt values
            # If they exceed the trigger parameters, begin autolevel
            relevel = False
            sample = self._particle_to_dict(sample)
            x_tilt = abs(sample[particles.LilySampleParticleKey.X_TILT])
            y_tilt = abs(sample[particles.LilySampleParticleKey.Y_TILT])
            x_trig = int(self._param_dict.get(Parameter.XTILT_TRIGGER))
            y_trig = int(self._param_dict.get(Parameter.YTILT_TRIGGER))
            if x_tilt > x_trig or y_tilt > y_trig:
                self._async_raise_fsm_event(ProtocolEvent.START_LEVELING)

    def _failed_leveling(self, axis):
        """
        Handle a failed leveling event.  Set the failed flag, disable auto relevel and notify the operator
        @param axis: Axis which failed leveling
        """
        log.error('Detected leveling error in %s axis!', axis)
        # Read only parameter, must be set outside of handler
        self._param_dict.set_value(Parameter.LEVELING_FAILED, True)
        # Use the handler to disable auto relevel to raise a config change event if needed.
        self._handler_autosample_set({Parameter.AUTO_RELEVEL: False})
        raise InstrumentDataException('LILY Leveling (%s) Failed.  Disabling auto relevel' % axis)

    def _check_completed_leveling(self, sample):
        """
        Check this sample if leveling is complete or failed
        @param sample: Sample to be checked
        """
        sample = self._particle_to_dict(sample)
        status = sample[particles.LilyLevelingParticleKey.STATUS]
        if status is not None:
            # Leveling status update received
            # If leveling complete, send STOP_LEVELING, set the _leveling_failed flag to False
            if 'Leveled' in status:
                if self._param_dict.get(Parameter.LEVELING_FAILED):
                    self._handler_autosample_set({Parameter.LEVELING_FAILED: False})
                self._async_raise_fsm_event(ProtocolEvent.STOP_LEVELING)
            # Leveling X failed!  Set the flag and raise an exception to notify the operator
            # and disable auto leveling. Let the instrument attempt to level
            # in the Y axis.
            elif 'X Axis out of range' in status:
                self._failed_leveling('X')
            # Leveling X failed!  Set the flag and raise an exception to notify the operator
            # and disable auto leveling. Send STOP_LEVELING
            elif 'Y Axis out of range' in status:
                self._async_raise_fsm_event(ProtocolEvent.STOP_LEVELING)
                self._failed_leveling('Y')

    def _check_pps_sync(self, sample):
        """
        Check if PPS sync status has changed.  Update driver flag and, if appropriate, trigger a time sync
        @param sample: sample to be checked
        """
        sample = self._particle_to_dict(sample)
        pps_sync = sample[particles.NanoSampleParticleKey.PPS_SYNC] == 'P'
        if pps_sync:
            if not self.has_pps:
                # pps sync regained, sync the time
                self.has_pps = True
                if self.get_current_state() == ProtocolState.AUTOSAMPLE:
                    self._async_raise_fsm_event(ProtocolEvent.NANO_TIME_SYNC)
        else:
            self.has_pps = False

    ########################################################################
    # Unknown handlers.
    ########################################################################

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Process discover event
        @return next_state, (next_state, result)
        """
        next_state = ProtocolState.AUTOSAMPLE

        result = []

        return next_state, (next_state, result)

    ########################################################################
    # Autosample handlers.
    ########################################################################

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        Enter autosample state.
        """
        # key off the initialization flag to determine if we should sync the time
        if self._init_type == InitializationType.STARTUP:
            self._handler_time_sync()

        self._init_params()
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

        # Since BOTPT is to discover into the Autosample state without a command execution, and remain that
        # way permanently, we make the call to start autosampling here
        self._handler_start_autosample()

    def _handler_autosample_get(self, *args, **kwargs):
        """
        Process GET event
        """
        return self._handler_get(*args, **kwargs)

    def _handler_autosample_set(self, *args, **kwargs):
        """
        Perform a set command.
        @param args[0] parameter : value dict.
        @return next_state, (next_state, result)
        @throws InstrumentParameterException
        """
        next_state = None
        result = []
        startup = False

        if len(args) < 1:
            raise InstrumentParameterException('Set command requires a parameter dict.')
        params = args[0]
        if len(args) > 1:
            startup = args[1]

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')
        if not isinstance(startup, bool):
            raise InstrumentParameterException('Startup not a bool.')

        self._set_params(params, startup)
        return next_state, result

    def _handler_autosample_start_direct(self):
        """
        Start direct access
        @return next_state, (next_state, result)
        """
        next_state = ProtocolState.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    def _handler_start_autosample(self):
        """
        Start autosample
        @return next_state, (next_state, result)
        """
        next_state = ProtocolState.AUTOSAMPLE
        result = []
        self._do_cmd_resp(InstrumentCommands.LILY_ON, expected_prompt=Prompt.LILY_ON)
        self._do_cmd_resp(InstrumentCommands.NANO_ON, expected_prompt=NANO_STRING)
        self._do_cmd_resp(InstrumentCommands.IRIS_ON, expected_prompt=Prompt.IRIS_ON)
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

    def _handler_direct_access_execute_direct(self, data):
        """
        Execute direct access command
        @return next_state, (next_state, result)
        """
        next_state = None
        result = []
        self._do_cmd_direct(data)
        self._sent_cmds.append(data)
        return next_state, (next_state, result)

    def _handler_direct_access_stop_direct(self):
        """
        Stop direct access
        @return next_state, (next_state, result)
        """
        next_state, (_, result) = self._handler_unknown_discover()

        return next_state, (next_state, result)

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

    def _handler_acquire_status(self, *args, **kwargs):
        """
        We generate these particles here to avoid the chunker.  This allows us to process status
        messages with embedded messages from the other parts of the instrument.
        @return next_state, (next_state, result)
        """
        next_state = None
        timeout = time.time() + STATUS_TIMEOUT

        ts = ntplib.system_to_ntp_time(time.time())
        parts = []

        for command, particle_class in [
            (InstrumentCommands.SYST_DUMP1, particles.SystStatusParticle),
            (InstrumentCommands.LILY_DUMP1, particles.LilyStatusParticle1),
            (InstrumentCommands.LILY_DUMP2, particles.LilyStatusParticle2),
            (InstrumentCommands.IRIS_DUMP1, particles.IrisStatusParticle1),
            (InstrumentCommands.IRIS_DUMP2, particles.IrisStatusParticle2),
            (InstrumentCommands.NANO_DUMP1, particles.NanoStatusParticle),
        ]:
            result, _ = self._do_cmd_resp(command, response_regex=particle_class.regex_compiled())
            parts.append(result)
        sample = self._extract_sample(particles.BotptStatusParticle,
                                      particles.BotptStatusParticle.regex_compiled(),
                                      NEWLINE.join(parts), ts)

        if self.get_current_state() == ProtocolState.AUTOSAMPLE:
            # acquiring status stops NANO output, restart it
            self._do_cmd_resp(InstrumentCommands.NANO_ON, expected_prompt=NANO_STRING)

        data_particles = self.wait_for_particles([particles.DataParticleType.BOTPT_STATUS], timeout)

        if not sample:
            raise InstrumentProtocolException('Failed to generate status particle')
        return next_state, (next_state, data_particles)

    def _handler_time_sync(self, *args, **kwargs):
        """
        Syncing time starts autosample...
        @return next_state, (next_state, result)
        """
        next_state = None
        result = self._do_cmd_resp(InstrumentCommands.NANO_SET_TIME, expected_prompt=NANO_STRING)
        return next_state, (next_state, result)

    def _handler_start_leveling(self):
        """
        Send the start leveling command
        @return next_state, (next_state, result)
        """
        next_state = None
        result = None
        if not self._param_dict.get(Parameter.LILY_LEVELING):
            self._schedule_leveling_timeout()
            result = self._do_cmd_resp(InstrumentCommands.LILY_START_LEVELING,
                                       expected_prompt=Prompt.LILY_START_LEVELING)
            self._param_dict.set_value(Parameter.LILY_LEVELING, True)
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)
        return next_state, (next_state, result)

    def _handler_stop_leveling(self):
        """
        Send the stop leveling command
        @return next_state, (next_state, result)
        """
        next_state = None
        result = []
        if self._param_dict.get(Parameter.LILY_LEVELING):
            self._remove_leveling_timeout()

            next_state = self._do_cmd_resp(InstrumentCommands.LILY_STOP_LEVELING,
                                           expected_prompt=Prompt.LILY_STOP_LEVELING)
            self._param_dict.set_value(Parameter.LILY_LEVELING, False)

            if self.get_current_state() == ProtocolState.AUTOSAMPLE:
                self._do_cmd_resp(InstrumentCommands.LILY_ON, expected_prompt=Prompt.LILY_ON)

            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        return next_state, (next_state, result)

    def _handler_leveling_timeout(self):
        """
        Leveling has timed out, disable auto-relevel and mark leveling as failed.
        handler_stop_leveling will raise the config change event.
        @throws InstrumentProtocolException
        """
        self._param_dict.set_value(Parameter.AUTO_RELEVEL, False)
        self._param_dict.set_value(Parameter.LEVELING_FAILED, True)
        self._handler_stop_leveling()
        raise InstrumentProtocolException('Leveling failed to complete within timeout, disabling auto-relevel')


def create_playback_protocol(callback):
    return Protocol(None, None, callback)