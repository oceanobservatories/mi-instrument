"""
@package mi.instrument.seabird.seaphox.driver
@file mi/instrument/seabird/seaphox/driver.py
@author Jake Ploskey
@brief Driver for the SeapHOx instrument.
"""

import re

from mi.core.common import BaseEnum
from mi.core.log import get_logger

from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.instrument_driver import (
    DriverAsyncEvent,
    DriverEvent,
    DriverProtocolState,
    SingleConnectionInstrumentDriver,
)
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol

from mi.core.exceptions import SampleException


__author__ = "Jake Ploskey"
__license__ = "Apache 2.0"

log = get_logger()

NEWLINE = "\r"


class Prompt(BaseEnum):
    """
    Device i/o prompts.
    """

    CR_NL = "\r\n"
    CR = "\r"


class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver.
    """
    PHSEN_H_FORMAT0 = "phsen_h_format0"


###############################################################################
# Data Particles
###############################################################################

PARTICLE_REGEX = r"""
\s*
(?P<framesync>DSPHOX\d{,9}),\s*
(?P<timestamp>[0-9T\-:]+),\s*
(?P<event_flags>\d+),\s*
(?P<temperature_counts>\d+),\s*
(?P<ph_external_reference_voltage_counts>\d+),\s*
(?P<ph_voltage_counts>\d+),\s*
(?P<ph_current_counts>\d+),\s*
(?P<ph_counter_current_counts>\d+),\s*
(?P<pressure_counts>\d+),\s*
(?P<pressure_temperature_counts>\d+),\s*
(?P<conductivity_frequency>-*\d+\.\d{3}),\s*
(?P<oxygen_phase_delay>-*\d+\.\d{3}),\s*
(?P<oxygen_thermistor_voltage>-*\d+\.\d{6}),\s*
(?P<internal_temperature_counts>\d+),\s*
(?P<internal_humidity_counts>\d+)\n
""".strip().replace("\n", "")


class SeapHOxParticleKey(BaseEnum):
    FRAMESYNC = "framesync"
    TIMESTAMP = "timestamp"
    EVENT_FLAGS = "event_flags"
    TEMPERATURE_COUNTS = "temperature_counts"
    PH_EXTERNAL_REFERENCE_VOLTAGE_COUNTS = "ph_external_reference_voltage_counts"
    PH_VOLTAGE_COUNTS = "ph_voltage_counts"
    PH_CURRENT_COUNTS = "ph_current_counts"
    PH_COUNTER_CURRENT_COUNTS = "ph_counter_current_counts"
    PRESSURE_COUNTS = "pressure_counts"
    PRESSURE_TEMPERATURE_COUNTS = "pressure_temperature_counts"
    CONDUCTIVITY_FREQUENCY = "conductivity_frequency"
    OXYGEN_PHASE_DELAY = "oxygen_phase_delay"
    OXYGEN_THERMISTOR_VOLTAGE = "oxygen_thermistor_voltage"
    INTERNAL_TEMPERATURE_COUNTS = "internal_temperature_counts"
    INTERNAL_HUMIDITY_COUNTS = "internal_humidity_counts"


class SeapHOxParticle(DataParticle):
    """
    Particle class for SeapHOx instrument.
    """

    _data_particle_type = DataParticleType.PHSEN_H_FORMAT0

    @staticmethod
    def regex():
        return PARTICLE_REGEX

    @staticmethod
    def regex_compiled():
        return re.compile(SeapHOxParticle.regex())

    def _build_parsed_values(self):
        """
        Convert the instrument sample into a data particle.
        :return: data particle as a dictionary
        """
        match = SeapHOxParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException(
                "No regex match of parsed sample data: [%s]" % self.raw_data
            )

        def encode(key, parser):
            return self._encode_value(key, match.group(str(key)), parser)

        result = [
            encode(SeapHOxParticleKey.FRAMESYNC, str),
            encode(SeapHOxParticleKey.TIMESTAMP, str),
            encode(SeapHOxParticleKey.EVENT_FLAGS, str),
            encode(SeapHOxParticleKey.TEMPERATURE_COUNTS, int),
            encode(SeapHOxParticleKey.PH_EXTERNAL_REFERENCE_VOLTAGE_COUNTS, int),
            encode(SeapHOxParticleKey.PH_VOLTAGE_COUNTS, int),
            encode(SeapHOxParticleKey.PH_CURRENT_COUNTS, int),
            encode(SeapHOxParticleKey.PH_COUNTER_CURRENT_COUNTS, int),
            encode(SeapHOxParticleKey.PRESSURE_COUNTS, int),
            encode(SeapHOxParticleKey.PRESSURE_TEMPERATURE_COUNTS, int),
            encode(SeapHOxParticleKey.CONDUCTIVITY_FREQUENCY, float),
            encode(SeapHOxParticleKey.OXYGEN_PHASE_DELAY, float),
            encode(SeapHOxParticleKey.OXYGEN_THERMISTOR_VOLTAGE, float),
            encode(SeapHOxParticleKey.INTERNAL_TEMPERATURE_COUNTS, int),
            encode(SeapHOxParticleKey.INTERNAL_HUMIDITY_COUNTS, int),
        ]

        return result


###############################################################################
# Driver
###############################################################################


class Parameter(BaseEnum):
    pass


class InstrumentDriver(SingleConnectionInstrumentDriver):
    @staticmethod
    def get_resource_params():
        """
        Return list of device parameters available.
        """
        return Parameter.list()

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(self._driver_event)


class ProtocolState(BaseEnum):
    """
    Instrument protocol states.
    """
    UNKNOWN = DriverProtocolState.UNKNOWN


class ProtocolEvent(BaseEnum):
    """
    Instrument protocol events.
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    DISCOVER = DriverEvent.DISCOVER


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """


class Protocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class for SeapHOx driver.
    """

    def __init__(self, driver_event):
        """
        Protocol constructor.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, Prompt, NEWLINE, driver_event)

        # Add event handlers for protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(
            ProtocolState,
            ProtocolEvent,
            ProtocolEvent.ENTER,
            ProtocolEvent.EXIT
        )

        handlers = {
            ProtocolState.UNKNOWN: [
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.EXIT, self._handler_unknown_exit),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ],
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        self._chunker = StringChunker(self.sieve_function)

        # Start state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples and status.
        """
        matchers = []
        return_list = []

        matchers.append(SeapHOxParticle.regex_compiled())
        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _max_buffer_size(self):
        return CommandResponseInstrumentProtocol._max_buffer_size(self) * 10

    def _got_chunk(self, chunk, timestamp):
        self._extract_sample(
            SeapHOxParticle,
            SeapHOxParticle.regex_compiled(),
            chunk,
            timestamp,
        )

    def _build_param_dict(self):
        """
        Build the parameter dictionary.
        """
        pass

    def _build_command_dict(self):
        """
        Populate the command dictionary with commands.
        """
        pass

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options.
        """
        pass

    ####################
    # Unknown State Handlers
    ####################
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
        pass

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state.
        @retval next_state, (next_state, result)
        """
        next_state = ProtocolState.UNKNOWN
        result = []

        return next_state, (next_state, result)


class PlaybackProtocol(Protocol):
    def __init__(self, driver_event):
        super(PlaybackProtocol, self).__init__(driver_event)


def create_playback_protocol(callback):
    return PlaybackProtocol(callback)
