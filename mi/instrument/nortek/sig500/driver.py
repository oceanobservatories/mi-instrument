"""
@package mi.instrument.mclane.ras.ooicore.driver
@file marine-integrations/mi/instrument/mclane/ras/ooicore/driver.py
@author Jake Ploskey
@brief Driver for Nortek Signature500
Release notes:

initial version
"""

from collections import defaultdict
from datetime import datetime
from time import strftime

__author__ = "Jake Ploskey"
__license__ = "Apache 2.0"

import re

from mi.core.common import BaseEnum
from mi.core.exceptions import  InstrumentProtocolException
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.instrument_driver import (
    DriverAsyncEvent,
    DriverEvent,
    DriverProtocolState,
    SingleConnectionInstrumentDriver,
)
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.log import get_logger
from mi.core.util import dict_equal

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
    Data particle types produced by this driver
    """

    CONFIG = "vadcp_b_config"
    VELOCITY_FIFTH_BEAM = "vadcp_b_velocity_beam5"
    VELOCITY_MAIN_BEAM = "vadcp_b_velocity_beam"


###############################################################################
# Data Particles
###############################################################################


class ConfigDataParticleKey(BaseEnum):
    head_type = "head_type"
    serial_number = "serial_number"
    num_cells = "num_cells"
    blanking_distance = "blanking_distance"
    cell_size = "cell_size"
    coordinate_system = "coordinate_system"


class EngineeringDataParticleKey(BaseEnum):
    ERROR_CODE = "error_code"
    STATUS_CODE = "status_code"
    SPEED_OF_SOUND = "speed_of_sound"
    HEADING_STDEV = "heading_stdev"
    HEADING = "heading"
    PITCH = "pitch"
    PITCH_STDEV = "pitch_stdev"
    ROLL = "roll"
    ROLL_STDEV = "roll_stdev"
    ADCP_PRESSURE = "adcp_pressure"
    ADCP_PRESSURE_STDEV = "adcp_pressure_stdev"
    ADCP_TEMPERATURE = "adcp_temperature"


class VelocityFifthBeamDataParticleKey(BaseEnum):
    ERROR_CODE = EngineeringDataParticleKey.ERROR_CODE
    STATUS_CODE = EngineeringDataParticleKey.STATUS_CODE
    SPEED_OF_SOUND = EngineeringDataParticleKey.SPEED_OF_SOUND
    HEADING_STDEV = EngineeringDataParticleKey.HEADING_STDEV
    HEADING = EngineeringDataParticleKey.HEADING
    PITCH = EngineeringDataParticleKey.PITCH
    PITCH_STDEV = EngineeringDataParticleKey.PITCH_STDEV
    ROLL = EngineeringDataParticleKey.ROLL
    ROLL_STDEV = EngineeringDataParticleKey.ROLL_STDEV
    ADCP_PRESSURE = EngineeringDataParticleKey.ADCP_PRESSURE
    ADCP_PRESSURE_STDEV = EngineeringDataParticleKey.ADCP_PRESSURE_STDEV
    ADCP_TEMPERATURE = EngineeringDataParticleKey.ADCP_TEMPERATURE

    CELL_NUMBER = "cell_number"
    CELL_POSITION = "cell_position"

    VELOCITY_BEAM5 = "velocity_beam5"
    AMPLITUDE_BEAM5 = "amplitude_beam5"
    CORRELATION_BEAM5 = "correlation_beam5"


class VelocityMainBeamDataParticleKey(BaseEnum):
    ERROR_CODE = EngineeringDataParticleKey.ERROR_CODE
    STATUS_CODE = EngineeringDataParticleKey.STATUS_CODE
    SPEED_OF_SOUND = EngineeringDataParticleKey.SPEED_OF_SOUND
    HEADING_STDEV = EngineeringDataParticleKey.HEADING_STDEV
    HEADING = EngineeringDataParticleKey.HEADING
    PITCH = EngineeringDataParticleKey.PITCH
    PITCH_STDEV = EngineeringDataParticleKey.PITCH_STDEV
    ROLL = EngineeringDataParticleKey.ROLL
    ROLL_STDEV = EngineeringDataParticleKey.ROLL_STDEV
    ADCP_PRESSURE = EngineeringDataParticleKey.ADCP_PRESSURE
    ADCP_PRESSURE_STDEV = EngineeringDataParticleKey.ADCP_PRESSURE_STDEV
    ADCP_TEMPERATURE = EngineeringDataParticleKey.ADCP_TEMPERATURE

    CELL_NUMBER = "cell_number"
    CELL_POSITION = "cell_position"

    VELOCITY_BEAM1 = "velocity_beam1"
    VELOCITY_BEAM2 = "velocity_beam2"
    VELOCITY_BEAM3 = "velocity_beam3"
    VELOCITY_BEAM4 = "velocity_beam4"
    AMPLITUDE_BEAM1 = "amplitude_beam1"
    AMPLITUDE_BEAM2 = "amplitude_beam2"
    AMPLITUDE_BEAM3 = "amplitude_beam3"
    AMPLITUDE_BEAM4 = "amplitude_beam4"
    CORRELATION_BEAM1 = "correlation_beam1"
    CORRELATION_BEAM2 = "correlation_beam2"
    CORRELATION_BEAM3 = "correlation_beam3"
    CORRELATION_BEAM4 = "correlation_beam4"


TIME_IN = "%m%d%y%H%M%S"
TIME_OUT = "%Y%m%dT%H%M%S.000Z"

# Regex for header string (5th or main).
CONFIG_PARTICLE_PATTERN = re.compile(
    r"(?P<head_type>\d),(?P<serial_number>\d+),(?P<num_beams>\d+),(?P<num_cells>\d+),(?P<blanking_distance>\d+\.\d{2}),(?P<cell_size>\d+\.\d{2}),(?P<coordinate_system>BEAM|XYZ|ENU)"
)

# Regex for engineering data string (5th or main).
ENGINEERING_PARTICLE_PATTERN = re.compile(
    r"\$PNORS1,(?P<date>\d{6}),(?P<time>\d{6}),(?P<error_code>\d+),(?P<status_code>\d{8}),(?P<voltage>-?\d+[.]\d),(?P<speed_of_sound>\d+[.]\d),(?P<heading_stdev>\d+[.]\d{2}),(?P<heading>\d+[.]\d{1}),(?P<pitch>-?\d+[.]\d{1}),(?P<pitch_stdev>-?\d+[.]\d{2}),(?P<roll>-?\d+[.]\d{1}),(?P<roll_stdev>-?\d+[.]\d{2}),(?P<pressure>\d+[.]\d{3}),(?P<pressure_stdev>-?\d+[.]\d{2}),(?P<temperature>\d+[.]\d{2})\*.{2}"
)

# Regex for all of  or 5th beam science data string.
FULL_BEAM_PATTERN = re.compile(
    ENGINEERING_PARTICLE_PATTERN.pattern + r"\r?\n"
    r"(?:\$PNORC1[^\r\n]*?\r?\n)+"
)

#  Regex for 5th beam science data string.
FIFTH_BEAM_VALUE_PATTERN = re.compile(
    r"\$PNORC1,(?P<date>\d{6}),(?P<time>\d{6}),(?P<cell_number>\d+),(?P<cell_position>\d+[.]\d),(?P<velocity_beam5>-?\d+[.]\d{3}),(?P<amplitude_beam5>-?\d+[.]\d),(?P<correlation_beam5>\d+)"
)

# Regex for 4 beam science data string.
MAIN_BEAM_VALUE_PATTERN = re.compile(
    r"\$PNORC1,(?P<date>\d{6}),(?P<time>\d{6}),(?P<cell_number>\d+),(?P<cell_position>\d+[.]\d),(?P<velocity_beam1>-?\d+[.]\d{3}),(?P<velocity_beam2>-?\d+[.]\d{3}),(?P<velocity_beam3>-?\d+[.]\d{3}),(?P<velocity_beam4>-?\d+[.]\d{3}),(?P<amplitude_beam1>-?\d+[.]\d),(?P<amplitude_beam2>-?\d+[.]\d),(?P<amplitude_beam3>-?\d+[.]\d),(?P<amplitude_beam4>-?\d+[.]\d),(?P<correlation_beam1>\d+),(?P<correlation_beam2>\d+),(?P<correlation_beam3>\d+),(?P<correlation_beam4>\d+)"
)

# Regex to Grab the entire Data Block
FULL_CAPTURE_PATTERN = re.compile(
    r"(?P<header_5th>\$PNORI1,\d,\d{6},1,.*?,BEAM\*.{2})\r?\n"
    r"(?P<eng_5th>\$PNORS1,.*?)\r?\n"
    r"(?P<science_5th>(\$PNORC1[^\r\n]*\r?\n)+)"
    r"(?P<header_main>\$PNORI1,\d,\d{6},[3-4],.*?,BEAM\*.{2})\r?\n"
    r"(?P<eng_main>\$PNORS1,[^\r\n]*)\r?\n"
    r"(?P<science_main>(\$PNORC1,[^\r\n]*\r?\n)+)"
)


class ConfigDataParticle(DataParticle):
    _data_particle_type = DataParticleType.CONFIG

    @staticmethod
    def regex_compiled():
        return CONFIG_PARTICLE_PATTERN

    def _build_parsed_values(self):
        match = ConfigDataParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise InstrumentProtocolException(
                "ConfigDataParticle: No regex match of parsed data: %r"
                % self.raw_data,
            )

        keys = ConfigDataParticleKey
        result = [
            self._encode_value(keys.head_type, match.group("head_type"), int),
            self._encode_value(keys.serial_number, match.group("serial_number"), int),
            self._encode_value(keys.num_cells, match.group("num_cells"), int),
            self._encode_value(keys.blanking_distance, match.group("blanking_distance"), float),
            self._encode_value(keys.cell_size, match.group("cell_size"), float),
            self._encode_value(keys.coordinate_system, match.group("coordinate_system"), str),
        ]

        return result


class EngineeringDataParticle(DataParticle):
    @staticmethod
    def regex_compiled():
        return ENGINEERING_PARTICLE_PATTERN

    def _build_parsed_values(self):
        match = EngineeringDataParticle.regex_compiled().search(self.raw_data)
        if not match:
            raise InstrumentProtocolException(
                "EngineeringDataParticle: No regex match of parsed data: %r"
                % self.raw_data,
            )

        time = datetime.strptime(
            match.group("date") + match.group("time"),
            TIME_IN,
        )

        self.set_internal_timestamp((time - datetime(1900, 1, 1)).total_seconds())

        keys = EngineeringDataParticleKey
        result = [
            self._encode_value(keys.ERROR_CODE, match.group("error_code"), int),
            self._encode_value(keys.STATUS_CODE, match.group("status_code"), int),
            self._encode_value(keys.SPEED_OF_SOUND, match.group("speed_of_sound"), float),
            self._encode_value(keys.HEADING_STDEV, match.group("heading_stdev"), float),
            self._encode_value(keys.HEADING, match.group("heading"), float),
            self._encode_value(keys.PITCH, match.group("pitch"), float),
            self._encode_value(keys.PITCH_STDEV, match.group("pitch_stdev"), float),
            self._encode_value(keys.ROLL, match.group("roll"), float),
            self._encode_value(keys.ROLL_STDEV, match.group("roll_stdev"), float),
            self._encode_value(keys.ADCP_PRESSURE, match.group("pressure"), float),
            self._encode_value(keys.ADCP_PRESSURE_STDEV, match.group("pressure_stdev"), float),
            self._encode_value(keys.ADCP_TEMPERATURE, match.group("temperature"), float),
        ]

        return result


class VelocityMainBeamParticle(EngineeringDataParticle):
    _data_particle_type = DataParticleType.VELOCITY_MAIN_BEAM

    @staticmethod
    def regex_compiled():
        return FULL_BEAM_PATTERN

    def _build_parsed_values(self):
        keys = VelocityMainBeamDataParticleKey

        result = super(VelocityMainBeamParticle, self)._build_parsed_values()
        groups = defaultdict(list)

        for match in MAIN_BEAM_VALUE_PATTERN.finditer(self.raw_data):
            groups[keys.CELL_NUMBER].append(int(match.group("cell_number")))
            groups[keys.CELL_POSITION].append(float(match.group("cell_position")))
            groups[keys.VELOCITY_BEAM1].append(float(match.group("velocity_beam1")))
            groups[keys.VELOCITY_BEAM2].append(float(match.group("velocity_beam2")))
            groups[keys.VELOCITY_BEAM3].append(float(match.group("velocity_beam3")))
            groups[keys.VELOCITY_BEAM4].append(float(match.group("velocity_beam4")))
            groups[keys.AMPLITUDE_BEAM1].append(float(match.group("amplitude_beam1")))
            groups[keys.AMPLITUDE_BEAM2].append(float(match.group("amplitude_beam2")))
            groups[keys.AMPLITUDE_BEAM3].append(float(match.group("amplitude_beam3")))
            groups[keys.AMPLITUDE_BEAM4].append(float(match.group("amplitude_beam4")))
            groups[keys.CORRELATION_BEAM1].append(float(match.group("correlation_beam1")))
            groups[keys.CORRELATION_BEAM2].append(float(match.group("correlation_beam2")))
            groups[keys.CORRELATION_BEAM3].append(float(match.group("correlation_beam3")))
            groups[keys.CORRELATION_BEAM4].append(float(match.group("correlation_beam4")))

        for key in groups:
            result.append(self._encode_value(key, groups[key], list))

        return result

class VelocityFifthBeamParticle(EngineeringDataParticle):
    _data_particle_type = DataParticleType.VELOCITY_FIFTH_BEAM

    @staticmethod
    def regex_compiled():
        return FULL_BEAM_PATTERN

    def _build_parsed_values(self):
        keys = VelocityFifthBeamDataParticleKey

        result = super(VelocityFifthBeamParticle, self)._build_parsed_values()
        groups = defaultdict(list)

        for match in FIFTH_BEAM_VALUE_PATTERN.finditer(self.raw_data):
            groups[keys.CELL_NUMBER].append(int(match.group("cell_number")))
            groups[keys.CELL_POSITION].append(float(match.group("cell_position")))
            groups[keys.VELOCITY_BEAM5].append(float(match.group("velocity_beam5")))
            groups[keys.AMPLITUDE_BEAM5].append(float(match.group("amplitude_beam5")))
            groups[keys.CORRELATION_BEAM5].append(float(match.group("correlation_beam5")))

        for key in groups:
            result.append(self._encode_value(key, groups[key], list))

        return result

###############################################################################
# Driver
###############################################################################


class Parameter(BaseEnum):
    pass

class InstrumentDriver(SingleConnectionInstrumentDriver):
    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################

    @staticmethod
    def get_resource_params():
        """
        Return list of device parameters available.
        """
        return Parameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(self._driver_event)


###########################################################################
# Protocol
###########################################################################

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
    def __init__(self, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, Prompt, NEWLINE, driver_event)

        # Build protocol state machine.
        self._protocol_fsm = ThreadSafeFSM(
            ProtocolState,
            ProtocolEvent,
            ProtocolEvent.ENTER,
            ProtocolEvent.EXIT
        )

        # Add event handlers for protocol state machine.
        handlers = {
            ProtocolState.UNKNOWN: [
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.EXIT, self._handler_unknown_exit),
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

        self._chunker = StringChunker(Protocol.sieve_function, self._max_buffer_size())

        # Start state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples and status.
        """
        spans = []

        for match in FULL_CAPTURE_PATTERN.finditer(raw_data):
            spans.append((match.start(), match.end()))

        if not spans:
            log.debug(
                "sieve_function: raw_data=%r, return_list=%s", raw_data, spans
            )

        return spans

    def _max_buffer_size(self):
        return CommandResponseInstrumentProtocol._max_buffer_size(self) * 10

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        config_match = ConfigDataParticle.regex_compiled().search(chunk)
        fifth_beam_match = VelocityFifthBeamParticle.regex_compiled().search(chunk)
        main_beam_match = VelocityMainBeamParticle.regex_compiled().search(chunk)

        if not config_match:
            log.warning("No ConfigDataParticle match for chunk: %r", chunk)
            return
        if not fifth_beam_match:
            log.warning("No VelocityFifthBeamPaticle match for chunk: %r", chunk)
            return
        if not main_beam_match:
            log.warning("No VelocityMainBeamPaticle match for chunk: %r", chunk)
            return

        self._extract_sample(
            ConfigDataParticle,
            ConfigDataParticle.regex_compiled(),
            config_match.group(),
            timestamp,
        )

        self._extract_sample(
            VelocityFifthBeamParticle,
            VelocityFifthBeamParticle.regex_compiled(),
            fifth_beam_match.group(),
            timestamp,
        )

        self._extract_sample(
            VelocityMainBeamParticle,
            VelocityMainBeamParticle.regex_compiled(),
            main_beam_match.group(),
            timestamp,
        )

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    ########################################################################
    # implement virtual methods from base class.
    ########################################################################

    def _set_params(self, *args, **kwargs):
        """
        Issue commands to the instrument to set various parameters.  If
        startup is set to true that means we are setting startup values
        and immutable parameters can be set.  Otherwise only READ_WRITE
        parameters can be set.

        must be overloaded in derived classes

        @param params dictionary containing parameter name and value pairs
        @param startup - a flag, true indicates initializing, false otherwise
        """

        params = args[0]

        # check for attempt to set readonly parameters (read-only or immutable set outside startup)
        self._verify_not_readonly(*args, **kwargs)
        old_config = self._param_dict.get_config()

        for key, val in params.iteritems():
            log.debug("KEY = " + str(key) + " VALUE = " + str(val))
            self._param_dict.set_value(key, val)

        new_config = self._param_dict.get_config()
        # check for parameter change
        if not dict_equal(old_config, new_config):
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def apply_startup_params(self):
        """
        Apply startup parameters
        """

        config = self.get_startup_config()

        for param in Parameter.list():
            if param in config:
                self._param_dict.set_value(param, config[param])

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

    def _update_params(self):
        pass

    ########################################################################
    # Event handlers for UNKNOWN state.
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
        pass

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state
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
