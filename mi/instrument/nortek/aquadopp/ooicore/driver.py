"""
@package mi.instrument.nortek.aquadopp.ooicore.driver
@author Rachel Manoni, Ronald Ronquillo
@brief Driver for the ooicore
Release notes:

Driver for Aquadopp DW
"""
import re

from mi.core.common import Units
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.log import get_logger
from mi.instrument.nortek import common
from mi.instrument.nortek.driver import (NortekInstrumentProtocol,
                                         InstrumentPrompts,
                                         Parameter)
from mi.instrument.nortek.particles import (AquadoppHardwareConfigDataParticle,
                                            AquadoppHeadConfigDataParticle,
                                            AquadoppUserConfigDataParticle,
                                            AquadoppEngClockDataParticle,
                                            AquadoppEngBatteryDataParticle,
                                            AquadoppEngIdDataParticle,
                                            AquadoppVelocityDataParticle,
                                            AquadoppDataParticleType)

__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

log = get_logger()

VELOCITY_DATA_LEN = 42
VELOCITY_DATA_SYNC_BYTES = '\xa5\x01\x15\x00'

VELOCITY_DATA_PATTERN = r'%s.{38}' % VELOCITY_DATA_SYNC_BYTES
VELOCITY_DATA_REGEX = re.compile(VELOCITY_DATA_PATTERN, re.DOTALL)


###############################################################################
# Driver
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Protocol builder.
    ########################################################################
    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(InstrumentPrompts, common.NEWLINE, self._driver_event)


###############################################################################
# Protocol
################################################################################


class Protocol(NortekInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses NortekInstrumentProtocol
    """

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        super(Protocol, self).__init__(prompts, newline, driver_event)

        # create chunker for processing instrument samples.
        self._chunker = StringChunker(self.sieve_function)
        self.velocity_sync_bytes = VELOCITY_DATA_SYNC_BYTES

    ########################################################################
    # overridden superclass methods
    ########################################################################
    @staticmethod
    def sieve_function(raw_data):
        """
        The method that detects data sample structures from instrument
        Should be in the format [[structure_sync_bytes, structure_len]*]
        """
        return_list = []
        sieve_matchers = common.NORTEK_COMMON_REGEXES + [VELOCITY_DATA_REGEX]

        for matcher in sieve_matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))
                log.debug("sieve_function: regex found %r", raw_data[match.start():match.end()])

        return return_list

    def _got_chunk(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if any((
                self._extract_sample(AquadoppHardwareConfigDataParticle, common.HARDWARE_CONFIG_DATA_REGEX, structure,
                                     timestamp),
                self._extract_sample(AquadoppHeadConfigDataParticle, common.HEAD_CONFIG_DATA_REGEX, structure, timestamp),
                self._extract_sample(AquadoppUserConfigDataParticle, common.USER_CONFIG_DATA_REGEX, structure, timestamp),
                self._extract_sample(AquadoppEngClockDataParticle, common.CLOCK_DATA_REGEX, structure, timestamp),
                self._extract_sample(AquadoppEngBatteryDataParticle, common.ID_BATTERY_DATA_REGEX, structure, timestamp),
                self._extract_sample(AquadoppEngIdDataParticle, common.ID_DATA_REGEX, structure, timestamp),
                self._extract_sample(AquadoppVelocityDataParticle, VELOCITY_DATA_REGEX, structure, timestamp),
        )):
            return

    def _handler_command_acquire_status(self, *args, **kwargs):
        """
        High level command for the operator to get all of the status from the instrument:
        Battery voltage, clock, hw configuration, head configuration, user configuration, and identification string
        """
        next_state, (next_state, _) = super(Protocol, self)._handler_command_acquire_status(*args, **kwargs)

        result = self.wait_for_particles([AquadoppDataParticleType.CLOCK, AquadoppDataParticleType.HARDWARE_CONFIG,
                                          AquadoppDataParticleType.HEAD_CONFIG, AquadoppDataParticleType.USER_CONFIG])

        return next_state, (next_state, result)

    def _clock_sync(self, *args, **kwargs):
        """
        The mechanics of synchronizing a clock
        @throws InstrumentCommandException if the clock was not synchronized
        """
        super(Protocol, self)._clock_sync(*args, **kwargs)
        clock_particle = self.wait_for_particles(AquadoppDataParticleType.CLOCK, 0)
        return clock_particle

    def _build_param_dict(self):
        """
        Overwrite base classes method.
        Creates base class's param dictionary, then sets parameter values for those specific to this instrument.
        """
        NortekInstrumentProtocol._build_param_dict(self)

        self._param_dict.add_basic(Parameter.TRANSMIT_PULSE_LENGTH,
                                   display_name="Transmit Pulse Length",
                                   range=(0, 65535),
                                   description="Pulse duration of the transmitted signal.",
                                   default_value=125,
                                   units=Units.COUNTS)
        self._param_dict.add_basic(Parameter.BLANKING_DISTANCE,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Blanking Distance",
                                   description="Minimum sensing range of the sensor. (0-65535)",
                                   range=(0, 65535),
                                   default_value=49,
                                   units=Units.COUNTS)
        self._param_dict.add_basic(Parameter.RECEIVE_LENGTH,
                                   display_name="Receive Length",
                                   description="Length of the received pulse.",
                                   range=(0, 65535),
                                   default_value=32,
                                   units=Units.COUNTS)
        self._param_dict.add_basic(Parameter.TIME_BETWEEN_PINGS,
                                   display_name="Time Between Pings",
                                   description="Length of time between each ping.",
                                   range=(0, 65535),
                                   units=Units.COUNTS,
                                   default_value=437)
        self._param_dict.add_basic(Parameter.TIME_BETWEEN_BURST_SEQUENCES,
                                   display_name="Time Between Burst Sequences",
                                   description="Length of time between each burst.",
                                   range=(0, 65535),
                                   default_value=512,
                                   units=Units.COUNTS)
        self._param_dict.add_basic(Parameter.NUMBER_PINGS,
                                   display_name="Number Pings",
                                   description="Number of pings in each burst sequence.",
                                   range=(0, 65535),
                                   default_value=1,
                                   units=Units.COUNTS)
        self._param_dict.add_basic(Parameter.AVG_INTERVAL,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Average Interval",
                                   description="Interval for continuous sampling. (1-65535)",
                                   range=(1, 65535),
                                   default_value=60,
                                   units=Units.SECOND)
        self._param_dict.add_basic(Parameter.MEASUREMENT_INTERVAL,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Measurement Interval",
                                   description="Interval for single measurements. (1-65535)",
                                   range=(1, 65535),
                                   default_value=60,
                                   units=Units.SECOND)
        self._param_dict.add_basic(Parameter.DIAGNOSTIC_INTERVAL,
                                   display_name="Diagnostic Interval",
                                   description='Number of seconds between diagnostics measurements.',
                                   range=(0, 65535),
                                   default_value=11250,
                                   units=Units.SECOND)
        self._param_dict.add_basic(Parameter.ADJUSTMENT_SOUND_SPEED,
                                   display_name="Adjustment Sound Speed",
                                   description='User input sound speed adjustment factor.',
                                   range=(0, 65535),
                                   units=Units.METER + '/' + Units.SECOND,
                                   default_value=1525)
        self._param_dict.add_basic(Parameter.NUMBER_SAMPLES_DIAGNOSTIC,
                                   display_name="Diagnostic Samples",
                                   description='Number of samples in diagnostics mode.',
                                   range=(0, 65535),
                                   default_value=20)
        self._param_dict.add_basic(Parameter.SW_VERSION,
                                   type=ParameterDictType.STRING,
                                   display_name="Software Version",
                                   description="Current software version installed on instrument.",
                                   range=(0, 65535),
                                   default_value=13902)
        self._param_dict.add_basic(Parameter.ANALOG_OUTPUT_SCALE,
                                   display_name="Analog Output Scale Factor",
                                   description="Scale factor used in calculating analog output.",
                                   range=(0, 65535),
                                   default_value=0)
        self._param_dict.add_basic(Parameter.SAMPLE_RATE,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="Sample Rate",
                                   description="Number of samples per burst.",
                                   startup_param=False,
                                   direct_access=False)


def create_playback_protocol(callback):
    return Protocol(None, None, callback)
