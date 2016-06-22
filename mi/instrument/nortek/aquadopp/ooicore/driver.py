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
        self.clock_particle = AquadoppDataParticleType.CLOCK
        self.status_particles = [AquadoppDataParticleType.CLOCK, AquadoppDataParticleType.HARDWARE_CONFIG,
                                 AquadoppDataParticleType.HEAD_CONFIG, AquadoppDataParticleType.USER_CONFIG]

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


def create_playback_protocol(callback):
    return Protocol(None, None, callback)
