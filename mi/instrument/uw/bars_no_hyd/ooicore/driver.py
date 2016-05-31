"""
@package mi.instrument.uw.bars_no_hyd.ooicore.driver
@file mi/instrument/uw/bars_no_hyd/ooicore/driver.py
@author Kirk Hunt
@brief Driver for the BARS instrument with no hydrogen sensors attached
Release notes:
This supports the UW BARS instrument from the Marv Tilley lab
"""
__author__ = 'Kirk Hunt'

from mi.core.common import BaseEnum

from mi.core.exceptions import SampleException
from mi.core.exceptions import InstrumentProtocolException

from mi.core.log import get_logger
from mi.core.log import get_logging_metaclass

from mi.core.instrument.data_particle import DataParticleKey

from mi.instrument.uw.bars.ooicore.driver import InstrumentDriver, Protocol, BarsDataParticle,\
    MENU, Prompt, NEWLINE, SAMPLE_REGEX

log = get_logger()


class BarsNoHydDataParticleKey(BaseEnum):
    RESISTIVITY_5 = "resistivity_5"
    RESISTIVITY_X1 = "resistivity_x1"
    RESISTIVITY_X5 = "resistivity_x5"
    REFERENCE_TEMP_VOLTS = "ref_temp_volts"
    REFERENCE_TEMP_DEG_C = "ref_temp_degc"
    RESISTIVITY_TEMP_VOLTS = "resistivity_temp_volts"
    RESISTIVITY_TEMP_DEG_C = "resistivity_temp_degc"
    BATTERY_VOLTAGE = "battery_voltage"

class BarsNoHydDataParticle(BarsDataParticle):

    def _build_parsed_values(self):
        """
        Take something in the sample format and split it into
        a PAR values (with an appropriate tag)

        @throw SampleException If there is a problem with sample creation
        """

        match = BarsDataParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" %
                                  self.raw_data)

        log.debug("Matching Sample Data Particle %r", match.groups())
        res_5 = float(match.group(1))
        res_x1 = float(match.group(2))
        res_x5 = float(match.group(3))
        ref_temp_v = float(match.group(8))
        ref_temp_c = float(match.group(9))
        res_temp_v = float(match.group(10))
        res_temp_c = float(match.group(11))
        batt_v = float(match.group(12))

        result = [{DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.RESISTIVITY_5,
                   DataParticleKey.VALUE: res_5},
                  {DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.RESISTIVITY_X1,
                   DataParticleKey.VALUE: res_x1},
                  {DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.RESISTIVITY_X5,
                   DataParticleKey.VALUE: res_x5},
                  {DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.REFERENCE_TEMP_VOLTS,
                   DataParticleKey.VALUE: ref_temp_v},
                  {DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.REFERENCE_TEMP_DEG_C,
                   DataParticleKey.VALUE: ref_temp_c},
                  {DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.RESISTIVITY_TEMP_VOLTS,
                   DataParticleKey.VALUE: res_temp_v},
                  {DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.RESISTIVITY_TEMP_DEG_C,
                   DataParticleKey.VALUE: res_temp_c},
                  {DataParticleKey.VALUE_ID: BarsNoHydDataParticleKey.BATTERY_VOLTAGE,
                   DataParticleKey.VALUE: batt_v}]

        return result

###############################################################################
# Driver
###############################################################################

class BarsNoHydInstrumentDriver(InstrumentDriver):
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
        self._protocol = BarsNoHydProtocol(MENU, Prompt, NEWLINE, self._driver_event)


###############################################################################
# Protocol
################################################################################

class BarsNoHydProtocol(Protocol):
    """
    Instrument protocol class
    Subclasses Protocol
    """
    __metaclass__ = get_logging_metaclass(log_level='debug')

    def __init__(self, menu, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """

        # Construct protocol superclass.
        Protocol.__init__(self, menu, prompts, newline, driver_event)

    def _got_chunk(self, chunk, timestamp):
        """
        extract samples from a chunk of data using the BarsNoHydDataParticle
        @param chunk: bytes to parse into a sample.
        """

        if not (self._extract_sample(BarsNoHydDataParticle, SAMPLE_REGEX, chunk, timestamp) or
                    self._extract_sample(BarsNoHydDataParticle, BarsNoHydDataParticle.regex_compiled(), chunk, timestamp)):
            raise InstrumentProtocolException("Unhandled chunk")

