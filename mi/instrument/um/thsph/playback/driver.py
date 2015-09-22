"""
@package mi.instrument.um.thsph.thsph.driver
@file marine-integrations/mi/instrument/um/thsph/thsph/driver.py
@author Richard Han
@brief Driver for the thsph
Release notes:

Vent Chemistry Instrument  Driver


"""

__author__ = 'Richard Han'
__license__ = 'Apache 2.0'

import re
from datetime import datetime

from mi.core.exceptions import SampleException, InstrumentProtocolException
from mi.core.log import get_logger

log = get_logger()

from mi.core.common import BaseEnum
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker



class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver
    """
    RAW = CommonDataParticleType.RAW
    THSPH_PARSED = 'thsph_sample'


###############################################################################
# Data Particles
###############################################################################
class THSPHDataParticleKey(BaseEnum):
    HIGH_IMPEDANCE_ELECTRODE_1 = "thsph_hie1"  # High Impedance Electrode 1 for pH
    HIGH_IMPEDANCE_ELECTRODE_2 = "thsph_hie2"  # High Impedance Electrode 2 for pH
    H2_ELECTRODE = "thsph_h2electrode"  # H2 electrode
    S2_ELECTRODE = "thsph_s2electrode"  # Sulfide Electrode
    THERMOCOUPLE1 = "thsph_thermocouple1"  # Type E thermocouple 1-high
    THERMOCOUPLE2 = "thsph_thermocouple2"  # Type E thermocouple 2-low
    REFERENCE_THERMISTOR = "thsph_rthermistor"  # Reference Thermistor
    BOARD_THERMISTOR = "thsph_bthermistor"  # Board Thermistor


class THSPHParticle(DataParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.

    The data signal is a concatenation of 8 channels of 14-bit resolution data.
    Each channel is output as a four ASCII character hexadecimal number (0000 to 3FFF).
    Each channel, 1-8, should be parsed as a 4 character hexadecimal number and converted
    to a raw decimal number.

    Sample:
       aH200A200720DE20AA10883FFF2211225E#

    Format:
       aHaaaabbbbccccddddeeeeffffgggghhhh#

       aaaa = Chanel 1 High Input Impedance Electrode;
       bbbb = Chanel 2 High Input Impedance Electrode;
       cccc = H2 Electrode;
       dddd = S2 Electrode;
       eeee = TYPE E Thermocouple 1;
       ffff = TYPE E Thermocouple 2;
       gggg = Thermistor;
       hhhh Board 2 Thermistor;

    """
    _data_particle_type = DataParticleType.THSPH_PARSED
    _compiled_regex = None
    ntp_epoch = datetime(1900, 1, 1)

    @staticmethod
    def regex():
        """
        Regular expression to match a sample pattern
        @return: regex string
        """
        pattern = r'aH'  # pattern starts with 'aH'
        pattern += r'([0-9A-F]{4})'  # Chanel 1 High Input Impedance Electrode
        pattern += r'([0-9A-F]{4})'  # Chanel 2 High Input Impedance Electrode
        pattern += r'([0-9A-F]{4})'  # H2 Electrode
        pattern += r'([0-9A-F]{4})'  # S2 Electrode
        pattern += r'([0-9A-F]{4})'  # Type E Thermocouple 1
        pattern += r'([0-9A-F]{4})'  # Type E Thermocouple 2
        pattern += r'([0-9A-F]{4})'  # Reference Thermistor
        pattern += r'([0-9A-F]{4})'  # Board Thermocouple
        pattern += r'#'  # pattern ends with '#'
        pattern += r'.*? board (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)'
        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        if THSPHParticle._compiled_regex is None:
            THSPHParticle._compiled_regex = re.compile(THSPHParticle.regex())
        return THSPHParticle._compiled_regex

    def _build_parsed_values(self):
        """
        Take something in the ADC data format and split it into
        Chanel 1 High Input Impedance Electrode, Chanel 2 High Input
        Impedance Electrode, H2 Electrode, S2 Electrode, Type E Thermocouple 1,
        Type E Thermocouple 2, Reference Thermistor, Board Thermistor

        @throws SampleException If there is a problem with sample creation
        """
        match = THSPHParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of THSPH parsed sample data: [%s]" %
                                  self.raw_data)

        try:
            electrode1 = int(match.group(1), 16)
            electrode2 = int(match.group(2), 16)
            h2electrode = int(match.group(3), 16)
            s2electrode = int(match.group(4), 16)
            thermocouple1 = int(match.group(5), 16)
            thermocouple2 = int(match.group(6), 16)
            ref_thermistor = int(match.group(7), 16)
            board_thermistor = int(match.group(8), 16)
            date_string = match.group(9)
            timestamp = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S %Z')
            self.set_internal_timestamp((timestamp - self.ntp_epoch).total_seconds())

        except ValueError:
            raise SampleException("ValueError while converting data: [%s]" %
                                  self.raw_data)

        result = [{DataParticleKey.VALUE_ID: THSPHDataParticleKey.HIGH_IMPEDANCE_ELECTRODE_1,
                   DataParticleKey.VALUE: electrode1},
                  {DataParticleKey.VALUE_ID: THSPHDataParticleKey.HIGH_IMPEDANCE_ELECTRODE_2,
                   DataParticleKey.VALUE: electrode2},
                  {DataParticleKey.VALUE_ID: THSPHDataParticleKey.H2_ELECTRODE,
                   DataParticleKey.VALUE: h2electrode},
                  {DataParticleKey.VALUE_ID: THSPHDataParticleKey.S2_ELECTRODE,
                   DataParticleKey.VALUE: s2electrode},
                  {DataParticleKey.VALUE_ID: THSPHDataParticleKey.THERMOCOUPLE1,
                   DataParticleKey.VALUE: thermocouple1},
                  {DataParticleKey.VALUE_ID: THSPHDataParticleKey.THERMOCOUPLE2,
                   DataParticleKey.VALUE: thermocouple2},
                  {DataParticleKey.VALUE_ID: THSPHDataParticleKey.REFERENCE_THERMISTOR,
                   DataParticleKey.VALUE: ref_thermistor},
                  {DataParticleKey.VALUE_ID: THSPHDataParticleKey.BOARD_THERMISTOR,
                   DataParticleKey.VALUE: board_thermistor}]

        return result


###########################################################################
# Protocol
###########################################################################
class THSPHProtocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """
    def __init__(self, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        CommandResponseInstrumentProtocol.__init__(self, None, None, driver_event)
        self._chunker = StringChunker(self.sieve_function)

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        """
        matchers = []
        return_list = []

        matchers.append(THSPHParticle.regex_compiled())

        for matcher in matchers:
            log.trace('matcher: %r raw_data: %r', matcher.pattern, raw_data)
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if not self._extract_sample(THSPHParticle, THSPHParticle.regex_compiled(), chunk, timestamp):
            raise InstrumentProtocolException("Unhandled chunk")

    def get_current_state(self):
        return DriverProtocolState.UNKNOWN
