"""
@package mi.instrument.seabird.sbe16plus_v2.dofst.driver
@file mi/instrument/seabird/sbe16plus_v2/dofst/driver.py
@author Mark Steiner
@brief Driver class for dissolved oxygen sensor for the sbe16plus V2 CTDPF-SBE43 instrument.
"""

import re

from mi.core.common import BaseEnum
from mi.core.log import get_logger
from mi.core.util import hex2value

from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import CommonDataParticleType, DataParticle, DataParticleKey
from mi.core.instrument.driver_dict import DriverDictKey
from mi.core.instrument.instrument_driver import DriverAsyncEvent, DriverEvent, DriverProtocolState
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol

from mi.core.exceptions import SampleException

from mi.instrument.seabird.sbe16plus_v2.driver import Prompt, NEWLINE


__author__ = 'Mark Steiner'
__license__ = 'Apache 2.0'

log = get_logger()


########################################
# Finite State Machine Configuration
#  - bare-bones, allowing discovery only
########################################
class ProtocolState(BaseEnum):
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND


class ProtocolEvent(BaseEnum):
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    DISCOVER = DriverEvent.DISCOVER


class Capability(BaseEnum):
    GET = ProtocolEvent.GET
    DISCOVER = ProtocolEvent.DISCOVER


########################################
# Particle Definitions
########################################
class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    DO_SAMPLE = 'do_fast_sample'


class DoFastSampleParticleKey(BaseEnum):
    EXT_VOLT0 = "ext_volt0"


class DoFastSampleParticle(DataParticle):
    """
    Class for handling the DO fast sample coming from CTDPF-SBE43.

    Sample:
       04570F0A1E910828FC47BC59F1 - CTDPF-SBE43

    Format:
       ttttttccccccppppppTTTTvvvv

       Temperature = tttttt
       Conductivity = cccccc
       quartz pressure = pppppp
       quartz pressure temperature compensation = TTTT
       First external voltage = vvvv (ext_volt0)
    """
    _data_particle_type = DataParticleType.DO_SAMPLE

    @staticmethod
    def regex():
        """
        This driver should only be used for instruments known to be
        configured with an optode, so it may be unnecessary to allow
        for missing optode records.
        """
        pattern = r'([0-9A-F]{6})'   # temperature
        pattern += r'([0-9A-F]{6})'  # conductivity
        pattern += r'([0-9A-F]{6})'  # pressure
        pattern += r'([0-9A-F]{4})'  # pressure temp
        pattern += r'([0-9A-F]{4})'  # volt0
        pattern += NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(DoFastSampleParticle.regex())

    def _build_parsed_values(self):
        """
        Convert the instrument sample into a data particle.
        :return: data particle as a dictionary
        """
        match = DoFastSampleParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" % self.raw_data)

        volt0 = hex2value(match.group(5))

        result = [{DataParticleKey.VALUE_ID: DoFastSampleParticleKey.EXT_VOLT0,
                   DataParticleKey.VALUE: volt0}]

        return result


###############################################################################
# Seabird Electronics 16plus V2 NO Attached DOSTA Driver.
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


####################################################################################
# Command Protocols - read-only, the attached CTD is used to control the instrument
####################################################################################
class Protocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class for SBE16 DOSTA driver.
    """

    particles = [
        DoFastSampleParticle,
    ]

    def __init__(self, prompts, newline, driver_event):
        """
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The SBE16 newline.
        @param driver_event Driver process event callback.
        """
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        # This driver does not process commands, the finite state machine and handlers are stubs
        self._protocol_fsm = ThreadSafeFSM(ProtocolState, ProtocolEvent,
                                           ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        handlers = {
            ProtocolState.UNKNOWN: {
                (ProtocolEvent.ENTER, self._handler_state_change()),
                (ProtocolEvent.EXIT, self._handler_pass_through()),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover()),
            },
            ProtocolState.COMMAND: {
                (ProtocolEvent.ENTER, self._handler_state_change()),
                (ProtocolEvent.EXIT, self._handler_pass_through()),
                (ProtocolEvent.GET, self._handler_pass_through()),
            },
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        self._build_param_dict()
        self._build_command_dict()
        self._build_driver_dict()

        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        self._chunker = StringChunker(self.sieve_function)

    @staticmethod
    def sieve_function(raw_data):
        """ The method that splits samples
        Over-ride sieve function to handle additional particles.
        """
        matchers = []
        return_list = []

        matchers.append(DoFastSampleParticle.regex_compiled())
        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _build_command_dict(self):
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover', timeout=1)

    def _build_param_dict(self):
        pass
        # self._param_dict.add(Parameter.OPTODE,
        #                      r'OPTODE>(.*)</OPTODE',
        #                      lambda match: True if match.group(1) == 'yes' else False,
        #                      self._true_false_to_string,
        #                      type=ParameterDictType.BOOL,
        #                      display_name="Optode Attached",
        #                      description="Enable optode: (true | false)",
        #                      range={'True': True, 'False': False},
        #                      startup_param=True,
        #                      direct_access=True,
        #                      default_value=True,
        #                      visibility=ParameterDictVisibility.IMMUTABLE)
        # self._param_dict.add(Parameter.VOLT1,
        #                      r'ExtVolt1>(.*)</ExtVolt1',
        #                      lambda match: True if match.group(1) == 'yes' else False,
        #                      self._true_false_to_string,
        #                      type=ParameterDictType.BOOL,
        #                      display_name="Volt 1",
        #                      description="Enable external voltage 1: (true | false)",
        #                      range={'True': True, 'False': False},
        #                      startup_param=True,
        #                      direct_access=True,
        #                      default_value=True,
        #                      visibility=ParameterDictVisibility.IMMUTABLE)

    def _got_chunk(self, chunk, timestamp):
        """
        Over-ride sieve function to handle additional particles.
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if self._extract_sample(DoFastSampleParticle, DoFastSampleParticle.regex_compiled(), chunk, timestamp):
            self._sampling = True
            return

    def _build_driver_dict(self):
        """
        Apparently VENDOR_SW_COMPATIBLE is required (TODO - move to the base class)
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, False)

    ####################
    # Command Handlers
    ####################
    def _handler_pass_through(self):
        pass

    def _handler_state_change(self):
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    # noinspection PyMethodMayBeStatic
    def _handler_unknown_discover(self):
        next_state = ProtocolState.COMMAND
        return next_state, (next_state, None)

    def _handler_command_enter(self):
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_command_get(self, *args, **kwargs):
        next_state, result = self._handler_get(*args, **kwargs)
        # TODO - need to find out why this doesn't match other handler return signatures:
        # TODO   (next_state, (next_state, result)
        return next_state, result


def create_playback_protocol(callback):
    return Protocol(None, None, callback)

