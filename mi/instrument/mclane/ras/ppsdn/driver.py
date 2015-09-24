"""
@package mi.instrument.mclane.rasfl.pps.driver
@file marine-integrations/mi/instrument/mclane/ras/ooicore/driver.py
@author Dan Mergens
@brief Driver for the PPSDN
Release notes:

initial version
"""

__author__ = 'Dan Mergens'
__license__ = 'Apache 2.0'

import re

from mi.core.log import get_logger

log = get_logger()

from mi.core.common import Units, Prefixes
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver

from mi.core.instrument.protocol_param_dict import \
    ProtocolParameterDict, \
    ParameterDictType, \
    ParameterDictVisibility

from mi.instrument.mclane.driver import \
    NEWLINE, \
    ProtocolEvent, \
    Parameter, \
    Prompt, \
    McLaneCommand, \
    McLaneResponse, \
    McLaneDataParticleType, \
    McLaneSampleDataParticle, \
    McLaneProtocol, \
    ProtocolState

from mi.instrument.mclane.ras.ppsdn.ppsdn_persistent_store import PpsdnPersistentStoreDict # TODO: Use this to add data persistence

NUM_PORTS = 24  # number of collection filters

####
#    Driver Constant Definitions
####


FLUSH_VOLUME = 150
FLUSH_RATE = 100
FLUSH_MIN_RATE = 75
FILL_VOLUME = 4000
FILL_RATE = 100
FILL_MIN_RATE = 75
CLEAR_VOLUME = 100
CLEAR_RATE = 100
CLEAR_MIN_RATE = 75


class Command(McLaneCommand):
    """
    Instrument command strings - case insensitive
    """
    CAPACITY = 'capacity'  # pump max flow rate mL/min


class Response(McLaneResponse):
    """
    Expected device response strings
    """
    # e.g. 03/25/14 20:24:02 PPS ML13003-01>
    READY = re.compile(r'(\d+/\d+/\d+\s+\d+:\d+:\d+\s+)PPS (.*)>')
    # Capacity: Maxon 250mL
    CAPACITY = re.compile(r'Capacity:\s(Maxon|Pittman)\s+(\d+)mL')  # pump make and capacity
    # McLane Research Laboratories, Inc.
    # CF2 Adaptive Water Transfer System
    # Version 2.02  of Jun  7 2013 18:17
    #  Configured for: Maxon 250ml pump
    VERSION = re.compile(
        r'McLane .*$' + NEWLINE +
        r'CF2 .*$' + NEWLINE +
        r'Version\s+(\S+)\s+of\s+(.*)$' + NEWLINE +  # version and release date
        r'.*$'
    )


class DataParticleType(McLaneDataParticleType):
    """
    Data particle types produced by this driver
    """
    # TODO - define which commands will be published to user
    PPSDN_PARSED = 'ppsdn_sample_result'


###############################################################################
# Data Particles
###############################################################################

# class PPSDNSampleDataParticleKey(McLaneSampleDataParticleKey):
#    # TODO - just use base class
#    PUMP_STATUS = 'pump_status'
#    PORT = 'port'
#    VOLUME_COMMANDED = 'volume_commanded'
#    FLOW_RATE_COMMANDED = 'flow_rate_commanded'
#    MIN_FLOW_COMMANDED = 'min_flow_commanded'
#    TIME_LIMIT = 'time_limit'
#    VOLUME_ACTUAL = 'volume'
#    FLOW_RATE_ACTUAL = 'flow_rate'
#    MIN_FLOW_ACTUAL = 'min_flow'
#    TIMER = 'elapsed_time'
#    DATE = 'date'
#    TIME = 'time_of_day'
#    BATTERY = 'battery_voltage'
#    CODE = 'code'
#

# data particle for forward, reverse, and result commands
#  e.g.:
#                      --- command ---   -------- result -------------
#     Result port  |  vol flow minf tlim  |  vol flow minf secs date-time  |  batt code
#        Status 00 |  75 100  25   4 |   1.5  90.7  90.7*  1 031514 001727 | 29.9 0
class PPSDNSampleDataParticle(McLaneSampleDataParticle):
    _data_particle_type = DataParticleType.PPSDN_PARSED


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
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################

# noinspection PyMethodMayBeStatic,PyUnusedLocal
class Protocol(McLaneProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        McLaneProtocol.__init__(self, prompts, newline, driver_event)

        # TODO - reset next_port on mechanical refresh of the PPS filters - how is the driver notified?
        # TODO - need to persist state for next_port to save driver restart
        self.next_port = 1  # next available port

        # get the following:
        # - VERSION
        # - CAPACITY (pump flow)
        # - BATTERY
        # - CODES (termination codes)
        # - COPYRIGHT (termination codes)

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        filling = self.get_current_state() in [ProtocolState.FILL, ProtocolState.UNKNOWN]
        log.debug("_got_chunk:\n%s", chunk)
        sample_dict = self._extract_sample(PPSDNSampleDataParticle,
                                           PPSDNSampleDataParticle.regex_compiled(), chunk, timestamp, publish=filling)

        if sample_dict and self.get_current_state() != ProtocolState.UNKNOWN:
            self._linebuf = ''
            self._promptbuf = ''
            self._protocol_fsm.on_event(ProtocolEvent.PUMP_STATUS,
                                        PPSDNSampleDataParticle.regex_compiled().search(chunk))

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with XR-420 parameters.
        For each parameter key add value formatting function for set commands.
        """
        # The parameter dictionary.
        self._param_dict = ProtocolParameterDict()

        # Add parameter handlers to parameter dictionary for instrument configuration parameters.
        self._param_dict.add(Parameter.FLUSH_VOLUME,
                             r'Flush Volume: (.*)mL',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FLUSH_VOLUME,
                             units=Prefixes.MILLI + Units.LITER,
                             startup_param=True,
                             display_name="Flush Volume",
                             description="Amount of sea water to flush prior to taking sample: (10 - 20000)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FLUSH_FLOWRATE,
                             r'Flush Flow Rate: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FLUSH_RATE,
                             units=Prefixes.MILLI + Units.LITER + '/' + Units.MINUTE,
                             startup_param=True,
                             display_name="Flush Flow Rate",
                             description="Rate at which to flush: (100 - 250)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FLUSH_MINFLOW,
                             r'Flush Min Flow: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FLUSH_MIN_RATE,
                             units=Prefixes.MILLI + Units.LITER + '/' + Units.MINUTE,
                             startup_param=True,
                             display_name="Flush Minimum Flow",
                             description="If the flow rate falls below this value the flush will be terminated, "
                                         "obstruction suspected: (75 - 100)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FILL_VOLUME,
                             r'Fill Volume: (.*)mL',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FILL_VOLUME,
                             units=Prefixes.MILLI + Units.LITER,
                             startup_param=True,
                             display_name="Fill Volume",
                             description="Amount of seawater to run through the collection filter (10 - 20000)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FILL_FLOWRATE,
                             r'Fill Flow Rate: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FILL_RATE,
                             units=Prefixes.MILLI + Units.LITER + '/' + Units.MINUTE,
                             startup_param=True,
                             display_name="Fill Flow Rate",
                             description="Flow rate during sampling: (100 - 250)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FILL_MINFLOW,
                             r'Fill Min Flow: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FILL_MIN_RATE,
                             units=Prefixes.MILLI + Units.LITER + '/' + Units.MINUTE,
                             startup_param=True,
                             display_name="Fill Minimum Flow",
                             description="If the flow rate falls below this value the fill will be terminated, "
                                         "obstruction suspected: (75 - 100)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.CLEAR_VOLUME,
                             r'Reverse Volume: (.*)mL',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=CLEAR_VOLUME,
                             units=Prefixes.MILLI + Units.LITER,
                             startup_param=True,
                             display_name="Clear Volume",
                             description="Amount of sea water to flush the home port after taking sample: (10 - 20000)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.CLEAR_FLOWRATE,
                             r'Reverse Flow Rate: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=CLEAR_RATE,
                             units=Prefixes.MILLI + Units.LITER + '/' + Units.MINUTE,
                             startup_param=True,
                             display_name="Clear Flow Rate",
                             description="Rate at which to flush: (100 - 250)",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.CLEAR_MINFLOW,
                             r'Reverse Min Flow: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=CLEAR_MIN_RATE,
                             units=Prefixes.MILLI + Units.LITER + '/' + Units.MINUTE,
                             startup_param=True,
                             display_name="Clear Minimum Flow",
                             description="If the flow rate falls below this value the reverse flush will be terminated, "
                                         "obstruction suspected: (75 - 100)",
                             visibility=ParameterDictVisibility.IMMUTABLE)

        self._param_dict.set_value(Parameter.FLUSH_VOLUME, FLUSH_VOLUME)
        self._param_dict.set_value(Parameter.FLUSH_FLOWRATE, FLUSH_RATE)
        self._param_dict.set_value(Parameter.FLUSH_MINFLOW, FLUSH_MIN_RATE)
        self._param_dict.set_value(Parameter.FILL_VOLUME, FILL_VOLUME)
        self._param_dict.set_value(Parameter.FILL_FLOWRATE, FILL_RATE)
        self._param_dict.set_value(Parameter.FILL_MINFLOW, FILL_MIN_RATE)
        self._param_dict.set_value(Parameter.CLEAR_VOLUME, CLEAR_VOLUME)
        self._param_dict.set_value(Parameter.CLEAR_FLOWRATE, CLEAR_RATE)
        self._param_dict.set_value(Parameter.CLEAR_MINFLOW, CLEAR_MIN_RATE)
