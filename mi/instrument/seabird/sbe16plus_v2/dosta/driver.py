"""
@package mi.instrument.seabird.sbe16plus_v2.dosta.driver
@file mi/instrument/seabird/sbe16plus_v2/dosta/driver.py
@author Dan Mergens
@brief Driver class for dissolved oxygen sensor for the sbe16plus V2 CTD instrument.
"""

import re
import time

from mi.core.log import get_logger
from mi.core.common import BaseEnum

from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.instrument_protocol import NoCommandInstrumentProtocol

from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import SampleException

from xml.dom.minidom import parseString

from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import OptodeCommands
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Parameter
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Command
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SendOptodeCommand
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19Protocol
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19DataParticle
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19StatusParticle
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19ConfigurationParticle
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import OptodeSettingsParticle

from mi.instrument.seabird.sbe16plus_v2.driver import \
    Prompt, SBE16InstrumentDriver, Sbe16plusBaseParticle, WAKEUP_TIMEOUT, NEWLINE, TIMEOUT, ProtocolState
from mi.core.instrument.protocol_param_dict import ParameterDictType, ParameterDictVisibility


__author__ = 'Dan Mergens'
__license__ = 'Apache 2.0'

log = get_logger()


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    DO_SAMPLE = 'do_sample'


class DostaDataParticle(SBE19DataParticle):
    """
    This data particle is identical to the corresponding one for CTDPF-Optode, except for the stream
    name, which we specify here
    """
    _data_particle_type = DataParticleType.DO_SAMPLE


class DoSampleParticleKey(DataParticleKey):
    OXYGEN = "oxygen"
    OXY_CALPHASE = "oxy_calphase"
    OXY_TEMP = "oxy_temp"
    EXT_VOLT0 = "ext_volt0"


class DoSampleParticle(Sbe16plusBaseParticle):
    """
    Class for handling the DO sample coming from CTDBP-N/O series as well
    as CTDPF-A/B series instruments.

    Sample:
       #04570F0A1E910828FC47BC59F199952C64C9

    Format:
       #ttttttccccccppppppvvvvvvvvvvvvoooooo

       Temperature = tttttt
       Conductivity = cccccc
       quartz pressure = pppppp
       quartz pressure temperature compensation = vvvv
       First external voltage = vvvv
       Second external voltage = vvvv
       Oxygen = oooooo
    """
    _data_particle_type = DataParticleType.DO_SAMPLE

    @staticmethod
    def regex():
        """
        this driver should only be used for instruments known to be
        configured with an optode, so it may be unnecessary to allow
        for missing optode records...
        """
        pattern = r'#? *'  # patter may or may not start with a '
        pattern += r'([0-9A-F]{22})'  # temp, cond, pres, pres temp
        pattern += r'([0-9A-F]{0,14})'  # volt0, volt1, oxygen
        pattern += NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(DoSampleParticle.regex())

    def _build_parsed_values(self):
        """
        Convert the instrument sample into a data particle.
        :return: data particle as a dictionary
        """
        match = DoSampleParticle.regex_compiled().match(self.raw_data)

        if not match:
            raise SampleException("No regex match of parsed sample data: [%s]" % self.raw_data)

        try:
            optode = match.group(5)

        except ValueError:
            raise SampleException("ValueError while converting data: [%s]" % self.raw_data)

        result = []

        if optode:
            oxy_calphase = self.hex2value(optode[:4])
            optode = optode[4:]
            if optode:
                oxy_temp = self.hex2value(optode[:4])
                optode = optode[4:]
                oxygen = self.hex2value(optode)

            result = [{DataParticleKey.VALUE_ID: DoSampleParticleKey.OXY_CALPHASE,
                       DataParticleKey.VALUE: oxy_calphase},
                      {DataParticleKey.VALUE_ID: DoSampleParticleKey.OXY_TEMP,
                       DataParticleKey.VALUE: oxy_temp},
                      {DataParticleKey.VALUE_ID: DoSampleParticleKey.OXYGEN,
                       DataParticleKey.VALUE: oxygen}
                      ]
        else:
            log.warning('Expected optode data missing from CTD record')

        return result


###############################################################################
# Seabird Electronics 16plus V2 NO Driver.
###############################################################################
class InstrumentDriver(NoCommandInstrumentProtocol):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################
    def get_resource_params(self):
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
        self._protocol = DostaProtocol(Prompt, NEWLINE, self._driver_event)


###############################################################################
# Read-only DOSTA command protocol
###############################################################################
class DostaProtocol(NoCommandInstrumentProtocol):
    """
    Instrument protocol class for SBE16 DOSTA driver.
    """
    def __init__(self, prompts, newline, driver_event):
        """
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The SBE16 newline.
        @param driver_event Driver process event callback.
        """
        NoCommandInstrumentProtocol.__init__(self, prompts, newline, driver_event)

    @staticmethod
    def sieve_function(raw_data):
        """ The method that splits samples
        Over-ride sieve function to handle additional particles.
        """
        matchers = []
        return_list = []

        matchers.append(DostaDataParticle.regex_compiled())
        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, timestamp):
        """
        Over-ride sieve function to handle additional particles.
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if self._extract_sample(DostaDataParticle, DostaDataParticle.regex_compiled(), chunk, timestamp):
            self._sampling = True
            return


    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_acquire_status_async(self, *args, **kwargs):
        """
        Get device status
        """
        next_state = ProtocolState.COMMAND
        result = []

        return next_state, (next_state, result)

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        Acquire sample from SBE16.
        @retval next_state, (next_state, result) tuple
        """
        next_state = None

        return next_state, (next_state, None)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        next_state = None
        result = []

        return next_state, (next_state, result)

    ########################################################################
    # response handlers.
    ########################################################################
    def _build_ctd_specific_params(self):
        self._param_dict.add(Parameter.PTYPE,
                             r"<Sensor id = 'Main Pressure'>.*?<type>(.*?)</type>.*?</Sensor>",
                             self._pressure_sensor_to_int,
                             str,
                             type=ParameterDictType.INT,
                             display_name="Pressure Sensor Type",
                             range={'Strain Gauge': 1, 'Quartz with Temp Comp': 3},
                             startup_param=True,
                             direct_access=True,
                             default_value=3,
                             description="Sensor type: (1:strain gauge | 3:quartz with temp comp)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             regex_flags=re.DOTALL)


def create_playback_protocol(callback):
    return DostaProtocol(None, None, callback)
