"""
@package mi.instrument.nortek.vector.ooicore.driver
@file mi/instrument/nortek/vector/ooicore/driver.py
@author Rachel Manoni, Ronald Ronquillo
@brief Driver for the ooicore
Release notes:

Driver for vector
"""
from datetime import datetime

import os
import re

from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.instrument_driver import DriverAsyncEvent, SingleConnectionInstrumentDriver
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.log import get_logger
from mi.instrument.nortek import common
from mi.instrument.nortek.driver import InstrumentPrompts, Parameter
from mi.instrument.nortek.driver import NortekInstrumentProtocol
from mi.instrument.nortek.driver import validate_checksum
from mi.instrument.nortek.particles import (VectorVelocityDataParticle, VectorSystemDataParticle,
                                            VectorVelocityHeaderDataParticle, VectorHardwareConfigDataParticle,
                                            VectorEngIdDataParticle, VectorEngBatteryDataParticle,
                                            VectorEngClockDataParticle, VectorUserConfigDataParticle,
                                            VectorHeadConfigDataParticle, VectorDataParticleType)

log = get_logger()

__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

VELOCITY_DATA_LEN = 24
VELOCITY_DATA_SYNC_BYTES = '\xa5\x10'
SYSTEM_DATA_LEN = 28
SYSTEM_DATA_SYNC_BYTES = '\xa5\x11\x0e\x00'
VELOCITY_HEADER_DATA_LEN = 42
VELOCITY_HEADER_DATA_SYNC_BYTES = '\xa5\x12\x15\x00'

VELOCITY_DATA_PATTERN = r'%s.{22}' % VELOCITY_DATA_SYNC_BYTES
VELOCITY_DATA_REGEX = re.compile(VELOCITY_DATA_PATTERN, re.DOTALL)
SYSTEM_DATA_PATTERN = r'%s.{24}' % SYSTEM_DATA_SYNC_BYTES
SYSTEM_DATA_REGEX = re.compile(SYSTEM_DATA_PATTERN, re.DOTALL)
VELOCITY_HEADER_DATA_PATTERN = r'%s.{38}' % VELOCITY_HEADER_DATA_SYNC_BYTES
VELOCITY_HEADER_DATA_REGEX = re.compile(VELOCITY_HEADER_DATA_PATTERN, re.DOTALL)

VECTOR_SAMPLE_REGEX = [VELOCITY_DATA_REGEX, SYSTEM_DATA_REGEX, VELOCITY_HEADER_DATA_REGEX]


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

    ########################################################################
    # overridden superclass methods
    ########################################################################
    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        NortekInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        # create chunker for processing instrument samples.
        self._chunker = StringChunker(self.sieve_function)
        self.velocity_sync_bytes = VELOCITY_DATA_SYNC_BYTES
        self.status_particles = [VectorDataParticleType.CLOCK, VectorDataParticleType.HARDWARE_CONFIG,
                                 VectorDataParticleType.HEAD_CONFIG, VectorDataParticleType.USER_CONFIG]
        self.clock_particle = VectorDataParticleType.CLOCK

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that detects data sample structures from instrument
        Should be in the format [[structure_sync_bytes, structure_len]*]
        """
        return_list = []
        sieve_matchers = common.NORTEK_COMMON_REGEXES + VECTOR_SAMPLE_REGEX

        for matcher in sieve_matchers:
            for match in matcher.finditer(raw_data):
                if matcher == VELOCITY_DATA_REGEX:
                    # two bytes is not enough for an accurate match
                    # check for a valid checksum
                    data = raw_data[match.start():match.end()]
                    if validate_checksum('<11H', data):
                        return_list.append((match.start(), match.end()))
                else:
                    return_list.append((match.start(), match.end()))
                    log.debug("sieve_function: regex found %r", raw_data[match.start():match.end()])

        return return_list

    def _got_chunk(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if any((
                self._extract_sample(VectorHardwareConfigDataParticle, common.HARDWARE_CONFIG_DATA_REGEX, structure,
                                     timestamp),
                self._extract_sample(VectorHeadConfigDataParticle, common.HEAD_CONFIG_DATA_REGEX, structure, timestamp),
                self._extract_sample(VectorUserConfigDataParticle, common.USER_CONFIG_DATA_REGEX, structure, timestamp),
                self._extract_sample(VectorEngClockDataParticle, common.CLOCK_DATA_REGEX, structure, timestamp),
                self._extract_sample(VectorEngBatteryDataParticle, common.ID_BATTERY_DATA_REGEX, structure, timestamp),
                self._extract_sample(VectorEngIdDataParticle, common.ID_DATA_REGEX, structure, timestamp),
                self._extract_sample(VectorVelocityDataParticle, VELOCITY_DATA_REGEX, structure, timestamp),
                self._extract_sample(VectorSystemDataParticle, SYSTEM_DATA_REGEX, structure, timestamp),
                self._extract_sample(VectorVelocityHeaderDataParticle, VELOCITY_HEADER_DATA_REGEX, structure,
                                     timestamp),
        )):
            return

    def _build_param_dict(self):
        super(Protocol, self)._build_param_dict()

        self._param_dict.add_basic(Parameter.SAMPLE_RATE,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Sample Rate",
                                   description="Number of samples per burst.",
                                   range=(0, 65535),
                                   startup_param=False,
                                   direct_access=False)
        self._param_dict.set_default_value(Parameter.TRANSMIT_PULSE_LENGTH, 2)
        self._param_dict.set_default_value(Parameter.BLANKING_DISTANCE, 16)
        self._param_dict.set_default_value(Parameter.RECEIVE_LENGTH, 7)
        self._param_dict.set_default_value(Parameter.TIME_BETWEEN_PINGS, 44)
        self._param_dict.set_default_value(Parameter.TIME_BETWEEN_BURST_SEQUENCES, 0)
        self._param_dict.set_default_value(Parameter.NUMBER_PINGS, 0)
        self._param_dict.set_default_value(Parameter.AVG_INTERVAL, 64)
        self._param_dict.set_default_value(Parameter.MEASUREMENT_INTERVAL, 600)
        self._param_dict.set_default_value(Parameter.DIAGNOSTIC_INTERVAL, 10800)
        self._param_dict.set_default_value(Parameter.ADJUSTMENT_SOUND_SPEED, 16657)
        self._param_dict.set_default_value(Parameter.NUMBER_SAMPLES_DIAGNOSTIC, 1)
        self._param_dict.set_default_value(Parameter.ANALOG_OUTPUT_SCALE, 6711)

        self._param_dict.set_visibility(Parameter.TRANSMIT_PULSE_LENGTH, ParameterDictVisibility.READ_WRITE)
        self._param_dict.set_visibility(Parameter.BLANKING_DISTANCE, ParameterDictVisibility.IMMUTABLE)
        self._param_dict.set_visibility(Parameter.RECEIVE_LENGTH, ParameterDictVisibility.READ_WRITE)
        self._param_dict.set_visibility(Parameter.TIME_BETWEEN_BURST_SEQUENCES, ParameterDictVisibility.READ_WRITE)
        self._param_dict.set_visibility(Parameter.AVG_INTERVAL, ParameterDictVisibility.IMMUTABLE)
        self._param_dict.set_visibility(Parameter.MEASUREMENT_INTERVAL, ParameterDictVisibility.IMMUTABLE)


###############################################################################
# PlaybackProtocol
################################################################################
class PlaybackProtocol(Protocol):
    def __init__(self, driver_event):
        """
        Protocol constructor.
        @param driver_event Driver process event callback.
        """
        super(PlaybackProtocol, self).__init__(None, None, driver_event)
        self.last_header_timestamp = None
        self.offset = 0
        self.offset_timestamp = None

    # Playback specific method due to incorrect time on deployed instrument
    def got_filename(self, filename):
        filename = os.path.basename(filename)
        date_time_regex = re.compile(r'(\d{8}T\d{4}_UTC)')
        date_format = '%Y%m%dT%H%M_%Z'
        dt = datetime.strptime(date_time_regex.search(filename).group(1), date_format)
        self.offset_timestamp = (dt - datetime(1900, 1, 1)).total_seconds()
        # if we have an RSN archive style filename we will store the time
        # and generate an offset on the next received system data particle

    ########################################################################
    # overridden superclass methods
    ########################################################################
    def _got_chunk(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if self._extract_sample(VectorVelocityDataParticle, VELOCITY_DATA_REGEX, structure,
                                timestamp, internal_timestamp=self.last_header_timestamp):
            return
        super(PlaybackProtocol, self)._got_chunk(structure, timestamp)

    def _extract_sample(self, particle_class, regex, line, timestamp, publish=True, internal_timestamp=None):
        """
        Overridden to allow us to supply an internal timestamp and to generate
        a timestamp offset during playback
        """
        if regex.match(line):

            particle = particle_class(line, port_timestamp=timestamp, internal_timestamp=internal_timestamp)
            parsed_sample = particle.generate()

            # grab the internal timestamp from the particle
            new_internal_timestamp = parsed_sample.get(DataParticleKey.INTERNAL_TIMESTAMP)

            if new_internal_timestamp is not None:
                if internal_timestamp is None:
                    self.last_header_timestamp = new_internal_timestamp
                    # this timestamp came from the instrument, check if we need to update our offset
                    if self.offset_timestamp is not None:
                        self.offset = self.offset_timestamp - new_internal_timestamp
                        log.info('Setting new offset: %r', self.offset)
                        self.offset_timestamp = None
                else:
                    # bump the last_header_timestamp value by 1/8th of a second (sample rate)
                    self.last_header_timestamp += 1.0 / 8

                parsed_sample[DataParticleKey.INTERNAL_TIMESTAMP] = new_internal_timestamp + self.offset

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample


def create_playback_protocol(callback):
    return PlaybackProtocol(callback)
