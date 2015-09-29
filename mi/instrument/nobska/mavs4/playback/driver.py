#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@package mi.instrument.nobska.mavs4.playback.driver
@file /marine-integrations/mi/instrument/nobska/mavs4/playback/driver.py
@author Pete Cable
@brief Driver for the mavs4
Release notes:

initial release
"""

__author__ = 'Pete Cable'
__license__ = 'Apache 2.0'

import time
import re

from mi.core.time_tools import timegm_to_float
from mi.core.common import BaseEnum
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.exceptions import SampleException
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logger
from mi.instrument.nobska.mavs4.ooicore.driver import DataParticleType, Mavs4SampleDataParticleKey

log = get_logger()

SAMPLE_DATA_PATTERN = (r'(\d+\s+\d+\s+\d+)' +  # date
                       '\s+(\d+\s+\d+\s+\d+)' +  # time
                       '\.(\d+)' +  # fractional second
                       '\s+(-*\d+\.\d+)' +  # vector A
                       '\s+(-*\d+\.\d+)' +  # vector B
                       '\s+(-*\d+\.\d+)' +  # vector C
                       '\s+(-*\d+\.\d+)' +  # vector D
                       '\s+(-*\d+\.\d+)' +  # east
                       '\s+(-*\d+\.\d+)' +  # north
                       '\s+(-*\d+\.\d+)' +  # west
                       '\s+(-*\d+\.\d+)' +  # temperature
                       '\s+(-*\d+\.\d+)' +  # MX
                       '\s+(-*\d+\.\d+)' +  # MY
                       '\s+(-*\d+\.\d+)' +  # pitch
                       '\s+(-*\d+\.\d+)\s+')  # roll

SAMPLE_DATA_REGEX = re.compile(SAMPLE_DATA_PATTERN)


class ProtocolStates(BaseEnum):
    """
    Protocol states for MAVS-4. Cherry picked from DriverProtocolState enum.
    """
    UNKNOWN = DriverProtocolState.UNKNOWN


class ProtocolEvent(BaseEnum):
    """
    Protocol events for MAVS-4. Cherry picked from DriverEvent enum.
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT


###############################################################################
# Data particles
###############################################################################
class Mavs4PlaybackParticle(DataParticle):
    """
    Class for parsing sample data into a data particle structure for the MAVS-4 sensor.
    """
    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take something in the data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        match = SAMPLE_DATA_REGEX.match(self.raw_data)

        if not match:
            raise SampleException("Mavs4SampleDataParticle: No regex match of \
                                  parsed sample data: [%s]", self.raw_data)

        try:
            fractional_second = int(match.group(3))
            datetime = "%s %s.%s" % (match.group(1), match.group(2), fractional_second)
            datetime_nofrac = "%s %s" % (match.group(1), match.group(2))
            timestamp = time.strptime(datetime_nofrac, "%m %d %Y %H %M %S")
            self.set_internal_timestamp(unix_time=(timegm_to_float(timestamp) + fractional_second))
            velocity_frame_east = float(match.group(8))
            velocity_frame_north = float(match.group(9))
            velocity_frame_up = float(match.group(10))
            temperature = float(match.group(11))
            compass_mx = float(match.group(12))
            compass_my = float(match.group(13))
            pitch = float(match.group(14))
            roll = float(match.group(15))
        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, self.raw_data))

        result = [{DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.DATE_TIME_STRING,
                   DataParticleKey.VALUE: datetime},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.VELOCITY_FRAME_EAST,
                   DataParticleKey.VALUE: velocity_frame_east},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.VELOCITY_FRAME_NORTH,
                   DataParticleKey.VALUE: velocity_frame_north},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.VELOCITY_FRAME_UP,
                   DataParticleKey.VALUE: velocity_frame_up},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.TEMPERATURE,
                   DataParticleKey.VALUE: temperature},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.COMPASS_MX,
                   DataParticleKey.VALUE: compass_mx},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.COMPASS_MY,
                   DataParticleKey.VALUE: compass_my},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.PITCH,
                   DataParticleKey.VALUE: pitch},
                  {DataParticleKey.VALUE_ID: Mavs4SampleDataParticleKey.ROLL,
                   DataParticleKey.VALUE: roll}]

        log.debug('Mavs4SampleDataParticle: particle=%s', result)
        return result


class Protocol(CommandResponseInstrumentProtocol):
    """
    This protocol implements a simple parser for archived RSN data
    """
    def __init__(self, driver_event):
        CommandResponseInstrumentProtocol.__init__(self, None, None, driver_event)
        self._chunker = StringChunker(self.sieve_function)

    @staticmethod
    def sieve_function(raw_data):
        # The method that detects data sample structures from instrument
        return [(match.start(), match.end()) for match in SAMPLE_DATA_REGEX.finditer(raw_data)]

    def _got_chunk(self, structure, timestamp):
        self._extract_sample(Mavs4PlaybackParticle, SAMPLE_DATA_REGEX,
                             structure, timestamp)

    def get_current_state(self):
        return ProtocolStates.UNKNOWN


def create_playback_protocol(callback):
    return Protocol(callback)
