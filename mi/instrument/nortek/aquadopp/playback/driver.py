"""
@package mi.instrument.nortek.aquadopp.playback.driver
@author Pete Cable
@brief Driver for the aquadopp ascii mode playback
Release notes:

Driver for Aquadopp DW
"""
import datetime
from mi.core.exceptions import SampleException
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import DriverAsyncEvent, SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument_fsm import InstrumentFSM
from mi.instrument.nortek.aquadopp.ooicore.driver import NortekDataParticleType

__author__ = 'Pete Cable'
__license__ = 'Apache 2.0'

import re

from mi.core.log import get_logger
log = get_logger()

from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.common import BaseEnum
from mi.instrument.nortek.driver import InstrumentPrompts, ProtocolState, ProtocolEvent
from mi.instrument.nortek.driver import NEWLINE

integer_pattern = r'\d+'
float_pattern = r'[+\-\d.]+'
VELOCITY_DATA_PATTERN = '\s+'.join([integer_pattern] * 8 +
                                   [float_pattern] * 3 +
                                   [integer_pattern] * 3 +
                                   [float_pattern] * 7 +
                                   [integer_pattern] * 2 +
                                   [float_pattern] * 2) + r'\r\n'
VELOCITY_DATA_REGEX = re.compile(VELOCITY_DATA_PATTERN)


class AquadoppDwVelocityDataParticleKey(BaseEnum):
    """
    Velocity Data particle
    """
    TIMESTAMP = "date_time_string"
    ERROR = "error_code"
    ANALOG1 = "analog1"
    BATTERY_VOLTAGE = "battery_voltage_dV"
    SOUND_SPEED_ANALOG2 = "sound_speed_dms"
    HEADING = "heading_decidegree"
    PITCH = "pitch_decidegree"
    ROLL = "roll_decidegree"
    PRESSURE = "pressure_mbar"
    STATUS = "status"
    TEMPERATURE = "temperature_centidegree"
    VELOCITY_BEAM1 = "velocity_beam1"
    VELOCITY_BEAM2 = "velocity_beam2"
    VELOCITY_BEAM3 = "velocity_beam3"
    AMPLITUDE_BEAM1 = "amplitude_beam1"
    AMPLITUDE_BEAM2 = "amplitude_beam2"
    AMPLITUDE_BEAM3 = "amplitude_beam3"


class AquadoppDwVelocityAsciiDataParticle(DataParticle):
    """
    Routine for parsing velocity data into a data particle structure for the Aquadopp DW sensor. 
    """
    _data_particle_type = NortekDataParticleType.VELOCITY
    ntp_epoch = datetime.datetime(1900, 1, 1)

    def _build_parsed_values(self):
        """
        Take the velocity data sample and parse it into values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        try:
            (month, day, year, hour, minute, second, error_code, status_code, velocity_beam1,
             velocity_beam2, velocity_beam3, amplitude_beam1, amplitude_beam2, amplitude_beam3,
             battery_voltage, sound_speed, heading, pitch, roll, pressure, temperature,
             analog1, analog2, speed, direction) = self.raw_data.split()

            day, month, year, hour, minute, second = int(day), int(month), int(year), int(hour), int(minute), int(second)

            ntp_timestamp = (datetime.datetime(year, month, day, hour, minute, second) - self.ntp_epoch).total_seconds()
            self.set_internal_timestamp(ntp_timestamp)

            # normally we don't adjust any data in a parser
            # this is a special case so that we can keep producing the same
            # stream from this instrument between the playback and live data

            timestamp = '%02d/%02d/%02d %02d:%02d:%02d' % (day, month, year, hour, minute, second)
            error_code = int(error_code)
            status_code = int(status_code)
            velocity_beam1 = int(float(velocity_beam1) * 1000)  # m/s to mm/s
            velocity_beam2 = int(float(velocity_beam2) * 1000)  # m/s to mm/s
            velocity_beam3 = int(float(velocity_beam3) * 1000)  # m/s to mm/s
            amplitude_beam1 = int(amplitude_beam1)
            amplitude_beam2 = int(amplitude_beam2)
            amplitude_beam3 = int(amplitude_beam3)
            battery_voltage = int(float(battery_voltage) * 10)  # V to 0.1 V
            sound_speed = int(float(sound_speed) * 10)  # m/s to 0.1 m/s
            heading = int(float(heading) * 10)  # deg to 0.1 deg
            pitch = int(float(pitch) * 10)  # deg to 0.1 deg
            roll = int(float(roll) * 10)  # deg to 0.1 deg
            pressure = int(float(pressure) * 1000)  # dbar to 0.001 dbar
            temperature = int(float(temperature) * 100)  # deg to .01 deg
            analog1 = int(analog1)

        except ValueError:
            raise SampleException("Unable to parse fields")

        VID = DataParticleKey.VALUE_ID
        VAL = DataParticleKey.VALUE
        ADVDPK = AquadoppDwVelocityDataParticleKey

        result = [{VID: ADVDPK.TIMESTAMP, VAL: timestamp},
                  {VID: ADVDPK.ERROR, VAL: error_code},
                  {VID: ADVDPK.ANALOG1, VAL: analog1},
                  {VID: ADVDPK.BATTERY_VOLTAGE, VAL: battery_voltage},
                  {VID: ADVDPK.SOUND_SPEED_ANALOG2, VAL: sound_speed},
                  {VID: ADVDPK.HEADING, VAL: heading},
                  {VID: ADVDPK.PITCH, VAL: pitch},
                  {VID: ADVDPK.ROLL, VAL: roll},
                  {VID: ADVDPK.STATUS, VAL: status_code},
                  {VID: ADVDPK.PRESSURE, VAL: pressure},
                  {VID: ADVDPK.TEMPERATURE, VAL: temperature},
                  {VID: ADVDPK.VELOCITY_BEAM1, VAL: velocity_beam1},
                  {VID: ADVDPK.VELOCITY_BEAM2, VAL: velocity_beam2},
                  {VID: ADVDPK.VELOCITY_BEAM3, VAL: velocity_beam3},
                  {VID: ADVDPK.AMPLITUDE_BEAM1, VAL: amplitude_beam1},
                  {VID: ADVDPK.AMPLITUDE_BEAM2, VAL: amplitude_beam2},
                  {VID: ADVDPK.AMPLITUDE_BEAM3, VAL: amplitude_beam3}]

        return result


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
        self._protocol = Protocol(InstrumentPrompts, NEWLINE, self._driver_event)


###############################################################################
# Protocol
################################################################################
class Protocol(CommandResponseInstrumentProtocol):
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
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        self._protocol_fsm = InstrumentFSM(ProtocolState,
                                           ProtocolEvent,
                                           ProtocolEvent.ENTER,
                                           ProtocolEvent.EXIT)

        handlers = {
            ProtocolState.UNKNOWN: [
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.EXIT, self._handler_unknown_exit)
            ],
        }

        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # create chunker for processing instrument samples.
        self._chunker = StringChunker(self.sieve_function)

    @classmethod
    def sieve_function(cls, raw_data):
        """
        The method that detects data sample structures from instrument
        Should be in the format [[structure_sync_bytes, structure_len]*]
        """
        return_list = []
        sieve_matchers = [VELOCITY_DATA_REGEX]

        for matcher in sieve_matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))
                log.debug("sieve_function: regex found %r", raw_data[match.start():match.end()])

        return return_list

    ########################################################################
    # overridden superclass methods
    ########################################################################
    def _got_chunk(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        self._extract_sample(AquadoppDwVelocityAsciiDataParticle, VELOCITY_DATA_REGEX, structure, timestamp)

    ########################################################################
    # Unknown handlers.
    ########################################################################

    def _handler_unknown_enter(self, *args, **kwargs):
        """
        Enter unknown state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_unknown_exit(self, *args, **kwargs):
        """
        Exiting Unknown state
        """
        pass