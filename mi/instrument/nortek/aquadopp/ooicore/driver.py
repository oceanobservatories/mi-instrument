"""
@package mi.instrument.nortek.aquadopp.ooicore.driver
@author Rachel Manoni, Ronald Ronquillo
@brief Driver for the ooicore
Release notes:

Driver for Aquadopp DW
"""
import struct

__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

import re

from mi.core.common import BaseEnum, Units

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import SampleException

from mi.core.instrument.data_particle import DataParticle, DataParticleKey, DataParticleValue

from mi.instrument.nortek.driver import NortekInstrumentProtocol, InstrumentPrompts, NortekProtocolParameterDict, \
    validate_checksum
from mi.instrument.nortek.driver import Parameter, NortekInstrumentDriver, NEWLINE
from mi.instrument.nortek.driver import NortekHardwareConfigDataParticle, NortekHeadConfigDataParticle, NortekUserConfigDataParticle\
    , NortekEngBatteryDataParticle, NortekEngIdDataParticle, NortekEngClockDataParticle

from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType

VELOCITY_DATA_LEN = 42
VELOCITY_DATA_SYNC_BYTES = '\xa5\x01\x15\x00'

VELOCITY_DATA_PATTERN = r'%s.{38}' % VELOCITY_DATA_SYNC_BYTES
VELOCITY_DATA_REGEX = re.compile(VELOCITY_DATA_PATTERN, re.DOTALL)


class NortekDataParticleType(BaseEnum):
    """
    List of data particles.  Names match those in the IOS, need to overwrite enum defined in base class
    """
    VELOCITY = 'velpt_velocity_data'
    HARDWARE_CONFIG = 'velpt_hardware_configuration'
    HEAD_CONFIG = 'velpt_head_configuration'
    USER_CONFIG = 'velpt_user_configuration'
    CLOCK = 'velpt_clock_data'
    BATTERY = 'velpt_battery_voltage'
    ID_STRING = 'velpt_identification_string'


NortekHardwareConfigDataParticle._data_particle_type = NortekDataParticleType.HARDWARE_CONFIG
NortekHeadConfigDataParticle._data_particle_type = NortekDataParticleType.HEAD_CONFIG
NortekUserConfigDataParticle._data_particle_type = NortekDataParticleType.USER_CONFIG
NortekEngBatteryDataParticle._data_particle_type = NortekDataParticleType.BATTERY
NortekEngIdDataParticle._data_particle_type = NortekDataParticleType.ID_STRING
NortekEngClockDataParticle._data_particle_type = NortekDataParticleType.CLOCK


class AquadoppDwVelocityDataParticleKey(BaseEnum):
    """
    Velocity Data particle
    """
    TIMESTAMP = "date_time_string"
    ERROR = "error_code"
    ANALOG1 = "analog1"
    BATTERY_VOLTAGE = "battery_voltage_dv"
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


class AquadoppDwVelocityDataParticle(DataParticle):
    """
    Routine for parsing velocity data into a data particle structure for the Aquadopp DW sensor. 
    """
    _data_particle_type = NortekDataParticleType.VELOCITY

    def _build_parsed_values(self):
        """
        Take the velocity data sample and parse it into values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        log.debug('AquadoppDwVelocityDataParticle: raw data =%r', self.raw_data)

        try:
            unpack_string = '<4s6s2h2H3hBbH4h3B1sH'

            sync, timestamp, error, analog1, battery_voltage, sound_speed, heading, pitch, roll, pressure_msb, status, \
               pressure_lsw, temperature, velocity_beam1, velocity_beam2, velocity_beam3, amplitude_beam1, \
               amplitude_beam2, amplitude_beam3, _, cksum = struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<20H', self.raw_data, cksum):
                log.warn("Bad velpt_velocity_data instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            timestamp = NortekProtocolParameterDict.convert_time(timestamp)
            pressure = pressure_msb * 0x10000 + pressure_lsw

        except Exception:
            log.error('Error creating particle velpt_velocity_data, raw data: %r', self.raw_data)
            raise SampleException

        result = [{DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.TIMESTAMP, DataParticleKey.VALUE: timestamp},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.ERROR, DataParticleKey.VALUE: error},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.ANALOG1, DataParticleKey.VALUE: analog1},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.BATTERY_VOLTAGE, DataParticleKey.VALUE: battery_voltage},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.SOUND_SPEED_ANALOG2, DataParticleKey.VALUE: sound_speed},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.HEADING, DataParticleKey.VALUE: heading},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.PITCH, DataParticleKey.VALUE: pitch},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.ROLL, DataParticleKey.VALUE: roll},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.STATUS, DataParticleKey.VALUE: status},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.PRESSURE, DataParticleKey.VALUE: pressure},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.TEMPERATURE, DataParticleKey.VALUE: temperature},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.VELOCITY_BEAM1, DataParticleKey.VALUE: velocity_beam1},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.VELOCITY_BEAM2, DataParticleKey.VALUE: velocity_beam2},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.VELOCITY_BEAM3, DataParticleKey.VALUE: velocity_beam3},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.AMPLITUDE_BEAM1, DataParticleKey.VALUE: amplitude_beam1},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.AMPLITUDE_BEAM2, DataParticleKey.VALUE: amplitude_beam2},
                  {DataParticleKey.VALUE_ID: AquadoppDwVelocityDataParticleKey.AMPLITUDE_BEAM3, DataParticleKey.VALUE: amplitude_beam3}]

        log.debug('AquadoppDwVelocityDataParticle: particle=%s', result)
        return result


###############################################################################
# Driver
###############################################################################
class InstrumentDriver(NortekInstrumentDriver):
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
class Protocol(NortekInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses NortekInstrumentProtocol
    """
    NortekInstrumentProtocol.velocity_data_regex.append(VELOCITY_DATA_REGEX)
    NortekInstrumentProtocol.velocity_sync_bytes = VELOCITY_DATA_SYNC_BYTES

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        super(Protocol, self).__init__(prompts, newline, driver_event)

    ########################################################################
    # overridden superclass methods
    ########################################################################
    def _got_chunk(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        self._extract_sample(AquadoppDwVelocityDataParticle, VELOCITY_DATA_REGEX, structure, timestamp)
        self._got_chunk_base(structure, timestamp)

    def _build_param_dict(self):
        """
        Overwrite base classes method.
        Creates base class's param dictionary, then sets parameter values for those specific to this instrument.
        """
        NortekInstrumentProtocol._build_param_dict(self)

        self._param_dict.add(Parameter.TRANSMIT_PULSE_LENGTH,
                             r'^.{%s}(.{2}).*' % str(4),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Transmit Pulse Length",
                             description="Pulse duration of the transmitted signal.",
                             default_value=125,
                             units=Units.COUNTS,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.BLANKING_DISTANCE,
                             r'^.{%s}(.{2}).*' % str(6),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Blanking Distance",
                             description="Minimum sensing range of the sensor.",
                             default_value=49,
                             units=Units.COUNTS,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.RECEIVE_LENGTH,
                             r'^.{%s}(.{2}).*' % str(8),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Receive Length",
                             description="Length of the received pulse.",
                             default_value=32,
                             units=Units.COUNTS,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.TIME_BETWEEN_PINGS,
                             r'^.{%s}(.{2}).*' % str(10),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Time Between Pings",
                             description="Length of time between each ping.",
                             units=Units.COUNTS,
                             default_value=437,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.TIME_BETWEEN_BURST_SEQUENCES,
                             r'^.{%s}(.{2}).*' % str(12),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Time Between Burst Sequences",
                             description="Length of time between each burst.",
                             default_value=512,
                             units=Units.COUNTS,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.NUMBER_PINGS,
                             r'^.{%s}(.{2}).*' % str(14),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Number Pings",
                             description="Number of pings in each burst sequence.",
                             default_value=1,
                             units=Units.HERTZ,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.AVG_INTERVAL,
                             r'^.{%s}(.{2}).*' % str(16),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Average Interval",
                             description="Interval for continuous sampling.",
                             default_value=60,
                             units=Units.SECOND,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.MEASUREMENT_INTERVAL,
                             r'^.{%s}(.{2}).*' % str(38),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Measurement Interval",
                             description="Interval for single measurements.",
                             default_value=60,
                             units=Units.SECOND,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.DIAGNOSTIC_INTERVAL,
                             r'^.{%s}(.{4}).*' % str(54),
                             lambda match: NortekProtocolParameterDict.convert_double_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.double_word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Diagnostic Interval",
                             description='Number of seconds between diagnostics measurements.',
                             default_value=11250,
                             startup_param=True,
                             units=Units.SECOND,
                             direct_access=True)
        self._param_dict.add(Parameter.ADJUSTMENT_SOUND_SPEED,
                             r'^.{%s}(.{2}).*' % str(60),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Adjustment Sound Speed",
                             description='User input sound speed adjustment factor.',
                             units=Units.METER + '/' + Units.SECOND,
                             default_value=1525,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.NUMBER_SAMPLES_DIAGNOSTIC,
                             r'^.{%s}(.{2}).*' % str(62),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Diagnostic Samples",
                             description='Number of samples in diagnostics mode.',
                             default_value=20,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.SW_VERSION,
                             r'^.{%s}(.{2}).*' % str(72),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Software Version",
                             description="Current software version installed on instrument.",
                             default_value=13902,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.ANALOG_OUTPUT_SCALE,
                             r'^.{%s}(.{2}).*' % str(456),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Analog Output Scale Factor",
                             description="Scale factor used in calculating analog output.",
                             default_value=0,
                             startup_param=True,
                             direct_access=True)