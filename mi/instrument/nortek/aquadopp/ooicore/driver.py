"""
@package mi.instrument.nortek.aquadopp.ooicore.driver
@author Rachel Manoni, Ronald Ronquillo
@brief Driver for the ooicore
Release notes:

Driver for Aquadopp DW
"""
import functools

__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

import re
from collections import namedtuple
import struct
from datetime import datetime

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import SampleException
from mi.core.common import BaseEnum, Units
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


namedtuple_store = {}
def unpack_from_format(name, unpack_format, data):
    format_string = ''.join([item[1] for item in unpack_format])
    fields = [item[0] for item in unpack_format]
    data = struct.unpack_from(format_string, data)
    if name not in namedtuple_store:
        namedtuple_store[name] = namedtuple(name, fields)
    _class = namedtuple_store[name]
    return _class(*data)


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

        typedef struct {
            unsigned char cSync; // sync = 0xa5
            unsigned char cId; // identification (0x01=normal, 0x80=diag)
            unsigned short hSize; // size of structure (words)
            PdClock clock; // date and time
            short hError; // error code:
            unsigned short hAnaIn1; // analog input 1
            unsigned short hBattery; // battery voltage (0.1 V)
            union {
                unsigned short hSoundSpeed; // speed of sound (0.1 m/s)
                unsigned short hAnaIn2; // analog input 2
            } u;
            short hHeading; // compass heading (0.1 deg)
            short hPitch; // compass pitch (0.1 deg)
            short hRoll; // compass roll (0.1 deg)
            unsigned char cPressureMSB; // pressure MSB
            char cStatus; // status:
            unsigned short hPressureLSW; // pressure LSW
            short hTemperature; // temperature (0.01 deg C)
            short hVel[3]; // velocity
            unsigned char cAmp[3]; // amplitude
            char cFill;
            short hChecksum; // checksum
        } PdMeas;
        """
        try:
            unpack_format = (
                ('sync',            '<4s'),  # cSync, cId, hSize
                ('timestamp',       '6s'),   # PdClock
                ('error',           'H'),    # defined as signed short, but represents bitmap, using unsigned
                ('analog1',         'H'),
                ('battery_voltage', 'H'),
                ('sound_speed',     'H'),
                ('heading',         'h'),
                ('pitch',           'h'),
                ('roll',            'h'),
                ('pressure_msb',    'B'),
                ('status',          'B'),    # defined as char, but represents bitmap, using unsigned
                ('pressure_lsw',    'H'),
                ('temperature',     'h'),
                ('velocity_beam1',  'h'),
                ('velocity_beam2',  'h'),
                ('velocity_beam3',  'h'),
                ('amplitude_beam1', 'B'),
                ('amplitude_beam2', 'B'),
                ('amplitude_beam3', 'B'),
            )

            data = unpack_from_format(self._data_particle_type, unpack_format, self.raw_data)

            if not validate_checksum('<20H', self.raw_data):
                log.warn("Failed checksum in %s from instrument (%r)", self._data_particle_type, self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            timestamp = NortekProtocolParameterDict.convert_time(data.timestamp)
            self.set_internal_timestamp((timestamp-datetime(1900, 1, 1)).total_seconds())

            pressure = data.pressure_msb * 0x10000 + data.pressure_lsw

        except Exception as e:
            log.error('Error creating particle velpt_velocity_data, raw data: %r', self.raw_data)
            raise SampleException(e)

        key = AquadoppDwVelocityDataParticleKey

        result = [{DataParticleKey.VALUE_ID: key.TIMESTAMP, DataParticleKey.VALUE: str(timestamp)},
                  {DataParticleKey.VALUE_ID: key.ERROR, DataParticleKey.VALUE: data.error},
                  {DataParticleKey.VALUE_ID: key.ANALOG1, DataParticleKey.VALUE: data.analog1},
                  {DataParticleKey.VALUE_ID: key.BATTERY_VOLTAGE, DataParticleKey.VALUE: data.battery_voltage},
                  {DataParticleKey.VALUE_ID: key.SOUND_SPEED_ANALOG2, DataParticleKey.VALUE: data.sound_speed},
                  {DataParticleKey.VALUE_ID: key.HEADING, DataParticleKey.VALUE: data.heading},
                  {DataParticleKey.VALUE_ID: key.PITCH, DataParticleKey.VALUE: data.pitch},
                  {DataParticleKey.VALUE_ID: key.ROLL, DataParticleKey.VALUE: data.roll},
                  {DataParticleKey.VALUE_ID: key.STATUS, DataParticleKey.VALUE: data.status},
                  {DataParticleKey.VALUE_ID: key.PRESSURE, DataParticleKey.VALUE: pressure},
                  {DataParticleKey.VALUE_ID: key.TEMPERATURE, DataParticleKey.VALUE: data.temperature},
                  {DataParticleKey.VALUE_ID: key.VELOCITY_BEAM1, DataParticleKey.VALUE: data.velocity_beam1},
                  {DataParticleKey.VALUE_ID: key.VELOCITY_BEAM2, DataParticleKey.VALUE: data.velocity_beam2},
                  {DataParticleKey.VALUE_ID: key.VELOCITY_BEAM3, DataParticleKey.VALUE: data.velocity_beam3},
                  {DataParticleKey.VALUE_ID: key.AMPLITUDE_BEAM1, DataParticleKey.VALUE: data.amplitude_beam1},
                  {DataParticleKey.VALUE_ID: key.AMPLITUDE_BEAM2, DataParticleKey.VALUE: data.amplitude_beam2},
                  {DataParticleKey.VALUE_ID: key.AMPLITUDE_BEAM3, DataParticleKey.VALUE: data.amplitude_beam3}]

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