"""
@package mi.instrument.nortek.vector.ooicore.driver
@file mi/instrument/nortek/vector/ooicore/driver.py
@author Rachel Manoni, Ronald Ronquillo
@brief Driver for the ooicore
Release notes:

Driver for vector
"""
import struct
from mi.core.exceptions import SampleException

__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

import re
import base64

from mi.core.common import BaseEnum
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.data_particle import DataParticle, DataParticleKey

from mi.instrument.nortek.driver import NortekDataParticleType, Parameter, ParameterUnits
from mi.instrument.nortek.driver import NortekInstrumentDriver
from mi.instrument.nortek.driver import NortekInstrumentProtocol
from mi.instrument.nortek.driver import NortekProtocolParameterDict
from mi.instrument.nortek.driver import InstrumentPrompts
from mi.instrument.nortek.driver import NEWLINE

from mi.core.log import get_logger
log = get_logger()

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


class DataParticleType(NortekDataParticleType):
    """
    List of data particles to collect
    """
    VELOCITY = 'vel3d_cd_velocity_data'
    VELOCITY_HEADER = 'vel3d_cd_data_header'
    SYSTEM = 'vel3d_cd_system_data'


class VectorVelocityDataParticleKey(BaseEnum):
    """
    Velocity Data Particles
    """
    ANALOG_INPUT2 = "analog_input_2"
    COUNT = "ensemble_counter"
    PRESSURE = "seawater_pressure_mbar"
    ANALOG_INPUT1 = "analog_input_1"
    VELOCITY_BEAM1 = "turbulent_velocity_east"
    VELOCITY_BEAM2 = "turbulent_velocity_north"
    VELOCITY_BEAM3 = "turbulent_velocity_vertical"
    AMPLITUDE_BEAM1 = "amplitude_beam_1"
    AMPLITUDE_BEAM2 = "amplitude_beam_2"
    AMPLITUDE_BEAM3 = "amplitude_beam_3"
    CORRELATION_BEAM1 = "correlation_beam_1"
    CORRELATION_BEAM2 = "correlation_beam_2"
    CORRELATION_BEAM3 = "correlation_beam_3"
    
            
class VectorVelocityDataParticle(DataParticle):
    """
    Routine for parsing velocity data into a data particle structure for the Vector sensor. 
    """
    _data_particle_type = DataParticleType.VELOCITY

    def _build_parsed_values(self):
        """
        Take the velocity data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        log.debug('VectorVelocityDataParticle: raw data =%r', self.raw_data)

        try:
        
            unpack_string = '<4s4B2H3h6B'
        
            sync, analog_input2_lsb, count, pressure_msb, analog_input2_msb, pressure_lsw, analog_input1,\
                velocity_beam1, velocity_beam2, velocity_beam3, amplitude_beam1, amplitude_beam2, amplitude_beam3, \
                correlation_beam1, correlation_beam2, correlation_beam3 = struct.unpack(unpack_string, self.raw_data)

            analog_input2 = analog_input2_msb * 0x100 + analog_input2_lsb
            pressure = pressure_msb * 0x10000 + pressure_lsw

        except Exception:
            log.error('Error creating particle vel3d_cd_velocity_data, raw data: %r', self.raw_data)
            raise SampleException

        result = [{DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.ANALOG_INPUT2, DataParticleKey.VALUE: analog_input2},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.COUNT, DataParticleKey.VALUE: count},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.PRESSURE, DataParticleKey.VALUE: pressure},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.ANALOG_INPUT1, DataParticleKey.VALUE: analog_input1},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.VELOCITY_BEAM1, DataParticleKey.VALUE: velocity_beam1},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.VELOCITY_BEAM2, DataParticleKey.VALUE: velocity_beam2},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.VELOCITY_BEAM3, DataParticleKey.VALUE: velocity_beam3},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM1, DataParticleKey.VALUE: amplitude_beam1},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM2, DataParticleKey.VALUE: amplitude_beam2},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM3, DataParticleKey.VALUE: amplitude_beam3},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.CORRELATION_BEAM1, DataParticleKey.VALUE: correlation_beam1},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.CORRELATION_BEAM2, DataParticleKey.VALUE: correlation_beam2},
                  {DataParticleKey.VALUE_ID: VectorVelocityDataParticleKey.CORRELATION_BEAM3, DataParticleKey.VALUE: correlation_beam3}]
 
        log.debug('VectorVelocityDataParticle: particle=%s', result)
        return result


class VectorVelocityHeaderDataParticleKey(BaseEnum):
    """
    Velocity Header data particles
    """
    TIMESTAMP = "date_time_string"
    NUMBER_OF_RECORDS = "number_velocity_records"
    NOISE1 = "noise_amp_beam1"
    NOISE2 = "noise_amp_beam2"
    NOISE3 = "noise_amp_beam3"
    CORRELATION1 = "noise_correlation_beam1"
    CORRELATION2 = "noise_correlation_beam2"
    CORRELATION3 = "noise_correlation_beam3"


class VectorVelocityHeaderDataParticle(DataParticle):
    """
    Routine for parsing velocity header data into a data particle structure for the Vector sensor. 
    """
    _data_particle_type = DataParticleType.VELOCITY_HEADER

    def _build_parsed_values(self):
        """
        Take the velocity header data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        log.debug('VectorVelocityHeaderDataParticle: raw data =%r', self.raw_data)

        try:
            unpack_string = '<4s6sH8B20sh'
            sync, timestamp, number_of_records, noise1, noise2, noise3, _, correlation1, correlation2, correlation3, _,\
                _, cksum = struct.unpack(unpack_string, self.raw_data)
        
            timestamp = NortekProtocolParameterDict.convert_time(timestamp)
            
        except Exception:
            log.error('Error creating particle vel3d_cd_data_header, raw data: %r', self.raw_data)
            raise SampleException
        
        result = [{DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.TIMESTAMP, DataParticleKey.VALUE: timestamp},
                  {DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.NUMBER_OF_RECORDS, DataParticleKey.VALUE: number_of_records},
                  {DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.NOISE1, DataParticleKey.VALUE: noise1},
                  {DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.NOISE2, DataParticleKey.VALUE: noise2},
                  {DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.NOISE3, DataParticleKey.VALUE: noise3},
                  {DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.CORRELATION1, DataParticleKey.VALUE: correlation1},
                  {DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.CORRELATION2, DataParticleKey.VALUE: correlation2},
                  {DataParticleKey.VALUE_ID: VectorVelocityHeaderDataParticleKey.CORRELATION3, DataParticleKey.VALUE: correlation3}]
 
        log.debug('VectorVelocityHeaderDataParticle: particle=%s', result)
        return result


class VectorSystemDataParticleKey(BaseEnum):
    """
    System data particles
    """
    TIMESTAMP = "date_time_string"
    BATTERY = "battery_voltage_dV"
    SOUND_SPEED = "sound_speed_dms"
    HEADING = "heading_decidegree"
    PITCH = "pitch_decidegree"
    ROLL = "roll_decidegree"
    TEMPERATURE = "temperature_centidegree"
    ERROR = "error_code"
    STATUS = "status_code"
    ANALOG_INPUT = "analog_input"


class VectorSystemDataParticle(DataParticle):
    """
    Routine for parsing system data into a data particle structure for the Vector sensor. 
    """
    _data_particle_type = DataParticleType.SYSTEM

    def _build_parsed_values(self):
        """
        Take the system data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        log.debug('VectorSystemDataParticle: raw data =%r', self.raw_data)

        try:
        
            unpack_string = '<4s6s2H4h2bHh'
            
            sync, timestamp, battery, sound_speed, heading, pitch, roll, temperature, error, status, analog_input, cksum =\
                struct.unpack_from(unpack_string, self.raw_data)
        
            timestamp = NortekProtocolParameterDict.convert_time(timestamp)
            # error = NortekProtocolParameterDict.convert_bytes_to_bit_field(error)
            # status = NortekProtocolParameterDict.convert_bytes_to_bit_field(status)

        except Exception:
            log.error('Error creating particle vel3d_cd_system_data, raw data: %r', self.raw_data)
            raise SampleException

        result = [{DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.TIMESTAMP, DataParticleKey.VALUE: timestamp},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.BATTERY, DataParticleKey.VALUE: battery},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.SOUND_SPEED, DataParticleKey.VALUE: sound_speed},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.HEADING, DataParticleKey.VALUE: heading},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.PITCH, DataParticleKey.VALUE: pitch},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.ROLL, DataParticleKey.VALUE: roll},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.TEMPERATURE, DataParticleKey.VALUE: temperature},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.ERROR, DataParticleKey.VALUE: error},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.STATUS, DataParticleKey.VALUE: status},
                  {DataParticleKey.VALUE_ID: VectorSystemDataParticleKey.ANALOG_INPUT, DataParticleKey.VALUE: analog_input}]
 
        log.debug('VectorSystemDataParticle: particle=%s', result)

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
    def __init__(self, evt_callback):
        """
        Driver constructor.
        @param evt_callback Driver process event callback.
        """
        NortekInstrumentDriver.__init__(self, evt_callback)

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
    NortekInstrumentProtocol.velocity_data_regex.extend(VECTOR_SAMPLE_REGEX)
    NortekInstrumentProtocol.velocity_sync_bytes = VELOCITY_DATA_SYNC_BYTES

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        super(Protocol, self).__init__(prompts, newline, driver_event)

    ########################################################################
    # overridden superclass methods
    ########################################################################
    def _got_chunk(self, structure, timestamp):
        """
        The base class got_data has gotten a structure from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes. 
        """
        self._extract_sample(VectorVelocityDataParticle, VELOCITY_DATA_REGEX, structure, timestamp)
        self._extract_sample(VectorSystemDataParticle, SYSTEM_DATA_REGEX, structure, timestamp)
        self._extract_sample(VectorVelocityHeaderDataParticle, VELOCITY_HEADER_DATA_REGEX, structure, timestamp)

        self._got_chunk_base(structure, timestamp)

    ########################################################################
    # Private helpers.
    ########################################################################
    def _build_param_dict(self):
        NortekInstrumentProtocol._build_param_dict(self)

        self._param_dict.add(Parameter.TRANSMIT_PULSE_LENGTH,
                                   r'^.{%s}(.{2}).*' % str(4),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Transmit Pulse Length",
                                   default_value=2,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.BLANKING_DISTANCE,
                                   r'^.{%s}(.{2}).*' % str(6),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Blanking Distance",
                                   default_value=16,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.RECEIVE_LENGTH,
                                   r'^.{%s}(.{2}).*' % str(8),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Receive Length",
                                   default_value=7,
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
                                   default_value=44,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.TIME_BETWEEN_BURST_SEQUENCES,
                                   r'^.{%s}(.{2}).*' % str(12),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Time Between Burst Sequences",
                                   default_value=0,
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
                                   default_value=0,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.AVG_INTERVAL,
                                   r'^.{%s}(.{2}).*' % str(16),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Average Interval",
                                   default_value=64,
                                   units=ParameterUnits.SECONDS,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.USER_NUMBER_BEAMS,
                                   r'^.{%s}(.{2}).*' % str(18),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="User Number Beams",
                                   value=3,
                                   direct_access=True)
        self._param_dict.add(Parameter.TIMING_CONTROL_REGISTER,
                                   r'^.{%s}(.{2}).*' % str(20),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Timing Control Register",
                                   direct_access=True,
                                   value=130)
        self._param_dict.add(Parameter.POWER_CONTROL_REGISTER,
                                   r'^.{%s}(.{2}).*' % str(22),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="Power Control Register",
                                   direct_access=True,
                                   value=0)
        self._param_dict.add(Parameter.A1_1_SPARE,
                                   r'^.{%s}(.{2}).*' % str(24),
                                   lambda match: match.group(1).encode('hex'),
                                   lambda string: string.decode('hex'),
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="A1 1 Spare",
                                   description='Not used.')
        self._param_dict.add(Parameter.B0_1_SPARE,
                                   r'^.{%s}(.{2}).*' % str(26),
                                   lambda match: match.group(1).encode('hex'),
                                   lambda string: string.decode('hex'),
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="B0 1 Spare",
                                   description='Not used.')
        self._param_dict.add(Parameter.B1_1_SPARE,
                                   r'^.{%s}(.{2}).*' % str(28),
                                   lambda match: match.group(1).encode('hex'),
                                   lambda string: string.decode('hex'),
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="B1 1 Spare",
                                   description='Not used.')
        self._param_dict.add(Parameter.COMPASS_UPDATE_RATE,
                                   r'^.{%s}(.{2}).*' % str(30),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Compass Update Rate",
                                   default_value=1,
                                   units=ParameterUnits.HERTZ,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.COORDINATE_SYSTEM,
                                   r'^.{%s}(.{2}).*' % str(32),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Coordinate System",
                                   description='Coordinate System (0=ENU, 1=XYZ, 2=BEAM)',
                                   default_value=2,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.NUMBER_BINS,
                                   r'^.{%s}(.{2}).*' % str(34),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Number Bins",
                                   default_value=1,
                                   units=ParameterUnits.METERS,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.BIN_LENGTH,
                                   r'^.{%s}(.{2}).*' % str(36),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Bin Length",
                                   default_value=7,
                                   units=ParameterUnits.SECONDS,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.MEASUREMENT_INTERVAL,
                                   r'^.{%s}(.{2}).*' % str(38),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Measurement Interval",
                                   default_value=600,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.DEPLOYMENT_NAME,
                                   r'^.{%s}(.{6}).*' % str(40),
                                   lambda match: NortekProtocolParameterDict.convert_bytes_to_string(match.group(1)),
                                   lambda string: string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Deployment Name",
                                   default_value='',
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.WRAP_MODE,
                                   r'^.{%s}(.{2}).*' % str(46),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Wrap Mode",
                                   description='Recorder wrap mode (0=NO WRAP, 1=WRAP WHEN FULL)',
                                   default_value=0,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.CLOCK_DEPLOY,
                                   r'^.{%s}(.{6}).*' % str(48),
                                   lambda match: NortekProtocolParameterDict.convert_words_to_datetime(match.group(1)),
                                   NortekProtocolParameterDict.convert_datetime_to_words,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.LIST,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Clock Deploy",
                                   description='Deployment start time.',
                                   default_value=[0, 0, 0, 0, 0, 0],
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
                                   default_value=10800,
                                   units=ParameterUnits.SECONDS,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.MODE,
                                   r'^.{%s}(.{2}).*' % str(58),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Mode",
                                   default_value=48,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.ADJUSTMENT_SOUND_SPEED,
                                   r'^.{%s}(.{2}).*' % str(60),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Adjustment Sound Speed",
                                   description='User input sound speed adjustment factor.',
                                   default_value=16657,
                                   units=ParameterUnits.METERS_PER_SECOND,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.NUMBER_SAMPLES_DIAGNOSTIC,
                                   r'^.{%s}(.{2}).*' % str(62),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Number Samples Diagnostic",
                                   description='Samples in diagnostics mode.',
                                   default_value=1,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC,
                                   r'^.{%s}(.{2}).*' % str(64),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Number Beams Cell Diagnostic",
                                   description='Beams/cell number to measure in diagnostics mode',
                                   default_value=1,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.NUMBER_PINGS_DIAGNOSTIC,
                                   r'^.{%s}(.{2}).*' % str(66),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Number Pings Diagnostic",
                                   description='Pings in diagnostics/wave mode.',
                                   default_value=1,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.MODE_TEST,
                                   r'^.{%s}(.{2}).*' % str(68),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Mode Test",
                                   default_value=4,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.ANALOG_INPUT_ADDR,
                                   r'^.{%s}(.{2}).*' % str(70),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Analog Input Address",
                                   startup_param=True,
                                   default_value=0,
                                   direct_access=True)
        self._param_dict.add(Parameter.SW_VERSION,
                                   r'^.{%s}(.{2}).*' % str(72),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   direct_access=True,
                                   display_name="Software Version")
        self._param_dict.add(Parameter.USER_1_SPARE,
                                   r'^.{%s}(.{2}).*' % str(74),
                                   lambda match: match.group(1).encode('hex'),
                                   lambda string: string.decode('hex'),
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="User 1 Spare",
                                   description='Not used.')
        self._param_dict.add(Parameter.VELOCITY_ADJ_TABLE,
                                   r'^.{%s}(.{180}).*' % str(76),
                                   lambda match: base64.b64encode(match.group(1)),
                                   lambda string: string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="Velocity Adj Table",
                                   units=ParameterUnits.PARTS_PER_TRILLION,
                                   direct_access=True)
        self._param_dict.add(Parameter.COMMENTS,
                                   r'^.{%s}(.{180}).*' % str(256),
                                   lambda match: NortekProtocolParameterDict.convert_bytes_to_string(match.group(1)),
                                   lambda string: string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Comments",
                                   default_value='',
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.WAVE_MEASUREMENT_MODE,
                                   r'^.{%s}(.{2}).*' % str(436),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Wave Measurement Mode",
                                   default_value=295,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.DYN_PERCENTAGE_POSITION,
                                   r'^.{%s}(.{2}).*' % str(438),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Dyn Percentage Position",
                                   description='Percentage for wave cell positioning.',
                                   default_value=32768,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.WAVE_TRANSMIT_PULSE,
                                   r'^.{%s}(.{2}).*' % str(440),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Wave Transmit Pulse",
                                   default_value=16384,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.WAVE_BLANKING_DISTANCE,
                                   r'^.{%s}(.{2}).*' % str(442),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Fixed Wave Blanking Distance",
                                   default_value=0,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.WAVE_CELL_SIZE,
                                   r'^.{%s}(.{2}).*' % str(444),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Wave Measurement Cell Size",
                                   default_value=0,
                                   units=ParameterUnits.METERS,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.NUMBER_DIAG_SAMPLES,
                                   r'^.{%s}(.{2}).*' % str(446),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Number Diag Samples",
                                   description='Number of diagnostics/wave samples.',
                                   default_value=0,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.A1_2_SPARE,
                                   r'^.{%s}(.{2}).*' % str(448),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="A1 2 Spare",
                                   description='Not used.',
                                   default_value=130,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.B0_2_SPARE,
                                   r'^.{%s}(.{2}).*' % str(450),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="B0 2 Spare",
                                   description='Not used.',
                                   default_value=0,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.NUMBER_SAMPLES_PER_BURST,
                                   r'^.{%s}(.{2}).*' % str(452),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="Number of Samples per Burst",
                                   value=0,
                                   direct_access=True)
        self._param_dict.add(Parameter.USER_2_SPARE,          # for Vector this is 'SAMPLE_RATE'
                                   r'^.{%s}(.{2}).*' % str(454),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.READ_WRITE,
                                   display_name="Sample Rate",
                                   default_value=16,
                                   startup_param=True)
        self._param_dict.add(Parameter.ANALOG_OUTPUT_SCALE,
                                   r'^.{%s}(.{2}).*' % str(456),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Analog Output Scale Factor",
                                   default_value=6711,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.CORRELATION_THRESHOLD,
                                   r'^.{%s}(.{2}).*' % str(458),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Correlation Threshold",
                                   description='Correlation threshold for resolving ambiguities.',
                                   default_value=0,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.USER_3_SPARE,
                                   r'^.{%s}(.{2}).*' % str(460),
                                   lambda match: match.group(1).encode('hex'),
                                   lambda string: string.decode('hex'),
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="User 3 Spare",
                                   description='Not used.')
        self._param_dict.add(Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG,
                                   r'^.{%s}(.{2}).*' % str(462),
                                   lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                                   NortekProtocolParameterDict.word_to_string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.INT,
                                   visibility=ParameterDictVisibility.IMMUTABLE,
                                   display_name="Transmit Pulse Length Second Lag",
                                   default_value=2,
                                   startup_param=True,
                                   direct_access=True)
        self._param_dict.add(Parameter.USER_4_SPARE,
                                   r'^.{%s}(.{30}).*' % str(464),
                                   lambda match: match.group(1).encode('hex'),
                                   lambda string: string.decode('hex'),
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="User 4 Spare",
                                   description='Not used.')
        self._param_dict.add(Parameter.QUAL_CONSTANTS,
                                   r'^.{%s}(.{16}).*' % str(494),
                                   lambda match: base64.b64encode(match.group(1)),
                                   lambda string: string,
                                   regex_flags=re.DOTALL,
                                   type=ParameterDictType.STRING,
                                   visibility=ParameterDictVisibility.READ_ONLY,
                                   display_name="Qual Constants",
                                   description='Stage match filter constants.',
                                   direct_access=True)