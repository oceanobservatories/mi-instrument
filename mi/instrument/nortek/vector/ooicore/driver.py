"""
@package mi.instrument.nortek.vector.ooicore.driver
@file mi/instrument/nortek/vector/ooicore/driver.py
@author Rachel Manoni, Ronald Ronquillo
@brief Driver for the ooicore
Release notes:

Driver for vector
"""

__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

import re
import base64
import struct

from mi.core.exceptions import SampleException
from mi.core.common import BaseEnum, Units
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.data_particle import DataParticle, DataParticleKey, DataParticleValue

from mi.instrument.nortek.driver import NortekDataParticleType, Parameter, InstrumentCmds, \
    USER_CONFIG_DATA_REGEX, validate_checksum
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
        
            unpack_string = '<2s4B2H3h6BH'
        
            sync_id, analog_input2_lsb, count, pressure_msb, analog_input2_msb, pressure_lsw, analog_input1,\
                velocity_beam1, velocity_beam2, velocity_beam3, amplitude_beam1, amplitude_beam2, amplitude_beam3, \
                correlation_beam1, correlation_beam2, correlation_beam3, checksum = struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<11H', self.raw_data, checksum):
                log.warn("Bad vel3d_cd_velocity_data from instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

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
            unpack_string = '<4s6sH8B20sH'
            sync, timestamp, number_of_records, noise1, noise2, noise3, _, correlation1, correlation2, correlation3, _,\
                _, cksum = struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<20H', self.raw_data, cksum):
                log.warn("Bad vel3d_cd_data_header from instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED
        
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
    BATTERY = "battery_voltage_dv"
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
        
            unpack_string = '<4s6s2H4h2bHH'
            
            sync, timestamp, battery, sound_speed, heading, pitch, roll, temperature, error, status, analog_input, cksum =\
                struct.unpack_from(unpack_string, self.raw_data)

            if not validate_checksum('<13H', self.raw_data, cksum):
                log.warn("Bad vel3d_cd_system_data from instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED
        
            timestamp = NortekProtocolParameterDict.convert_time(timestamp)

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
 
        log.debug('VectorSystemDataParticle: particle=%r', result)

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
    NortekInstrumentProtocol.velocity_data_regex.extend(VECTOR_SAMPLE_REGEX)
    NortekInstrumentProtocol.velocity_sync_bytes = VELOCITY_DATA_SYNC_BYTES

    order_of_user_config = [
        Parameter.TRANSMIT_PULSE_LENGTH,
        Parameter.BLANKING_DISTANCE,
        Parameter.RECEIVE_LENGTH,
        Parameter.TIME_BETWEEN_PINGS,
        Parameter.TIME_BETWEEN_BURST_SEQUENCES,
        Parameter.NUMBER_PINGS,
        Parameter.AVG_INTERVAL,
        Parameter.USER_NUMBER_BEAMS,
        Parameter.TIMING_CONTROL_REGISTER,
        Parameter.POWER_CONTROL_REGISTER,
        Parameter.A1_1_SPARE,
        Parameter.B0_1_SPARE,
        Parameter.B1_1_SPARE,
        Parameter.COMPASS_UPDATE_RATE,
        Parameter.COORDINATE_SYSTEM,
        Parameter.NUMBER_BINS,
        Parameter.BIN_LENGTH,
        Parameter.MEASUREMENT_INTERVAL,
        Parameter.DEPLOYMENT_NAME,
        Parameter.WRAP_MODE,
        Parameter.CLOCK_DEPLOY,
        Parameter.DIAGNOSTIC_INTERVAL,
        Parameter.MODE,
        Parameter.ADJUSTMENT_SOUND_SPEED,
        Parameter.NUMBER_SAMPLES_DIAGNOSTIC,
        Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC,
        Parameter.NUMBER_PINGS_DIAGNOSTIC,
        Parameter.MODE_TEST,
        Parameter.ANALOG_INPUT_ADDR,
        Parameter.SW_VERSION,
        Parameter.USER_1_SPARE,
        Parameter.VELOCITY_ADJ_TABLE,
        Parameter.COMMENTS,
        Parameter.WAVE_MEASUREMENT_MODE,
        Parameter.DYN_PERCENTAGE_POSITION,
        Parameter.WAVE_TRANSMIT_PULSE,
        Parameter.WAVE_BLANKING_DISTANCE,
        Parameter.WAVE_CELL_SIZE,
        Parameter.NUMBER_DIAG_SAMPLES,
        Parameter.A1_2_SPARE,
        Parameter.B0_2_SPARE,
        Parameter.NUMBER_SAMPLES_PER_BURST,
        Parameter.SAMPLE_RATE,
        Parameter.ANALOG_OUTPUT_SCALE,
        Parameter.CORRELATION_THRESHOLD,
        Parameter.USER_3_SPARE,
        Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG,
        Parameter.USER_4_SPARE,
        Parameter.QUAL_CONSTANTS]

    spare_param_values = {Parameter.A1_1_SPARE: '',
                            Parameter.B0_1_SPARE: '',
                            Parameter.B1_1_SPARE: '',
                            Parameter.USER_1_SPARE: '',
                            Parameter.A1_2_SPARE: '',
                            Parameter.B0_2_SPARE: '',
                            Parameter.USER_3_SPARE: '',
                            Parameter.USER_4_SPARE: ''}

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

    def _update_params(self):
        """
        Update the parameter dictionary. Issue the read config command. The response
        needs to be saved to param dictionary.
        """
        ret_config = self._do_cmd_resp(InstrumentCmds.READ_USER_CONFIGURATION, response_regex=USER_CONFIG_DATA_REGEX)
        self._param_dict.update(ret_config)

        self.spare_param_values[Parameter.A1_1_SPARE] = ret_config[24:26]
        self.spare_param_values[Parameter.B0_1_SPARE] = ret_config[26:28]
        self.spare_param_values[Parameter.B1_1_SPARE] = ret_config[28:30]
        self.spare_param_values[Parameter.USER_1_SPARE] = ret_config[74:76]
        self.spare_param_values[Parameter.A1_2_SPARE] = ret_config[448:450]
        self.spare_param_values[Parameter.B0_2_SPARE] = ret_config[450:452]
        self.spare_param_values[Parameter.USER_3_SPARE] = ret_config[460:462]
        self.spare_param_values[Parameter.USER_4_SPARE] = ret_config[464:494]

    def _create_set_output(self, parameters):
        """
        load buffer with sync byte (A5), ID byte (01), and size word (# of words in little-endian form)
        'user' configuration is 512 bytes = 256 words long = size 0x100
        """
        output = ['\xa5\x00\x00\x01']
        CHECK_SUM_SEED = 0xb58c

        for param in self.order_of_user_config:
            log.trace('_create_set_output: adding %s to list', param)
            if param == Parameter.COMMENTS:
                output.append(parameters.format(param).ljust(180, "\x00"))
            elif param == Parameter.DEPLOYMENT_NAME:
                output.append(parameters.format(param).ljust(6, "\x00"))
            elif param == Parameter.QUAL_CONSTANTS:
                output.append('\x00'.ljust(16, "\x00"))
            elif param == Parameter.VELOCITY_ADJ_TABLE:
                output.append(base64.b64decode(parameters.format(param)))
            elif param in [Parameter.A1_1_SPARE, Parameter.B0_1_SPARE, Parameter.B1_1_SPARE, Parameter.USER_1_SPARE,
                           Parameter.A1_2_SPARE, Parameter.B0_2_SPARE, Parameter.USER_2_SPARE, Parameter.USER_3_SPARE]:
                output.append(self.spare_param_values.get(param).ljust(2, "\x00"))
            elif param in [Parameter.WAVE_MEASUREMENT_MODE, Parameter.WAVE_TRANSMIT_PULSE, Parameter.WAVE_BLANKING_DISTANCE,
                           Parameter.WAVE_CELL_SIZE, Parameter.NUMBER_DIAG_SAMPLES, Parameter.DYN_PERCENTAGE_POSITION]:
                output.append('\x00'.ljust(2, "\x00"))
            elif param == Parameter.USER_4_SPARE:
                output.append(self.spare_param_values.get(param).ljust(30, "\x00"))
            else:
                output.append(parameters.format(param))
            log.trace('_create_set_output: ADDED %s output size = %s', param, len(output))

        log.debug("Created set output: %r with length: %s", output, len(output))

        checksum = CHECK_SUM_SEED
        output = "".join(output)
        for word_index in range(0, len(output), 2):
            word_value = NortekProtocolParameterDict.convert_word_to_int(output[word_index:word_index+2])
            checksum = (checksum + word_value) % 0x10000
        log.debug('_create_set_output: user checksum = %r', checksum)

        output += (NortekProtocolParameterDict.word_to_string(checksum))

        return output

    ########################################################################
    # Private helpers.
    ########################################################################
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
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Transmit Pulse Length",
                             description="Pulse duration of the transmitted signal.",
                             default_value=2,
                             units=Units.COUNTS,
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
                             description="Minimum sensing range of the sensor.",
                             default_value=16,
                             units=Units.COUNTS,
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
                             description="Length of the received pulse.",
                             default_value=7,
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
                             default_value=44,
                             units=Units.COUNTS,
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
                             description="Length of time between each burst.",
                             default_value=0,
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
                             default_value=0,
                             units=Units.HERTZ,
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
                             description="Interval for continuous sampling.",
                             default_value=64,
                             units=Units.SECOND,
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
                             description="Interval for single measurements.",
                             units=Units.SECOND,
                             default_value=600,
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
                             units=Units.SECOND,
                             startup_param=True,
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
                             default_value=16657,
                             units=Units.METER + '/' + Units.SECOND,
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
                             default_value=1,
                             startup_param=True,
                             direct_access=True)
        self._param_dict.add(Parameter.SW_VERSION,
                             r'^.{%s}(.{2}).*' % str(72),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.STRING,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             direct_access=True,
                             display_name="Software Version",
                             description="Current software version installed on instrument.")
        self._param_dict.add(Parameter.SAMPLE_RATE,
                             r'^.{%s}(.{2}).*' % str(454),
                             lambda match: NortekProtocolParameterDict.convert_word_to_int(match.group(1)),
                             NortekProtocolParameterDict.word_to_string,
                             regex_flags=re.DOTALL,
                             type=ParameterDictType.INT,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Sample Rate",
                             description="Number of samples per burst.",
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
                             description="Scale factor used in calculating analog output.",
                             default_value=6711,
                             startup_param=True,
                             direct_access=True)
