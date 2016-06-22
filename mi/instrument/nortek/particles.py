import base64
import struct
from collections import namedtuple
from datetime import datetime

import re

from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException
from mi.core.instrument.data_particle import DataParticle, CommonDataParticleType, DataParticleKey, DataParticleValue
from mi.instrument.nortek import common
from mi.instrument.nortek.user_configuration import UserConfigKey, UserConfigCompositeKey, UserConfiguration
from ooi.logging import log


VID = DataParticleKey.VALUE_ID
VAL = DataParticleKey.VALUE

namedtuple_store = {}


def validate_checksum(str_struct, raw_data, offset=-2):
    checksum = struct.unpack_from('<H', raw_data, offset)[0]
    if (0xb58c + sum(struct.unpack_from(str_struct, raw_data))) & 0xffff != checksum:
        return False
    return True


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
    List of particles
    """
    RAW = CommonDataParticleType.RAW
    HARDWARE_CONFIG = 'nortek_hardware_configuration'
    HEAD_CONFIG = 'nortek_head_configuration'
    USER_CONFIG = 'nortek_user_configuration'
    CLOCK = 'nortek_clock_data'
    BATTERY = 'nortek_battery_voltage'
    ID_STRING = 'nortek_identification_string'


class NortekHardwareConfigDataParticleKey(BaseEnum):
    """
    Particle key for the hw config
    """
    SERIAL_NUM = 'instrmt_type_serial_number'
    RECORDER_INSTALLED = 'recorder_installed'
    COMPASS_INSTALLED = 'compass_installed'
    BOARD_FREQUENCY = 'board_frequency'
    PIC_VERSION = 'pic_version'
    HW_REVISION = 'hardware_revision'
    RECORDER_SIZE = 'recorder_size'
    VELOCITY_RANGE = 'velocity_range'
    FW_VERSION = 'firmware_version'
    STATUS = 'status'
    CONFIG = 'config'
    CHECKSUM = 'checksum'


class NortekHardwareConfigDataParticle(DataParticle):
    """
    Routine for parsing hardware config data into a data particle structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.HARDWARE_CONFIG

    def _build_parsed_values(self):
        """
        Take the hardware config data and parse it into
        values with appropriate tags.
        """
        try:
            unpack_string = '<4s14s2s4H2s12s4sh2s'
            (sync, serial_num, config, board_frequency, pic_version, hw_revision,
             recorder_size, status, spare, fw_version, cksum, _) = struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<23H', self.raw_data, -4):
                log.warn("_parse_read_hw_config: Bad read hw response from instrument (%r)", self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            config = common.convert_word_to_bit_field(config)
            status = common.convert_word_to_bit_field(status)
            recorder_installed = config[-1]
            compass_installed = config[-2]
            velocity_range = status[-1]

        except Exception:
            log.error('Error creating particle hardware config, raw data: %r', self.raw_data)
            raise SampleException

        result = [{VID: NortekHardwareConfigDataParticleKey.SERIAL_NUM, VAL: serial_num},
                  {VID: NortekHardwareConfigDataParticleKey.RECORDER_INSTALLED, VAL: recorder_installed},
                  {VID: NortekHardwareConfigDataParticleKey.COMPASS_INSTALLED, VAL: compass_installed},
                  {VID: NortekHardwareConfigDataParticleKey.BOARD_FREQUENCY, VAL: board_frequency},
                  {VID: NortekHardwareConfigDataParticleKey.PIC_VERSION, VAL: pic_version},
                  {VID: NortekHardwareConfigDataParticleKey.HW_REVISION, VAL: hw_revision},
                  {VID: NortekHardwareConfigDataParticleKey.RECORDER_SIZE, VAL: recorder_size},
                  {VID: NortekHardwareConfigDataParticleKey.VELOCITY_RANGE, VAL: velocity_range},
                  {VID: NortekHardwareConfigDataParticleKey.FW_VERSION, VAL: fw_version}]

        log.debug('NortekHardwareConfigDataParticle: particle=%r', result)
        return result


class NortekHeadConfigDataParticleKey(BaseEnum):
    """
    Particle key for the head config
    """
    PRESSURE_SENSOR = 'pressure_sensor'
    MAG_SENSOR = 'magnetometer_sensor'
    TILT_SENSOR = 'tilt_sensor'
    TILT_SENSOR_MOUNT = 'tilt_sensor_mounting'
    HEAD_FREQ = 'head_frequency'
    HEAD_TYPE = 'head_type'
    HEAD_SERIAL = 'head_serial_number'
    SYSTEM_DATA = 'system_data'
    NUM_BEAMS = 'number_beams'
    CONFIG = 'config'
    CHECKSUM = 'checksum'


class NortekHeadConfigDataParticle(DataParticle):
    """
    Routine for parsing head config data into a data particle structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.HEAD_CONFIG

    def _build_parsed_values(self):
        """
        Take the head config data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        try:
            unpack_string = '<4s2s2H12s176s22sHh2s'
            sync, config, head_freq, head_type, head_serial, system_data, _, num_beams, cksum, _ = struct.unpack(
                unpack_string, self.raw_data)

            if not validate_checksum('<111H', self.raw_data, -4):
                log.warn("Failed checksum in %s from instrument (%r)", self._data_particle_type, self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            config = common.convert_word_to_bit_field(config)
            system_data = base64.b64encode(system_data)
            head_serial = head_serial.split('\x00', 1)[0]

            pressure_sensor = config[-1]
            mag_sensor = config[-2]
            tilt_sensor = config[-3]
            tilt_mount = config[-4]

        except Exception:
            log.error('Error creating particle head config, raw data: %r', self.raw_data)
            raise SampleException

        result = [{VID: NortekHeadConfigDataParticleKey.PRESSURE_SENSOR, VAL: pressure_sensor},
                  {VID: NortekHeadConfigDataParticleKey.MAG_SENSOR, VAL: mag_sensor},
                  {VID: NortekHeadConfigDataParticleKey.TILT_SENSOR, VAL: tilt_sensor},
                  {VID: NortekHeadConfigDataParticleKey.TILT_SENSOR_MOUNT, VAL: tilt_mount},
                  {VID: NortekHeadConfigDataParticleKey.HEAD_FREQ, VAL: head_freq},
                  {VID: NortekHeadConfigDataParticleKey.HEAD_TYPE, VAL: head_type},
                  {VID: NortekHeadConfigDataParticleKey.HEAD_SERIAL, VAL: head_serial},
                  {VID: NortekHeadConfigDataParticleKey.SYSTEM_DATA, VAL: system_data, DataParticleKey.BINARY: True},
                  {VID: NortekHeadConfigDataParticleKey.NUM_BEAMS, VAL: num_beams}]

        log.debug('NortekHeadConfigDataParticle: particle=%r', result)
        return result


class NortekUserConfigDataParticle(DataParticle):
    """
    Routine for parsing user config data into a data particle structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.USER_CONFIG

    def _build_parsed_values(self):
        """
        Take the user config data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        try:
            config = UserConfiguration(self.raw_data)
        except Exception as e:
            log.error('Error creating particle user config, raw data: %r', self.raw_data)
            raise SampleException(e)

        fields = [
            UserConfigKey.TX_LENGTH,
            UserConfigKey.BLANK_DIST,
            UserConfigKey.RX_LENGTH,
            UserConfigKey.TIME_BETWEEN_PINGS,
            UserConfigKey.TIME_BETWEEN_BURSTS,
            UserConfigKey.NUM_PINGS,
            UserConfigKey.AVG_INTERVAL,
            UserConfigKey.NUM_BEAMS,
            UserConfigKey.PROFILE_TYPE,
            UserConfigKey.MODE_TYPE,
            UserConfigKey.POWER_TCM1,
            UserConfigKey.POWER_TCM2,
            UserConfigKey.SYNC_OUT_POSITION,
            UserConfigKey.SAMPLE_ON_SYNC,
            UserConfigKey.START_ON_SYNC,
            UserConfigKey.POWER_PCR1,
            UserConfigKey.POWER_PCR2,
            UserConfigKey.COMPASS_UPDATE_RATE,
            UserConfigKey.COORDINATE_SYSTEM,
            UserConfigKey.NUM_CELLS,
            UserConfigKey.CELL_SIZE,
            UserConfigKey.MEASUREMENT_INTERVAL,
            UserConfigKey.DEPLOYMENT_NAME,
            UserConfigKey.WRAP_MODE,
            UserConfigCompositeKey.DEPLOY_START_TIME,
            UserConfigCompositeKey.DIAG_INTERVAL,
            UserConfigKey.USE_SPEC_SOUND_SPEED,
            UserConfigKey.DIAG_MODE_ON,
            UserConfigKey.ANALOG_OUTPUT_ON,
            UserConfigKey.OUTPUT_FORMAT,
            UserConfigKey.SCALING,
            UserConfigKey.SERIAL_OUT_ON,
            UserConfigKey.STAGE_ON,
            UserConfigKey.ANALOG_POWER_OUTPUT,
            UserConfigKey.SOUND_SPEED_ADJUST,
            UserConfigKey.NUM_DIAG_SAMPLES,
            UserConfigKey.NUM_BEAMS_PER_CELL,
            UserConfigKey.NUM_PINGS_DIAG,
            UserConfigKey.USE_DSP_FILTER,
            UserConfigKey.FILTER_DATA_OUTPUT,
            UserConfigKey.ANALOG_INPUT_ADDR,
            UserConfigKey.SW_VER,
            UserConfigCompositeKey.VELOCITY_ADJ_FACTOR,
            UserConfigKey.FILE_COMMENTS,
            UserConfigKey.WAVE_DATA_RATE,
            UserConfigKey.WAVE_CELL_POS,
            UserConfigKey.DYNAMIC_POS_TYPE,
            UserConfigKey.PERCENT_WAVE_CELL_POS,
            UserConfigKey.WAVE_TX_PULSE,
            UserConfigKey.FIX_WAVE_BLANK_DIST,
            UserConfigKey.WAVE_CELL_SIZE,
            UserConfigKey.NUM_DIAG_PER_WAVE,
            UserConfigKey.NUM_SAMPLE_PER_BURST,
            UserConfigKey.ANALOG_SCALE_FACTOR,
            UserConfigKey.CORRELATION_THRS,
            UserConfigKey.TX_PULSE_LEN_2ND,
            UserConfigCompositeKey.FILTER_CONSTANTS,
            ]

        result = [{VID: field, VAL: getattr(config, field)} for field in fields]

        log.debug('NortekUserConfigDataParticle: particle=%r', result)
        return result


class NortekEngClockDataParticleKey(BaseEnum):
    """
    Particles for the clock data
    """
    DATE_TIME_ARRAY = "date_time_array"


class NortekEngClockDataParticle(DataParticle):
    """
    Routine for parsing clock engineering data into a data particle structure
    for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.CLOCK

    def _build_parsed_values(self):
        """
        Take the clock data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        try:
            minutes, seconds, day, hour, year, month, _ = struct.unpack('<6B2s', self.raw_data)
        except Exception as e:
            log.error('Error creating particle clock data raw data: %r', self.raw_data)
            raise SampleException(e)

        minutes = int('%02x' % minutes)
        seconds = int('%02x' % seconds)
        day = int('%02x' % day)
        hour = int('%02x' % hour)
        year = int('%02x' % year)
        month = int('%02x' % month)

        result = [{VID: NortekEngClockDataParticleKey.DATE_TIME_ARRAY,
                   VAL: [minutes, seconds, day, hour, year, month]}]

        log.debug('NortekEngClockDataParticle: particle=%r', result)
        return result


class NortekEngBatteryDataParticleKey(BaseEnum):
    """
    Particles for the battery data
    """
    BATTERY_VOLTAGE = "battery_voltage_mv"


class NortekEngBatteryDataParticle(DataParticle):
    """
    Routine for parsing battery engineering data into a data particle
    structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.BATTERY

    def _build_parsed_values(self):
        """
        Take the battery data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        match = common.BATTERY_DATA_REGEX.search(self.raw_data)
        if not match:
            raise SampleException(
                "NortekEngBatteryDataParticle: No regex match of parsed sample data: [%r]" % self.raw_data)

        # Calculate value
        battery_voltage = common.convert_word_to_int(match.group(1))
        if battery_voltage is None:
            raise SampleException("No battery_voltage value parsed")

        # report values
        result = [{VID: NortekEngBatteryDataParticleKey.BATTERY_VOLTAGE,
                   VAL: battery_voltage}]
        log.debug('NortekEngBatteryDataParticle: particle=%r', result)
        return result


class NortekEngIdDataParticleKey(BaseEnum):
    """
    Particles for identification data
    """
    ID = "identification_string"


class NortekEngIdDataParticle(DataParticle):
    """
    Routine for parsing id engineering data into a data particle
    structure for the Nortek sensor.
    """
    _data_particle_type = NortekDataParticleType.ID_STRING

    def _build_parsed_values(self):
        """
        Take the id data and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        match = common.ID_DATA_REGEX.match(self.raw_data)
        if not match:
            raise SampleException("NortekEngIdDataParticle: No regex match of parsed sample data: [%r]" % self.raw_data)

        id_str = match.group(1).split('\x00', 1)[0]

        # report values
        result = [{VID: NortekEngIdDataParticleKey.ID, VAL: id_str}]
        log.debug('NortekEngIdDataParticle: particle=%r', result)
        return result

# VECTOR #


class VectorDataParticleType(BaseEnum):
    """
    List of particles
    """
    RAW = CommonDataParticleType.RAW
    HARDWARE_CONFIG = 'vel3d_cd_hardware_configuration'
    HEAD_CONFIG = 'vel3d_cd_head_configuration'
    USER_CONFIG = 'vel3d_cd_user_configuration'
    CLOCK = 'vel3d_clock_data'
    BATTERY = 'vel3d_cd_battery_voltage'
    ID_STRING = 'vel3d_cd_identification_string'
    VELOCITY = 'vel3d_cd_velocity_data'
    VELOCITY_HEADER = 'vel3d_cd_data_header'
    SYSTEM = 'vel3d_cd_system_data'


class VectorHardwareConfigDataParticle(NortekHardwareConfigDataParticle):
    _data_particle_type = VectorDataParticleType.HARDWARE_CONFIG


class VectorHeadConfigDataParticle(NortekHeadConfigDataParticle):
    _data_particle_type = VectorDataParticleType.HEAD_CONFIG


class VectorUserConfigDataParticle(NortekUserConfigDataParticle):
    _data_particle_type = VectorDataParticleType.USER_CONFIG


class VectorEngClockDataParticle(NortekEngClockDataParticle):
    _data_particle_type = VectorDataParticleType.CLOCK


class VectorEngBatteryDataParticle(NortekEngBatteryDataParticle):
    _data_particle_type = VectorDataParticleType.BATTERY


class VectorEngIdDataParticle(NortekEngIdDataParticle):
    _data_particle_type = VectorDataParticleType.ID_STRING


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
    _data_particle_type = VectorDataParticleType.VELOCITY

    def _build_parsed_values(self):
        """
        Take the velocity data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        log.debug('VectorVelocityDataParticle: raw data =%r', self.raw_data)

        try:

            unpack_string = '<2s4B2H3h6BH'

            (sync_id, analog_input2_lsb, count, pressure_msb, analog_input2_msb, pressure_lsw,
             analog_input1, velocity_beam1, velocity_beam2, velocity_beam3, amplitude_beam1,
             amplitude_beam2, amplitude_beam3, correlation_beam1, correlation_beam2,
             correlation_beam3, checksum) = struct.unpack(unpack_string, self.raw_data)

            if not validate_checksum('<11H', self.raw_data):
                log.warn("Failed checksum in %s from instrument (%r)", self._data_particle_type, self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            analog_input2 = analog_input2_msb * 0x100 + analog_input2_lsb
            pressure = pressure_msb * 0x10000 + pressure_lsw

        except Exception as e:
            log.error('Error creating particle vel3d_cd_velocity_data, raw data: %r', self.raw_data)
            raise SampleException(e)

        result = [{VID: VectorVelocityDataParticleKey.ANALOG_INPUT2, VAL: analog_input2},
                  {VID: VectorVelocityDataParticleKey.COUNT, VAL: count},
                  {VID: VectorVelocityDataParticleKey.PRESSURE, VAL: pressure},
                  {VID: VectorVelocityDataParticleKey.ANALOG_INPUT1, VAL: analog_input1},
                  {VID: VectorVelocityDataParticleKey.VELOCITY_BEAM1, VAL: velocity_beam1},
                  {VID: VectorVelocityDataParticleKey.VELOCITY_BEAM2, VAL: velocity_beam2},
                  {VID: VectorVelocityDataParticleKey.VELOCITY_BEAM3, VAL: velocity_beam3},
                  {VID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM1, VAL: amplitude_beam1},
                  {VID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM2, VAL: amplitude_beam2},
                  {VID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM3, VAL: amplitude_beam3},
                  {VID: VectorVelocityDataParticleKey.CORRELATION_BEAM1, VAL: correlation_beam1},
                  {VID: VectorVelocityDataParticleKey.CORRELATION_BEAM2, VAL: correlation_beam2},
                  {VID: VectorVelocityDataParticleKey.CORRELATION_BEAM3, VAL: correlation_beam3}]

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
    _data_particle_type = VectorDataParticleType.VELOCITY_HEADER

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

            if not validate_checksum('<20H', self.raw_data):
                log.warn("Failed checksum in %s from instrument (%r)", self._data_particle_type, self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            timestamp = common.convert_time(timestamp)
            self.set_internal_timestamp((timestamp-datetime(1900, 1, 1)).total_seconds())

        except Exception as e:
            log.error('Error creating particle vel3d_cd_data_header, raw data: %r', self.raw_data)
            raise SampleException(e)

        result = [{VID: VectorVelocityHeaderDataParticleKey.TIMESTAMP, VAL: str(timestamp)},
                  {VID: VectorVelocityHeaderDataParticleKey.NUMBER_OF_RECORDS, VAL: number_of_records},
                  {VID: VectorVelocityHeaderDataParticleKey.NOISE1, VAL: noise1},
                  {VID: VectorVelocityHeaderDataParticleKey.NOISE2, VAL: noise2},
                  {VID: VectorVelocityHeaderDataParticleKey.NOISE3, VAL: noise3},
                  {VID: VectorVelocityHeaderDataParticleKey.CORRELATION1, VAL: correlation1},
                  {VID: VectorVelocityHeaderDataParticleKey.CORRELATION2, VAL: correlation2},
                  {VID: VectorVelocityHeaderDataParticleKey.CORRELATION3, VAL: correlation3}]

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
    _data_particle_type = VectorDataParticleType.SYSTEM

    def _build_parsed_values(self):
        """
        Take the system data sample format and parse it into
        values with appropriate tags.
        @throws SampleException If there is a problem with sample creation
        """
        log.debug('VectorSystemDataParticle: raw data =%r', self.raw_data)

        try:

            unpack_string = '<4s6s2H4h2bHH'

            (sync, timestamp, battery, sound_speed, heading, pitch,
             roll, temperature, error, status, analog_input, cksum) = struct.unpack_from(unpack_string, self.raw_data)

            if not validate_checksum('<13H', self.raw_data):
                log.warn("Failed checksum in %s from instrument (%r)", self._data_particle_type, self.raw_data)
                self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

            timestamp = common.convert_time(timestamp)
            self.set_internal_timestamp((timestamp-datetime(1900, 1, 1)).total_seconds())

        except Exception as e:
            log.error('Error creating particle vel3d_cd_system_data, raw data: %r', self.raw_data)
            raise SampleException(e)

        result = [{VID: VectorSystemDataParticleKey.TIMESTAMP, VAL: str(timestamp)},
                  {VID: VectorSystemDataParticleKey.BATTERY, VAL: battery},
                  {VID: VectorSystemDataParticleKey.SOUND_SPEED, VAL: sound_speed},
                  {VID: VectorSystemDataParticleKey.HEADING, VAL: heading},
                  {VID: VectorSystemDataParticleKey.PITCH, VAL: pitch},
                  {VID: VectorSystemDataParticleKey.ROLL, VAL: roll},
                  {VID: VectorSystemDataParticleKey.TEMPERATURE, VAL: temperature},
                  {VID: VectorSystemDataParticleKey.ERROR, VAL: error},
                  {VID: VectorSystemDataParticleKey.STATUS, VAL: status},
                  {VID: VectorSystemDataParticleKey.ANALOG_INPUT, VAL: analog_input}]

        log.debug('VectorSystemDataParticle: particle=%r', result)

        return result

# AQUADOPP #


class AquadoppDataParticleType(BaseEnum):
    """
    List of particles
    """
    RAW = CommonDataParticleType.RAW
    VELOCITY = 'velpt_velocity_data'
    HARDWARE_CONFIG = 'velpt_hardware_configuration'
    HEAD_CONFIG = 'velpt_head_configuration'
    USER_CONFIG = 'velpt_user_configuration'
    CLOCK = 'velpt_clock_data'
    BATTERY = 'velpt_battery_voltage'
    ID_STRING = 'velpt_identification_string'


class AquadoppHardwareConfigDataParticle(NortekHardwareConfigDataParticle):
    _data_particle_type = AquadoppDataParticleType.HARDWARE_CONFIG


class AquadoppHeadConfigDataParticle(NortekHeadConfigDataParticle):
    _data_particle_type = AquadoppDataParticleType.HEAD_CONFIG


class AquadoppUserConfigDataParticle(NortekUserConfigDataParticle):
    _data_particle_type = AquadoppDataParticleType.USER_CONFIG


class AquadoppEngClockDataParticle(NortekEngClockDataParticle):
    _data_particle_type = AquadoppDataParticleType.CLOCK


class AquadoppEngBatteryDataParticle(NortekEngBatteryDataParticle):
    _data_particle_type = AquadoppDataParticleType.BATTERY


class AquadoppEngIdDataParticle(NortekEngIdDataParticle):
    _data_particle_type = AquadoppDataParticleType.ID_STRING


class AquadoppVelocityDataParticleKey(BaseEnum):
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


class AquadoppVelocityDataParticle(DataParticle):
    """
    Routine for parsing velocity data into a data particle structure for the Aquadopp DW sensor.
    """
    _data_particle_type = AquadoppDataParticleType.VELOCITY

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

            timestamp = common.convert_time(data.timestamp)
            self.set_internal_timestamp((timestamp-datetime(1900, 1, 1)).total_seconds())

            pressure = data.pressure_msb * 0x10000 + data.pressure_lsw

        except Exception as e:
            log.error('Error creating particle velpt_velocity_data, raw data: %r', self.raw_data)
            raise SampleException(e)

        key = AquadoppVelocityDataParticleKey

        result = [{VID: key.TIMESTAMP, VAL: str(timestamp)},
                  {VID: key.ERROR, VAL: data.error},
                  {VID: key.ANALOG1, VAL: data.analog1},
                  {VID: key.BATTERY_VOLTAGE, VAL: data.battery_voltage},
                  {VID: key.SOUND_SPEED_ANALOG2, VAL: data.sound_speed},
                  {VID: key.HEADING, VAL: data.heading},
                  {VID: key.PITCH, VAL: data.pitch},
                  {VID: key.ROLL, VAL: data.roll},
                  {VID: key.STATUS, VAL: data.status},
                  {VID: key.PRESSURE, VAL: pressure},
                  {VID: key.TEMPERATURE, VAL: data.temperature},
                  {VID: key.VELOCITY_BEAM1, VAL: data.velocity_beam1},
                  {VID: key.VELOCITY_BEAM2, VAL: data.velocity_beam2},
                  {VID: key.VELOCITY_BEAM3, VAL: data.velocity_beam3},
                  {VID: key.AMPLITUDE_BEAM1, VAL: data.amplitude_beam1},
                  {VID: key.AMPLITUDE_BEAM2, VAL: data.amplitude_beam2},
                  {VID: key.AMPLITUDE_BEAM3, VAL: data.amplitude_beam3}]

        return result
