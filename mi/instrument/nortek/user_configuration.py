import base64
import struct
import binascii
from _ctypes import addressof, sizeof

from ctypes import LittleEndianStructure, c_uint
from ctypes import c_ubyte, c_ushort, c_char, memmove
from io import BytesIO
from pprint import pformat

from mi.core.common import BaseEnum


class UserConfigCompositeKey(BaseEnum):
    TCR = 'tcr'
    PCR = 'pcr'
    DEPLOY_START_TIME = 'deployment_start_time'
    DIAG_INTERVAL = 'diagnostics_interval'
    MODE = 'mode'
    MODE_TEST = 'mode_test'
    VELOCITY_ADJ_FACTOR = 'velocity_adjustment_factor'
    WAVE_MODE = 'wave_mode'
    FILTER_CONSTANTS = 'filter_constants'


class UserConfigKey(BaseEnum):
    """
    User Config particle keys
    """
    SYNC = 'sync'
    ID = 'id'
    SIZE = 'size'
    TX_LENGTH = 'transmit_pulse_length'
    BLANK_DIST = 'blanking_distance'
    RX_LENGTH = 'receive_length'
    TIME_BETWEEN_PINGS = 'time_between_pings'
    TIME_BETWEEN_BURSTS = 'time_between_bursts'
    NUM_PINGS = 'number_pings'
    AVG_INTERVAL = 'average_interval'
    NUM_BEAMS = 'number_beams'
    PROFILE_TYPE = 'profile_type'
    MODE_TYPE = 'mode_type'
    # TCR
    POWER_TCM1 = 'power_level_tcm1'
    POWER_TCM2 = 'power_level_tcm2'
    SYNC_OUT_POSITION = 'sync_out_position'
    SAMPLE_ON_SYNC = 'sample_on_sync'
    START_ON_SYNC = 'start_on_sync'
    # END TCR
    # PCR
    POWER_PCR1 = 'power_level_pcr1'
    POWER_PCR2 = 'power_level_pcr2'
    # END PCR
    COMPASS_UPDATE_RATE = 'compass_update_rate'
    COORDINATE_SYSTEM = 'coordinate_system'
    NUM_CELLS = 'number_cells'
    CELL_SIZE = 'cell_size'
    MEASUREMENT_INTERVAL = 'measurement_interval'
    DEPLOYMENT_NAME = 'deployment_name'
    WRAP_MODE = 'wrap_mode'
    DEPLOY_START_TIME_BYTES = 'deployment_start_time_bytes'
    DIAG_INTERVAL_LOW = 'diag_interval_low'
    DIAG_INTERVAL_HIGH = 'diag_interval_high'
    # MODE
    USE_SPEC_SOUND_SPEED = 'use_specified_sound_speed'
    DIAG_MODE_ON = 'diagnostics_mode_enable'
    ANALOG_OUTPUT_ON = 'analog_output_enable'
    OUTPUT_FORMAT = 'output_format_nortek'
    SCALING = 'scaling'
    SERIAL_OUT_ON = 'serial_output_enable'
    STAGE_ON = 'stage_enable'
    ANALOG_POWER_OUTPUT = 'analog_power_output'
    # END MODE
    SOUND_SPEED_ADJUST = 'sound_speed_adjust_factor'
    NUM_DIAG_SAMPLES = 'number_diagnostics_samples'
    NUM_BEAMS_PER_CELL = 'number_beams_per_cell'
    NUM_PINGS_DIAG = 'number_pings_diagnostic'
    # MODE TEST
    USE_DSP_FILTER = 'use_dsp_filter'
    FILTER_DATA_OUTPUT = 'filter_data_output'
    # END MODE TEST
    ANALOG_INPUT_ADDR = 'analog_input_address'
    SW_VER = 'software_version'
    VELOCITY_ADJ_FACTOR_BYTES = 'velocity_adjustment_factor_bytes'
    FILE_COMMENTS = 'file_comments'
    # WAVE MODE
    WAVE_DATA_RATE = 'wave_data_rate'
    WAVE_CELL_POS = 'wave_cell_position'
    DYNAMIC_POS_TYPE = 'dynamic_position_type'
    # END WAVE MODE
    PERCENT_WAVE_CELL_POS = 'percent_wave_cell_position'
    WAVE_TX_PULSE = 'wave_transmit_pulse'
    FIX_WAVE_BLANK_DIST = 'fixed_wave_blanking_distance'
    WAVE_CELL_SIZE = 'wave_measurement_cell_size'
    NUM_DIAG_PER_WAVE = 'number_diagnostics_per_wave'
    NUM_SAMPLE_PER_BURST = 'number_samples_per_burst'
    SAMPLE_RATE = 'sample_rate'
    ANALOG_SCALE_FACTOR = 'analog_scale_factor'
    CORRELATION_THRS = 'correlation_threshold'
    TX_PULSE_LEN_2ND = 'transmit_pulse_length_2nd'
    FILTER_CONSTANTS_BYTES = 'filter_constants_bytes'
    CHECKSUM = 'checksum'


class SpareKey(BaseEnum):
    SPARE0 = 'spare0'
    SPARE1 = 'spare1'
    SPARE2 = 'spare2'
    SPARE3 = 'spare3'
    SPARE4 = 'spare4'
    SPARE5 = 'spare5'
    SPARE6 = 'spare6'
    SPARE7 = 'spare7'
    SPARE8 = 'spare8'
    SPARE9 = 'spare9'
    SPARE10 = 'spare10'
    SPARE11 = 'spare11'
    SPARE12 = 'spare12'
    SPARE13 = 'spare13'
    SPARE14 = 'spare14'
    SPARE15 = 'spare15'


class UserConfiguration(LittleEndianStructure):
    _fields_ = [
        (UserConfigKey.SYNC, c_ubyte),
        (UserConfigKey.ID, c_ubyte),
        (UserConfigKey.SIZE, c_ushort),
        (UserConfigKey.TX_LENGTH, c_ushort),
        (UserConfigKey.BLANK_DIST, c_ushort),
        (UserConfigKey.RX_LENGTH, c_ushort),
        (UserConfigKey.TIME_BETWEEN_PINGS, c_ushort),
        (UserConfigKey.TIME_BETWEEN_BURSTS, c_ushort),
        (UserConfigKey.NUM_PINGS, c_ushort),
        (UserConfigKey.AVG_INTERVAL, c_ushort),
        (UserConfigKey.NUM_BEAMS, c_ushort),
        # TCR
        (SpareKey.SPARE0, c_ushort, 1),
        (UserConfigKey.PROFILE_TYPE, c_ushort, 1),
        (UserConfigKey.MODE_TYPE, c_ushort, 1),
        (SpareKey.SPARE1, c_ushort, 2),
        (UserConfigKey.POWER_TCM1, c_ushort, 1),
        (UserConfigKey.POWER_TCM2, c_ushort, 1),
        (UserConfigKey.SYNC_OUT_POSITION, c_ushort, 1),
        (UserConfigKey.SAMPLE_ON_SYNC, c_ushort, 1),
        (UserConfigKey.START_ON_SYNC, c_ushort, 1),
        (SpareKey.SPARE2, c_ushort, 6),
        # TCR (END)
        # PCR
        (SpareKey.SPARE3, c_ushort, 5),
        (UserConfigKey.POWER_PCR1, c_ushort, 1),
        (UserConfigKey.POWER_PCR2, c_ushort, 1),
        (SpareKey.SPARE4, c_ushort, 9),
        (SpareKey.SPARE5, c_uint),
        (SpareKey.SPARE6, c_ushort),
        # PCR (END)
        (UserConfigKey.COMPASS_UPDATE_RATE, c_ushort),
        (UserConfigKey.COORDINATE_SYSTEM, c_ushort),
        (UserConfigKey.NUM_CELLS, c_ushort),
        (UserConfigKey.CELL_SIZE, c_ushort),
        (UserConfigKey.MEASUREMENT_INTERVAL, c_ushort),
        (UserConfigKey.DEPLOYMENT_NAME, c_char * 6),
        (UserConfigKey.WRAP_MODE, c_ushort),
        (UserConfigKey.DEPLOY_START_TIME_BYTES, c_ubyte * 6),
        (UserConfigKey.DIAG_INTERVAL_LOW, c_ushort),
        (UserConfigKey.DIAG_INTERVAL_HIGH, c_ushort),
        # MODE
        (UserConfigKey.USE_SPEC_SOUND_SPEED, c_ushort, 1),
        (UserConfigKey.DIAG_MODE_ON, c_ushort, 1),
        (UserConfigKey.ANALOG_OUTPUT_ON, c_ushort, 1),
        (UserConfigKey.OUTPUT_FORMAT, c_ushort, 1),
        (UserConfigKey.SCALING, c_ushort, 1),
        (UserConfigKey.SERIAL_OUT_ON, c_ushort, 1),
        (SpareKey.SPARE7, c_ushort, 1),
        (UserConfigKey.STAGE_ON, c_ushort, 1),
        (UserConfigKey.ANALOG_POWER_OUTPUT, c_ushort, 1),
        (SpareKey.SPARE8, c_ushort, 7),
        # MODE (END)
        (UserConfigKey.SOUND_SPEED_ADJUST, c_ushort),
        (UserConfigKey.NUM_DIAG_SAMPLES, c_ushort),
        (UserConfigKey.NUM_BEAMS_PER_CELL, c_ushort),
        (UserConfigKey.NUM_PINGS_DIAG, c_ushort),
        # MODE_TEST
        (UserConfigKey.USE_DSP_FILTER, c_ushort, 1),
        (UserConfigKey.FILTER_DATA_OUTPUT, c_ushort, 1),
        (SpareKey.SPARE9, c_ushort, 14),
        # MODE_TEST (END)
        (UserConfigKey.ANALOG_INPUT_ADDR, c_ushort),
        (UserConfigKey.SW_VER, c_ushort),
        (SpareKey.SPARE10, c_ushort),
        (UserConfigKey.VELOCITY_ADJ_FACTOR_BYTES, c_ubyte * 180),
        (UserConfigKey.FILE_COMMENTS, c_char * 180),
        # WAVE_MODE
        (UserConfigKey.WAVE_DATA_RATE, c_ushort, 1),
        (UserConfigKey.WAVE_CELL_POS, c_ushort, 1),
        (UserConfigKey.DYNAMIC_POS_TYPE, c_ushort, 1),
        (SpareKey.SPARE11, c_ushort, 13),
        # WAVE_MODE (END)
        (UserConfigKey.PERCENT_WAVE_CELL_POS, c_ushort),
        (UserConfigKey.WAVE_TX_PULSE, c_ushort),
        (UserConfigKey.FIX_WAVE_BLANK_DIST, c_ushort),
        (UserConfigKey.WAVE_CELL_SIZE, c_ushort),
        (UserConfigKey.NUM_DIAG_PER_WAVE, c_ushort),
        (SpareKey.SPARE12, c_ushort),
        (SpareKey.SPARE13, c_ushort),
        (UserConfigKey.NUM_SAMPLE_PER_BURST, c_ushort),
        (UserConfigKey.SAMPLE_RATE, c_ushort),
        (UserConfigKey.ANALOG_SCALE_FACTOR, c_ushort),
        (UserConfigKey.CORRELATION_THRS, c_ushort),
        (SpareKey.SPARE14, c_ushort),
        (UserConfigKey.TX_PULSE_LEN_2ND, c_ushort),
        (SpareKey.SPARE15, c_ubyte * 30),
        (UserConfigKey.FILTER_CONSTANTS_BYTES, c_ubyte * 16),
        (UserConfigKey.CHECKSUM, c_ushort),
    ]

    def __init__(self, data=None):
        super(UserConfiguration, self).__init__()
        if data is not None:
            buf = BytesIO(data)
            buf.readinto(self)
            self.calculated_checksum = self.generate_checksum(data)
        else:
            self.sync = 165
            self.id = 0
            self.size = 256

    def _get_short_at(self, offset):
        return struct.unpack_from('<H', buffer(self), offset)[0]

    def _set_short_at(self, offset, value):
        max_offset = sizeof(self) - 2
        if offset > max_offset:
            raise ValueError
        addr = addressof(self) + offset
        value_string = struct.pack('<H', value)
        memmove(addr, value_string, 2)

    @property
    def tcr(self):
        tcr_offset = UserConfiguration.spare0.offset
        return self._get_short_at(tcr_offset)

    @tcr.setter
    def tcr(self, value):
        tcr_offset = UserConfiguration.spare0.offset
        self._set_short_at(tcr_offset, value)

    @property
    def pcr(self):
        pcr_offset = UserConfiguration.spare3.offset
        return self._get_short_at(pcr_offset)

    @pcr.setter
    def pcr(self, value):
        pcr_offset = UserConfiguration.spare3.offset
        self._set_short_at(pcr_offset, value)

    @property
    def mode(self):
        mode_offset = UserConfiguration.use_specified_sound_speed.offset
        return self._get_short_at(mode_offset)

    @mode.setter
    def mode(self, value):
        mode_offset = UserConfiguration.use_specified_sound_speed.offset
        self._set_short_at(mode_offset, value)

    @property
    def mode_test(self):
        mode_test_offset = UserConfiguration.use_dsp_filter.offset
        return self._get_short_at(mode_test_offset)

    @mode_test.setter
    def mode_test(self, value):
        mode_test_offset = UserConfiguration.use_dsp_filter.offset
        self._set_short_at(mode_test_offset, value)

    @property
    def mode_wave(self):
        mode_wave_offset = UserConfiguration.wave_data_rate.offset
        return self._get_short_at(mode_wave_offset)

    @mode_wave.setter
    def mode_wave(self, value):
        mode_wave_offset = UserConfiguration.wave_data_rate.offset
        self._set_short_at(mode_wave_offset, value)

    @property
    def deployment_start_time(self):
        return [int(binascii.hexlify(chr(c))) for c in self.deployment_start_time_bytes]

    @deployment_start_time.setter
    def deployment_start_time(self, values):
        if not isinstance(values, (list, tuple)) or len(values) != 6:
            raise TypeError
        for each in values:
            if not isinstance(each, int):
                raise TypeError
        self.deployment_start_time_bytes[:] = [int(str(x), 16) for x in values]

    @property
    def velocity_adjustment_factor(self):
        return base64.b64encode(self.velocity_adjustment_factor_bytes)

    @velocity_adjustment_factor.setter
    def velocity_adjustment_factor(self, value):
        value = base64.b64decode(value)
        if not len(value) == 180:
            raise TypeError
        self.velocity_adjustment_factor_bytes[:] = bytearray(value)

    @property
    def filter_constants(self):
        return base64.b64encode(self.filter_constants_bytes)

    @filter_constants.setter
    def filter_constants(self, value):
        value = base64.b64decode(value)
        if not len(value) == 16:
            raise TypeError
        self.filter_constants_bytes[:] = bytearray(value)

    @property
    def diagnostics_interval(self):
        return (self.diag_interval_high << 16) + self.diag_interval_low

    @diagnostics_interval.setter
    def diagnostics_interval(self, value):
        if value >= 2**32:
            raise ValueError
        self.diag_interval_low = value & 0xffff
        self.diag_interval_high = (value & 0xffff0000) >> 16

    @property
    def valid(self):
        return self.calculated_checksum == self.checksum

    @staticmethod
    def generate_checksum(data):
        return 0xb58c + sum(struct.unpack_from('<255H', data)) & 0xffff

    def __repr__(self):
        base = buffer(self)[:-2]
        checksum = self.generate_checksum(base)
        return base + struct.pack('<H', checksum)

    def __str__(self):
        rdict = {}
        for field in self._fields_:
            name = field[0]
            value = getattr(self, name)
            if not isinstance(value, (int, long)):
                value = str(list(value))

            rdict[name] = value
        return pformat(rdict)

    def _dict(self):
        rdict = {}
        for field in self._fields_:
            name = field[0]
            value = getattr(self, name)
            if not isinstance(value, (int, long)):
                value = str(list(value))

            rdict[name] = value
        return rdict

    def diff(self, other):
        diff = {}
        for field in self._fields_:
            name = field[0]
            this = getattr(self, name)
            that = getattr(other, name)
            if not isinstance(this, (int, long)):
                this = list(this)
                that = list(that)

            if this != that:
                diff[name] = (this, that)
        return diff
