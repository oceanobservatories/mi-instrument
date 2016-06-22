import base64
from binascii import unhexlify

from unittest2 import TestCase

from mi.core.instrument.data_particle import DataParticleKey
from mi.instrument.nortek.test.test_driver import user_config_sample, user_config_particle
from mi.instrument.nortek.user_configuration import UserConfiguration


class UserConfigurationTest(TestCase):
    def test_create(self):
        uc = UserConfiguration(user_config_sample())
        self.assertIsNotNone(uc)

    def test_empty(self):
        empty = UserConfiguration()
        self.assertEqual(repr(empty), '\xa5\x00\x00\x01' + '\x00' * 506 + '\x31\xb7')

    def test_round_trip(self):
        sample = user_config_sample()[:-2]
        uc = UserConfiguration(sample)
        uc_repr = repr(uc)
        self.assertTrue(uc.valid)
        self.assertEqual(len(uc_repr), len(sample))

    def test_values(self):
        uc = UserConfiguration(user_config_sample())

        for row in user_config_particle:
            key = row[DataParticleKey.VALUE_ID]
            value = row[DataParticleKey.VALUE]

            self.assertEqual(getattr(uc, key, None), value, msg=key)

    def test_set_non_integer(self):
        uc = UserConfiguration()
        with self.assertRaises(TypeError):
            uc.sample_rate = 'blue'

    def test_set_non_integer_array(self):
        uc = UserConfiguration()
        with self.assertRaises(TypeError):
            uc.deployment_start_time = ['green', 'yellow', 'red', 'blue', 'orange', 'banana']

    def test_set_deploy_start_time(self):
        uc = UserConfiguration(user_config_sample())
        new_time = [0, 0, 0, 0, 0, 0]
        uc.deployment_start_time = new_time
        self.assertEqual(new_time, uc.deployment_start_time)

        new_time = [1, 2, 3, 4, 5, 6]
        uc.deployment_start_time = new_time
        self.assertEqual(new_time, uc.deployment_start_time)

    def test_set_invalid_deploy_start_time(self):
        uc = UserConfiguration(user_config_sample())
        new_time = [0, 0, 0, 0, 0, 0, 0]
        with self.assertRaises(TypeError):
            uc.deployment_start_time = new_time

    def test_set_velocity_adjustment_factor(self):
        uc = UserConfiguration(user_config_sample())
        new_velocity_adjustment_factor_bytes = bytearray(range(180))
        new_velocity_adjustment_factor = base64.b64encode(new_velocity_adjustment_factor_bytes)
        uc.velocity_adjustment_factor = new_velocity_adjustment_factor
        self.assertEqual(new_velocity_adjustment_factor, uc.velocity_adjustment_factor)
        self.assertEqual(new_velocity_adjustment_factor_bytes, uc.velocity_adjustment_factor_bytes)

    def test_set_velocity_adjustment_factor_invalid(self):
        uc = UserConfiguration(user_config_sample())
        new_velocity_adjustment_factor_bytes = bytearray(range(179))
        new_velocity_adjustment_factor = base64.b64encode(new_velocity_adjustment_factor_bytes)
        with self.assertRaises(TypeError):
            uc.velocity_adjustment_factor = new_velocity_adjustment_factor

    def test_set_diag_interval(self):
        uc = UserConfiguration(user_config_sample())

        # low only
        uc.diagnostics_interval = 1
        self.assertEqual(uc.diag_interval_low, 1)
        self.assertEqual(uc.diag_interval_high, 0)
        self.assertEqual(uc.diagnostics_interval, 1)

        # high only
        uc.diagnostics_interval = 256
        self.assertEqual(uc.diag_interval_low, 0)
        self.assertEqual(uc.diag_interval_high, 1)
        self.assertEqual(uc.diagnostics_interval, 256)

        # both
        uc.diagnostics_interval = 257
        self.assertEqual(uc.diag_interval_low, 1)
        self.assertEqual(uc.diag_interval_high, 1)
        self.assertEqual(uc.diagnostics_interval, 257)

        # max
        uc.diagnostics_interval = 65535
        self.assertEqual(uc.diag_interval_low, 255)
        self.assertEqual(uc.diag_interval_high, 255)
        self.assertEqual(uc.diagnostics_interval, 65535)

        # too big
        with self.assertRaises(ValueError):
            uc.diagnostics_interval = 65536

    def test_set_filter_constants(self):
        uc = UserConfiguration(user_config_sample())
        new_bytes = bytearray(range(16))
        new_value = base64.b64encode(new_bytes)
        uc.filter_constants = new_value
        self.assertEqual(new_value, uc.filter_constants)
        self.assertEqual(new_bytes, uc.filter_constants_bytes)

    def test_set_filter_constants_invalid(self):
        uc = UserConfiguration(user_config_sample())
        new_bytes = bytearray(range(17))
        new_value = base64.b64encode(new_bytes)
        with self.assertRaises(TypeError):
            uc.filter_constants = new_value

    def test_tcr_bit_set(self):
        uc = UserConfiguration(user_config_sample())
        uc.tcr = 0

        uc.profile_type = 1
        self.assertEqual(uc.tcr, 0b10)

        uc.mode_type = 1
        self.assertEqual(uc.tcr, 0b110)

        uc.power_level_tcm1 = 1
        self.assertEqual(uc.tcr, 0b100110)

        uc.power_level_tcm2 = 1
        self.assertEqual(uc.tcr, 0b1100110)

        uc.sync_out_position = 1
        self.assertEqual(uc.tcr, 0b11100110)

        uc.sample_on_sync = 1
        self.assertEqual(uc.tcr, 0b111100110)

        uc.start_on_sync = 1
        self.assertEqual(uc.tcr, 0b1111100110)

    def test_get_tcr(self):
        uc = UserConfiguration(user_config_sample())
        expected = 130
        self.assertEqual(uc.tcr, expected)

    def test_set_tcr(self):
        """
        Test setting each bit in the TCR individually
        1    PROFILE_TYPE = 'profile_type'
        2    MODE_TYPE = 'mode_type'
        5    POWER_TCM1 = 'power_level_tcm1'
        6    POWER_TCM2 = 'power_level_tcm2'
        7    SYNC_OUT_POSITION = 'sync_out_position'
        8    SAMPLE_ON_SYNC = 'sample_on_sync'
        9    START_ON_SYNC = 'start_on_sync'
        """
        uc = UserConfiguration(user_config_sample())

        uc.tcr = 0
        self.assertEqual(uc.profile_type, 0)
        self.assertEqual(uc.mode_type, 0)
        self.assertEqual(uc.power_level_tcm1, 0)
        self.assertEqual(uc.power_level_tcm2, 0)
        self.assertEqual(uc.sync_out_position, 0)
        self.assertEqual(uc.sample_on_sync, 0)
        self.assertEqual(uc.start_on_sync, 0)

        uc.tcr = 0b10
        self.assertEqual(uc.profile_type, 1)
        self.assertEqual(uc.mode_type, 0)
        self.assertEqual(uc.power_level_tcm1, 0)
        self.assertEqual(uc.power_level_tcm2, 0)
        self.assertEqual(uc.sync_out_position, 0)
        self.assertEqual(uc.sample_on_sync, 0)
        self.assertEqual(uc.start_on_sync, 0)

        uc.tcr = 0b100
        self.assertEqual(uc.profile_type, 0)
        self.assertEqual(uc.mode_type, 1)
        self.assertEqual(uc.power_level_tcm1, 0)
        self.assertEqual(uc.power_level_tcm2, 0)
        self.assertEqual(uc.sync_out_position, 0)
        self.assertEqual(uc.sample_on_sync, 0)
        self.assertEqual(uc.start_on_sync, 0)

        uc.tcr = 0b100000
        self.assertEqual(uc.profile_type, 0)
        self.assertEqual(uc.mode_type, 0)
        self.assertEqual(uc.power_level_tcm1, 1)
        self.assertEqual(uc.power_level_tcm2, 0)
        self.assertEqual(uc.sync_out_position, 0)
        self.assertEqual(uc.sample_on_sync, 0)
        self.assertEqual(uc.start_on_sync, 0)

        uc.tcr = 0b1000000
        self.assertEqual(uc.profile_type, 0)
        self.assertEqual(uc.mode_type, 0)
        self.assertEqual(uc.power_level_tcm1, 0)
        self.assertEqual(uc.power_level_tcm2, 1)
        self.assertEqual(uc.sync_out_position, 0)
        self.assertEqual(uc.sample_on_sync, 0)
        self.assertEqual(uc.start_on_sync, 0)

        uc.tcr = 0b10000000
        self.assertEqual(uc.profile_type, 0)
        self.assertEqual(uc.mode_type, 0)
        self.assertEqual(uc.power_level_tcm1, 0)
        self.assertEqual(uc.power_level_tcm2, 0)
        self.assertEqual(uc.sync_out_position, 1)
        self.assertEqual(uc.sample_on_sync, 0)
        self.assertEqual(uc.start_on_sync, 0)

        uc.tcr = 0b100000000
        self.assertEqual(uc.profile_type, 0)
        self.assertEqual(uc.mode_type, 0)
        self.assertEqual(uc.power_level_tcm1, 0)
        self.assertEqual(uc.power_level_tcm2, 0)
        self.assertEqual(uc.sync_out_position, 0)
        self.assertEqual(uc.sample_on_sync, 1)
        self.assertEqual(uc.start_on_sync, 0)

        uc.tcr = 0b1000000000
        self.assertEqual(uc.profile_type, 0)
        self.assertEqual(uc.mode_type, 0)
        self.assertEqual(uc.power_level_tcm1, 0)
        self.assertEqual(uc.power_level_tcm2, 0)
        self.assertEqual(uc.sync_out_position, 0)
        self.assertEqual(uc.sample_on_sync, 0)
        self.assertEqual(uc.start_on_sync, 1)

    def test_pcr_bit_set(self):
        uc = UserConfiguration(user_config_sample())
        uc.pcr = 0

        uc.power_level_pcr1 = 1
        self.assertEqual(uc.pcr, 0b100000)

        uc.power_level_pcr2 = 1
        self.assertEqual(uc.pcr, 0b1100000)

    def test_get_pcr(self):
        uc = UserConfiguration(user_config_sample())
        expected = 0
        self.assertEqual(uc.pcr, expected)

    def test_set_pcr(self):
        """
        Test setting each bit in the TCR individually
        5    POWER_PCR1 = 'power_level_pcr1'
        6    POWER_PCR2 = 'power_level_pcr2'
        """
        uc = UserConfiguration(user_config_sample())

        uc.pcr = 0
        self.assertEqual(uc.power_level_pcr1, 0)
        self.assertEqual(uc.power_level_pcr2, 0)

        uc.pcr = 0b100000
        self.assertEqual(uc.power_level_pcr1, 1)
        self.assertEqual(uc.power_level_pcr2, 0)

        uc.pcr = 0b1000000
        self.assertEqual(uc.power_level_pcr1, 0)
        self.assertEqual(uc.power_level_pcr2, 1)

    def test_mode_bit_set(self):
        uc = UserConfiguration(user_config_sample())
        uc.mode = 0

        uc.use_specified_sound_speed = 1
        self.assertEqual(uc.mode, 0b1)

        uc.diagnostics_mode_enable = 1
        self.assertEqual(uc.mode, 0b11)

        uc.analog_output_enable = 1
        self.assertEqual(uc.mode, 0b111)

        uc.output_format_nortek = 1
        self.assertEqual(uc.mode, 0b1111)

        uc.scaling = 1
        self.assertEqual(uc.mode, 0b11111)

        uc.serial_output_enable = 1
        self.assertEqual(uc.mode, 0b111111)

        uc.stage_enable = 1
        self.assertEqual(uc.mode, 0b10111111)

        uc.analog_power_output = 1
        self.assertEqual(uc.mode, 0b110111111)

    def test_get_mode(self):
        uc = UserConfiguration(user_config_sample())
        expected = 48
        self.assertEqual(uc.mode, expected)

    def test_set_mode(self):
        """
        Test setting each bit in the mode individually
        0    USE_SPEC_SOUND_SPEED = 'use_specified_sound_speed'
        1    DIAG_MODE_ON = 'diagnostics_mode_enable'
        2    ANALOG_OUTPUT_ON = 'analog_output_enable'
        3    OUTPUT_FORMAT = 'output_format_nortek'
        4    SCALING = 'scaling'
        5    SERIAL_OUT_ON = 'serial_output_enable'
        7    STAGE_ON = 'stage_enable'
        8    ANALOG_POWER_OUTPUT = 'analog_power_output'
        """
        uc = UserConfiguration(user_config_sample())

        uc.mode = 0
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b1
        self.assertEqual(uc.use_specified_sound_speed, 1)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b10
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 1)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b100
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 1)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b1000
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 1)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b10000
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 1)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b100000
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 1)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b10000000
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 1)
        self.assertEqual(uc.analog_power_output, 0)

        uc.mode = 0b100000000
        self.assertEqual(uc.use_specified_sound_speed, 0)
        self.assertEqual(uc.diagnostics_mode_enable, 0)
        self.assertEqual(uc.analog_output_enable, 0)
        self.assertEqual(uc.output_format_nortek, 0)
        self.assertEqual(uc.scaling, 0)
        self.assertEqual(uc.serial_output_enable, 0)
        self.assertEqual(uc.stage_enable, 0)
        self.assertEqual(uc.analog_power_output, 1)

    def test_mode_test_bit_set(self):
        uc = UserConfiguration(user_config_sample())
        uc.mode_test = 0

        uc.use_dsp_filter = 1
        self.assertEqual(uc.mode_test, 0b1)

        uc.filter_data_output = 1
        self.assertEqual(uc.mode_test, 0b11)

    def test_get_mode_test(self):
        uc = UserConfiguration(user_config_sample())
        expected = 4
        self.assertEqual(uc.mode_test, expected)

    def test_set_mode_test(self):
        """
        Test setting each bit in the mode individually
        0    USE_DSP_FILTER = 'use_dsp_filter'
        1    FILTER_DATA_OUTPUT = 'filter_data_output'
        """
        uc = UserConfiguration(user_config_sample())

        uc.mode_test = 0
        self.assertEqual(uc.use_dsp_filter, 0)
        self.assertEqual(uc.filter_data_output, 0)

        uc.mode_test = 0b1
        self.assertEqual(uc.use_dsp_filter, 1)
        self.assertEqual(uc.filter_data_output, 0)

        uc.mode_test = 0b10
        self.assertEqual(uc.use_dsp_filter, 0)
        self.assertEqual(uc.filter_data_output, 1)

    def test_mode_wave_bit_set(self):
        uc = UserConfiguration(user_config_sample())
        uc.mode_wave = 0

        uc.wave_data_rate = 1
        self.assertEqual(uc.mode_wave, 0b1)

        uc.wave_cell_position = 1
        self.assertEqual(uc.mode_wave, 0b11)

        uc.dynamic_position_type = 1
        self.assertEqual(uc.mode_wave, 0b111)

    def test_get_mode_wave(self):
        uc = UserConfiguration(user_config_sample())
        expected = 4615
        self.assertEqual(uc.mode_wave, expected)

    def test_set_mode_wave(self):
        """
        Test setting each bit in the mode individually
        0    WAVE_DATA_RATE = 'wave_data_rate'
        1    WAVE_CELL_POS = 'wave_cell_position'
        2    DYNAMIC_POS_TYPE = 'dynamic_position_type'
        """
        uc = UserConfiguration(user_config_sample())

        uc.mode_wave = 0
        self.assertEqual(uc.wave_data_rate, 0)
        self.assertEqual(uc.wave_cell_position, 0)
        self.assertEqual(uc.dynamic_position_type, 0)

        uc.mode_wave = 0b1
        self.assertEqual(uc.wave_data_rate, 1)
        self.assertEqual(uc.wave_cell_position, 0)
        self.assertEqual(uc.dynamic_position_type, 0)

        uc.mode_wave = 0b10
        self.assertEqual(uc.wave_data_rate, 0)
        self.assertEqual(uc.wave_cell_position, 1)
        self.assertEqual(uc.dynamic_position_type, 0)

        uc.mode_wave = 0b100
        self.assertEqual(uc.wave_data_rate, 0)
        self.assertEqual(uc.wave_cell_position, 0)
        self.assertEqual(uc.dynamic_position_type, 1)

    def test_ocean_data(self):
        velptd106_hex = '''
            a500 0001 7d00 3100 2000 b501 8000 0600
            0100 0300 2222 0000 0000 0000 0000 0001
            0000 0001 0020 0001 0000 0000 0000 0000
            0049 4430 1815 06c0 a800 0020 0011 4114
            0001 0014 0004 0000 004e 365e 0102 3d1e
            3d39 3d53 3d6e 3d88 3da2 3dbb 3dd4 3ded
            3d06 3e1e 3e36 3e4e 3e65 3e7d 3e93 3eaa
            3ec0 3ed6 3eec 3e02 3f17 3f2c 3f41 3f55
            3f69 3f7d 3f91 3fa4 3fb8 3fca 3fdd 3ff0
            3f02 4014 4026 4037 4049 405a 406b 407c
            408c 409c 40ac 40bc 40cc 40db 40ea 40f9
            4008 4117 4125 4133 4142 414f 415d 416a
            4178 4185 4192 419e 41ab 41b7 41c3 41cf
            41db 41e7 41f2 41fd 4108 4213 421e 4228
            4233 423d 4247 4251 425b 4264 426e 4277
            4280 4289 4291 429a 42a2 42aa 42b2 42ba
            4200 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 001e 005a 005a 00bc
            0232 0000 0000 0000 0007 0000 0000 0000
            0000 0000 0000 001e 0000 0000 002a 0000
            0002 0014 00ea 0114 00ea 010a 0005 0000
            0040 0040 0002 000f 005a 0000 0001 00c8
            0000 0000 000f 00ea 01ea 0100 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0006
            0014 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 000a
            ffcd ff8b 00e5 00ee 000b 0084 ff3d ffa8'''.translate(None, ' \n')
        velptd106 = unhexlify(velptd106_hex)

        velptd302_hex = '''
            a500 0001 7d00 3100 2000 b501 8000 0600
            0100 0300 2222 0000 0000 0000 0000 0001
            0000 0001 0020 0001 0000 0000 0000 0000
            0049 4430 1815 06c0 a800 0020 0011 4114
            0001 0014 0004 0000 004e 365e 0102 3d1e
            3d39 3d53 3d6e 3d88 3da2 3dbb 3dd4 3ded
            3d06 3e1e 3e36 3e4e 3e65 3e7d 3e93 3eaa
            3ec0 3ed6 3eec 3e02 3f17 3f2c 3f41 3f55
            3f69 3f7d 3f91 3fa4 3fb8 3fca 3fdd 3ff0
            3f02 4014 4026 4037 4049 405a 406b 407c
            408c 409c 40ac 40bc 40cc 40db 40ea 40f9
            4008 4117 4125 4133 4142 414f 415d 416a
            4178 4185 4192 419e 41ab 41b7 41c3 41cf
            41db 41e7 41f2 41fd 4108 4213 421e 4228
            4233 423d 4247 4251 425b 4264 426e 4277
            4280 4289 4291 429a 42a2 42aa 42b2 42ba
            4200 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 001e 005a 005a 00bc
            0232 0000 0000 0000 0007 0000 0000 0000
            0000 0000 0000 001e 0000 0000 002a 0000
            0002 0014 00ea 0114 00ea 010a 0005 0000
            0040 0040 0002 000f 005a 0000 0001 00c8
            0000 0000 000f 00ea 01ea 0100 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0006
            0014 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 000a
            ffcd ff8b 00e5 00ee 000b 0084 ff3d ffa8'''.translate(None, ' \n')
        velptd302 = unhexlify(velptd302_hex)

        uc1 = UserConfiguration(velptd106)
        uc2 = UserConfiguration(velptd302)

        print uc1
