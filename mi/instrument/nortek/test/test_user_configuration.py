import base64
from binascii import unhexlify

from unittest import TestCase

from mi.core.instrument.data_particle import DataParticleKey
from mi.instrument.nortek.test.test_driver import user_config_sample, user_config_particle
from mi.instrument.nortek.user_configuration import UserConfiguration


class UserConfigurationTest(TestCase):
    def assert_round_trip(self, config_string):
        uc = UserConfiguration(config_string)
        self.assertTrue(uc.valid)
        self.assertEqual(config_string, repr(uc))

    def test_create(self):
        uc = UserConfiguration(user_config_sample())
        self.assertIsNotNone(uc)

    def test_empty(self):
        empty = UserConfiguration()
        self.assertEqual(repr(empty), '\xa5\x00\x00\x01' + '\x00' * 506 + '\x31\xb7')

    def test_round_trip(self):
        sample = user_config_sample()[:-2]
        self.assert_round_trip(sample)

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
        velptd106 = '''
            a500 0001 7d00 3100 2000 b501 8000 0600
            0100 0300 2200 0000 0000 0000 0000 0100
            0000 0100 2000 0100 0000 0000 0000 0000
            4944 3018 1506 c0a8 0000 2000 1141 1400
            0100 1400 0400 0000 4e36 5e01 023d 1e3d
            393d 533d 6e3d 883d a23d bb3d d43d ed3d
            063e 1e3e 363e 4e3e 653e 7d3e 933e aa3e
            c03e d63e ec3e 023f 173f 2c3f 413f 553f
            693f 7d3f 913f a43f b83f ca3f dd3f f03f
            0240 1440 2640 3740 4940 5a40 6b40 7c40
            8c40 9c40 ac40 bc40 cc40 db40 ea40 f940
            0841 1741 2541 3341 4241 4f41 5d41 6a41
            7841 8541 9241 9e41 ab41 b741 c341 cf41
            db41 e741 f241 fd41 0842 1342 1e42 2842
            3342 3d42 4742 5142 5b42 6442 6e42 7742
            8042 8942 9142 9a42 a242 aa42 b242 ba42
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 1e00 5a00 5a00 bc02
            3200 0000 0000 0000 0700 0000 0000 0000
            0000 0000 0000 1e00 0000 0000 2a00 0000
            0200 1400 ea01 1400 ea01 0a00 0500 0000
            4000 4000 0200 0f00 5a00 0000 0100 c800
            0000 0000 0f00 ea01 ea01 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0600
            1400 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0aff
            cdff 8b00 e500 ee00 0b00 84ff 3dff a8f2'''.translate(None, ' \n')
        velptd106 = unhexlify(velptd106)

        vel3dc107 = '''
            a500 0001 0200 1000 0700 2c00 0002 0100
            4000 0300 8200 0000 cc4e 0000 0000 0100
            0000 0100 0700 5802 0000 0000 0000 0000
            0312 0917 1507 302a 0000 3000 1141 0100
            0100 1400 0400 0000 8535 5401 f33c 0e3d
            2a3d 443d 5f3d 793d 933d ad3d c63d df3d
            f73d 103e 283e 403e 573e 6e3e 853e 9c3e
            b23e c83e de3e f43e 093f 1e3f 333f 473f
            5c3f 703f 843f 973f aa3f bd3f d03f e33f
            f53f 0740 1940 2b40 3c40 4d40 5e40 6f40
            7f40 9040 a040 b040 bf40 cf40 de40 ed40
            fc40 0b41 1941 2841 3641 4341 5141 5f41
            6c41 7941 8641 9341 9f41 ac41 b841 c441
            d041 db41 e741 f241 fd41 0842 1342 1d42
            2842 3242 3c42 4642 5042 5942 6342 6c42
            7542 7e42 8642 8f42 9742 a042 a842 b042
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 1e00 5a00 5a00 bc02
            3200 0000 0000 0000 0700 0000 0000 0000
            0000 0000 0000 0100 0000 0000 2a00 0000
            0200 1400 ea01 1400 ea01 0a00 0500 0000
            4000 4000 0200 0f00 5a00 0000 0100 c800
            0000 0000 0f00 ea01 ea01 0000 0000 0000
            0000 0000 0712 0080 0040 0000 0000 0000
            8200 0000 0000 1000 b12b 0000 0000 0200
            0100 0000 0000 0000 0000 0000 0000 0000
            0000 0000 0000 0000 0000 0000 0000 0aff
            cdff 8b00 e500 ee00 0b00 84ff 3dff 698b'''.translate(None, ' \n')
        vel3dc107 = unhexlify(vel3dc107)

        self.assert_round_trip(velptd106)
        self.assert_round_trip(vel3dc107)
