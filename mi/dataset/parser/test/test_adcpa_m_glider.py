#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_adcpa_m_glider.py
@author Jeff Roy
@brief Test code for a adcpa_m_glider data parser
Parts of this test code were taken from test_adcpa.py
Due to the nature of the records in PD0 files, (large binary records with hundreds of parameters)
this code verifies select items in the parsed data particle
"""
import copy
import os

import yaml
from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.moas.gl.adcpa.resource import RESOURCE_PATH
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()


@attr('UNIT', group='mi')
class AdcpsMGliderParserUnitTestCase(ParserUnitTestCase):
    """
    AdcpMGlider Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_recov = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'velocity': 'VelocityGlider',
                'engineering': 'GliderEngineering',
                'config': 'GliderConfig',
                'bottom_track': 'EarthBottom',
                'bottom_track_config': 'BottomConfig',
            }
        }

        self.config_telem = self.config_recov

    def test_simple_recov(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, 'ND072022.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)
            particles = parser.get_records(6)

            log.debug('got back %d particles', len(particles))

            self.assert_particles(particles, 'ND072022_recov.yml', RESOURCE_PATH)

    def test_simple_telem(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, 'ND072022.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_telem, stream_handle, self.exception_callback)

            particles = parser.get_records(6)

            log.debug('got back %d particles', len(particles))

            self.assert_particles(particles, 'ND072022_telem.yml', RESOURCE_PATH)

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'ND072023.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(195)
            log.debug('got back %d records', len(particles))

            self.assert_particles(particles, 'ND072023_recov.yml', RESOURCE_PATH)

    def test_with_status_data(self):
        """
        Verify the parser will work with a file that contains the status data block
        This was found during integration test with real recovered data
        """

        with open(os.path.join(RESOURCE_PATH, 'ND161646.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(250)
            log.debug('got back %d records', len(particles))

            self.assert_particles(particles, 'ND161646.yml', RESOURCE_PATH)

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # LB180210_3_corrupted.PD0 has three records in it, the 2nd record was corrupted
        with open(os.path.join(RESOURCE_PATH, 'LB180210_3_corrupted.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)

            # try to get 3 particles, should only get 2 back
            # the second one should correspond to ensemble 3
            parser.get_records(3)

            log.debug('Exceptions : %s', self.exception_callback_value[0])

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))


def convert_yml(input_file):
    bottom_config = [
        'bt_pings_per_ensemble',
        'bt_delay_before_reacquire',
        'bt_corr_magnitude_min',
        'bt_eval_magnitude_min',
        'bt_percent_good_min',
        'bt_mode',
        'bt_error_velocity_max',
        'bt_max_depth',
        ]

    bottom = [
        'bt_beam1_range',
        'bt_beam2_range',
        'bt_beam3_range',
        'bt_beam4_range',
        'bt_eastward_velocity',
        'bt_northward_velocity',
        'bt_upward_velocity',
        'bt_error_velocity',
        'bt_beam1_correlation',
        'bt_beam2_correlation',
        'bt_beam3_correlation',
        'bt_beam4_correlation',
        'bt_beam1_eval_amp',
        'bt_beam2_eval_amp',
        'bt_beam3_eval_amp',
        'bt_beam4_eval_amp',
        'bt_beam1_percent_good',
        'bt_beam2_percent_good',
        'bt_beam3_percent_good',
        'bt_beam4_percent_good',
        'bt_eastward_ref_layer_velocity',
        'bt_northward_ref_layer_velocity',
        'bt_upward_ref_layer_velocity',
        'bt_error_ref_layer_velocity',
        'bt_beam1_ref_correlation',
        'bt_beam2_ref_correlation',
        'bt_beam3_ref_correlation',
        'bt_beam4_ref_correlation',
        'bt_beam1_ref_intensity',
        'bt_beam2_ref_intensity',
        'bt_beam3_ref_intensity',
        'bt_beam4_ref_intensity',
        'bt_beam1_ref_percent_good',
        'bt_beam2_ref_percent_good',
        'bt_beam3_ref_percent_good',
        'bt_beam4_ref_percent_good',
        'bt_beam1_rssi_amplitude',
        'bt_beam2_rssi_amplitude',
        'bt_beam3_rssi_amplitude',
        'bt_beam4_rssi_amplitude',
        'bt_ref_layer_min',
        'bt_ref_layer_near',
        'bt_ref_layer_far',
        'bt_gain',
    ]

    earth = [
        'num_cells',
        'cell_length',
        'bin_1_distance',
        'ensemble_number',
        'heading',
        'pitch',
        'roll',
        'salinity',
        'temperature',
        'transducer_depth',
        'pressure',
        'sysconfig_vertical_orientation',
        'water_velocity_east',
        'water_velocity_north',
        'water_velocity_up',
        'error_velocity',
        'water_velocity_forward',
        'water_velocity_starboard',
        'water_velocity_vertical',
        'correlation_magnitude_beam1',
        'correlation_magnitude_beam2',
        'correlation_magnitude_beam3',
        'correlation_magnitude_beam4',
        'echo_intensity_beam1',
        'echo_intensity_beam2',
        'echo_intensity_beam3',
        'echo_intensity_beam4',
        'percent_good_3beam',
        'percent_transforms_reject',
        'percent_bad_beams',
        'percent_good_4beam',
    ]

    config = [
        'firmware_version',
        'firmware_revision',
        'data_flag',
        'lag_length',
        'num_beams',
        'num_cells',
        'pings_per_ensemble',
        'cell_length',
        'blank_after_transmit',
        'signal_processing_mode',
        'low_corr_threshold',
        'num_code_repetitions',
        'percent_good_min',
        'error_vel_threshold',
        'time_per_ping_minutes',
        'time_per_ping_seconds',
        'heading_alignment',
        'heading_bias',
        'reference_layer_start',
        'reference_layer_stop',
        'false_target_threshold',
        'low_latency_trigger',
        'transmit_lag_distance',
        'cpu_board_serial_number',
        'system_bandwidth',
        'system_power',
        'serial_number',
        'beam_angle',
        'sysconfig_frequency',
        'sysconfig_beam_pattern',
        'sysconfig_sensor_config',
        'sysconfig_head_attached',
        'sysconfig_vertical_orientation',
        'sysconfig_beam_angle',
        'sysconfig_beam_config',
        'coord_transform_type',
        'coord_transform_tilts',
        'coord_transform_beams',
        'coord_transform_mapping',
        'sensor_source_speed',
        'sensor_source_depth',
        'sensor_source_heading',
        'sensor_source_pitch',
        'sensor_source_roll',
        'sensor_source_conductivity',
        'sensor_source_temperature',
        'sensor_source_temperature_eu',
        'sensor_available_speed',
        'sensor_available_depth',
        'sensor_available_heading',
        'sensor_available_pitch',
        'sensor_available_roll',
        'sensor_available_conductivity',
        'sensor_available_temperature',
        'sensor_available_temperature_eu',
    ]

    engineering = [
        'transmit_pulse_length',
        'speed_of_sound',
        'mpt_minutes',
        'mpt_seconds',
        'heading_stdev',
        'pitch_stdev',
        'roll_stdev',
        'pressure_variance',
        'adc_ambient_temp',
        'adc_attitude',
        'adc_attitude_temp',
        'adc_contamination_sensor',
        'adc_pressure_minus',
        'adc_pressure_plus',
        'adc_transmit_current',
        'adc_transmit_voltage',
    ]

    stream_map = {
        'adcp_velocity_glider': ('VelocityGlider', earth),
        'adcp_config': ('GliderConfig', config),
        'adcp_engineering': ('GliderEngineering', engineering),
        'adcp_bottom_track_earth': ('EarthBottom', bottom),
        'adcp_bottom_track_config': ('BottomConfig', bottom_config),
    }

    streams = [
        'adcp_velocity_glider',
        'adcp_config',
        'adcp_engineering',
        'adcp_bottom_track_earth',
        'adcp_bottom_track_config'
    ]

    always = ['adcp_velocity_glider', 'adcp_bottom_track_earth']

    last = {}

    def create_particle(record, index, stream):
        klass, fields = stream_map.get(stream)
        particle = {field: record.get(field) for field in fields if field in record}
        particle['_index'] = index
        particle['particle_object'] = klass
        particle['particle_type'] = stream
        particle['internal_timestamp'] = record['internal_timestamp']

        if 'time_per_ping_seconds' in fields:
            seconds = particle['time_per_ping_seconds']
            int_seconds = int(seconds)
            hundredths = int(100 * (seconds - int_seconds))
            particle['time_per_ping_hundredths'] = hundredths
            particle['time_per_ping_seconds'] = int_seconds

        if 'mpt_seconds' in fields:
            seconds = particle['mpt_seconds']
            int_seconds = int(seconds)
            hundredths = int(100 * (seconds - int_seconds))
            particle['mpt_hundredths'] = hundredths
            particle['mpt_seconds'] = int_seconds

        if stream == 'adcp_engineering':
            bit = (record['bit_error_count'] << 8) + record['bit_error_number']
            particle['bit_result'] = bit

        return particle

    def changed(particle):
        particle = copy.deepcopy(particle)
        stream = particle.pop('particle_type')
        particle.pop('particle_object')
        particle.pop('_index')
        particle.pop('internal_timestamp')
        last_values = last.get(stream)
        if last_values == particle:
            return False

        last[stream] = particle
        return True

    out_records = []
    records = yaml.load(open(input_file))
    index = 1
    for record in records['data']:
        for stream in streams:
            particle = create_particle(record, index, stream)
            if stream in always or changed(particle):
                out_records.append(particle)
                index += 1

    records['data'] = out_records
    yaml.dump(records, open(input_file, 'w'))


def convert_all():
    yml_files = [
        'ND072023_recov.yml',
        'ND072022_recov.yml',
        'ND072022_telem.yml',
        'ND072023_telem.yml',
        'ND161646.yml',
        'NE051351.yml',
        'NE051400.yml',
    ]

    for f in yml_files:
        convert_yml(os.path.join(RESOURCE_PATH, f))

