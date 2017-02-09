#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_adcpa_n.py
@author Jeff Roy
@brief Test code for a adcpa_n data parser

"""
import copy
import os

import yaml
from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.adcpa_n.resource import RESOURCE_PATH
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class AdcpNParserUnitTestCase(ParserUnitTestCase):
    """
    AdcpNParser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpa_n',
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'velocity': 'VelocityInst',
                'engineering': 'AuvEngineering',
                'config': 'AuvConfig',
                'bottom_track': 'InstBottom',
                'bottom_track_config': 'BottomConfig',
            }
        }

    def test_simple(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp_auv_3.pd0'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(13)

            log.debug('got back %d particles', len(particles))

            # Note yaml file was generated but hand checked against output of
            # vendor supplied MATLAB tool outputs
            self.assert_particles(particles, 'adcp_auv_3.yml', RESOURCE_PATH)
            self.assertEqual(len(self.exception_callback_value), 0)

    def test_get_many(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp_auv_51.pd0'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(178)

            log.debug('got back %d particles', len(particles))

            # Note yaml file was generated but hand checked against output of
            # vendor supplied MATLAB tool outputs
            self.assert_particles(particles, 'adcp_auv_51.yml', RESOURCE_PATH)
            self.assertEqual(len(self.exception_callback_value), 0)

    def test_long_stream(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp.adc'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(20000)  # ask for 20000 should get 13294

            log.debug('got back %d particles', len(particles))
            self.assertEqual(len(particles), 13294)
            self.assertEqual(self.exception_callback_value, [])


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
        'bt_forward_velocity',
        'bt_starboard_velocity',
        'bt_vertical_velocity',
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
        'bt_forward_ref_layer_velocity',
        'bt_starboard_ref_layer_velocity',
        'bt_vertical_ref_layer_velocity',
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

    ship = [
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
        'bit_result',
        'error_status_word'
    ]

    stream_map = {
        'adcp_velocity_inst': ('VelocityInst', ship),
        'adcp_config': ('AuvConfig', config),
        'adcp_engineering': ('AuvEngineering', engineering),
        'adcp_bottom_track_inst': ('InstBottom', bottom),
        'adcp_bottom_track_config': ('BottomConfig', bottom_config),
    }

    streams = [
        'adcp_velocity_inst',
        'adcp_config',
        'adcp_engineering',
        'adcp_bottom_track_inst',
        'adcp_bottom_track_config'
    ]

    always = ['adcp_velocity_ship', 'adcp_bottom_track_ship']

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
            bit_result = (
                record.get('bit_result_demod_1', 0) * 0b10000 +
                record.get('bit_result_demod_0', 0) * 0b1000 +
                record.get('bit_result_timing', 0) * 0b10
            )
            particle['bit_result'] = bit_result

            esw = (
                record.get('bus_error_exception', 0) +
                record.get('address_error_exception', 0) * 0b10 +
                record.get('illegal_instruction_exception', 0) * 0b100 +
                record.get('zero_divide_instruction', 0) * 0b1000 +
                record.get('emulator_exception', 0) * 0b10000 +
                record.get('unassigned_exception', 0) * 0b100000 +
                record.get('pinging', 0) * (0b1 << 8) +
                record.get('cold_wakeup_occurred', 0) * (0b1000000 << 8) +
                record.get('unknown_wakeup_occurred', 0) * (0b10000000 << 8) +
                record.get('clock_read_error', 0) * (0b1 << 16) +
                record.get('spurious_uart_interrupt', 0) * (0b100000 << 24) +
                record.get('spurious_clock_interrupt', 0) * (0b1000000 << 24) +
                record.get('power_fail', 0) * (0b1000 << 24)
            )
            particle['error_status_word'] = esw

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
    yaml.dump(records, open(input_file, 'w'), default_flow_style=False)


def convert_all():
    yml_files = ['adcp_auv_3.yml', 'adcp_auv_51.yml']
    for f in yml_files:
        convert_yml(os.path.join(RESOURCE_PATH, f))
