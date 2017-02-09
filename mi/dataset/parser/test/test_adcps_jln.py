#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@stream_handle marine-integrations/mi/dataset/parser/test/test_adcps_jln.py
@author Jeff Roy
@brief Test code for a adcps_jln data parser
Parts of this test code were taken from test_adcpa.py
Due to the nature of the records in PD0 files, (large binary records with hundreds of parameters)
this code verifies select items in the parsed data particle
"""
import copy
import os
from collections import Counter

import numpy
import yaml
from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.adcps_jln.stc.resource import RESOURCE_PATH
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser, AdcpDataParticleType
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()


@attr('UNIT', group='mi')
class AdcpsJlnParserUnitTestCase(ParserUnitTestCase):
    """
    adcps_jln Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'velocity': 'VelocityEarth',
                'engineering': 'AdcpsEngineering',
                'config': 'AdcpsConfig',
                'bottom_track': 'EarthBottom',
                'bottom_track_config': 'BottomConfig',
            }
        }
        # Define test data particles and their associated timestamps which will be
        # compared with returned results

        # test01 data was all manually verified against the IDD
        # and double checked with PD0decoder_v2 MATLAB tool
        self.test01 = {'internal_timestamp': 3581719370.030000,
                       'echo_intensity_beam1': [89, 51, 44, 43, 43, 43, 43, 44, 43, 44, 43, 43, 44, 44, 44,
                                                43, 43, 44, 43, 44, 44, 43, 43, 44, 44, 44, 44, 44, 44, 44,
                                                43, 43, 43, 43, 43, 43, 43, 44, 44, 43, 44, 44, 43, 43, 44,
                                                43, 43, 44, 44, 43, 43, 44, 43, 43, 44],
                       'correlation_magnitude_beam1': [68, 70, 18, 19, 17, 17, 20, 19, 17, 15, 17, 20, 16,
                                                       17, 16, 17, 17, 18, 18, 17, 17, 19, 18, 17, 17, 19,
                                                       19, 17, 16, 16, 18, 19, 19, 17, 19, 19, 19, 18, 20,
                                                       17, 19, 19, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                       'percent_good_3beam': [53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                       'water_velocity_east': [383, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768],
                       'water_velocity_north': [314, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                -32768, -32768, -32768, -32768, -32768, -32768, -32768],
                       'water_velocity_up': [459, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                             -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                             -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                             -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                             -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                             -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                             -32768],
                       'error_velocity': [80, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                          -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                          -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                          -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                          -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                          -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                          -32768]}
        # test02 data was extracted using the PD0decoder_v2 MATLAB tool
        # ensemble 1 of file ADCP_CCE1T_20.000
        self.test02 = {'ensemble_number': 1, 'heading': 21348, 'pitch': 4216, 'roll': 3980}

        # test03 data was extracted using the PD0decoder_v2 MATLAB tool
        # ensemble 20 of file ADCP_CCE1T_20.000
        self.test03 = {'ensemble_number': 20, 'heading': 538, 'pitch': 147, 'roll': 221}

    def assert_result(self, test, particle):
        """
        Suite of tests to run against each returned particle and expected
        results of the same.  The test parameter should be a dictionary
        that contains the keys to be tested in the particle
        the 'internal_timestamp' and 'position' keys are
        treated differently than others but can be verified if supplied
        """

        particle_dict = particle.generate_dict()

        # for efficiency turn the particle values list of dictionaries into a dictionary
        particle_values = {}
        for param in particle_dict.get('values'):
            particle_values[param['value_id']] = param['value']

        # compare each key in the test to the data in the particle
        for key in test:
            test_data = test[key]

            # get the correct data to compare to the test
            if key == 'internal_timestamp':
                particle_data = particle.get_value('internal_timestamp')
                # the timestamp is in the header part of the particle
            elif key == 'position':
                particle_data = self.state_callback_value['position']
                # position corresponds to the position in the file
            else:
                particle_data = particle_values.get(key)
                # others are all part of the parsed values part of the particle

            if particle_data is None:
                # generally OK to ignore index keys in the test data, verify others
                log.warning("\nWarning: assert_result ignoring test key %s, does not exist in particle", key)
            else:
                if isinstance(test_data, float):
                    # slightly different test for these values as they are floats.
                    compare = numpy.abs(test_data - particle_data) <= 1e-5
                    self.assertTrue(compare)
                else:
                    # otherwise they are all ints and should be exactly equal
                    self.assertEqual(test_data, particle_data, msg=particle_values)

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ADCP_data_20130702.PD0 has one record in it
        with open(os.path.join(RESOURCE_PATH, 'ADCP_data_20130702.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(4)

            # this simple test shows the 2 ways to verify results
            self.assert_result(self.test01, particles[0])

            self.assert_particles(particles, 'ADCP_data_20130702.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        # ADCP_CCE1T_20.000 has 20 records in it
        with open(os.path.join(RESOURCE_PATH, 'ADCP_CCE1T_20.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(50)

            self.assert_result(self.test02, particles[0])
            self.assert_result(self.test03, particles[43])

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # ADCP_data_Corrupted.PD0 has one bad record followed by one good in it
        with open(os.path.join(RESOURCE_PATH, 'ADCP_data_Corrupted.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(1)
            self.assert_result(self.test01, particles[0])

    def test_long_stream(self):
        """
        Verify an entire file against a yaml result file.
        """
        with open(os.path.join(RESOURCE_PATH, 'ADCP_CCE1T_20.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(47)

            self.assert_particles(particles, 'ADCP_CCE1T_20.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_bug_10136(self):
        """
        Ensure that bad ensembles are skipped and all valid ensembles are returned.
        """
        with open(os.path.join(RESOURCE_PATH, 'SN_18596_Recovered_Data_RDI_000.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(40000)

            particle_counter = Counter()

            for particle in particles:
                particle_counter[particle._data_particle_type] += 1

            self.assertEqual(particle_counter[AdcpDataParticleType.VELOCITY_EARTH], 13913)

            self.assertTrue(len(self.exception_callback_value) > 0)


def convert_yml(input_file):
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
        'adcp_velocity_earth': ('VelocityEarth', earth),
        'adcp_config': ('AdcpsConfig', config),
        'adcp_engineering': ('AdcpsEngineering', engineering),
    }

    streams = [
        'adcp_velocity_earth',
        'adcp_config',
        'adcp_engineering',
    ]

    always = streams[:1]

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
                record.get('watchdog_restart_occurred', 0) * 0b1000000 +
                record.get('battery_saver_power', 0) * 0b10000000 +
                record.get('pinging', 0) * (0b1 << 8) +
                record.get('cold_wakeup_occurred', 0) * (0b1000000 << 8) +
                record.get('unknown_wakeup_occurred', 0) * (0b10000000 << 8) +
                record.get('clock_read_error', 0) * (0b1 << 16) +
                record.get('unexpected_alarm', 0) * (0b10 << 16) +
                record.get('clock_jump_forward', 0) * (0b100 << 16) +
                record.get('clock_jump_backward', 0) * (0b1000 << 16) +
                record.get('power_fail', 0) * (0b1000 << 24) +
                record.get('spurious_dsp_interrupt', 0) * (0b10000 << 24) +
                record.get('spurious_uart_interrupt', 0) * (0b100000 << 24) +
                record.get('spurious_clock_interrupt', 0) * (0b1000000 << 24) +
                record.get('level_7_interrupt', 0) * (0b10000000 << 24)
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
    yaml.dump(records, open(input_file, 'w'))


def convert_all():
    yml_files = [
        'ADCP_data_20130702.yml',
        'ADCP_CCE1T_20.yml',
    ]

    for f in yml_files:
        convert_yml(os.path.join(RESOURCE_PATH, f))
