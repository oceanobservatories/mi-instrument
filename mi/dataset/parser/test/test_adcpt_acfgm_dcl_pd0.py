#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcpt_acfgm_dcl_pd0
@fid marine-integrations/mi/dataset/parser/test/test_adcpt_acfgm_dcl_pd0.py
@author Jeff Roy
@brief Test code for a adcpt_acfgm_dcl_pd0 data parser
"""
import copy
import os
from datetime import datetime

import yaml
from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.driver.adcpt_acfgm.dcl.pd0.resource import RESOURCE_PATH
from mi.dataset.parser.adcpt_acfgm_dcl_pd0 import AdcptAcfgmDclPd0Parser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class AdcptAcfgmPd0DclParserUnitTestCase(ParserUnitTestCase):
    """
    Adcp_jln Parser unit test suite
    """
    def state_callback(self, state, fid_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.fid_ingested_value = fid_ingested

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_recov = {}
        self.config_telem = self.config_recov
        self.fid_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write('  - _index: %d\n' %(i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def test_recov(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, '20140424.adcpt.log'), 'rU') as stream_handle:
            parser = AdcptAcfgmDclPd0Parser(self.config_recov,
                                            stream_handle,
                                            self.exception_callback,
                                            self.state_callback,
                                            self.publish_callback)

            particles = parser.get_records(31)

            log.debug('got back %d particles', len(particles))

            # Note the yml file was produced from the parser output but was hand verified
            # against the sample outputs provided in the IDD
            self.assert_particles(particles, '20140424.recov.adcpt.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_telem(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, '20140424.adcpt.log'), 'rb') as stream_handle:
            parser = AdcptAcfgmDclPd0Parser(self.config_telem,
                                            stream_handle,
                                            self.exception_callback,
                                            self.state_callback,
                                            self.publish_callback)

            particles = parser.get_records(31)

            log.debug('got back %d particles', len(particles))

            self.assert_particles(particles, '20140424.telem.adcpt.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        #20140424.adcpt_BAD.log has a corrupt record in it
        with open(os.path.join(RESOURCE_PATH, '20140424.adcpt_BAD.log'), 'rb') as stream_handle:
            parser = AdcptAcfgmDclPd0Parser(self.config_recov,
                                            stream_handle,
                                            self.exception_callback,
                                            self.state_callback,
                                            self.publish_callback)

            #try to get a particle, should get none
            parser.get_records(1)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

    def test_live_data(self):
        files_without_records = [
            '20140424.adcpt_BAD.log',
            '20141007.adcpt.log',
            '20141008.adcpt.log',
        ]
        for filename in os.listdir(RESOURCE_PATH):
            if filename.endswith('.log'):
                log.debug('Testing file: %s', filename)
                with open(os.path.join(RESOURCE_PATH, filename), 'rb') as fh:

                    parser = AdcptAcfgmDclPd0Parser(self.config_telem,
                                                    fh,
                                                    self.exception_callback,
                                                    self.state_callback,
                                                    self.publish_callback)

                    particles = parser.get_records(100)

                    log.debug('got back %d particles', len(particles))
                    if filename not in files_without_records:
                        self.assertGreater(len(particles), 0)


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
    ntp_epoch = datetime(1900, 1, 1)

    def create_internal_timestamp(record):
        rtc = record['real_time_clock']
        dts = datetime(rtc[0] + 2000, *rtc[1:-1])
        print dts

        rtc_time = (dts - ntp_epoch).total_seconds() + rtc[-1] / 100.0
        return rtc_time

    def create_particle(record, index, stream, int_ts):
        klass, fields = stream_map.get(stream)
        particle = {field: record.get(field) for field in fields if field in record}
        particle['_index'] = index
        particle['particle_object'] = klass
        particle['particle_type'] = stream
        particle['internal_timestamp'] = create_internal_timestamp(record)
        particle['port_timestamp'] = record['internal_timestamp']

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
        particle.pop('port_timestamp')
        last_values = last.get(stream)
        if last_values == particle:
            return False

        last[stream] = particle
        return True

    out_records = []
    records = yaml.load(open(input_file))
    index = 1
    base_internal_ts = 3607286478.639999866
    increment = 600
    for tindex, record in enumerate(records['data']):
        for stream in streams:
            particle = create_particle(record, index, stream, base_internal_ts + increment * tindex)
            if stream in always or changed(particle):
                out_records.append(particle)
                index += 1

    records['data'] = out_records
    yaml.dump(records, open(input_file, 'w'))


def convert_all():
    yml_files = [
        '20140424.recov.adcpt.yml',
        '20140424.telem.adcpt.yml',
    ]

    for f in yml_files:
        convert_yml(os.path.join(RESOURCE_PATH, f))
