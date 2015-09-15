"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/publisher.py
@author Peter Cable
@brief Event publisher
Release notes:

initial release
"""
import json
import urllib
import qpid.messaging as qm
import time
import urlparse

from ooi.exception import ApplicationException

from ooi.logging import log


def extract_param(param, query):
    params = urlparse.parse_qsl(query, keep_blank_values=True)
    return_value = None
    new_params = []

    for name, value in params:
        if name == param:
            return_value = value
        else:
            new_params.append((name, value))

    return return_value, urllib.urlencode(new_params)


class Publisher(object):
    @staticmethod
    def from_url(url, headers=None):
        if headers is None:
            headers = {}

        result = urlparse.urlsplit(url)
        if result.scheme == 'qpid':
            # remove the queue from the url
            queue, query = extract_param('queue', result.query)

            if queue is None:
                raise ApplicationException('No queue provided in qpid url!')

            new_url = urlparse.urlunsplit((result.scheme, result.netloc, result.path,
                                           query, result.fragment))
            return QpidPublisher(new_url, queue, headers)

        elif result.scheme == 'log':
            return LogPublisher()


class QpidPublisher(Publisher):
    def __init__(self, url, queue, headers):
        self.connection = qm.Connection(url, reconnect=True, ssl=False)
        self.queue = queue
        self.session = None
        self.sender = None
        self.headers = headers
        self.connect()
        super(QpidPublisher, self).__init__()

    def connect(self):
        self.connection.open()
        self.session = self.connection.session()
        self.sender = self.session.sender('%s; {create: always, node: {type: queue, durable: True}}' % self.queue)

    def publish(self, events, headers=None):
        msg_headers = self.headers
        if headers is not None:
            # apply any new header values
            msg_headers.update(headers)

        if not isinstance(events, list):
            events = [events]

        # HACK!
        self.connection.error = None

        for event in events:
            message = qm.Message(content=json.dumps(event), content_type='text/plain', durable=True,
                                 properties=msg_headers, user_id='guest')
            log.info('Publishing message: %r', message)
            self.sender.send(message, sync=False)

        self.sender.sync()

class LogPublisher(Publisher):
    def publish(self, event):
        log.info('Publish event: %r', event)


def test_particles():
    payload = '{"type": "DRIVER_ASYNC_EVENT_SAMPLE", "value": {"quality_flag": "ok", "preferred_timestamp": "port_timestamp", "internal_timestamp": 3646499448.04, "stream_name": "adcp_pd0_beam_parsed", "values": [{"value_id": "ensemble_start_time", "value": 3646499448.04}, {"value_id": "checksum", "value": 13349}, {"value_id": "offset_data_types", "value": [18, 77, 142, 944, 1346, 1748]}, {"value_id": "real_time_clock", "value": [20, 15, 7, 21, 20, 30, 48, 4]}, {"value_id": "velocity_data_id", "value": 256}, {"value_id": "beam_1_velocity", "value": [130, 25, 58, 28, 83, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768]}, {"value_id": "beam_2_velocity", "value": [79, 184, 225, 281, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768]}, {"value_id": "beam_3_velocity", "value": [70, 234, 267, 281, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768]}, {"value_id": "beam_4_velocity", "value": [33, 174, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768]}, {"value_id": "percent_good_id", "value": 1024}, {"value_id": "percent_good_beam1", "value": [100, 100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}, {"value_id": "percent_good_beam2", "value": [100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}, {"value_id": "percent_good_beam3", "value": [100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}, {"value_id": "percent_good_beam4", "value": [100, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}, {"value_id": "header_id", "value": 127}, {"value_id": "data_source_id", "value": 127}, {"value_id": "num_bytes", "value": 2152}, {"value_id": "num_data_types", "value": 6}, {"value_id": "fixed_leader_id", "value": 0}, {"value_id": "firmware_version", "value": 50}, {"value_id": "firmware_revision", "value": 40}, {"value_id": "data_flag", "value": 0}, {"value_id": "lag_length", "value": 53}, {"value_id": "num_beams", "value": 4}, {"value_id": "num_cells", "value": 100}, {"value_id": "pings_per_ensemble", "value": 1}, {"value_id": "cell_length", "value": 800}, {"value_id": "blank_after_transmit", "value": 704}, {"value_id": "signal_processing_mode", "value": 1}, {"value_id": "low_corr_threshold", "value": 64}, {"value_id": "num_code_repetitions", "value": 5}, {"value_id": "percent_good_min", "value": 0}, {"value_id": "error_vel_threshold", "value": 2000}, {"value_id": "time_per_ping_minutes", "value": 0}, {"value_id": "heading_alignment", "value": 0}, {"value_id": "heading_bias", "value": 0}, {"value_id": "bin_1_distance", "value": 1690}, {"value_id": "transmit_pulse_length", "value": 974}, {"value_id": "reference_layer_start", "value": 1}, {"value_id": "reference_layer_stop", "value": 5}, {"value_id": "false_target_threshold", "value": 50}, {"value_id": "low_latency_trigger", "value": 0}, {"value_id": "transmit_lag_distance", "value": 198}, {"value_id": "cpu_board_serial_number", "value": "713015694232387714"}, {"value_id": "system_bandwidth", "value": 0}, {"value_id": "system_power", "value": 255}, {"value_id": "serial_number", "value": "18444"}, {"value_id": "beam_angle", "value": 20}, {"value_id": "variable_leader_id", "value": 128}, {"value_id": "ensemble_number", "value": 557}, {"value_id": "ensemble_number_increment", "value": 0}, {"value_id": "speed_of_sound", "value": 1525}, {"value_id": "transducer_depth", "value": 0}, {"value_id": "heading", "value": 31014}, {"value_id": "pitch", "value": 232}, {"value_id": "roll", "value": -402}, {"value_id": "salinity", "value": 35}, {"value_id": "temperature", "value": 2115}, {"value_id": "mpt_minutes", "value": 0}, {"value_id": "heading_stdev", "value": 0}, {"value_id": "pitch_stdev", "value": 0}, {"value_id": "roll_stdev", "value": 0}, {"value_id": "adc_transmit_current", "value": 120}, {"value_id": "adc_transmit_voltage", "value": 186}, {"value_id": "adc_ambient_temp", "value": 87}, {"value_id": "adc_pressure_plus", "value": 79}, {"value_id": "adc_pressure_minus", "value": 79}, {"value_id": "adc_attitude_temp", "value": 80}, {"value_id": "adc_attitude", "value": 131}, {"value_id": "adc_contamination_sensor", "value": 160}, {"value_id": "pressure", "value": 17}, {"value_id": "pressure_variance", "value": 0}, {"value_id": "sysconfig_frequency", "value": 75}, {"value_id": "sysconfig_beam_pattern", "value": 1}, {"value_id": "sysconfig_sensor_config", "value": 0}, {"value_id": "sysconfig_head_attached", "value": 1}, {"value_id": "sysconfig_vertical_orientation", "value": 1}, {"value_id": "coord_transform_type", "value": 0}, {"value_id": "coord_transform_tilts", "value": 1}, {"value_id": "coord_transform_beams", "value": 1}, {"value_id": "coord_transform_mapping", "value": 1}, {"value_id": "sensor_source_speed", "value": 1}, {"value_id": "sensor_source_depth", "value": 1}, {"value_id": "sensor_source_heading", "value": 1}, {"value_id": "sensor_source_pitch", "value": 1}, {"value_id": "sensor_source_roll", "value": 1}, {"value_id": "sensor_source_conductivity", "value": 0}, {"value_id": "sensor_source_temperature", "value": 1}, {"value_id": "sensor_available_depth", "value": 1}, {"value_id": "sensor_available_heading", "value": 1}, {"value_id": "sensor_available_pitch", "value": 1}, {"value_id": "sensor_available_roll", "value": 1}, {"value_id": "sensor_available_conductivity", "value": 0}, {"value_id": "sensor_available_temperature", "value": 1}, {"value_id": "bit_result_demod_0", "value": 0}, {"value_id": "bit_result_demod_1", "value": 0}, {"value_id": "bit_result_timing", "value": 0}, {"value_id": "bus_error_exception", "value": 0}, {"value_id": "address_error_exception", "value": 0}, {"value_id": "illegal_instruction_exception", "value": 0}, {"value_id": "zero_divide_instruction", "value": 0}, {"value_id": "emulator_exception", "value": 0}, {"value_id": "unassigned_exception", "value": 0}, {"value_id": "watchdog_restart_occurred", "value": 0}, {"value_id": "battery_saver_power", "value": 0}, {"value_id": "pinging", "value": 0}, {"value_id": "cold_wakeup_occurred", "value": 0}, {"value_id": "unknown_wakeup_occurred", "value": 0}, {"value_id": "clock_read_error", "value": 0}, {"value_id": "unexpected_alarm", "value": 0}, {"value_id": "clock_jump_forward", "value": 0}, {"value_id": "clock_jump_backward", "value": 0}, {"value_id": "power_fail", "value": 0}, {"value_id": "spurious_dsp_interrupt", "value": 0}, {"value_id": "spurious_uart_interrupt", "value": 0}, {"value_id": "spurious_clock_interrupt", "value": 0}, {"value_id": "level_7_interrupt", "value": 0}, {"value_id": "time_per_ping_seconds", "value": 1.0}, {"value_id": "mpt_seconds", "value": 0.01}, {"value_id": "correlation_magnitude_id", "value": 512}, {"value_id": "correlation_magnitude_beam1", "value": [75, 76, 81, 89, 69, 16, 18, 17, 9, 12, 6, 6, 7, 5, 12, 11, 13, 14, 4, 13, 11, 15, 8, 1, 23, 5, 6, 13, 11, 16, 16, 18, 11, 7, 5, 5, 6, 13, 18, 3, 9, 11, 10, 7, 14, 8, 10, 10, 7, 13, 10, 12, 11, 5, 6, 19, 11, 9, 12, 20, 12, 6, 6, 7, 13, 5, 9, 7, 18, 8, 6, 10, 11, 13, 12, 8, 17, 15, 9, 18, 16, 17, 18, 18, 7, 6, 9, 12, 14, 26, 4, 17, 3, 16, 14, 11, 10, 9, 9, 15]}, {"value_id": "correlation_magnitude_beam2", "value": [77, 89, 89, 88, 53, 23, 5, 9, 20, 6, 16, 12, 10, 11, 4, 10, 9, 6, 13, 8, 14, 6, 15, 17, 9, 9, 12, 20, 5, 21, 14, 17, 14, 4, 6, 8, 7, 5, 9, 15, 8, 13, 15, 29, 11, 13, 13, 8, 9, 10, 12, 11, 11, 14, 7, 16, 5, 5, 6, 5, 15, 22, 9, 15, 9, 15, 25, 8, 7, 4, 8, 19, 17, 7, 11, 9, 11, 8, 8, 13, 6, 14, 21, 14, 11, 12, 5, 15, 13, 6, 11, 6, 14, 14, 9, 16, 9, 11, 13, 23]}, {"value_id": "correlation_magnitude_beam3", "value": [74, 79, 77, 68, 48, 23, 9, 14, 14, 9, 11, 17, 17, 13, 15, 9, 6, 20, 9, 14, 10, 19, 13, 9, 7, 4, 8, 8, 11, 17, 12, 8, 6, 13, 9, 11, 6, 15, 12, 16, 11, 13, 6, 6, 9, 13, 8, 18, 11, 16, 9, 7, 12, 22, 18, 13, 14, 9, 11, 13, 13, 7, 9, 13, 9, 8, 5, 5, 6, 6, 11, 21, 9, 12, 9, 14, 16, 7, 10, 10, 4, 12, 8, 10, 5, 21, 7, 5, 8, 14, 16, 11, 11, 10, 5, 8, 13, 8, 10, 11]}, {"value_id": "correlation_magnitude_beam4", "value": [89, 81, 40, 48, 18, 9, 19, 8, 9, 6, 25, 7, 14, 14, 15, 11, 8, 13, 10, 16, 11, 4, 14, 13, 13, 13, 7, 13, 4, 7, 13, 8, 1, 11, 7, 16, 11, 8, 11, 6, 21, 13, 27, 7, 20, 8, 9, 13, 15, 23, 13, 4, 19, 15, 17, 9, 5, 6, 10, 8, 5, 10, 7, 9, 12, 9, 11, 10, 11, 6, 12, 8, 15, 8, 21, 6, 19, 8, 8, 8, 3, 18, 12, 10, 6, 16, 17, 18, 9, 8, 9, 9, 5, 15, 14, 6, 9, 9, 5, 13]}, {"value_id": "echo_intensity_id", "value": 768}, {"value_id": "echo_intensity_beam1", "value": [172, 136, 97, 58, 35, 30, 32, 27, 30, 30, 26, 27, 30, 27, 27, 32, 28, 28, 30, 27, 28, 31, 28, 27, 32, 26, 28, 32, 27, 26, 30, 26, 27, 31, 28, 27, 29, 26, 28, 30, 27, 27, 30, 27, 28, 32, 27, 28, 28, 26, 27, 33, 27, 27, 36, 28, 26, 28, 27, 27, 29, 28, 27, 28, 27, 27, 28, 26, 27, 29, 29, 27, 28, 29, 29, 29, 30, 27, 30, 29, 27, 29, 29, 29, 28, 29, 27, 29, 29, 27, 28, 31, 27, 29, 31, 28, 29, 31, 28, 28]}, {"value_id": "echo_intensity_beam2", "value": [188, 150, 105, 64, 39, 32, 33, 30, 33, 32, 29, 30, 32, 30, 30, 32, 31, 30, 32, 30, 30, 32, 30, 29, 32, 29, 30, 33, 30, 30, 32, 28, 29, 32, 29, 29, 32, 29, 30, 32, 30, 30, 32, 29, 30, 35, 30, 30, 32, 28, 30, 33, 29, 29, 37, 30, 29, 30, 30, 29, 29, 30, 30, 30, 29, 30, 30, 29, 29, 33, 32, 31, 32, 31, 30, 33, 32, 31, 32, 31, 30, 32, 31, 32, 32, 32, 30, 32, 32, 31, 32, 33, 30, 31, 33, 30, 32, 33, 31, 32]}, {"value_id": "echo_intensity_beam3", "value": [180, 144, 103, 63, 40, 37, 38, 32, 38, 38, 32, 33, 37, 32, 32, 40, 33, 33, 37, 34, 34, 36, 35, 33, 37, 31, 35, 36, 33, 33, 36, 31, 33, 36, 32, 32, 36, 32, 35, 38, 31, 32, 35, 32, 34, 40, 32, 32, 34, 31, 32, 37, 33, 32, 41, 33, 31, 34, 32, 32, 34, 35, 33, 35, 32, 31, 34, 31, 32, 35, 35, 32, 35, 34, 33, 36, 35, 32, 35, 33, 32, 35, 35, 33, 33, 34, 33, 34, 33, 32, 34, 36, 33, 33, 36, 32, 35, 36, 32, 34]}, {"value_id": "echo_intensity_beam4", "value": [178, 140, 101, 63, 37, 32, 37, 28, 34, 34, 29, 32, 36, 30, 28, 36, 31, 30, 34, 31, 32, 35, 32, 28, 36, 29, 32, 36, 30, 30, 35, 28, 29, 34, 30, 29, 32, 29, 32, 34, 30, 29, 33, 30, 31, 38, 29, 31, 32, 28, 29, 35, 30, 29, 38, 30, 29, 32, 31, 28, 31, 32, 30, 32, 29, 30, 31, 27, 28, 32, 32, 30, 32, 32, 31, 34, 35, 29, 33, 33, 31, 33, 33, 32, 32, 34, 29, 31, 33, 30, 32, 36, 30, 31, 36, 30, 33, 36, 31, 32]}], "port_timestamp": 3646499450.1649103, "driver_timestamp": 3646499452.616436, "pkt_format_id": "JSON_Data", "pkt_version": 1}, "time": 1437510652.625027}'
    payload = json.loads(payload)
    headers = {'sensor': 'xxx-xxx-xxx', 'deliveryType': 'streamed'}
    publisher = Publisher.from_url('qpid://guest/guest@192.168.33.10:5672?queue=Ingest.instrument_particles', headers)
    for _ in xrange(1000):
        publisher.publish([payload] * 10)
        time.sleep(.001)


def test_events():
    payload = {'type': 'test_event', 'value': 'test_value', 'time': 1437510652.625027}
    headers = {'sensor': 'xxx-xxx-xxx', 'deliveryType': 'streamed'}
    publisher = Publisher.from_url('qpid://guest/guest@192.168.33.10:5672?queue=Ingest.instrument_events', headers)
    for _ in xrange(1000):
        publisher.publish(payload)
        time.sleep(.001)

def test_bad_particle():
    payload = {'type': 'DRIVER_ASYNC_EVENT_SAMPLE', 'value': 'BAD', 'time': 1437510652.625027}
    headers = {'sensor': 'xxx-xxx-xxx', 'deliveryType': 'streamed'}
    publisher = Publisher.from_url('qpid://guest/guest@192.168.33.10:5672?queue=Ingest.instrument_particles', headers)
    for _ in xrange(10):
        publisher.publish(payload)
        time.sleep(.001)

if __name__ == '__main__':
    publisher = Publisher.from_url('qpid://guest/guest@uf2.local:5672?queue=test')
    publisher.headers = {'test': 'header'}
    now = time.time()
    count = 10
    for _ in xrange(count):
        publisher.publish({'hello': 'world'})

    elapsed = time.time() - now
    print 'sent %d messages in %.3f secs (%.2f/s)' % (count, elapsed, count/elapsed)