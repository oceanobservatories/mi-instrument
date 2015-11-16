#!/usr/bin/env python
"""
@package mi.instrument.teledyne.pd0_parser
@file marine-integrations/mi/instrument/teledyne/pd0_parser.py
@author Peter Cable
@brief Parser for ADCP PD0 data
Release notes:
"""
from collections import namedtuple
import pprint
import struct

import sys

namedtuple_store = {}
bitmapped_namedtuple_store = {}


class PD0ParsingException(Exception):
    pass


class InsufficientDataException(PD0ParsingException):
    pass


class UnhandledBlockException(PD0ParsingException):
    pass


class ChecksumException(PD0ParsingException):
    pass


class BlockId(object):
    FIXED_DATA = 0
    VARIABLE_DATA = 128
    VELOCITY_DATA = 256
    CORRELATION_DATA = 512
    ECHO_INTENSITY_DATA = 768
    PERCENT_GOOD_DATA = 1024
    STATUS_DATA_ID = 1280
    BOTTOM_TRACK = 1536
    AUV_NAV_DATA = 8192


def count_zero_bits(bitmask):
    if not bitmask:
        return 0
    zero_digits = 0
    submask = 1
    while True:
        x = bitmask & submask
        submask <<= 1
        if x != 0:
            break
        zero_digits += 1
    return zero_digits


class AdcpPd0Record(object):
    def __init__(self, data, glider=False):
        self.data = data
        self.header = None
        self.offsets = None
        self.fixed_data = None
        self.variable_data = None
        self.echo_intensity = None
        self.velocities = None
        self.correlation_magnitudes = None
        self.percent_good = None
        self.sysconfig = None
        self.sensor_source = None
        self.sensor_avail = None
        self.bit_result = None
        self.error_word = None
        self.stored_checksum = None
        self._process(glider)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def _unpack_from_format(self, name, formatter, offset):
        format_string = ''.join([item[1] for item in formatter])
        fields = [item[0] for item in formatter]
        data = struct.unpack_from('<' + format_string, self.data, offset)
        if name not in namedtuple_store:
            namedtuple_store[name] = namedtuple(name, fields)
        _class = namedtuple_store[name]
        return _class(*data)

    def _unpack_cell_data(self, name, format_string, offset):
        _class = namedtuple(name, ('id', 'beam1', 'beam2', 'beam3', 'beam4'))
        data = struct.unpack_from('<H%d%s' % (self.fixed_data.number_of_cells * 4, format_string), self.data, offset)
        _object = _class(data[0], [], [], [], [])
        _object.beam1[:] = data[1::4]
        _object.beam2[:] = data[2::4]
        _object.beam3[:] = data[3::4]
        _object.beam4[:] = data[4::4]
        return _object

    @staticmethod
    def _unpack_bitmapped(name, formatter, source_data):
        # short circuit if we've seen this bitmap before
        short_circuit_key = (name, source_data)
        if short_circuit_key in bitmapped_namedtuple_store:
            return bitmapped_namedtuple_store[short_circuit_key]

        # create the namedtuple class if it doesn't already exist
        fields = [item[0] for item in formatter]
        if name not in namedtuple_store:
            namedtuple_store[name] = namedtuple(name, fields)
        _class = namedtuple_store[name]

        # create an instance of the namedtuple for this data
        data = []
        for _, bitmask, lookup_table in formatter:
            raw = (source_data & bitmask) >> count_zero_bits(bitmask)
            if lookup_table is not None:
                data.append(lookup_table[raw])
            else:
                data.append(raw)
        value = _class(*data)

        # store this value for future short circuit operations
        bitmapped_namedtuple_store[short_circuit_key] = value
        return value

    def _validate(self):
        self._process_header()
        self._validate_checksum()

    def _validate_checksum(self):
        if len(self.data) < self.header.num_bytes + 2:
            raise InsufficientDataException(
                'Insufficient data in PD0 record (expected %d bytes, found %d)' %
                (self.header.num_bytes + 2, len(self.data)))

        calculated_checksum = sum(bytearray(self.data[:-2])) & 65535
        self.stored_checksum = struct.unpack_from('<H', self.data, self.header.num_bytes)[0]

        if calculated_checksum != self.stored_checksum:
            raise ChecksumException('Checksum failure in PD0 data (expected %d, calculated %d' %
                                      (self.stored_checksum, calculated_checksum))

    def _process(self, glider):
        self._validate()
        self._parse_offset_data()
        self._parse_sysconfig()
        self._parse_coord_transform()
        self._parse_sensor_source(glider)
        self._parse_sensor_avail(glider)
        self._parse_bit_result()
        self._parse_error_word()

    def _process_header(self):
        header_format = (
            ('id', 'B'),
            ('data_source', 'B'),
            ('num_bytes', 'H'),
            ('spare', 'B'),
            ('num_data_types', 'B')
        )
        self.header = self._unpack_from_format('header', header_format, 0)
        self.data = self.data[:self.header.num_bytes + 2]

    def _parse_offset_data(self):
        self.offsets = struct.unpack_from('<%dH' % self.header.num_data_types, self.data, 6)
        for offset in self.offsets:
            block_id = struct.unpack_from('<H', self.data, offset)[0]
            if block_id == BlockId.FIXED_DATA:
                self._parse_fixed(offset)
            elif block_id == BlockId.VARIABLE_DATA:
                self._parse_variable(offset)
            elif block_id == BlockId.VELOCITY_DATA:
                self._parse_velocity(offset)
            elif block_id == BlockId.CORRELATION_DATA:
                self._parse_correlation(offset)
            elif block_id == BlockId.ECHO_INTENSITY_DATA:
                self._parse_echo(offset)
            elif block_id == BlockId.PERCENT_GOOD_DATA:
                self._parse_percent_good(offset)
            elif block_id == BlockId.BOTTOM_TRACK:
                self._parse_bottom_track(offset)
            elif block_id == BlockId.AUV_NAV_DATA:
                pass
            elif block_id == BlockId.STATUS_DATA_ID:
                pass
            else:
                print >> sys.stderr, block_id
                raise UnhandledBlockException('Found unhandled data type id: %d' % block_id)

    def _parse_fixed(self, offset):
        fixed_format = (
            ('id', 'H'),
            ('cpu_firmware_version', 'B'),
            ('cpu_firmware_revision', 'B'),
            ('system_configuration', 'H'),
            ('simulation_data_flag', 'B'),
            ('lag_length', 'B'),
            ('number_of_beams', 'B'),
            ('number_of_cells', 'B'),
            ('pings_per_ensemble', 'H'),
            ('depth_cell_length', 'H'),
            ('blank_after_transmit', 'H'),
            ('signal_processing_mode', 'B'),
            ('low_corr_threshold', 'B'),
            ('num_code_reps', 'B'),
            ('minimum_percentage', 'B'),
            ('error_velocity_max', 'H'),
            ('tpp_minutes', 'B'),
            ('tpp_seconds', 'B'),
            ('tpp_hundredths', 'B'),
            ('coord_transform', 'B'),
            ('heading_alignment', 'H'),
            ('heading_bias', 'H'),
            ('sensor_source', 'B'),
            ('sensor_available', 'B'),
            ('bin_1_distance', 'H'),
            ('transmit_pulse_length', 'H'),
            ('starting_depth_cell', 'B'),
            ('ending_depth_cell', 'B'),
            ('false_target_threshold', 'B'),
            ('spare1', 'B'),
            ('transmit_lag_distance', 'H'),
            ('cpu_board_serial_number', 'Q'),
            ('system_bandwidth', 'H'),
            ('system_power', 'B'),
            ('spare2', 'B'),
            ('serial_number', 'I'),
            ('beam_angle', 'B')
        )
        self.fixed_data = self._unpack_from_format('fixed', fixed_format, offset)

    def _parse_variable(self, offset):
        variable_format = (
            ('id', 'H'),
            ('ensemble_number', 'H'),
            ('rtc_year', 'B'),
            ('rtc_month', 'B'),
            ('rtc_day', 'B'),
            ('rtc_hour', 'B'),
            ('rtc_minute', 'B'),
            ('rtc_second', 'B'),
            ('rtc_hundredths', 'B'),
            ('ensemble_roll_over', 'B'),
            ('bit_result', 'H'),
            ('speed_of_sound', 'H'),
            ('depth_of_transducer', 'H'),
            ('heading', 'H'),
            ('pitch', 'h'),
            ('roll', 'h'),
            ('salinity', 'H'),
            ('temperature', 'h'),
            ('mpt_minutes', 'B'),
            ('mpt_seconds', 'B'),
            ('mpt_hundredths', 'B'),
            ('heading_standard_deviation', 'B'),
            ('pitch_standard_deviation', 'B'),
            ('roll_standard_deviation', 'B'),
            ('transmit_current', 'B'),
            ('transmit_voltage', 'B'),
            ('ambient_temperature', 'B'),
            ('pressure_positive', 'B'),
            ('pressure_negative', 'B'),
            ('attitude_temperature', 'B'),
            ('attitude', 'B'),
            ('contamination_sensor', 'B'),
            ('error_status_word', 'I'),
            ('reserved', 'H'),
            ('pressure', 'I'),
            ('pressure_variance', 'I'),
            ('spare', 'B'),
            ('rtc_y2k_century', 'B'),
            ('rtc_y2k_year', 'B'),
            ('rtc_y2k_month', 'B'),
            ('rtc_y2k_day', 'B'),
            ('rtc_y2k_hour', 'B'),
            ('rtc_y2k_minute', 'B'),
            ('rtc_y2k_seconds', 'B'),
            ('rtc_y2k_hundredths', 'B')
        )
        self.variable_data = self._unpack_from_format('variable', variable_format, offset)

    def _parse_velocity(self, offset):
        self.velocities = self._unpack_cell_data('velocity', 'h', offset)

    def _parse_correlation(self, offset):
        self.correlation_magnitudes = self._unpack_cell_data('correlation', 'B', offset)

    def _parse_echo(self, offset):
        self.echo_intensity = self._unpack_cell_data('echo_intensity', 'B', offset)

    def _parse_percent_good(self, offset):
        self.percent_good = self._unpack_cell_data('percent_good', 'B', offset)

    def _parse_bottom_track(self, offset):
        bottom_track_format = (
            ('id', 'H'),
            ('pings_per_ensemble', 'H'),
            ('delay_before_reacquire', 'H'),
            ('correlation_mag_min', 'B'),
            ('eval_amplitude_min', 'B'),
            ('percent_good_minimum', 'B'),
            ('mode', 'B'),
            ('error_velocity_max', 'H'),
            ('reserved', 'I'),
            ('range_1', 'H'),
            ('range_2', 'H'),
            ('range_3', 'H'),
            ('range_4', 'H'),
            ('velocity_1', 'h'),
            ('velocity_2', 'h'),
            ('velocity_3', 'h'),
            ('velocity_4', 'h'),
            ('corr_1', 'B'),
            ('corr_2', 'B'),
            ('corr_3', 'B'),
            ('corr_4', 'B'),
            ('amp_1', 'B'),
            ('amp_2', 'B'),
            ('amp_3', 'B'),
            ('amp_4', 'B'),
            ('pcnt_1', 'B'),
            ('pcnt_2', 'B'),
            ('pcnt_3', 'B'),
            ('pcnt_4', 'B'),
            ('ref_layer_min', 'H'),
            ('ref_layer_near', 'H'),
            ('ref_layer_far', 'H'),
            ('ref_velocity_1', 'h'),
            ('ref_velocity_2', 'h'),
            ('ref_velocity_3', 'h'),
            ('ref_velocity_4', 'h'),
            ('ref_corr_1', 'B'),
            ('ref_corr_2', 'B'),
            ('ref_corr_3', 'B'),
            ('ref_corr_4', 'B'),
            ('ref_amp_1', 'B'),
            ('ref_amp_2', 'B'),
            ('ref_amp_3', 'B'),
            ('ref_amp_4', 'B'),
            ('ref_pcnt_1', 'B'),
            ('ref_pcnt_2', 'B'),
            ('ref_pcnt_3', 'B'),
            ('ref_pcnt_4', 'B'),
            ('max_depth', 'H'),
            ('rssi_1', 'B'),
            ('rssi_2', 'B'),
            ('rssi_3', 'B'),
            ('rssi_4', 'B'),
            ('gain', 'B'),
            ('range_msb_1', 'B'),
            ('range_msb_2', 'B'),
            ('range_msb_3', 'B'),
            ('range_msb_4', 'B'),
        )
        self.bottom_track = self._unpack_from_format('bottom_track', bottom_track_format, offset)

    def _parse_sysconfig(self):
        """
        LSB
        BITS 7 6 5 4 3 2 1 0
         - - - - - 0 0 0 75-kHz SYSTEM
         - - - - - 0 0 1 150-kHz SYSTEM
         - - - - - 0 1 0 300-kHz SYSTEM
         - - - - - 0 1 1 600-kHz SYSTEM
         - - - - - 1 0 0 1200-kHz SYSTEM
         - - - - - 1 0 1 2400-kHz SYSTEM
         - - - - 0 - - - CONCAVE BEAM PAT.
         - - - - 1 - - - CONVEX BEAM PAT.
         - - 0 0 - - - - SENSOR CONFIG #1
         - - 0 1 - - - - SENSOR CONFIG #2
         - - 1 0 - - - - SENSOR CONFIG #3
         - 0 - - - - - - XDCR HD NOT ATT.
         - 1 - - - - - - XDCR HD ATTACHED
         0 - - - - - - - DOWN FACING BEAM
         1 - - - - - - - UP-FACING BEAM
        MSB
        BITS 7 6 5 4 3 2 1 0
         - - - - - - 0 0 15E BEAM ANGLE
         - - - - - - 0 1 20E BEAM ANGLE
         - - - - - - 1 0 30E BEAM ANGLE
         - - - - - - 1 1 OTHER BEAM ANGLE
         0 1 0 0 - - - - 4-BEAM JANUS CONFIG
         0 1 0 1 - - - - 5-BM JANUS CFIG DEMOD)
         1 1 1 1 - - - - 5-BM JANUS CFIG.(2 DEMD)
        """
        frequencies = [75, 150, 300, 600, 1200, 2400]
        sysconfig_format = (
            ('frequency', 0b111, frequencies),
            ('beam_pattern', 0b1000, None),
            ('sensor_config', 0b110000, None),
            ('xdcr_head_attached', 0b1000000, None),
            ('beam_facing', 0b10000000, None),
            ('beam_angle', 0b11 << 8, None),
            ('janus_config', 0b11110000 << 8, None))

        self.sysconfig = self._unpack_bitmapped('sysconfig', sysconfig_format, self.fixed_data.system_configuration)

    def _parse_coord_transform(self):
        """
         xxx00xxx = NO TRANSFORMATION (BEAM COORDINATES)
         xxx01xxx = INSTRUMENT COORDINATES
         xxx10xxx = SHIP COORDINATES
         xxx11xxx = EARTH COORDINATES
         xxxxx1xx = TILTS (PITCH AND ROLL) USED IN SHIP OR EARTH TRANSFORMATION
         xxxxxx1x = 3-BEAM SOLUTION USED IF ONE BEAM IS BELOW THE CORRELATION THRESHOLD SET BY THE WC-COMMAND
         xxxxxxx1 = BIN MAPPING USED
        """
        coord_transform_format = (
            ('coord_transform', 0b11000, None),
            ('tilts_used', 0b100, None),
            ('three_beam_used', 0b10, None),
            ('bin_mapping_used', 0b1, None))

        self.coord_transform = self._unpack_bitmapped('coord_transform', coord_transform_format,
                                                      self.fixed_data.coord_transform)

    def _parse_sensor_source(self, glider):
        """
        FIELD DESCRIPTION
         x1xxxxxx = CALCULATES EC (SPEED OF SOUND) FROM ED, ES, AND ET
         xx1xxxxx = USES ED FROM DEPTH SENSOR
         xxx1xxxx = USES EH FROM TRANSDUCER HEADING SENSOR
         xxxx1xxx = USES EP FROM TRANSDUCER PITCH SENSOR
         xxxxx1xx = USES ER FROM TRANSDUCER ROLL SENSOR
         xxxxxx1x = USES ES (SALINITY) FROM CONDUCTIVITY SENSOR
         xxxxxxx1 = USES ET FROM TRANSDUCER TEMPERATURE SENSOR

         FIELD DESCRIPTION (ExplorerDVL)
         1xxxxxxx = CALCULATES EC (SPEED OF SOUND) FROM ED, ES, AND ET
         x1xxxxxx = USES ED FROM DEPTH SENSOR
         xx1xxxxx = USES EH FROM TRANSDUCER HEADING SENSOR
         xxx1xxxx = USES EP FROM TRANSDUCER PITCH SENSOR
         xxxx1xxx = USES ER FROM TRANSDUCER ROLL SENSOR
         xxxxx1xx = USES ES (SALINITY) FROM CONDUCTIVITY SENSOR
         xxxxxx1x = USES ET FROM TRANSDUCER TEMPERATURE SENSOR
         xxxxxxx1 = USES EU FROM TRANSDUCER TEMPERATURE SENSOR
        """
        if glider:
            sensor_source_format = (
                ('calculate_ec', 0b10000000, None),
                ('depth_used', 0b1000000, None),
                ('heading_used', 0b100000, None),
                ('pitch_used', 0b10000, None),
                ('roll_used', 0b1000, None),
                ('conductivity_used', 0b100, None),
                ('temperature_used', 0b10, None),
                ('temperature_eu_used', 0b1, None))

            self.sensor_source = self._unpack_bitmapped('sensor_source_glider', sensor_source_format,
                                                        self.fixed_data.sensor_source)

        else:
            sensor_source_format = (
                ('calculate_ec', 0b1000000, None),
                ('depth_used', 0b100000, None),
                ('heading_used', 0b10000, None),
                ('pitch_used', 0b1000, None),
                ('roll_used', 0b100, None),
                ('conductivity_used', 0b10, None),
                ('temperature_used', 0b1, None))

            self.sensor_source = self._unpack_bitmapped('sensor_source', sensor_source_format,
                                                        self.fixed_data.sensor_source)

    def _parse_sensor_avail(self, glider):
        """
        Fields match sensor source above
        """
        if glider:
            sensor_avail_format = (
                ('speed_avail', 0b10000000, None),
                ('depth_avail', 0b1000000, None),
                ('heading_avail', 0b100000, None),
                ('pitch_avail', 0b10000, None),
                ('roll_avail', 0b1000, None),
                ('conductivity_avail', 0b100, None),
                ('temperature_avail', 0b10, None),
                ('temperature_eu_avail', 0b1, None))

            self.sensor_avail = self._unpack_bitmapped('sensor_avail_glider', sensor_avail_format,
                                                       self.fixed_data.sensor_available)

        else:
            sensor_avail_format = (
                ('speed_avail', 0b1000000, None),
                ('depth_avail', 0b100000, None),
                ('heading_avail', 0b10000, None),
                ('pitch_avail', 0b1000, None),
                ('roll_avail', 0b100, None),
                ('conductivity_avail', 0b10, None),
                ('temperature_avail', 0b1, None))

            self.sensor_avail = self._unpack_bitmapped('sensor_avail', sensor_avail_format,
                                                       self.fixed_data.sensor_available)

    def _parse_bit_result(self):
        """
        BYTE 13 BYTE 14 (BYTE 14 RESERVED FOR FUTURE USE)
        1xxxxxxx xxxxxxxx = RESERVED
        x1xxxxxx xxxxxxxx = RESERVED
        xx1xxxxx xxxxxxxx = RESERVED
        xxx1xxxx xxxxxxxx = DEMOD 1 ERROR
        xxxx1xxx xxxxxxxx = DEMOD 0 ERROR
        xxxxx1xx xxxxxxxx = RESERVED
        xxxxxx1x xxxxxxxx = TIMING CARD ERROR
        xxxxxxx1 xxxxxxxx = RESERVED
        """
        bit_result_format = (
            ('demod1_error', 0b10000, None),
            ('demod0_error', 0b1000, None),
            ('timing_card_error', 0b10, None))

        self.bit_result = self._unpack_bitmapped('bit_result', bit_result_format, self.variable_data.bit_result)

    def _parse_error_word(self):
        """
        Low 16 BITS
        LSB
        BITS 07 06 05 04 03 02 01 00
         x x x x x x x 1 Bus Error exception
         x x x x x x 1 x Address Error exception
         x x x x x 1 x x Illegal Instruction exception
         x x x x 1 x x x Zero Divide exception
         x x x 1 x x x x Emulator exception
         x x 1 x x x x x Unassigned exception
         x 1 x x x x x x Watchdog restart occurred
         1 x x x x x x x Battery Saver power
        87-88 44 Low 16 BITS
        MSB
        BITS 15 14 13 12 11 10 09 08
         x x x x x x x 1 Pinging
         x x x x x x 1 x Not Used
         x x x x x 1 x x Not Used
         x x x x 1 x x x Not Used
         x x x 1 x x x x Not Used
         x x 1 x x x x x Not Used
         x 1 x x x x x x Cold Wakeup occurred
         1 x x x x x x x Unknown Wakeup occurred
        89-90 45 High 16 BITS
        LSB
        BITS 24 23 22 21 20 19 18 17
         x x x x x x x 1 Clock Read error occurred
         x x x x x x 1 x Unexpected alarm
         x x x x x 1 x x Clock jump forward
         x x x x 1 x x x Clock jump backward
         x x x 1 x x x x Not Used
         x x 1 x x x x x Not Used
         x 1 x x x x x x Not Used
         1 x x x x x x x Not Used
                High 16 BITS
        MSB
        BITS 32 31 30 29 28 27 26 25
         x x x x x x x 1 Not Used
         x x x x x x 1 x Not Used
         x x x x x 1 x x Not Used
         x x x x 1 x x x Power Fail (Unrecorded)
         x x x 1 x x x x Spurious level 4 intr (DSP)
         x x 1 x x x x x Spurious level 5 intr (UART)
         x 1 x x x x x x Spurious level 6 intr (CLOCK)
         1 x x x x x x x Level 7 interrupt occurred
        """
        error_word_format = (
            ('bus_error', 0b1, None),
            ('address_error', 0b10, None),
            ('illegal_instruction', 0b100, None),
            ('zero_divide', 0b1000, None),
            ('emulator', 0b10000, None),
            ('unassigned', 0b100000, None),
            ('watchdog_restart', 0b1000000, None),
            ('battery_saver', 0b10000000, None),
            ('pinging', 0b1 << 8, None),
            ('cold_wakeup', 0b1000000 << 8, None),
            ('unknown_wakeup', 0b10000000 << 8, None),
            ('clock_read', 0b1 << 16, None),
            ('unexpected_alarm', 0b10 << 16, None),
            ('clock_jump_forward', 0b100 << 16, None),
            ('clock_jump_backward', 0b1000 << 16, None),
            ('power_fail', 0b1000 << 24, None),
            ('spurious_dsp', 0b10000 << 24, None),
            ('spurious_uart', 0b100000 << 24, None),
            ('spurious_clock', 0b1000000 << 24, None),
            ('level_7_interrupt', 0b10000000 << 24, None),
        )

        self.error_word = self._unpack_bitmapped('error_word', error_word_format, self.variable_data.error_status_word)
