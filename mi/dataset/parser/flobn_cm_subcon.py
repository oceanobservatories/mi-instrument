#!/usr/bin/env python

"""
@package mi.dataset.parser.flobn_a_subcon
@file mi-dataset/mi/dataset/parser/flobn_a_subcon.py
@author Rachel Manoni
@brief Parser for the flobn_a_subcon dataset driver

Release notes:

Initial Release
"""
__author__ = 'Rachel Manoni'
__license__ = 'Apache 2.0'

import calendar
import csv
import time
import math
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.dataset_parser import SimpleParser
from mi.core.log import get_logger
log = get_logger()


FLOBN_C_COIL_DATA_MAP = [
    ('tracking_id', 0, str),
    ('sample_time', 1, str),
    ('boron_concentration', 2, float),
    ('barium_concentration_um', 3, float),
    ('calcium_concentration', 4, float),
    ('potassium_concentration', 5, float),
    ('lithium_concentration', 6, float),
    ('magnesium_concentration', 7, float),
    ('sodium_concentration', 8, float),
    ('sulfur_concentration', 9, float),
    ('strontium_concentration', 10, float),
    ('tracer_percent', 11, float),
    ('flow_rate', 12, float)
]

FLOBN_M_POSITION_XY_DATA_MAP = [
    ('sample_time', 0, str),
    ('tracer_concentration', 1, float)
]

FLOBN_M_POSITION_Z_DATA_MAP = [
    ('sample_time', 0, str),
    ('tracer_concentration', 1, float),
    ('calcium_concentration', 2, float),
    ('magnesium_concentration', 3, float),
    ('sodium_concentration', 4, float),
    ('potassium_concentration', 5, float),
    ('sulfate_concentration', 6, float),
    ('chloride_concentration', 7, float)
]

FLOBN_M_FLOW_RATE_DATA_MAP = [
    ('sample_time', 0, str),
    ('flow_rate', 1, float)
]

FLOBN_M_TEMP_DATA_MAP = [
    ('sample_time', 1, str),
    ('ambient_temperature', 2, float)
]


class FlobnCMDataParticle(DataParticle):
    instrument_particle_map = []
    time_struct = "%Y/%m/%d %H"
    start_time_index = 1

    def __init__(self, raw_data, *args, **kwargs):

        super(FlobnCMDataParticle, self).__init__(raw_data, *args, **kwargs)

        # set timestamp
        struct_time = time.strptime(self.raw_data[self.start_time_index], self.time_struct)
        elapsed_seconds = calendar.timegm(struct_time)
        self.set_internal_timestamp(unix_time=elapsed_seconds)

    def _build_parsed_values(self):
        """
        Build parsed values for the Instrument Data Particle.
        """
        return [self._encode_value(name, self.raw_data[group], function)
                for name, group, function in self.instrument_particle_map]

    def _encode_value(self, name, value, encoding_function):
        """
        Encode a value using the encoding function
        If there is an error, return None as a value for that parameter
        """
        try:
            if encoding_function is float and math.isnan(float(value)):
                encoded_val = None
            else:
                encoded_val = encoding_function(value)
        except:
            log.warn("Data particle error encoding. Name:%s Value:%s", name, value)
            encoded_val = None
        finally:
            return {DataParticleKey.VALUE_ID: name,
                    DataParticleKey.VALUE: encoded_val}


class FlobnCUpperCoilDataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_c_upper_coil_recovered'
    instrument_particle_map = FLOBN_C_COIL_DATA_MAP


class FlobnCLowerCoilDataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_c_lower_coil_recovered'
    instrument_particle_map = FLOBN_C_COIL_DATA_MAP


class FlobnMPositionX1DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_x1_recovered'
    instrument_particle_map = FLOBN_M_POSITION_XY_DATA_MAP
    start_time_index = 0


class FlobnMPositionX2DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_x2_recovered'
    instrument_particle_map = FLOBN_M_POSITION_XY_DATA_MAP
    start_time_index = 0


class FlobnMPositionY1DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_y1_recovered'
    instrument_particle_map = FLOBN_M_POSITION_XY_DATA_MAP
    start_time_index = 0


class FlobnMPositionY2DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_y2_recovered'
    instrument_particle_map = FLOBN_M_POSITION_XY_DATA_MAP
    start_time_index = 0


class FlobnMPositionZ1DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_z1_recovered'
    instrument_particle_map = FLOBN_M_POSITION_Z_DATA_MAP
    start_time_index = 0

class FlobnMPositionZ2DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_z2_recovered'
    instrument_particle_map = FLOBN_M_POSITION_Z_DATA_MAP
    start_time_index = 0


class FlobnMPositionZ3DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_z3_recovered'
    instrument_particle_map = FLOBN_M_POSITION_Z_DATA_MAP
    start_time_index = 0


class FlobnMPositionZ4DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_z4_recovered'
    instrument_particle_map = FLOBN_M_POSITION_Z_DATA_MAP
    start_time_index = 0


class FlobnMPositionZ5DataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_position_z5_recovered'
    instrument_particle_map = FLOBN_M_POSITION_Z_DATA_MAP
    start_time_index = 0


class FlobnMDirectionXFlowRateDataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_direction_x_flow_rate_recovered'
    instrument_particle_map = FLOBN_M_FLOW_RATE_DATA_MAP
    start_time_index = 0


class FlobnMDirectionYFlowRateDataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_direction_y_flow_rate_recovered'
    instrument_particle_map = FLOBN_M_FLOW_RATE_DATA_MAP
    start_time_index = 0


class FlobnMDirectionZFlowRateDataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_direction_z_flow_rate_recovered'
    instrument_particle_map = FLOBN_M_FLOW_RATE_DATA_MAP
    start_time_index = 0


class FlobnMAmbientTemperatureDataParticle(FlobnCMDataParticle):
    _data_particle_type = 'flobn_m_ambient_temperature_recovered'
    instrument_particle_map = FLOBN_M_TEMP_DATA_MAP
    time_struct = "%Y/%m/%d %H:%M"


class FlobnMSubconTemperatureParser(SimpleParser):

    def parse_file(self):
        """
        Parse a .csv input file. Build a data particle from the parsed data.
        """
        reader = csv.reader(self._stream_handle)
        for row in reader:
            log.debug("Read in line: %r", row)
            try:
                # Extract a particle and append it to the record buffer
                particle = self._extract_sample(FlobnMAmbientTemperatureDataParticle, None, row, None)
                self._record_buffer.append(particle)
            except:
                log.warn("Data cannot be parsed: %r", row)


class FlobnMSubconParser(SimpleParser):

    def parse_file(self):
        """
        Parse a .csv input file. Build a data particle from the parsed data.
        """
        reader = csv.reader(self._stream_handle)
        for row in reader:
            log.debug("Read in line: %r", row)

            try:
                #chop data into chunks for parsing
                log.debug("Part of line1: %r", row[1:3])
                particle = self._extract_sample(FlobnMPositionX1DataParticle, None, row[1:3], None)
                self._record_buffer.append(particle)
                log.debug("Part of line2: %r", row[3:5])
                particle = self._extract_sample(FlobnMPositionX2DataParticle, None, row[3:5], None)
                self._record_buffer.append(particle)

                log.debug("Part of line3: %r", row[5:7])
                particle = self._extract_sample(FlobnMPositionY1DataParticle, None, row[5:7], None)
                self._record_buffer.append(particle)
                log.debug("Part of line4: %r", row[7:9])
                particle = self._extract_sample(FlobnMPositionY2DataParticle, None, row[7:9], None)
                self._record_buffer.append(particle)

                log.debug("Part of line5: %r", row[9:17])
                particle = self._extract_sample(FlobnMPositionZ1DataParticle, None, row[9:17], None)
                self._record_buffer.append(particle)
                log.debug("Part of line6: %r", row[17:25])
                particle = self._extract_sample(FlobnMPositionZ2DataParticle, None, row[17:25], None)
                self._record_buffer.append(particle)
                log.debug("Part of line7: %r", row[25:33])
                particle = self._extract_sample(FlobnMPositionZ3DataParticle, None, row[25:33], None)
                self._record_buffer.append(particle)
                log.debug("Part of line8: %r", row[33:41])
                particle = self._extract_sample(FlobnMPositionZ4DataParticle, None, row[33:41], None)
                self._record_buffer.append(particle)
                log.debug("Part of line9: %r", row[41:49])
                particle = self._extract_sample(FlobnMPositionZ5DataParticle, None, row[41:49], None)
                self._record_buffer.append(particle)

                log.debug("Part of line10: %r", row[49:51])
                particle = self._extract_sample(FlobnMDirectionXFlowRateDataParticle, None, row[49:51], None)
                self._record_buffer.append(particle)
                log.debug("Part of line11: %r", row[51:53])
                particle = self._extract_sample(FlobnMDirectionYFlowRateDataParticle, None, row[51:53], None)
                self._record_buffer.append(particle)
                log.debug("Part of line12: %r", row[53:])
                particle = self._extract_sample(FlobnMDirectionZFlowRateDataParticle, None, row[53:], None)
                self._record_buffer.append(particle)

            except:
                log.warn("Data cannot be parsed: %r", row)


class FlobnCSubconParser(SimpleParser):

    def parse_file(self):
        """
        Parse a .csv input file. Build a data particle from the parsed data.
        """
        reader = csv.reader(self._stream_handle)
        for row in reader:
            log.debug("Read in line: %r", row)

            #chop in half send to each particle
            log.debug("First half of line: %r", row[1:14])
            log.debug("Second half of line: %r", row[14:])

            try:
                # Extract a particle and append it to the record buffer
                particle = self._extract_sample(FlobnCUpperCoilDataParticle, None, row[1:14], None)
                self._record_buffer.append(particle)
                particle = self._extract_sample(FlobnCLowerCoilDataParticle, None, row[14:], None)
                self._record_buffer.append(particle)

            except:
                log.warn("Data cannot be parsed: %r", row)