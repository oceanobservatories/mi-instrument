#!/usr/bin/env python

"""
@package mi.dataset.parser.rasfl_a_subcon
@file marine-integrations/mi/dataset/parser/rasfl_a_subcon_recovered_driver.py
@author Rachel Manoni
@brief Parser for the rasfl_a_subcon dataset driver

Release notes:

Initial Release
"""

__author__ = 'Rachel Manoni'
__license__ = 'Apache 2.0'

import calendar
import time
import csv
import math

from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.dataset_parser import SimpleParser
from mi.core.log import get_logger
log = get_logger()

RASFL_DATA_MAP = [
    ('serial_number', 1, str),
    ('start_time', 2, str),
    ('stop_time', 3, str),
    ('port_number', 4, int),
    ('lat', 5, float),
    ('lon', 6, float),
    ('m_depth', 7, int),
    ('temp', 8, float),
    ('acidity', 9, float),
    ('alkalinity', 10, float),
    ('hydrogen_sulfide_concentration', 11, float),
    ('silicon_concentration', 12, float),
    ('ammonia_concentration', 13, float),
    ('chloride_concentration', 14, float),
    ('sulfate_concentration', 15, float),
    ('sodium_concentration', 16, float),
    ('potassium_concentration', 17, float),
    ('magnesium_concentration', 18, float),
    ('calcium_concentration', 19, float),
    ('bromide_concentration', 20, float),
    ('iron_concentration', 21, float),
    ('manganese_concentration', 22, float),
    ('lithium_concentration', 23, float),
    ('strontium_icpaes_concentration', 24, float),
    ('boron_concentration', 25, float),
    ('rubidium_concentration', 26, float),
    ('cesium_concentration', 27, float),
    ('strontium_icpms_concentration', 28, float),
    ('barium_concentration', 29, float),
    ('cobalt_concentration', 30, float),
    ('nickel_concentration', 31, float),
    ('copper_concentration', 32, float),
    ('zinc_concentration', 33, float),
    ('molybdenum_concentration', 34, float),
    ('silver_concentration', 35, float),
    ('cadmium_concentration', 36, float),
    ('titanium_concentration', 37, float),
    ('aluminum_concentration', 38, float),
    ('lead_concentration', 39, float),
    ('vanadium_concentration', 40, float),
    ('uranium_concentration', 41, float),
    ('yttrium_concentration', 42, float),
    ('gadolinium_concentration', 43, float)
]

START_TIME_IDX = 2


class RasflASubconInstrumentDataParticle(DataParticle):

    _data_particle_type = 'rasfl_a_subcon_instrument_recovered'
    instrument_particle_map = RASFL_DATA_MAP

    def __init__(self, raw_data, *args, **kwargs):

        super(RasflASubconInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # set timestamp mm/dd/yy hh:mm
        struct_time = time.strptime(self.raw_data[START_TIME_IDX], "%m/%d/%y %H:%M")
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
        Encode a value using the encoding function,
        If there is an error, return None as a value for that parameter
        """
        try:
            if encoding_function is float and math.isnan(float(value)):
                encoded_val = None
            else:
                encoded_val = encoding_function(value)
        except Exception as e:
            log.warn("Data particle error encoding. Name:%s Value:%s", name, value)
            encoded_val = None
        finally:
            return {DataParticleKey.VALUE_ID: name,
                    DataParticleKey.VALUE: encoded_val}


class RasflASubconParser(SimpleParser):

    def parse_file(self):
        """
        Parse a .csv input file. Build a data particle from the parsed data.
        """
        reader = csv.reader(self._stream_handle)
        for row in reader:
            log.debug("Read in line: %r", row)

            try:
                # Extract a particle and append it to the record buffer
                particle = self._extract_sample(RasflASubconInstrumentDataParticle, None, row, None)
                self._record_buffer.append(particle)
            except:
                log.warn("Data cannot be parsed: %r", row)
