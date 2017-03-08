#!/usr/bin/env python

"""
@package mi.dataset.parser.ppsdn_a_subcon
@file mi-dataset/mi/dataset/parser/ppsdn_a_subcon_recovered_driver.py
@author Rachel Manoni
@brief Parser for the ppsdn_a_subcon dataset driver

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

PPSDN_DATA_MAP = [
    ('serial_number', 1, str),
    ('start_time', 2, str),
    ('stop_time', 3, str),
    ('port_number', 4, int),
    ('lat', 5, float),
    ('lon', 6, float),
    ('m_depth', 7, int),
    ('ncbi_sequence_read_archive_url', 8, str),
    ('fasta_url', 9, str),
    ('vamps_url', 10, str),
]

START_TIME_IDX = 2


class PpsdnASubconInstrumentDataParticle(DataParticle):

    _data_particle_type = 'ppsdn_a_subcon_instrument_recovered'
    instrument_particle_map = PPSDN_DATA_MAP

    def __init__(self, raw_data, *args, **kwargs):

        super(PpsdnASubconInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # set timestamp mm/dd/yyyy hh:mm
        struct_time = time.strptime(self.raw_data[START_TIME_IDX], "%m/%d/%Y %H:%M")
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
        except Exception as e:
            log.warn("Data particle error encoding. Name:%s Value:%s", name, value)
            encoded_val = None
        finally:
            return {DataParticleKey.VALUE_ID: name,
                    DataParticleKey.VALUE: encoded_val}


class PpsdnASubconParser(SimpleParser):

    def parse_file(self):
        """
        Parse a .csv input file. Build a data particle from the parsed data.
        """
        reader = csv.reader(self._stream_handle)
        for row in reader:
            log.debug("Read in line: %r", row)

            try:
                # Extract a particle and append it to the record buffer
                particle = self._extract_sample(PpsdnASubconInstrumentDataParticle, None, row, None)
                self._record_buffer.append(particle)
            except:
                log.warn("Data cannot be parsed: %r", row)

