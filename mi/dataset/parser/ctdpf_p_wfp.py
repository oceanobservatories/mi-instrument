#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdpf_p_wfp
@file mi-dataset/mi/dataset/parser/ctdpf_p_wfp.py
@author Samuel Dahlberg
@brief Parser for the ctdpf_p_wfp dataset driver.

This file contains code for the ctdpf_p_wfp parser and code to produce data particles
for the instrument  data from the Prawler WFP instrument. Files are mixed binary and ascii.
"""
import re
from struct import unpack_from
from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException
from mi.dataset.dataset_parser import SimpleParser
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()

# constants
SCI_DATA_SIZE = 31

# byte representations of constants
STRING = b'(.+)'
INTEGER = b'([+-]?[0-9]+)'
NEWLINE = b'(?:\r\n|\n)?'
FLOAT = b'([+-]?\d+.\d+[Ee]?[+-]?\d*)'  # includes scientific notation

# Regex pattern for the start of a binary prawler data packet
REC_PATTERN = (
    b'Record\[(\d+)\]:@@@'
)
REC_REGEX = re.compile(REC_PATTERN, re.DOTALL)


class DataParticleType(BaseEnum):
    CTDPF_P_TELEMETERED = 'ctdpf_p_wfp_instrument'
    CTDPF_P_RECOVERED = 'ctdpf_p_wfp_instrument_recovered'
    __metaclass__ = get_logging_metaclass(log_level='trace')


class CtdpfPTelemeteredDataParticle(DataParticle):
    """
    Class for generating the ctdpf prawler telemetered instrument particle.
    """

    _data_particle_type = DataParticleType.CTDPF_P_TELEMETERED

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]


class CtdpfPRecoveredDataParticle(DataParticle):
    """
    Class for generating the ctdpf prawler recovered instrument particle.
    """

    _data_particle_type = DataParticleType.CTDPF_P_RECOVERED

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]


class CtdpfPParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted for the data particle.
    """

    PRESSURE = 'pressure'
    TEMPERATURE = 'temp'
    CONDUCTIVITY = 'conductivity'


class CtdpfPWfpParser(SimpleParser):
    """
    ctdpf_p_wfp parser.
    Will parse the ctdpf specific data from the generic prawler file.
    """

    def ascii_hex_long_to_long(self, start, data):
        """
        Converts a 4 char sequence of ascii hex characters into a long value in big endian order
        """
        # int conversion requires no comma trailing hex long
        long_str = unpack_from('4s', data, start)[0]
        try:
            return int(long_str, 16)
        except:
            return -9999

    def parse_record(self, data):
        """
        Parse a data record from the prawler data file, storing only the ctdpf_p instrument data
        """

        # Note: have seen science data packets terminate prematurely in files. When this
        # happens, ascii_hex_long_to_long logs an exception. Check to make sure all values are positive

        epoch_time = unpack_from('>L', data)[0]
        pressure = float(self.ascii_hex_long_to_long(5, data))
        temperature = float(self.ascii_hex_long_to_long(10, data))
        conductivity = float(self.ascii_hex_long_to_long(15, data))
        # opt_temp = float(self.ascii_hex_long_to_long(20, data)) / 1000.0
        # opt_o2 = float(self.ascii_hex_long_to_long(25, data)) / 1000.0

        if pressure == -9999 or temperature == -9999 or conductivity == -9999:
            return None, None

        # Decimal precision provided by McClane

        pressure = pressure / 100.0
        temperature = temperature / 1000.0
        conductivity = conductivity / 1000.0

        ctdpf_particle_data = {
            CtdpfPParticleKey.PRESSURE: pressure,
            CtdpfPParticleKey.TEMPERATURE: temperature,
            CtdpfPParticleKey.CONDUCTIVITY: conductivity
        }

        return ctdpf_particle_data, epoch_time

    def parse_file(self):
        """
        Parse the prawler file.
        Read file line by line.
        @return: dictionary of data values with the particle names as keys
        """

        data = self._stream_handle.read()

        matches = REC_REGEX.finditer(data)

        for record_match in matches:
            start = record_match.start()

            # Scan past "Record[nnn]:" to type of records
            rec_type_start = data.find(b':', start)
            if rec_type_start >= 0:
                start = rec_type_start + 1

            rectype = data[start + 7]

            # Science profile data
            if rectype == 'E':
                rec_start = unpack_from('<3s', data, start)[0]

                if rec_start != b'@@@':
                    continue

                # skip column list (ends in \r\n)
                here = start + 8
                eol = unpack_from('2s', data, here)[0]
                while eol != b'\x0d\x0a':
                    here = here + 1
                    eol = unpack_from('2s', data, here)[0]
                here = here + 2

                # The actual science data
                eol = unpack_from('2s', data, here)[0]
                while eol != b'\x0d\x0a':
                    data_chunk = data[here:here + SCI_DATA_SIZE]

                    ctdpf_particle_data, epoch_timestamp = self.parse_record(data_chunk)

                    # If there is no data returned, there is most likely no more science data, or it is not recoverable.
                    # We will exit this section of science data if no data is returned
                    if ctdpf_particle_data is None:
                        log.error('Erroneous data found')
                        self._exception_callback(
                            SampleException('Erroneous data found')
                        )
                        break

                    # Convert timestamp to ntp timestamp
                    timestamp = epoch_timestamp + 2208988800

                    particle = self._extract_sample(self._particle_class, None, ctdpf_particle_data,
                                                    internal_timestamp=timestamp)

                    if particle is not None:
                        self._record_buffer.append(particle)
                        log.trace('Parsed particle: %s' % particle.generate_dict())

                    here = here + SCI_DATA_SIZE
                    eol = unpack_from('2s', data, here)[0]

            # Engineering Data (unparsed)
            elif rectype == 'G':
                continue
            # Station list data (unparsed)
            elif rectype == 'S':
                continue
