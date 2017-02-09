#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdpf_ckl_wfp_sio_mule
@file marine-integrations/mi/dataset/parser/ctdpf_ckl_wfp_sio.py
@author cgoodrich
@brief Parser for the ctdpf_ckl_wfp_sio dataset driver
Release notes:

Initial Release
"""

__author__ = 'cgoodrich'
__license__ = 'Apache 2.0'

import re
import struct
import ntplib

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.parser.sio_mule_common import SioParser

DATA_RECORD_BYTES = 11  # Number of bytes in a WC-type file
TIME_RECORD_BYTES = 8   # Two four byte timestamps
ETX_BYTE = 1            # The 1 byte ETX marker (\x03)
HEADER_BYTES = 33       # Number of bytes in the SIO header
DECIMATION_SPACER = 2   # This may or may not be present in the input stream

FOOTER_BYTES = DATA_RECORD_BYTES + TIME_RECORD_BYTES + DECIMATION_SPACER + ETX_BYTE

WC_HEADER_REGEX = b'\x01(WC)[0-9]{7}_([0-9a-fA-F]{4})[a-zA-Z]([0-9a-fA-F]{8})_([0-9a-fA-F]{2})_([0-9a-fA-F]{4})\x02'
WC_HEADER_MATCHER = re.compile(WC_HEADER_REGEX)

STD_EOP_REGEX = b'(\xFF{11})([\x00-\xFF]{8})\x03'
STD_EOP_MATCHER = re.compile(STD_EOP_REGEX)

DECI_EOP_REGEX = b'(\xFF{11})([\x00-\xFF]{8})([\x00-\xFF]{2})\x03'
DECI_EOP_MATCHER = re.compile(DECI_EOP_REGEX)

DATA_REGEX = b'([\x00-\xFF]{11})'
DATA_MATCHER = re.compile(DATA_REGEX)

EOP_REGEX = b'(\xFF{11})'
EOP_MATCHER = re.compile(EOP_REGEX)


class DataParticleType(BaseEnum):
    DATA = 'ctdpf_ckl_wfp_instrument'
    METADATA = 'ctdpf_ckl_wfp_sio_mule_metadata'
    RECOVERED_DATA = 'ctdpf_ckl_wfp_instrument_recovered'
    RECOVERED_METADATA = 'ctdpf_ckl_wfp_metadata_recovered'


class CtdpfCklWfpSioDataParticleKey(BaseEnum):
    CONDUCTIVITY = 'conductivity'
    TEMPERATURE = 'temperature'
    PRESSURE = 'pressure'


class CtdpfCklWfpSioMetadataParticleKey(BaseEnum):
    WFP_TIME_ON = 'wfp_time_on'
    WFP_TIME_OFF = 'wfp_time_off'
    WFP_NUMBER_SAMPLES = 'wfp_number_samples'
    WFP_DECIMATION_FACTOR = 'wfp_decimation_factor'


class CtdpfCklWfpSioDataParticle(DataParticle):
    """
    Class for creating the data particle
    """
    _data_particle_type = DataParticleType.DATA

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """
        result = [self._encode_value(CtdpfCklWfpSioDataParticleKey.CONDUCTIVITY, self.raw_data[0], int),
                  self._encode_value(CtdpfCklWfpSioDataParticleKey.TEMPERATURE, self.raw_data[1], int),
                  self._encode_value(CtdpfCklWfpSioDataParticleKey.PRESSURE, self.raw_data[2], int)
        ]

        return result


class CtdpfCklWfpSioMetadataParticle(DataParticle):
    """
    Class for creating the metadata particle
    """
    _data_particle_type = DataParticleType.METADATA

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """
        result = [self._encode_value(CtdpfCklWfpSioMetadataParticleKey.WFP_TIME_ON,
                                     self.raw_data[0], int),
                  self._encode_value(CtdpfCklWfpSioMetadataParticleKey.WFP_TIME_OFF,
                                     self.raw_data[1], int),
                  self._encode_value(CtdpfCklWfpSioMetadataParticleKey.WFP_NUMBER_SAMPLES,
                                     self.raw_data[2], int)]

        # Have to split the result build due to a bug in the _encode_value code.
        if self.raw_data[3] is not None:
            result.append(self._encode_value(CtdpfCklWfpSioMetadataParticleKey.WFP_DECIMATION_FACTOR,
                                             self.raw_data[3], int))
        else:
            result.append({DataParticleKey.VALUE_ID: CtdpfCklWfpSioMetadataParticleKey.WFP_DECIMATION_FACTOR,
                           DataParticleKey.VALUE: None})

        return result


class CtdpfCklWfpSioParser(SioParser):
    """
    Make use of the common Sio Mule file parser
    """
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        super(CtdpfCklWfpSioParser, self).__init__(config,
                                                   stream_handle,
                                                   exception_callback)
        self._metadataSent = False
        self._data_length = 0
        self._start_index = HEADER_BYTES + 1
        self._end_index = 0
        self._good_header = False
        self._good_footer = False
        self._number_of_records = 0
        self._record_number = 0.0
        self._time_increment = 1
        self._decimation_factor = None
        self._start_time = 0
        self._end_time = 0
        self._start_data = 0
        self._footer_data = None
        self._record_data = None

    def process_header(self, chunk):
        """
        Determine if this is the header for a WC file
        @retval True (good header), False (bad header)
        """
        header = chunk[0:HEADER_BYTES]
        match = WC_HEADER_MATCHER.match(header)
        if match:
            self._data_length = int(match.group(2), 16)
            self._start_index = match.start(0)
            self._end_index = match.end(0) + self._data_length
            self._start_data = match.end(0)
            self._good_header = True
        else:
            self._good_header = False

    def process_footer(self, chunk):
        """
        Determine if this footer has a decimation factor (and what it is) or not.
        Also determine the instrument start/stop times and the number of records in the chunk
        @retval True (good footer), False (bad footer)
        """
        footer = chunk[((self._end_index - FOOTER_BYTES) + 1):self._end_index + 1]
        std_match = STD_EOP_MATCHER.search(footer)
        deci_match = DECI_EOP_MATCHER.search(footer)
        final_match = deci_match
        if deci_match:
            self._number_of_records = ((self._data_length + 1) - FOOTER_BYTES) / 11
            self._decimation_factor = struct.unpack('>H', final_match.group(3))[0]
            self._good_footer = True
        elif std_match:
            footer_start = std_match.start(0)
            footer_end = std_match.end(0)
            footer = footer[footer_start:footer_end]
            final_match = STD_EOP_MATCHER.search(footer)
            self._number_of_records = ((self._data_length + 1) - (FOOTER_BYTES - DECIMATION_SPACER)) / 11
            self._decimation_factor = 0
            self._good_footer = True
        else:
            self._good_footer = False
            log.warning('CTDPF_CKL_SIO_MULE: Bad footer detected, cannot parse chunk')

        if self._good_footer:
            time_fields = struct.unpack('>II', final_match.group(2))
            self._start_time = int(time_fields[0])
            self._end_time = int(time_fields[1])
            if self._number_of_records > 0:
                self._time_increment = float(self._end_time - self._start_time) / float(self._number_of_records)
            else:
                self._good_footer = False
                log.warning('CTDPF_CKL_SIO_MULE: Bad footer detected, cannot parse chunk')

    # Overrides the parse_chunks routine in SioMuleCommon
    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If it is a valid data piece, build a particle,
        update the position and timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples
        """
        result_particles = []
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)

        while chunk is not None:

            self.process_header(chunk)

            if self._good_header:
                self.process_footer(chunk)

                if self._good_footer:

                    timestamp = float(ntplib.system_to_ntp_time(self._start_time))

                    self._footer_data = (self._start_time,
                                         self._end_time,
                                         self._number_of_records,
                                         self._decimation_factor)

                    sample = self._extract_sample(CtdpfCklWfpSioMetadataParticle,
                                                  None, self._footer_data, timestamp)

                    if sample is not None:
                        result_particles.append(sample)

                    more_records = True
                    data_record = chunk[self._start_data:self._start_data + DATA_RECORD_BYTES]
                    self._start_data += DATA_RECORD_BYTES
                    self._record_number = 0.0
                    timestamp = float(ntplib.system_to_ntp_time(float(self._start_time) +
                                                                (self._record_number * self._time_increment)))

                    while more_records:
                        data_fields = struct.unpack('>I', '\x00' + data_record[0:3]) + \
                                      struct.unpack('>I', '\x00' + data_record[3:6]) + \
                                      struct.unpack('>I', '\x00' + data_record[6:9]) + \
                                      struct.unpack('>H', data_record[9:11])

                        self._record_data = (data_fields[0], data_fields[1], data_fields[2])
                        sample = self._extract_sample(CtdpfCklWfpSioDataParticle,
                                                      None, self._record_data, timestamp)

                        if sample is not None:

                            result_particles.append(sample)

                        data_record = chunk[self._start_data:self._start_data + DATA_RECORD_BYTES]
                        self._record_number += 1.0
                        timestamp = float(ntplib.system_to_ntp_time(float(self._start_time) +
                                                                    (self._record_number * self._time_increment)))

                        eop_match = EOP_MATCHER.search(data_record)

                        if eop_match:
                            more_records = False
                        else:
                            self._start_data += DATA_RECORD_BYTES

            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)

        return result_particles
