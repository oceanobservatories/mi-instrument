#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/flord_l_wfp_sio.py
@author Maria Lutz, Mark Worden
@brief Parser for the flord_l_wfp_sio_mule dataset driver
Release notes:

Initial Release
"""

import re
import struct
import ntplib

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import UnexpectedDataException
from mi.dataset.parser.sio_mule_common import SioParser, SIO_HEADER_MATCHER
from mi.dataset.parser.WFP_E_file_common import HEADER_BYTES, STATUS_BYTES, \
    STATUS_BYTES_AUGMENTED, STATUS_START_MATCHER

__author__ = 'Maria Lutz'
__license__ = 'Apache 2.0'

# E header regex for global sites
E_HEADER_REGEX = b'(\x00\x01\x00{5,5}[\x01|\x04|\x0c]\x00{7,7}\x01)([\x00-\xff]{8,8})'
E_HEADER_MATCHER = re.compile(E_HEADER_REGEX)

E_GLOBAL_SAMPLE_BYTES = 30

log = get_logger()


class DataParticleType(BaseEnum):
    SAMPLE = 'flord_l_wfp_instrument'


class FlordLWfpSioDataParticleKey(BaseEnum):
    # params collected for the flord_l_wfp_instrument stream
    RAW_SIGNAL_CHL = 'raw_signal_chl'
    RAW_SIGNAL_BETA = 'raw_signal_beta'  # corresponds to 'ntu' from E file
    RAW_INTERNAL_TEMP = 'raw_internal_temp'
    WFP_TIMESTAMP = 'wfp_timestamp'


class FlordLWfpSioDataParticle(DataParticle):

    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        fields_prof = struct.unpack('>I f f f f f h h h', self.raw_data)
        result = [self._encode_value(FlordLWfpSioDataParticleKey.RAW_SIGNAL_CHL,
                                     fields_prof[6], int),
                  self._encode_value(FlordLWfpSioDataParticleKey.RAW_SIGNAL_BETA,
                                     fields_prof[7], int),
                  self._encode_value(FlordLWfpSioDataParticleKey.RAW_INTERNAL_TEMP,
                                     fields_prof[8], int),
                  self._encode_value(FlordLWfpSioDataParticleKey.WFP_TIMESTAMP,
                                     fields_prof[0], int)]

        return result


class FlordLWfpSioParser(SioParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        super(FlordLWfpSioParser, self).__init__(config,
                                                 stream_handle,
                                                 exception_callback)

        self._result_particles = []

    def _process_we_record(self, payload):

        indices_list = self.we_split_function(payload)
        for indices in indices_list:
            e_record = payload[indices[0]:indices[1]]

            if not STATUS_START_MATCHER.match(e_record[0:STATUS_BYTES]):
                fields = struct.unpack('>I', e_record[0:4])
                self._timestamp = ntplib.system_to_ntp_time(float(fields[0]))

                sample = self._extract_sample(FlordLWfpSioDataParticle,
                                              None,
                                              e_record,
                                              self._timestamp)
                if sample:
                    # create particle
                    self._result_particles.append(sample)

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
        parsing, plus the state. An empty list of nothing was parsed.
        """

        (timestamp, chunk) = self._chunker.get_next_data()

        while chunk is not None:
            # Parse/match the SIO header
            sio_header_match = SIO_HEADER_MATCHER.match(chunk)
            end_of_header = sio_header_match.end(0)

            if sio_header_match.group(1) == 'WE':

                # Parse/match the E file header
                e_header_match = E_HEADER_MATCHER.search(
                    chunk[end_of_header:end_of_header+HEADER_BYTES])

                if e_header_match:

                    # '-1' to remove the '\x03' end-of-record marker
                    payload = chunk[end_of_header+HEADER_BYTES:-1]

                    self._process_we_record(payload)

                else:
                    message = "Found unexpected data."
                    log.warn(message)
                    self._exception_callback(UnexpectedDataException(
                        message))

            else:  # no e header match
                message = "Found unexpected data."
                log.warn(message)
                self._exception_callback(UnexpectedDataException(message))

            (timestamp, chunk) = self._chunker.get_next_data()

        return self._result_particles

    def we_split_function(self, raw_data):
        """
        Sort through the raw data to identify new blocks of data that need processing.

        :param raw_data: Unprocessed data from the instrument to be parsed.
        """
        form_list = []

        """
        The Status messages can have an optional 2 bytes on the end, and since the
        rest of the data consists of relatively unformated packed binary records,
        detecting the presence of that optional 2 bytes can be difficult. The only
        pattern we have to detect is the STATUS_START field ( 4 bytes FF FF FF F[A-F]).
        We peel this appart by parsing backwards, using the end-of-record as an
        additional anchor point.
        """
        parse_end_point = len(raw_data)
        while parse_end_point > 0:
            # look for a status message at postulated message header position

            # look for an augmented status
            if STATUS_START_MATCHER.match(raw_data[parse_end_point-STATUS_BYTES_AUGMENTED:parse_end_point]):
                # A hit for the status message at the augmented offset
                # NOTE, we don't need the status messages and only deliver a stream of
                # samples to build_parsed_values
                parse_end_point -= STATUS_BYTES_AUGMENTED

                # check if this is an unaugmented status
            elif STATUS_START_MATCHER.match(raw_data[parse_end_point-STATUS_BYTES:parse_end_point]):
                # A hit for the status message at the unaugmented offset
                # NOTE: same as above
                parse_end_point = parse_end_point-STATUS_BYTES
            else:
                # assume if not a stat that hit above, we have a sample. Mis-parsing will result
                # in extra bytes at the end and a sample exception.
                form_list.append((parse_end_point-E_GLOBAL_SAMPLE_BYTES, parse_end_point))
                parse_end_point -= E_GLOBAL_SAMPLE_BYTES

            # if the remaining bytes are less than data sample bytes, all we might have left is a status sample
            if parse_end_point != 0 and parse_end_point < STATUS_BYTES \
                    and parse_end_point < E_GLOBAL_SAMPLE_BYTES  \
                    and parse_end_point < STATUS_BYTES_AUGMENTED:
                self._exception_callback(UnexpectedDataException(
                    "Error sieving WE data, inferred sample/status alignment incorrect"))
                return_list = []
                return return_list

        # Because we parsed this backwards, we need to reverse the list to deliver the data in the correct order
        return_list = form_list[::-1]
        log.debug("returning we sieve/split list %s", return_list)
        return return_list
