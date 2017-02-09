#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/dosta_ln_wfp_sio.py
@author Christopher Fortin
@brief Parser for the dosta_ln_wfp_sio dataset driver
Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import struct
import ntplib
from binascii import hexlify

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import SampleException, UnexpectedDataException
from mi.dataset.parser.sio_mule_common import SioParser, SIO_HEADER_MATCHER
from mi.dataset.parser.WFP_E_file_common import HEADER_BYTES, STATUS_BYTES, \
    STATUS_BYTES_AUGMENTED, STATUS_START_MATCHER, \
    WFP_E_GLOBAL_FLAGS_HEADER_MATCHER, \
    WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES as E_GLOBAL_SAMPLE_BYTES, \
    WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_MATCHER as WFP_E_SAMPLE_MATCHER


class DataParticleType(BaseEnum):
    SAMPLE = 'dosta_ln_wfp_instrument'


class DostaLnWfpSioDataParticleKey(BaseEnum):
    OPTODE_OXYGEN = 'estimated_oxygen_concentration'
    OPTODE_TEMPERATURE = 'optode_temperature'
    WFP_TIMESTAMP = 'wfp_timestamp'

SIO_HEADER_BYTES = 32


class DostaLnWfpSioDataParticle(DataParticle):
    """
    Class for parsing data from the dosta_ln_wfp_sio data set
    """

    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        # NOTE: since we are dropping the status messages in the sieve, only
        # samples should make it here
        if len(self.raw_data) != E_GLOBAL_SAMPLE_BYTES:
            raise SampleException("Error while decoding parameters in data: ")

        try:
            match = WFP_E_SAMPLE_MATCHER.match(self.raw_data)
            # grab the timestamp from the first match group
            fields_prof = struct.unpack('>I', match.group(1))
            wfp_timestamp = fields_prof[0]

            # and parse the rest of the data from the next match group
            fields_prof = struct.unpack('>f f f f f H H H', match.group(2))
            optode_oxygen = fields_prof[3]
            optode_temperature = fields_prof[4]

        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, match.group(0)))

        result = [self._encode_value(DostaLnWfpSioDataParticleKey.OPTODE_OXYGEN,
                                     optode_oxygen, float),
                  self._encode_value(DostaLnWfpSioDataParticleKey.OPTODE_TEMPERATURE,
                                     optode_temperature, float),
                  self._encode_value(DostaLnWfpSioDataParticleKey.WFP_TIMESTAMP,
                                     wfp_timestamp, int)]

        log.debug('DostLnWfpSioMuleDataParticle: particle=%s', result)

        return result


class DostaLnWfpSioParser(SioParser):

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list if nothing was parsed.
        """
        result_particles = []
        (timestamp, chunk) = self._chunker.get_next_data()

        while chunk is not None:
            sio_header_match = SIO_HEADER_MATCHER.match(chunk)

            log.debug('parsing header %s', sio_header_match.group(0)[1:SIO_HEADER_BYTES])

            if sio_header_match.group(1) != 'WE':
                log.warn(" chunk did not match header WE %s", chunk[0:SIO_HEADER_BYTES])
                # get the next chunk
                (timestamp, chunk) = self._chunker.get_next_data()
                continue  # jump to next iteration of the chunk loop

                # Parse/match the E file header
            e_header_match = WFP_E_GLOBAL_FLAGS_HEADER_MATCHER.search(
                chunk[SIO_HEADER_BYTES:SIO_HEADER_BYTES+HEADER_BYTES+1])

            if not e_header_match:
                # no e header match
                log.warn("*BAD E HEADER 0x%s",
                         ":".join("{:02x}".format(ord(c)) for c in chunk))
                self._exception_callback(UnexpectedDataException("Found unexpected data."))
                # get the next chunk
                (timestamp, chunk) = self._chunker.get_next_data()
                continue  # jump to next iteration of the chunk loop

            payload = chunk[SIO_HEADER_BYTES+HEADER_BYTES+1:]

            data_split = self.we_split_function(payload)

            log.debug('Found data match in chunk %s', chunk[1:SIO_HEADER_BYTES])
            for ii in range(0, len(data_split)):
                e_record = payload[data_split[ii][0]:data_split[ii][1]]
                log.debug('Extracted E Record to store in particle %s', hexlify(e_record))

                # particle-ize the data block received, return the record
                if not STATUS_START_MATCHER.match(e_record[0:STATUS_BYTES]):

                    fields = struct.unpack('>I', e_record[0:4])
                    timestamp_s = float(fields[0])
                    timestamp = ntplib.system_to_ntp_time(timestamp_s)

                    if len(e_record) == E_GLOBAL_SAMPLE_BYTES:
                        # create particle
                        log.debug('Particle created with raw data %s', hexlify(e_record))
                        log.debug('Particle timestamp = %f', timestamp)
                        sample = self._extract_sample(DostaLnWfpSioDataParticle,
                                                      None,
                                                      e_record,
                                                      timestamp)
                        result_particles.append(sample)

            (timestamp, chunk) = self._chunker.get_next_data()

        return result_particles

    def we_split_function(self, raw_data):
        """
        Sort through the raw data to identify new blocks of data that need processing.
        This is needed instead of a regex because blocks are identified by position
        in this binary file.
        """

        form_list = []
        raw_data_len = len(raw_data)

        """
        The Status messages
        can have an optional 2 bytes on the end, and since the rest of the data
        is relatively un-formatted packed binary records, detecting the presence of
        that optional 2 bytes can be difficult.  The only pattern we have to detect
        is the STATUS_START field ( 4 bytes FF FF FF F[A-F] ).  Peel this
        apart by parsing backwards, using the end-of-record as an additional anchor
        point.
        """

        # '-1' to remove the '\x03' end-of-record marker
        parse_end_point = raw_data_len - 1
        while parse_end_point > 0:

            # look for a status message at postulated message header position

            # look for an augmented status
            if STATUS_START_MATCHER.match(raw_data[parse_end_point-STATUS_BYTES_AUGMENTED:parse_end_point]):
                # A hit for the status message at the augmented offset
                # NOTE, we don't need the status messages, so we drop them on the floor here
                # and only deliver a stream of samples to build_parse_values
                parse_end_point = parse_end_point-STATUS_BYTES_AUGMENTED

                # check if this is a un-augmented status
            elif STATUS_START_MATCHER.match(raw_data[parse_end_point-STATUS_BYTES:parse_end_point]):
                # A hit for the status message at the un-augmented offset
                # NOTE: same as above
                parse_end_point = parse_end_point-STATUS_BYTES

            else:
                # assume if not a stat that hit above, we have a sample.  If we mis-parse, we
                # will end up with extra bytes when we finish, and sample_except at that point.
                form_list.append((parse_end_point-E_GLOBAL_SAMPLE_BYTES, parse_end_point))
                parse_end_point = parse_end_point-E_GLOBAL_SAMPLE_BYTES

            # if the remaining bytes are less than the data sample bytes,
            # all we might have left is a status sample, if we don't we're done
            if parse_end_point != 0 and \
               parse_end_point < STATUS_BYTES and \
               parse_end_point < E_GLOBAL_SAMPLE_BYTES and \
               parse_end_point < STATUS_BYTES_AUGMENTED:

                self._exception_callback(UnexpectedDataException(
                    "Error sieving WE data, inferred sample/status alignment incorrect"))
                return_list = []
                return return_list

        # since we parsed this backwards, we need to reverse to list to deliver the data in the correct order
        return_list = form_list[::-1]
        log.debug("returning we sieve list %s", return_list)
        return return_list

