#!/usr/bin/env python

"""
@package mi.dataset.parser.sio_mule_common data set parser
@file mi/dataset/parser/sio_mule_common.py
@author Emily Hahn
This file contains classes that handle parsing instruments which pass through
sio which contain the common sio header.
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re
import struct
import time
import ntplib

from mi.core.log import get_logger
log = get_logger()
from mi.dataset.dataset_parser import BufferLoadingParser

# SIO Main controller header (ascii) and data (binary):
#   Start of header
#   Header
#   End of header
#   Data
#   End of block

SIO_TIMESTAMP_NUM_BYTES=8

# SIO block sentinels:
SIO_HEADER_START = b'\x01'
SIO_HEADER_END = b'\x02'
SIO_BLOCK_END = b'\x03'

# Supported Instrument IDs.
INSTRUMENT_IDS = b'(CT|AD|FL|DO|PH|PS|CS|WA|WC|WE|CO|PS|CS)'

# SIO controller header:
SIO_HEADER_REGEX = SIO_HEADER_START     # Start of SIO Header (start of SIO block)
SIO_HEADER_REGEX += INSTRUMENT_IDS      # Any 1 of the Instrument IDs
SIO_HEADER_REGEX += b'[0-9]{5}'         # Controller ID
SIO_HEADER_REGEX += b'[0-9]{2}'         # Number of Instrument / Inductive ID
SIO_HEADER_REGEX += b'_'                # Spacer (0x5F)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{4})' # Number of Data Bytes (hex)
SIO_HEADER_REGEX += b'[0-9A-Za-z]'      # MFLM Processing Flag (coded value)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{8})' # POSIX Timestamp of Controller (hex)
SIO_HEADER_REGEX += b'_'                # Spacer (0x5F)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{2})' # Block Number (hex)
SIO_HEADER_REGEX += b'_'                # Spacer (0x5F)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{4})' # CRC Checksum (hex)
SIO_HEADER_REGEX += SIO_HEADER_END      # End of SIO Header (binary data follows)
SIO_HEADER_MATCHER = re.compile(SIO_HEADER_REGEX)

# The SIO_HEADER_MATCHER produces the following groups:
SIO_HEADER_GROUP_ID = 1             # Instrument ID
SIO_HEADER_GROUP_DATA_LENGTH = 2    # Number of Data Bytes
SIO_HEADER_GROUP_TIMESTAMP = 3      # POSIX timestamp
SIO_HEADER_GROUP_BLOCK_NUMBER = 4   # Block Number
SIO_HEADER_GROUP_CHECKSUM = 5       # checksum

# blocks can be uniquely identified a combination of block number and timestamp,
# since block numbers roll over after 255
# each block may contain multiple data samples

# constants for accessing unprocessed and in process data
START_IDX = 0
END_IDX = 1
SAMPLES_PARSED = 2
SAMPLES_RETURNED = 3


class SioParser(BufferLoadingParser):

    def __init__(self, config, stream_handle, exception_callback):
        """
        @param: config The configuration parameters to feed into the parser
        @param: stream_handle An already open file-like file handle
        @param: exception_callback The callback from the agent driver to
           send an exception to
        """
        super(SioParser, self).__init__(config,
                                        stream_handle,
                                        None,
                                        self.sieve_function,
                                        None,
                                        None,
                                        exception_callback)

        self.all_data = None
        self.input_file = stream_handle
        self._record_buffer = []  # holds list of records

    @staticmethod
    def calc_checksum(data):
        """
        Calculate SIO header checksum of data
        @param: data input data to calculate the checksum on
        """
        crc = 65535
        if len(data) == 0:
            return '0000'
        for iData in range(0, len(data)):
            short = struct.unpack('H', data[iData] + '\x00')
            point = 255 & short[0]
            crc ^= point
            for i in range(7, -1, -1):
                if crc & 1:
                    crc = (crc >> 1) ^ 33800
                else:
                    crc >>= 1
        crc = ~crc
        # convert to unsigned
        if crc < 0:
            crc += 65536
        # get rid of the '0x' from the hex string
        crc = "%s" % hex(crc)[2:].upper()
        # make sure we have the right format for comparing, must be 4 hex digits
        if len(crc) == 3:
            crc = '0' + crc
        elif len(crc) == 2:
            crc = '00' + crc
        elif len(crc) == 1:
            crc = '000' + crc
        return crc

    def get_records(self, num_records):
        """
        Go ahead and execute the data parsing loop up to a point. This involves
        getting data from the file, stuffing it in to the chunker, then parsing
        it and publishing.
        @param: num_records The number of records to gather
        @returns: Return the list of particles requested, [] if none available
        """
        if num_records <= 0:
            return []

        if self.all_data is None:
            # need to read in the entire data file first and store it because escape sequences shift position of
            # in process and unprocessed blocks
            self.all_data = self.read_file()
            self.file_complete = True

            # there is more data, add it to the chunker
            self._chunker.add_chunk(self.all_data, ntplib.system_to_ntp_time(time.time()))

            # parse the chunks now that there is new data in the chunker
            result = self.parse_chunks()

            # clear out any non matching data.  Don't do this during parsing because
            # it cleans out actual data too because of the way the chunker works
            (nd_timestamp, non_data) = self._chunker.get_next_non_data(clean=True)
            while non_data is not None:
                (nd_timestamp, non_data) = self._chunker.get_next_non_data(clean=True)

            # add the parsed chunks to the record_buffer
            self._record_buffer.extend(result)

        if len(self._record_buffer) < num_records:
            num_to_fetch = len(self._record_buffer)
        else:
            num_to_fetch = num_records
        # pull particles out of record_buffer and publish
        return_list = self._yank_particles(num_to_fetch)

        return return_list

    def read_file(self):
        """
        This function reads the entire input file.
        @returns: A string containing the contents of the entire file.
        """
        input_buffer = ''

        while True:
            # read data in small blocks in order to not block processing
            next_data = self._stream_handle.read(1024)
            if next_data != '':
                input_buffer = input_buffer + next_data
            else:
                break

        return input_buffer

    def sieve_function(self, raw_data):
        """
        Sieve function for SIO Parser.
        Sort through the raw data to identify blocks of data that need processing.
        This sieve identifies the SIO header, verifies the checksum,
        calculates the end of the SIO block, and returns a list of
        start,end indices.
        @param: raw_data The raw data to search
        @returns: list of matched start,end index found in raw_data
        """
        return_list = []

        #
        # Search the entire input buffer to find all possible SIO headers.
        #
        for match in SIO_HEADER_MATCHER.finditer(raw_data):
            #
            # Calculate the expected end index of the SIO block.
            # If there are enough bytes to comprise an entire SIO block,
            # continue processing.
            # If there are not enough bytes, we're done parsing this input.
            #
            data_len = int(match.group(SIO_HEADER_GROUP_DATA_LENGTH), 16)
            end_packet_idx = match.end(0) + data_len

            if end_packet_idx < len(raw_data):
                #
                # Get the last byte of the SIO block
                # and make sure it matches the expected value.
                #
                end_packet = raw_data[end_packet_idx]
                if end_packet == SIO_BLOCK_END:
                    #
                    # Calculate the checksum on the data portion of the
                    # SIO block (excludes start of header, header,
                    # and end of header).
                    #
                    actual_checksum = SioParser.calc_checksum(
                        raw_data[match.end(0):end_packet_idx])

                    expected_checksum = match.group(SIO_HEADER_GROUP_CHECKSUM)

                    #
                    # If the checksums match, add the start,end indices to
                    # the return list.  The end of SIO block byte is included.
                    #
                    if actual_checksum == expected_checksum:
                        # even if this is not the right instrument, keep track that
                        # this packet was processed
                        return_list.append((match.start(0), end_packet_idx+1))
                    else:
                        log.debug("Calculated checksum %s != received checksum %s for header %s and packet %d to %d",
                                  actual_checksum, expected_checksum,
                                  match.group(0)[1:32],
                                  match.end(0), end_packet_idx)
                else:
                    log.debug('End packet at %d is not x03 for header %s',
                              end_packet_idx, match.group(0)[1:32])

        return return_list

    def _yank_particles(self, num_to_fetch):
        """
        Get particles out of the buffer and publish them. Update the state
        of what has been published, too.
        @param: num_to_fetch The number of particles to remove from the buffer
        @returns: A list with num_to_fetch elements from the buffer. If num_to_fetch
        cannot be collected (perhaps due to an EOF), the list will have the
        elements it was able to collect.
        """
        return_list = []
        if len(self._record_buffer) < num_to_fetch:
            num_to_fetch = len(self._record_buffer)

        records_to_return = self._record_buffer[:num_to_fetch]
        self._record_buffer = self._record_buffer[num_to_fetch:]
        if len(records_to_return) > 0:
            for item in records_to_return:
                return_list.append(item)

        return return_list
