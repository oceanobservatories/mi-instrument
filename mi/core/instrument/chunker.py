#!/usr/bin/env python

"""
@package mi.core.instrument.chunker Chunking buffer for MI work
@file mi/core/instrument/chunker.py
@author Steve Foley
@brief A buffer structure that allows for combining fragments and breaking
    apart multiple instances of the same data from an instrument's data stream.
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from mi.core.log import get_logger
log = get_logger()


class StringChunker(object):
    """
    A great big buffer that ingests incoming data from an instrument, then
    sieves it out into different streams for recognized data segments and non
    data. In the process it aggregates data fragments into whole chunks and
    breaks apart collections of data segments so they can be broken into
    individual blocks.
    """
    def __init__(self, data_sieve_fn, max_buff_size=65535):
        """
        Initialize the buffer and indexing structures
        The lists keep track of the start and stop index values (inclusive)
        of the particular type in the data buffer. The lists are tuples with
        (start, stop)

        @param data_sieve_fn A function that takes in a chunk of raw data (in
            whatever format is needed by the Chunker subclass) and spits out
            a list of (start_index, end_index) tuples. start_index is the
            array index of the first item in the data list, end_index is one more
            than the last item's index. This allows
            buffer[start_index:end_index] to properly describe the data block.
            If no data is present, return and empty list. If multiple data
            blocks are found, the returned list will contain multiple tuples,
            IN SEQUENTIAL ORDER and WITHOUT OVERLAP.
        """
        self.sieve = data_sieve_fn
        self.max_buff_size = max_buff_size

        self.buffer = ""
        self.timestamps = []
        self.chunks = []

    def add_chunk(self, raw_data, timestamp):
        """
        Adds a chunk of data to the end of the buffer
        @param raw_data Input data (string)
        @param timestamp The time (in NTP4 float format) that the data was collected at the port agent
        """
        start_index = len(self.buffer)
        end_index = start_index + len(raw_data)

        if end_index > self.max_buff_size:
            oversize = end_index - self.max_buff_size
            log.warn('Chunker buffer has grown beyond specified limit (%d), truncating %d bytes',
                     self.max_buff_size, oversize)

            self._rebase_times(oversize)
            self.buffer = self.buffer[oversize:]
            start_index -= oversize
            end_index -= oversize

        self.timestamps.append((start_index, end_index, timestamp))
        self.buffer += raw_data
        self._make_chunks()

    def get_next_data(self):
        """
        Yield a chunk (timestamp, data) if there are any available
        """
        if len(self.chunks) == 0:
            return None, None

        return self.chunks.pop(0)

    def clean(self):
        self.chunks = []
        self.timestamps = []
        self.buffer = ''

    @staticmethod
    def _prune_overlaps(results):
        """
        Remove any overlapping results from the results list. First match wins.
        """
        if len(results) < 2:
            return results

        remove_indices = []

        for index in xrange(len(results)-1):
            s1, e1 = results[index]
            s2, e2 = results[index+1]
            if s2 < e1:
                remove_indices.append(index+1)

        # remove overlapping segments from right to left
        # so the indexes don't change...
        if remove_indices:
            log.error('Found overlapping matches from sieve function: %r %r', remove_indices, len(results))
            for index in reversed(remove_indices):
                results.pop(index)

        return results

    def _find_timestamp(self, index):
        """
        Given an index into the buffer, find the corresponding timestamp
        """
        for start, stop, timestamp in self.timestamps:
            if start <= index < stop:
                return timestamp
        log.error('Failed to find timestamp for chunk!')
        return 0

    def _rebase_times(self, index):
        """
        Buffer is going to be pruned, adjust all timestamp indexes to match
        """
        new_times = []
        for start, stop, timestamp in self.timestamps:
            if stop <= index:
                continue
            start -= index
            stop -= index
            new_times.append((start, stop, timestamp))
        self.timestamps = new_times

    def _make_chunks(self):
        """
        Run the buffer through our sieve function. Generate a chunk (timestamp, data) for
        each non-overlapping result found. Prune the buffer to the index of the last found data.
        """
        results = sorted(self.sieve(self.buffer))
        results = self._prune_overlaps(results)

        end = 0
        for start, end in results:
            chunk = self.buffer[start:end]
            timestamp = self._find_timestamp(start)
            self.chunks.append((timestamp, chunk))

        if end > 0:
            self._rebase_times(end)
            self.buffer = self.buffer[end:]

    @staticmethod
    def regex_sieve_function(raw_data, regex_list=None):
        """
        Generate a sieve function given a list of regexes.
        Intended to be used with partial function application, as so:
        StringChunker(partial(self._chunker.regex_sieve_function, regex_list=[regex]))
        @param raw_data The raw data to run through this regex sieve
        @param regex_list a list of pre-compiled regexes
        @retval A list of (start, end) tuples for each match the regexs find
        """
        return_list = []
        if regex_list is not None:
            for matcher in regex_list:
                for match in matcher.finditer(raw_data):
                    return_list.append((match.start(), match.end()))

        return return_list
