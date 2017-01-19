#!/usr/bin/env python

"""
@package mi.core.instrument.test.test_chunker
@file mi/core/instrument/test/test_chunker.py
@author Steve Foley
@brief Test cases for the base chunker module
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from functools import partial

import re
from mi.core.instrument.chunker import StringChunker
from mi.core.unit_test import MiUnitTestCase
from mi.logging import log
from nose.plugins.attrib import attr


@attr('UNIT', group='mi')
class UnitTestStringChunker(MiUnitTestCase):
    """
    Test the basic functionality of the chunker system via unit tests
    """
    # For testing, use PAR sensor data here...short and easy to work with...
    # But cheat with the checksum. Make it easy to recognize which sample
    SAMPLE_1 = "SATPAR0229,10.01,2206748111,111"
    SAMPLE_2 = "SATPAR0229,10.02,2206748222,222"
    SAMPLE_3 = "SATPAR0229,10.03,2206748333,333"

    FRAGMENT_1 = "SATPAR0229,10.01,"
    FRAGMENT_2 = "2206748544,123"
    FRAGMENT_SAMPLE = FRAGMENT_1+FRAGMENT_2

    MULTI_SAMPLE_1 = "%s\r\n%s" % (SAMPLE_1,
                                   SAMPLE_2)

    TIMESTAMP_1 = 3569168821.102485
    TIMESTAMP_2 = 3569168822.202485
    TIMESTAMP_3 = 3569168823.302485

    @staticmethod
    def sieve_function(raw_data):
        """ The method that splits samples
        """
        return_list = []
        pattern = r'SATPAR(?P<sernum>\d{4}),(?P<timer>\d{1,7}.\d\d),(?P<counts>\d{10}),(?P<checksum>\d{1,3})'
        regex = re.compile(pattern)

        for match in regex.finditer(raw_data):
            return_list.append((match.start(), match.end()))
            log.debug("Sieving: %s...%s",
                      raw_data[match.start():match.start()+5],
                      raw_data[match.end()-5:match.end()])

        return return_list

    def setUp(self):
        """ Setup a chunker for use in tests """
        self._chunker = StringChunker(UnitTestStringChunker.sieve_function)

    def test_sieve(self):
        """
        Do a quick test of the sieve to make sure it does what we want.
        """
        self.assertEquals([(0,31)],
                          UnitTestStringChunker.sieve_function(self.SAMPLE_1))
        self.assertEquals([],
                          UnitTestStringChunker.sieve_function(self.FRAGMENT_1))
        self.assertEquals([(0,31), (33, 64)],
                          UnitTestStringChunker.sieve_function(self.MULTI_SAMPLE_1))

    def test_regex_sieve(self):
        """
        Do a test of the regex based sieve to make sure it does what we want.
        """
        pattern = r'SATPAR(?P<sernum>\d{4}),(?P<timer>\d{1,7}.\d\d),(?P<counts>\d{10}),(?P<checksum>\d{1,3})'
        regex = re.compile(pattern)

        self._chunker = StringChunker(partial(self._chunker.regex_sieve_function, regex_list=[regex]))

        self.assertEquals([(0,31)],
                          self._chunker.regex_sieve_function(self.SAMPLE_1, [regex]))
        self.assertEquals([],
                          self._chunker.regex_sieve_function(self.FRAGMENT_1, [regex]))
        self.assertEquals([(0,31), (33, 64)],
                          self._chunker.regex_sieve_function(self.MULTI_SAMPLE_1, [regex]))

    def test_make_chunks(self):
        sample_string = "Foo%sBar%sBat" % (self.SAMPLE_1, self.SAMPLE_2)
        self._chunker.add_chunk(sample_string, self.TIMESTAMP_1)

        chunks = self._chunker.chunks

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0][0], self.TIMESTAMP_1)
        self.assertEqual(chunks[0][1], self.SAMPLE_1)
        self.assertEqual(chunks[1][1], self.SAMPLE_2)

    def test_add_get_simple(self):
        """
        Add a simple string of data to the buffer, get the next chunk out
        """
        self._chunker.add_chunk(self.SAMPLE_1, self.TIMESTAMP_1)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(time, self.TIMESTAMP_1)
        self.assertEquals(result, self.SAMPLE_1)

        # It got cleared at the last fetch...
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(time, None)
        self.assertEquals(result, None)

        self.assertEqual(self._chunker.buffer, '')

    def test_rebase_timestamps(self):
        """
        Test an add/get without cleaning
        """
        self._chunker.add_chunk(self.SAMPLE_1, self.TIMESTAMP_1)
        self._chunker.add_chunk("BLEH", self.TIMESTAMP_2)
        self._chunker.get_next_data()

        timestamps = self._chunker.timestamps

        self.assertEqual(len(timestamps), 1)
        self.assertEqual(timestamps[0][0], 0)
        self.assertEqual(timestamps[0][1], 4)
        self.assertEqual(timestamps[0][2], self.TIMESTAMP_2)

    def test_add_many_get_simple(self):
        """
        Add a few simple strings of data to the buffer, get the chunks out
        """
        self._chunker.add_chunk(self.SAMPLE_1, self.TIMESTAMP_1)
        self._chunker.add_chunk(self.SAMPLE_2, self.TIMESTAMP_2)
        self._chunker.add_chunk(self.SAMPLE_3, self.TIMESTAMP_3)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(time, self.TIMESTAMP_1)
        self.assertEquals(result, self.SAMPLE_1)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(time, self.TIMESTAMP_2)
        self.assertEquals(result, self.SAMPLE_2)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(time, self.TIMESTAMP_3)
        self.assertEquals(result, self.SAMPLE_3)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(result, None)
        self.assertEquals(time, None)

    def test_add_get_fragment(self):
        """
        Add some fragments of a string, then verify that value is stitched together
        """
        # Add a part of a sample
        self._chunker.add_chunk(self.FRAGMENT_1, self.TIMESTAMP_1)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(time, None)
        self.assertEquals(result, None)

        # add the rest of the sample
        self._chunker.add_chunk(self.FRAGMENT_2, self.TIMESTAMP_2)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(result, self.FRAGMENT_SAMPLE)
        self.assertEquals(time, self.TIMESTAMP_1)

    def test_add_multiple_in_one(self):
        """
        Test multiple data bits input in a single sample. They will ultimately
        need to be split apart.
        """
        self._chunker.add_chunk(self.MULTI_SAMPLE_1, self.TIMESTAMP_1)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(result, self.SAMPLE_1)
        self.assertEquals(time, self.TIMESTAMP_1)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(time, self.TIMESTAMP_1)
        self.assertEquals(result, self.SAMPLE_2)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(result, None)
        self.assertEquals(time, None)

    def test_funky_chunks(self):
        def funky_sieve(_):
            return [(3,6),(0,3)]

        self._chunker = StringChunker(funky_sieve)
        self._chunker.add_chunk("BarFoo", self.TIMESTAMP_1)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(result, "Bar")
        self.assertEquals(time, self.TIMESTAMP_1)
        (time, result) = self._chunker.get_next_data()
        self.assertEquals(result, "Foo")
        self.assertEquals(time, self.TIMESTAMP_1)

    def test_overlap(self):
        self.assertEqual([(0, 5)], StringChunker._prune_overlaps([(0, 5)]))
        self.assertEqual([], StringChunker._prune_overlaps([]))
        self.assertEqual([(0, 5)], StringChunker._prune_overlaps([(0, 5), (3, 6)]))
        self.assertEqual([(0, 5), (5, 7)], StringChunker._prune_overlaps([(0, 5), (5, 7), (6, 8)]))
