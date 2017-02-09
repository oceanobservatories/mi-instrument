"""
@package mi.dataset.parser.test
@file mi/dataset/parser/test/test_fdchp_a.py
@author Emily Hahn
@brief A test parser for the fdchp series a instrument directly recovered
"""

import os
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.fdchp_a import FdchpAParser

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'fdchp_a', 'resource')


@attr('UNIT', group='mi')
class FdchpAParserUnitTestCase(ParserUnitTestCase):

    def test_simple(self):
        """
        Test a simple case
        """
        # the short file contains the first 10 records from fdchp_20141201_000000.dat
        with open(os.path.join(RESOURCE_PATH, 'fdchp_1201_short.dat'), 'rb') as file_handle:
            parser = FdchpAParser(file_handle, self.exception_callback)

            particles = parser.get_records(10)

            self.assert_particles(particles, "fdchp_1201_short.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_simple_2(self):
        """
        Test another simple case with a file that has values for wind, the 1201 file has all the same values
        """
        # the short file contains the first 5 records from fdchp_20141219_201000.dat
        with open(os.path.join(RESOURCE_PATH, 'fdchp_1219_short.dat'), 'rb') as file_handle:
            parser = FdchpAParser(file_handle, self.exception_callback)

            particles = parser.get_records(5)

            self.assert_particles(particles, "fdchp_1219_short.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_long(self):
        """
        Test with a full file and confirm the correct number of particles occurs and there are no errors
        """
        with open(os.path.join(RESOURCE_PATH, 'fdchp_20141201_000000.dat'), 'rb') as file_handle:
            parser = FdchpAParser(file_handle, self.exception_callback)

            # request a few extra particles, there are 12011 in the file
            particles = parser.get_records(12020)
            self.assertEquals(len(particles), 12011)

            self.assertEqual(self.exception_callback_value, [])

        # check a second file
        with open(os.path.join(RESOURCE_PATH, 'fdchp_20141218_180000.dat'), 'rb') as file_handle:
            parser = FdchpAParser(file_handle, self.exception_callback)

            # there are 12011 in the file
            particles = parser.get_records(12011)
            self.assertEquals(len(particles), 12011)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_size(self):
        """
        Test that a file with a bad size (not evenly divisible by the record size) does not return any records and
        raises a sample exception
        """
        with self.assertRaises(SampleException):
            file_handle = open(os.path.join(RESOURCE_PATH, 'fdchp_bad_size.dat'), 'rb')
            parser = FdchpAParser(file_handle, self.exception_callback)

            particles = parser.get_records(10)
            # confirm no particles have been returned
            self.assertEquals(len(particles), 0)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_size_real(self):
        """
        Test with a real file that has a bad size
        """
        with self.assertRaises(SampleException):
            file_handle = open(os.path.join(RESOURCE_PATH, 'fdchp_20141219_201000.dat'), 'rb')
            parser = FdchpAParser(file_handle, self.exception_callback)

            particles = parser.get_records(10)
            # confirm no particles have been returned
            self.assertEquals(len(particles), 0)

            self.assertEqual(self.exception_callback_value, [])
