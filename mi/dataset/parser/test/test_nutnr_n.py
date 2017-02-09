"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_nutnr_n.py
@author Emily Hahn
@brief Test code for a nutnr_n data parser
"""
import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import SampleException
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH, ParserUnitTestCase
from mi.dataset.parser.nutnr_n import NutnrNParser

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'nutnr_n', 'resource')

@attr('UNIT', group='mi')
class NutnrNParserUnitTestCase(ParserUnitTestCase):

    def test_simple(self):
        """
        Simple test to confirm particles can be read and compared to those in the .yml
        """
        with open(os.path.join(RESOURCE_PATH, 'suna_short.sun'), 'rb') as file_handle:
            parser = NutnrNParser(file_handle, self.exception_callback)

            particles = parser.get_records(2)

            self.assert_particles(particles, "suna_short.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_long(self):
        """
        Longer simple test to confirm particles can be read and compared to those in the .yml
        """
        with open(os.path.join(RESOURCE_PATH, 'suna_long.sun'), 'rb') as file_handle:
            parser = NutnrNParser(file_handle, self.exception_callback)

            particles = parser.get_records(8)

            self.assert_particles(particles, "suna_long.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_full(self):
        """
        Read the entire very long original file, confirm the number of particles and exceptions that occur is correct
        """
        with open(os.path.join(RESOURCE_PATH, 'suna.sun'), 'rb') as file_handle:
            parser = NutnrNParser(file_handle, self.exception_callback)
            log.info("Starting Long Test, Takes a minute")

            particles = parser.get_records(20000)
            self.assertEquals(len(particles), 20000)

            particles2 = parser.get_records(20000)
            self.assertEquals(len(particles2), 20000)

            particles3 = parser.get_records(20000)
            self.assertEquals(len(particles3), 20000)

            particles4 = parser.get_records(15000)
            self.assertEquals(len(particles4), 13245)

            self.assertEqual(len(self.exception_callback_value), 16)
            for i in range(0, len(self.exception_callback_value)):
                self.assertIsInstance(self.exception_callback_value[i], SampleException)

    def test_empty(self):
        """
        Test that an empty file doesn't produce any exceptions
        """
        with open(os.path.join(RESOURCE_PATH, 'empty.sun'), 'rb') as file_handle:
            parser = NutnrNParser(file_handle, self.exception_callback)

            particles = parser.get_records(1)
            self.assertEquals(len(particles), 0)

            self.assertEquals(self.exception_callback_value, [])

    def test_unknown_at_start(self):
        """
        Test unknown data at the start of the file, confirm it is causes an exception, then returns particles
        """
        with open(os.path.join(RESOURCE_PATH, 'suna_unknown_start.sun'), 'rb') as file_handle:
            parser = NutnrNParser(file_handle, self.exception_callback)

            particles = parser.get_records(2)

            self.assert_particles(particles, "suna_short.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], SampleException)

    def test_bad_time(self):
        """
        Test a file with a bad timestamp has an exception and doesn't return that particle
        """
        with open(os.path.join(RESOURCE_PATH, 'suna_bad_time.sun'), 'rb') as file_handle:
            parser = NutnrNParser(file_handle, self.exception_callback)

            particles = parser.get_records(2)
            # 2 particles in file, but 1 has bad time, so should only get 1 back
            self.assertEquals(len(particles), 1)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], SampleException)

    def test_bad_checksum(self):
        """
        Test a file with a bad checksum and confirm the correct particles are returned and the exception occurs
        """
        with open(os.path.join(RESOURCE_PATH, 'suna_bad_checksum.sun'), 'rb') as file_handle:
            parser = NutnrNParser(file_handle, self.exception_callback)

            particles = parser.get_records(8)
            # one has the checksum error
            self.assertEquals(len(particles), 7)

            # make sure we get the rest of the particles as expected
            self.assert_particles(particles, "suna_bad_checksum.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], SampleException)
