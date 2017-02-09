"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_nutnr_m.py
@author Emily Hahn
@brief Test code for the nutnr series m data parser
"""
import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import SampleException
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH, ParserUnitTestCase
from mi.dataset.parser.nutnr_m import NutnrMParser

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'nutnr_m', 'resource')

@attr('UNIT', group='mi')
class NutnrMParserUnitTestCase(ParserUnitTestCase):
    """
    Other error checking was performed in test_nutnr_n which uses the same common parser suna_common.py and still
    applies to this parser.
    """

    def test_simple(self):
        """
        Simple test to confirm particles can be read and compared to those in the .yml
        """
        with open(os.path.join(RESOURCE_PATH, 'nl_short.bin'), 'rb') as file_handle:
            parser = NutnrMParser(file_handle, self.exception_callback)

            particles = parser.get_records(3)

            self.assert_particles(particles, "nl_short.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_long(self):
        """
        Longer simple test to confirm particles can be read and compared to those in the .yml
        """
        with open(os.path.join(RESOURCE_PATH, 'nl_long.bin'), 'rb') as file_handle:
            parser = NutnrMParser(file_handle, self.exception_callback)

            particles = parser.get_records(10)

            self.assert_particles(particles, "nl_long.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_full(self):
        """
        Read the entire original file, confirm the number of particles and exceptions that occur is correct
        """
        with open(os.path.join(RESOURCE_PATH, 'nl181437.bin'), 'rb') as file_handle:
            parser = NutnrMParser(file_handle, self.exception_callback)

            # request more particles than are available
            particles = parser.get_records(50)

            # confirm we get the expected 29 particles, the last particles's header was corrupted
            self.assertEquals(len(particles), 29)

            self.assertEquals(len(self.exception_callback_value), 1)

            self.assertIsInstance(self.exception_callback_value[0], SampleException)

        # clear the exception callback for the new file
        self.exception_callback_value = []

        with open(os.path.join(RESOURCE_PATH, 'nl181450.bin'), 'rb') as file_handle:
            parser = NutnrMParser(file_handle, self.exception_callback)

            # request more particles than are available
            particles = parser.get_records(100)
            self.assertEquals(len(particles), 88)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_checksum(self):
        """
        Test a file with a bad checksum and confirm the correct particles are returned and the exception occurs
        """
        with open(os.path.join(RESOURCE_PATH, 'nl_bad_checksum.bin'), 'rb') as file_handle:
            parser = NutnrMParser(file_handle, self.exception_callback)

            particles = parser.get_records(3)
            # first one has the checksum error
            self.assertEquals(len(particles), 2)

            # make sure we get the rest of the particles as expected
            self.assert_particles(particles, "nl_bad_checksum.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], SampleException)
