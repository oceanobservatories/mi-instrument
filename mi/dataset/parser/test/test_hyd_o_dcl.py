"""
@package mi.dataset.parser.test.test_wavss_a_dcl
@file mi/dataset/parser/test/test_hyd_o_dcl.py
@author Emily Hahn
@brief A test parser for the hydrogen series o instrument through a DCL
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import os
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.hyd_o_dcl import HydODclParser

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'hyd_o', 'dcl', 'resource')

@attr('UNIT', group='mi')
class HydODclParserUnitTestCase(ParserUnitTestCase):

    def test_simple_telem(self):
        """
        Test a simple telemetered case
        """
        with open(os.path.join(RESOURCE_PATH, 'first.hyd1.log'), 'rU') as file_handle:
            parser = HydODclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(8)

            self.assert_particles(particles, "first_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_simple_recov(self):
        """
        Test a simple recovered case
        """
        with open(os.path.join(RESOURCE_PATH, 'first.hyd1.log'), 'rU') as file_handle:
            parser = HydODclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(8)

            self.assert_particles(particles, "first_recov.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_long_telem(self):
        """
        Test with the full file and confirm the correct number of particles occurs
        """
        with open(os.path.join(RESOURCE_PATH, '20140904.hyd1.log'), 'rU') as file_handle:
            parser = HydODclParser(file_handle, self.exception_callback, is_telemetered=True)

            # there are 813 lines in the file, but 70 are ignored so we should get 743 particles
            particles = parser.get_records(813)
            self.assertEquals(len(particles), 743)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_format_telem(self):
        """
        Test a file with two lines not formatted properly
        """
        with open(os.path.join(RESOURCE_PATH, 'bad_format.hyd1.log'), 'rU') as file_handle:
            parser = HydODclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(10)
            self.assertEquals(len(particles), 8)

            # particles in the file should still match the good data in first.hyd1.log, just skipping the bad lines
            self.assert_particles(particles, "first_telem.yml", RESOURCE_PATH)

            # confirm we get two exceptions, one for each bad line
            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], SampleException)
            self.assertIsInstance(self.exception_callback_value[1], SampleException)

    def test_log_only(self):
        """
        Test with a file that only contains dcl logs, no data, and confirm no particles are returned
        """
        with open(os.path.join(RESOURCE_PATH, 'log_only.hyd1.log'), 'rU') as file_handle:
            parser = HydODclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(10)
            self.assertEquals(len(particles), 0)

            self.assertEqual(self.exception_callback_value, [])