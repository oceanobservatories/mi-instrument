"""
@package mi.dataset.parser.test
@file mi/dataset/parser/test/test_fdchp_a_dcl.py
@author Emily Hahn
@brief Parser test for the fdchp series a instrument through a DCL
"""

import os
from nose.plugins.attrib import attr
from mi.core.exceptions import UnexpectedDataException, SampleException, SampleEncodingException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.fdchp_a_dcl import FdchpADclParser

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'fdchp_a', 'dcl', 'resource')


@attr('UNIT', group='mi')
class FdchpADclParserUnitTestCase(ParserUnitTestCase):

    def test_simple_telem(self):
        """
        Test a simple telemetered case
        """
        with open(os.path.join(RESOURCE_PATH, 'start.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(2)

            self.assert_particles(particles, "start_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_simple_recov(self):
        """
        Test a simple recovered case
        """
        with open(os.path.join(RESOURCE_PATH, 'start.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(2)

            self.assert_particles(particles, "start_recov.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_long_telem(self):
        """
        Test a longer telemetered case
        """
        with open(os.path.join(RESOURCE_PATH, 'long.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(8)

            self.assert_particles(particles, "long_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_long_recov(self):
        """
        Test a longer telemetered case
        """
        with open(os.path.join(RESOURCE_PATH, 'long.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(8)

            self.assert_particles(particles, "long_recov.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_full_recov(self):
        """
        Test with the full file, just compare that the number of particles is correct
        and there have been no exceptions
        """
        with open(os.path.join(RESOURCE_PATH, '20141215.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=False)

            # request a few more particles than are available, should only get the number in the file
            particles = parser.get_records(25)
            self.assertEquals(len(particles), 22)

            self.assertEqual(self.exception_callback_value, [])

    def test_unexpected(self):
        """
        Test with a file that has an unexpected data line in it
        Confirm we get all expected particles and call an exception for the unexpected data
        """
        with open(os.path.join(RESOURCE_PATH, 'unexpected_line.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(2)

            self.assert_particles(particles, "start_telem.yml", RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], UnexpectedDataException)

    def test_missing_vals(self):
        """
        Test that a file with missing data values is parsed correctly
        The first line is missing a value, but still has a comma separating for the right number of values
        The second line is missing a value, so it does not have the right number of values to be parsed
        Neither line should produce a partcle
        """
        with open(os.path.join(RESOURCE_PATH, 'missing_vals.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(2)

            # 1st particle is returned but has encoding error due to missing value
            self.assertEqual(len(particles), 0)

            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], SampleException)
            self.assertIsInstance(self.exception_callback_value[1], SampleException)

    def test_logs_ignored(self):
        """
        Test with a real file that has additional logs which should be ignored in it
        """
        # file was obtained from the acquisition server CP01CNSM deployment 2
        with open(os.path.join(RESOURCE_PATH, '20141119.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(4)

            self.assert_particles(particles, "20141119_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_instrument_stop_start(self):
        """
        Test with a real file where the instrument stops and starts in the middle
        """
        # file was obtained from the acquisition server CP01CNSM deployment 2
        with open(os.path.join(RESOURCE_PATH, '20141211.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(3)
            self.assertEquals(len(particles), 3)

            self.assertEqual(self.exception_callback_value, [])

    def test_bug_10002(self):
        """
        Redmine Ticket 10002 found files from early deployments had incorrect firmware
        that was omitting commas between some paramaters.  Still get 66 parameters but some
        only separated by space.
        Verify we get particles from files from early deployments
        """
        with open(os.path.join(RESOURCE_PATH, '20140912.fdchp.log'), 'r') as file_handle:
            parser = FdchpADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(30)

            self.assertEquals(len(particles), 23)

            self.assertEqual(self.exception_callback_value, [])
