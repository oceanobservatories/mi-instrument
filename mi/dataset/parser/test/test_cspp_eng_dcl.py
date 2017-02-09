#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_cspp_eng_dcl.py
@author Jeff Roy
@brief Test code for a cspp_eng_dcl data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.driver.cspp_eng.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.cspp_eng_dcl import CsppEngDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()
MODULE_NAME = 'mi.dataset.parser.cspp_eng_dcl'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class CsppEngDclParserUnitTestCase(ParserUnitTestCase):
    """
    cspp_eng_dcl Parser unit test suite
    """

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The file all_responses contains at least one of all expected
        NMEA responses copied from various sample logs.
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'all_responses.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assert_particles(particles, 'all_responses.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_bad_data(self):
        """
        Verify RecoverableSampleException is raised when a malformed line is encountered
        and processing continues parsing valid lines.
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'bad_data.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assert_particles(particles, 'all_responses.yml', RESOURCE_PATH)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    def test_bad_checksum(self):
        """
        Verify RecoverableSampleException is raised when a bad checksum is encountered
        and processing continues parsing valid lines.
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'bad_checksum.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assertEqual(len(particles), 3)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    def test_bad_nmea_count(self):
        """
        Verify RecoverableSampleException is raised when a nmea record is encountered
        that does not contain the expected number of fields
        and processing continues parsing valid lines.
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'bad_count.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assertEqual(len(particles), 3)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    def test_just_header(self):
        """
        Verify a file containing just the header is parsed correctly
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'just_header.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assertEqual(len(particles), 1)
            self.assertEqual(self.exception_callback_value, [])

    def test_partial_header(self):
        """
        Verify a file containing just the header is parsed correctly
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'partial_header.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assertEqual(len(particles), 1)
            self.assertEqual(self.exception_callback_value, [])
            self.assertEqual(particles[0]._values[1].get('value'), None)

    def test_no_date(self):
        """
        Verify that a file that does not contain a DATE record
        correctly creates the partially populated Data particle
        with alternate timestamp.
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'no_date.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assert_particles(particles, 'no_date.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_no_response(self):
        """
        Verify a file containing no responses is parsed correctly
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'no_response.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)
            self.assertEqual(len(particles), 0)
            self.assertEqual(self.exception_callback_value, [])

