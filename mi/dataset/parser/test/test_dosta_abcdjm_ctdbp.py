#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_dosta_abcdjm_ctdbp.py
@author Jeff Roy
@brief Test code for dosta_abcdjm_ctdbp data parser

Files used for testing:

"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.driver.dosta_abcdjm.ctdbp.resource import RESOURCE_PATH
from mi.dataset.parser.dosta_abcdjm_ctdbp import DostaAbcdjmCtdbpParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()
MODULE_NAME = 'mi.dataset.parser.dosta_abcdjm_ctdbp'


@attr('UNIT', group='mi')
class DostaAbcdjmCtdbpParserUnitTestCase(ParserUnitTestCase):
    """
    dosta_abcdjm_ctdbp Parser unit test suite
    """

    def test_simple(self):
        """
        Simple test to verify that records are successfully read and parsed from a data file
        """
        log.debug('===== START SIMPLE TEST =====')

        path = RESOURCE_PATH
        log.info(path)

        # test with Endurance data
        with open(os.path.join(RESOURCE_PATH, 'simple_test.log'), 'rU') as file_handle:

            parser = DostaAbcdjmCtdbpParser(file_handle,
                                            self.exception_callback)

            # In a single read, get all particles in this file.
            number_expected_results = 5
            result = parser.get_records(number_expected_results)
            self.assertEqual(len(result), number_expected_results)

            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END SIMPLE TEST =====')

    def test_verify_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')

        # test with Endurance data
        with open(os.path.join(RESOURCE_PATH, 'data1.log'), 'rU') as file_handle:

            parser = DostaAbcdjmCtdbpParser(file_handle,
                                            self.exception_callback)

            # In a single read, get all particles in this file.
            number_expected_results = 143
            result = parser.get_records(number_expected_results)
            self.assert_particles(result, 'data1.yml', RESOURCE_PATH)

            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END YAML TEST =====')

    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that no instrument particles are produced
        and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')

        # test with Endurance data
        with open(os.path.join(RESOURCE_PATH, 'invalid_data.log'), 'rU') as file_handle:

            parser = DostaAbcdjmCtdbpParser(file_handle,
                                            self.exception_callback)

            # Try to get records and verify that none are returned.
            result = parser.get_records(1)
            self.assertEqual(result, [])
            self.assertEqual(len(self.exception_callback_value), 11)

        self.exception_callback_value = []  # reset exceptions

        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')

        # test with Endurance data
        with open(os.path.join(RESOURCE_PATH, 'no_sensor_data.log'), 'rU') as file_handle:

            parser = DostaAbcdjmCtdbpParser(file_handle,
                                            self.exception_callback)

            # Try to get a record and verify that none are produced.
            result = parser.get_records(1)
            self.assertEqual(result, [])

            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST NO SENSOR DATA =====')

