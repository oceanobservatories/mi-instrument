#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_optaa_dj_dcl
@file marine-integrations/mi/dataset/parser/test/test_optaa_dj_dcl.py
@author Steve Myerson (Raytheon)
@brief Test code for a optaa_dj_dcl data parser

Files used for testing:

20010314_010314.optaa1.log
  Records - 3, Measurements - 1, 3, 14

20020704_020704.optaa2.log
  Records - 5, Measurements - 0, 2, 7, 4, 27

20031031_031031.optaa3.log
  Records - 3, Measurements - 50, 255, 125

20041220_041220.optaa4.log
  Records - 4, Measurements - 255, 175, 150, 255

20050401_050401.optaa5.log
  Records - 3, Measurements - 1, 2, 3
  All records have a checksum error - No particles will be produced

20061225_061225.optaa6.log
  Records - 10, Measurements - 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import DatasetParserException, RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.optaa_dj.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.optaa_dj_dcl import OptaaDjDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


MODULE_NAME = 'mi.dataset.parser.optaa_dj_dcl'

FILE1 = '20010314_010314.optaa1.log'
FILE2 = '20020704_020704.optaa2.log'
FILE3 = '20031031_031031.optaa3.log'
FILE4 = '20041220_041220.optaa4.log'
FILE5 = '20050401_050401.optaa5.log'
FILE6 = '20061225_061225.optaa6.log'
FILE_BAD_FILENAME = '20190401.optaa19.log'


@attr('UNIT', group='mi')
class OptaaDjDclParserUnitTestCase(ParserUnitTestCase):
    """
    optaa_dj_dcl Parser unit test suite
    """
    def create_rec_parser(self, file_handle, filename):
        """
        This function creates a OptaaDjDcl parser for recovered data.
        """
        return OptaaDjDclParser(self.config,
                                file_handle, self.exception_callback, filename, False)

    def create_tel_parser(self, file_handle, filename):
        """
        This function creates a OptaaDjDcl parser for telemetered data.
        """
        return OptaaDjDclParser(self.config,
                                file_handle, self.exception_callback, filename, True)

    def open_file(self, filename):
        return open(os.path.join(RESOURCE_PATH, filename), mode='rb')

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.maxDiff = None

    def test_bad_filename(self):
        """
        This test verifies that a DatasetParserException occurs if the filename
        is bad.
        """
        log.debug('===== START TEST BAD FILENAME =====')
        in_file = self.open_file(FILE_BAD_FILENAME)

        with self.assertRaises(DatasetParserException):
            self.create_rec_parser(in_file, FILE_BAD_FILENAME)

        with self.assertRaises(DatasetParserException):
            self.create_tel_parser(in_file, FILE_BAD_FILENAME)

        log.debug('===== END TEST BAD FILENAME =====')

    def test_big_giant_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done during
        integration and qualification testing.
        """
        log.debug('===== START TEST BIG GIANT INPUT RECOVERED =====')
        in_file = self.open_file(FILE6)
        parser = self.create_rec_parser(in_file, FILE6)

        # In a single read, get all particles in this file.
        number_expected_results = 11
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

        log.debug('===== START TEST BIG GIANT INPUT TELEMETERED =====')
        in_file = self.open_file(FILE4)
        parser = self.create_tel_parser(in_file, FILE4)

        # In a single read, get all particles in this file.
        number_expected_results = 5
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

        log.debug('===== END TEST BIG GIANT INPUT =====')

    def test_checksum_errors(self):
        """
        This test verifies that records containing checksum errors
        are detected and that particles are not generated.
        """
        log.debug('===== START TEST CHECKSUM ERRORS =====')
        in_file = self.open_file(FILE5)
        parser = self.create_rec_parser(in_file, FILE5)

        # Try to get a record and verify that none are produced.
        # Verify that the correct number of checksum errors are detected.
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(len(self.exception_callback_value), 3)
        for exception in self.exception_callback_value:
            self.assertIsInstance(exception, RecoverableSampleException)
        in_file.close()

        # reset self.exception_callback_value
        self.exception_callback_value = []

        in_file = self.open_file(FILE5)
        parser = self.create_tel_parser(in_file, FILE5)

        # Try to get a record and verify that none are produced.
        # Verify that the correct number of checksum errors are detected.
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(len(self.exception_callback_value), 3)
        for exception in self.exception_callback_value:
            self.assertIsInstance(exception, RecoverableSampleException)
        in_file.close()

        log.debug('===== END TEST CHECKSUM ERRORS =====')

    def test_simple(self):

        with self.open_file('20010314_010314.optaa1.log') as in_file:

            parser = self.create_rec_parser(in_file, FILE1)
            # ask for 10, should get 4
            result = parser.get_records(10)

            self.assertEqual(len(result), 4)
            self.assert_particles(result, 'rec_20010314_010314.optaa1.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_with_omc_file(self):

        with self.open_file('20131208_110016.optaa1.log') as in_file:

            parser = self.create_rec_parser(in_file, FILE1)
            # ask for 200, should get 180
            result = parser.get_records(200)

            self.assertEqual(len(result), 180)
            self.assertEqual(self.exception_callback_value, [])

