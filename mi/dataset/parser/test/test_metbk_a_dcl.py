#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_metbk_a_dcl
@file marine-integrations/mi/dataset/parser/test/test_metbk_a_dcl.py
@author Ronald Ronquillo
@brief Test code for a metbk_a_dcl data parser

In the following files, Metadata consists of 4 records.
There is 1 group of Sensor Data records for each set of metadata.

Files used for testing:

20140805.metbk2.log
  Metadata - 4 set,  Sensor Data - 1430 records

20140901.metbk2.log
  Metadata - 7 sets,  Sensor Data - 1061 records

20140902.metbk2.log
  Metadata - 0 sets,  Sensor Data - 863 records

20140917.metbk2.log
  Metadata - 0 sets,  Sensor Data - 904 records

20140805.metbk2_bad_sensor.log
    Metadata - 4 sets,  Sensor Data - 9 records

20140901.metbk2_no_sensor.log
    Metadata - 7 sets,  Sensor Data - 0 records

"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.metbk_a_dcl import MetbkADclParser
from mi.dataset.driver.metbk_a.dcl.metbk_dcl_a_driver import MODULE_NAME, \
    RECOVERED_PARTICLE_CLASS, TELEMETERED_PARTICLE_CLASS

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

log = get_logger()
RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'metbk_a', 'dcl', 'resource')

FILE_4_1430 = '20140805.metbk2.log'
FILE_4_9 = '20140805.metbk2_bad_sensor.log'
FILE_7_0 = '20140901.metbk2_no_sensor.log'
FILE_7_1061 = '20140901.metbk2.log'
FILE_0_863 = '20140902.metbk2.log'
FILE_0_904 = '20140917.metbk2.log'

YML_0_863 = 'rec_20140902.metbk2.yml'
YML_4_1430 = 'tel_20140805.metbk2.yml'
YML_7_1061 = 'tel_20140901.metbk2.yml'
YML_0_904 = 'rec_20140917.metbk2.yml'


RECORDS_FILE_4_1430 = 1430      # number of records expected
RECORDS_FILE_7_1061 = 1061      # number of records expected
RECORDS_FILE_0_863 = 863        # number of records expected
RECORDS_FILE_0_904 = 904        # number of records expected

TOTAL_RECORDS_FILE_7_0 = 7      # total number of records
TOTAL_RECORDS_FILE_4_9 = 60     # total number of records
RECORDS_FILE_4_9 = 9            # number of records expected
EXCEPTIONS_FILE_4_0 = 47        # number of exceptions expected


@attr('UNIT', group='mi')
class MetbkADclParserUnitTestCase(ParserUnitTestCase):
    """
    metbk_a_dcl Parser unit test suite
    """

    def create_parser(self, particle_class, file_handle):
        """
        This function creates a MetbkADcl parser for recovered data.
        """
        parser = MetbkADclParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            file_handle,
            self.exception_callback)
        return parser

    def open_file(self, filename):
        my_file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return my_file

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    def test_big_giant_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done in the
        tests below.
        """
        log.debug('===== START TEST BIG GIANT INPUT RECOVERED =====')
        in_file = self.open_file(FILE_0_863)
        parser = self.create_parser(RECOVERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles in this file.
        number_expected_results = RECORDS_FILE_0_863
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== START TEST BIG GIANT INPUT TELEMETERED =====')
        in_file = self.open_file(FILE_0_904)
        parser = self.create_parser(TELEMETERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles in this file.
        number_expected_results = RECORDS_FILE_0_904
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST BIG GIANT INPUT =====')
        
    def test_get_many(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST GET MANY RECOVERED =====')
        in_file = self.open_file(FILE_0_863)
        parser = self.create_parser(RECOVERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(RECORDS_FILE_0_863)

        # self.assertEqual(result, expected_particle)
        self.assert_particles(result, YML_0_863, RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

        log.debug('===== START TEST GET MANY TELEMETERED =====')
        in_file = self.open_file(FILE_7_1061)
        parser = self.create_parser(TELEMETERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(RECORDS_FILE_7_1061)
        self.assert_particles(result, YML_7_1061, RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

        log.debug('===== END TEST GET MANY =====')

    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that only the expected number of instrument particles are produced
        and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE_4_9)
        parser = self.create_parser(RECOVERED_PARTICLE_CLASS, in_file)

        # Try to get records and verify expected number of particles are returned.
        result = parser.get_records(TOTAL_RECORDS_FILE_4_9)

        self.assertEqual(len(result), RECORDS_FILE_4_9)
        self.assertEqual(len(self.exception_callback_value), EXCEPTIONS_FILE_4_0)

        in_file.close()

        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE_7_0)
        parser = self.create_parser(RECOVERED_PARTICLE_CLASS, in_file)

        # Try to get a record and verify that none are produced.
        result = parser.get_records(TOTAL_RECORDS_FILE_7_0)
        self.assertEqual(result, [])

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

        log.debug('===== END TEST NO SENSOR DATA =====')

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE RECOVERED =====')
        in_file = self.open_file(FILE_4_1430)
        parser = self.create_parser(TELEMETERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(RECORDS_FILE_4_1430)
        self.assert_particles(result, YML_4_1430, RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

        log.debug('===== END TEST SIMPLE =====')

    def test_bug_9692(self):
        """
        Test to verify change made to dcl_file_common.py works with DCL
        timestamps containing seconds >59
        """
        in_file = self.open_file("20140805.metbk2A.log")
        parser = self.create_parser(TELEMETERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(5)
        self.assertEqual(len(result), 4)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

