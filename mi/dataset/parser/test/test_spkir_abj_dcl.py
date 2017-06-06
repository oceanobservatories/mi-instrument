#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_spkir_abj_dcl
@file marine-integrations/mi/dataset/parser/test/test_spkir_abj_dcl.py
@author Steve Myerson
@brief Test code for a spkir_abj_dcl data parser

In the following files, Metadata consists of 4 records.
There is 1 group of Sensor Data records for each set of metadata.

Files used for testing:

20010101.spkir1.log
  Metadata - 1 set,  Sensor Data - 0 records

20020113.spkir2.log
  Metadata - 1 set,  Sensor Data - 13 records

20030208.spkir3.log
  Metadata - 2 sets,  Sensor Data - 8 records

20040305.spkir4.log
  Metadata - 3 sets,  Sensor Data - 5 records

20050403.spkir5.log
  Metadata - 4 sets,  Sensor Data - 3 records

20061220.spkir6.log
  Metadata - 1 set,  Sensor Data - 400 records

20071225.spkir7.log
  Metadata - 2 sets,  Sensor Data - 250 records

20080401.spkir8.log
  This file contains a boatload of invalid sensor data records.
  See metadata in file for a list of the errors.
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.spkir_abj.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.spkir_abj_dcl import \
    SpkirAbjDclRecoveredParser, \
    SpkirAbjDclTelemeteredParser

from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


FILE1 = '20010101.spkir1.log'
FILE2 = '20020113.spkir2.log'
FILE3 = '20030208.spkir3.log'
FILE4 = '20040305.spkir4.log'
FILE5 = '20050403.spkir5.log'
FILE6 = '20061220.spkir6.log'
FILE7 = '20071225.spkir7.log'
FILE8 = '20080401.spkir8.log'

EXPECTED_FILE6 = 400
EXPECTED_FILE7 = 500

MODULE_NAME = 'mi.dataset.parser.spkir_abj_dcl'


@attr('UNIT', group='mi')
class SpkirAbjDclParserUnitTestCase(ParserUnitTestCase):
    
    def create_rec_parser(self, file_handle):
        """
        This function creates a SpkirAbjDcl parser for recovered data.
        """
        parser = SpkirAbjDclRecoveredParser(self.rec_config,
                                            file_handle,
                                            self.rec_exception_callback)
        return parser

    def create_tel_parser(self, file_handle):
        """
        This function creates a SpkirAbjDcl parser for telemetered data.
        """
        parser = SpkirAbjDclTelemeteredParser(self.tel_config,
                                              file_handle,
                                              self.tel_exception_callback)
        return parser

    def open_file(self, filename):
        fid = open(os.path.join(RESOURCE_PATH, filename), mode='rb')
        return fid

    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def rec_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.rec_exception_callback_value = exception
        self.rec_exceptions_detected += 1

    def tel_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.tel_exception_callback_value = exception
        self.tel_exceptions_detected += 1

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.tel_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.rec_state_callback_value = None
        self.rec_file_ingested_value = False
        self.rec_publish_callback_value = None
        self.rec_exception_callback_value = None
        self.rec_exceptions_detected = 0

        self.tel_state_callback_value = None
        self.tel_file_ingested_value = False
        self.tel_publish_callback_value = None
        self.tel_exception_callback_value = None
        self.tel_exceptions_detected = 0

        self.maxDiff = None

    def test_big_giant_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done during
        integration and qualification testing.
        """
        log.debug('===== START TEST BIG GIANT INPUT RECOVERED =====')
        in_file = self.open_file(FILE6)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = EXPECTED_FILE6
        result = parser.get_records(number_expected_results)

        self.assertEqual(len(result), number_expected_results)
        in_file.close()
        self.assertEqual(self.rec_exception_callback_value, None)

        log.debug('===== START TEST BIG GIANT INPUT TELEMETERED =====')
        in_file = self.open_file(FILE7)
        parser = self.create_tel_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = EXPECTED_FILE7
        result = parser.get_records(number_expected_results)

        self.assertEqual(len(result), number_expected_results)
        in_file.close()
        self.assertEqual(self.tel_exception_callback_value, None)

        log.debug('===== END TEST BIG GIANT INPUT =====')
        
    def test_get_many(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST GET MANY RECOVERED =====')

        expected_particle = 12

        in_file = self.open_file(FILE5)
        parser = self.create_rec_parser(in_file)
        # In a single read, get all particles for this file.
        result = parser.get_records(expected_particle)

        self.assertEqual(len(result), expected_particle)
        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== START TEST GET MANY TELEMETERED =====')
        in_file = self.open_file(FILE4)
        parser = self.create_tel_parser(in_file)
        # In a single read, get all particles for this file.
        result = parser.get_records(expected_particle)

        self.assertEqual(len(result), expected_particle)
        self.assertEqual(self.tel_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST GET MANY =====')

    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that no instrument particles are produced.
        """
        log.debug('===== START TEST INVALID SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE8)
        parser = self.create_rec_parser(in_file)
        # Try to get records and verify that none are returned.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        in_file.close()

        log.debug('===== START TEST INVALID SENSOR DATA TELEMETERED =====')
        in_file = self.open_file(FILE8)
        parser = self.create_tel_parser(in_file)
        # Try to get records and verify that none are returned.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        in_file.close()

        log.debug('===== END TEST INVALID SENSOR DATA =====')
        
    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE1)
        parser = self.create_rec_parser(in_file)
        # Try to get a record and verify that none are produced.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== START TEST NO SENSOR DATA TELEMETERED =====')
        in_file = self.open_file(FILE1)
        parser = self.create_tel_parser(in_file)
        # Try to get a record and verify that none are produced.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        self.assertEqual(self.tel_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST SENSOR DATA =====')
        
    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE RECOVERED =====')
        in_file = self.open_file(FILE2)
        parser = self.create_rec_parser(in_file)
        # Get record and verify.
        result = parser.get_records(1)

        self.assertEqual(len(result), 1)
        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== START TEST SIMPLE TELEMETERED =====')
        in_file = self.open_file(FILE3)
        parser = self.create_tel_parser(in_file)
        # Get record and verify.
        result = parser.get_records(1)

        self.assertEqual(len(result), 1)
        self.assertEqual(self.tel_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST SIMPLE =====')

    def test_many_with_yml(self):
        """
        Read a file and verify that all records can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST MANY WITH YML RECOVERED =====')

        num_particles = 13

        in_file = self.open_file(FILE2)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(num_particles)

        log.debug("Num particles: %d", len(particles))

        self.assert_particles(particles, "rec_20020113.spkir2.yml", RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== START TEST MANY WITH YML TELEMETERED =====')

        in_file = self.open_file(FILE2)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(num_particles)

        log.debug("Num particles: %d", len(particles))

        self.assert_particles(particles, "tel_20020113.spkir2.yml", RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== END TEST MANY WITH YML =====')
