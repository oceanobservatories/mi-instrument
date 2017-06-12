#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_dosta_abcdjm_dcl
@file marine-integrations/mi/dataset/parser/test/test_dosta_abcdjm_dcl.py
@author Steve Myerson
@brief Test code for a Dosta_abcdjm_dcl data parser

In the following files, Metadata consists of 4 records
and Garbled consist of 3 records.
There is 1 group of Sensor Data records for each set of metadata.

Files used for testing:

20000101.dosta0.log
  Metadata - 1 set,  Sensor Data - 0 records,  Garbled - 0,  Newline - \n

20010121.dosta1.log
  Metadata - 1 set,  Sensor Data - 21 records,  Garbled - 0,  Newline - \n

20020222.dosta2.log
  Metadata - 2 sets,  Sensor Data - 22 records,  Garbled - 0,  Newline - \r\n

20030314.dosta3.log
  Metadata - 3 sets,  Sensor Data - 14 records,  Garbled - 0,  Newline - \n

20041225.dosta4.log
  Metadata - 2 sets,  Sensor Data - 250 records,  Garbled - 0,  Newline - \n

20050103.dosta5.log
   Metadata - 1 set,  Sensor Data - 3 records,  Garbled - 1,  Newline - \n

20060207.dosta6.log
  Metadata - 2 sets,  Sensor Data - 7 records,  Garbled - 2,  Newline \r\n

20070114.dosta7.log
  This file contains a boatload of invalid sensor data records.  Newline - \r\n
   1. invalid year
   2. invalid month
   3. invalid day
   4. invalid hour
   5. invalid minute
   6. invalid second
   7. invalid product
   8. spaces instead of tabs
   9. a 2-digit serial number
  10. floating point number missing the decimal point
  11. serial number missing
  12. one of the floating point numbers missing
  13. Date in form YYYY-MM-DD
  14. time field missing milliseconds
  15. extra floating point number in sensor data
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import UnexpectedDataException
from mi.core.log import get_logger
from mi.dataset.driver.dosta_abcdjm.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.dosta_abcdjm_dcl import \
    DostaAbcdjmDclRecoveredParser, \
    DostaAbcdjmDclTelemeteredParser
from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

FILE0 = '20000101.dosta0.log'
FILE1 = '20010121.dosta1.log'
FILE3 = '20030314.dosta3.log'
FILE4 = '20041225.dosta4.log'
FILE5 = '20050103.dosta5.log'
FILE6 = '20060207.dosta6.log'
FILE7 = '20070114.dosta7.log'


@attr('UNIT', group='mi')
class DostaAbcdjmDclParserUnitTestCase(ParserUnitTestCase):

    def create_rec_parser(self, file_handle):
        """
        This function creates a DostaAbcdjmDcl parser for recovered data.
        """
        parser = DostaAbcdjmDclRecoveredParser(
            file_handle, self.exception_callback)
        return parser

    def create_tel_parser(self, file_handle):
        """
        This function creates a DostaAbcdjmDcl parser for telemetered data.
        """
        parser = DostaAbcdjmDclTelemeteredParser(
            file_handle, self.exception_callback)
        return parser

    def open_file(self, filename):
        in_file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return in_file

    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def test_big_giant_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done during
        integration and qualification testing.
        File used for this test has 500 total particles.
        """
        log.debug('===== START TEST BIG GIANT INPUT RECOVERED =====')
        in_file = self.open_file(FILE4)
        parser = self.create_rec_parser(in_file)

        number_expected_results = 500

        # In a single read, get all particles in this file.
        result = parser.get_records(number_expected_results)

        self.assertEqual(len(result), number_expected_results)
        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

        log.debug('===== START TEST BIG GIANT INPUT TELEMETERED =====')
        in_file = self.open_file(FILE4)
        parser = self.create_tel_parser(in_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(number_expected_results)

        self.assertEqual(len(result), number_expected_results)
        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

        log.debug('===== END TEST BIG GIANT INPUT =====')

    def test_get_many(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST GET MANY RECOVERED =====')

        expected_particle = 40
        in_file = self.open_file(FILE3)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(expected_particle)

        self.assertEqual(len(result), expected_particle)
        self.assertEqual(self.exception_callback_value, [])

        in_file.close()

        log.debug('===== START TEST GET MANY TELEMETERED =====')
        in_file = self.open_file(FILE3)
        parser = self.create_tel_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(expected_particle)

        self.assertEqual(len(result), expected_particle)
        self.assertEqual(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== END TEST GET MANY =====')

    def test_invalid_metadata_records(self):
        """
        Read data from a file containing invalid metadata records as well
        as valid metadata records and sensor data records.
        Verify that the sensor data records can be read correctly
        and that invalid metadata records are detected.
        File 5 has 3 invalid metadata records.
        File 6 has 6 invalid metadata records.
        """
        log.debug('===== START TEST INVALID METADATA RECOVERED =====')

        expected_particle = 1

        in_file = self.open_file(FILE5)
        parser = self.create_rec_parser(in_file)

        # Get record and verify.
        result = parser.get_records(expected_particle)

        self.assertEqual(len(result), expected_particle)
        self.assertEqual(len(self.exception_callback_value), 3)
        in_file.close()

        self.exception_callback_value = []  # reset exceptions

        log.debug('===== START TEST INVALID METADATA TELEMETERED =====')
        in_file = self.open_file(FILE6)
        parser = self.create_tel_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(expected_particle)

        self.assertEqual(len(result), expected_particle)
        self.assertEqual(len(self.exception_callback_value), 6)

        in_file.close()
        log.debug('===== END TEST INVALID METADATA =====')

    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that no instrument particles are produced
        and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE7)
        parser = self.create_rec_parser(in_file)

        expected_exceptions = 15

        # Try to get records and verify that none are returned.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        self.assertEqual(len(self.exception_callback_value), expected_exceptions)

        in_file.close()
        self.exception_callback_value = []  # reset exceptions

        log.debug('===== START TEST INVALID SENSOR DATA TELEMETERED =====')
        in_file = self.open_file(FILE7)
        parser = self.create_tel_parser(in_file)

        # Try to get records and verify that none are returned.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        self.assertEqual(len(self.exception_callback_value), expected_exceptions)

        in_file.close()
        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE0)
        parser = self.create_rec_parser(in_file)

        # Try to get a record and verify that none are produced.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        self.assertEquals(self.exception_callback_value, [])
        in_file.close()

        log.debug('===== START TEST NO SENSOR DATA TELEMETERED =====')
        in_file = self.open_file(FILE0)
        parser = self.create_tel_parser(in_file)

        # Try to get a record and verify that none are produced.
        result = parser.get_records(1)

        self.assertEqual(result, [])
        self.assertEquals(self.exception_callback_value, [])
        in_file.close()

        log.debug('===== END TEST SENSOR DATA =====')

    def test_many_with_yml(self):
        """
        Read a file and verify that all records can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST MANY WITH YML RECOVERED =====')

        num_particles = 21

        in_file = self.open_file(FILE1)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(num_particles)

        log.debug("Num particles: %d", len(particles))

        self.assert_particles(particles, "rec_20010121.dosta1.yml", RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== START TEST MANY WITH YML TELEMETERED =====')

        in_file = self.open_file(FILE1)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(num_particles)

        log.debug("Num particles: %d", len(particles))

        self.assert_particles(particles, "tel_20010121.dosta1.yml", RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== END TEST MANY WITH YML =====')

    def test_Bug_4433(self):
        """
        Read a file and verify that all records can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """

        num_particles = 10000

        in_file = self.open_file('20150330.dosta1.log')
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(num_particles)

        log.debug("Num particles: %d", len(particles))

        # make sure we only get UnexpectedDataException
        for exception in self.exception_callback_value:
            self.assertIsInstance(exception, UnexpectedDataException)

        in_file.close()
