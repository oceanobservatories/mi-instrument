#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_flort_dj_dcl
@file marine-integrations/mi/dataset/parser/test/test_flort_dj_dcl.py
@author Steve Myerson
@brief Test code for a flort_dj_dcl data parser

In the following files, Metadata consists of 4 records.
There is 1 group of Sensor Data records for each set of metadata.

Files used for testing:

20010101.flort1.log
  Metadata - 1 set,  Sensor Data - 0 records

20020215.flort2.log
  Metadata - 2 sets,  Sensor Data - 15 records

20030413.flort3.log
  Metadata - 4 sets,  Sensor Data - 13 records

20040505.flort4.log
  Metadata - 5 sets,  Sensor Data - 5 records

20050406.flort5.log
  Metadata - 4 sets,  Sensor Data - 6 records

20061220.flort6.log
  Metadata - 1 set,  Sensor Data - 300 records

20071225.flort7.log
  Metadata - 2 sets,  Sensor Data - 200 records

20080401.flort8.log
  This file contains a boatload of invalid sensor data records.
  See metadata in file for a list of the errors.
  20 metadata records, 47 sensor data records

Change History:

    Date         Ticket#    Engineer     Description
    ------------ ---------- -----------  --------------------------
    5/16/17      #9809      janeenP      Added functionality for testing
                                         combined CTDBP with FLORT
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.flort_dj.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.flort_dj_dcl import \
    FlortDjDclParser

from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()
MODULE_NAME = 'mi.dataset.parser.flort_dj_dcl'
PARSER_NAME = 'FlortDjDclParser'

FILE1 = '20010101.flort1.log'
FILE2 = '20020215.flort2.log'
FILE3 = '20030413.flort3.log'
FILE4 = '20040505.flort4.log'
FILE5 = '20050406.flort5.log'
FILE6 = '20061220.flort6.log'
FILE7 = '20071225.flort7.log'
FILE8 = '20080401.flort8.log'

RECORDS_FILE6 = 300      # number of records expected
RECORDS_FILE7 = 400      # number of records expected
EXCEPTIONS_FILE8 = 47    # number of exceptions expected

INSTRUMENT_PARTICLE_MAP = [
    ('measurement_wavelength_beta', 9,   int),
    ('raw_signal_beta',             10,  int),
    ('measurement_wavelength_chl',  11,  int),
    ('raw_signal_chl',              12,  int),
    ('measurement_wavelength_cdom', 13,  int),
    ('raw_signal_cdom',             14,  int),
    ('raw_internal_temp',           15,  int)
]

CTDBP_FLORT_PARTICLE_MAP = [
    ('raw_signal_beta',             7,   int),
    ('raw_signal_chl',              8,   int),
    ('raw_signal_cdom',             9,   int)
]


@attr('UNIT', group='mi')
class FlortDjDclParserUnitTestCase(ParserUnitTestCase):
    """
    flort_dj_dcl Parser unit test suite
    """
    def open_file(self, filename):
        my_file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return my_file

    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS:
            'FlortDjDclRecoveredInstrumentDataParticle'
        }

        self.tel_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS:
            'FlortDjDclTelemeteredInstrumentDataParticle'
        }

    def test_big_giant_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done during
        integration and qualification testing.
        """
        log.debug('===== START TEST BIG GIANT INPUT RECOVERED =====')
        with self.open_file(FILE6) as in_file:
            parser = FlortDjDclParser(self.rec_config, in_file,
                                      self.exception_callback)

            # In a single read, get all particles in this file.
            number_expected_results = RECORDS_FILE6
            result = parser.get_records(number_expected_results)

            self.assertEqual(len(result), number_expected_results)
            self.assertEqual(self.exception_callback_value, [])

        log.debug('===== START TEST BIG GIANT INPUT TELEMETERED =====')
        with self.open_file(FILE7) as in_file:
            parser = FlortDjDclParser(self.tel_config,
                                      in_file, self.exception_callback)

            # In a single read, get all particles in this file.
            number_expected_results = RECORDS_FILE7
            result = parser.get_records(number_expected_results)

            self.assertEqual(len(result), number_expected_results)
            self.assertEqual(self.exception_callback_value, [])

        log.debug('===== END TEST BIG GIANT INPUT =====')

    def test_get_many(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST GET MANY RECOVERED =====')

        expected_particle = 24

        with self.open_file(FILE5) as in_file:
            parser = FlortDjDclParser(self.rec_config, in_file,
                                      self.exception_callback)

            # In a single read, get all particles for this file.
            result = parser.get_records(expected_particle)

            self.assertEqual(len(result), expected_particle)
            self.assertEqual(self.exception_callback_value, [])

        log.debug('===== START TEST GET MANY TELEMETERED =====')
        with self.open_file(FILE4) as in_file:
            parser = FlortDjDclParser(self.tel_config, in_file,
                                      self.exception_callback)

            # In a single read, get all particles for this file.
            result = parser.get_records(expected_particle)

            self.assertEqual(len(result), expected_particle)
            self.assertEqual(self.exception_callback_value, [])

        log.debug('===== END TEST GET MANY =====')

    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that no instrument particles are produced
        and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA RECOVERED =====')
        with self.open_file(FILE8) as in_file:
            parser = FlortDjDclParser(self.rec_config, in_file,
                                      self.exception_callback)

            # Try to get records and verify that none are returned.
            result = parser.get_records(1)

            self.assertEqual(result, [])
            self.assertEqual(len(self.exception_callback_value),
                             EXCEPTIONS_FILE8)

        log.debug('===== START TEST INVALID SENSOR DATA TELEMETERED =====')

        self.exception_callback_value = []  # reset exceptions
        with self.open_file(FILE8) as in_file:
            parser = FlortDjDclParser(self.tel_config, in_file,
                                      self.exception_callback)

            # Try to get records and verify that none are returned.
            result = parser.get_records(1)

            self.assertEqual(result, [])
            self.assertEqual(len(self.exception_callback_value),
                             EXCEPTIONS_FILE8)

        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')
        with self.open_file(FILE1) as in_file:
            parser = FlortDjDclParser(self.rec_config, in_file,
                                      self.exception_callback)

            # Try to get a record and verify that none are produced.
            result = parser.get_records(1)

            self.assertEqual(result, [])
            self.assertEqual(self.exception_callback_value, [])

        log.debug('===== START TEST NO SENSOR DATA TELEMETERED =====')
        with self.open_file(FILE1) as in_file:
            parser = FlortDjDclParser(self.tel_config, in_file,
                                      self.exception_callback)

            # Try to get a record and verify that none are produced.
            result = parser.get_records(1)

            self.assertEqual(result, [])
            self.assertEqual(self.exception_callback_value, [])

        log.debug('===== END TEST NO SENSOR DATA =====')

    def test_many_with_yml(self):
        """
        Read a file and verify that all records can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST MANY WITH YML RECOVERED =====')

        num_particles = 30
        with self.open_file(FILE2) as in_file:
            parser = FlortDjDclParser(self.rec_config, in_file,
                                      self.exception_callback)
            particles = parser.get_records(num_particles)
            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "rec_20020215.flort2.yml", RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== START TEST MANY WITH YML TELEMETERED =====')

        with self.open_file(FILE2) as in_file:
            parser = FlortDjDclParser(self.tel_config, in_file,
                                      self.exception_callback)

            particles = parser.get_records(num_particles)
            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "tel_20020215.flort2.yml",
                                  RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST MANY WITH YML =====')

    def test_bug_9692(self):
        """
        This test verifies a fix to accommodate DCL timestamps with Seconds >59
        The test file is a trimmed down copy of a recovered file from
        a real deployment
        """
        log.debug('===== START TEST BUG 9692 =====')

        with self.open_file('20151023.flort.log') as in_file:
            parser = FlortDjDclParser(self.rec_config, in_file,
                                      self.exception_callback)

            particles = parser.get_records(5)
            log.debug("Num particles: %d", len(particles))

            self.assertEquals(len(particles), 3)
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST BUG 9692 =====')

    log.debug('===== START TESTS ENHANCEMENT 9809  =====')
    """
    Combined ctdbp_cdef_dcl with flort d Parser unit test suite
    """
    def test_ctdbp_cdef_dcl_flort_d(self):
        """
        Verify that data records from a ctdbp with a flort_d plugged in
        will produce expected data particles for the flort_d and ignore the
        ctdbp data.

        test with control data only and CTD ID
        2015/12/11 01:29:19.067 [ctdbp3:DLOGP6]:Instrument Started [Power On]
        """
        log.debug('===== START TEST CTDBPwFLORT:control only =====')
        with self.open_file('20151211.ctdbp3_controlOnly.log') as in_file:
            num_particles_to_request = 10
            num_expected_particles = 0

            parser = eval(PARSER_NAME)(self.tel_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)
            self.assertEquals(len(self.exception_callback_value), 0)

        log.debug('===== END TEST CTDBPwFLORT:control only =====')

        """
        test 1 rec with CTD ID
        2015/01/03 00:30:23.395 [ctdbp3:DLOGP6]: 12.3772,  3.73234,    1.087, 0, 0, 0, 03 Jan 2015 00:30:16
        """
        log.debug('===== START TEST CTDBPwFLORT:1 rec Low Battery =====')
        with self.open_file('20150103.ctdbp3_1recCtdID_w_LowBattery.log') as \
                in_file:

            num_particles_to_request = 10
            num_expected_particles = 1
            parser = eval(PARSER_NAME)(self.tel_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20150103.ctdbp3_1recCtdID_w_LowBattery_flort.yml", RESOURCE_PATH)

        log.debug('===== END TEST CTDBPwFLORT:1 rec Low Battery =====')

        """
        test 3 recs with no CTD ID
        2016/10/09 00:30:26.290  13.3143,  3.56698,    1.088, 1672, 278, 84, 09 Oct 2016 00:30:20
        """
        log.debug('===== START TEST CTDBPwFLORT:3 rec no ctd id =====')
        with self.open_file('20161009.ctdbp3_3rec_noCtdId.log') as in_file:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = eval(PARSER_NAME)(self.tel_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20161009.ctdbp3_3rec_noCtdId_flort.yml", RESOURCE_PATH)
        log.debug('===== END TEST CTDBPwFLORT:3 rec no ctd id =====')

        """
        test 3 recs, 1 with hash separator, no CTD ID
        2014/10/17 21:30:23.684 # 14.7850,  3.96796,    0.981, 740, 222, 73,
        17 Oct 2014 21:30:17
        """
        log.debug('===== START TEST CTDBPwFLORT:3 recs, 1 with #  =====')
        with self.open_file('20141017.ctdbp3_3rec_w_1hash.log') as in_file:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = eval(PARSER_NAME)(self.tel_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20141017.ctdbp3_3rec_w_1hash_flort.yml", RESOURCE_PATH)
        log.debug('===== END TEST CTDBPwFLORT:3 recs, 1 with #  =====')
        """
        test 3 recs with negative pressure
        2014/10/02 00:30:28.063 [ctdbp3:DLOGP6]: 20.9286,  0.00003,   -0.011, 4130, 1244, 4130, 02 Oct 2014 00:30:23
        """
        log.debug('===== START TEST CTDBPwFLORT:3 recs, with neg press  =====')
        with self.open_file('20141002.ctdbp3_3Rec_negPressure.log') as in_file:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = eval(PARSER_NAME)(self.tel_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20141002.ctdbp3_3Rec_negPressure_flort.yml", RESOURCE_PATH)
        log.debug('===== END TEST CTDBPwFLORT:3 recs, with neg press  =====')

        """
        test 18 recs with one damaged data line
        """
        log.debug('===== START TEST CTDBPwFLORT:18 recs, 1 damaged =====')
        with self.open_file('20161025.ctdbp3_damagedRec.log') as in_file:

            num_particles_to_request = 20
            num_expected_particles = 18

            parser = eval(PARSER_NAME)(self.tel_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20161025.ctdbp3_damagedRec_flort.yml", RESOURCE_PATH)
        log.debug('===== END TEST CTDBPwFLORT:18 recs, 1 damaged =====')

        """
        test large file, 24 recs
        """
        log.debug('===== START TEST CTDBPwFLORT:24 recs =====')
        with self.open_file('20140928.ctdbp3_24rec.log') as in_file:

            num_particles_to_request = 30
            num_expected_particles = 24

            parser = eval(PARSER_NAME)(self.tel_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20140928.ctdbp3_24rec_flort.yml", RESOURCE_PATH)
        log.debug('===== END TEST CTDBPwFLORT:24 recs =====')

        """
        Above tests for #9809 are all for the telemetered data path.
        Next set of tests are for the recovered path.  Data files are
        identical format as for telemetered.

        test 4 recs recovered data
        """
        log.debug('===== START TEST CTDBPwFLORT:4 recs Recovered =====')
        with self.open_file('20150412.ctdbp3_4rec_flort_recovered.log') as in_file:

            num_particles_to_request = 10
            num_expected_particles = 4

            parser = eval(PARSER_NAME)(self.rec_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20150412.ctdbp3_4rec_flort_recovered.yml", RESOURCE_PATH)
        log.debug('===== END TEST CTDBPwFLORT:4 recs Recovered =====')

        """
        test 13 recs recovered data
        """
        log.debug('===== START TEST CTDBPwFLORT:13 recs Recovered =====')
        with self.open_file('20140929.ctdbp3_13rec_flort_recovered.log') as in_file:

            num_particles_to_request = 20
            num_expected_particles = 13

            parser = eval(PARSER_NAME)(self.rec_config, in_file,
                                       self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20140929.ctdbp3_13rec_flort_recovered.yml", RESOURCE_PATH)

        log.debug('===== END TEST CTDBPwFLORT:13 recs Recovered =====')

        log.debug('===== END TESTS ENHANCEMENT 9809  =====')
