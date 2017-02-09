#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_vel3d_k_wfp_stc.py
@author Steve Myerson (Raytheon)
@brief Test code for a vel3d_k_wfp_stc data parser
"""

import os
from StringIO import StringIO

import ntplib
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.vel3d_k.wfp_stc.resource import RESOURCE_PATH
from mi.dataset.parser.vel3d_k_wfp_stc import \
    Vel3dKWfpStcParser, \
    Vel3dKWfpStcMetadataParticle, \
    Vel3dKWfpStcInstrumentParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


FLAG_RECORD_SIZE = 26 
VELOCITY_RECORD_SIZE = 24  # fixed only for test data - variable in real life
TIME_RECORD_SIZE = 8

## First byte of flag record is bad.
TEST_DATA_BAD_FLAG_RECORD = \
    '\x09\x00\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x01\x01\x01\x01\x01\x01\x01\x00\x00\x00' \
    '\x71\x08\x1D\x10\x00\x11\xC3\x08\xC9\x0B\x60\x03\xF9\xFD\xFC\xE8' \
    '\x52\x60\x53\x24\x53\x42\x40\x44' \
    '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x52\x48\x4E\x82\x52\x48\x4F\x9B'

## Flag record is too short.
TEST_DATA_SHORT_FLAG_RECORD = \
    '\x01\x00\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

## Flag record, first velocity record, and time record
## from A000010.DEC sample file. IDD has expected outputs.
TEST_DATA_GOOD_1_REC = \
    '\x01\x00\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x01\x01\x01\x01\x01\x01\x01\x00\x00\x00' \
    '\x71\x08\x1D\x10\x00\x11\xC3\x08\xC9\x0B\x60\x03\xF9\xFD\xFC\xE8' \
    '\x52\x60\x53\x24\x53\x42\x40\x44' \
    '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x52\x48\x4E\x82\x52\x48\x4F\x9B'

## Flag record, first and last velocity record, and time record
## from A000010.DEC sample file. IDD has expected outputs.
TEST_DATA_GOOD_2_REC = \
    '\x01\x00\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x01\x01\x01\x01\x01\x01\x01\x00\x00\x00' \
    '\x71\x08\x1D\x10\x00\x11\xC3\x08\xC9\x0B\x60\x03\xF9\xFD\xFC\xE8' \
    '\x52\x60\x53\x24\x53\x42\x40\x44' \
    '\x71\x08\x1D\x10\x04\x25\xC4\x08\xBF\x0B\x5E\x03\xF0\xFD' \
    '\xFC\x26\x51\xF6\x51\xFC\x50\x43\x40\x45' \
    '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x52\x48\x4E\x82\x52\x48\x4F\x9B'

## Flag record, many velocity records, and time record.
## Multiple records are the first and last repeated in pairs.
TEST_DATA_GOOD_BIG_FILE = \
    '\x01\x00\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x01\x01\x01\x01\x01\x01\x01\x00\x00\x00' \
    '\x71\x08\x1D\x10\x00\x11\xC3\x08\xC9\x0B\x60\x03\xF9\xFD\xFC\xE8' \
    '\x52\x60\x53\x24\x53\x42\x40\x44' \
    '\x71\x08\x1D\x10\x04\x25\xC4\x08\xBF\x0B\x5E\x03\xF0\xFD' \
    '\xFC\x26\x51\xF6\x51\xFC\x50\x43\x40\x45' \
    '\x71\x08\x1D\x10\x00\x11\xC3\x08\xC9\x0B\x60\x03\xF9\xFD\xFC\xE8' \
    '\x52\x60\x53\x24\x53\x42\x40\x44' \
    '\x71\x08\x1D\x10\x04\x25\xC4\x08\xBF\x0B\x5E\x03\xF0\xFD' \
    '\xFC\x26\x51\xF6\x51\xFC\x50\x43\x40\x45' \
    '\x71\x08\x1D\x10\x00\x11\xC3\x08\xC9\x0B\x60\x03\xF9\xFD\xFC\xE8' \
    '\x52\x60\x53\x24\x53\x42\x40\x44' \
    '\x71\x08\x1D\x10\x04\x25\xC4\x08\xBF\x0B\x5E\x03\xF0\xFD' \
    '\xFC\x26\x51\xF6\x51\xFC\x50\x43\x40\x45' \
    '\x71\x08\x1D\x10\x00\x11\xC3\x08\xC9\x0B\x60\x03\xF9\xFD\xFC\xE8' \
    '\x52\x60\x53\x24\x53\x42\x40\x44' \
    '\x71\x08\x1D\x10\x04\x25\xC4\x08\xBF\x0B\x5E\x03\xF0\xFD' \
    '\xFC\x26\x51\xF6\x51\xFC\x50\x43\x40\x45' \
    '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x00\x00\x00\x00\x00\x00\x00\x00' \
    '\x52\x48\x4E\x82\x52\x48\x4F\x9B'

VELOCITY_1_GROUPS = (113, 8, 29, 16, 0, 17, 2243, 3017, 864, 
    -519, -4, 21224, 21344, 21284, 66, 64, 68)

VELOCITY_2_GROUPS = (113, 8, 29, 16, 4, 37, 2244, 3007, 862, 
    -528, -4, 20774, 20982, 20732, 67, 64, 69)

TIME_1_GROUPS = (1380470402, 1380470683, 1)
TIME_2_GROUPS = (1380470402, 1380470683, 2)
TIME_8_GROUPS = (1380470402, 1380470683, 8)


@attr('UNIT', group='mi')
class Vel3dKWfpStcParserUnitTestCase(ParserUnitTestCase):
    """
    Vel3d_k__stc_imodem Parser unit test suite
    """
    def create_parser(self, file_handle):
        """
        This function creates a Vel3dKWfpStcParser parser.
        """
        parser = Vel3dKWfpStcParser(self.config,
                                    file_handle,
                                    self.exception_callback)
        return parser

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: \
                'mi.dataset.parser.vel3d_k_wfp_stc',
            DataSetDriverConfigKeys.PARTICLE_CLASS: \
                ['Vel3dKWfpStcMetadataParticle',
                 'Vel3dKWfpStcInstrumentDataParticle']
        }

        # Define test data particles and their associated timestamps 
        # which will be compared with returned results

        #
        # This parser stores the groups from the data matcher in raw_data.
        #
        ntp_time = ntplib.system_to_ntp_time(1380470402.0)
        self.expected_particle1 = Vel3dKWfpStcInstrumentParticle(
            VELOCITY_1_GROUPS, internal_timestamp=ntp_time)

        ntp_time = ntplib.system_to_ntp_time(1380470402.5)
        self.expected_particle2 = Vel3dKWfpStcInstrumentParticle(
            VELOCITY_2_GROUPS, internal_timestamp=ntp_time)

        ntp_time = ntplib.system_to_ntp_time(1380470403.0)
        self.expected_particle3 = Vel3dKWfpStcInstrumentParticle(
            VELOCITY_1_GROUPS, internal_timestamp=ntp_time)

        ntp_time = ntplib.system_to_ntp_time(1380470403.5)
        self.expected_particle4 = Vel3dKWfpStcInstrumentParticle(
            VELOCITY_2_GROUPS, internal_timestamp=ntp_time)

        ntp_time = ntplib.system_to_ntp_time(1380470402.0)
        self.expected_time1 = Vel3dKWfpStcMetadataParticle(
            TIME_1_GROUPS, internal_timestamp=ntp_time)

        ntp_time = ntplib.system_to_ntp_time(1380470402.0)
        self.expected_time2 = Vel3dKWfpStcMetadataParticle(
            TIME_2_GROUPS, internal_timestamp=ntp_time)

        ntp_time = ntplib.system_to_ntp_time(1380470402.0)
        self.expected_time8 = Vel3dKWfpStcMetadataParticle(
            TIME_8_GROUPS, internal_timestamp=ntp_time)

    def verify_contents(self, actual_particle, expected_particle):
        # log.debug('EXP %s XXX', dir(expected_particle))
        # log.debug('ACT %s YYY', dir(actual_particle))
        self.assertEqual(actual_particle, [expected_particle])

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        File is valid.  Has 1 velocity record.
        """
        log.info("START SIMPLE")
        log.info("Simple length %d", len(TEST_DATA_GOOD_1_REC))
        input_file = StringIO(TEST_DATA_GOOD_1_REC)
        self.parser = self.create_parser(input_file)

        log.info("SIMPLE VERIFY VELOCITY RECORD 1")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_particle1)

        log.info("SIMPLE VERIFY TIME RECORD")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_time1)

        log.info("END SIMPLE")

    def test_get_some(self):
        """
        Read test data and pull out multiple data particles one at a time.
        Assert that the results are those we expected.
        File is valid.  Has 2 velocity records.
        """
        log.info("START SOME")
        log.info("Some length %d", len(TEST_DATA_GOOD_2_REC))
        input_file = StringIO(TEST_DATA_GOOD_2_REC)
        self.parser = self.create_parser(input_file)

        log.info("SOME VERIFY VELOCITY RECORD 1")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_particle1)

        log.info("SOME VERIFY VELOCITY RECORD 2")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_particle2)

        log.info("SOME VERIFY TIME RECORD")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_time2)

        log.info("END SOME")

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        File is valid.  Has many velocity records.
        """
        log.info("START MANY")
        log.info("Many length %d", len(TEST_DATA_GOOD_BIG_FILE))
        input_file = StringIO(TEST_DATA_GOOD_BIG_FILE)
        self.parser = self.create_parser(input_file)

        log.info("MANY VERIFY VELOCITY RECORD 1")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_particle1)

        expected_file_position = FLAG_RECORD_SIZE + VELOCITY_RECORD_SIZE

        log.info("MANY VERIFY VELOCITY RECORDS 2-4")
        result = self.parser.get_records(3)
        self.assertEqual(result, [self.expected_particle2,
            self.expected_particle3, self.expected_particle4])

        # Skip over the next 4 velocity records.
        log.info("MANY SKIPPING")
        skip_result = self.parser.get_records(4)
        expected_file_position += 4 * VELOCITY_RECORD_SIZE

        # We should now be at the time record.
        log.info("MANY VERIFY TIME RECORD")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_time8)

        log.info("END MANY")

    def test_bad_flag_record(self):
        """
        Ensure that bad data is skipped when flag record is invalid.
        This should raise an exception indicating that the Flag record
        is invalid.
        """
        log.info("START BAD FLAG")
        log.info("Bad Flag length %d", len(TEST_DATA_BAD_FLAG_RECORD))
        input_file = StringIO(TEST_DATA_BAD_FLAG_RECORD)

        self.parser = self.create_parser(input_file)
        particles = self.parser.get_records(1)

        self.assertEquals(len(particles), 0)
        self.assertEquals(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        log.info("END BAD FLAG")

    def test_short_flag_record(self):
        """
        Ensure that data is skipped when flag record is too short.
        This should raise an exception indicating that end of file was
        reached while reading the Flag record.
        """
        log.info("START SHORT FLAG")
        log.info("Short Flag length %d", len(TEST_DATA_SHORT_FLAG_RECORD))
        input_file = StringIO(TEST_DATA_SHORT_FLAG_RECORD)

        self.parser = self.create_parser(input_file)
        particles = self.parser.get_records(1)

        self.assertEquals(len(particles), 0)
        self.assertEquals(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))
        log.info("END SHORT FLAG")

    def test_real_file_1(self):
        """
        Ensure that data is skipped when flag record is too short.
        This should raise an exception indicating that end of file was
        reached while reading the Flag record.
        """
        log.info("START REAL FILE 1")

        with open(os.path.join(RESOURCE_PATH, 'A0000010_another.DEC')) as file_handle:

            self.parser = self.create_parser(file_handle)

            particles = self.parser.get_records(100)

            self.assert_particles(particles, 'A0000010_another.yml', RESOURCE_PATH)

        log.info("END REAL FILE 1")

    def test_real_file_2(self):
        """
        Ensure that data is skipped when flag record is too short.
        This should raise an exception indicating that end of file was
        reached while reading the Flag record.
        """
        log.info("START REAL FILE 2")

        with open(os.path.join(RESOURCE_PATH, 'A0000000_NoBeams.DEC')) as file_handle:

            self.parser = self.create_parser(file_handle)

            particles = self.parser.get_records(100)

            self.assert_particles(particles, 'A0000000_NoBeams.yml', RESOURCE_PATH)

        log.info("END REAL FILE 2")

    def test_real_file_3(self):
        """
        Ensure that data is skipped when flag record is too short.
        This should raise an exception indicating that end of file was
        reached while reading the Flag record.
        """
        log.info("START REAL FILE 3")

        with open(os.path.join(RESOURCE_PATH, 'A0000001_WithBeams.DEC')) as file_handle:

            self.parser = self.create_parser(file_handle)

            particles = self.parser.get_records(100)

            self.assert_particles(particles, 'A0000001_WithBeams.yml', RESOURCE_PATH)

        log.info("END REAL FILE 3")

    def test_real_file_4(self):
        """
        Ensure that data is skipped when flag record is too short.
        This should raise an exception indicating that end of file was
        reached while reading the Flag record.
        """
        log.info("START REAL FILE 4")

        with open(os.path.join(RESOURCE_PATH, 'valid_A0000004.DEC')) as file_handle:

            self.parser = self.create_parser(file_handle)

            particles = self.parser.get_records(1000)

            self.assert_particles(particles, 'valid_A0000004.yml', RESOURCE_PATH)

        log.info("END REAL FILE 4")

    def test_bug_8637(self):
        """
        Ensure that data is skipped when flag record is too short.
        This should raise an exception indicating that end of file was
        reached while reading the Flag record.
        """
        log.info("START Bug 8637")

        with open(os.path.join(RESOURCE_PATH, 'A0000015.DEC')) as file_handle:

            self.parser = self.create_parser(file_handle)

            particles = self.parser.get_records(1000)

            # unless told otherwise, should return nothing
            # when there is no valid end record found
            self.assertEquals(len(particles), 0)
            self.assertEquals(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        log.info("END Bug 8637")
