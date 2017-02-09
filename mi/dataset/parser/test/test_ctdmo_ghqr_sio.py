#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_ctdmo_ghqr_sio
@file marine-integrations/mi/dataset/parser/test/test_ctdmo_ghqr_sio.py
@author Emily Hahn, Steve Myerson (recovered)
@brief Test code for a Ctdmo ghqr sio data parser
Recovered CO files:
  CTD02000.DAT
    1 CT block
    0 CO blocks
  CTD02001.DAT
    1 CT
    1 CO w/6 records, 5 valid IDs
  CTD02002.DAT
    1 CO w/4 records, 3 valid IDs
    1 CT
    1 CO w/6 records, 4 valid IDs
  CTD02004.DAT
    1 CT
    1 CO w/2 records, 0 valid IDs
    1 CO w/2 records, 1 valid ID
    1 CO w/5 records, 4 valid IDs
    1 CT
    1 CO w/10 records, 10 valid IDs
  CTD02100.DAT
    1 CT
    1 CO w/100 records, 100 valid IDs
    1 CO w/150 records, 150 valid IDs

Recovered CT files:
  SBE37-IM_20100000_2011_00_00.hex - 0 CT records
  SBE37-IM_20110101_2011_01_01.hex - 3 CT records
  SBE37-IM_20120314_2012_03_14.hex - 9 CT records
  SBE37-IM_20130704_2013_07_04.hex - 18 CT records
  SBE37-IM_20141231_2014_12_31.hex - 99 CT records
"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH

from mi.dataset.parser.ctdmo_ghqr_sio import \
    CtdmoGhqrSioRecoveredCoParser, \
    CtdmoGhqrRecoveredCtParser, \
    CtdmoGhqrSioTelemeteredParser, \
    INDUCTIVE_ID_KEY

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.exceptions import DatasetParserException, UnexpectedDataException

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'ctdmo_ghqr', 'sio', 'resource')


@attr('UNIT', group='mi')
class CtdmoGhqrSioParserUnitTestCase(ParserUnitTestCase):

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: [
                'CtdmoGhqrSioTelemeteredInstrumentDataParticle',
                'CtdmoGhqrSioTelemeteredOffsetDataParticle'
            ]
        }

        self.config_rec_co = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdmoGhqrSioRecoveredOffsetDataParticle',
        }

        self.config_rec_ct = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdmoGhqrRecoveredInstrumentDataParticle',
            INDUCTIVE_ID_KEY: 55
        }

    def test_simple(self):
        """
        Read test data from the file and assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_2.ctdmo.dat'), 'rb') as stream_handle:
            parser = CtdmoGhqrSioTelemeteredParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(12)
            self.assertEqual(len(particles), 12)
            self.assert_particles(particles, 'node59p1_2.ctdmo.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_get_many(self):
        """
        Read test data from the file and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.ctdmo.dat'), 'rb') as stream_handle:
            parser = CtdmoGhqrSioTelemeteredParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(10)
            particles2 = parser.get_records(10)
            self.assertEquals(len(particles2), 10)
            particles.extend(particles2)
            particles3 = parser.get_records(10)
            # 24 records total, should only have 4 remaining at this point
            self.assertEquals(len(particles3), 4)
            particles.extend(particles3)

            self.assert_particles(particles, 'node59p1_1.ctdmo.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_co_and_ct(self):
        """
        Test with both co and ct particle types verified
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_3.ctdmo.dat'), 'rb') as stream_handle:
            parser = CtdmoGhqrSioTelemeteredParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(24)
            self.assertEqual(len(particles), 24) 
            self.assert_particles(particles, 'node59p1_3.ctdmo.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        Test a long file and confirm the number of particles is correct
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_0.ctdmo.dat'), 'rb') as stream_handle:
            parser = CtdmoGhqrSioTelemeteredParser(self.config, stream_handle, self.exception_callback)

            # require more records than are available in the file
            result = parser.get_records(2000)
            # confirm we only get the number in the file (10 CO * 12/block = 120, 129 CT blocks * 12/block = 1548)
            self.assertEqual(len(result), 1668)

        self.assertEqual(self.exception_callback_value, [])

    def test_unexpected_id(self):
        """
        Read test data from the file including an sio block with an unexpected id.
        Assert that the results are those we expected and an exception occurs.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.extra.ctdmo.dat'), 'rb') as stream_handle:
            parser = CtdmoGhqrSioTelemeteredParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(24)
            self.assertEqual(len(particles), 24)
            self.assert_particles(particles, 'node59p1_1.ctdmo.yml', RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

    def test_unexpected_data(self):
        """
        Read test data from the file including unexpected data.
        Assert that the results are those we expected and an exception occurs.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.bad.ctdmo.dat'), 'rb') as stream_handle:
            parser = CtdmoGhqrSioTelemeteredParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(24)
            self.assertEqual(len(particles), 24)
            self.assert_particles(particles, 'node59p1_1.ctdmo.yml', RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

    def test_rec_co_big_giant_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done during
        integration and qualification testing.
        File used for this test has 250 total CO particles.
        """
        in_file = open(os.path.join(RESOURCE_PATH, 'CTD02100.DAT'), 'rb')
        parser = CtdmoGhqrSioRecoveredCoParser(self.config_rec_co, in_file, self.exception_callback)

        number_expected_results = 250

        # In a single read, get all particles in this file.
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        self.assert_particles(result, 'CTD02100.yml', RESOURCE_PATH)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_co_get_many(self):
        """
        Read Recovered CO data and pull out multiple data particles in two blocks.
        Verify that the results are those we expected.
        File used for this test has 2 CO SIO blocks.
        """
        in_file = open(os.path.join(RESOURCE_PATH, 'CTD02002.DAT'), 'rb')
        parser = CtdmoGhqrSioRecoveredCoParser(self.config_rec_co, in_file, self.exception_callback)

        particles = parser.get_records(6)
        particles2 = parser.get_records(6)

        particles.extend(particles2)

        self.assertEqual(len(particles), 10)
        self.assert_particles(particles, 'CTD02002.yml', RESOURCE_PATH)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_co_long_stream(self):
        """
        Read test data and pull out all particles from a file at once.
        File used for this test has 3 CO SIO blocks and a total of 19 CO records.
        """
        in_file = open(os.path.join(RESOURCE_PATH, 'CTD02004.DAT'), 'rb')
        parser = CtdmoGhqrSioRecoveredCoParser(self.config_rec_co, in_file, self.exception_callback)

        particles = parser.get_records(19)
        self.assertEqual(len(particles), 19)

        # there should be no more particles in the file, ensure no more are returned
        particles2 = parser.get_records(19)
        self.assertEqual(len(particles2), 0)

        self.assert_particles(particles, 'CTD02004.yml', RESOURCE_PATH)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_co_no_records(self):
        """
        Read a Recovered CO data file that has no CO records.
        Verify that no particles are generated.
        """
        in_file = open(os.path.join(RESOURCE_PATH, 'CTD02000.DAT'), 'rb')
        parser = CtdmoGhqrSioRecoveredCoParser(self.config_rec_co, in_file, self.exception_callback)

        # Not expecting any particles.
        expected_results = []

        # Try to get one particle and verify we didn't get any.
        result = parser.get_records(1)
        self.assertEqual(result, expected_results)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_co_simple(self):
        """
        Read Recovered CO data from the file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'CTD02001.DAT'), 'rb') as in_file:

            parser = CtdmoGhqrSioRecoveredCoParser(self.config_rec_co, in_file, self.exception_callback)

            particles = parser.get_records(7)

            self.assertEqual(len(particles), 6)

            self.assert_particles(particles, 'CTD02001.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_co_real(self):
        """
        Test with a real rather than generated CO file
        """
        with open(os.path.join(RESOURCE_PATH, 'CTD15906.DAT'), 'rb') as in_file:

            parser = CtdmoGhqrSioRecoveredCoParser(self.config_rec_co, in_file, self.exception_callback)

            # only 1 CO block with 12 records in real file
            particles = parser.get_records(14)
            self.assertEqual(len(particles), 12)

        self.assertEqual(self.exception_callback_value, [])

    def test_rec_ct_long_stream(self):
        """
        Read test data and pull out all particles from a file at once.
        """
        in_file = open(os.path.join(RESOURCE_PATH,
                                    'SBE37-IM_20141231_2014_12_31.hex'), 'r')
        parser = CtdmoGhqrRecoveredCtParser(self.config_rec_ct, in_file, self.exception_callback)

        total_records = 99
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), total_records)
        self.assert_particles(particles, 'SBE37-IM_20141231_2014_12_31.yml', RESOURCE_PATH)

        # confirm there are no more particles in this file
        particles2 = parser.get_records(5)
        self.assertEqual(len(particles2), 0)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_ct_missing_end(self):
        """
        Read a Recovered CT data file that has no end configuration record.
        Verify that no particles are generated.
        """
        in_file = open(os.path.join(RESOURCE_PATH,
                                    'SBE37-IM_20110101_missing_end.hex'), 'r')
        parser = CtdmoGhqrRecoveredCtParser(self.config_rec_ct, in_file, self.exception_callback)

        # Not expecting any particles.
        expected_results = []

        # Try to get one particle and verify we didn't get any.
        result = parser.get_records(1)
        self.assertEqual(result, expected_results)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_ct_missing_inductive_id_config(self):
        """
        Make sure that an exception is raised when building the
        Recovered CT parser if the inductive ID is missing in the config.
        """
        in_file = open(os.path.join(RESOURCE_PATH,
                                    'SBE37-IM_20110101_2011_01_01.hex'), 'r')

        bad_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdmoGhqrRecoveredInstrumentDataParticle',
        }

        with self.assertRaises(DatasetParserException):
            CtdmoGhqrRecoveredCtParser(bad_config, in_file, self.exception_callback)

    def test_rec_ct_missing_serial(self):
        """
        Read a Recovered CT data file that has no Serial record.
        Verify that no particles are generated.
        """
        in_file = open(os.path.join(RESOURCE_PATH,
                                    'SBE37-IM_20110101_missing_serial.hex'), 'r')
        parser = CtdmoGhqrRecoveredCtParser(self.config_rec_ct, in_file, self.exception_callback)

        # Not expecting any particles.
        expected_results = []

        # Try to get one particle and verify we didn't get any.
        result = parser.get_records(1)
        self.assertEqual(result, expected_results)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_ct_no_records(self):
        """
        Read a Recovered CT data file that has no CT records.
        Verify that no particles are generated.
        """
        in_file = open(os.path.join(RESOURCE_PATH,
                                    'SBE37-IM_20100000_2010_00_00.hex'), 'r')
        parser = CtdmoGhqrRecoveredCtParser(self.config_rec_ct, in_file, self.exception_callback)

        # Not expecting any particles.
        expected_results = []

        # Try to get one particle and verify we didn't get any.
        result = parser.get_records(1)
        self.assertEqual(result, expected_results)

        in_file.close()
        self.assertEqual(self.exception_callback_value, [])

    def test_rec_ct_simple(self):
        """
        Read Recovered CT data from the file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'SBE37-IM_20110101_2011_01_01.hex'), 'r') as in_file:
            parser = CtdmoGhqrRecoveredCtParser(self.config_rec_ct, in_file, self.exception_callback)

            particles = parser.get_records(3)

            self.assertEqual(len(particles), 3)

            self.assert_particles(particles, 'SBE37-IM_20110101_2011_01_01.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_ct_real(self):
        """
        Test with a real CT file rather than a generated one
        """
        with open(os.path.join(RESOURCE_PATH, 'SBE37-IM_03710261_2013_07_25.hex'), 'r') as in_file:
            parser = CtdmoGhqrRecoveredCtParser(self.config_rec_ct, in_file, self.exception_callback)

            particles = parser.get_records(482)

            self.assertEqual(len(particles), 482)

            self.assertEqual(self.exception_callback_value, [])