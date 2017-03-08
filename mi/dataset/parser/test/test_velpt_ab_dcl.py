#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_velpt_ab
@file mi-dataset/mi/dataset/parser/test/test_velpt_ab_dcl.py
@author Chris Goodrich
@brief Test code for the velpt_ab parser
"""

__author__ = 'Chris Goodrich'

import os
import re

from nose.plugins.attrib import attr

from mi.core.exceptions import ConfigurationException
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.velpt_ab.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.common_regexes import FLOAT_REGEX, END_OF_LINE_REGEX
from mi.dataset.parser.velpt_ab_dcl import VelptAbDclParser, VelptAbDclParticleClassKey
from mi.dataset.parser.velpt_ab_dcl_particles import VelptAbDclInstrumentDataParticle,\
    VelptAbDclDiagnosticsHeaderParticle, VelptAbDclDiagnosticsDataParticle, VelptAbDclInstrumentDataParticleRecovered,\
    VelptAbDclDiagnosticsHeaderParticleRecovered, VelptAbDclDiagnosticsDataParticleRecovered
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.logging import log


@attr('UNIT', group='mi')
class VelptAbDclParserUnitTestCase(ParserUnitTestCase):
    """
    velpt_ab_dcl Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._telemetered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                VelptAbDclParticleClassKey.METADATA_PARTICLE_CLASS: VelptAbDclDiagnosticsHeaderParticle,
                VelptAbDclParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS: VelptAbDclDiagnosticsDataParticle,
                VelptAbDclParticleClassKey.INSTRUMENT_PARTICLE_CLASS: VelptAbDclInstrumentDataParticle
            }
        }

        self._recovered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                VelptAbDclParticleClassKey.METADATA_PARTICLE_CLASS: VelptAbDclDiagnosticsHeaderParticleRecovered,
                VelptAbDclParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS: VelptAbDclDiagnosticsDataParticleRecovered,
                VelptAbDclParticleClassKey.INSTRUMENT_PARTICLE_CLASS: VelptAbDclInstrumentDataParticleRecovered
            }
        }

        self._incomplete_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self._bad_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {}
        }

    def test_simple(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        This is the happy path.
        """
        log.debug('===== START TEST SIMPLE =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE =====')

    def test_too_few_diagnostics_records(self):
        """
        The file used in this test has only 19 diagnostics records in the second set.
        Twenty are expected.
        """
        log.debug('===== START TEST NOT ENOUGH DIAGNOSTICS RECORDS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'too_few_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_few_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'too_few_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_too_few_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST NOT ENOUGH DIAGNOSTICS RECORDS =====')

    def test_too_many_diagnostics_records(self):
        """
        The file used in this test has 21 diagnostics records in the second set.
        Twenty are expected.
        """
        log.debug('===== START TEST TOO MANY DIAGNOSTICS RECORDS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'too_many_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_many_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'too_many_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_too_many_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST TOO MANY DIAGNOSTICS RECORDS =====')

    def test_invalid_sync_byte(self):
        """
        The file used in this test has extra bytes between records which need to be skipped
        in order to process the correct number of particles.
        """
        log.debug('===== START TEST INVALID SYNC BYTE =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'extra_bytes_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'extra_bytes_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID SYNC BYTE =====')

    def test_invalid_record_id(self):
        """
        The file used in this test has extra bytes between records which need to be skipped
        in order to process the correct number of particles.
        """
        log.debug('===== START TEST INVALID RECORD ID =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_id_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_id_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID RECORD ID =====')

    def test_bad_diagnostic_checksum(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD CHECKSUM =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diagnostic_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diagnostic_checksum_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_diagnostic_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diagnostic_checksum_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD CHECKSUM =====')

    def test_truncated_file(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND TRUNCATED FILE =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'truncated_file_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'truncated_file_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'truncated_file_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_truncated_file_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND TRUNCATED FILE =====')

    def test_bad_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST BAD CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = VelptAbDclParser(self._bad_parser_config,
                                          file_handle,
                                          self.exception_callback)

        log.debug('===== END TEST BAD CONFIGURATION =====')

    def test_bad_velocity_checksum(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 49 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST FOUND BAD VELOCITY CHECKSUM =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_velocity_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_velocity_checksum_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_velocity_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_velocity_checksum_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD VELOCITY CHECKSUM =====')

    def test_diag_header_bad_checksum(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 491 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST FOUND BAD DIAGNOSTIC HEADER CHECKSUM =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diag_hdr_checksum_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAGNOSTIC HEADER CHECKSUM =====')

    def test_missing_diag_header(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 491 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST MISSING DIAGNOSTIC HEADER =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_header_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'missing_diag_header_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_header_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_missing_diag_header_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING DIAGNOSTIC HEADER =====')

    def test_random_diag_record(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 491 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST FOUND RANDOM DIAGNOSTIC RECORD =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'random_diag_record_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'random_diag_record_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'random_diag_record_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_random_diag_record_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND RANDOM DIAGNOSTIC RECORD =====')

    def test_missing_diag_recs(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 49 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST MISSING DIAGNOSTIC RECORDS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_recs_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 29

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'missing_diag_recs_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_recs_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 29

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_missing_diag_recs_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING DIAGNOSTIC RECORDS =====')

    def test_partial_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST PARTIAL CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = VelptAbDclParser(self._incomplete_parser_config,
                                          file_handle,
                                          self.exception_callback)

        log.debug('===== END TEST PARTIAL CONFIGURATION =====')

    def test_bad_diag_checksum_19_recs(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD DIAG HDR CHECKSUM AND TOO FEW RECS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_19_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 48

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_19_diag_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_19_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 48

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diag_hdr_checksum_19_diag_20140813.velpt.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAG HDR CHECKSUM AND TOO FEW RECS =====')

    def test_bad_diag_checksum_21_recs(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD DIAG HDR CHECKSUM AND TOO MANY RECS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_21_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_21_diag_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_21_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diag_hdr_checksum_21_diag_20140813.velpt.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAG HDR CHECKSUM AND TOO MANY RECS =====')

    def fix_yml_pressure_params(self):
        """
        This helper tool was used to modify the yml files in response to ticket #4341
        """

        pressure_regex = r'\s+pressure:\s+(0.\d+)'

        for file_name in os.listdir(RESOURCE_PATH):

            if file_name.endswith('.yml'):

                with open(os.path.join(RESOURCE_PATH, file_name), 'rU') as in_file_id:

                    out_file_name = file_name + '.new'
                    log.info('fixing file %s', file_name)
                    log.info('creating file %s', out_file_name)

                    out_file_id = open(os.path.join(RESOURCE_PATH, out_file_name), 'w')

                    for line in in_file_id:
                        match = re.match(pressure_regex, line)
                        if match is not None:
                            new_value = float(match.group(1)) * 1000.0
                            new_line = '    pressure_mbar:  ' + str(new_value)
                            out_file_id.write(new_line + '\n')
                        else:
                            out_file_id.write(line)

                    out_file_id.close()

    def particle_to_yml(self, particles, filename):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(filename, 'w')
        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')
        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            fid.write('  - _index: %d\n' % (i+1))
            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))
            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.3f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def parse_live_logs(self):
        """
        These tests were used to view the output of files associated with Bug_4341
        """

        with open(os.path.join(RESOURCE_PATH, '20141110.velpt2.log'), 'rb') as file_handle:

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(100)

            self.particle_to_yml(particles, os.path.join(RESOURCE_PATH, '20141110.velpt2.yml'))

        with open(os.path.join(RESOURCE_PATH, '20150613.velpt.log'), 'rb') as file_handle:

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(100)

            self.particle_to_yml(particles, os.path.join(RESOURCE_PATH, '20150613.velpt.yml'))

        with open(os.path.join(RESOURCE_PATH, '20150518.velpt.log'), 'rb') as file_handle:

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(100)

            self.particle_to_yml(particles, os.path.join(RESOURCE_PATH, '20150518.velpt.yml'))

        with open(os.path.join(RESOURCE_PATH, '20150409.velpt1.log'), 'rb') as file_handle:

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(100)

            self.particle_to_yml(particles, os.path.join(RESOURCE_PATH, '20150409.velpt1.yml'))

        with open(os.path.join(RESOURCE_PATH, '20150428.velpt2.log'), 'rb') as file_handle:

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(100)

            self.particle_to_yml(particles, os.path.join(RESOURCE_PATH, '20150428.velpt2.yml'))

        with open(os.path.join(RESOURCE_PATH, '20150824.velpt1.log'), 'rb') as file_handle:

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(100)

            self.particle_to_yml(particles, os.path.join(RESOURCE_PATH, '20150824.velpt1.yml'))

        with open(os.path.join(RESOURCE_PATH, '20150829.velpt2.log'), 'rb') as file_handle:

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(100)

            self.particle_to_yml(particles, os.path.join(RESOURCE_PATH, '20150829.velpt2.yml'))

    def fix_yml_float_params(self):
        """
        This helper tool was used to modify the yml files in response to ticket #8564
        """

        param_change_table = [
            ('battery_voltage', 'battery_voltage_dV', 10),
            ('sound_speed_analog2', 'sound_speed_dms', 10),
            ('heading', 'heading_decidegree', 10),
            ('pitch', 'pitch_decidegree', 10),
            ('roll', 'roll_decidegree', 10),
            ('pressure_mbar', 'pressure_mbar', 1),
            ('temperature', 'temperature_centidegree', 100),
            ('velocity_beam1', 'velocity_beam1', 1),
            ('velocity_beam2', 'velocity_beam2', 1),
            ('velocity_beam3', 'velocity_beam3', 1)
        ]

        class_change_table = [
            ('VelptAbInstrumentDataParticle', 'VelptAbDclInstrumentDataParticle'),
            ('VelptAbDiagnosticsHeaderParticle', 'VelptAbDclDiagnosticsHeaderParticle'),
            ('VelptAbDiagnosticsDataParticle', 'VelptAbDclDiagnosticsDataParticle'),
            ('VelptAbInstrumentDataParticleRecovered', 'VelptAbDclInstrumentDataParticleRecovered'),
            ('VelptAbDiagnosticsHeaderParticleRecovered', 'VelptAbDclDiagnosticsHeaderParticleRecovered'),
            ('VelptAbDiagnosticsDataParticleRecovered', 'VelptAbDclDiagnosticsDataParticleRecovered')
        ]

        for file_name in os.listdir(RESOURCE_PATH):

            if file_name.endswith('.yml'):

                with open(os.path.join(RESOURCE_PATH, file_name), 'rU') as in_file_id:

                    out_file_name = file_name + '.new'
                    log.info('fixing file %s', file_name)
                    log.info('creating file %s', out_file_name)

                    out_file_id = open(os.path.join(RESOURCE_PATH, out_file_name), 'w')

                    for line in in_file_id:
                        new_line = line

                        for param_name, new_name, mult in param_change_table:

                            param_regex = r'\s+' + param_name + r':\s+(' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX
                            match = re.match(param_regex, line)
                            if match is not None:
                                new_value = int(float(match.group(1)) * mult)
                                new_line = '    ' + new_name + ':  ' + str(new_value) + '\n'
                                log.info('%s', new_line)

                        for old_class, new_class in class_change_table:

                            class_regex = r'\s+' + r'particle_object:\s+(' + old_class + ')' + END_OF_LINE_REGEX
                            match = re.match(class_regex, line)
                            if match is not None:
                                new_line = '    ' + 'particle_object:  ' + new_class + '\n'
                                log.info('%s', new_line)

                        out_file_id.write(new_line)

                    out_file_id.close()
