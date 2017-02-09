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
from mi.dataset.driver.velpt_ab.resource import RESOURCE_PATH
from mi.dataset.parser.common_regexes import FLOAT_REGEX, END_OF_LINE_REGEX
from mi.dataset.parser.velpt_ab import VelptAbParser, VelptAbParticleClassKey
from mi.dataset.parser.velpt_ab_particles import VelptAbInstrumentDataParticle,\
    VelptAbDiagnosticsHeaderParticle, VelptAbDiagnosticsDataParticle, VelptAbInstrumentMetadataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.logging import log


@attr('UNIT', group='mi')
class VelptAbParserUnitTestCase(ParserUnitTestCase):
    """
    velpt_ab_dcl Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                VelptAbParticleClassKey.METADATA_PARTICLE_CLASS: VelptAbDiagnosticsHeaderParticle,
                VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS: VelptAbDiagnosticsDataParticle,
                VelptAbParticleClassKey.INSTRUMENT_METADATA_PARTICLE_CLASS: VelptAbInstrumentMetadataParticle,
                VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS: VelptAbInstrumentDataParticle
            }
        }

        self._incomplete_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self._bad_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {}
        }

    def test_simple(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        This is the happy path test.
        """
        log.debug('===== START TEST SIMPLE =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE =====')

    def test_jumbled(self):
        """
        Read files and verify that all expected particles can be read.
        This particular data file has the velocity data records
        preceded by the diagnostics records, a situation not likely
        to occur on a deployed instrument but anything is possible!
        The logic in the parser will not produce an instrument metadata
        particle (configuration data) until it encounters a velocity or
        a diagnostics record. Assumes that all the configuration records are
        at the beginning of the file. This is reasonable as the instrument is
        configured before being deployed. So the config records would be stored
        first. Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST SIMPLE NOT IN ORDER =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'jumbled_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'jumbled_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE NOT IN ORDER =====')

    def test_too_few_diagnostics_records(self):
        """
        The file used in this test has only 19 diagnostics records in the second set.
        Twenty are expected. The records are all still processed.
        The error is simply noted.
        """
        log.debug('===== START TEST NOT ENOUGH DIAGNOSTICS RECORDS =====')

        with open(os.path.join(RESOURCE_PATH, 'too_few_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 71

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_few_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST NOT ENOUGH DIAGNOSTICS RECORDS =====')

    def test_too_many_diagnostics_records(self):
        """
        The file used in this test has 21 diagnostics records in the second set.
        Twenty are expected. The records are all still processed.
        The error is simply noted.
        """
        log.debug('===== START TEST TOO MANY DIAGNOSTICS RECORDS =====')

        with open(os.path.join(RESOURCE_PATH, 'too_many_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 73

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_many_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST TOO MANY DIAGNOSTICS RECORDS =====')

    def test_invalid_sync_byte(self):
        """
        The file used in this test has extra bytes between records which need to be skipped
        in order to process the correct number of particles. All records are still processed.
        """
        log.debug('===== START TEST INVALID SYNC BYTE =====')

        with open(os.path.join(RESOURCE_PATH, 'extra_bytes_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID SYNC BYTE =====')

    def test_invalid_record_id(self):
        """
        The file used in this test has one record with an invalid ID byte.
        This results in 71 particles being retrieved instead of 72.
        """
        log.debug('===== START TEST INVALID RECORD ID =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_id_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 71

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_id_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID RECORD ID =====')

    def test_truncated_file(self):
        """
        The file used in this test has a malformed (too short) record at
        the end of the file.This results in 71 particles being retrieved
        instead of 72.
        """
        log.debug('===== START TEST FOUND TRUNCATED FILE =====')

        with open(os.path.join(RESOURCE_PATH, 'truncated_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 71

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'truncated_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND TRUNCATED FILE =====')

    def test_bad_velocity_checksum(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 71 particles being retrieved instead of 72.
        """
        log.debug('===== START TEST FOUND BAD VELOCITY CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_velocity_checksum_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as \
                file_handle:

            num_particles_to_request = num_expected_particles = 71

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_velocity_checksum_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD VELOCITY CHECKSUM =====')

    def test_bad_diagnostic_checksum(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 71 particles being retrieved instead of 72.
        """
        log.debug('===== START TEST FOUND BAD DIAGNOSTICS CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_diag_checksum_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 71

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_few_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAGNOSTICS CHECKSUM =====')

    def test_missing_hardware_config(self):
        """
        The file used in this test has no hardware configuration record.
        Instrument metadata will still be produced but the fields from
        the hardware config will NOT be included.
        """
        log.debug('===== START TEST MISSING HARDWARE CONFIG =====')

        with open(os.path.join(RESOURCE_PATH, 'no_hardware_config_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_hardware_config_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING HARDWARE CONFIG =====')

    def test_missing_head_config(self):
        """
        The file used in this test has no head configuration record.
        Instrument metadata will still be produced but the fields from
        the head config will NOT be included.
        """
        log.debug('===== START TEST MISSING HEAD CONFIG =====')

        with open(os.path.join(RESOURCE_PATH, 'no_head_config_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_head_config_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING HEAD CONFIG =====')

    def test_missing_user_config(self):
        """
        The file used in this test has no user configuration record.
        Instrument metadata will still be produced but the fields from
        the user config will NOT be included.
        """
        log.debug('===== START TEST MISSING USER CONFIG =====')

        with open(os.path.join(RESOURCE_PATH, 'no_user_config_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_user_config_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING USER CONFIG =====')

    def test_missing_all_config(self):
        """
        The file used in this test has no user configuration record.
        Instrument metadata will still be produced but the fields from
        the user config will NOT be included.
        """
        log.debug('===== START TEST MISSING ALL CONFIG RECORDS =====')

        with open(os.path.join(RESOURCE_PATH, 'no_config_recs_VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_config_recs_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING ALL CONFIG RECORDS =====')

    def test_head_config_bad_checksum(self):
        """
        The file used in this test has a bad checksum in the head configuration record.
        Instrument metadata will still be produced but the fields from
        the head config will NOT be included.
        """
        log.debug('===== START TEST HEAD CONFIG BAD CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_checksum_in_head_config_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_head_config_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST HEAD CONFIG BAD CHECKSUM =====')

    def test_hardware_config_bad_checksum(self):
        """
        The file used in this test has a bad checksum in the hardware configuration record.
        Instrument metadata will still be produced but the fields from
        the hardware config will NOT be included.
        """
        log.debug('===== START TEST HARDWARE CONFIG BAD CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_checksum_in_hardware_config_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_hardware_config_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST HARDWARE CONFIG BAD CHECKSUM =====')

    def test_user_config_bad_checksum(self):
        """
        The file used in this test has a bad checksum in the head configuration record.
        Instrument metadata will still be produced but the fields from
        the head config will NOT be included.
        """
        log.debug('===== START TEST USER CONFIG BAD CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_checksum_in_user_config_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_user_config_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST USER CONFIG BAD CHECKSUM =====')

    def test_diag_header_bad_checksum(self):
        """
        The file used in this test has a bad checksum in the head configuration record.
        Instrument metadata will still be produced but the fields from
        the head config will NOT be included.
        """
        log.debug('===== START TEST DIAGNOSTICS HEADER BAD CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_checksum_in_diag_header_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 71

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_diag_header_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST DIAGNOSTICS HEADER BAD CHECKSUM =====')

    def test_missing_diag_header(self):
        """
        The file used in this test has a bad checksum in the head configuration record.
        Instrument metadata will still be produced but the fields from
        the head config will NOT be included.
        """
        log.debug('===== START TEST MISSING DIAGNOSTICS HEADER =====')

        with open(os.path.join(RESOURCE_PATH, 'no_diag_header_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 71

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_diag_header_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING DIAGNOSTICS HEADER =====')

    def test_random_diag_record(self):
        """
        The file used in this test has a bad checksum in the head configuration record.
        Instrument metadata will still be produced but the fields from
        the head config will NOT be included.
        """
        log.debug('===== START TEST RANDOM DIAGNOSTIC RECORD FOUND =====')

        with open(os.path.join(RESOURCE_PATH, 'random_diag_record_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 72

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'random_diag_record_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST RANDOM DIAGNOSTIC RECORD FOUND =====')

    def test_no_diag_recs(self):
        """
        The file used in this test has a single diagnostic header record but no diagnostic
        records. No diagnostic particles will be produced.
        """
        log.debug('===== START TEST NO DIAGNOSTIC RECORDS FOUND =====')

        with open(os.path.join(RESOURCE_PATH, 'no_diag_recs_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'no_diag_recs_VELPT_SN_11402_2014-07-02.yml', RESOURCE_PATH)

        log.debug('===== END TEST NO DIAGNOSTIC RECORDS FOUND =====')

    def test_bad_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST BAD CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, 'VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = VelptAbParser(self._bad_parser_config,
                                       file_handle,
                                       self.exception_callback)

        log.debug('===== END TEST BAD CONFIGURATION =====')

    def test_partial_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST PARTIAL CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, 'VELPT_SN_11402_2014-07-02.aqd'), 'rb') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = VelptAbParser(self._incomplete_parser_config,
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

        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_19_diag_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 116

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_19_diag_VELPT_SN_11402_2014-07-02.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAG HDR CHECKSUM AND TOO FEW RECS =====')

    def test_bad_diag_checksum_21_recs(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD DIAG HDR CHECKSUM AND TOO MANY RECS =====')

        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_21_diag_VELPT_SN_11402_2014-07-02.aqd'), 'rb')\
                as file_handle:

            num_particles_to_request = num_expected_particles = 118

            parser = VelptAbParser(self._parser_config,
                                   file_handle,
                                   self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_21_diag_VELPT_SN_11402_2014-07-02.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAG HDR CHECKSUM AND TOO MANY RECS =====')

    def fix_yml_pressure_params(self):
        """
        This helper tool was used to modify the yml files in response to ticket #4341
        """

        pressure_regex = r'    pressure:\s+(0.\d+)'

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

                        out_file_id.write(new_line)

                    out_file_id.close()
