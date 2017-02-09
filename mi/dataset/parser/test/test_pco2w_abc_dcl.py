#!/usr/bin/env python

__author__ = 'Joe Padula'

"""
@package mi.dataset.parser.test.test_pco2w_abc_dcl
@author Joe Padula
@brief Test code for the pco2w_abc_dcl parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.logging import log
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH, ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.pco2w_abc import Pco2wAbcParticleClassKey
from mi.dataset.parser.pco2w_abc_dcl import Pco2wAbcDclParser
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcDclInstrumentBlankRecoveredDataParticle, \
    Pco2wAbcDclMetadataRecoveredDataParticle, \
    Pco2wAbcDclPowerRecoveredDataParticle, \
    Pco2wAbcDclInstrumentRecoveredDataParticle, \
    Pco2wAbcDclMetadataTelemeteredDataParticle, \
    Pco2wAbcDclPowerTelemeteredDataParticle, \
    Pco2wAbcDclInstrumentTelemeteredDataParticle, \
    Pco2wAbcDclInstrumentBlankTelemeteredDataParticle

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH,
                             'pco2w_abc', 'dcl', 'resource')


@attr('UNIT', group='mi')
class Pco2wAbcDclParserUnitTestCase(ParserUnitTestCase):
    """
    pco2w_abc_dcl Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._recovered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS: Pco2wAbcDclMetadataRecoveredDataParticle,
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS: Pco2wAbcDclPowerRecoveredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS: Pco2wAbcDclInstrumentRecoveredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
                Pco2wAbcDclInstrumentBlankRecoveredDataParticle,
            }
        }

        self._telemetered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS: Pco2wAbcDclMetadataTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS: Pco2wAbcDclPowerTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS: Pco2wAbcDclInstrumentTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
                Pco2wAbcDclInstrumentBlankTelemeteredDataParticle,
            }
        }

    def test_happy_path_single(self):
        """
        Read a file and verify that a single record can be read.
        Verify that the contents of the particle is correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST HAPPY PATH SINGLE =====')

        # Recovered
        with open(os.path.join(RESOURCE_PATH, 'single.log'), 'r') as file_handle:

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback)

            particles = parser.get_records(1)

            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "rec_single.yml", RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        # Telemetered
        with open(os.path.join(RESOURCE_PATH, 'single.log'), 'r') as file_handle:

            parser = Pco2wAbcDclParser(self._telemetered_parser_config,
                                       file_handle,
                                       self.exception_callback)

            particles = parser.get_records(14)

            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "tel_single.yml", RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST HAPPY PATH SINGLE =====')

    def test_happy_path_many(self):
        """
        Read a file and verify that all records can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST HAPPY PATH MANY =====')

        # Recovered
        with open(os.path.join(RESOURCE_PATH, 'happy_path.log'), 'r') as file_handle:

            num_particles = 5

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback)

            particles = parser.get_records(num_particles)

            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "happy_path_rec.yml", RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        # Telemetered
        with open(os.path.join(RESOURCE_PATH, 'happy_path.log'), 'r') as file_handle:

            parser = Pco2wAbcDclParser(self._telemetered_parser_config,
                                       file_handle,
                                       self.exception_callback)

            particles = parser.get_records(num_particles)

            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "happy_path_tel.yml", RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST HAPPY PATH MANY =====')

    def test_invalid_metadata_timestamp(self):
        """
        The file used in this test has error in the timestamp for the first metadata record.
        This results in 4 particles being retrieved instead of 5, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INVALID METADATA TIMESTAMP =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_metadata_timestamp.log'), 'r') as file_handle:

            num_particles_to_request = 5
            num_expected_particles = 4

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback,
                                       None,
                                       None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "invalid_metadata_timestamp.yml", RESOURCE_PATH)

            log.debug('Exceptions : %s', self.exception_callback_value)

            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        log.debug('===== END TEST INVALID METADATA TIMESTAMP =====')

    def test_invalid_record_type(self):
        """
        The file used in this test has a record type in the second record that does not match any
        of the expected record types.
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INVALID RECORD TYPE =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_record_type.log'), 'r') as file_handle:

            num_particles_to_request = 6
            num_expected_particles = 6

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback,
                                       None,
                                       None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "invalid_record_type.yml", RESOURCE_PATH)

            log.debug('Exceptions : %s', self.exception_callback_value)

            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        log.debug('===== END TEST INVALID RECORD TYPE =====')

    def test_power_record_missing_timestamp(self):
        """
        The file used in this test has a power record (the second record) with a missing timestamp.
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST POWER RECORD MISSING TIMESTAMP =====')

        with open(os.path.join(RESOURCE_PATH, 'power_record_missing_timestamp.log'), 'r') as file_handle:

            num_particles_to_request = 6
            num_expected_particles = 6

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback,
                                       None,
                                       None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "power_record_missing_timestamp.yml", RESOURCE_PATH)

            log.debug('Exceptions : %s', self.exception_callback_value)

            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        log.debug('===== END TEST POWER RECORD MISSING TIMESTAMP =====')

    def test_no_particles(self):
        """
        The file used in this test only contains DCL Logging records.
        Verify that no particles are produced if the input file
        has no instrument data records, i.e., they just contain DCL Logging records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, 'no_particles.log'), 'r') as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 0

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback,
                                       None,
                                       None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST NO PARTICLES =====')

    def test_incorrect_length(self):
        """
        The last five records in the file used in this test have a length that does not match the Length
        field in the record. This tests for this requirement:
        If the beginning of another instrument data record (* character), is encountered before "Length"
        bytes have been found, where "Length" is the record length specified in a record, then we can not
        reliably parse the record.
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INCORRECT LENGTH =====')

        with open(os.path.join(RESOURCE_PATH, 'incorrect_data_length.log'), 'r') as file_handle:

            num_particles_to_request = 6
            num_expected_particles = 5

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback,
                                       None,
                                       None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "incorrect_data_length.yml", RESOURCE_PATH)

        log.debug('Exceptions : %s', self.exception_callback_value)

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        log.debug('===== END TEST INCORRECT LENGTH =====')

    def test_invalid_checksum(self):
        """
        The last five records in the file used in this test have a length that does not match the Length
        field in the record. This tests for this requirement:
        If the beginning of another instrument data record (* character), is encountered before "Length"
        bytes have been found, where "Length" is the record length specified in a record, then we can not
        reliably parse the record.
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INVALID CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_checksum.log'), 'r') as file_handle:

            num_particles_to_request = 1
            num_expected_particles = 1

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback,
                                       None,
                                       None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "invalid_checksum.yml", RESOURCE_PATH)

            # No exception should be thrown
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST INVALID CHECKSUM =====')

    def test_real_file(self):
        """
        The file used in this test, is a real file from the IDD. It contains 14 records:
        7 power records, 6 CO2 records (normal) and 1 CO2 record (blank).
        (No control files are in the real file.)
        Verify that the correct number of particles are generated
        from a real file.
        """
        log.debug('===== START TEST REAL FILE =====')

        with open(os.path.join(RESOURCE_PATH, '20140507.pco2w1.log'), 'r') as file_handle:

            num_particles_to_request = 2500
            num_expected_particles = 14

            parser = Pco2wAbcDclParser(self._recovered_parser_config,
                                       file_handle,
                                       self.exception_callback,
                                       None,
                                       None)

            particles = parser.get_records(num_particles_to_request)

            log.info(len(particles))

            self.assertEquals(len(particles), num_expected_particles)

            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST REAL FILE =====')
