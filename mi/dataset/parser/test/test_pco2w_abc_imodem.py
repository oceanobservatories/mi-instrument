#!/usr/bin/env python

__author__ = 'mworden'

"""
@package mi.dataset.parser.test.test_pco2w_abc_imodem
@author Mark Worden
@brief Test code for the pco2w_abc_imodem parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.pco2w_abc.imodem.resource import RESOURCE_PATH
from mi.dataset.parser.pco2w_abc_imodem import Pco2wAbcImodemParser
from mi.dataset.parser.pco2w_abc_particles import \
    Pco2wAbcImodemMetadataTelemeteredDataParticle, \
    Pco2wAbcImodemMetadataRecoveredDataParticle, \
    Pco2wAbcImodemPowerTelemeteredDataParticle, \
    Pco2wAbcImodemPowerRecoveredDataParticle, \
    Pco2wAbcImodemInstrumentTelemeteredDataParticle, \
    Pco2wAbcImodemInstrumentRecoveredDataParticle, \
    Pco2wAbcImodemInstrumentBlankTelemeteredDataParticle, \
    Pco2wAbcImodemInstrumentBlankRecoveredDataParticle, \
    Pco2wAbcImodemControlTelemeteredDataParticle, \
    Pco2wAbcImodemControlRecoveredDataParticle, \
    Pco2wAbcParticleClassKey
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.logging import log


@attr('UNIT', group='mi')
class Pco2wAbcParserUnitTestCase(ParserUnitTestCase):
    """
    pco2w_abc Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._telem_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS:
                    Pco2wAbcImodemMetadataTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS:
                    Pco2wAbcImodemPowerTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    Pco2wAbcImodemInstrumentTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
                    Pco2wAbcImodemInstrumentBlankTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.CONTROL_PARTICLE_CLASS:
                    Pco2wAbcImodemControlTelemeteredDataParticle,
            }
        }

        self._recov_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS:
                    Pco2wAbcImodemMetadataRecoveredDataParticle,
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS:
                    Pco2wAbcImodemPowerRecoveredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    Pco2wAbcImodemInstrumentRecoveredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
                    Pco2wAbcImodemInstrumentBlankRecoveredDataParticle,
                Pco2wAbcParticleClassKey.CONTROL_PARTICLE_CLASS:
                    Pco2wAbcImodemControlRecoveredDataParticle,
            }
        }

    def test_happy_path(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST HAPPY PATH =====')

        num_particles_to_request = 10
        num_expected_particles = 7

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1624.DAT'), 'r') as file_handle:


            parser = Pco2wAbcImodemParser(self._telem_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1624.telem.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1624.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._recov_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1624.recov.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        log.debug('===== END TEST HAPPY PATH =====')

    def test_invalid_data_telem(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that invalid data is handled appropriately with the
        correct exceptions being reported.
        """
        log.debug('===== START TEST INVALID DATA TELEMETERED =====')

        num_particles_to_request = 10
        num_expected_particles = 7

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1625.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._telem_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1625.telem.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 2)

            for exception in self.exception_callback_value:
                self.assertIsInstance(exception, RecoverableSampleException)

        log.debug('===== END TEST INVALID DATA TELEMETERED =====')

    def test_invalid_data_recov(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that invalid data is handled appropriately with the
        correct exceptions being reported.
        """
        log.debug('===== START TEST INVALID DATA RECOVERED =====')

        num_particles_to_request = 10
        num_expected_particles = 7

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1625.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._recov_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1625.recov.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 2)

            for exception in self.exception_callback_value:
                self.assertIsInstance(exception, RecoverableSampleException)

        log.debug('===== END TEST INVALID DATA RECOVERED =====')

    def test_incomplete_metadata_one(self):
        """
        Read a file containing insufficient data to create a metadata particle.
        In this case, the line specifying the sample count is missing.
        Verify that the contents of the particles are correct ensuring no metadata
        particle was generated.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST INCOMPLETE METADATA ONE =====')

        num_particles_to_request = 10
        num_expected_particles = 7

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1626.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._telem_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1626.telem.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1626.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._recov_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1626.recov.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        log.debug('===== END TEST INCOMPLETE METADATA ONE =====')

    def test_incomplete_metadata_two(self):
        """
        Read a file containing insufficient data to create a metadata particle.
        In this case, the line specifying the serial number is missing.
        Verify that the contents of the particles are correct ensuring no metadata
        particle was generated.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST INCOMPLETE METADATA TWO =====')

        num_particles_to_request = 10
        num_expected_particles = 7

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1627.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._telem_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1627.telem.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1627.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._recov_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1627.recov.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        log.debug('===== END TEST INCOMPLETE METADATA TWO =====')

    def test_missing_file_time_telem(self):
        """
        Read a file that is missing the file time metadata
        A RecoverableException should be reported.
        """
        log.debug('===== START TEST MISSING FILE TIME TELEM =====')

        num_particles_to_request = 10
        num_expected_particles = 6

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1628.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._telem_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1628.telem.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)

            for exception in self.exception_callback_value:
                self.assertIsInstance(exception, RecoverableSampleException)

        log.debug('===== END TEST MISSING FILE TIME TELEM =====')

    def test_missing_file_time_recov(self):
        """
        Read a file that is missing the file time metadata
        A RecoverableException should be reported.
        """
        log.debug('===== START TEST MISSING FILE TIME RECOV =====')

        num_particles_to_request = 10
        num_expected_particles = 6

        with open(os.path.join(RESOURCE_PATH, 'pco2wXYZ_11212014_1628.DAT'), 'r') as file_handle:

            parser = Pco2wAbcImodemParser(self._recov_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "pco2wXYZ_11212014_1628.recov.yml", RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)

            for exception in self.exception_callback_value:
                self.assertIsInstance(exception, RecoverableSampleException)

        log.debug('===== END TEST MISSING FILE TIME RECOV =====')
