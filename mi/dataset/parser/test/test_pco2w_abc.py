#!/usr/bin/env python

__author__ = 'mworden'

"""
@package mi.dataset.parser.test.test_pco2w_abc
@author Mark Worden
@brief Test code for the pco2w_abc parser
"""

import os

from nose.plugins.attrib import attr

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.pco2w_abc.resource import RESOURCE_PATH
from mi.dataset.parser.pco2w_abc import Pco2wAbcParser, Pco2wAbcParticleClassKey
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcMetadataDataParticle, Pco2wAbcPowerDataParticle, \
    Pco2wAbcInstrumentDataParticle, Pco2wAbcInstrumentBlankDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.logging import log


@attr('UNIT', group='mi')
class Pco2wAbcParserUnitTestCase(ParserUnitTestCase):
    """
    pco2w_abc Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._exception_occurred = False

        self._parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS: Pco2wAbcMetadataDataParticle,
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS: Pco2wAbcPowerDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS: Pco2wAbcInstrumentDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS: Pco2wAbcInstrumentBlankDataParticle,
            }
        }

    def exception_callback(self, exception):
        log.debug(exception)
        self._exception_occurred = True

    def test_happy_path(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST HAPPY PATH =====')

        with open(os.path.join(RESOURCE_PATH, 'happy_path.txt'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = NUM_EXPECTED_PARTICLES = 10

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    self.exception_callback,
                                    None,
                                    None)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "happy_path.yml", RESOURCE_PATH)

            self.assertEqual(self._exception_occurred, False)

        log.debug('===== END TEST HAPPY PATH =====')

    def test_invalid_metadata_timestamp(self):
        """
        The file used in this test has error in the timestamp for the first metadata record.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INVALID METADATA TIMESTAMP =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_metadata_timestamp.txt'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 10
            NUM_EXPECTED_PARTICLES = 9

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    self.exception_callback,
                                    None,
                                    None)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "invalid_metadata_timestamp.yml", RESOURCE_PATH)

            self.assertEqual(self._exception_occurred, True)

        log.debug('===== END TEST INVALID METADATA TIMESTAMP =====')

    def test_invalid_record_type(self):
        """
        The file used in this test has a record type that does not match any of the expected record types.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INVALID RECORD TYPE =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_record_type.txt'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 10
            NUM_EXPECTED_PARTICLES = 9

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    self.exception_callback,
                                    None,
                                    None)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "invalid_record_type.yml", RESOURCE_PATH)

            self.assertEqual(self._exception_occurred, True)

        log.debug('===== END TEST INVALID RECORD TYPE =====')

    def test_power_record_missing_timestamp(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST POWER RECORD MISSING TIMESTAMP =====')

        with open(os.path.join(RESOURCE_PATH, 'power_record_missing_timestamp.txt'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 10
            NUM_EXPECTED_PARTICLES = 9

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    self.exception_callback,
                                    None,
                                    None)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "power_record_missing_timestamp.yml", RESOURCE_PATH)

            self.assertEqual(self._exception_occurred, True)

        log.debug('===== END TEST POWER RECORD MISSING TIMESTAMP =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, 'no_particles.txt'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 10
            NUM_EXPECTED_PARTICLES = 0

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    self.exception_callback,
                                    None,
                                    None)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assertEqual(self._exception_occurred, False)

        log.debug('===== END TEST NO PARTICLES =====')

    def test_real_file(self):
        """
        Verify that the correct number of particles are generated
        from a real file.
        """
        log.debug('===== START TEST REAL FILE =====')

        with open(os.path.join(RESOURCE_PATH, 'SAMI_C0069_300614.txt'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 2500
            NUM_EXPECTED_PARTICLES = 2063

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    self.exception_callback,
                                    None,
                                    None)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            log.info(len(particles))

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assertEqual(self._exception_occurred, False)

        log.debug('===== END TEST REAL FILE =====')
