#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_fuelcell_eng_dcl.py
@author Chris Goodrich
@brief Test code for the fuelcell_eng_dcl parser
Release notes:

initial release
"""

__author__ = 'cgoodrich'

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import ConfigurationException
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.fuelcell_eng.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.fuelcell_eng_dcl import FuelCellEngDclParser
from mi.dataset.parser.fuelcell_eng_dcl import FuelCellEngDclParticleClassKey,\
    FuelCellEngDclDataParticleRecovered,\
    FuelCellEngDclDataParticleTelemetered
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.logging import log


@attr('UNIT', group='mi')
class FuelCellEngDclParserUnitTestCase(ParserUnitTestCase):
    """
    fuelcell_eng_dcl Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._recovered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.fuelcell_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                FuelCellEngDclParticleClassKey.ENGINEERING_DATA_PARTICLE_CLASS: FuelCellEngDclDataParticleRecovered
            }
        }

        self._telemetered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.fuelcell_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                FuelCellEngDclParticleClassKey.ENGINEERING_DATA_PARTICLE_CLASS: FuelCellEngDclDataParticleTelemetered
            }
        }

        self._incomplete_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.fuelcell_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self._bad_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.fuelcell_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {}
        }

    def test_simple(self):
        """
        Read file and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        This is the happy path.
        """
        log.debug('===== START TEST SIMPLE =====')

        num_particles_to_request = 25
        num_expected_particles = 20

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, '20141207s.pwrsys.log'), 'rU') as file_handle:

            parser = FuelCellEngDclParser(self._recovered_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20141207s.pwrsys.yml', RESOURCE_PATH)

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, '20141207s.pwrsys.log'), 'rU') as file_handle:

            parser = FuelCellEngDclParser(self._telemetered_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'telemetered_20141207s.pwrsys.yml', RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE =====')

    def test_bigfile(self):
        """
        Read file and verify that all expected particles can be read.
        Verify that the expected number of particles are produced.
        Only one test is run as the content of the input files is the
        same for recovered or telemetered.
        """
        log.debug('===== START TEST BIGFILE =====')

        num_particles_to_request = num_expected_particles = 870

        with open(os.path.join(RESOURCE_PATH, '20141207.pwrsys.log'), 'rU') as file_handle:

            parser = FuelCellEngDclParser(self._recovered_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

        log.debug('===== END TEST BIGFILE =====')

    def test_bad_checksum(self):
        """
        Read file and verify that all expected particles can be read.
        There are two lines with bad checksums in the file. The checksum
        after the colon is incorrect on lines 10 and 23 of the input file.
        Only one test is run as the content of the input files is the
        same for recovered or telemetered.
        """
        log.debug('===== START TEST BAD CHECKSUM =====')

        num_particles_to_request = num_expected_particles = 18

        with open(os.path.join(RESOURCE_PATH, '20141207s_bcs.pwrsys.log'), 'rU') as file_handle:

            parser = FuelCellEngDclParser(self._recovered_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

        log.debug('===== END TEST BAD CHECKSUM =====')

    def test_badly_formed(self):
        """
        Read file and verify that all expected particles can be read.
        Line 1 Improperly formatted - No particle generated
        Line 2 Improperly formatted - No particle generated
        Line 9 - Bad checksum - No particle generated
        No fuel cell data present on line 11 - No particle generated
        No fuel cell data present on line 12 - No particle generated
        No fuel cell data present on line 13 - No particle generated
        No fuel cell data present on line 14 - No particle generated
        No fuel cell data present on line 15 - No particle generated
        Line 20 - Bad checksum - No particle generated
        Line 24 Improperly formatted - No particle generated
        Line 26 Improperly formatted - No particle generated
        Line 27 Improperly formatted - No particle generated
        Line 28 Bad/Missing Timestamp - No particle generated
        Line 29 Bad/Missing Timestamp - No particle generated
        Line 30 No data found  - No particle generated
        Line 31 No terminator found - No particle generated
        Line 32 Improper format - No particle generated
        Only one test is run as the content of the input files
        is the same for recovered or telemetered.
        """
        log.debug('===== START TEST BADLY FORMED =====')

        num_particles_to_request = 33
        num_expected_particles = 16

        with open(os.path.join(RESOURCE_PATH, '20141207_badform.pwrsys.log'), 'rU') as file_handle:

            parser = FuelCellEngDclParser(self._recovered_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

        log.debug('===== END TEST BADLY FORMED =====')

    def test_bad_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST BAD CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, '20141207s.pwrsys.log'), 'rU') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = FuelCellEngDclParser(self._bad_parser_config,
                                              file_handle,
                                              self.exception_callback)

        log.debug('===== END TEST BAD CONFIGURATION =====')

    def test_partial_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST PARTIAL CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, '20141207s.pwrsys.log'), 'rU') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = FuelCellEngDclParser(self._incomplete_parser_config,
                                              file_handle,
                                              self.exception_callback)

        log.debug('===== END TEST PARTIAL CONFIGURATION =====')

    def test_blank_line(self):
        """
        Read file and verify that all expected particles can be read.
        Verify that the contents of the particles are correct. There are
        blank lines interspersed in the file. This test verifies that
        these blank lines do not adversely affect the parser. Only one
        test is run as the content of the input files is the same for
        recovered or telemetered.
        """
        log.debug('===== START TEST BLANK LINE =====')

        num_particles_to_request = 25
        num_expected_particles = 20

        with open(os.path.join(RESOURCE_PATH, '20141207sbl.pwrsys.log'), 'rU') as file_handle:

            parser = FuelCellEngDclParser(self._recovered_parser_config,
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

        log.debug('===== END TEST BLANK LINE =====')
