#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_ctdbp_p_dcl.py
@author Jeff Roy
@brief Test code for a CTDBP P DCL Common data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.ctdbp_p.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.ctdbp_p_dcl import CtdbpPDclCommonParser
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()

MODULE_NAME = 'mi.dataset.parser.ctdbp_p_dcl'

CTDBP_TELEM_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdbpPDclTelemeteredDataParticle'
}

CTDBP_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdbpPDclRecoveredDataParticle'
}

DOSTA_TELEM_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaAbcdjmCtdbpPDclTelemeteredDataParticle'
}

DOSTA_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaAbcdjmCtdbpPDclRecoveredDataParticle'
}

FLORD_TELEM_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordGCtdbpPDclTelemeteredDataParticle'
}

FLORD_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordGCtdbpPDclRecoveredDataParticle'
}


@attr('UNIT', group='mi')
class CtdbpPDclParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_p_dcl Parser unit test suite
    """

    def test_ctdbp(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== TEST SIMPLE: TELEM =====')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734.DAT'), 'rU') as file_handle:
            parser = CtdbpPDclCommonParser(CTDBP_TELEM_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            # Make sure there were no errors
            self.assertEqual(self.exception_callback_value, [])
            self.assert_particles(particles, 'ctdbp01_20150804_061734_ctdbp_telemetered.yml', RESOURCE_PATH)

        log.debug('===== TEST SIMPLE: RECOV =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734.DAT'), 'rU') as file_handle:
            parser = CtdbpPDclCommonParser(CTDBP_RECOV_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            # Make sure there were no errors
            self.assertEqual(self.exception_callback_value, [])
            self.assert_particles(particles, 'ctdbp01_20150804_061734_ctdbp_recovered.yml', RESOURCE_PATH)

    def test_dosta(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== TEST SIMPLE: TELEM =====')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734.DAT'), 'rU') as file_handle:
            parser = CtdbpPDclCommonParser(DOSTA_TELEM_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, 'ctdbp01_20150804_061734_dosta_telemetered.yml', RESOURCE_PATH)

        log.debug('===== TEST SIMPLE: RECOV =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734.DAT'), 'rU') as file_handle:
            parser = CtdbpPDclCommonParser(DOSTA_RECOV_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            # Make sure there were no errors
            self.assertEqual(self.exception_callback_value, [])
            self.assert_particles(particles, 'ctdbp01_20150804_061734_dosta_recovered.yml', RESOURCE_PATH)

    def test_flord(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== TEST SIMPLE: TELEM =====')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734.DAT'), 'rU') as file_handle:
            parser = CtdbpPDclCommonParser(FLORD_TELEM_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, 'ctdbp01_20150804_061734_flord_telemetered.yml', RESOURCE_PATH)

        log.debug('===== TEST SIMPLE: RECOV =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734.DAT'), 'rU') as file_handle:
            parser = CtdbpPDclCommonParser(FLORD_RECOV_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            # Make sure there were no errors
            self.assertEqual(self.exception_callback_value, [])
            self.assert_particles(particles, 'ctdbp01_20150804_061734_flord_recovered.yml', RESOURCE_PATH)

    def test_invalid_record(self):
        """
        The file used here has a damaged tide record ( missing datum )
        """
        log.debug('===== START TEST INVALID RECORD =====')

        # check error handling on an uncorrected Endurance data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734_inval.DAT'), 'rU') as file_handle:

            parser = CtdbpPDclCommonParser(CTDBP_TELEM_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(1)

            self.assertEquals(len(particles), 0)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, 'ctdbp01_20150804_061734_norec.DAT'), 'rU') as file_handle:

            num_particles_to_request = 1
            num_expected_particles = 0

            parser = CtdbpPDclCommonParser(CTDBP_TELEM_CONFIG,
                                           file_handle,
                                           self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

        log.debug('===== END TEST NO PARTICLES =====')

