#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_ctdbp_p.py
@author Jeff Roy, Rene Gelinas
@brief Test code for a CTDBP P DCL Common data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.ctdbp_p.resource import RESOURCE_PATH
from mi.dataset.parser.ctdbp_p import CtdbpPCommonParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.ctdbp_p'

CTDBP_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdbpPRecoveredDataParticle'
}

DOSTA_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaAbcdjmCtdbpPRecoveredDataParticle'
}

FLORD_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordGCtdbpPRecoveredDataParticle'
}


@attr('UNIT', group='mi')
class CtdbpPParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_p Parser unit test suite
    """

    def test_ctdbp(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST 1 SIMPLE: CTDBP-P INSTRUMENT RECOVERED DATA =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp_p_test_1-3.hex'), 'rU') as file_handle:
            parser = CtdbpPCommonParser(CTDBP_RECOV_CONFIG, file_handle, self.exception_callback)

            particles = parser.get_records(2)

            # Make sure we obtained 3 particles
            self.assertTrue(len(particles) == 2)
            # Make sure there were no errors
            self.assertEqual(self.exception_callback_value, [])
            self.assert_particles(particles, 'ctdbp_p_test_1.exp_results.yml', RESOURCE_PATH)
        log.debug('===== END TEST 1 SIMPLE: CTDBP-P INSTRUMENT RECOVERED DATA =====')

    def test_dosta(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST 2 SIMPLE: ATTACHED DOSTA INSTRUMENT RECOVERED DATA =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp_p_test_1-3.hex'), 'rU') as file_handle:
            parser = CtdbpPCommonParser(DOSTA_RECOV_CONFIG, file_handle, self.exception_callback)

            particles = parser.get_records(2)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 2)
            # Make sure there were no errors
            self.assertEqual(self.exception_callback_value, [])
            self.assert_particles(particles, 'ctdbp_p_test_2.exp_results.yml', RESOURCE_PATH)
        log.debug('===== END TEST 2 SIMPLE: ATTACHED DOSTA INSTRUMENT RECOVERED DATA =====')

    def test_flord(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST 3 SIMPLE: ATTACHED DOSTA INSTRUMENT RECOVERED DATA =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, 'ctdbp_p_test_1-3.hex'), 'rU') as file_handle:
            parser = CtdbpPCommonParser(FLORD_RECOV_CONFIG, file_handle, self.exception_callback)

            particles = parser.get_records(2)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 2)
            # Make sure there were no errors
            self.assertEqual(self.exception_callback_value, [])
            self.assert_particles(particles, 'ctdbp_p_test_3.exp_results.yml', RESOURCE_PATH)
        log.debug('===== END TEST 3 SIMPLE: ATTACHED DOSTA INSTRUMENT RECOVERED DATA =====')

    def test_invalid_record(self):
        """
        The file used here has an invalid record with an incorrect length.
        """
        log.debug('===== START TEST 4: INVALID RECORD =====')
        # check error handling on an uncorrected Endurance data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, 'ctdbp_p_test_4.hex'), 'rU') as file_handle:

            parser = CtdbpPCommonParser(CTDBP_RECOV_CONFIG, file_handle, self.exception_callback)

            particles = parser.get_records(2)

            self.assertEquals(len(particles), 1)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))
        log.debug('===== END TEST 4: INVALID RECORD =====')

    def test_invalid_value(self):
        """
        The file used here has an invalid record with a non-hexadecimal character.
        """
        log.debug('===== START TEST 5: INVALID VALUE =====')
        # check error handling on an uncorrected Endurance data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, 'ctdbp_p_test_5.hex'), 'rU') as file_handle:

            parser = CtdbpPCommonParser(CTDBP_RECOV_CONFIG, file_handle, self.exception_callback)

            particles = parser.get_records(2)

            self.assertEquals(len(particles), 1)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))
        log.debug('===== END TEST 5: INVALID VALUE =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file has no instrument records.
        """
        log.debug('===== START TEST 6: NO PARTICLES =====')
        with open(os.path.join(RESOURCE_PATH, 'ctdbp_p_test_6.hex'), 'rU') as file_handle:
            parser = CtdbpPCommonParser(CTDBP_RECOV_CONFIG, file_handle, self.exception_callback)

            particles = parser.get_records(1)

            self.assertEquals(len(particles), 0)
        log.debug('===== END TEST 6: NO PARTICLES =====')
