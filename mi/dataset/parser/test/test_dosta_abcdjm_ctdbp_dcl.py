#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_dosta_abcdjm_ctdbp_dcl.py
@author Jeff Roy
@brief Test code for a dosta_d_ctdbp_dcl data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.driver.dosta_abcdjm.ctdbp.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.dosta_abcdjm_ctdbp_dcl import DostaAbcdjmCtdbpDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()
MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_dcl_ce'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class DostaDCtdbpDclCeParserUnitTestCase(ParserUnitTestCase):
    """
    dosta_d_ctdbp_dcl Parser unit test suite
    """

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== TEST SIMPLE =====')
        # test the corrected file format, use the recovered path

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_1rec_corr1st.log'), 'rU') as file_handle:
            parser = DostaAbcdjmCtdbpDclParser(False,
                                               file_handle,
                                               self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particles
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20140930.dosta1_1rec_corr.yml', RESOURCE_PATH)

        # test the corrected file format, use the telemetered path
        # second variant
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_1rec_corr2nd.log'), 'rU') as file_handle:
            parser = DostaAbcdjmCtdbpDclParser(True,
                                               file_handle,
                                               self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particles
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20140930.dosta1_1tel_corr.yml', RESOURCE_PATH)

        # test with uncorrected Endurance data
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_1rec_uncorr.log'), 'rU') as file_handle:
            parser = DostaAbcdjmCtdbpDclParser(True,
                                               file_handle,
                                               self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(1)

            # Make sure we obtained no particles
            self.assertTrue(len(particles) == 0)

        # test with Pioneer data
        # Note: Pioneer data files should not be sent to this parser but the parser was coded to avoid
        # unnecessary error messages being produced if they are.
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec.log'), 'rU') as file_handle:
            parser = DostaAbcdjmCtdbpDclParser(True,
                                               file_handle,
                                               self.exception_callback)

            # Get a single data record using the telemetered path
            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 0)
        log.debug('===== END TEST SIMPLE =====')

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST MANY =====')

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant_many.log'), 'rU') as file_handle:
            parser = DostaAbcdjmCtdbpDclParser(False,
                                               file_handle,
                                               self.exception_callback)
            particles = parser.get_records(14)
            # Make sure we obtained 7 particles
            self.assertTrue(len(particles) == 7)
            self.assert_particles(particles, "20140930.ctdbp1_many_corr.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr2ndVariant_many.log'), 'rU') as file_handle:
            parser = DostaAbcdjmCtdbpDclParser(False,
                                               file_handle,
                                               self.exception_callback)
            particles = parser.get_records(14)
            # Make sure we obtained 7 particles
            self.assertTrue(len(particles) == 7)
            self.assert_particles(particles, "20140930.ctdbp1_many_corr.yml", RESOURCE_PATH)

        log.debug('===== END TEST MANY =====')

    def test_invalid_record(self):
        """
        The file used here has a damaged tide record ( missing datum )
        """
        log.debug('===== START TEST INVALID RECORD =====')

        # check error handling on a truncated, corrected file
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant_many_broken.log'), 'rU') as file_handle:

            num_particles_to_request = 14
            num_expected_particles = 6

            parser = DostaAbcdjmCtdbpDclParser(False,
                                               file_handle,
                                               self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "20140930.dosta1_many_corr_broken.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        log.debug('===== END TEST INVALID TIDE RECORD =====')

    def test_bug_11368(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, '20161005.ctdbp2.log'), 'rU') as file_handle:
            parser = DostaAbcdjmCtdbpDclParser(False,
                                               file_handle,
                                               self.exception_callback)

            particles = parser.get_records(25)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 24)
            self.assertEquals(len(self.exception_callback_value), 0)
