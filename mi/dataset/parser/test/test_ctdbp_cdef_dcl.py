#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_ctdbp_cdef_dcl.py
@author Jeff Roy
@brief Test code for a ctdbp_cdef_dcl data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import \
    RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.driver.ctdbp_cdef.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.ctdbp_cdef_dcl import CtdbpCdefDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()
MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_dcl'


@attr('UNIT', group='mi')
class CtdbpCdefDclParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_cdef_dcl Parser unit test suite
    """

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== TEST SIMPLE: UNCORR ENDURANCE TELEM =====')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_1rec_uncorr.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20140918.ctdbp_1rec_uncorr_t.yml', RESOURCE_PATH)

        log.debug('===== TEST SIMPLE: UNCORR ENDURANCE RECOV =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_1rec_uncorr.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(1)

        # Make sure we obtained 1 particle
        self.assertTrue(len(particles) == 1)
        self.assert_particles(particles, '20140918.ctdbp_1rec_uncorr_r.yml', RESOURCE_PATH)

        # test the corrected Endurance file format, use the recovered path
        log.debug('===== TEST SIMPLE: CORR ENDURANCE V1 =====')
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_1rec_corr1st.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20140930.ctdbp1_1rec_corr.yml', RESOURCE_PATH)

        # test the corrected file format, use the recovered path
        log.debug('===== TEST SIMPLE: CORR ENDURANCE V2 =====')
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_1rec_corr2nd.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(1)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20140930.ctdbp1_1rec_corr.yml', RESOURCE_PATH)

        # test the telemetered uncorrected format from Pioneer
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            # Get a single data record using the telemetered path
            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20131123.ctdbp1_1rec.yml', RESOURCE_PATH)

        # test the recovered uncorrected format from Pioneer
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            # grab a record from the recovered path
            particles = parser.get_records(1)

        # Make sure we obtained 1 particle
        self.assertTrue(len(particles) == 1)

        self.assert_particles(particles, '20131123.ctdbp1_1rec_r.yml', RESOURCE_PATH)

        # test the corrected file format, use the recovered path
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec_c.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            # Grab a record of the corrected format, using the recovered path
            particles = parser.get_records(1)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 1)

            self.assert_particles(particles, '20131123.ctdbp1_1rec_r.yml', RESOURCE_PATH)
        log.debug('===== END TEST SIMPLE =====')

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST MANY =====')
        # test with uncorrected Endurance data, telemetered
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_many.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)
            particles = parser.get_records(14)

        # Make sure we obtained 24 particles
        self.assertTrue(len(particles) == 14)
        self.assert_particles(particles, "20140918.ctdbp_many_uncorr_t.yml", RESOURCE_PATH)

        # test with corrected Endurance data, recovered
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant_many.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(7)
            # Make sure we obtained 7 particles
            self.assertTrue(len(particles) == 7)
            self.assert_particles(particles, "20140930.ctdbp1_many_corr.yml", RESOURCE_PATH)

        # test with corrected Endurance data 2nd variant, recovered
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr2ndVariant_many.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(7)
            # Make sure we obtained 7 particles
            self.assertTrue(len(particles) == 7)
            self.assert_particles(particles, "20140930.ctdbp1_many_corr.yml", RESOURCE_PATH)

        # test with uncorrected Pioneer data, telemetered
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)
            particles = parser.get_records(24)

            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assert_particles(particles, "20131123.ctdbp1_many_telemetered.yml", RESOURCE_PATH)

        # test with uncorrected Pioneer data, recovered
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(24)
            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assert_particles(particles, "20131123.ctdbp1_many_recovered.yml", RESOURCE_PATH)

        # test with corrected Pioneer data, recovered
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_corrected.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)
            particles = parser.get_records(24)
            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assert_particles(particles, "20131123.ctdbp1_many_recovered.yml", RESOURCE_PATH)

        log.debug('===== END TEST MANY =====')

    def test_long_stream(self):
        """
        Test a long stream
        """
        log.debug('===== START TEST LONG STREAM =====')

        # tests with endurance files
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(291)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 291)

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(18)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 18)

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr2ndVariant.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(18)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 18)

        # test with Pioneer file
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1.log'), 'r') as file_handle:

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(3389)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 3389)

        log.debug('===== END TEST LONG STREAM =====')

    def test_invalid_record(self):
        """
        The file used here has a damaged tide record ( missing datum )
        """
        log.debug('===== START TEST INVALID RECORD =====')

        # check error handling on an uncorrected Endurance data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_many_broken.log'), 'rU') as file_handle:

            num_particles_to_request = 14
            num_expected_particles = 13

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20140918.ctdbp_many_uncorr_t_broken.yml", RESOURCE_PATH)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        # similarly, check error handling on a truncated, corrected Endurance file
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant_many_broken.log'), 'rU') as file_handle:

            num_particles_to_request = 14
            num_expected_particles = 6

            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "20140930.ctdbp1_many_corr_broken.yml", RESOURCE_PATH)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        # check error handling on an uncorrected Pioneer data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_1inval.log'), 'rU') as file_handle:

            num_particles_to_request = 24
            num_expected_particles = 23

            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "20131123.ctdbp1_many_recovered_1inval.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        # similarly, check error handling on a truncated, corrected Pioneer file
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_corrected_1inval.log'), 'rU') as file_handle:

            num_particles_to_request = 24
            num_expected_particles = 23

            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "20131123.ctdbp1_many_recovered_1inval.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
        log.debug('===== END TEST INVALID TIDE RECORD =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        # test with Endurance file
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_0rec_corr2nd.log'), 'rU') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 0

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)
            self.assertEquals(len(self.exception_callback_value), 0)

        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_0rec.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 0

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

        log.debug('===== END TEST NO PARTICLES =====')

    def test_bug_11367(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, '20161005.ctdbp2.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(25)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 24)
            self.assertEquals(len(self.exception_callback_value), 0)

