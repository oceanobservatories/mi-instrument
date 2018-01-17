#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_ctdbp_cdef_dcl.py
@author Jeff Roy
@brief Test code for a ctdbp_cdef_dcl data parser

Change History:

Date         Ticket#    Engineer     Description
------------ ---------- -----------  --------------------------
4/28/17      #9809      janeenP      Added functionality for combined CTDBP
                                     with FLORT

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import \
    RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.driver.ctdbp_cdef.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.ctdbp_cdef_dcl import CtdbpCdefDclParser
from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()
MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_dcl'


@attr('UNIT', group='mi')
class CtdbpCdefDclParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_cdef_dcl Parser unit test suite
    """
    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

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

            # Make sure we obtained 1 particles
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
            # Make sure we obtained 14 particles
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
            self.assertTrue(len(particles) == 291)

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(18)
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
        log.debug('===== START TEST bug 11367 =====')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, '20161005.ctdbp2.log'), 'rU') as file_handle:
            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(25)

            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assertEquals(len(self.exception_callback_value), 0)
        log.debug('===== END TEST bug 11367 =====')

    # ticket #9809
    def test_ctdbp_cdef_dcl_flort_d(self):
        """
        Verify that data records from a ctdbp with a flort_d plugged in
        will produce expected data particles for the ctdbp and ignore the
        flort data.
        """
        log.debug('===== START TEST CTDBP WITH FLORT =====')

        """
        test with control data only and CTD ID
        2015/12/11 01:29:19.067 [ctdbp3:DLOGP6]:Instrument Started [Power On]
        """
        with open(os.path.join(RESOURCE_PATH, '20151211.ctdbp3_controlOnly.log'), 'rU') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 0

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)
            self.assertEquals(len(self.exception_callback_value), 0)

        """
        test 1 rec with CTD ID
        2015/01/03 00:30:23.395 [ctdbp3:DLOGP6]: 12.3772,  3.73234,    1.087, 0, 0, 0, 03 Jan 2015 00:30:16
        """
        with open(os.path.join(RESOURCE_PATH, '20150103.ctdbp3_1recCtdID_w_LowBattery.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 1

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20150103.ctdbp3_1recCtdID_w_LowBattery.yml", RESOURCE_PATH)

        """
        test 3 recs with no CTD ID
        2016/10/09 00:30:26.290  13.3143,  3.56698,    1.088, 1672, 278, 84, 09 Oct 2016 00:30:20
        """
        with open(os.path.join(RESOURCE_PATH, '20161009.ctdbp3_3rec_noCtdId.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)
            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20161009.ctdbp3_3rec_noCtdId.yml", RESOURCE_PATH)

        """
        test 3 recs, 1 with hash separator, no CTD ID
        2014/10/17 21:30:23.684 # 14.7850,  3.96796,    0.981, 740, 222, 73,
        17 Oct 2014 21:30:17
        """
        with open(os.path.join(RESOURCE_PATH, '20141017.ctdbp3_3rec_w_1hash.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20141017.ctdbp3_3rec_w_1hash.yml", RESOURCE_PATH)

        """
        test 3 recs with negative pressure
        2014/10/02 00:30:28.063 [ctdbp3:DLOGP6]: 20.9286,  0.00003,   -0.011, 4130, 1244, 4130, 02 Oct 2014 00:30:23
        """
        with open(os.path.join(RESOURCE_PATH, '20141002.ctdbp3_3Rec_negPressure.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20141002.ctdbp3_3Rec_negPressure.yml", RESOURCE_PATH)

        """
        test 18 recs with one damaged data line
        """
        with open(os.path.join(RESOURCE_PATH, '20161025.ctdbp3_damagedRec.log'), 'r') as file_handle:

            num_particles_to_request = 20
            num_expected_particles = 18

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20161025.ctdbp3_damagedRec.yml", RESOURCE_PATH)

        """
        test large file, 24 recs
        """
        with open(os.path.join(RESOURCE_PATH, '20140928.ctdbp3_24rec.log'), 'r') as file_handle:

            num_particles_to_request = 30
            num_expected_particles = 24

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20140928.ctdbp3_24rec.yml", RESOURCE_PATH)

        """
        test 3 recs with negative pressure
        2014/10/02 00:30:28.063 [ctdbp3:DLOGP6]: 20.9286,  0.00003,   -0.011, 4130, 1244, 4130, 02 Oct 2014 00:30:23
        """
        with open(os.path.join(RESOURCE_PATH, '20141002.ctdbp3_3Rec_negPressure.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = CtdbpCdefDclParser(True,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20141002.ctdbp3_3Rec_negPressure.yml", RESOURCE_PATH)

        """
        Above tests for #9809 are all for the telemetered data path.
        Next set of tests are for the recovered path.  Data files are
        identical format as for telemetered.

        test 3 recs recovered data
        """

        with open(os.path.join(RESOURCE_PATH,
                               '20141001.ctdbp3_3rec_recovered.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 3

            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles,
                                  "20141001.ctdbp3_3rec_recovered.yml",
                                  RESOURCE_PATH)

        """
        test 6 recs recovered data
        """
        with open(os.path.join(RESOURCE_PATH,
                               '20141010.ctdbp3_6rec_recovered.log'), 'r') as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 6

            parser = CtdbpCdefDclParser(False,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            CtdbpCdefDclParserUnitTestCase.create_yml(self, particles,
                                                      '20141010.ctdbp3_6rec_recovered.yml')

            # Make sure we obtained expected particle(s)
            self.assertEquals(len(particles), num_expected_particles)
            self.assert_particles(particles, "20141010.ctdbp3_6rec_recovered.yml", RESOURCE_PATH)

        log.debug('===== END TEST CTDBP WITH FLORT =====')
