#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc
@file marine-integrations/mi/dataset/parser/test/test_presf_abc.py
@author Christopher Fortin, Jeff Roy, Rene Gelinas
@brief Test code for a presf_abc data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import \
    ParserUnitTestCase, \
    BASE_RESOURCE_PATH

from mi.dataset.parser.presf_abc import PresfAbcParser

from mi.core.log import get_logger
log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH,
                             'presf_abc',
                             'resource')

MODULE_NAME = 'mi.dataset.parser.presf_abc'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class PresfAbcParserUnitTestCase(ParserUnitTestCase):
    """
    presf_abc Parser unit test suite
    """

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_1.hex'), 'rU') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(2)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)

            self.assert_particles(particles,
                                  'presf_abc_test_1.exp_results.yml',
                                  RESOURCE_PATH)

    def test_big(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_2.hex'), 'rU') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(6)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 6 particles
            self.assertTrue(len(particles) == 6)

            self.assert_particles(particles,
                                  "presf_abc_test_2.exp_results.yml",
                                  RESOURCE_PATH)

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_3.hex'), 'rU') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(20)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 20)

            self.assert_particles(particles,
                                  "presf_abc_test_3.exp_results.yml",
                                  RESOURCE_PATH)

    def test_multiple_sessions(self):
        """
        Read test data from multiple sessions and pull out multiple data
        particles at one time. Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_4a.hex'), 'rU') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(40)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 40)

            self.assert_particles(particles,
                                  "presf_abc_test_4a.exp_results.yml",
                                  RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_4b.hex'), 'rU') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(8)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 8)

            self.assert_particles(particles,
                                  "presf_abc_test_4b.exp_results.yml",
                                  RESOURCE_PATH)

    def test_invalid_tide_record(self):
        """
        The file used here has an invalid tide record.
        """

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_5.hex'), 'rU') \
                as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 1

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertTrue(len(particles) == num_expected_particles)

            self.assert_particles(particles,
                                  "presf_abc_test_5.exp_results.yml",
                                  RESOURCE_PATH)

            # Check the expected exceptions
            self.assertTrue(len(self.exception_callback_value) == 1)
            self.assert_(isinstance(self.exception_callback_value[0],
                                    RecoverableSampleException))

    def test_invalid_wave_record(self):
        """
        Two of the wave records in this file are damaged.  The first is missing
        the pt subrecord, and the second is missing the termination of the wave
        record.
        """

        # Test the wrong number of wave burst records.
        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_6a.hex'), 'rU') \
                as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 1

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertTrue(len(particles) == num_expected_particles)

            self.assert_particles(particles,
                                  "presf_abc_test_6a.exp_results.yml",
                                  RESOURCE_PATH)

        # Test an invalid format of a wave burst record.
        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_6b.hex'), 'rU') \
                as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 1

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertTrue(len(particles) == num_expected_particles)

            self.assert_particles(particles,
                                  "presf_abc_test_6b.exp_results.yml",
                                  RESOURCE_PATH)

            # Check the expected exceptions
            self.assertTrue(len(self.exception_callback_value) == 3)
            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i],
                                        RecoverableSampleException))

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_7.hex'), 'rU') \
                as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 0

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)
            self.assertTrue(len(particles) == num_expected_particles)
            self.assertTrue(len(self.exception_callback_value) == 0)

    def test_invalid_session_data(self):
        """
        Verify that the session data check is executed.
        """

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_8.hex'), 'rU') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(2)

            # Make sure there was an exception found
            self.assertTrue(len(self.exception_callback_value) == 1)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)

            self.assert_particles(particles,
                                  'presf_abc_test_1.exp_results.yml',
                                  RESOURCE_PATH)

            # Check the expected exceptions
            self.assertTrue(len(self.exception_callback_value) == 1)
            self.assert_(isinstance(self.exception_callback_value[0],
                                    RecoverableSampleException))
