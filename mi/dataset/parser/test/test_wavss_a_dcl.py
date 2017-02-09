"""
@package mi.dataset.parser.test.test_wavss_a_dcl
@file mi/dataset/parser/test/test_wavss_a_dcl.py
@author Emily Hahn
@brief A test parser for the wavss series a instrument through a DCL
"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.wavss_a_dcl import WavssADclParser
log = get_logger()

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'wavss_a', 'dcl', 'resource')


@attr('UNIT', group='mi')
class WavssADclParserUnitTestCase(ParserUnitTestCase):

    def test_tspwa_telem(self):
        """
        Test a simple telemetered case that we can parse a single $TSPWA message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspwa.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspwa_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_tspna_telem(self):
        """
        Test a simple case that we can parse a single $TSPNA message
        """
        # this file also is missing a newline at the end of the file which tests the case of a missing line terminator
        with open(os.path.join(RESOURCE_PATH, 'tspna.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspna_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_tspna_with_nans_telem(self):
        """
        Test a simple case that we can parse a single $TSPNA message
        """
        # this file also is missing a newline at the end of the file which tests the case of a missing line terminator
        with open(os.path.join(RESOURCE_PATH, 'tspna_with_nan.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assertEqual(self.exception_callback_value, [])

    def test_tspma_telem(self):
        """
        Test a simple case that we can parse a single $TSPMA message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspma.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspma_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_tspha_telem(self):
        """
        Test a simple case that we can parse a single $TSPHA message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspha.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspha_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_tspfb_telem(self):
        """
        Test a simple case that we can parse a single $TSPFB message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspfb.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspfb_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_simple(self):
        """
        Test a simple telemetered and recovered case with all the particle types
        """
        with open(os.path.join(RESOURCE_PATH, '20140825.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(6)

            self.assert_particles(particles, "20140825_telem.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140825.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(6)

            self.assert_particles(particles, "20140825_recov.yml", RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_tspwa_with_dplog(self):
        """
        Test a file with many tspwas that we ignore dplog marker lines
        """
        with open(os.path.join(RESOURCE_PATH, '20140804.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            # request more particles than are available
            particles = parser.get_records(26)

            # make sure we only get the available number of particles
            self.assertEqual(len(particles), 23)
            self.assertEqual(self.exception_callback_value, [])

    def test_bad_number_samples(self):
        """
        Test bad number of samples for all data particle types except tspwa (since that size is fixed in the regex)
        """
        with open(os.path.join(RESOURCE_PATH, 'bad_num_samples.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            n_test = 4  # number of particles to test
            particles = parser.get_records(n_test)

            # make sure none of the particles succeeded
            self.assertEqual(len(particles), 0)
            # check that there were 3 recoverable sample exceptions
            self.assertEqual(len(self.exception_callback_value), n_test)
            for i in range(0, n_test):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

    def test_unexpected(self):
        """
        Test with an unexpected line, confirm we get an exception
        """
        with open(os.path.join(RESOURCE_PATH, 'unexpected.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            # there are 4 lines in the file, 3rd, has error, TSPSA record is ignored
            n_test = 4
            particles = parser.get_records(n_test)
            self.assertEqual(len(particles), 2)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

    def test_bug_10046(self):
        """
        Test sample files from Redmine ticket #10046
        NaN values were found in other record types causing parser to fail.
        Verify parser parses files without error
        """
        with open(os.path.join(RESOURCE_PATH, '20150314.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(200)

            self.assertEqual(len(particles), 120)
            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, '20150315.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(200)

            self.assertEqual(len(particles), 120)
            self.assertEqual(self.exception_callback_value, [])
