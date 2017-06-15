#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_zplsc_c_dcl
@file mi-dataset/mi/dataset/parser/test/test_zplsc_c_dcl.py
@author Richard Han (Raytheon), Ronald Ronquillo (Raytheon)
@brief Test code for a zplsc_c_dcl data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.zplsc_c.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.parser.zplsc_c_dcl import ZplscCDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.zplsc_c_dcl'
CLASS_NAME = 'ZplscCInstrumentDataParticle'
PARTICLE_TYPE = 'zplsc_c_instrument'


@attr('UNIT', group='mi')
class ZplscCDclParserUnitTestCase(ParserUnitTestCase):
    """
    Zplsc_c_dcl Parser unit test suite
    """

    def create_zplsc_c_dcl_parser(self, file_handle):
        """
        This function creates a ZplscCDCL parser for recovered data.
        """
        return ZplscCDclParser(self.config, file_handle, self.rec_exception_callback)

    def file_path(self, filename):
        log.debug('resource path = %s, file name = %s', RESOURCE_PATH, filename)
        return os.path.join(RESOURCE_PATH, filename)

    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def rec_exception_callback(self, exception):
        """
        Call back method to watch what comes in via the exception callback
        """
        self.exception_callback_value.append(exception)
        self.exceptions_detected += 1

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
        }

        self.exception_callback_value = []
        self.exceptions_detected = 0

    def test_zplsc_c_dcl_parser(self):
        """
        Test Zplsc C DCL parser
        Just test that it is able to parse the file and records are generated.
        """
        log.debug('===== START TEST ZPLSC_C_DCL Parser =====')

        with open(self.file_path('20150406.zplsc.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(15)

            self.assertEqual(len(result), 1)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST ZPLSC_C_DCL Parser  =====')

    def test_telem(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """

        log.debug('===== START TEST TELEM  =====')

        with open(self.file_path('20150407.zplsc.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(15)

            self.assertEqual(len(result), 15)
            self.assert_particles(result, '20150407.zplsc.yml', RESOURCE_PATH)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST TELEM  =====')

    def test_variable_num_of_channels(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        All test log files usually contain 4 channels with 19 bins each.
        This tests a manually edited log file to exercise the logic for handling a variable
        number of channels and number of bins.
        """
        log.debug('===== START TEST VARIABLE NUM OF CHANNELS =====')

        with open(self.file_path('20150407.zplsc_var_channels.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(15)

            self.assertEqual(len(result), 15)
            self.assert_particles(result, '20150407.zplsc_var_channels.yml', RESOURCE_PATH)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST VARIABLE NUM OF CHANNELS =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        See '20150407.zplsc_corrupt.log' file for line by line details of expected errors.
        """
        log.debug('===== START TEST BAD DATA  =====')

        with open(self.file_path('20150407.zplsc_corrupt.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(100)

            self.assertEqual(len(result), 1)
            self.assertEqual(len(self.exception_callback_value), 6)

            for i in range(len(self.exception_callback_value)):
                log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD DATA  =====')

    def test_bug_9692(self):
        """
        Test to verify change made works with DCL
        timestamps containing seconds >59
        """

        with open(self.file_path('20150407A.zplsc.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(5)

            self.assertEqual(len(result), 3)
            self.assertListEqual(self.exception_callback_value, [])
