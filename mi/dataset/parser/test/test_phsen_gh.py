#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_phsen_gh
@file mi-dataset/mi/dataset/parser/test/test_phsen_gh.py
@author Samuel Dahlberg
@brief Test code for a phsen_gh data parser. This file contains two classes, one for standard phsen_gh data,
and the other for phsen_gh dcl data.
"""

import os

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.phsen_gh import PhsenGhParser
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.driver.phsen_gh.resource import RESOURCE_PATH
from mi.dataset.driver.phsen_gh.dcl.resource import RESOURCE_PATH as RESOURCE_PATH_DCL

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.phsen_gh'
CLASS_NAME = 'PhsenGhDataParticle'
CLASS_DCL_NAME = 'PhsenGhDclDataParticle'
PARTICLE_TYPE = 'phsen_gh_instrument'
PARTICLE_DCL_TYPE = 'phsen_gh_dcl_instrument'


class PhsenGhUnitTestCase(ParserUnitTestCase):
    """
    phsen_gh Parser unit test suite
    """

    def create_phsen_gh_parser(self, file_handle):
        return PhsenGhParser(self.config, file_handle, self.rec_exception_callback)

    def file_path(self, filename):
        log.debug('resource path = %s, file name = %s', RESOURCE_PATH, filename)
        return os.path.join(RESOURCE_PATH, filename)

    def rec_exception_callback(self, exception):
        """
        Call back method to watch what comes in via the exception callback
        """
        self.exception_callback_value.append(exception)
        self.exceptions_detected += 1

    def setup(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
        }

        self.exception_callback_value = []
        self.exceptions_detected = 0

    def test_phsen_gh_parser(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """

        # Used when running the test individually
        self.setup()

        log.debug('===== START TEST PHSEN_GH Parser =====')

        with open(self.file_path('seaphox04_20240508_141129.DAT')) as in_file:
            parser = self.create_phsen_gh_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 1)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST PHSEN_GH Parser  =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # Used when running the test individually
        self.setup()

        log.debug('===== START TEST BAD PHSEN_GH DATA  =====')

        with open(self.file_path('seaphox04_20240508_BAD.DAT')) as in_file:
            parser = self.create_phsen_gh_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 1)
            self.assertEqual(len(self.exception_callback_value), 0)

            for i in range(len(self.exception_callback_value)):
                log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD PHSEN_GH DATA  =====')


class PhsenGhDclUnitTestCase(ParserUnitTestCase):
    """
    phsen_gh Parser unit test suite for dcl data config
    """

    def create_phsen_gh_dcl_parser(self, file_handle):
        return PhsenGhParser(self.config, file_handle, self.rec_exception_callback)

    def file_path(self, filename):
        log.debug('resource path = %s, file name = %s', RESOURCE_PATH_DCL, filename)
        return os.path.join(RESOURCE_PATH_DCL, filename)

    def rec_exception_callback(self, exception):
        """
        Call back method to watch what comes in via the exception callback
        """
        self.exception_callback_value.append(exception)
        self.exceptions_detected += 1

    def setup(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_DCL_NAME
        }

        self.exception_callback_value = []
        self.exceptions_detected = 0

    def test_phsen_gh_parser(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """

        # Used when running the test individually
        self.setup()

        log.debug('===== START TEST PHSEN_GH_DCL Parser =====')

        with open(self.file_path('20240810.cphox.log')) as in_file:
            parser = self.create_phsen_gh_dcl_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 12)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST PHSEN_GH_DCL Parser  =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # Used when running the test individually
        self.setup()

        log.debug('===== START TEST BAD PHSEN_GH_DCL DATA  =====')

        with open(self.file_path('20240810_BAD.cphox.log')) as in_file:
            parser = self.create_phsen_gh_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 9)
            self.assertEqual(len(self.exception_callback_value), 1)

            for i in range(len(self.exception_callback_value)):
                log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD PHSEN_GH_DCL DATA  =====')
