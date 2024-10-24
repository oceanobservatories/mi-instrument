#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_plims_a_hdr
@file mi-dataset/mi/dataset/parser/test/test_plims_a_hdr.py
@author Samuel Dahlberg
@brief Test code for a plims_a_hdr data parser
"""

import os

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.plims_a.resource import RESOURCE_PATH
from mi.dataset.parser.plims_a_hdr import PlimsAHdrParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.plims_a_hdr'
CLASS_NAME = 'PlimsAHdrDataParticle'
PARTICLE_TYPE = 'plims_a_instrument'


class PlimsAHdrUnitTestCase(ParserUnitTestCase):
    """
    plims_a_hdr Parser unit test suite
    """

    def create_plims_a_parser(self, file_handle):
        return PlimsAHdrParser(self.config, file_handle, self.rec_exception_callback)

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

    def test_plims_a_hdr_parser(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST PLIMS_A_HDR Parser =====')
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195.hdr')) as in_file:
            parser = self.create_plims_a_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 1)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST PLIMS_A_HDR Parser  =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        log.debug('===== START TEST BAD DATA  =====')
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195_corrupt.hdr')) as in_file:
            parser = self.create_plims_a_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 0)
            self.assertEqual(len(self.exception_callback_value), 1)

            for i in range(len(self.exception_callback_value)):
                log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD DATA  =====')
