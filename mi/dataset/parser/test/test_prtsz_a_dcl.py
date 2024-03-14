#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_prtsz_a_dcl
@file mi-dataset/mi/dataset/parser/test/test_prtsz_a_dcl.py
@author Samuel Dahlberg
@brief Test code for a prtsz_a_dcl data parser
"""

import os

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.prtsz_a_dcl import PrtszADclParser
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.driver.prtsz_a.dcl.resource import RESOURCE_PATH

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.prtsz_a_dcl'
CLASS_NAME = 'PrtszADataParticle'
PARTICLE_TYPE = 'prtsz_a_instrument'


class PrtszADclUnitTestCase(ParserUnitTestCase):
    """
    Prtsz_a_dcl Parser unit test suite
    """

    def create_prtsz_a_parser(self, file_handle):
        return PrtszADclParser(self.config, file_handle, self.rec_exception_callback)

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

    def test_prtsz_a_dcl_parser(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """
        self.setup()

        log.debug('===== START TEST PRTSZ_A_DCL Parser =====')

        with open(self.file_path('20231107.prtsz.log')) as in_file:
            parser = self.create_prtsz_a_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 30)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST PRTSZ_A_DCL Parser  =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        log.debug('===== START TEST BAD DATA  =====')

        with open(self.file_path('20231107_corrupt.prtsz.log')) as in_file:
            parser = self.create_prtsz_a_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 1)
            self.assertEqual(len(self.exception_callback_value), 29)

            for i in range(len(self.exception_callback_value)):
                log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD DATA  =====')
