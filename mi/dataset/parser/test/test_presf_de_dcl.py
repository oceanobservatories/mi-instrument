#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_de_dcl
@file mi-dataset/mi/dataset/parser/test/test_presf_de_dcl.py
@author Samuel Dahlberg
@brief Test code for a presf_de_dcl data parser
"""

import os

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.presf_de_dcl import PresfDeDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.driver.presf_de.dcl.resource import RESOURCE_PATH

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.presf_de_dcl'
CLASS_NAME = 'PresfDeDataParticle'
PARTICLE_TYPE = 'presf_de_instrument'


class PresfDeDclUnitTestCase(ParserUnitTestCase):

    def create_presf_de_parser(self, file_handle):
        """
        This function creates a Presf DE parser for telemetered data.
        """

        return PresfDeDclParser(self.config, file_handle, self.rec_exception_callback)

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
        # Needed for now to run specific tests
        self.setup()

        log.debug('===== START TEST PRESF_DE_DCL Parser =====')

        with open(self.file_path('20231107.rbrq3.log')) as in_file:
            parser = self.create_presf_de_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 13)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST PRESF_DE_DCL Parser  =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # Needed for now to run specific tests
        self.setup()

        log.debug('===== START TEST BAD DATA  =====')

        with open(self.file_path('20231107_corrupt.rbrq3.log')) as in_file:
            parser = self.create_presf_de_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 10)

            for i in range(len(self.exception_callback_value)):
               log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD DATA  =====')
