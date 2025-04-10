#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_dofst_p_wfp
@file mi-dataset/mi/dataset/parser/test/test_dofst_p_wfp.py
@author Samuel Dahlberg
@brief Test code for a dofst_p_wfp data parser
"""
import json
import os

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.dofst_p_wfp import DofstPWfpParser
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.driver.dofst_p.wfp.resource import RESOURCE_PATH

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.dofst_p_wfp'
CLASS_NAME = 'DofstPDataParticle'
PARTICLE_TYPE = 'dofst_p_wfp_instrument'


class DofstPWfpUnitTestCase(ParserUnitTestCase):
    """
    Dofst_p_wfp Parser unit test suite
    """

    def create_dofst_p_parser(self, file_handle):
        return DofstPWfpParser(self.config, file_handle, self.rec_exception_callback)

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

    def test_dofst_p_wfp_parser(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """

        # self.setup()  # Needed for single case testing

        log.debug('===== START TEST DOFST_P_WFP Parser =====')

        with open(self.file_path('prkt_20240502_130021.DAT')) as in_file:
            parser = self.create_dofst_p_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(200)

            self.assertEqual(len(result), 155)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST DOFST_P_WFP Parser  =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # self.setup()  # Needed for single case testing

        log.debug('===== START TEST BAD DATA =====')

        with open(self.file_path('prkt_20240417_005946_BAD_CUTOFFSCIENCE.DAT')) as in_file:
            parser = self.create_dofst_p_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(2000)

            self.assertEqual(len(result), 100)
            self.assertEqual(self.exceptions_detected, 1)

        log.debug('===== END TEST BAD DATA  =====')
