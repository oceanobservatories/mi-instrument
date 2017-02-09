#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_rte_o_dcl
@file marine-integrations/mi/dataset/parser/test/test_rte_o_dcl.py
@author Jeff Roy
@brief Test code for a rte_o_dcl data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.rte_o_dcl import RteODclParser
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH
log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'cg_stc_eng', 'stc', 'resource')


@attr('UNIT', group='mi')
class RteODclParserUnitTestCase(ParserUnitTestCase):

    """
    RteODcl Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.rte_o_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'RteODclParserDataParticle'
            }

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        with open(os.path.join(RESOURCE_PATH, 'bad_data.rte.log'), 'rU') as file_handle:
            parser = RteODclParser(self.config, file_handle, self.exception_callback)

            particles = parser.get_records(5)
            self.assertEqual(len(particles), 4)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    def test_bug_9692(self):
        """
        Test to verify change made works with DCL
        timestamps containing seconds >59
        """

        with open(os.path.join(RESOURCE_PATH, '20131115A.rte.log'), 'rU') as file_handle:
            parser = RteODclParser(self.config, file_handle, self.exception_callback)

            result = parser.get_records(10)
            self.assertEqual(len(result), 5)
            self.assertListEqual(self.exception_callback_value, [])

    def test_with_file_recov(self):

        config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.rte_o_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'RteODclParserRecoveredDataParticle'
            }
        with open(os.path.join(RESOURCE_PATH, 'four_samp.rte.log'), 'rU') as file_handle:
            parser = RteODclParser(config, file_handle, self.exception_callback)

            particles = parser.get_records(5)
            self.assertEqual(len(particles), 4)
            self.assertListEqual(self.exception_callback_value, [])

            self.assert_particles(particles, 'four_samp_rte_recov.result.yml', RESOURCE_PATH)

    def test_with_file_telem(self):

        with open(os.path.join(RESOURCE_PATH, 'first.rte.log'), 'rU') as file_handle:
            parser = RteODclParser(self.config, file_handle, self.exception_callback)

            particles = parser.get_records(5)
            self.assertEqual(len(particles), 2)
            self.assertListEqual(self.exception_callback_value, [])

            self.assert_particles(particles, 'first_rte.result.yml', RESOURCE_PATH)



