#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_wfp_eng_wfp_sio.py
@author Mark Worden
@brief Test code for a wfp_eng_wfp_sio data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import UnexpectedDataException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.WFP_ENG.wfp_sio.resource import RESOURCE_PATH
from mi.dataset.parser.wfp_eng_wfp_sio import WfpEngWfpSioParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class WfpEngWfpSioParserUnitTestCase(ParserUnitTestCase):
    """
    wfp_eng_wfp_sio Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.wfp_eng_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }
        # Define test data particles and their associated timestamps which will be
        # compared with returned results

        self._exceptions = []

    def test_parsing_of_input_file_without_decimation_factor(self):
        """
        This test method will process a wfp_eng_wfp_sio input file that does not include a status
        particle with a decimation factor.
        """
        file_path = os.path.join(RESOURCE_PATH, 'node58p1_0.we_wfp.dat')

        # Open the file holding the test sample data
        with open(file_path, 'rb') as stream_handle:

            self.parser = WfpEngWfpSioParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 1000 particles
            particles = self.parser.get_records(1000)

            # We should end up with 174 particles
            self.assertTrue(len(particles) == 174)

            self.assert_particles(particles, 'node58p1_0.we_wfp.yml', RESOURCE_PATH)

    def test_parsing_of_input_file_with_decimation_factor(self):
        """
        This test method will process a wfp_eng_wfp_sio input file that includes a status
        particle with a decimation factor.
        """
        file_path = os.path.join(RESOURCE_PATH, 'node58p1_3.we_wfp.dat')

        # Open the file holding the test sample data
        with open(file_path, 'rb') as stream_handle:

            self.parser = WfpEngWfpSioParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 1000 particles
            particles = self.parser.get_records(1000)

            # We should end up with 53 particles
            self.assertTrue(len(particles) == 53)

            self.assert_particles(particles, 'node58p1_3.we_wfp.yml', RESOURCE_PATH)

    def test_parsing_of_input_file_with_unexpected_data(self):
        """
        This test method will process a wfp_eng_wfp_sio input file that includes unexpected data.
        """
        file_path = os.path.join(RESOURCE_PATH, 'wfp_eng_wfp_sio_unexpected_data.dat')

        # Open the file holding the test sample data
        with open(file_path, 'rb') as stream_handle:

            self.parser = WfpEngWfpSioParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 1000 particles
            particles = self.parser.get_records(1000)

            self.assertEquals(len(self.exception_callback_value), 3)

            for i in range(0,len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], UnexpectedDataException))

            # We should end up with 0 particles
            self.assertTrue(len(particles) == 0)
