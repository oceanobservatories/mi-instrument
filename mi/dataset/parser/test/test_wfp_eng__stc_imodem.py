#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_wfp_eng__stc_imodem
@file marine-integrations/mi/dataset/parser/test/test_wfp_eng__stc_imodem.py
@author Emily Hahn
@brief Test code for a Wfp_eng__stc_imodem data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.WFP_ENG.STC_IMODEM.resource import RESOURCE_PATH
from mi.dataset.parser.wfp_eng__stc_imodem import WfpEngStcImodemParser
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemEngineeringRecoveredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemEngineeringTelemeteredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStartRecoveredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStartTelemeteredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStatusRecoveredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStatusTelemeteredDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class WfpEngStcImodemParserUnitTestCase(ParserUnitTestCase):
    """
    Wfp_eng__stc_imodem Parser unit test suite
    """
    
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self._recov_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.wfp_eng__stc_imodem_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'status_data_particle_class': WfpEngStcImodemStatusRecoveredDataParticle,
                'start_data_particle_class': WfpEngStcImodemStartRecoveredDataParticle,
                'engineering_data_particle_class': WfpEngStcImodemEngineeringRecoveredDataParticle
            }
        }
        self._telem_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.wfp_eng__stc_imodem_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'status_data_particle_class': WfpEngStcImodemStatusTelemeteredDataParticle,
                'start_data_particle_class': WfpEngStcImodemStartTelemeteredDataParticle,
                'engineering_data_particle_class': WfpEngStcImodemEngineeringTelemeteredDataParticle
            }
        }

    def test_simple_recovered(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, 'simple.dat')
        
        with open(file_path, 'rb') as stream_handle:

            parser = WfpEngStcImodemParser(
                self._recov_config,
                None,
                stream_handle,
                lambda state, ingested: None,
                lambda data: None)

            result = parser.get_records(5)

            self.assert_particles(result, 'simple_recov.yml', RESOURCE_PATH)

    def test_simple_telemetered(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, 'simple.dat')

        with open(file_path, 'rb') as stream_handle:
            parser = WfpEngStcImodemParser(
                self._telem_config,
                None,
                stream_handle,
                lambda state, ingested: None,
                lambda data: None)

            result = parser.get_records(5)

            self.assert_particles(result, 'simple_telem.yml', RESOURCE_PATH)

    def test_long_stream_recovered(self):
        """
        Test a long stream of data
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000039.DAT')

        with open(file_path, 'rb') as stream_handle:
            parser = WfpEngStcImodemParser(
                self._recov_config,
                None,
                stream_handle,
                lambda state, ingested: None,
                lambda data: None)

            # start with the start time record
            result = parser.get_records(100)

            self.assert_particles(result, 'E0000039_recov.yml', RESOURCE_PATH)

    def test_long_stream_telemetered(self):
        """
        Test a long stream of data
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000039.DAT')

        with open(file_path, 'rb') as stream_handle:
            parser = WfpEngStcImodemParser(
                self._telem_config,
                None,
                stream_handle,
                lambda state, ingested: None,
                lambda data: None)

            # start with the start time record
            result = parser.get_records(100)

            self.assert_particles(result, 'E0000039_telem.yml', RESOURCE_PATH)

    def test_bad_flags_recovered(self):
        """
        test that we don't parse any records when the flags are not what we expect
        """
        file_path = os.path.join(RESOURCE_PATH, 'bad_flags.dat')

        with self.assertRaises(SampleException):
            with open(file_path, 'rb') as stream_handle:
                parser = WfpEngStcImodemParser(
                    self._recov_config,
                    None,
                    stream_handle,
                    lambda state, ingested: None,
                    lambda data: None)

    def test_bad_flags_telemetered(self):
        """
        test that we don't parse any records when the flags are not what we expect
        """
        file_path = os.path.join(RESOURCE_PATH, 'bad_flags.dat')

        with self.assertRaises(SampleException):
            with open(file_path, 'rb') as stream_handle:
                parser = WfpEngStcImodemParser(
                    self._telem_config,
                    None,
                    stream_handle,
                    lambda state, ingested: None,
                    lambda data: None)

    def test_bad_data_recovered(self):
        """
        Ensure that missing data causes us to miss records
        TODO: This test should be improved if we come up with a more accurate regex for the data sample
        """
        file_path = os.path.join(RESOURCE_PATH, 'bad_data.dat')

        with open(file_path, 'rb') as stream_handle: 
            parser = WfpEngStcImodemParser(
                self._recov_config,
                None,
                stream_handle,
                lambda state, ingested: None,
                lambda data: None)
        
            result = parser.get_records(5)
        
            self.assert_particles(result, 'bad_data_recov.yml', RESOURCE_PATH)
    
    def test_bad_data_telemetered(self):
        """
        Ensure that missing data causes us to miss records
        TODO: This test should be improved if we come up with a more accurate regex for the data sample
        """
        file_path = os.path.join(RESOURCE_PATH, 'bad_data.dat')

        with open(file_path, 'rb') as stream_handle:
            parser = WfpEngStcImodemParser(
                self._telem_config,
                None,
                stream_handle,
                lambda state, ingested: None,
                lambda data: None)

            result = parser.get_records(5)

            self.assert_particles(result, 'bad_data_telem.yml', RESOURCE_PATH)

    def test_bug_3241(self):
        """
        This test was created to validate fixes to bug #3241
        It validates the parser can parse a file recovered from
        Global platforms
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000000.DAT')

        with open(file_path, 'rb') as stream_handle:
            parser = WfpEngStcImodemParser(
                self._recov_config,
                None,
                stream_handle,
                lambda state, ingested: None,
                lambda data: None)

            result = parser.get_records(100)

            # make sure we get 100 particles back
            self.assertEquals(len(result), 100)

            # make sure there are no errors
            self.assertEquals(len(self.exception_callback_value), 0)

