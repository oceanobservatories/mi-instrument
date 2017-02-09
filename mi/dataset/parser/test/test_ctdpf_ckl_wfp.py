#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_ctdpf_ckl_wfp
@file marine-integrations/mi/dataset/parser/test/test_ctdpf_ckl_wfp.py
@author cgoodrich
@brief Test code for a ctdpf_ckl_wfp data parser
"""
import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.ctdpf_ckl.wfp.resource import RESOURCE_PATH
from mi.dataset.parser.ctdpf_ckl_wfp import CtdpfCklWfpParser, \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpRecoveredDataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpRecoveredMetadataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpTelemeteredDataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpTelemeteredMetadataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()


@attr('UNIT', group='mi')
class CtdpfCklWfpParserUnitTestCase(ParserUnitTestCase):
    """
    ctdpf_ckl_wfp Parser unit test suite
    """


    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self._recov_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                DATA_PARTICLE_CLASS_KEY: CtdpfCklWfpRecoveredDataParticle,
                METADATA_PARTICLE_CLASS_KEY: CtdpfCklWfpRecoveredMetadataParticle
            },
        }
        self._telem_config =  {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                DATA_PARTICLE_CLASS_KEY: CtdpfCklWfpTelemeteredDataParticle,
                METADATA_PARTICLE_CLASS_KEY: CtdpfCklWfpTelemeteredMetadataParticle
            }
        }

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        filepath = os.path.join(RESOURCE_PATH, 'simple.dat')
        filesize = os.path.getsize(filepath)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with open(filepath, 'rb') as stream_handle:
            recovered_parser = CtdpfCklWfpParser(
                self._recov_config, stream_handle,
                self.exception_callback,
                filesize)

            particles = recovered_parser.get_records(5)

            self.assert_particles(particles, 'test_simple_recov.yml', RESOURCE_PATH)

        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with open(filepath, 'rb') as stream_handle:
            telemetered_parser = CtdpfCklWfpParser(
                self._telem_config, stream_handle,
                self.exception_callback,
                filesize)

            particles = telemetered_parser.get_records(5)

            self.assert_particles(particles, 'test_simple_telem.yml', RESOURCE_PATH)

    def test_simple_pad(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        filepath = os.path.join(RESOURCE_PATH, 'simple_pad.dat')
        filesize = os.path.getsize(filepath)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with open(filepath, 'rb') as stream_handle:

            recovered_parser = CtdpfCklWfpParser(
                self._recov_config, stream_handle,
                self.exception_callback,
                filesize)

            particles = recovered_parser.get_records(5)

            self.assert_particles(particles, 'test_simple_pad_recov.yml', RESOURCE_PATH)

        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with open(filepath, 'rb') as stream_handle:

            telemetered_parser = CtdpfCklWfpParser(
                self._telem_config, stream_handle,
                self.exception_callback,
                filesize)

            particles = telemetered_parser.get_records(1)

            self.assert_particles(particles, 'test_simple_pad_telem.yml', RESOURCE_PATH)

    def test_long_stream(self):
        """
        Test a long stream
        """
        filepath = os.path.join(RESOURCE_PATH, 'C0000038.dat')
        filesize = os.path.getsize(filepath)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with open(filepath) as stream_handle:
        
            recovered_parser = CtdpfCklWfpParser(
                self._recov_config, stream_handle,
                self.exception_callback, filesize)
            self.parser = recovered_parser

            recovered_result = self.parser.get_records(271)

            self.assert_particles(recovered_result, 'C0000038_recov.yml', RESOURCE_PATH)

        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with open(filepath) as stream_handle:

            telemetered_parser = CtdpfCklWfpParser(
                self._telem_config, stream_handle,
                self.exception_callback, filesize)
            self.parser = telemetered_parser

            telemetered_result = self.parser.get_records(271)

            self.assert_particles(telemetered_result, 'C0000038_telem.yml', RESOURCE_PATH)

    def test_bad_time_data(self):
        """
        If the timestamps are missing, raise a sample exception and do not parse the file
        """
        filepath = os.path.join(RESOURCE_PATH, 'bad_time_data.dat')
        filesize = os.path.getsize(filepath)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with self.assertRaises(SampleException):
            with open(filepath, 'rb') as stream_handle:
                CtdpfCklWfpParser(
                    self._recov_config, stream_handle,
                    self.exception_callback,
                    filesize)

        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with self.assertRaises(SampleException):
            with open(filepath, 'rb') as stream_handle:
                CtdpfCklWfpParser(
                    self._telem_config, stream_handle,
                    self.exception_callback,
                    filesize)

    def test_bad_size_data(self):
        """
        If any of the data records in the file are not 11 bytes long, raise a sample exception
        and do not parse the file.
        """
        filepath = os.path.join(RESOURCE_PATH, 'bad_size_data.dat')
        filesize = os.path.getsize(filepath)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with self.assertRaises(SampleException):
            with open(filepath, 'rb') as stream_handle:
                CtdpfCklWfpParser(
                    self._recov_config, stream_handle,
                    self.exception_callback,
                    filesize)
        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with self.assertRaises(SampleException):
            with open(filepath, 'rb') as stream_handle:
                CtdpfCklWfpParser(
                    self._recov_config, stream_handle,
                    self.exception_callback,
                    filesize)

    def test_bad_eop_data(self):
        """
        If the last "data" record in the file is not 11 byes of 0xFF, raise a sample exception
        and do not parse the file.
        """
        filepath = os.path.join(RESOURCE_PATH, 'bad_eop_data.dat')
        filesize = os.path.getsize(filepath)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with self.assertRaises(SampleException):
            with open(filepath, 'rb') as stream_handle:
                CtdpfCklWfpParser(
                    self._recov_config, stream_handle,
                    self.exception_callback,
                    filesize)
        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with self.assertRaises(SampleException):
            with open(filepath, 'rb') as stream_handle:
                CtdpfCklWfpParser(
                    self._recov_config, stream_handle,
                    self.exception_callback,
                    filesize)
