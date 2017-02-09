#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_wc_hmr_cspp.py
@author Jeff Roy
@brief Test code for a wc_hmr_cspp data parser

wc_hmr_cspp is based on cspp_base.py
ttest_wc_hmr_cspp.py fully tests all of the capabilities of the
base parser.  That level of testing is omitted from this test suite
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger

from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY

from mi.dataset.parser.wc_hmr_cspp import \
    WcHmrCsppParser, \
    WcHmrEngRecoveredDataParticle, \
    WcHmrEngTelemeteredDataParticle, \
    WcHmrMetadataRecoveredDataParticle, \
    WcHmrMetadataTelemeteredDataParticle, \
    WcHmrDataTypeKey

log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'wc_hmr', 'cspp', 'resource')


@attr('UNIT', group='mi')
class WcHmrCsppParserUnitTestCase(ParserUnitTestCase):
    """
    wc_hmr_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            WcHmrDataTypeKey.WC_HMR_CSPP_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcHmrMetadataTelemeteredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcHmrEngTelemeteredDataParticle,
                }
            },
            WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcHmrMetadataRecoveredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcHmrEngRecoveredDataParticle,
                }
            },
        }

        # Define test data particles and their associated timestamps which will be
        # compared with returned results

    def test_simple(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_HMR.txt')
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '11079364_WC_HMR_recov.yml', RESOURCE_PATH)

        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_HMR.txt')
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 stream_handle,
                                 self.exception_callback)

        # try to get 2000 particles, there are more data records in the file
        # so should get 2000 including the meta data
        particles = parser.get_records(2000)

        log.debug("*** test_get_many Num particles %s", len(particles))
        self.assertEqual(len(particles), 2000)

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # the first and 7th data record in this file are corrupted and will be ignored
        # we expect to get the metadata particle with the
        # timestamp from the 2nd data record and all of the valid engineering
        # data records

        file_path = os.path.join(RESOURCE_PATH, '11079364_BAD_WC_HMR.txt')
        stream_handle = open(file_path, 'rU')

        log.info(self.exception_callback_value)

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 stream_handle,
                                 self.exception_callback)

        # 18 particles
        particles = parser.get_records(18)

        self.assert_particles(particles, 'WC_HMR_bad_data_records.yml', RESOURCE_PATH)

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        stream_handle.close()

    def test_extra_data(self):

        """
        Ensure that bad data is skipped when it exists.
        """

        # the first 2nd and 8th data record in this file are corrupted by adding additional
        # data values separated by tabs and will be ignored
        # we expect to get the metadata particle and only the valid
        # engineering data particles

        file_path = os.path.join(RESOURCE_PATH, '11079364_EXTRA_DATA_WC_HMR.txt')

        stream_handle = open(file_path, 'rU')

        log.info(self.exception_callback_value)

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(18)

        self.assert_particles(particles, 'WC_HMR_extra_data_values.yml', RESOURCE_PATH)

        self.assertTrue(self.exception_callback_value is not None)

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        # expect to see a recoverable sample exception in the log
        log.debug('TEST EXTRA DATA exception call back is %s', self.exception_callback_value)

        self.assertTrue(len(particles) == 18)

        stream_handle.close()
