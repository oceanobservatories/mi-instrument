#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/
@author Jeff Roy
@brief Test code for a wc_sbe_cspp data parser

wc_sbe_cspp is based on cspp_base.py
test_wc_sbe_cspp.py fully tests all of the capabilities of the
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

from mi.dataset.parser.wc_sbe_cspp import \
    WcSbeCsppParser, \
    WcSbeEngRecoveredDataParticle, \
    WcSbeEngTelemeteredDataParticle, \
    WcSbeMetadataRecoveredDataParticle, \
    WcSbeMetadataTelemeteredDataParticle, \
    WcSbeDataTypeKey

log = get_logger()
RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'wc_sbe', 'cspp', 'resource')


@attr('UNIT', group='mi')
class WcSbeCsppParserUnitTestCase(ParserUnitTestCase):
    """
    wc_sbe_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            WcSbeDataTypeKey.WC_SBE_CSPP_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcSbeMetadataTelemeteredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcSbeEngTelemeteredDataParticle,
                }
            },
            WcSbeDataTypeKey.WC_SBE_CSPP_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcSbeMetadataRecoveredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcSbeEngRecoveredDataParticle,
                }
            },
        }

    def test_simple(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_SBE.txt')
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcSbeCsppParser(self.config.get(WcSbeDataTypeKey.WC_SBE_CSPP_RECOVERED),
                                 stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '11079364_WC_SBE_recov.yml', RESOURCE_PATH)

        stream_handle.close()

    def test_simple_telem(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_SBE.txt')
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcSbeCsppParser(self.config.get(WcSbeDataTypeKey.WC_SBE_CSPP_TELEMETERED),
                                 stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '11079364_WC_SBE_telem.yml', RESOURCE_PATH)
        # check the first particle, which should be the metadata particle (recovered)

        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_SBE.txt')
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcSbeCsppParser(self.config.get(WcSbeDataTypeKey.WC_SBE_CSPP_TELEMETERED),
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

        # the first useful record in this file is corrupted and will be ignored
        # we expect to get the metadata particle with the
        # timestamp from the 2nd data record and all of the valid engineering
        # data records

        file_path = os.path.join(RESOURCE_PATH, '11079364_BAD_WC_SBE.txt')
        stream_handle = open(file_path, 'rU')

        log.info(self.exception_callback_value)

        parser = WcSbeCsppParser(self.config.get(WcSbeDataTypeKey.WC_SBE_CSPP_RECOVERED),
                                 stream_handle,
                                 self.exception_callback)

        parser.get_records(20)

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        stream_handle.close()
