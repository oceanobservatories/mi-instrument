
"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_spkir_abj_cspp.py
@author Jeff Roy
@brief Test code for a spkir_abj_cspp data parser

spkir_abj_cspp is based on cspp_base.py
test_dosta_abcdjm_cspp.py fully tests all of the capabilities of the
base parser.  That level of testing is omitted from this test suite
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.spkir_abj.cspp.resource import RESOURCE_PATH
from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.spkir_abj_cspp import \
    SpkirAbjCsppParser, \
    SpkirAbjCsppInstrumentTelemeteredDataParticle, \
    SpkirAbjCsppMetadataTelemeteredDataParticle, \
    SpkirAbjCsppInstrumentRecoveredDataParticle, \
    SpkirAbjCsppMetadataRecoveredDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()


@attr('UNIT', group='mi')
class SpkirAbjCsppParserUnitTestCase(ParserUnitTestCase):
    """
    spkir_abj_cspp Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self._telem_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.spkir_abj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: SpkirAbjCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: SpkirAbjCsppInstrumentTelemeteredDataParticle,
            }
        }

        self._recov_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.spkir_abj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: SpkirAbjCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: SpkirAbjCsppInstrumentRecoveredDataParticle,
            }
        }

    def test_simple(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, '11079364_PPD_OCR.txt'), 'rU') as file_handle:
            # Note: since the recovered and teelemetered parser and particles are common
            # to each other, testing one is sufficient, will be completely tested
            # in driver tests

            parser = SpkirAbjCsppParser(self._recov_config,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(20)

            log.debug("*** test_simple Num particles %s", len(particles))

            self.assert_particles(particles, '11079364_PPD_OCR_recov.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '11079364_PPD_OCR.txt'), 'rU') as file_handle:
            # Note: since the recovered and teelemetered parser and particles are common
            # to each other, testing one is sufficient, will be completely tested
            # in driver tests

            parser = SpkirAbjCsppParser(self._telem_config,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(20)

            log.debug("*** test_simple Num particles %s", len(particles))

            self.assert_particles(particles, '11079364_PPD_OCR_telem.yml', RESOURCE_PATH)

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, '11079419_PPB_OCR.txt'), 'rU') as file_handle:
            # Note: since the recovered and teelemetered parser and particles are common
            # to each other, testing one is sufficient, will be completely tested
            # in driver tests

            parser = SpkirAbjCsppParser(self._recov_config,
                                        file_handle,
                                        self.exception_callback)

            # try to get 2000 particles, there are only 1623 data records
            # so should get 1624 including the meta data
            particles = parser.get_records(2000)

            log.debug("*** test_get_many Num particles %s", len(particles))

            self.assert_particles(particles, '11079419_PPB_OCR_recov.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '11079419_PPB_OCR.txt'), 'rU') as file_handle:
            # Note: since the recovered and teelemetered parser and particles are common
            # to each other, testing one is sufficient, will be completely tested
            # in driver tests

            parser = SpkirAbjCsppParser(self._telem_config,
                                        file_handle,
                                        self.exception_callback)

            # try to get 2000 particles, there are only 1623 data records
            # so should get 1624 including the meta data
            particles = parser.get_records(2000)

            log.debug("*** test_get_many Num particles %s", len(particles))

            self.assert_particles(particles, '11079419_PPB_OCR_telem.yml', RESOURCE_PATH)

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # the first data record in this file is corrupted and will be ignored
        # we expect the first 2 particles to be the metadata particle and the
        # intrument particle from the data record after the corrupted one
        with open(os.path.join(RESOURCE_PATH, '11079419_BAD_PPB_OCR.txt'), 'rU') as file_handle:

            log.debug(self.exception_callback_value)

            parser = SpkirAbjCsppParser(self._recov_config,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(2)

            self.assert_particles(particles, 'bad_data_record_recov.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '11079419_BAD_PPB_OCR.txt'), 'rU') as file_handle:

            log.debug(self.exception_callback_value)

            parser = SpkirAbjCsppParser(self._telem_config,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(2)

            self.assert_particles(particles, 'bad_data_record_telem.yml', RESOURCE_PATH)

    def test_extra_data(self):

        """
        Ensure that bad data is skipped when it exists.
        """

        # the first 2 data record in this file are corrupted by adding additional
        # data vlaues separated by tabs and will be ignored
        # we expect the first 2 particles to be the metadata particle and the
        # intrument particle from the data record after the corrupted one
        with open(os.path.join(RESOURCE_PATH, '11079364_EXTRA_DATA_PPD_OCR.txt'), 'rU') as file_handle:

            log.debug(self.exception_callback_value)

            parser = SpkirAbjCsppParser(self._recov_config,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(2)

            self.assertEquals(len(self.exception_callback_value), 2)

            for exception in self.exception_callback_value:
                self.assert_(isinstance(exception, RecoverableSampleException))

            # expect to see a recoverable sample exception in the log
            log.debug('TEST EXTRA DATA exception call back is %s', self.exception_callback_value)

            self.assert_particles(particles, 'extra_data_values_recov.yml', RESOURCE_PATH)

            self.exception_callback_value = []

        with open(os.path.join(RESOURCE_PATH, '11079364_EXTRA_DATA_PPD_OCR.txt'), 'rU') as file_handle:

            log.debug(self.exception_callback_value)

            parser = SpkirAbjCsppParser(self._telem_config,
                                        file_handle,
                                        self.exception_callback)

            particles = parser.get_records(2)

            self.assertEquals(len(self.exception_callback_value), 2)

            for exception in self.exception_callback_value:
                self.assert_(isinstance(exception, RecoverableSampleException))

            # expect to see a recoverable sample exception in the log
            log.debug('TEST EXTRA DATA exception call back is %s', self.exception_callback_value)

            self.assert_particles(particles, 'extra_data_values_telem.yml', RESOURCE_PATH)
