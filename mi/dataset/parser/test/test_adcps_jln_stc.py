#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcps_jln_stc
@file marine-integrations/mi/dataset/parser/test/test_adcps_jln_stc.py
@author Maria Lutz
@brief Test code for a adcps_jln_stc data parser
"""
import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.adcps_jln.stc.resource import RESOURCE_PATH
from mi.dataset.parser.adcps_jln_stc import AdcpsJlnStcParser, \
    AdcpsJlnStcInstrumentTelemeteredDataParticle, \
    AdcpsJlnStcInstrumentRecoveredDataParticle, \
    AdcpsJlnStcMetadataTelemeteredDataParticle, \
    AdcpsJlnStcMetadataRecoveredDataParticle, \
    AdcpsJlnStcParticleClassKey
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()


@attr('UNIT', group='mi')
class AdcpsJlnStcParserUnitTestCase(ParserUnitTestCase):
    """
    adcps_jln_stc Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self._telem_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcps_jln_stc',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                AdcpsJlnStcParticleClassKey.METADATA_PARTICLE_CLASS:
                    AdcpsJlnStcMetadataTelemeteredDataParticle,
                AdcpsJlnStcParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    AdcpsJlnStcInstrumentTelemeteredDataParticle,
            }
        }
        self._recov_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcps_jln_stc',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                AdcpsJlnStcParticleClassKey.METADATA_PARTICLE_CLASS:
                    AdcpsJlnStcMetadataRecoveredDataParticle,
                AdcpsJlnStcParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    AdcpsJlnStcInstrumentRecoveredDataParticle,
            }
        }

    def test_simple(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'adcpt_20130929_091817.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._telem_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(6)

            self.assert_particles(result, 'adcpt_20130929_091817.telem.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        with open(os.path.join(RESOURCE_PATH, 'adcpt_20130929_091817.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._recov_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(6)

            self.assert_particles(result, 'adcpt_20130929_091817.recov.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

    def test_bad_data_telem(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # Bad checksum
        # If checksum is bad, skip the record and continue parsing.
        with open(os.path.join(RESOURCE_PATH, 'adcps_jln_stc.bad_checksum.DAT'), 'rb') as file_handle:

            parser = AdcpsJlnStcParser(self._telem_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(10)

            self.assert_particles(result, 'adcps_jln_stc.bad_checksum.telem.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)

            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

            self.exception_callback_value.pop()

        # Incorrect number of bytes
        # If numbytes is incorrect, skip the record and continue parsing.
        with open(os.path.join(RESOURCE_PATH, 'adcps_jln_stc.bad_num_bytes.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._telem_config,
                                       file_handle,
                                       self.exception_callback)
            result = parser.get_records(10)

            self.assert_particles(result, 'adcps_jln_stc.bad_num_bytes.telem.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)

            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

    def test_bad_data_recov(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # Bad checksum
        # If checksum is bad, skip the record and continue parsing.
        with open(os.path.join(RESOURCE_PATH, 'adcps_jln_stc.bad_checksum.DAT'), 'rb') as file_handle:

            parser = AdcpsJlnStcParser(self._recov_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(10)

            self.assert_particles(result, 'adcps_jln_stc.bad_checksum.recov.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)

            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

            self.exception_callback_value.pop()

        # Incorrect number of bytes
        # If numbytes is incorrect, skip the record and continue parsing.
        with open(os.path.join(RESOURCE_PATH, 'adcps_jln_stc.bad_num_bytes.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._recov_config,
                                       file_handle,
                                       self.exception_callback)
            result = parser.get_records(10)

            self.assert_particles(result, 'adcps_jln_stc.bad_num_bytes.recov.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)

            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

    def test_receive_fail_telem(self):
        # ReceiveFailure
        # If record marked with 'ReceiveFailure', skip the record and continue parsing.
        with open(os.path.join(RESOURCE_PATH, 'adcps_jln_stc.bad_rx_failure.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._telem_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(10)

            self.assert_particles(result, 'adcps_jln_stc.bad_rx_failure.telem.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

    def test_receive_fail_recov(self):
        # ReceiveFailure
        # If record marked with 'ReceiveFailure', skip the record and continue parsing.
        with open(os.path.join(RESOURCE_PATH, 'adcps_jln_stc.bad_rx_failure.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._recov_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(10)

            self.assert_particles(result, 'adcps_jln_stc.bad_rx_failure.recov.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

    def test_real_file(self):

        with open(os.path.join(RESOURCE_PATH, 'adcpt_20140504_015742.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._telem_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(1000)

            self.assert_particles(result, 'adcpt_20140504_015742.telem.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

        with open(os.path.join(RESOURCE_PATH, 'adcpt_20140504_015742.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._recov_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(1000)

            self.assert_particles(result, 'adcpt_20140504_015742.recov.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 0)

    def test_bug_2979_1(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'adcpt_20140613_105345.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._telem_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(100)

            self.assertEquals(len(result), 13)

            self.assertEquals(len(self.exception_callback_value), 0)

    def test_bug_2979_2(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'adcpt_20140707_200310.DAT'), 'rb') as file_handle:
            parser = AdcpsJlnStcParser(self._telem_config,
                                       file_handle,
                                       self.exception_callback)

            result = parser.get_records(100)

            self.assertEquals(len(result), 0)

            self.assertEquals(len(self.exception_callback_value), 0)

