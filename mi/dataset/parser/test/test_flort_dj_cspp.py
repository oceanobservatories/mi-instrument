#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_flort_dj_cspp.py
@author Jeremy Amundson
@brief Test code for a flort_dj_cspp data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.flort_dj.cspp.resource import RESOURCE_PATH
from mi.dataset.parser.cspp_base import METADATA_PARTICLE_CLASS_KEY, DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.flort_dj_cspp import \
    FlortDjCsppParser, \
    FlortDjCsppMetadataRecoveredDataParticle, \
    FlortDjCsppInstrumentRecoveredDataParticle, FlortDjCsppMetadataTelemeteredDataParticle, \
    FlortDjCsppInstrumentTelemeteredDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()

TEST_RECOVERED = 'first_data_recovered.yml'


@attr('UNIT', group='mi')
class FlortDjCsppParserUnitTestCase(ParserUnitTestCase):
    """
    flort_dj_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._recovered_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: FlortDjCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: FlortDjCsppInstrumentRecoveredDataParticle
            }
        }

        self._telemetered_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: FlortDjCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: FlortDjCsppInstrumentTelemeteredDataParticle,
            }
        }

    def test_simple(self):
        """
        retrieves and verifies the first 6 particles
        """
        file_path = os.path.join(RESOURCE_PATH, 'first_data.txt')
        stream_handle = open(file_path, 'rU')

        parser = FlortDjCsppParser(self._recovered_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(20)

        self.assert_particles(particles, 'first_data_20_recovered.yml', RESOURCE_PATH)

        self.assertEqual(len(self.exception_callback_value), 0)

        stream_handle.close()

    def test_simple_telem(self):
        """
        retrieves and verifies the first 6 particles
        """
        file_path = os.path.join(RESOURCE_PATH, 'first_data.txt')
        stream_handle = open(file_path, 'rU')

        parser = FlortDjCsppParser(self._telemetered_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(20)

        self.assert_particles(particles, 'first_data_20_telemetered.yml', RESOURCE_PATH)

        self.assertEqual(len(self.exception_callback_value), 0)

        stream_handle.close()

    def test_long_stream(self):
        """
        retrieve all of particles, verify the expected number, confirm results
        """
        file_path = os.path.join(RESOURCE_PATH, 'first_data.txt')
        stream_handle = open(file_path, 'rU')

        parser = FlortDjCsppParser(self._recovered_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(1000)

        self.assertTrue(len(particles) == 193)

        self.assert_particles(particles, 'first_data_recovered.yml', RESOURCE_PATH)

        self.assertEqual(len(self.exception_callback_value), 0)

        stream_handle.close()

    def test_long_stream_telem(self):
        """
        retrieve all of particles, verify the expected number, confirm results
        """
        file_path = os.path.join(RESOURCE_PATH, 'first_data.txt')
        stream_handle = open(file_path, 'rU')

        parser = FlortDjCsppParser(self._telemetered_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(1000)

        self.assertTrue(len(particles) == 193)

        self.assert_particles(particles, 'first_data_telemetered.yml', RESOURCE_PATH)

        self.assertEqual(len(self.exception_callback_value), 0)

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists. A variety of malformed
        records are used in order to verify this
        """

        file_path = os.path.join(RESOURCE_PATH, 'BAD.txt')
        stream_handle = open(file_path, 'rU')

        parser = FlortDjCsppParser(self._recovered_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(4)

        self.assert_particles(particles, 'BAD_recovered.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

        stream_handle.close()

    def test_bad_data_telem(self):
        """
        Ensure that bad data is skipped when it exists. A variety of malformed
        records are used in order to verify this
        """

        file_path = os.path.join(RESOURCE_PATH, 'BAD.txt')
        stream_handle = open(file_path, 'rU')

        parser = FlortDjCsppParser(self._telemetered_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(4)

        self.assert_particles(particles, 'BAD_telemetered.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

        stream_handle.close()
