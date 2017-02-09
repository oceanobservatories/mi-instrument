#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_flord_l_wfp
@file marine-integrations/mi/dataset/parser/test/test_flord_l_wfp.py
@author Joe Padula
@brief Test code for a flord_l_wfp data parser (which uses the GlobalWfpEFileParser)
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.flord_l_wfp.resource import RESOURCE_PATH
from mi.dataset.parser.global_wfp_e_file_parser import GlobalWfpEFileParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class FlordLWfpParserUnitTestCase(ParserUnitTestCase):
    """
    Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Call back method to match what comes in via the exception callback """
        self.exception_callback_value = exception

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpInstrumentParserDataParticle'
        }

    def test_simple(self):
        """
        Read test data and pull out data particles six at a time.
        Assert that the results of sixth particle are those we expected.
        """

        file_path = os.path.join(RESOURCE_PATH, 'small.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, None, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        particles = parser.get_records(6)

        log.debug("particles: %s", particles)
        for particle in particles:
            log.info("*** test particle: %s", particle.generate_dict())

        self.assert_particles(particles, 'good.yml', RESOURCE_PATH)

        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, None, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        particles = parser.get_records(50)

        # Should end up with 20 particles
        self.assertTrue(len(particles) == 50)

        self.assert_particles(particles, 'E0000001_50.yml', RESOURCE_PATH)
        stream_handle.close()

    def test_long_stream(self):
        """
        Test a long stream
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, None, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        particles = parser.get_records(1000)
        # File is 20,530 bytes
        #   minus 24 header
        #   minus 16 footer
        #   each particle is 30 bytes
        # Should end up with 683 particles
        self.assertTrue(len(particles) == 683)
        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-DATA.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, None, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        with self.assertRaises(SampleException):
            parser.get_records(1)

        stream_handle.close()

    def test_bad_header(self):
        """
        Ensure that bad header is skipped when it exists.
        """

        # This case tests against a header that does not match
        # 0000 0000 0000 0100 0000 0000 0000 0151
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-HEADER1.DAT')
        stream_handle = open(file_path, 'rb')

        with self.assertRaises(SampleException):
            GlobalWfpEFileParser(self.config, None, stream_handle,
                                 self.state_callback, self.pub_callback, self.exception_callback)

        stream_handle.close()

        # This case tests against a header that does not match global E-type data, but matches coastal
        # E-type data: 0001 0000 0000 0000 0001 0001 0000 0000
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-HEADER2.DAT')
        stream_handle = open(file_path, 'rb')

        with self.assertRaises(SampleException):
            GlobalWfpEFileParser(self.config, None, stream_handle,
                                 self.state_callback, self.pub_callback, self.exception_callback)

        stream_handle.close()
