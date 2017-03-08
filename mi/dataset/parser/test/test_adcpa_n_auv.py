#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_adcpa_n_auv.py
@author Jeff Roy
@brief Test code for a adcpa_n_auv data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException, SampleEncodingException
from mi.core.log import get_logger
from mi.dataset.driver.adcpa_n.auv.resource import RESOURCE_PATH
from mi.dataset.parser.adcpa_n_auv import AdcpaNAuvParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class AdcpaNAuvTestCase(ParserUnitTestCase):
    """
    adcpa_n_auv Parser unit test suite
    """

    def test_simple(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first input record to be skipped due to invalid timestamp
        """

        fid = open(os.path.join(RESOURCE_PATH, 'subset_reduced.csv'), 'rU')

        stream_handle = fid
        parser = AdcpaNAuvParser(stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(22)

        self.assert_particles(particles, 'adcpa_n_auv_22.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        fid.close()

    def test_long_stream(self):
        """
        Read test data and pull out data particles.
        Assert the expected number of particles is captured and there are no exceptions
        """

        fid = open(os.path.join(RESOURCE_PATH, 'subset.csv'), 'rU')

        stream_handle = fid
        parser = AdcpaNAuvParser(stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(4130)

        self.assertEqual(len(particles), 4130)

        self.assertEqual(self.exception_callback_value, [])

        fid.close()

    def test_missing_param(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get an error due to missing parameters
        """

        fid = open(os.path.join(RESOURCE_PATH, 'subset_missing_param.csv'), 'rU')

        stream_handle = fid
        parser = AdcpaNAuvParser(stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(4)  # ask for 4 should get 3

        self.assert_particles(particles, 'adcpa_n_auv_missing_param.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

        fid.close()

    def test_bad_param(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get an error due to incorrect parameter format
        """

        fid = open(os.path.join(RESOURCE_PATH, 'subset_bad_param.csv'), 'rU')

        stream_handle = fid
        parser = AdcpaNAuvParser(stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(4)  # ask for 4 should get 3

        self.assert_particles(particles, 'adcpa_n_auv_bad_param.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], SampleEncodingException)

        fid.close()

    def test_bad_timestamp(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get an error due to incorrect epoch format
        """

        fid = open(os.path.join(RESOURCE_PATH, 'subset_bad_epoch.csv'), 'rU')

        stream_handle = fid
        parser = AdcpaNAuvParser(stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(4)  # ask for 4 should get 3

        self.assert_particles(particles, 'adcpa_n_auv_bad_epoch.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

        fid.close()
