#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_dosta_ln_auv.py
@author Jeff Roy
@brief Test code for a dosta_ln_auv data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException, SampleEncodingException
from mi.core.log import get_logger
from mi.dataset.driver.dosta_ln.auv.resource import RESOURCE_PATH
from mi.dataset.parser.dosta_ln_auv import DostaLnAuvParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class DostaLnAuvTestCase(ParserUnitTestCase):
    """
    adcpa_n_auv Parser unit test suite
    """

    def test_simple_telem(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first input record to be skipped due to invalid timestamp
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset_reduced.csv'), 'rU')

        parser = DostaLnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=True)

        particles = parser.get_records(20)

        self.assert_particles(particles, 'dosta_ln_telem_20.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_simple_recov(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first input record to be skipped due to invalid timestamp
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset_reduced.csv'), 'rU')

        parser = DostaLnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=False)

        particles = parser.get_records(20)

        self.assert_particles(particles, 'dosta_ln_recov_20.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_long_stream_telem(self):
        """
        Read test data and pull out data particles.
        Assert the expected number of particles is captured and there are no exceptions
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset.csv'), 'rU')

        parser = DostaLnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=True)

        particles = parser.get_records(5000)

        self.assertEqual(len(particles), 4201)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_missing_param_telem(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get an error due to missing parameters
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'dosta_ln_missing_param.csv'), 'rU')

        parser = DostaLnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=True)

        particles = parser.get_records(4)  # ask for 4 should get 3

        self.assert_particles(particles, 'dosta_ln_missing_param_telem.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

        stream_handle.close()

    def test_bad_param_recov(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get an error due to incorrect parameter format
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'dosta_ln_bad_param.csv'), 'rU')

        parser = DostaLnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=False)

        particles = parser.get_records(4)  # ask for 4 should get 3

        self.assert_particles(particles, 'dosta_ln_bad_param_recov.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], SampleEncodingException)

        stream_handle.close()

    def test_bad_timestamp_telem(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 2 errors due to incorrect epoch format
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'dosta_ln_bad_timestamps.csv'), 'rU')

        parser = DostaLnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=True)

        particles = parser.get_records(20)  # ask for 20 should get 18

        self.assert_particles(particles, 'dosta_ln_bad_timestamps_telem.yml', RESOURCE_PATH)

        self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
        self.assertIsInstance(self.exception_callback_value[1], RecoverableSampleException)

        stream_handle.close()

