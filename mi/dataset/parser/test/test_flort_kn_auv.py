#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_flort_kn_auv.py
@author Jeff Roy
@brief Test code for a flort_kn_auv data parser

NOTE:  As this is the third parser built on AuvCommonParser testing of
the error checking was not done.  See adcpa_n_auv and dosta_ln_auv
for more complete testing of the AuvCommonParser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.driver.flort_kn.auv.resource import RESOURCE_PATH
from mi.dataset.parser.flort_kn_auv import FlortKnAuvParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class FlortKnAuvTestCase(ParserUnitTestCase):
    """
    flort_kn_auv Parser unit test suite
    """

    def test_simple_telem(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first input record to be skipped due to invalid timestamp
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset_reduced.csv'), 'rU')

        parser = FlortKnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=True)

        particles = parser.get_records(45)

        self.assert_particles(particles, 'flort_kn_auv_telem_45.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_simple_recov(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first input record to be skipped due to invalid timestamp
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset_reduced.csv'), 'rU')

        parser = FlortKnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=False)

        particles = parser.get_records(45)

        self.assert_particles(particles, 'flort_kn_auv_recov_45.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_long_stream_telem(self):
        """
        Read test data and pull out data particles.
        Assert the expected number of particles is captured and there are no exceptions
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset.csv'), 'rU')

        parser = FlortKnAuvParser(stream_handle,
                                  self.exception_callback,
                                  is_telemetered=True)
        particles = parser.get_records(10000)

        self.assertEqual(len(particles), 7697)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

