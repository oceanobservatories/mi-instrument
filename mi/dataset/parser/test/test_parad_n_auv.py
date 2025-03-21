#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_parad_n_auv.py
@author Jeff Roy
@brief Test code for a parad_n_auv data parser

NOTE:  As this is the 5th parser built from AuvCommonParser
full negative testing is not done.  See dosta_ln_auv and adcpa_n_auv
for complete testing of AuvCommonParser


"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.driver.parad_n.auv.resource import RESOURCE_PATH
from mi.dataset.parser.parad_n_auv import ParadNAuvParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class ParadNAuvTestCase(ParserUnitTestCase):
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

        parser = ParadNAuvParser(stream_handle,
                                 self.exception_callback,
                                 is_telemetered=True)

        particles = parser.get_records(22)

        self.assert_particles(particles, 'parad_n_auv_telem_22.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_simple_recov(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first input record to be skipped due to invalid timestamp
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset_reduced.csv'), 'rU')

        parser = ParadNAuvParser(stream_handle,
                                 self.exception_callback,
                                 is_telemetered=False)

        particles = parser.get_records(22)

        self.assert_particles(particles, 'parad_n_auv_recov_22.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_long_stream_telem(self):
        """
        Read test data and pull out data particles.
        Assert the expected number of particles is captured and there are no exceptions
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset.csv'), 'rU')

        parser = ParadNAuvParser(stream_handle,
                                 self.exception_callback,
                                 is_telemetered=True)

        particles = parser.get_records(5000)
        # there are over 77,000 samples in the file.  5000 should suffice!

        self.assertEqual(len(particles), 5000)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()
