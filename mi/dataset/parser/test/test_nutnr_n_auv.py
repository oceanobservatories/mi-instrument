#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_nutnr_n_auv.py
@author Jeff Roy
@brief Test code for a nutnr_n_auv data parser

NOTE:  As this is the 5th parser built from AuvCommonParser
full negative testing is not done.  See dosta_ln_auv and adcpa_n_auv
for complete testing of AuvCommonParser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleEncodingException
from mi.core.log import get_logger
from mi.dataset.driver.nutnr_n.auv.resource import RESOURCE_PATH
from mi.dataset.parser.nutnr_n_auv import NutnrNAuvParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class NutnrNAuvTestCase(ParserUnitTestCase):
    """
    adcpa_n_auv Parser unit test suite
    """

    def test_simple(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first input record to be skipped due to invalid timestamp
        """

        with open(os.path.join(RESOURCE_PATH, 'subset2_reduced.csv'), 'rU') as stream_handle:

            parser = NutnrNAuvParser(stream_handle,
                                     self.exception_callback)

            particles = parser.get_records(25)  # ask for 25 should get 23

            self.assert_particles(particles, 'nutnr_n_auv_telem_23.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_data(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect 2 sample encoding errors
        """

        with open(os.path.join(RESOURCE_PATH, 'nutnr_n_auv_bad_data.csv'), 'rU') as stream_handle:

            parser = NutnrNAuvParser(stream_handle,
                                     self.exception_callback)

            particles = parser.get_records(8)  # ask for 8 should get 7

            self.assert_particles(particles, 'nutnr_n_auv_bad_data_7.yml', RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], SampleEncodingException)
            self.assertIsInstance(self.exception_callback_value[1], SampleEncodingException)

    def test_long_stream(self):
        """
        Read test data and pull out data particles.
        Assert the expected number of particles is captured and there are no exceptions
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'subset2.csv'), 'rU')

        parser = NutnrNAuvParser(stream_handle,
                                 self.exception_callback)

        particles = parser.get_records(10000)

        self.assertEqual(len(particles), 6823)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

