#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid mi-instrument/mi/dataset/parser/test/test_ctdav_nbosi_auv.py
@author Rene Gelinas
@brief Test code for a ctdav_nbosi_auv data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.driver.ctdav_nbosi.auv.resource import RESOURCE_PATH
from mi.dataset.parser.ctdav_nbosi_auv import CtdavNbosiAuvParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class CtdavNbosiAuvTestCase(ParserUnitTestCase):
    """
    ctdav_nbosi_auv Parser unit test suite
    """

    def test_simple(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        Expect the first two input records to be skipped due to invalid timestamp.
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'CP05MOAS-A6264_AUVsubset_reduced.csv'), 'rU')

        parser = CtdavNbosiAuvParser(stream_handle,
                                     self.exception_callback)

        particles = parser.get_records(20)

        self.assert_particles(particles, 'ctdav_nbosi_auv.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_long_stream(self):
        """
        Read test data and pull out data particles.
        Assert the expected number of particles is captured and there are no exceptions
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'CP05MOAS-A6264_AUVsubset.csv'), 'rU')

        parser = CtdavNbosiAuvParser(stream_handle,
                                     self.exception_callback)

        particles = parser.get_records(10000)

        self.assertEqual(len(particles), 10000)

        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()
