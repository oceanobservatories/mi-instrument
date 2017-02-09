#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_camds.py
@author Dan Mergens
@brief Test code for a camds data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.camds import CamdsHtmlParser
from mi.dataset.driver.camds.resource import RESOURCE_PATH
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


SAMPLE_DATA = ['20150816T160224,178', '20100716T001808,321', '20100101T140322,378', '20150219T170005,545',
               '20160420T191456,943', '20160923T080447,820']


@attr('UNIT', group='mi')
class CamdsParserUnitTestCase(ParserUnitTestCase):
    """
    camds_abc_dcl Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.camds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CamdsMetadataParticle',
        }

    def test_simple(self):
        """

        """
        for root in SAMPLE_DATA:
            data = root + '.htm'
            yml = root + '.yml'
            file_path = os.path.join(RESOURCE_PATH, data)
            # stream_handle = open(file_path, 'r')
            with open(file_path, 'r') as stream_handle:

                parser = CamdsHtmlParser(stream_handle, self.exception_callback)

                particles = parser.get_records(1)

            log.debug("*** test_simple Num particles %s", len(particles))

            self.assert_particles(particles, yml, RESOURCE_PATH)

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists and a RecoverableSampleException is thrown.
        Note: every other data record has bad data (float instead of int, extra column etc.)
        We don't have any examples of corrupt html from the CAMDS, so we'll just try to read in one of the YAML files
        instead of using a known corrupt file.
        """

        file_path = os.path.join(RESOURCE_PATH, SAMPLE_DATA[0] + '.yml')
        with open(file_path, 'rU') as stream_handle:

            with self.assertRaises(SampleException):
                parser = CamdsHtmlParser(stream_handle, self.exception_callback)

                parser.get_records(1)
