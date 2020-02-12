#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_metbk_ct_dcl
@author Tim Fisher (recovered)
@brief Test code for a Metbk ct dcl data parser
Recovered CT files:
  SBE37SM-RS485_20190930_2019_09_30-no_records.hex- 0 CT records
  SBE37SM-RS485_20190930_2019_09_30-missing_end.hex - 3 CT records
  SBE37SM-RS485_20190930_2019_09_30-missing_serial.hex - 3 CT records
  SBE37SM-RS485_20190930_2019_09_30-simple.hex - 3 CT records
  SBE37SM-RS485_20190930_2019_09_30-long.hex - 99 CT records
"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.metbk_ct.dcl.metbk_ct_dcl_driver import MODULE_NAME, PARTICLE_CLASS
from mi.dataset.driver.metbk_ct.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.metbk_ct_dcl import MetbkCtDclParser
from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

FILE_LONG = 'SBE37SM-RS485_20190930_2019_09_30-long.hex'
FILE_MISSING_END = 'SBE37SM-RS485_20190930_2019_09_30-missing_end.hex'
FILE_MISSING_SERIAL = 'SBE37SM-RS485_20190930_2019_09_30-missing_serial.hex'
FILE_NO_RECORDS = 'SBE37SM-RS485_20190930_2019_09_30-no_records.hex'
FILE_SIMPLE = 'SBE37SM-RS485_20190930_2019_09_30-simple.hex'

YML_LONG = 'SBE37SM-RS485_20190930_2019_09_30-long.yml'
YML_SIMPLE = 'SBE37SM-RS485_20190930_2019_09_30-simple.yml'

RECORDS_LONG = 99       # number of records expected
RECORDS_SIMPLE = 3      # number of records expected


@attr('UNIT', group='mi')
class MetbkCtDclParserUnitTestCase(ParserUnitTestCase):

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: PARTICLE_CLASS
        }

    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def test_rec_long_stream(self):
        """
        Read test data with 99 detail records and pull out all particles from a file at once.
        """

        with open(os.path.join(RESOURCE_PATH, FILE_LONG), 'r') as in_file:
            parser = MetbkCtDclParser(self.config, in_file, self.exception_callback)

            particles = parser.get_records(RECORDS_LONG)
            #self.create_yml(particles, YML_LONG)
            self.assertEqual(len(particles), RECORDS_LONG)
            self.assert_particles(particles, YML_LONG, RESOURCE_PATH)

            # confirm there are no more particles in this file
            particles2 = parser.get_records(5)
            self.assertEqual(len(particles2), 0)

        self.assertEqual(self.exception_callback_value, [])

    def test_rec_simple(self):
        """
        Read test data with 3 detail records and pull out all particles from a file at once.
        """

        with open(os.path.join(RESOURCE_PATH, FILE_SIMPLE), 'r') as in_file:
            parser = MetbkCtDclParser(self.config, in_file, self.exception_callback)

            particles = parser.get_records(RECORDS_SIMPLE)
            #self.create_yml(particles, YML_SIMPLE)
            self.assertEqual(len(particles), RECORDS_SIMPLE)
            self.assert_particles(particles, YML_SIMPLE, RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_rec_missing_end(self):
        """
        Read a Recovered CT data file that has no end configuration record.
        Verify that no particles are generated.
        """
        with open(os.path.join(RESOURCE_PATH, FILE_MISSING_END), 'r') as in_file:
            parser = MetbkCtDclParser(self.config, in_file, self.exception_callback)

            # Not expecting any particles.
            expected_results = []

            # Try to get one particle and verify we didn't get any.
            result = parser.get_records(1)
            self.assertEqual(result, expected_results)

        self.assertEqual(self.exception_callback_value, [])

    def test_rec_missing_serial(self):
        """
        Read a Recovered CT data file that has no Serial record.
        Verify that no particles are generated.
        """
        with open(os.path.join(RESOURCE_PATH, FILE_MISSING_SERIAL), 'r') as in_file:
            parser = MetbkCtDclParser(self.config, in_file, self.exception_callback)

            # Not expecting any particles.
            expected_results = []

            # Try to get one particle and verify we didn't get any.
            result = parser.get_records(1)
            self.assertEqual(result, expected_results)

        self.assertEqual(self.exception_callback_value, [])

    def test_rec_no_records(self):
        """
        Read a Recovered CT data file that has no CT records.
        Verify that no particles are generated.
        """
        with open(os.path.join(RESOURCE_PATH, FILE_NO_RECORDS), 'r') as in_file:
            parser = MetbkCtDclParser(self.config, in_file, self.exception_callback)

            # Not expecting any particles.
            expected_results = []

            # Try to get one particle and verify we didn't get any.
            result = parser.get_records(1)
            self.assertEqual(result, expected_results)

        self.assertEqual(self.exception_callback_value, [])
