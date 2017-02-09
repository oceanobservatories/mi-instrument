"""
@package mi.dataset.parser.test
@file mi/dataset/parser/test/test_nutnr_m_glider.py
@author Emily Hahn
@brief A test parser for the nutnr series m instrument through a glider
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.exceptions import SampleException, ConfigurationException, DatasetParserException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.glider import GliderParser

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'nutnr_m', 'glider', 'resource')


@attr('UNIT', group='mi')
class NutnrMGliderParserUnitTestCase(ParserUnitTestCase):

    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'NutnrMDataParticle'
    }

    def test_simple(self):
        """
        Test a simple case that we can parse a single message
        """
        with open(os.path.join(RESOURCE_PATH, 'single.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            particles = parser.get_records(1)

            self.assert_particles(particles, "single.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_many(self):
        """
        Test a simple case with more messages
        """
        with open(os.path.join(RESOURCE_PATH, 'many.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            particles = parser.get_records(12)
            # requested more than are available in file, should only be 10
            self.assertEquals(len(particles), 10)

            self.assert_particles(particles, "many.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_full(self):
        """
        Test a full file and confirm the right number of particles is returned
        """
        with open(os.path.join(RESOURCE_PATH, 'unit_514-2014-351-2-0.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            particles = parser.get_records(40)
            # requested more than are available in file, should only be 10
            self.assertEquals(len(particles), 31)

            self.assertEqual(self.exception_callback_value, [])

    def test_empty(self):
        """
        An empty file will return a sample exception since it cannot read the header
        """
        file_handle = open(os.path.join(RESOURCE_PATH, 'empty.mrg'), 'rU')

        with self.assertRaises(DatasetParserException):
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            particles = parser.get_records(1)
            # requested more than are available in file, should only be 10
            self.assertEquals(len(particles), 0)

    def test_bad_config(self):
        """
        Test that a set of bad configurations produces the expected exceptions
        """
        file_handle = open(os.path.join(RESOURCE_PATH, 'single.mrg'), 'rU')

        # confirm a configuration exception occurs if no config is passed in
        with self.assertRaises(ConfigurationException):
            GliderParser({}, file_handle, self.exception_callback)

        # confirm a config missing the particle class causes an exception
        bad_config = {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider'}
        with self.assertRaises(ConfigurationException):
            GliderParser(bad_config, file_handle, self.exception_callback)

        # confirm a config with a non existing class causes an exception
        bad_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'BadDataParticle'
        }
        with self.assertRaises(AttributeError):
            GliderParser(bad_config, file_handle, self.exception_callback)

    def test_bad_headers(self):
        """
        Test that a file with a short header raises a sample exception
        """

        # this file does not have enough header lines
        file_handle = open(os.path.join(RESOURCE_PATH, 'short_header.mrg'), 'rU')

        with self.assertRaises(DatasetParserException):
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            parser.get_records(1)

        # this file specifies a number of header lines other than 14
        file_handle = open(os.path.join(RESOURCE_PATH, 'bad_num_header_lines.mrg'), 'rU')

        with self.assertRaises(DatasetParserException):
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            parser.get_records(1)

        # this file specifies a number of label lines other than 3
        file_handle = open(os.path.join(RESOURCE_PATH, 'bad_num_label_lines.mrg'), 'rU')

        with self.assertRaises(DatasetParserException):
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            parser.get_records(1)

    def test_missing_time(self):
        """
        Test that a file which is missing the required m_present_time field for timestamps raises a sample exception
        """
        # this file is missing the m_present_time label
        file_handle = open(os.path.join(RESOURCE_PATH, 'no_time_label.mrg'), 'rU')

        with self.assertRaises(DatasetParserException):
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            parser.get_records(1)

    def test_short_data(self):
        """
        Test that if the number of columns in the header do not match the number of columns in the data an
        exception occurs
        """
        # this file is has two columns removed from the data libe
        file_handle = open(os.path.join(RESOURCE_PATH, 'short_data.mrg'), 'rU')

        with self.assertRaises(DatasetParserException):
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            parser.get_records(1)

    def test_bad_sensors_per_cycle(self):
        """
        Test that if the number of sensors per cycle from the header does not match that in the header that an
        exception in the callback occurs, but processing continues
        """
        with open(os.path.join(RESOURCE_PATH, 'bad_sensors_per_cycle.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            particles = parser.get_records(1)

            self.assert_particles(particles, "single.yml", RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 1)

            self.assertIsInstance(self.exception_callback_value[0], SampleException)

    def test_short_units(self):
        """
        Test that if the number of label columns does not match the units number of columns an exception occurs
        """
        # this file is has two columns removed from the data libe
        file_handle = open(os.path.join(RESOURCE_PATH, 'short_units.mrg'), 'rU')

        with self.assertRaises(DatasetParserException):
            parser = GliderParser(self.config, file_handle, self.exception_callback)

            parser.get_records(1)
