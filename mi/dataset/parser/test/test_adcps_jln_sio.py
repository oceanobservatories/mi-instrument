#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcps_jln_sio
@file mi/dataset/parser/test/test_adcps_jln_sio.py
@author Emily Hahn
@brief An set of tests for the adcps jln series through the sio dataset agent parser
"""
import struct
import yaml

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import RecoverableSampleException, UnexpectedDataException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.adcps_jln_sio import AdcpsJlnSioParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'adcps_jln', 'sio', 'resource')


@attr('UNIT', group='mi')
class AdcpsJlnSioParserUnitTestCase(ParserUnitTestCase):

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = \
            {
                DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcps_jln_sio',
                DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcpsJlnSioDataParticle'
            }

    def test_simple(self):
        """
        Read test data from 2 files and compare the particles in .yml files.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.adcps.dat')) as stream_handle:

            parser = AdcpsJlnSioParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(2)
            self.assert_particles(particles, "adcps_telem_1.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'node59p1_2.adcps.dat')) as stream_handle:
            parser = AdcpsJlnSioParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(3)
            self.assert_particles(particles, "adcps_telem_2.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        test a longer file
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_0.adcps.dat')) as stream_handle:

            parser = AdcpsJlnSioParser(self.config, stream_handle, self.exception_callback)
            # request more particles than are available, make sure we only get the number in the file
            particles = parser.get_records(150)
            self.assertEqual(len(particles), 130)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_xml_checksum(self):
        """
        test an exception is raised for a bad number of bytes
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_bad_xml_checksum.adcps.dat')) as stream_handle:

            parser = AdcpsJlnSioParser(self.config, stream_handle, self.exception_callback)
            # 2 records in file, first has bad xml checksum which should call exception
            particles = parser.get_records(2)
            self.assertEqual(len(particles), 1)

            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    def test_adcps_error(self):
        """
        test an exception is raised for an adcps error message
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_error.adcps.dat')) as stream_handle:

            parser = AdcpsJlnSioParser(self.config, stream_handle, self.exception_callback)
            # 2 records with error messages in them
            particles = parser.get_records(2)
            # make sure no particles were returned for the failure messages
            self.assertEqual(len(particles), 0)

            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
            self.assertIsInstance(self.exception_callback_value[1], RecoverableSampleException)

    def test_id_named_file(self):
        """
        Test with a new file containing the controller and instrument ID
        """
        with open(os.path.join(RESOURCE_PATH, 'node11p1_0.adcps_1327803.dat')) as stream_handle:

            parser = AdcpsJlnSioParser(self.config, stream_handle, self.exception_callback)
            # request more particles than are available, make sure we only get the number in the file
            particles = parser.get_records(1000)
            self.assertEqual(len(particles), 831)

            self.assertEqual(len(self.exception_callback_value), 52)
            for i in range(0, 52):
                if i in [12, 15, 35, 47]:
                    self.assertIsInstance(self.exception_callback_value[i], UnexpectedDataException)
                else:
                    self.assertIsInstance(self.exception_callback_value[i], RecoverableSampleException)


def swap(val):
    return struct.unpack('<h', struct.pack('>h', val))[0]


def swap_list(values):
    return [swap(v) for v in values]


def convert_yml(input_file):
    records = yaml.load(open(input_file))
    fields = [
        'error_velocity',
        'water_velocity_up',
        'water_velocity_north',
        'water_velocity_east',
    ]

    for record in records['data']:
        for field in fields:
            values = record[field]
            record[field] = swap_list(values)

    yaml.dump(records, open(input_file, 'w'))


def convert_all():
    files = [
        'adcps_telem_1.yml',
        'adcps_telem_2.yml',
    ]

    for f in files:
        convert_yml(os.path.join(RESOURCE_PATH, f))

