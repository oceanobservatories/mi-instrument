#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_adcpt_m_log9.py
@author Tapana Gupta
@brief Test code for adcpt_m_log9 data parser

Files used for testing:

ADCPT_M_LOG9_simple.txt
  File contains 25 valid data records

ADCPT_M_LOG9_large.txt
  File contains 615 valid data records

ADCPT_M_LOG9_bad.txt
  File contains 4 invalid data records

"""

import unittest
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger; log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcpt_m_log9 import AdcptMLog9Parser

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'adcpt_m', 'resource')

MODULE_NAME = 'mi.dataset.parser.adcpt_m_log9'

SIMPLE_LOG_FILE = "ADCPT_M_LOG9_simple.txt"
LARGE_LOG_FILE = "ADCPT_M_LOG9_large.txt"

# Define number of expected records/exceptions for various tests
NUM_REC_LARGE_LOG_FILE = 615
NUM_REC_SIMPLE_LOG_FILE = 25

YAML_FILE = "ADCPT_M_LOG9_simple.yml"
LARGE_YAML_FILE = "ADCPT_M_LOG9_large.yml"

INVALID_DATA_FILE_1 = 'ADCPT_M_LOG9_bad.txt'

NUM_INVALID_EXCEPTIONS = 9


@attr('UNIT', group='mi')
class AdcptMLog9ParserUnitTestCase(ParserUnitTestCase):
    """
    adcpt_m_log9 Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def open_file_write(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='w')
        return file

    def create_rec_parser(self, file_handle):
        """
        This function creates a Adcpt_m_log9 parser for recovered data.
        """
        parser = AdcptMLog9Parser(self.rec_config,
                                   file_handle,
                                   self.exception_callback)
        return parser

    def test_verify_record(self):
        """
        Simple test to verify that records are successfully read and parsed from a data file
        """
        log.debug('===== START SIMPLE TEST =====')
        in_file = self.open_file(SIMPLE_LOG_FILE)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_SIMPLE_LOG_FILE
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END SIMPLE TEST =====')

    def test_invalid_data(self):
        """
        Read data from a file containing invalid data.
        Verify that no particles are created and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_DATA_FILE_1)
        parser = self.create_rec_parser(in_file)

        # Try to get records and verify that none are returned.
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(len(self.exception_callback_value), NUM_INVALID_EXCEPTIONS)

        in_file.close()

        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_verify_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(LARGE_LOG_FILE)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_LARGE_LOG_FILE
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, LARGE_YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END YAML TEST =====')

    def create_yml_file(self):
        """
        Create a yml file corresponding to an actual recovered dataset. This is not an actual test - it allows
        us to create what we need for integration testing, i.e. a yml file.
        """
        in_file = self.open_file(LARGE_LOG_FILE)
        parser = self.create_rec_parser(in_file)

        log.debug("Getting records...")

        # In a single read, get all particles in this file.
        result = parser.get_records(NUM_REC_LARGE_LOG_FILE)

        log.debug("Done.")

        self.particle_to_yml(result, LARGE_YAML_FILE)

    def particle_to_yml(self, particles, filename):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = self.open_file_write(filename)
        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')
        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            fid.write('  - _index: %d\n' % (i+1))
            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))
            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.3f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()
