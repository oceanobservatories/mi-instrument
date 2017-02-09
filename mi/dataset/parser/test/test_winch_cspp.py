#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_winch_cspp.py
@author Richard Han
@brief Test code for Winch Cspp data parser

Files used for testing:

20141114-194242-WINCH.LOG
  Contains engineering data for CSPP platform

"""


import unittest
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger; log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.winch_cspp import WinchCsppParser

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'winch_cspp', 'resource')

MODULE_NAME = 'mi.dataset.parser.winch_cspp'
CLASS_NAME = 'WinchCsppDataParticle'

WINCH_CSPP_LOG_FILE = "20141114-194242-WINCH.LOG"
WINCH_CSPP_LOG_FILE_2 = "20141114-194242-WINCH_2.LOG"

# Define number of expected records/exceptions for various tests
NUM_REC_WINCH_CSPP_LOG_FILE = 1617

YAML_FILE = "winch_cspp_test_data.yml"

INVALID_DATA_FILE_1 = '20141114-194242-WINCH_invalid1.LOG'

NUM_INVALID_EXCEPTIONS = 5


@attr('UNIT', group='mi')
class WinchCsppParserUnitTestCase(ParserUnitTestCase):
    """
    winch_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
        }

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def open_file_write(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='w')
        return file

    def create_parser(self, file_handle):
        """
        This function creates a WinchCspp parser for Winch CSPP data.
        """
        parser = WinchCsppParser(self.rec_config,
                                   file_handle,
                                   self.exception_callback)
        return parser

    def test_verify_record(self):
        """
        Simple test to verify that records are successfully read and parsed from a data file
        """
        log.debug('===== START SIMPLE TEST =====')
        in_file = self.open_file(WINCH_CSPP_LOG_FILE)
        parser = self.create_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_WINCH_CSPP_LOG_FILE
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END SIMPLE TEST =====')

    def test_invalid_data(self):
        """
        Test the parser to handle non conformed format data. There are a total of six records in the
        test file. The first line contains good data and the next 5 lines are bad lines with either wrong
        delimiter (expect white space but found ',') or wrong input type (expect integer but found float).
        Verify that one particle is generated and five exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_DATA_FILE_1)
        parser = self.create_parser(in_file)

        number_expected_results = 1
        # Try to get records and verify that none are returned.
        result = parser.get_records(NUM_REC_WINCH_CSPP_LOG_FILE)
        self.assertEqual(len(result), 1)
        self.assertEqual(len(self.exception_callback_value), NUM_INVALID_EXCEPTIONS)

        in_file.close()

        log.debug('===== END TEST INVALID SENSOR DATA =====')


    def test_verify_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(WINCH_CSPP_LOG_FILE_2)
        parser = self.create_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = 6
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END YAML TEST =====')

    def create_yml_file(self):
        """
        Create a yml file corresponding to a Winch Cspp dataset. This is not an actual test. It allows
        us to create what we need for integration testing, i.e. a yml file.
        """
        in_file = self.open_file(WINCH_CSPP_LOG_FILE_2)
        parser = self.create_parser(in_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(100)

        self.particle_to_yml(result, YAML_FILE)

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        Write particle dictionaries to a yaml file
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
                    fid.write('    %s: %16.5f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()
