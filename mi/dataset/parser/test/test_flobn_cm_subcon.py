#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_flobn_cm_subcon
@fid marine-integrations/mi/dataset/parser/test/test_flobn_cm_subcon.py
@author Rachel Manoni
@brief Test code for FLOBN-CM data parser
"""
from mi.dataset.parser.flobn_cm_subcon import FlobnMSubconTemperatureParser, FlobnCSubconParser, FlobnMSubconParser

__author__ = 'Rachel Manoni'

import os
from mi.core.log import get_logger
log = get_logger()
from nose.plugins.attrib import attr
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'flobn', 'resource')

TEMPERATURE_LOG_FILE = 'FLOBN-M_Temp_Record_ver_0-05.csv'
TEMPERATURE_YAML_FILE = 'FLOBN-M_Temp_Record_ver_0-05.yml'
INVALID_TEMPERATURE_DATA_FILE = 'FLOBN-M_Temp_Record_bad.csv'
TEMPERATURE_RECORDS = 242

C_LOG_FILE = 'FLOBN-C_Sample_Record_ver_0-05.csv'
C_YAML_FILE = 'FLOBN-C_Sample_Record_ver_0-05.yml'
INVALID_C_DATA_FILE = 'FLOBN-C_Sample_Record_bad.csv'
C_RECORDS = 168

M_LOG_FILE = 'FLOBN-M_Sample_Record_ver_0-05.csv'
M_YAML_FILE = 'FLOBN-M_Sample_Record_ver_0-05.yml'
INVALID_M_DATA_FILE = 'FLOBN-M_Sample_Record_bad.csv'
M_RECORDS = 1008


@attr('UNIT', group='mi')
class FlobnCmSubconParserUnitTestCase(ParserUnitTestCase):
    """
    flobn_cm_subcon Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flobn_cm_subcon',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

    def open_file(self, filename):
        return open(os.path.join(RESOURCE_PATH, filename), mode='r')

    def open_file_write(self, filename):
        return open(os.path.join(RESOURCE_PATH, filename), mode='w')

    def create_temp_rec_parser(self, file_handle):
        return FlobnMSubconTemperatureParser(self.rec_config, file_handle, self.exception_callback)

    def create_c_parser(self, file_handle):
        return FlobnCSubconParser(self.rec_config, file_handle, self.exception_callback)

    def create_m_parser(self, file_handle):
        return FlobnMSubconParser(self.rec_config, file_handle, self.exception_callback)

    def create_yml_file(self, input_file, output_file, number_samples):
        """
        Create a yml file corresponding to an actual recovered dataset. This is not an actual test - it allows
        us to create what we need for integration testing, i.e. a yml file.
        """
        in_file = self.open_file(input_file)
        parser = self.create_c_parser(in_file)
        log.debug("Getting records...")
        result = parser.get_records(number_samples)
        log.debug("Done.")
        self.particle_to_yml(result, output_file)
        log.debug("File written")

    def particle_to_yml(self, particles, filename):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml here.
        """
        fid = self.open_file_write(filename)
        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')
        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            fid.write('  - _index: %d\n' % (i + 1))
            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))
            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def test_subcon_m_record_invalid_data(self):
        """
        Read data from a file containing invalid data.
        Verify that no particles are created and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_M_DATA_FILE)
        parser = self.create_m_parser(in_file)

        # Try to get records and verify that none are returned.
        # Input file's records contain all invalid samples
        result = parser.get_records(1)
        self.assertEqual(result, [])

        in_file.close()
        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_verify_subcon_m_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(M_LOG_FILE)
        parser = self.create_m_parser(in_file)

        #uncomment to create yml results file
        #self.create_yml_file(M_LOG_FILE, M_YAML_FILE, M_RECORDS)

        result = parser.get_records(M_RECORDS)
        self.assert_particles(result, M_YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])
        log.debug('===== END YAML TEST =====')

    def test_subcon_c_record_invalid_data(self):
        """
        Read data from a file containing invalid data.
        Verify that no particles are created and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_C_DATA_FILE)
        parser = self.create_c_parser(in_file)

        # Try to get records and verify that none are returned.
        # Input file's records contain all invalid samples
        result = parser.get_records(1)
        self.assertEqual(result, [])

        in_file.close()
        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_verify_subcon_c_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(C_LOG_FILE)
        parser = self.create_c_parser(in_file)

        #uncomment to create yml results file
        #self.create_yml_file(C_LOG_FILE, C_YAML_FILE, C_RECORDS)

        result = parser.get_records(C_RECORDS)
        self.assert_particles(result, C_YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])
        log.debug('===== END YAML TEST =====')

    def test_temp_record_invalid_data(self):
        """
        Read data from a file containing invalid data.
        Verify that no particles are created and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_TEMPERATURE_DATA_FILE)
        parser = self.create_temp_rec_parser(in_file)

        # Try to get records and verify that none are returned.
        # Input file's records contain all invalid samples
        result = parser.get_records(1)
        self.assertEqual(result, [])

        in_file.close()
        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_verify_temp_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(TEMPERATURE_LOG_FILE)
        parser = self.create_temp_rec_parser(in_file)

        #uncomment to create yml results file
        #self.create_yml_file(TEMPERATURE_LOG_FILE, TEMPERATURE_YAML_FILE, TEMPERATURE_RECORDS)

        result = parser.get_records(TEMPERATURE_RECORDS)
        self.assert_particles(result, TEMPERATURE_YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])
        log.debug('===== END YAML TEST =====')