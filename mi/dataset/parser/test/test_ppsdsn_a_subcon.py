#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_ppsdn_a_subcon
@fid marine-integrations/mi/dataset/parser/test/test_ppsdn_a_subcon.py
@author Rachel Manoni
@brief Test code for RASFL data parser
"""
from mi.dataset.parser.ppsdn_a_subcon import PpsdnASubconParser

__author__ = 'Rachel Manoni'

import os
from mi.core.log import get_logger
log = get_logger()
from nose.plugins.attrib import attr
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'ppsdn', 'resource')
LOG_FILE = 'ppsdn_gendata_example.csv'
YAML_FILE = 'ppsdn_gendata_example.yml'
INVALID_DATA_FILE = 'ppsdn_bad_example.csv'
NUM_RECORDS = 24


@attr('UNIT', group='mi')
class PpsndASubconParserUnitTestCase(ParserUnitTestCase):
    """
    ppsdn_a_subcon Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ppsdn_a_subcon',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

    def open_file(self, filename):
        return open(os.path.join(RESOURCE_PATH, filename), mode='r')

    def open_file_write(self, filename):
        return open(os.path.join(RESOURCE_PATH, filename), mode='w')

    def create_rec_parser(self, file_handle):
        return PpsdnASubconParser(self.rec_config, file_handle, self.exception_callback)

    def test_invalid_data(self):
        """
        Read data from a file containing invalid data.
        Verify that no particles are created and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_DATA_FILE)
        parser = self.create_rec_parser(in_file)

        # Try to get records and verify that none are returned.
        # Input file's records contain all invalid samples
        result = parser.get_records(1)
        self.assertEqual(result, [])

        in_file.close()
        log.debug('===== END TEST INVALID SENSOR DATA =====')

    def test_verify_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(LOG_FILE)
        parser = self.create_rec_parser(in_file)

        #uncomment to create yml results file
        #self.create_yml_file()

        result = parser.get_records(NUM_RECORDS)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])
        log.debug('===== END YAML TEST =====')

    def create_yml_file(self):
        """
        Create a yml file corresponding to an actual recovered dataset. This is not an actual test - it allows
        us to create what we need for integration testing, i.e. a yml file.
        """
        in_file = self.open_file(LOG_FILE)
        parser = self.create_rec_parser(in_file)
        log.debug("Getting records...")
        result = parser.get_records(NUM_RECORDS)
        log.debug("Done.")
        self.particle_to_yml(result, YAML_FILE)
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


