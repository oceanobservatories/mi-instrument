#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_zplsc_c
@file mi-dataset/mi/dataset/parser/test/test_zplsc_c.py
@author Rene Gelinas
@brief Test code for a zplsc_c data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.zplsc_c.resource import RESOURCE_PATH
from mi.dataset.parser.zplsc_c import ZplscCParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.zplsc_c'
CLASS_NAME = 'ZplscCRecoveredDataParticle'
PARTICLE_TYPE = 'zplsc_c_recovered'


@attr('UNIT', group='mi')
class ZplscCParserUnitTestCase(ParserUnitTestCase):
    """
    Zplsc_c Parser unit test suite
    """

    def create_zplsc_c_parser(self, file_handle):
        """
        This function creates a ZplscCDCL parser for recovered data.
        """
        return ZplscCParser(self.config, file_handle, self.rec_exception_callback)

    def file_path(self, filename):
        log.debug('resource path = %s, file name = %s', RESOURCE_PATH, filename)
        return os.path.join(RESOURCE_PATH, filename)

    def rec_exception_callback(self, exception):
        """
        Call back method to watch what comes in via the exception callback
        """
        self.exception_callback_value.append(exception)
        self.exceptions_detected += 1

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
        }

        self.exception_callback_value = []
        self.exceptions_detected = 0

    def test_zplsc_c_parser(self):
        """
        Test Zplsc C parser
        Just test that it is able to parse the file and records are generated.
        """
        log.debug('===== START TEST ZPLSC_C Parser =====')

        with open(self.file_path('15100520-Test.01A')) as in_file:

            parser = self.create_zplsc_c_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(10)

            self.assertEqual(len(result), 10)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST ZPLSC_C Parser  =====')

    def test_recovered(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """

        log.debug('===== START TEST TELEM  =====')

        with open(self.file_path('15100520-Test.01A'), 'rb') as in_file:

            parser = self.create_zplsc_c_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(10)

            self.assertEqual(len(result), 10)
            self.assert_particles(result, '15100520-Test.01A.yml', RESOURCE_PATH)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST TELEM  =====')

    def test_large_recovered(self):
        """
        Read a large file and pull out a data particle.
        Verify that the results are those we expected.
        """

        log.debug('===== START LARGE RECOVERED  =====')

        with open(self.file_path('16100100-Test.01A'), 'rb') as in_file:

            parser = self.create_zplsc_c_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(60)

            self.assertEqual(len(result), 60)
            self.assert_particles(result, '16100100-Test.01A.yml', RESOURCE_PATH)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END LARGE RECOVERED  =====')

    def test_variable_length_channels(self):
        """
        The raw data binary file used in the test_recovered test above was modifed.
        The channel data for the first two records have been modified by removing a
        random number of values from the four "channel values" lists.  The new number
        of bins is updated in the "number of bins" parameter for those records.  The
        yml file used for the test_recovered test was used as a starting point and
        the same changes made in the raw data file were applied to the expected results
        yml file.
        """
        log.debug('===== START TEST VARIABLE NUM OF CHANNELS =====')

        with open(self.file_path('15100520-Test-Var_Chans.01A')) as in_file:

            parser = self.create_zplsc_c_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(10)

            self.assertEqual(len(result), 10)
            self.assert_particles(result, '15100520-Test-Var_Chans.01A.yml', RESOURCE_PATH)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST VARIABLE NUM OF CHANNELS =====')

    def test_bad_timestamp(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        log.debug('===== START TEST BAD TIMESTAMP  =====')

        with open(self.file_path('15100520-Test-Corrupt.01A')) as in_file:

            parser = self.create_zplsc_c_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(10)

            self.assertEqual(len(result), 8)
            self.assert_particles(result, '15100520-Test-Corrupt.01A.yml', RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 2)
            for i in range(len(self.exception_callback_value)):
                log.error('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD TIMESTAMP  =====')

    def test_bad_delimiter(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        log.debug('===== START TEST BAD DELIMITER  =====')

        with open(self.file_path('15100520-Test-Corrupt-1.01A')) as in_file:

            parser = self.create_zplsc_c_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(10)

            self.assertEqual(len(result), 9)
            self.assert_particles(result, '15100520-Test-Corrupt-1.01A.yml', RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 1)
            for i in range(len(self.exception_callback_value)):
                log.error('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD DELIMITER  =====')

    def create_large_yml(self):
        """
        Create a large yml file corresponding to an actual recovered dataset.
        This is not an actual test - it allows us to create what we need
        for integration testing, i.e. a yml file.
        """

        with open(self.file_path('16100100-Test.01A')) as in_file:

            parser = self.create_zplsc_c_parser(in_file)
            result = parser.get_records(1000)

            out_file = '.'.join([in_file.name, 'yml'])

            self.particle_to_yml(result, out_file)

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests.
        Since the same particles will be used for the driver test it is helpful to
        write them to .yml in the same form they need in the results.yml file here.
        """
        # open write append, if you want to start from scratch manually delete this file
        with open(self.file_path(filename), mode) as fid:
            fid.write('header:\n')
            fid.write("    particle_object: %s\n" % CLASS_NAME)
            fid.write("    particle_type: %s\n" % PARTICLE_TYPE)
            fid.write('data:\n')
            for index in range(len(particles)):
                particle_dict = particles[index].generate_dict()
                fid.write('  - _index: %d\n' % (index+1))
                fid.write('    internal_timestamp: %.7f\n' % particle_dict.get('internal_timestamp'))
                fid.write('    port_timestamp: %.7f\n' % particle_dict.get('port_timestamp'))

                values_dict = {}
                for value in particle_dict.get('values'):
                    values_dict[value.get('value_id')] = value.get('value')

                for key in sorted(values_dict.iterkeys()):
                    value = values_dict[key]
                    if value is None:
                        fid.write('    %s: %s\n' % (key, 'Null'))
                    elif isinstance(value, float):
                        fid.write('    %s: %15.4f\n' % (key, value))
                    elif isinstance(value, str):
                        fid.write("    %s: '%s'\n" % (key, value))
                    else:
                        fid.write('    %s: %s\n' % (key, value))
