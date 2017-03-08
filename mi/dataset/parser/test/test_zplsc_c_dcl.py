#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_zplsc_c_dcl
@file mi-dataset/mi/dataset/parser/test/test_zplsc_c_dcl.py
@author Richard Han (Raytheon), Ronald Ronquillo (Raytheon)
@brief Test code for a zplsc_c_dcl data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.zplsc_c.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.zplsc_c_dcl import ZplscCDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.zplsc_c_dcl'
CLASS_NAME = 'ZplscCInstrumentDataParticle'
PARTICLE_TYPE = 'zplsc_c_instrument'


@attr('UNIT', group='mi')
class ZplscCDclParserUnitTestCase(ParserUnitTestCase):
    """
    Zplsc_c_dcl Parser unit test suite
    """

    def create_zplsc_c_dcl_parser(self, file_handle):
        """
        This function creates a ZplscCDCL parser for recovered data.
        """
        return ZplscCDclParser(self.config, file_handle, self.rec_exception_callback)

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

    def test_zplsc_c_dcl_parser(self):
        """
        Test Zplsc C DCL parser
        Just test that it is able to parse the file and records are generated.
        """
        log.debug('===== START TEST ZPLSC_C_DCL Parser =====')

        with open(self.file_path('20150406.zplsc.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(15)

            self.assertEqual(len(result), 1)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST ZPLSC_C_DCL Parser  =====')

    def test_telem(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """

        log.debug('===== START TEST TELEM  =====')

        with open(self.file_path('20150407.zplsc.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(15)

            self.assertEqual(len(result), 15)
            self.assert_particles(result, '20150407.zplsc.yml', RESOURCE_PATH)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST TELEM  =====')

    def test_variable_num_of_channels(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        All test log files usually contain 4 channels with 19 bins each.
        This tests a manually edited log file to exercise the logic for handling a variable
        number of channels and number of bins.
        """
        log.debug('===== START TEST VARIABLE NUM OF CHANNELS =====')

        with open(self.file_path('20150407.zplsc_var_channels.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(15)

            self.assertEqual(len(result), 15)
            self.assert_particles(result, '20150407.zplsc_var_channels.yml', RESOURCE_PATH)
            self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST VARIABLE NUM OF CHANNELS =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        See '20150407.zplsc_corrupt.log' file for line by line details of expected errors.
        """
        log.debug('===== START TEST BAD DATA  =====')

        with open(self.file_path('20150407.zplsc_corrupt.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(100)

            self.assertEqual(len(result), 1)
            self.assertEqual(len(self.exception_callback_value), 6)

            for i in range(len(self.exception_callback_value)):
                log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD DATA  =====')

    def create_large_yml(self):
        """
        Create a large yml file corresponding to an actual recovered dataset.
        This is not an actual test - it allows us to create what we need
        for integration testing, i.e. a yml file.
        """

        with open(self.file_path('20150407.zplsc_var_channels.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            date, name, ext = in_file.name.split('.')
            date = date.split('/')[-1]
            out_file = '.'.join([date, name, 'yml'])
            log.debug(out_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(1000)

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
            for i in range(0, len(particles)):
                particle_dict = particles[i].generate_dict()
                fid.write('  - _index: %d\n' % (i+1))
                # fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
                # fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
                fid.write('    internal_timestamp: %.7f\n' % particle_dict.get('internal_timestamp'))
                for val in particle_dict.get('values'):
                    if val.get('value') is None:
                        fid.write('    %s: %s\n' % (val.get('value_id'), 'Null'))
                    elif isinstance(val.get('value'), float):
                        fid.write('    %s: %2.1f\n' % (val.get('value_id'), val.get('value')))
                    elif isinstance(val.get('value'), str):
                        fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                    else:
                        fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))

    def test_bug_9692(self):
        """
        Test to verify change made works with DCL
        timestamps containing seconds >59
        """

        with open(self.file_path('20150407A.zplsc.log')) as in_file:

            parser = self.create_zplsc_c_dcl_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records(5)

            self.assertEqual(len(result), 3)
            self.assertListEqual(self.exception_callback_value, [])

