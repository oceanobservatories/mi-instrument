#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_dosta_ln_wfp
@file marine-integrations/mi/dataset/parser/test/test_dosta_ln_wfp.py
@author Mark Worden
@brief Test code for a dosta_ln_wfp data parser
"""

import os

import numpy
import yaml
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.dosta_ln.wfp_sio.resource import RESOURCE_PATH
from mi.dataset.parser.WFP_E_file_common import HEADER_BYTES, StateKey
from mi.dataset.parser.dosta_ln_wfp import DostaLnWfpParser, WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES, \
    DostaLnWfpInstrumentParserDataParticleKey
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class DostaLnWfpParserUnitTestCase(ParserUnitTestCase):
    """
    dosta_ln_wfp Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Call back method to match what comes in via the exception callback """
        self.exception_callback_value = exception

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_ln_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaLnWfpInstrumentParserDataParticle'
        }
        # Define test data particles and their associated timestamps which will be
        # compared with returned results

        self.start_state = {StateKey.POSITION: 0}

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None

        self.test_particle1 = {}
        self.test_particle1['internal_timestamp'] = 3583638177
        self.test_particle1[StateKey.POSITION] = 204
        self.test_particle1[DostaLnWfpInstrumentParserDataParticleKey.ESTIMATED_OXYGEN_CONCENTRATION] = \
            154.23699951171875
        self.test_particle1[DostaLnWfpInstrumentParserDataParticleKey.OPTODE_TEMPERATURE] = 1.4819999933242798
        self.test_particle1[DostaLnWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649377

        self.test_particle2 = {}
        self.test_particle2['internal_timestamp'] = 3583638247
        self.test_particle2[StateKey.POSITION] = 414
        self.test_particle2[DostaLnWfpInstrumentParserDataParticleKey.ESTIMATED_OXYGEN_CONCENTRATION] = \
            153.7899932861328
        self.test_particle2[DostaLnWfpInstrumentParserDataParticleKey.OPTODE_TEMPERATURE] = 1.4950000047683716
        self.test_particle2[DostaLnWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649447

        self.test_particle3 = {}
        self.test_particle3['internal_timestamp'] = 3583638317
        self.test_particle3[StateKey.POSITION] = 624
        self.test_particle3[DostaLnWfpInstrumentParserDataParticleKey.ESTIMATED_OXYGEN_CONCENTRATION] = \
            153.41099548339844
        self.test_particle3[DostaLnWfpInstrumentParserDataParticleKey.OPTODE_TEMPERATURE] = 1.5
        self.test_particle3[DostaLnWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649517

        self.test_particle4 = {}
        self.test_particle4['internal_timestamp'] = 3583638617
        self.test_particle4[StateKey.POSITION] = 1524
        self.test_particle4[DostaLnWfpInstrumentParserDataParticleKey.ESTIMATED_OXYGEN_CONCENTRATION] = \
            152.13600158691406
        self.test_particle4[DostaLnWfpInstrumentParserDataParticleKey.OPTODE_TEMPERATURE] = 1.5019999742507935
        self.test_particle4[DostaLnWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649817


    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """

        file_path = os.path.join(RESOURCE_PATH, 'small.DAT')
        self.stream_handle = open(file_path, 'rb')

        self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                     self.state_callback, self.pub_callback, self.exception_callback)

        particles = self.parser.get_records(6)

        # Make sure the fifth particle has the correct values
        self.assert_result(self.test_particle1, particles[5])

        test_data = self.get_dict_from_yml('good.yml')

        for i in range(0,6):
            self.assert_result(test_data['data'][i], particles[i])

        self.stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        self.stream_handle = open(file_path, 'rb')

        self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                     self.state_callback, self.pub_callback, self.exception_callback)

        particles = self.parser.get_records(20)

        # Should end up with 20 particles
        self.assertTrue(len(particles) == 20)

        self.assert_result(self.test_particle3, particles[19])

        particles = self.parser.get_records(30)

        # Should end up with 30 particles
        self.assertTrue(len(particles) == 30)

        self.assert_result(self.test_particle4, particles[29])

        self.stream_handle.close()

    def test_verify_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        self.stream_handle = open(file_path, 'rb')

        self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                     self.state_callback, self.pub_callback, self.exception_callback)

        # In a single read, get all particles in this file.
        result = self.parser.get_records(1000)

        self.assert_particles(result, 'E0000001.yml', RESOURCE_PATH)

        self.stream_handle.close()

    def create_large_yml(self):
        """
        Create a large yml file corresponding to an actual recovered dataset. This is not an actual test - it allows
        us to create what we need for integration testing, i.e. a yml file.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        self.stream_handle = open(file_path, 'rb')

        self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                     self.state_callback, self.pub_callback, self.exception_callback)

        # In a single read, get all particles in this file.
        result = self.parser.get_records(1000)

        self.particle_to_yml(result, 'E0000001.yml')


    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)
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

    def test_long_stream(self):
        """
        Test a long stream
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000002.DAT')
        self.stream_handle = open(file_path, 'rb')

        self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                       self.state_callback, self.pub_callback, self.exception_callback)

        particles = self.parser.get_records(1000)

        # Should end up with 683 particles
        self.assertTrue(len(particles) == 683)

        self.stream_handle.close()

    def test_mid_state_start(self):
        """
        Test starting the parser in a state in the middle of processing
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        self.stream_handle = open(file_path, 'rb')

        # Moving the file position past the header and two records
        new_state = {StateKey.POSITION: HEADER_BYTES+(WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES*2)}

        self.parser = DostaLnWfpParser(self.config, new_state, self.stream_handle,
                                       self.state_callback, self.pub_callback, self.exception_callback)

        particles = self.parser.get_records(4)

        # Should end up with 4 particles
        self.assertTrue(len(particles) == 4)

        self.assert_result(self.test_particle1, particles[3])

        self.stream_handle.close()

    def test_set_state(self):
        """
        Test changing to a new state after initializing the parser and
        reading data, as if new data has been found and the state has
        changed
        """
        filepath = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        self.stream_handle = open(filepath, 'rb')

        # Moving the file position past the header and two records
        new_state = {StateKey.POSITION: HEADER_BYTES+(WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES*2)}

        self.parser = DostaLnWfpParser(self.config, new_state, self.stream_handle,
                                       self.state_callback, self.pub_callback, self.exception_callback)

        particles = self.parser.get_records(4)

        # Should end up with 4 particles
        self.assertTrue(len(particles) == 4)

        self.assert_result(self.test_particle1, particles[3])

        # Moving the file position past the header and three records
        new_state = {StateKey.POSITION: HEADER_BYTES+(WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES*3)}

        self.parser = DostaLnWfpParser(self.config, new_state, self.stream_handle,
                                       self.state_callback, self.pub_callback, self.exception_callback)

        particles = self.parser.get_records(10)

        # Should end up with 10 particles
        self.assertTrue(len(particles) == 10)

        self.assert_result(self.test_particle2, particles[9])

        self.stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-DATA.DAT')
        self.stream_handle = open(file_path, 'rb')

        self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                       self.state_callback, self.pub_callback, self.exception_callback)


        with self.assertRaises(SampleException):
             self.parser.get_records(1)

        self.stream_handle.close()

    def test_bad_header(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # This case tests against a header that does not match
        # 0000 0000 0000 0100 0000 0000 0000 0151
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-HEADER1.DAT')
        self.stream_handle = open(file_path, 'rb')

        with self.assertRaises(SampleException):
            self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                           self.state_callback, self.pub_callback, self.exception_callback)

        self.stream_handle.close()

        # This case tests against a header that does not match global, but matches coastal
        # 0001 0000 0000 0000 0001 0001 0000 0000
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-HEADER2.DAT')
        self.stream_handle = open(file_path, 'rb')

        with self.assertRaises(SampleException):
            self.parser = DostaLnWfpParser(self.config, self.start_state, self.stream_handle,
                                           self.state_callback, self.pub_callback, self.exception_callback)

        self.stream_handle.close()

    def assert_result(self, test, particle):
        """
        Suite of tests to run against each returned particle and expected
        results of the same.  The test parameter should be a dictionary
        that contains the keys to be tested in the particle
        the 'internal_timestamp' and 'position' keys are
        treated differently than others but can be verified if supplied
        """

        particle_dict = particle.generate_dict()

        #for efficiency turn the particle values list of dictionaries into a dictionary
        particle_values = {}
        for param in particle_dict.get('values'):
            particle_values[param['value_id']] = param['value']

        # compare each key in the test to the data in the particle
        for key in test:
            test_data = test[key]

            #get the correct data to compare to the test
            if key == 'internal_timestamp':
                particle_data = particle.get_value('internal_timestamp')
                #the timestamp is in the header part of the particle

            elif key == 'position':
                particle_data = self.state_callback_value['position']
                #position corresponds to the position in the file

            else:
                particle_data = particle_values.get(key)
                #others are all part of the parsed values part of the particle

            if particle_data is None:
                #generally OK to ignore index keys in the test data, verify others

                log.warning("\nWarning: assert_result ignoring test key %s, does not exist in particle", key)
            else:
                if isinstance(test_data, float):
                    # slightly different test for these values as they are floats.
                    compare = numpy.abs(test_data - particle_data) <= 1e-5
                    self.assertTrue(compare)
                else:
                    # otherwise they are all ints and should be exactly equal
                    self.assertEqual(test_data, particle_data)

    def get_dict_from_yml(self, filename):
        """
        This utility routine loads the contents of a yml file
        into a dictionary
        """

        fid = open(os.path.join(RESOURCE_PATH, filename), 'r')
        result = yaml.load(fid)
        fid.close()

        return result
