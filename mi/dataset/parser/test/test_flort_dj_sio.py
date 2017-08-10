#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_flort_dj_sio.py
@author Emily Hahn, Joe Padula (telemetered)
@brief Test code for a flort_dj_sio data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import UnexpectedDataException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.flort_dj.sio.resource import RESOURCE_PATH
from mi.dataset.parser.flort_dj_sio import FlortDjSioParser, \
    FlortdRecoveredParserDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class FlortDjSioParserUnitTestCase(ParserUnitTestCase):

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.telem_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flort_dj_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortdParserDataParticle'
        }

        self.recov_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flort_dj_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortdRecoveredParserDataParticle'
        }

        # particles from FLO15908.DAT and FLO_short.DAT
        self.particle_a_recov = FlortdRecoveredParserDataParticle(
            '51EC760117/12/13\t00:00:05\t700\t4130\t695\t700\t460\t4130\t547')
        self.particle_b_recov = FlortdRecoveredParserDataParticle(
            '51EC798517/12/13\t00:15:04\t700\t4130\t695\t708\t460\t4130\t548')
        self.particle_c_recov = FlortdRecoveredParserDataParticle(
            '51EC7D0917/12/13\t00:30:04\t700\t4130\t695\t702\t460\t4130\t548')
        self.particle_d_recov = FlortdRecoveredParserDataParticle(
            '51EC808D17/12/13\t00:45:04\t700\t4130\t695\t710\t460\t4130\t548')
        self.particle_e_recov = FlortdRecoveredParserDataParticle(
            '51EC841117/12/13\t01:00:04\t700\t4130\t695\t708\t460\t4130\t548')
        self.particle_f_recov = FlortdRecoveredParserDataParticle(
            '51EC879517/12/13\t01:15:04\t700\t4130\t695\t700\t460\t4130\t548')

        # particles from FLO15908.DAT
        self.particle_long_before_last = FlortdRecoveredParserDataParticle(
            '51EDC07917/12/13\t23:30:05\t700\t4130\t695\t677\t460\t4130\t545')
        self.particle_long_last = FlortdRecoveredParserDataParticle(
            '51EDC3FD17/12/13\t23:45:05\t700\t4130\t695\t674\t460\t4130\t545')

        self.stream_handle = None

    def assert_result(self, result, particle):
        self.assertEqual(result, [particle])

    def build_telem_parser(self):
        """
        Build a telemetered parser, storing it in self.parser
        """
        if self.stream_handle is None:
            self.fail("Must set stream handle before building telemetered parser")
        self.parser = FlortDjSioParser(self.telem_config, self.stream_handle,
                                       self.exception_callback)

    def build_recov_parser(self):
        """
        Build a telemetered parser, storing it in self.parser
        This requires stream handle to be set before calling it
        """
        if self.stream_handle is None:
            self.fail("Must set stream handle before building recovered parser")
        self.parser = FlortDjSioParser(self.recov_config, self.stream_handle,
                                       self.exception_callback)

    def test_simple_recov(self):
        """
        Test that we can pull out data particles one at a time from for a recovered
        parser and file.
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'FLO_short.DAT'))
        self.build_recov_parser()

        # get all 6 records in this file one at a time, comparing the state and particle
        result = self.parser.get_records(1)
        self.assert_result(result, self.particle_a_recov)
        result = self.parser.get_records(1)
        self.assert_result(result, self.particle_b_recov)
        result = self.parser.get_records(1)
        self.assert_result(result, self.particle_c_recov)
        result = self.parser.get_records(1)
        self.assert_result(result, self.particle_d_recov)
        result = self.parser.get_records(1)
        self.assert_result(result, self.particle_e_recov)
        result = self.parser.get_records(1)
        self.assert_result(result, self.particle_f_recov)

        # make sure there are no more records
        result = self.parser.get_records(1)
        self.assertEqual(result, [])

        # make sure there were no exceptions
        self.assertEqual(self.exception_callback_value, [])

        self.stream_handle.close()

    def test_get_many(self):
        """
        Read test data from the file and pull out multiple data particles a few a time.
        Assert that the results are those we expected.
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node59p1_0.flort.dat'))
        self.build_telem_parser()

        # get 18 total
        result = self.parser.get_records(3)
        result.extend(self.parser.get_records(10))
        result.extend(self.parser.get_records(5))

        self.stream_handle.close()
        self.assert_particles(result, "node59p1_0.flort.yml", RESOURCE_PATH)

        # make sure there were no exceptions
        self.assertEqual(self.exception_callback_value, [])

    def test_get_many_recov(self):
        """
        Read recovered test data from the file and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'FLO_short.DAT'))
        self.build_recov_parser()

        # get all 6 records
        result = self.parser.get_records(6)
        # compare returned particles
        self.assertEqual(result,
                         [self.particle_a_recov,
                          self.particle_b_recov,
                          self.particle_c_recov,
                          self.particle_d_recov,
                          self.particle_e_recov,
                          self.particle_f_recov])

        # make sure there were no exceptions
        self.assertEqual(self.exception_callback_value, [])

    def test_dash(self):
        """
        Test that the particle with a field replaced by dashes is found
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node59p1_0_dash.flort.dat'))
        self.build_telem_parser()

        result = self.parser.get_records(18)
        self.assert_particles(result, "node59p1_0_dash.flort.yml", RESOURCE_PATH)

        # make sure there were no exceptions
        self.assertEqual(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        Read test data and pull out telemetered data particles and compare against yml
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'node59p1_0.flort.dat'))
        self.build_telem_parser()

        particles = self.parser.get_records(18)
        self.assert_particles(particles, "node59p1_0.flort.yml", RESOURCE_PATH)

        # confirm no exceptions occurred
        self.assertEqual(self.exception_callback_value, [])

    def test_long_stream_recov(self):
        """
        test that a longer file can be read and compare the end particles
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'FLO15908.DAT'))
        self.build_recov_parser()

        result = self.parser.get_records(96)
        for particle in result:
            log.debug(particle.generate())

        # compare returned particles at the start of the file
        self.assertEqual(result[0], self.particle_a_recov)
        self.assertEqual(result[1], self.particle_b_recov)
        self.assertEqual(result[2], self.particle_c_recov)
        # compare returned particles at the end of the file
        self.assertEqual(result[-2], self.particle_long_before_last)
        self.assertEqual(result[-1], self.particle_long_last)

        # make sure there were no exceptions
        self.assertEqual(self.exception_callback_value, [])

    def test_against_yml_recov(self):
        """
        Read test data and pull out recovered data particles and compare against yml
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'FLO15908.DAT'))
        self.build_recov_parser()

        # get 20 particles
        particles = self.parser.get_records(96)
        self.assert_particles(particles, "FLO15908.yml", RESOURCE_PATH)

        # confirm no exceptions occurred
        self.assertEqual(self.exception_callback_value, [])

    def test_bad_header(self):
        """
        The file used in this test has a header with 'D0' instead of 'FL' in the first record.
        (A dosta_abcdjm_sio record was copied in for the test.)
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST BAD HEADER =====')

        num_particles_to_request = 6
        num_expected_particles = 5

        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'FLO_bad_header.DAT'))
        self.build_recov_parser()

        particles = self.parser.get_records(num_particles_to_request)

        self.assertEquals(len(particles), num_expected_particles)

        self.assert_particles(particles, "flo_bad_header.yml", RESOURCE_PATH)

        log.debug('Exceptions : %s', self.exception_callback_value)

        self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

        log.debug('===== END TEST BAD HEADER =====')

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
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write('    %s: \'%s\'\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'FLO15908.DAT'))
        self.build_recov_parser()
        particles = self.parser.get_records(96)

        self.particle_to_yml(particles, 'FLO15908.yml')
        self.stream_handle.close()
