#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_ctdpf_ckl_mmp_cds
@file marine-integrations/mi/dataset/parser/test/test_ctdpf_ckl_mmp_cds.py
@author Mark Worden
@brief Test code for a ctdpf_ckl_mmp_cds data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.ctdpf_ckl.mmp_cds.resource import RESOURCE_PATH
from mi.dataset.parser.mmp_cds_base import MmpCdsParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MSGPACK_TEST_FILE_LENGTH = 5278

# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser


@attr('UNIT', group='mi')
class CtdpfCklMmpCdsParserUnitTestCase(ParserUnitTestCase):
    """
    ctdpf_ckl_mmp_cds Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdpfCklMmpCdsParserDataParticle'
        }

    def test_simple(self):
        """
        This test reads in a small number of particles and verifies the result of one of the particles.
        """

        with open(os.path.join(RESOURCE_PATH, 'ctd_1_20131124T005004_458.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            # parser.get_records(4) # throw first 4 particles away, yml file has 5th particle
            particles = parser.get_records(6)

            # the yml file only has particle 5 in it
            self.assert_particles(particles[5:6], 'good.yml', RESOURCE_PATH)

    def test_get_many(self):
        """
        This test exercises retrieving 20 particles, verifying the 20th particle, then retrieves 30 particles
         and verifies the 30th particle.
        """

        with open(os.path.join(RESOURCE_PATH, 'ctd_1_20131124T005004_458.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(20)

            # Should end up with 20 particles
            self.assertTrue(len(particles) == 20)

            # the yml file only has particle 19 in it
            self.assert_particles(particles[19:20], 'get_many_one.yml', RESOURCE_PATH)

            particles = parser.get_records(30)

            # Should end up with 30 particles
            self.assertTrue(len(particles) == 30)

            # the yml file only has particle 29 in it
            self.assert_particles(particles[29:30], 'get_many_two.yml', RESOURCE_PATH)

    def test_long_stream(self):
        """
        This test exercises retrieve approximately 200 particles.
        """

        # Using two concatenated msgpack files to simulate two chunks to get more particles.
        with open(os.path.join(RESOURCE_PATH, 'ctd_concat.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 200 particles, but we will retrieve less
            particles = parser.get_records(200)

            # Should end up with 172 particles
            self.assertEqual(len(particles), 172)

    def test_bad_data_one(self):
        """
        This test verifies that a SampleException is raised when msgpack data is malformed.
        """

        with open(os.path.join(RESOURCE_PATH, 'ctd_1_20131124T005004_BAD.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            parser.get_records(1)

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

    def test_bad_data_two(self):
        """
        This test verifies that a SampleException is raised when an entire msgpack buffer is not msgpack.
        """

        with open(os.path.join(RESOURCE_PATH, 'not-msg-pack.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            parser.get_records(1)
            self.assertTrue(len(self.exception_callback_value) >= 1)
            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

