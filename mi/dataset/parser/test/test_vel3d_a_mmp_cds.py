#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_vel3d_a_mmp_cds
@file marine-integrations/mi/dataset/parser/test/test_vel3d_a_mmp_cds.py
@author Jeremy Amundson
@brief Test code for a vel3d_a_mmp_cds data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.vel3d_a.mmp_cds.resource import RESOURCE_PATH
from mi.dataset.parser.mmp_cds_base import MmpCdsParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class Vel3dAMmpCdsParserUnitTestCase(ParserUnitTestCase):
    """
    acmpf_ckl_mmp_cds Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_a_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'Vel3dAMmpCdsParserDataParticle'
        }

    def test_simple(self):
        """
        This test reads in a small number of particles and verifies the result of one of the particles.
        """

        with open(os.path.join(RESOURCE_PATH, 'first_data.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(4)

            self.assert_particles(particles, 'first_four.yml', RESOURCE_PATH)

    def test_get_many(self):
        """
        This test exercises retrieving 20 particles, verifying the particles, then retrieves 30 particles
         and verifies the 30 particles.
        """

        with open(os.path.join(RESOURCE_PATH, 'first_data.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(193)

            # Should end up with 20 particles
            self.assertTrue(len(particles) == 193)

            self.assert_particles(particles, 'first_data.yml', RESOURCE_PATH)

    def test_long_stream(self):
        """
        This test exercises retrieve approximately 200 particles.
        """

        # Using two concatenated msgpack files to simulate two chunks to get more particles.
        with open(os.path.join(RESOURCE_PATH, 'acm_concat.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 400 particles, but we will retrieve less
            particles = parser.get_records(400)

            # Should end up with 386 particles
            self.assertTrue(len(particles) == 386)

    def test_bad_data_one(self):
        """
        This test verifies that a SampleException is raised when msgpack data is malformed.
        """

        with open(os.path.join(RESOURCE_PATH, 'acm_1_20131124T005004_458-BAD.mpk'), 'rb') as stream_handle:

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

