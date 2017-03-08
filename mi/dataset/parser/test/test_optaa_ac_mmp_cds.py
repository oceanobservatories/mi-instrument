#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_optaa_ac_mmp_cds
@file marine-integrations/mi/dataset/parser/test/test_optaa_ac_mmp_cds.py
@author Mark Worden
@brief Test code for a optaa_ac_mmp_cds data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.optaa_ac.mmp_cds.resource import RESOURCE_PATH
from mi.dataset.parser.mmp_cds_base import MmpCdsParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class OptaaAcMmpCdsParserUnitTestCase(ParserUnitTestCase):
    """
    optaa_ac_mmp_cds Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.optaa_ac_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'OptaaAcMmpCdsParserDataParticle'
        }

    def test_simple(self):
        """
        This test reads in a small number of particles and verifies the result of one of the particles.
        """

        with open(os.path.join(RESOURCE_PATH, 'simple.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(1)

            self.assert_particles(particles, 'simple.yml', RESOURCE_PATH)

    def test_get_many(self):
        """
        This test exercises retrieving 20 particles, verifying the 20th particle, then retrieves 30 particles
         and verifies the 30th particle.
        """

        with open(os.path.join(RESOURCE_PATH, 'get_many.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(50)

            # Should end up with 50 particles
            self.assertTrue(len(particles) == 50)

            self.assert_particles(particles, 'get_many.yml', RESOURCE_PATH)

    def test_long_stream(self):
        """
        This test exercises retrieve approximately 200 particles.
        """

        with open(os.path.join(RESOURCE_PATH, 'large_import.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 1000 particles
            particles = parser.get_records(1000)

            # Should end up with 1000 particles
            self.assertTrue(len(particles) == 1000)

            self.assert_particles(particles, 'large_import.yml', RESOURCE_PATH)

    def test_bad_data_one(self):
        """
        This test verifies that a SampleException is raised when msgpack data is malformed.
        """

        with open(os.path.join(RESOURCE_PATH, 'acs_archive.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(100)

            self.assertTrue(len(particles) == 40)

        with open(os.path.join(RESOURCE_PATH, 'acs_archive_BAD.mpk'), 'rb') as stream_handle:

            parser = MmpCdsParser(self.config, stream_handle, self.exception_callback)

            parser.get_records(1)

            self.assertTrue(len(self.exception_callback_value) >= 1)
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
