#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_dosta_abcdjm_sio
@file mi/dataset/parser/test/test_dosta_abcdjm_sio.py
@author Emily Hahn
@brief An dosta series a,b,c,d,j,m through sio specific dataset agent parser
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.dosta_abcdjm_sio import \
    DostaAbcdjmSioRecoveredDataParticle, \
    DostaAbcdjmSioTelemeteredDataParticle, \
    DostaAbcdjmSioRecoveredMetadataDataParticle, \
    DostaAbcdjmSioTelemeteredMetadataDataParticle, \
    DostaAbcdjmSioParser, \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'dosta_abcdjm', 'sio', 'resource')


@attr('UNIT', group='mi')
class DostaAbcdjmSioParserUnitTestCase(ParserUnitTestCase):

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_telem = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_abcdjm_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: DostaAbcdjmSioTelemeteredMetadataDataParticle,
                DATA_PARTICLE_CLASS_KEY: DostaAbcdjmSioTelemeteredDataParticle
            }
        }

        self.config_recov = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_abcdjm_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: DostaAbcdjmSioRecoveredMetadataDataParticle,
                DATA_PARTICLE_CLASS_KEY: DostaAbcdjmSioRecoveredDataParticle
            }
        }

    def test_simple(self):
        """
        Read test data from the file and pull out telemetered data particles.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.dosta.dat')) as stream_handle:
            parser = DostaAbcdjmSioParser(self.config_telem, stream_handle, self.exception_callback)

            particles = parser.get_records(7)

            self.assert_particles(particles, "dosta_telem_1.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_simple_recovered(self):
        """
        Read test data and pull out recovered data particles
        """
        with open(os.path.join(RESOURCE_PATH, 'DOS15908_1st7.DAT')) as stream_handle:
            parser = DostaAbcdjmSioParser(self.config_recov, stream_handle, self.exception_callback)
            # get the first 4 particles
            particles = parser.get_records(4)
            self.assert_particles(particles, "dosta_recov_1.yml", RESOURCE_PATH)

            # get the next 4 particles (confirming we can break up getting records)
            particles = parser.get_records(4)
            self.assert_particles(particles, "dosta_recov_2.yml", RESOURCE_PATH)

            # confirm no exceptions occurred
            self.assertEqual(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        Read test data and pull out recovered data particles
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_0.dosta.dat')) as stream_handle:
            parser = DostaAbcdjmSioParser(self.config_telem, stream_handle, self.exception_callback)

            # request more particles than available
            particles = parser.get_records(40)
            # confirm we only get requested number
            self.assertEqual(len(particles), 37)

            # confirm no exceptions occurred
            self.assertEqual(self.exception_callback_value, [])

    def test_long_stream_recovered(self):
        """
        Read test data and pull out recovered data particles
        """
        with open(os.path.join(RESOURCE_PATH, 'DOS15908.DAT')) as stream_handle:
            parser = DostaAbcdjmSioParser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(100)

            # confirm we get requested number
            self.assertEqual(len(particles), 97)

            self.assertEqual(self.exception_callback_value, [])

    def test_drain(self):
        """
        This test ensures that we stop parsing chunks when we have completed parsing
        all the records in the input file.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_0.dosta.dat')) as stream_handle:
            parser = DostaAbcdjmSioParser(self.config_telem, stream_handle, self.exception_callback)

            # request more particles than available
            particles = parser.get_records(40)
            # confirm we only get requested number
            self.assertEqual(len(particles), 37)

           # request more particles than available
            particles = parser.get_records(40)
            # confirm we only get requested number
            self.assertEqual(len(particles), 0)

            # confirm no exceptions occurred
            self.assertEqual(self.exception_callback_value, [])

    def test_10603(self):
        """
        Read test data from the file and pull out telemetered data particles.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'node25p1_46.dosta_1237201.dat')) as stream_handle:
            parser = DostaAbcdjmSioParser(self.config_telem, stream_handle, self.exception_callback)

            particles = parser.get_records(700)

            self.assertEqual(len(particles), 45)
            self.assertEqual(self.exception_callback_value, [])

