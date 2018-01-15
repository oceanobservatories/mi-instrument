#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_adcpa_n.py
@author Jeff Roy
@brief Test code for a adcpa_n data parser

"""
import copy
import os

import yaml
from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.adcpa_n.resource import RESOURCE_PATH
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.parser.utilities import particle_to_yml

log = get_logger()


@attr('UNIT', group='mi')
class AdcpNParserUnitTestCase(ParserUnitTestCase):
    """
    AdcpNParser unit test suite
    """
    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpa_n',
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'velocity': 'VelocityInst',
                'engineering': 'AuvEngineering',
                'config': 'AuvConfig',
                'bottom_track': 'InstBottom',
                'bottom_track_config': 'BottomConfig',
            }
        }

    def test_simple(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp_auv_3.pd0'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(13)

            log.debug('got back %d particles', len(particles))

            # Note yaml file was generated but hand checked against output of
            # vendor supplied MATLAB tool outputs
            self.assert_particles(particles, 'adcp_auv_3.yml', RESOURCE_PATH)
            self.assertEqual(len(self.exception_callback_value), 0)

    def test_get_many(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp_auv_51.pd0'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(178)

            log.debug('got back %d particles', len(particles))

            # Note yaml file was generated but hand checked against output of
            # vendor supplied MATLAB tool outputs
            self.assert_particles(particles, 'adcp_auv_51.yml', RESOURCE_PATH)
            self.assertEqual(len(self.exception_callback_value), 0)

    def test_long_stream(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp.adc'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(20000)  # ask for 20000 should get 13294

            log.debug('got back %d particles', len(particles))
            self.assertEqual(len(particles), 13294)
            self.assertEqual(self.exception_callback_value, [])
