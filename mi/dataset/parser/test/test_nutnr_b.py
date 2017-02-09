#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_nutnrb
@file mi/dataset/parser/test/test_nutnr_b.py
@author Mark Worden
@brief Test code for a nutnr_b data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.nutnr_b.resource import RESOURCE_PATH
from mi.dataset.parser.nutnr_b import NutnrBParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


# Add a mixin here if needed
@attr('UNIT', group='mi')
class NutnrBParserUnitTestCase(ParserUnitTestCase):
    """
    NutnrBParser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.nutnr_b_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        }

        self._exceptions_detected = 0

    def exception_callback(self, exception):
        log.debug("Exception received: %s", exception)
        self._exceptions_detected += 1

    def test_happy_path(self):
        """
        Test the happy path of operations where the parser takes the input
        and spits out a valid data particle given the stream.
        """
        log.debug("Running test_happy_path")

        filename = "happy_path.DAT"

        with open(os.path.join(RESOURCE_PATH, filename), 'r') as file_handle:

            self.parser = NutnrBParser(self.config, file_handle, self.exception_callback)

            particles = self.parser.get_records(1000)

            log.debug("Num particles: %d", len(particles))

            self.assertEqual(len(particles), 17)

            self.assert_particles(particles, 'happy_path.yml', RESOURCE_PATH)

            self.assertEqual(self._exceptions_detected, 0)

            log.debug("Exceptions: %d", self._exceptions_detected)

    def test_invalid_fields(self):
        """
        Test the invalid field handling.
        """
        log.debug("Running test_invalid_fields")

        filename = "invalid_fields.DAT"

        with open(os.path.join(RESOURCE_PATH, filename), 'r') as file_handle:

            self.parser = NutnrBParser(self.config, file_handle, self.exception_callback)

            particles = self.parser.get_records(1000)

            log.debug("Num particles: %d", len(particles))

            self.assertEqual(len(particles), 3)

            self.assert_particles(particles, 'invalid_fields.yml', RESOURCE_PATH)

            self.assertEqual(self._exceptions_detected, 31)

            log.debug("Exceptions: %d", self._exceptions_detected)

    def test_missing_metadata(self):
        """
        Test the missing metadata handling.
        """
        log.debug("Running test_missing_metadata")

        filename = "missing_metadata.DAT"

        with open(os.path.join(RESOURCE_PATH, filename), 'r') as file_handle:

            self.parser = NutnrBParser(self.config, file_handle, self.exception_callback)

            particles = self.parser.get_records(1000)

            log.debug("Num particles: %d", len(particles))

            self.assertEqual(len(particles), 9)

            self.assert_particles(particles, 'missing_metadata.yml', RESOURCE_PATH)

            self.assertEqual(self._exceptions_detected, 1)

            log.debug("Exceptions: %d", self._exceptions_detected)

    def test_missing_instrument_data(self):
        """
        Test the missing instrument handling.
        """
        log.debug("Running test_missing_instrument_data")

        filename = "missing_instrument_data.DAT"

        with open(os.path.join(RESOURCE_PATH, filename), 'r') as file_handle:

            self.parser = NutnrBParser(self.config, file_handle, self.exception_callback)

            particles = self.parser.get_records(1000)

            log.debug("Num particles: %d", len(particles))

            self.assertEqual(len(particles), 5)

            self.assert_particles(particles, 'missing_instrument_data.yml', RESOURCE_PATH)

            self.assertEqual(self._exceptions_detected, 4)

            log.debug("Exceptions: %d", self._exceptions_detected)
