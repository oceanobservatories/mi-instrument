import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.driver.suna.resource import RESOURCE_PATH
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.suna import SunaParser
from mi.dataset.driver.suna.suna_driver import MODULE_NAME, RECOVERED_PARTICLE_CLASS, \
    INSTRUMENT_RECOVERED_PARTICLE_CLASS

log = get_logger()


@attr('UNIT', group='mi')
class SunaDclParserUnitTestCase(ParserUnitTestCase):

    def create_parser(self, particle_class, file_handle):
        """
        This function creates a MetbkADcl parser for recovered data.
        """
        parser = SunaParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            file_handle,
            self.exception_callback)
        return parser

    @staticmethod
    def open_file(filename):
        my_file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return my_file

    @staticmethod
    def create_yml(particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    def assert_particles_processed(self, filename, particle_class, num_particles):
        """
        Common function to test particles
        """
        with open(os.path.join(RESOURCE_PATH, filename), 'r') as file_handle:
            parser = self.create_parser(particle_class, file_handle)
            particles = parser.get_records(num_particles + 10)
            log.debug("Num particles: %d", len(particles))

            self.assertEqual(len(particles), num_particles)

    def test_good_dcl(self):
        """
        Test dcl with good data
        """
        self.assert_particles_processed('dcl_good.log', RECOVERED_PARTICLE_CLASS, 144)

    def test_bad_dcl(self):
        """
        Test dcl with some good data, and some bad data (4 of them)
        """
        self.assert_particles_processed('dcl_bad.log', RECOVERED_PARTICLE_CLASS, 140)

    def test_good_instrument_recovered(self):
        """
        Test instrument recovered with some good data
        """
        self.assert_particles_processed('instrument_recovered_good.CSV', INSTRUMENT_RECOVERED_PARTICLE_CLASS, 39)

    def test_bad_instrument_recovered(self):
        """
        Test instrument recovered with some bad data (
        """
        self.assert_particles_processed('instrument_recovered_bad.CSV', INSTRUMENT_RECOVERED_PARTICLE_CLASS, 35)
