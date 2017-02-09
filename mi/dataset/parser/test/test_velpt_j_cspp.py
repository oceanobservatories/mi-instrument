
"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_velpt_j_cspp.py
@author Jeremy Amundson
@brief Test code for a velpt_j_cspp data parser

Notes on test data:
11079364_PPB_ADCP.txt and 11079364_PPD_ADCP.txt are taken from the IDD
as examples of recovered and telemetered data respectively. Other files
are modified versions of 11079364_PPB_ADCP.
"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.cspp_base import METADATA_PARTICLE_CLASS_KEY, DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.velpt_j_cspp import VelptJCsppParser
from mi.dataset.parser.velpt_j_cspp import VelptJCsppMetadataRecoveredDataParticle, \
    VelptJCsppInstrumentRecoveredDataParticle, VelptJCsppMetadataTelemeteredDataParticle, \
    VelptJCsppInstrumentTelemeteredDataParticle

log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'velpt_j', 'cspp', 'resource')


@attr('UNIT', group='mi')
class VelptJCsppParserUnitTestCase(ParserUnitTestCase):
    """
    velpt_j_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_recov = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: VelptJCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: VelptJCsppInstrumentRecoveredDataParticle,
            }
        }

        self.config_telem = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: VelptJCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: VelptJCsppInstrumentTelemeteredDataParticle,
            }
        }

    def test_simple(self):
        """
        retrieves and verifies the first 6 particles
        """
        with open(os.path.join(RESOURCE_PATH, 'short_PPB_ADCP.txt'), 'rU') as stream_handle:

            parser = VelptJCsppParser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(7)

            # check that there are no more particles in file
            particles2 = parser.get_records(3)
            self.assertEquals(len(particles2), 0)

            self.assert_particles(particles, 'short_PPB_ADCP.yml', RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

    def test_get_many(self):
        """
        get 10 particles 3 times, verify length and results
        """
        with open(os.path.join(RESOURCE_PATH, '11079364_PPD_ADCP.txt'), 'rU') as stream_handle:

            parser = VelptJCsppParser(self.config_telem, stream_handle, self.exception_callback)

            particles = parser.get_records(10)
            particles2 = parser.get_records(10)
            particles.extend(particles2)
            # request past the end of the file, should only be 4 remaining records
            particles3 = parser.get_records(10)
            self.assertEquals(len(particles3), 4)
            particles.extend(particles3)

            self.assert_particles(particles, '11079364_PPD_ADCP.yml', RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        retrieve all of particles, verify the expected number, confirm results
        """
        with open(os.path.join(RESOURCE_PATH, '11079364_PPB_ADCP.txt'), 'rU') as stream_handle:

            parser = VelptJCsppParser(self.config_recov, stream_handle, self.exception_callback)

            # request more particles than are available in the file
            particles = parser.get_records(1000)

            # confirm we get the number in the file
            self.assertTrue(len(particles) == 231)

            self.assert_particles(particles, '11079364_PPB_ADCP.yml', RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

    def test_bad_recov(self):
        """
        Ensure that bad data is skipped when it exists. A variety of malformed
        records are used in order to verify this
        """

        with open(os.path.join(RESOURCE_PATH, 'BAD_PPB_ADCP.txt'), 'rU') as stream_handle:

            parser = VelptJCsppParser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(5)
            self.assertTrue(len(particles) == 4)

            self.assert_particles(particles, 'rec_BAD_ADCP.yml', RESOURCE_PATH)

            n_exceptions = 13
            # file issues by line:
            #     depth float missing values after decimal
            #     no timestamp
            #     invalid char in yes or no suspect timestamp ('w')
            #     bad format float sound speed
            #     bad format float roll
            #     space instead of tab separator
            #     B1 vel bad format float
            #     B2 vel bad format float
            #     missing B3 vel
            #     two data lines run together, missing end of first
            #     int instead of float in roll
            #     int instead of float in pressure
            #     extra spaces
            #     4 okay lines

            self.assertEquals(len(self.exception_callback_value), n_exceptions)
            for exception in self.exception_callback_value:
                self.assertIsInstance(exception, RecoverableSampleException)

    def test_missing_header(self):
        """
        Test with a file missing the entire header, should still make all the particles except metadata and
        throw an exception through the callback
        """
        with open(os.path.join(RESOURCE_PATH, 'missing_header_PPB_ADCP.txt'), 'rU') as stream_handle:

            parser = VelptJCsppParser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(7)

            # check that there are no more particles in file
            particles2 = parser.get_records(3)
            self.assertEquals(len(particles2), 0)

            self.assert_particles(particles, 'missing_header_PPB_ADCP.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    def test_partial_header(self):
        """
        Test with a file missing part of the header, should still make all the particles
        """
        with open(os.path.join(RESOURCE_PATH, 'partial_header_PPB_ADCP.txt'), 'rU') as stream_handle:

            parser = VelptJCsppParser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(7)

            # check that there are no more particles in file
            particles2 = parser.get_records(3)
            self.assertEquals(len(particles2), 0)

            self.assert_particles(particles, 'partial_header_PPB_ADCP.yml', RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])
