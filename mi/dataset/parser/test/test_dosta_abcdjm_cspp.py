
"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_dosta_abcdjm_cspp.py
@author Mark Worden
@brief Test code for a dosta_abcdjm_cspp data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.core.exceptions import RecoverableSampleException, SampleEncodingException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.cspp_base import METADATA_PARTICLE_CLASS_KEY, DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.dosta_abcdjm_cspp import DostaAbcdjmCsppParser
from mi.dataset.parser.dosta_abcdjm_cspp import DostaAbcdjmCsppMetadataRecoveredDataParticle, \
    DostaAbcdjmCsppInstrumentRecoveredDataParticle, DostaAbcdjmCsppMetadataTelemeteredDataParticle, \
    DostaAbcdjmCsppInstrumentTelemeteredDataParticle

log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'dosta_abcdjm', 'cspp', 'resource')


@attr('UNIT', group='mi')
class DostaAbcdjmCsppParserUnitTestCase(ParserUnitTestCase):
    """
    dosta_abcdjm_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_recovered = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_abcdjm_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: DostaAbcdjmCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: DostaAbcdjmCsppInstrumentRecoveredDataParticle,
            }
        }
        self.config_telemetered = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_abcdjm_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: DostaAbcdjmCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: DostaAbcdjmCsppInstrumentTelemeteredDataParticle,
            }
        }

    def test_simple_telem(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, '11194982_PPD_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_telemetered, stream_handle, self.exception_callback)

            # Attempt to retrieve 20 particles, there are only 18 in the file though
            particles = parser.get_records(20)

            # Should end up with 18 particles
            self.assertTrue(len(particles) == 18)

            self.assert_particles(particles, '11194982_PPD_OPT.yml', RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

    def test_simple_recov(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, '11079894_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            # Attempt to retrieve 20 particles, there are more in this file but only verify 20
            particles = parser.get_records(20)

            # Should end up with 20 particles
            self.assertTrue(len(particles) == 20)

            self.assert_particles(particles, '11079894_PPB_OPT.yml', RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

    def test_get_many(self):
        """
        Read test data and pull out data particles in smaller groups.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, '11079419_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            # Attempt to retrieve 20 total particles
            particles = parser.get_records(5)
            particles2 = parser.get_records(10)
            particles.extend(particles2)
            particles3 = parser.get_records(5)
            particles.extend(particles3)

            # Should end up with 20 particles
            self.assertTrue(len(particles) == 20)

            self.assert_particles(particles, '11079419_PPB_OPT.yml', RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        Test a long stream 
        """
        with open(os.path.join(RESOURCE_PATH, '11079364_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            # Let's attempt to retrieve more particles than are available
            particles = parser.get_records(300)

            # Should end up with 272 particles
            self.assertTrue(len(particles) == 272)

            self.assertEquals(self.exception_callback_value, [])

    def test_bad_data_record(self):
        """
        Ensure that bad data creates a recoverable sample exception and parsing continues
        """

        with open(os.path.join(RESOURCE_PATH, 'BadDataRecord_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            particles = parser.get_records(19)

            self.assert_particles(particles, 'BadDataRecord_PPB_OPT.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    def test_bad_header_source_file_name(self):
        """
        Ensure that bad source file name produces an error
        """

        with open(os.path.join(RESOURCE_PATH, 'BadHeaderSourceFileName_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            parser.get_records(1)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], SampleEncodingException)

    def test_bad_header_start_date(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        with open(os.path.join(RESOURCE_PATH, 'BadHeaderProcessedData_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            # parser should return metadata without start date filled in
            parser.get_records(1)
            self.assertEqual(self.exception_callback_value, [])

    def test_linux_source_path_handling(self):
        """
        Read linux source path test data and assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'linux_11079894_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            particles = parser.get_records(20)

            self.assertTrue(len(particles) == 20)

            self.assert_particles(particles, 'linux.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_air_saturation_preset(self):
        """
        Ensure that input files containing the air saturation field are parsed correctly.
        Redmine #10238 Identified additional parameter enabled after first deployment
        """
        with open(os.path.join(RESOURCE_PATH, 'ucspp_32260420_PPB_OPT.txt'), 'rU') as stream_handle:

            parser = DostaAbcdjmCsppParser(self.config_recovered, stream_handle, self.exception_callback)

            # get the metadata particle and first 2 instrument particles and verify values.
            particles = parser.get_records(3)

            self.assertTrue(len(particles) == 3)
            self.assertEqual(self.exception_callback_value, [])

            self.assert_particles(particles, 'ucspp_32260420_PPB_OPT.yml', RESOURCE_PATH)

            # get remaining particles and verify parsed without error.
            particles = parser.get_records(100)

            self.assertTrue(len(particles) == 93)
            self.assertEqual(self.exception_callback_value, [])
