"""
@package mi.dataset.parser.test.test_optaa_dj_cspp
@file marine-integrations/mi/dataset/parser/test/test_optaa_dj_cspp.py
@author Joe Padula
@brief Test code for a optaa_dj_cspp data parser

Notes on test data:

11079364_ACS_ACS.txt and 11079364_ACD_ACS.txt are taken from the IDD
as examples of recovered and telemetered data respectively.
11079419_ACS_ACS.txt is taken from the recovered zip attached to the IDD.
Other files
are modified versions of 11079364_ACS_ACS.txt.
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY

from mi.dataset.parser.optaa_dj_cspp import \
    OptaaDjCsppParser, \
    OptaaDjCsppInstrumentTelemeteredDataParticle, \
    OptaaDjCsppMetadataTelemeteredDataParticle, \
    OptaaDjCsppInstrumentRecoveredDataParticle, \
    OptaaDjCsppMetadataRecoveredDataParticle

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH, ParserUnitTestCase

log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH,
                             'optaa_dj', 'cspp', 'resource')

RECOVERED_SAMPLE_DATA = '11079364_ACS_ACS.txt'
TELEMETERED_SAMPLE_DATA = '11079364_ACD_ACS.txt'

O_MODE = 'rU'


@attr('UNIT', group='mi')
class OptaaDjCsppParserUnitTestCase(ParserUnitTestCase):
    """
    optaa_dj_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._telemetered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.optaa_dj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: OptaaDjCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: OptaaDjCsppInstrumentTelemeteredDataParticle
            }
        }

        self._recovered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.optaa_dj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: OptaaDjCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: OptaaDjCsppInstrumentRecoveredDataParticle
            }
        }

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write(" particle_object: 'MULTIPLE'\n")
        fid.write(" particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write(' - _index: %d\n' % (i+1))

            fid.write('   particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('   particle_type: %s\n' % particle_dict.get('stream_name'))

            fid.write('   internal_timestamp: %16.3f\n' %
                      particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('   %s: %16.3f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("   %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('   %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """
        fid = open(os.path.join(RESOURCE_PATH,
                                "11079419_ACS_ACS.txt"),
                   O_MODE)

        stream_handle = fid
        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(1044)

        self.particle_to_yml(particles, '11079419_ACS_ACS.yml')
        fid.close()

    def test_simple(self):
        """
        Read test data and pull out the first particle (metadata).
        Assert that the results are those we expected.
        """
        log.debug('===== START TEST HAPPY PATH SINGLE =====')

        file_path = os.path.join(RESOURCE_PATH, '11079364_ACS_ACS_one_data_record.txt')

        stream_handle = open(file_path, O_MODE)

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(1)

        log.debug("*** test_simple Num particles %s", len(particles))

        # check the values against expected results.
        self.assert_particles(particles, '11079364_ACS_ACS_one_data_record.yml', RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

        log.debug('===== END TEST HAPPY PATH SINGLE =====')

    def test_real_file(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        log.debug('===== START TEST REAL FILE =====')

        # Recovered
        file_path = os.path.join(RESOURCE_PATH, RECOVERED_SAMPLE_DATA)

        stream_handle = open(file_path, O_MODE)

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(1044)

        log.debug("*** test_real_file Num particles %s", len(particles))

        # check all the values against expected results.
        self.assert_particles(particles, '11079364_ACS_ACS_recov.yml', RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

        # Telemetered
        file_path = os.path.join(RESOURCE_PATH, TELEMETERED_SAMPLE_DATA)

        stream_handle = open(file_path, O_MODE)

        parser = OptaaDjCsppParser(self._telemetered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_real_file Num particles %s", len(particles))

        # check all the values against expected results.
        self.assert_particles(particles, '11079364_ACD_ACS_telem.yml', RESOURCE_PATH)
        stream_handle.close()

        log.debug('===== END TEST REAL FILE =====')

    def test_real_file_2(self):
        """
        Read recovered data from zip file attached to the IDD
        and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        log.debug('===== START TEST REAL FILE 2 =====')

        # Recovered
        file_path = os.path.join(RESOURCE_PATH, "11079419_ACS_ACS.txt")

        stream_handle = open(file_path, O_MODE)

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(845)

        log.debug("*** test_real_file_2 Num particles %s", len(particles))

        # check all the values against expected results.
        self.assert_particles(particles, '11079419_ACS_ACS_recov.yml', RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

        log.debug('===== END TEST REAL FILE 2 =====')

    def test_long_stream(self):
        """
        Read test data and pull out multiple data particles
        Assert that we have the correct number of particles
        """
        log.debug('===== START TEST LONG STREAM =====')

        file_path = os.path.join(RESOURCE_PATH, RECOVERED_SAMPLE_DATA)
        stream_handle = open(file_path, O_MODE)

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        # try to get 1044 particles, 1043 data records plus one metadata
        particles = parser.get_records(1044)

        log.debug("*** test_long_stream Num particles is: %s", len(particles))
        self.assertEqual(len(particles), 1044)
        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

        log.debug('===== END TEST LONG STREAM =====')

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists and a RecoverableSampleException is thrown.
        Note: every other data record has bad data (float instead of int, extra column etc.).

        Data Record 1: timestamp is int
        Data Record 3: timestamp has non-digit
        Data Record 5: depth is int
        Data Record 7: suspect_timestamp is digit
        Data Record 9: serial number has letters
        Data Record 11: on seconds is missing
        Data Record 13: num wavelengths is letters
        Data Record 15: c ref dark is float
        Data Record 17: a c ref count is letters
        Data Record 19: begin with tab
        Data Record 21: begin with space
        Data Record 23: a sig count has a letter
        Data Record 25: external temp count is a float
        Data Record 27: internal temp count has a letter
        Data Record 29: pressure counts has a letter
        Data Record 31: has byte loss - from sample file in IDD
        Data Record 33: fields separated by space instead of tab
        Data Record 35: fields separated by multiple spaces instead of tab
           (between depth and Suspect Timestamp)
        Data Record 37: record ends with space then line feed
        Data Record 39: line starting with the timestamp, depth, and
           suspect timestamp, followed by all hex ascii chars
        Data Record 41: a line that is not data and does not start with Timestamp

        Particles will still get created for the metadata and valid instrument records.

        """
        log.debug('===== START TEST BAD DATA =====')

        file_path = os.path.join(RESOURCE_PATH, '11079364_BAD_ACS_ACS.txt')

        stream_handle = open(file_path, O_MODE)

        log.debug(self.exception_callback_value)

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(25)

        log.debug("Exception callback value: %s", self.exception_callback_value)

        self.assertTrue(self.exception_callback_value is not None)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        # 21 bad records
        self.assertEqual(len(self.exception_callback_value), 21)

        # check all the values against expected results.
        self.assert_particles(particles, '11079364_BAD_ACS_ACS.yml', RESOURCE_PATH)

        stream_handle.close()

        log.debug('===== END TEST BAD DATA =====')

    def test_missing_source_file(self):
        """
        Ensure that a missing source file line will cause a RecoverableSampleException to be thrown
        and the metadata particle will not be created. However, an
        instrument particle should still get created.
        """

        log.debug('===== START TEST MISSING SOURCE FILE =====')

        file_path = os.path.join(RESOURCE_PATH, '11079364_ACS_ACS_missing_source_file_record.txt')
        stream_handle = open(file_path, O_MODE)

        log.debug(self.exception_callback_value)

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(10)

        log.debug("Exception callback value: %s", self.exception_callback_value)

        self.assertTrue(self.exception_callback_value is not None)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        # 1 bad record
        self.assertEqual(len(self.exception_callback_value), 1)

        # check all the values against expected results.
        self.assert_particles(particles, '11079364_ACS_ACS_missing_source_file_record.yml', RESOURCE_PATH)

        stream_handle.close()

        log.debug('===== END TEST MISSING SOURCE FILE =====')

    def test_no_header(self):
        """
        Ensure that missing entire header will cause a RecoverableSampleException to be thrown
        and the metadata particle will not be created. However, an
        instrument particle should still get created.
        """

        log.debug('===== START TEST NO HEADER =====')

        file_path = os.path.join(RESOURCE_PATH, '11079364_ACS_ACS_no_header.txt')
        stream_handle = open(file_path, O_MODE)

        log.debug(self.exception_callback_value)

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(10)

        log.debug("Exception callback value: %s", self.exception_callback_value)

        self.assertTrue(self.exception_callback_value is not None)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        # 1 bad record
        self.assertEqual(len(self.exception_callback_value), 1)

        # check all the values against expected results.
        self.assert_particles(particles, '11079364_ACS_ACS_no_header.yml', RESOURCE_PATH)

        stream_handle.close()

        log.debug('===== END TEST NO HEADER =====')

    def test_no_trailing_tab(self):
        """
        Ensure that we can handle records that do not have trailing tabs. If we encounter them,
        no exception should be thrown and the particle should be created as usual.
        The second and fourth data records do not have trailing tabs.
        """

        log.debug('===== START TEST NO TRAILING TAB =====')

        # This test file has some records with a trailing tab, and others do not
        no_trailing_tab_file = '11079364_ACS_ACS_no_trailing_tab.txt'

        file_path = os.path.join(RESOURCE_PATH, no_trailing_tab_file)

        stream_handle = open(file_path, O_MODE)

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = OptaaDjCsppParser(self._recovered_parser_config,
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(45)

        log.debug("*** test_simple Num particles %s", len(particles))

        # check all the values against expected results.
        self.assert_particles(particles, '11079364_ACS_ACS_no_trailing_tab.yml', RESOURCE_PATH)

        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

        log.debug('===== END TEST NO TRAILING TAB =====')
