"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_nutnr_j_cspp.py
@author Emily Hahn
@brief Test code for a nutnr_j_cspp data parser
"""
import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import RecoverableSampleException, \
    ConfigurationException

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH, ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY

from mi.dataset.parser.nutnr_j_cspp import NutnrJCsppParser, \
    NutnrJCsppMetadataTelemeteredDataParticle, \
    NutnrJCsppTelemeteredDataParticle, \
    NutnrJCsppDarkTelemeteredDataParticle, \
    NutnrJCsppDarkRecoveredDataParticle, \
    NutnrJCsppMetadataRecoveredDataParticle, \
    NutnrJCsppRecoveredDataParticle, \
    LIGHT_PARTICLE_CLASS_KEY, \
    DARK_PARTICLE_CLASS_KEY

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'nutnr_j', 'cspp', 'resource')

MODE_ASCII_READ = 'r'


@attr('UNIT', group='mi')
class NutnrJCsppParserUnitTestCase(ParserUnitTestCase):
    """
    nutnr_j_cspp Parser unit test suite
    """

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
                    if val.get('value_id') is "time_of_sample":
                        fid.write('   %s: %16.5f\n' % (val.get('value_id'), val.get('value')))
                    else:
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
                                '11079419_SNA_SNA.txt'),
                   MODE_ASCII_READ)

        stream_handle = fid

        self.create_parser(stream_handle, True)

        particles = self.parser.get_records(1000)

        self.particle_to_yml(particles, '11079419_SNA_SNA_telem.yml')
        fid.close()

    def create_parser(self, stream_handle, telem_flag=True):
        """
        Initialize the parser with the given stream handle, using the
        telemetered config if the flag is set, recovered if it is not
        """
        if telem_flag:
            # use telemetered config
            config = {
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: NutnrJCsppMetadataTelemeteredDataParticle,
                    LIGHT_PARTICLE_CLASS_KEY: NutnrJCsppTelemeteredDataParticle,
                    DARK_PARTICLE_CLASS_KEY: NutnrJCsppDarkTelemeteredDataParticle
                }
            }
        else:
            # use recovered config
            config = {
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: NutnrJCsppMetadataRecoveredDataParticle,
                    LIGHT_PARTICLE_CLASS_KEY: NutnrJCsppRecoveredDataParticle,
                    DARK_PARTICLE_CLASS_KEY: NutnrJCsppDarkRecoveredDataParticle
                }
            }

        self.parser = NutnrJCsppParser(config, stream_handle,
                                       self.exception_callback)

    def test_simple_telem(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'short_SNA_SNA.txt'), MODE_ASCII_READ)

        self.create_parser(stream_handle)

        # get and compare the metadata particle
        particles = self.parser.get_records(6)

        # check all the values against expected results.
        self.assert_particles(particles, 'short_SNA_telem.yml', RESOURCE_PATH)

        # confirm no exceptions occurred
        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_simple_recov(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'short_SNA_SNA.txt'), MODE_ASCII_READ)

        self.create_parser(stream_handle, telem_flag=False)

        # get and compare the metadata particle
        particles = self.parser.get_records(6)

        # check all the values against expected results.
        self.assert_particles(particles, 'short_SNA_recov.yml', RESOURCE_PATH)

        # confirm no exceptions occurred
        self.assertEqual(self.exception_callback_value, [])

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists. Confirm
        that exceptions occur.
        """
        # bad data file has:
        # 1 bad status
        # particle A has bad timestamp
        # particle B has bad dark fit
        # particle C has bad frame type
        # particle D has bad year
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'bad_SNA_SNA.txt'), MODE_ASCII_READ)

        self.create_parser(stream_handle, telem_flag=False)

        # get E, since it is first it will generate a metadata
        particles = self.parser.get_records(2)

        # check all the values against expected results.
        self.assert_particles(particles, 'last_and_meta_SNA_recov.yml', RESOURCE_PATH)

        # should have had 5 exceptions by now
        self.assertEqual(len(self.exception_callback_value), 5)

        for exception in self.exception_callback_value:
            self.assert_(isinstance(exception, RecoverableSampleException))

    def test_missing_source_file(self):
        """
        Test that a file with a missing source file path in the header
        fails to create a metadata particle and throws an exception
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'no_source_file_SNA_SNA.txt'), MODE_ASCII_READ)

        self.create_parser(stream_handle)

        # get A-E, without metadata
        particles = self.parser.get_records(5)

        # check all the values against expected results.
        self.assert_particles(particles, 'no_source_file_SNA_SNA_telem.yml', RESOURCE_PATH)

        # confirm an exception occurred
        self.assertEqual(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        stream_handle.close()

    def test_no_header(self):
        """
        Test that a file with no header lines
        fails to create a metadata particle and throws an exception
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'no_header_SNA_SNA.txt'), MODE_ASCII_READ)

        self.create_parser(stream_handle)

        # get A-E, without metadata
        particles = self.parser.get_records(5)

        # check all the values against expected results.
        self.assert_particles(particles, 'short_SNA_telem_no_meta.yml', RESOURCE_PATH)

        # confirm an exception occurred
        self.assertEqual(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        stream_handle.close()

    def test_partial_header(self):
        """
        Test a case where we are missing part of the header, but it is not
        the source file so we still want to create the header
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'part_header_SNA_SNA.txt'), MODE_ASCII_READ)

        self.create_parser(stream_handle)

        # get A-E, also metadata
        particles = self.parser.get_records(6)

        # check all the values against expected results.
        self.assert_particles(particles, 'short_SNA_telem_part.yml', RESOURCE_PATH)

        # confirm no exceptions occurred
        self.assertEqual(self.exception_callback_value, [])
        stream_handle.close()

    def test_bad_config(self):
        """
        Test that configurations with a missing data particle dict and missing
        data particle class key causes a configuration exception
        """
        # test a config with a missing particle classes dict
        config = {}
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'short_SNA_SNA.txt'), MODE_ASCII_READ)

        with self.assertRaises(ConfigurationException):
            self.parser = NutnrJCsppParser(config, stream_handle,
                                           self.exception_callback)

        # test a config with a missing data particle class key
        config = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: NutnrJCsppMetadataTelemeteredDataParticle,
            }
        }

        with self.assertRaises(ConfigurationException):
            self.parser = NutnrJCsppParser(config, stream_handle,
                                           self.exception_callback)

    def test_real_file(self):
        """
        Read test data from IDD and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        log.info('===== START TEST BYTE LOSS =====')

        # Recovered
        file_path = os.path.join(RESOURCE_PATH, '11079364_SNA_SNA.txt')

        stream_handle = open(file_path, MODE_ASCII_READ)

        self.create_parser(stream_handle, telem_flag=False)

        particles = self.parser.get_records(182)

        log.debug("*** test_real_file Num particles %s", len(particles))

        # check all the values against expected results.
        self.assert_particles(particles, '11079364_SNA_SNA_recov.yml', RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

        # Telemetered
        file_path = os.path.join(RESOURCE_PATH, '11079419_SNA_SNA.txt')

        stream_handle = open(file_path, MODE_ASCII_READ)

        self.create_parser(stream_handle)

        particles = self.parser.get_records(172)

        log.debug("*** test_real_file Num particles %s", len(particles))

        # check all the values against expected results.
        self.assert_particles(particles, '11079419_SNA_SNA_telem.yml', RESOURCE_PATH)
        stream_handle.close()

        log.info('===== END TEST REAL FILE =====')

    def test_bad_match(self):
        """
        Test that a file that has a data sample that is causing the regex
        matcher to hang. This test confirms
        the fix doesn't hang and causes exceptions for not matching data.
        """
        log.info('===== START TEST BAD MATCH =====')

        # Telemetered
        file_path = os.path.join(RESOURCE_PATH, '11129553_SNA_SNA.txt')

        stream_handle = open(file_path, MODE_ASCII_READ)

        self.create_parser(stream_handle)

        particles = self.parser.get_records(57)

        log.debug("*** test_bad_match Num particles %s", len(particles))

        # 2 bad samples
        self.assertEqual(len(self.exception_callback_value), 2)
        stream_handle.close()

        log.info('===== END TEST BAD MATCH =====')

    def test_byte_loss(self):
        """
        Test that a file with known byte loss occurring in the form of hex ascii
        lines of data creates an exception.
        """
        log.info('===== START TEST REAL FILE =====')

        # Telemetered
        file_path = os.path.join(RESOURCE_PATH, '11330408_SNA_SNA.txt')

        stream_handle = open(file_path, MODE_ASCII_READ)

        self.create_parser(stream_handle)

        particles = self.parser.get_records(3)

        log.debug("*** test_byte_loss Num particles %s", len(particles))

        # check all the values against expected results.
        self.assert_particles(particles, 'byte_loss.yml', RESOURCE_PATH)
        stream_handle.close()

        log.info('===== END TEST BYTE LOSS =====')

