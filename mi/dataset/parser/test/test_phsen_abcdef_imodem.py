"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_phsen_abcdef_imodem.py
@author Joe Padula
@brief Test code for the phsen_abcdef_imodem parser

"""

__author__ = 'Joe Padula'

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import UnexpectedDataException
from mi.logging import log
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH, ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.phsen_abcdef_imodem import \
    PhsenAbcdefImodemParticleClassKey, \
    PhsenAbcdefImodemParser
from mi.dataset.parser.phsen_abcdef_imodem_particles import \
    PhsenAbcdefImodemMetadataRecoveredDataParticle, \
    PhsenAbcdefImodemControlRecoveredDataParticle, \
    PhsenAbcdefImodemInstrumentRecoveredDataParticle, \
    PhsenAbcdefImodemMetadataTelemeteredDataParticle, \
    PhsenAbcdefImodemControlTelemeteredDataParticle, \
    PhsenAbcdefImodemInstrumentTelemeteredDataParticle

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH,
                             'phsen_abcdef', 'imodem', 'resource')

O_MODE = 'rU'   # Universal Open mode


@attr('UNIT', group='mi')
class PhsenAbcdefImodemParserUnitTestCase(ParserUnitTestCase):
    """
    phsen_abcdef_imodem Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._recovered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef_imodem_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                PhsenAbcdefImodemParticleClassKey.METADATA_PARTICLE_CLASS:
                PhsenAbcdefImodemMetadataRecoveredDataParticle,
                PhsenAbcdefImodemParticleClassKey.CONTROL_PARTICLE_CLASS:
                PhsenAbcdefImodemControlRecoveredDataParticle,
                PhsenAbcdefImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                PhsenAbcdefImodemInstrumentRecoveredDataParticle,
            }
        }

        self._telemetered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef_imodem_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                PhsenAbcdefImodemParticleClassKey.METADATA_PARTICLE_CLASS:
                PhsenAbcdefImodemMetadataTelemeteredDataParticle,
                PhsenAbcdefImodemParticleClassKey.CONTROL_PARTICLE_CLASS:
                PhsenAbcdefImodemControlTelemeteredDataParticle,
                PhsenAbcdefImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                PhsenAbcdefImodemInstrumentTelemeteredDataParticle,
            }
        }

    def build_telem_parser(self):
        """
        Build a telemetered parser, storing it in self.parser
        """
        if self.stream_handle is None:
            self.fail("Must set stream handle before building telemetered parser")
        self.parser = PhsenAbcdefImodemParser(self._telemetered_parser_config, self.stream_handle,
                                              self.exception_callback)

    def build_recov_parser(self):
        """
        Build a telemetered parser, storing it in self.parser
        This requires stream handle to be set before calling it
        """
        if self.stream_handle is None:
            self.fail("Must set stream handle before building recovered parser")
        self.parser = PhsenAbcdefImodemParser(self._recovered_parser_config, self.stream_handle,
                                              self.exception_callback)

    def test_happy_path_simple(self):
        """
        Read a file and verify that a pH and control records and header/footer can be read.
        Verify that the contents of the instrument, control and metadata particles are correct.
        The last record is a control record with battery data.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST HAPPY PATH SINGLE =====')

        # Recovered
        with open(os.path.join(RESOURCE_PATH, 'example1.DAT'), O_MODE) as file_handle:

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(5)

            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "example1_rec.yml", RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        # Telemetered
        with open(os.path.join(RESOURCE_PATH, 'example1.DAT'), O_MODE) as file_handle:

            parser = PhsenAbcdefImodemParser(self._telemetered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(5)

            log.debug("Num particles: %d", len(particles))

            self.assert_particles(particles, "example1_tel.yml", RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST HAPPY PATH SINGLE =====')

    def test_invalid_header_timestamp(self):
        """
        The file used in this test has error in the File Date Time for the header record.
        This results in 4 particles being created instead of 5
        (metadata particle is not created), and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INVALID METADATA TIMESTAMP =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_header_timestamp.DAT'), O_MODE) as file_handle:

            num_particles_to_request = 5
            num_expected_particles = 4

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "invalid_header_timestamp_rec.yml", RESOURCE_PATH)

            log.debug('Exceptions : %s', self.exception_callback_value)

            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

        log.debug('===== END TEST INVALID METADATA TIMESTAMP =====')

    def test_invalid_record_type(self):
        """
        The file used in this test has a record type in the second record that does not match any
        of the expected record types.
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INVALID RECORD TYPE =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_record_type.DAT'), O_MODE) as file_handle:

            num_particles_to_request = 6
            num_expected_particles = 5

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "invalid_record_type_rec.yml", RESOURCE_PATH)

            log.debug('Exceptions : %s', self.exception_callback_value)

            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

        log.debug('===== END TEST INVALID RECORD TYPE =====')

    def test_ph_record_missing_timestamp(self):
        """
        The file used in this test has a pH record (the second record - Record[331])
        with a missing timestamp.
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST PH RECORD MISSING TIMESTAMP =====')

        with open(os.path.join(RESOURCE_PATH, 'ph_record_missing_timestamp.DAT'), O_MODE) as file_handle:

            num_particles_to_request = 6
            num_expected_particles = 5

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "ph_record_missing_timestamp_rec.yml", RESOURCE_PATH)

            log.debug('Exceptions : %s', self.exception_callback_value)

            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

        log.debug('===== END TEST PH RECORD MISSING TIMESTAMP =====')

    def test_no_science_particles(self):
        """
        The file used in this test only contains header and footer records.
        Verify that no science (pH or Control) particles are produced if the input file
        has no pH data records or control data, i.e., they just contain header and footer records.
        In this case only the metadata particle will get created.
        """
        log.debug('===== START TEST NO SCIENCE PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, 'header_and_footer_only.DAT'), O_MODE) as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 1

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "header_and_footer_only_rec.yml", RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST NO SCIENCE PARTICLES =====')

    def test_incorrect_length(self):
        """
        The last records in the file used in this test has a length that does not match the Length
        field in the record. This tests for this requirement:
        If the beginning of another instrument data record (* character), is encountered before "Length"
        bytes have been found, where "Length" is the record length specified in a record, then we can not
        reliably parse the record.
        This results in 5 particles being retrieved instead of 6, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST INCORRECT LENGTH =====')

        with open(os.path.join(RESOURCE_PATH, 'incorrect_data_length.DAT'), O_MODE) as file_handle:

            num_particles_to_request = 6
            num_expected_particles = 5

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "incorrect_data_length_rec.yml", RESOURCE_PATH)

        log.debug('Exceptions : %s', self.exception_callback_value)

        self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

        log.debug('===== END TEST INCORRECT LENGTH =====')

    def test_invalid_checksum(self):
        """
        The first record in the file used in this test has in invalid checksum. An instrument particle will
        still get created, but the passed_checksum parameter will be 0 (no warning or error msg generated).
        """
        log.debug('===== START TEST INVALID CHECKSUM =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_checksum.DAT'), O_MODE) as file_handle:

            num_particles_to_request = 5
            num_expected_particles = 5

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "invalid_checksum_rec.yml", RESOURCE_PATH)

            # No exception should be thrown
            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST INVALID CHECKSUM =====')

    def test_invalid_header_fields(self):
        """
        The header in the file used in this test has in invalid Voltage and Number of Samples Written.
        A metadata particle will still get created, but there will be None in some of the parameters
        (an exception will be generated).
        """
        log.debug('===== START TEST INVALID HEADER FIELDS =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid_header_fields.DAT'), O_MODE) as file_handle:

            num_particles_to_request = 5
            num_expected_particles = 5

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "invalid_header_fields_rec.yml", RESOURCE_PATH)

            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

        log.debug('===== END TEST INVALID HEADER FIELDS =====')

    def test_real_file(self):
        """
        The file used in this test, is a real file from the acquisition server.
        It contains 20 pH records:
        Verify that 20 instrument particles and one metadata particle are generated
        from the real file.
        """
        log.debug('===== START TEST REAL FILE =====')

        num_particles_to_request = 25
        num_expected_particles = 21

        with open(os.path.join(RESOURCE_PATH, 'phsen1_20140730_190554.DAT'), O_MODE) as file_handle:

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            log.info(len(particles))

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "phsen1_20140730_190554_rec.yml", RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'phsen1_20140730_190554.DAT'), O_MODE) as file_handle:

            parser = PhsenAbcdefImodemParser(self._telemetered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            log.info(len(particles))

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "phsen1_20140730_190554_tel.yml", RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST REAL FILE =====')

    def test_real_file_2(self):
        """
        The file used in this test, is a real file from the acquisition server.
        It contains 9 pH records:
        Verify that 9 instrument particles and one metadata particle are generated
        from the real file.
        """
        log.debug('===== START TEST REAL 2 FILE =====')

        num_particles_to_request = 10
        num_expected_particles = 10

        with open(os.path.join(RESOURCE_PATH, 'phsen1_20140725_192532.DAT'), O_MODE) as file_handle:

            parser = PhsenAbcdefImodemParser(self._recovered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            log.info(len(particles))

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "phsen1_20140725_192532_rec.yml", RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'phsen1_20140725_192532.DAT'), O_MODE) as file_handle:

            parser = PhsenAbcdefImodemParser(self._telemetered_parser_config,
                                             file_handle,
                                             self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            log.info(len(particles))

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, "phsen1_20140725_192532_tel.yml", RESOURCE_PATH)

            self.assertEquals(self.exception_callback_value, [])

        log.debug('===== END TEST REAL 2 FILE =====')

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write('  - _index: %d\n' % (i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write('    %s: \'%s\'\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'phsen1_20140725_192532.DAT'), O_MODE)
        self.build_telem_parser()
        particles = self.parser.get_records(21)

        self.particle_to_yml(particles, 'phsen1_20140725_192532_tel.yml')
        self.stream_handle.close()
