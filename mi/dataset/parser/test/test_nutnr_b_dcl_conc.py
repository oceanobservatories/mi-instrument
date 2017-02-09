#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_nutnr_b_dcl_conc
@file mi/dataset/parser/test/test_nutnr_b_dcl_conc.py
@author Steve Myerson (Raytheon), Mark Worden
@brief Test code for a nutnr_b_dcl_conc data parser

Files used for testing:

Filename             Blocks  Instrument Records
20010101.nutnr_b_dcl_conc.log     1         0
20020125.nutnr_b_dcl_conc.log     1        25
20031129.nutnr_b_dcl_conc.log     2        11, 29
20040509.nutnr_b_dcl_conc.log     3        4, 5, 9
20051220.nutnr_b_dcl_conc.log     2        40, 75
20061225.nutnr_b_dcl_conc.log     3        50, 80, 125

19990401.nutnr_b_dcl_conc.log    1        19 valid, 1 invalid frame type
19980401.nutnr_b_dcl_conc.log    1        20, metadata incomplete
19970401.nutnr_b_dcl_conc.log    1        No valid records (various fields incorrect)

Real file:
20140430.nutnr_b_dcl_conc.log has 8 NDC records and 56 NLC records.
Should generate 8 metadata particles and 64 instrument particles.

Each block produces 1 metadata particle unless there are no instrument records.
Each instrument record produces 1 instrument particle.
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.nutnr_b.dcl_conc.resource import RESOURCE_PATH
from mi.dataset.parser.nutnr_b_dcl_conc import \
    NutnrBDclConcRecoveredParser, \
    NutnrBDclConcTelemeteredParser
from mi.dataset.parser.nutnr_b_particles import \
    NutnrBDclConcRecoveredInstrumentDataParticle, \
    NutnrBDclDarkConcRecoveredInstrumentDataParticle, \
    NutnrBDclConcTelemeteredInstrumentDataParticle, \
    NutnrBDclDarkConcTelemeteredInstrumentDataParticle, \
    NutnrBDclConcRecoveredMetadataDataParticle, \
    NutnrBDclConcTelemeteredMetadataDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.nutnr_b_particles'

FILE1 = '20010101.nutnr_b_dcl_conc.log'
FILE2 = '20020125.nutnr_b_dcl_conc.log'
FILE3 = '20031129.nutnr_b_dcl_conc.log'
FILE4 = '20040509.nutnr_b_dcl_conc.log'
FILE5 = '20051220.nutnr_b_dcl_conc.log'
FILE6 = '20061225.nutnr_b_dcl_conc.log'
FILE_INVALID_FRAME_TYPE = '19990401.nutnr_b_dcl_conc.log'
FILE_MISSING_METADATA = '19980401.nutnr_b_dcl_conc.log'
FILE_INVALID_FIELDS = '19970401.nutnr_b_dcl_conc.log'
FILE_REAL = '20140430.nutnr_b_dcl_conc.log'
SECOND_BLOCK_IN_DATA_BLOCK_FILE = '20040901.nutnr_b_dcl_conc.log'

EXPECTED_PARTICLES1 = 0
EXPECTED_PARTICLES2 = 26
EXPECTED_PARTICLES3 = 42
EXPECTED_PARTICLES4 = 21
EXPECTED_PARTICLES5 = 117
EXPECTED_PARTICLES6 = 258
EXPECTED_META_PARTICLES_REAL = 8
EXPECTED_INST_PARTICLES_REAL = 64

EXPECTED_PARTICLES_INVALID_FRAME_TYPE = 19
EXPECTED_EXCEPTIONS_INVALID_FRAME_TYPE = 3
EXPECTED_PARTICLES_MISSING_METADATA = 20
EXPECTED_EXCEPTIONS_MISSING_METADATA = 2
EXPECTED_PARTICLES_INVALID_FIELDS = 2
EXPECTED_EXCEPTIONS_INVALID_FIELDS = 20
EXPECTED_PARTICLES_SECOND_BLOCK_IN_DATA_BLOCK = 7
EXPECTED_EXCEPTIONS_SECOND_BLOCK_IN_DATA_BLOCK = 0

REC_YML2 = 'rec_20020125.nutnr_b_dcl_conc.yml'
REC_YML3 = 'rec_20031129.nutnr_b_dcl_conc.yml'
REC_YML4 = 'rec_20040509.nutnr_b_dcl_conc.yml'
REC_YML5 = 'rec_20051220.nutnr_b_dcl_conc.yml'
REC_YML6 = 'rec_20061225.nutnr_b_dcl_conc.yml'
REC_YML_INVALID_FIELDS = 'rec_19970401.nutnr_b_dcl_conc.yml'
TEL_YML2 = 'tel_20020125.nutnr_b_dcl_conc.yml'
TEL_YML3 = 'tel_20031129.nutnr_b_dcl_conc.yml'
TEL_YML4 = 'tel_20040509.nutnr_b_dcl_conc.yml'
TEL_YML5 = 'tel_20051220.nutnr_b_dcl_conc.yml'
TEL_YML6 = 'tel_20061225.nutnr_b_dcl_conc.yml'
TEL_YML_INVALID_FIELDS = 'tel_19970401.nutnr_b_dcl_conc.yml'

HAPPY_PATH_TABLE = [
    (FILE2, EXPECTED_PARTICLES2, REC_YML2, TEL_YML2),
    (FILE3, EXPECTED_PARTICLES3, REC_YML3, TEL_YML3),
    (FILE4, EXPECTED_PARTICLES4, REC_YML4, TEL_YML4),
    (FILE5, EXPECTED_PARTICLES5, REC_YML5, TEL_YML5),
    (FILE6, EXPECTED_PARTICLES6, REC_YML6, TEL_YML6)
]


@attr('UNIT', group='mi')
class NutnrBDclConcParserUnitTestCase(ParserUnitTestCase):
    """
    nutnr_b_dcl_conc Parser unit test suite
    """

    def create_rec_parser(self, file_handle, new_state=None):
        """
        This function creates a NutnrBDclConc parser for recovered data.
        """
        return NutnrBDclConcRecoveredParser(
            self.rec_config,
            file_handle, lambda state, ingested: None,
            lambda data: None, self.rec_exception_callback)

    def create_tel_parser(self, file_handle, new_state=None):
        """
        This function creates a NutnrBDclConc parser for telemetered data.
        """
        return NutnrBDclConcTelemeteredParser(
            self.tel_config,
            file_handle, lambda state, ingested: None,
            lambda data: None, self.tel_exception_callback)

    def open_file(self, filename):
        return open(os.path.join(RESOURCE_PATH, filename), mode='r')

    def rec_state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.rec_state_callback_value = state
        self.rec_file_ingested_value = file_ingested

    def tel_state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.tel_state_callback_value = state
        self.tel_file_ingested_value = file_ingested

    def rec_pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.rec_publish_callback_value = pub

    def tel_pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.tel_publish_callback_value = pub

    def rec_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.rec_exception_callback_value = exception
        self.rec_exceptions_detected += 1

    def tel_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.tel_exception_callback_value = exception
        self.tel_exceptions_detected += 1

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.tel_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.rec_state_callback_value = None
        self.rec_file_ingested_value = False
        self.rec_publish_callback_value = None
        self.rec_exception_callback_value = None
        self.rec_exceptions_detected = 0

        self.tel_state_callback_value = None
        self.tel_file_ingested_value = False
        self.tel_publish_callback_value = None
        self.tel_exception_callback_value = None
        self.tel_exceptions_detected = 0

        self.maxDiff = None

    def test_happy_path(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        """
        log.debug('===== START TEST HAPPY PATH =====')

        for input_file, expected_particles, rec_yml_file, tel_yml_file in HAPPY_PATH_TABLE:

            in_file = self.open_file(input_file)
            parser = self.create_rec_parser(in_file)
            particles = parser.get_records(expected_particles)
            self.assert_particles(particles, rec_yml_file, RESOURCE_PATH)
            self.assertEqual(self.rec_exceptions_detected, 0)
            in_file.close()

            in_file = self.open_file(input_file)
            parser = self.create_tel_parser(in_file)
            particles = parser.get_records(expected_particles)
            self.assert_particles(particles, tel_yml_file, RESOURCE_PATH)
            self.assertEqual(self.tel_exceptions_detected, 0)
            in_file.close()

        log.debug('===== END TEST HAPPY PATH =====')

    def test_invalid_fields(self):
        """
        The file used in this test has errors in every instrument record
        except the first NDC record.
        This results in 1 metadata particle and 1 instrument particle.
        """
        log.debug('===== START TEST INVALID FIELDS =====')

        input_file = FILE_INVALID_FIELDS
        expected_particles = EXPECTED_PARTICLES_INVALID_FIELDS
        expected_exceptions = EXPECTED_EXCEPTIONS_INVALID_FIELDS
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, REC_YML_INVALID_FIELDS, RESOURCE_PATH)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, TEL_YML_INVALID_FIELDS, RESOURCE_PATH)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

        log.debug('===== END TEST INVALID FIELDS =====')

    def test_invalid_frame_type(self):
        """
        The file used in this test has an valid frame type instead
        of the NDC (dark) type and 1 other invalid frame type.
        This results in 1 metadata,
        instrument particles for the other valid instrument types,
        plus 2 Recoverable exceptions.
        """
        log.debug('===== START TEST INVALID FRAME TYPE =====')

        input_file = FILE_INVALID_FRAME_TYPE
        expected_particles = EXPECTED_PARTICLES_INVALID_FRAME_TYPE
        expected_exceptions = EXPECTED_EXCEPTIONS_INVALID_FRAME_TYPE
        total_records = expected_particles + expected_exceptions

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

        log.debug('===== END TEST INVALID FRAME TYPE =====')

    def test_missing_metadata(self):
        """
        The file used in this test is missing one of the required
        metadata records.
        This causes no metadata particles to be generated and 20 science particles
        """
        log.debug('===== START TEST MISSING METADATA =====')

        input_file = FILE_MISSING_METADATA
        expected_particles = EXPECTED_PARTICLES_MISSING_METADATA
        expected_exceptions = EXPECTED_EXCEPTIONS_MISSING_METADATA
        total_records = expected_particles + expected_exceptions

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)

        inst_particles = 0
        meta_particles = 0
        for particle in particles:
            if isinstance(particle, NutnrBDclConcRecoveredInstrumentDataParticle) or \
               isinstance(particle, NutnrBDclDarkConcRecoveredInstrumentDataParticle):
                inst_particles += 1
            elif isinstance(particle, NutnrBDclConcRecoveredMetadataDataParticle):
                meta_particles += 1

        self.assertEqual(inst_particles, expected_particles)
        self.assertEqual(meta_particles, 0)

        in_file.close()

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)

        inst_particles = 0
        meta_particles = 0
        for particle in particles:
            if isinstance(particle, NutnrBDclConcTelemeteredInstrumentDataParticle) or \
               isinstance(particle, NutnrBDclDarkConcTelemeteredInstrumentDataParticle):
                inst_particles += 1
            elif isinstance(particle, NutnrBDclConcTelemeteredMetadataDataParticle):
                meta_particles += 1

        self.assertEqual(inst_particles, expected_particles)
        self.assertEqual(meta_particles, 0)

        in_file.close()

        log.debug('===== END TEST MISSING METADATA =====')

    def test_second_nitrate_dark_in_data_block(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST SECOND DARK IN DATA BLOCK =====')

        input_file = SECOND_BLOCK_IN_DATA_BLOCK_FILE
        expected_particles = EXPECTED_PARTICLES_SECOND_BLOCK_IN_DATA_BLOCK
        expected_exceptions = EXPECTED_EXCEPTIONS_SECOND_BLOCK_IN_DATA_BLOCK
        total_records = expected_particles

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

        log.debug('===== END TEST SECOND DARK IN DATA BLOCK =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        input_file = FILE1
        expected_particles = EXPECTED_PARTICLES1
        total_records = expected_particles + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, 0)
        in_file.close()

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, 0)
        in_file.close()

        log.debug('===== END TEST NO PARTICLES =====')

    def test_real_file(self):
        """
        Verify that the correct number of particles are generated
        from a real file.
        """
        log.debug('===== START TEST REAL FILE =====')

        input_file = FILE_REAL
        expected_inst_particles = EXPECTED_INST_PARTICLES_REAL
        expected_meta_particles = EXPECTED_META_PARTICLES_REAL
        expected_particles = expected_meta_particles + expected_inst_particles

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(expected_particles)
        self.assertEqual(len(particles), expected_particles)

        inst_particles = 0
        meta_particles = 0
        for particle in particles:
            if isinstance(particle, NutnrBDclConcRecoveredInstrumentDataParticle) or \
               isinstance(particle, NutnrBDclDarkConcRecoveredInstrumentDataParticle):
                inst_particles += 1
            elif isinstance(particle, NutnrBDclConcRecoveredMetadataDataParticle):
                meta_particles += 1

        self.assertEqual(inst_particles, expected_inst_particles)
        self.assertEqual(meta_particles, expected_meta_particles)

        in_file.close()

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(expected_particles)
        self.assertEqual(len(particles), expected_particles)

        inst_particles = 0
        meta_particles = 0
        for particle in particles:
            if isinstance(particle, NutnrBDclConcTelemeteredInstrumentDataParticle) or \
               isinstance(particle, NutnrBDclDarkConcTelemeteredInstrumentDataParticle):
                inst_particles += 1
            elif isinstance(particle, NutnrBDclConcTelemeteredMetadataDataParticle):
                meta_particles += 1

        self.assertEqual(inst_particles, expected_inst_particles)
        self.assertEqual(meta_particles, expected_meta_particles)

        in_file.close()

        log.debug('===== END TEST REAL FILE =====')