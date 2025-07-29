#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_plims_a_hdr
@file mi-dataset/mi/dataset/parser/test/test_plims_a_hdr.py
@author Samuel Dahlberg, Joffrey Peters
@brief Test code for a plims_a_hdr data parser
"""

import os
from pickle import INST

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.plims_a.resource import RESOURCE_PATH
from mi.dataset.parser.plims_a_hdr import PlimsAHdrParser
from mi.dataset.parser.plims_a_particles import PlimsAHdrClassKey
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.plims_a_particles'
INSTRUMENT_DATA_PARTICLE = 'PlimsAHdrInstrumentDataParticle'
ENGGINEERING_DATA_PARTICLE = 'PlimsAHdrEngineeringDataParticle'


class PlimsAHdrUnitTestCase(ParserUnitTestCase):
    """
    plims_a_hdr Parser unit test suite
    """

    def create_plims_a_hdr_parser(self, file_handle):
        return PlimsAHdrParser(self.config, file_handle, self.rec_exception_callback)

    def file_path(self, filename):
        log.debug('resource path = %s, file name = %s', RESOURCE_PATH, filename)
        return os.path.join(RESOURCE_PATH, filename)

    def rec_exception_callback(self, exception):
        """
        Call back method to watch what comes in via the exception callback
        """
        self.exception_callback_value.append(exception)
        self.exceptions_detected += 1
    
    def setup(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                PlimsAHdrClassKey.ENGINEERING: ENGGINEERING_DATA_PARTICLE,
                PlimsAHdrClassKey.INSTRUMENT: INSTRUMENT_DATA_PARTICLE,
            }
        }

        self.exception_callback_value = []
        self.exceptions_detected = 0

    def test_plims_a_hdr_parser(self):
        """
        Read a file and pull out instrument and an engineering data particles.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST PLIMS_A_HDR Parser =====')
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195.hdr')) as in_file:
            parser = self.create_plims_a_hdr_parser(in_file)

            # In a single read, get all particles in this file.
            result = parser.get_records(30)

            self.assertEqual(len(result), 2) # one instrument and one engineering particle
            self.assertListEqual(self.exception_callback_value, [])
            hdr_instrument = result[0].generate_dict()
            hdr_engineering = result[1].generate_dict()

            log.debug("Instrument particle: %s", hdr_instrument)
            log.debug("Engineering particle: %s", hdr_engineering)
        
            # self.assertEqual(hdr_instrument['particle_type'], INSTRUMENT_DATA_PARTICLE)
            # self.assertEqual(hdr_engineering['particle_type'], ENGGINEERING_DATA_PARTICLE)
            for value in hdr_instrument['values']:
                if value['value_id'] == 'sample_filename':
                    self.assertEqual(value['value'], 'D20230222T174812_IFCB195')
            for value in hdr_engineering['values']:
                if value['value_id'] == 'sample_filename':
                    self.assertEqual(value['value'], 'D20230222T174812_IFCB195')

        log.debug('===== END TEST PLIMS_A_HDR Instrument Parser  =====')

    def test_missing_instrument_data(self):
        """
        Ensure that missing instrument data raises exception when missing.
        """
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195_missing_instrument_values.hdr')) as in_file:
            parser = self.create_plims_a_hdr_parser(in_file)

            with self.assertRaises(KeyError):
                # In a single read, attempt to read all particles for this file.
                result = parser.get_records(1)


    def test_missing_engineering_data(self):
        """
        Ensure that missing engineering data raises exception when missing.
        """
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195_missing_engineering_values.hdr')) as in_file:
            parser = self.create_plims_a_hdr_parser(in_file)
            
            with self.assertRaises(KeyError):
                # In a single read, get all particles for this file.
                result = parser.get_records(30)
                

    def test_corrupt_values_instrument_data(self):
        """
        Ensure that bad data raises ValueError when it exists.
        """
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195_corrupt_instrument_values.hdr')) as in_file:
            parser = self.create_plims_a_hdr_parser(in_file)

            with self.assertRaises(ValueError):
                # In a single read, get all particles for this file.
                result = parser.get_records(30)

    def test_corrupt_values_engineering_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195_corrupt_engineering_values.hdr')) as in_file:
            parser = self.create_plims_a_hdr_parser(in_file)
            
            with self.assertRaises(ValueError):
                # In a single read, get all particles for this file.
                result = parser.get_records(30)


    def test_corrupt_values_both_data_types(self):
        """
        Ensure that bad data raises ValueErrors when it exists.
        """
        self.setup()

        with open(self.file_path('D20230222T174812_IFCB195_corrupt_values.hdr')) as in_file:
            parser = self.create_plims_a_hdr_parser(in_file)
            
            with self.assertRaises(ValueError):
                # In a single read, get all particles for this file.
                result = parser.get_records(30)
