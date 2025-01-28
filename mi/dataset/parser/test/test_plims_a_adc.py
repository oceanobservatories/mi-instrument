"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_plims_a_adc.py
@author Joffrey Peters
@brief Test code for a plims_a_adc data parser
"""

import os

import numpy as np

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.plims_a.resource import RESOURCE_PATH
from mi.dataset.parser.plims_a_adc import PlimsAAdcParser
from mi.dataset.parser.plims_a_particles import (
    DataParticleType,
    PlimsAAdcDataParticle,
    PlimsAAdcParticleKey,
)
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

MODULE_NAME = 'mi.dataset.parser.plims_a_adc'
CLASS = PlimsAAdcDataParticle
PARTICLE_TYPE = DataParticleType.PLIMS_A_ADC_TELEMETERED_PARTICLE_TYPE


class PlimsAAdclUnitTestCase(ParserUnitTestCase):
    """
    plims_a_adc Parser unit test suite
    """

    def create_plims_a_parser(self, file_handle):
        log.debug("Create plims a ADC parser")
        return PlimsAAdcParser(self.config, file_handle, self.rec_exception_callback)

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
        log.debug("\n\nPLIMS A ADC SETUP\n\n")
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS
        }

        self.exception_callback_value = []
        self.exceptions_detected = 0

    def test_plims_a_adc_parser(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST PLIMS_A_ADC Parser =====')
        self.setup()

        with open(self.file_path('D20231021T175900_IFCB199.adc')) as in_file:
            parser = self.create_plims_a_parser(in_file)

            # In a single read, get all particles in this file.
            results = parser.get_records(2)
            rest_of_results = parser.get_records(442)

        self.assertEqual(len(results), 2)
        self.assertEqual(len(rest_of_results), 442)
        self.assert_particles(results, "D20231021T175900_IFCB199_adc_check.yml", RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST PLIMS_A_ADC Parser  =====')

    def test_plims_a_adc_parser_types(self):
        """
        Read a file and pull out a data particle.
        Verify that the results have the expected types.
        """
        # log.debug('===== START TEST PLIMS_A_ADC Parser Types =====')
        self.setup()

        with open(self.file_path('D20231021T175900_IFCB199.adc')) as in_file:
            parser = self.create_plims_a_parser(in_file)

            # In a single read, get all particles in this file.
            results = parser.get_records(444)

        self.assertEqual(len(results), 444)

        self.assertListEqual(self.exception_callback_value, [])

        # Check for appropriate type conversion
        for result in results:
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.TRIGGER_NUMBER)), np.integer))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.GRAB_TIME_END)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.TIME_OF_FLIGHT)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.PMTA)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.PMTB)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.INHIBIT_TIME)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.PEAK_A)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.PEAK_B)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.ROI_X)), np.integer))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.ROI_Y)), np.integer))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.ROI_WIDTH)), np.integer))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.ROI_HEIGHT)), np.integer))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.STATUS)), np.integer))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.START_BYTE)), np.integer))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.RUN_TIME)), np.float))
            self.assertTrue(np.issubdtype(type(result.get_value_from_values(PlimsAAdcParticleKey.INHIBIT_TIME)), np.float))
            
            # log.debug('===== END TEST PLIMS_A_ADC Parser Types  =====')


    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        log.debug('===== START TEST PLIMS_A_ADC BAD DATA  =====')
        self.setup()

        with open(self.file_path('D20231021T175900_IFCB199_incomplete.adc')) as in_file:
            parser = self.create_plims_a_parser(in_file)

            # In a single read, get all particles for this file.
            result = parser.get_records()

            self.assertEqual(len(result), 0)
            self.assertEqual(len(self.exception_callback_value), 1)

            for i in range(len(self.exception_callback_value)):
                log.debug('Exception: %s', self.exception_callback_value[i])

        log.debug('===== END TEST BAD DATA  =====')
