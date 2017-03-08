#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_cg_cpm_eng_cpm
@file mi/dataset/parser/test/test_cg_cpm_eng_cpm.py
@author Mark Worden
@brief Test code for a cg_cpm_eng_cpm data parser
"""
import os
import pprint

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_cpm_eng.cpm.resource import RESOURCE_PATH
from mi.dataset.parser.cg_cpm_eng_cpm import CgCpmEngCpmParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class CgParserUnitTestCase(ParserUnitTestCase):
    """
    Cg_stc_eng_stc Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_cpm_eng_cpm',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CgCpmEngCpmRecoveredDataParticle'
        }

        self._exceptions_detected = 0

    def exception_callback(self, exception):
        log.debug("Exception received: %s", exception)
        self._exceptions_detected += 1

    def test_happy_path(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST HAPPY PATH =====')

        with open(os.path.join(RESOURCE_PATH, 'cpm_status.20140817_1255.txt')) as file_handle:
            self.parser = CgCpmEngCpmParser(self.config, file_handle,
                                            self.exception_callback)

            result = self.parser.get_records(1)

            log.debug("Result: %s", pprint.pformat(result[0].generate_dict()))

            self.assertEqual(self._exceptions_detected, 0)

            self.assert_particles(result, 'cpm_status.20140817_1255.yml', RESOURCE_PATH)

        log.debug('===== END TEST HAPPY PATH =====')

    def test_invalid_fields(self):
        """
        The file used in this test has errors.
        """
        log.debug('===== START TEST INVALID FIELDS =====')

        with open(os.path.join(RESOURCE_PATH, 'cpm_status.invalid_inputs.txt')) as file_handle:
            self.parser = CgCpmEngCpmParser(self.config, file_handle,
                                            self.exception_callback)

            """
            Expected invalid lines:
            STATUS.last_err.C_PS=***Warning, PPS error message
            MPIC.main_v=3110
            MPIC.hotel=wake 2 ir 0 1.6 4.9 0 fwf 3 11.9 305.5 0 gps 1 sbd 0 0 pps 0 dcl 08 esw 1 dsl 1
            MPIC.stc_flag2=00000000
            Pwrsys.pv4=0 000 0.00
            Sched.cpm.wake=started 10,3,6,9,12,15,18,21:53:33   Remaining: 1857 sec
            sbc.bd=0x6c88
            """

            result = self.parser.get_records(1)

            log.debug("Result: %s", result)

            self.assertEqual(self._exceptions_detected, 7)

            self.assert_particles(result, 'cpm_status.invalid_inputs.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID FIELDS =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with self.assertRaises(SampleException):
            with open(os.path.join(RESOURCE_PATH, 'cpm_status.no_particles.txt')) as file_handle:
                self.parser = CgCpmEngCpmParser(self.config, file_handle,
                                                self.exception_callback)
                result = self.parser.get_records(1)
                self.assertTrue(len(result) == 0)

        log.debug('===== END TEST NO PARTICLES =====')
