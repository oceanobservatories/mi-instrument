#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcpt_acfgm
@fid marine-integrations/mi/dataset/parser/test/test_adcpt_acfgm.py
@author Ronald Ronquillo
@brief Test code for a Adcpt_Acfgm_Dcl data parser
"""

from nose.plugins.attrib import attr
import os

from mi.core.log import get_logger
from mi.dataset.parser.utilities import particle_to_yml

log = get_logger()

from mi.core.exceptions import RecoverableSampleException
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.adcpt_acfgm.dcl.pd8.adcpt_acfgm_dcl_pd8_driver_common import \
    AdcptAcfgmPd8Parser, MODULE_NAME, ADCPT_ACFGM_DCL_PD8_RECOVERED_PARTICLE_CLASS, \
    ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS

from mi.dataset.driver.adcpt_acfgm.dcl.pd8.resource import RESOURCE_PATH


@attr('UNIT', group='mi')
class AdcptAcfgmPd8ParserUnitTestCase(ParserUnitTestCase):
    """
    Adcpt_Acfgm_Dcl Parser unit test suite
    """

    def create_parser(self, particle_class, file_handle):
        """
        This function creates a AdcptAcfgmDcl parser for recovered data.
        """
        parser = AdcptAcfgmPd8Parser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            file_handle,
            self.exception_callback)
        return parser

    def open_file(self, filename):
        my_file = open(os.path.join(RESOURCE_PATH, filename), mode='rU')
        return my_file

    def file_path(self, filename):
        log.debug('resource path = %s, file name = %s', RESOURCE_PATH, filename)
        return os.path.join(RESOURCE_PATH, filename)

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    def test_parse_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done in the
        tests below.
        """
        in_file = self.open_file('20131201.adcp_mod.log')
        parser = self.create_parser(ADCPT_ACFGM_DCL_PD8_RECOVERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(23)
        self.assertEqual(len(result), 23)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

    def test_recov(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.
        """
        in_file = self.open_file('20131201.adcp_mod.log')
        parser = self.create_parser(ADCPT_ACFGM_DCL_PD8_RECOVERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(23)

        # creating .yml file
        out_file = '20131201.adcp_mod_recov.yml'
        particle_to_yml(result, self.file_path(out_file))

        self.assertEqual(len(result), 23)
        self.assert_particles(result, '20131201.adcp_mod_recov.yml', RESOURCE_PATH)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()

    def test_telem(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.
        """
        in_file = self.open_file('20131201.adcp_mod.log')
        parser = self.create_parser(ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(23)

        self.assertEqual(len(result), 23)
        self.assert_particles(result, '20131201.adcp_mod.yml', RESOURCE_PATH)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # Line 1: DCL Log missing opening square bracket
        # Line 40: Timestamp day has a float
        # Line 79: Heading is not a float
        # Line 118: Temp is not a float
        # Line 119: Header typo
        # Line 158: Timestamp has non digit
        # Line 197: Timestamp missing milliseconds
        # Line 234: Bin missing
        # Line 272: Dir missing
        # Line 310: Mag missing
        # Line 348: E/W missing
        # Line 386: N/S missing
        # Line 424: Vert missing
        # Line 462: Err missing
        # Line 500: Echo1 missing
        # Line 538: Echo2 missing
        # Line 576: Echo3 missing
        # Line 614: Echo4 missing
        # Line 652: Dir is not a float
        # Line 690: Dir has a non digit
        # Line 728: Mag is not a float
        # Line 766: Mag has a non digit
        # Line 804: E/W is a float
        # Line 842: E/W has a non digit
        # Line 880: N/S is a float
        # Line 918: N/S is a non digit
        # Line 956: Vert is a float
        # Line 994: Vert is a non digit
        # Line 1032: Err is a float
        # Line 1070: Err has a non digit
        # Line 1108: Echo1 is a float
        # Line 1146: Echo1 has a non digit
        # Line 1184: Echo2 is a float
        # Line 1222: Echo2 has a non digit
        # Line 1260: Echo3 is negative
        # Line 1298: Timestamp missing secconds
        # Line 1331: DCL Logging missing closing square bracket
        # Line 1384: Ensemble number is a float
        # Line 1409: Pitch is not a float
        # Line 1448: Speed of sound is a float
        # Line 1485: Roll is not a float
        # Line 1523: Heading has a non digit
        # Line 1561: Pitch has a non digit
        # Line 1599: Roll has a non digit

        fid = open(os.path.join(RESOURCE_PATH, '20131201.adcp_corrupt.log'), 'rb')

        parser = self.create_parser(ADCPT_ACFGM_DCL_PD8_RECOVERED_PARTICLE_CLASS, fid)

        parser.get_records(66)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
            log.debug('Exception: %s', self.exception_callback_value[i])

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        fid.close()

    def test_telem_3021(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.

        This test uses a real file from a deployment.
        Used to verify fixes in responses to Redmine # 3021
        """
        in_file = self.open_file('20141208.adcp.log')
        parser = self.create_parser(ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(23)

        self.assertEqual(len(result), 14)
        self.assert_particles(result, '20141208.adcp.yml', RESOURCE_PATH)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()

    def test_telem_9692(self):
        """
        Test to verify change made to dcl_file_common.py works with DCL
        timestamps containing seconds >59
        """
        in_file = self.open_file('20131201.adcpA.log')
        parser = self.create_parser(ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(20)

        self.assertEqual(len(result), 1)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()

