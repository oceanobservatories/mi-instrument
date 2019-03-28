#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_pco2a_a_sample
@file marine-integrations/mi/dataset/parser/test/test_pco2a_a_sample.py
@author Tim Fisher
@brief Test code for a pco2a_a_sample data parser

Files used for testing:

20140217.pco2a.log
  Sensor Data - 216 records

20140217.pco2a_failure.log
  Sensor Data - ??? records

"""

import os
from nose.plugins.attrib import attr

from mi.core.log import log
from mi.core.exceptions import UnexpectedDataException
from mi.dataset.parser.utilities import particle_to_yml

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.pco2a_a.sample.resource import RESOURCE_PATH
from mi.dataset.parser.pco2a_a_sample import Pco2aADclParser
from mi.dataset.parser.pco2a_a_sample import SENSOR_DATA_MATCHER_WATER
from mi.dataset.driver.pco2a_a.sample.pco2a_a_sample_driver import \
     MODULE_NAME, PARTICLE_CLASSES

FILE = '20140217.pco2a.log'
FILE_FAILURE = '20140217.pco2a_failure.log'

YAML_FILE = 'rec_20140217_pco2a.yml'

RECORDS = 216  # number of records expected


@attr('UNIT', group='mi')
class Pco2aADclParserUnitTestCase(ParserUnitTestCase):
    """
    pco2a_a_dcl Parser unit test suite
    """

    def create_parser(self, particle_classes, file_handle):
        """
        This function creates a Pco2aADcl parser.
        """
        parser = Pco2aADclParser(
                {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                 DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                 DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: particle_classes},
                file_handle,
                self.exception_callback)
        return parser

    def open_file(self, filename):
        my_file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return my_file

    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    # There should be 20 groups where the last group is '11.7'
    def test_sensor_water_pattern(self):
        data = "2019/02/27 00:00:09.290 W M,2019,02,27,00,00,11,44027,41190,446.74,40.60,11.37,28.52,1021,40.30,40.90,11.7\r\n"
        m = SENSOR_DATA_MATCHER_WATER.match(data)
        self.assertEqual(20,m.re.groups)
        self.assertEqual('11.7',m.group(m.re.groups))

    def test_verify_record(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are there.
        """
        log.debug('===== START TEST verify record parser =====')
        in_file = self.open_file(FILE)
        parser = self.create_parser(PARTICLE_CLASSES, in_file)

        parser.get_records(RECORDS)
        self.assertListEqual(self.exception_callback_value, [])

        record = parser.get_records(1)
        self.assertNotEqual(record, None)

        in_file.close()
        log.debug('===== END TEST verify record parser =====')

    def test_verify_parser(self):
        """
        Read Telemetered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains metadata entries.
        This test includes testing metadata entries as well
        """
        log.debug('===== START TEST verify parser =====')
        in_file = self.open_file(FILE)
        parser = self.create_parser(PARTICLE_CLASSES, in_file)
        result = parser.get_records(RECORDS)

        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== END TEST verify parser =====')

    def test_verify_record_recovered(self):
        """
        Read recovered data from a file and pull out data particles
        one at a time. Verify that the results are there.
        """
        log.debug('===== START TEST verify_parser RECOVERED =====')
        in_file = self.open_file(FILE)
        parser = self.create_parser(PARTICLE_CLASSES, in_file)
        parser.get_records(RECORDS)

        self.assertListEqual(self.exception_callback_value, [])

        record = parser.get_records(1)
        self.assertNotEqual(record, None)
        in_file.close()
        log.debug('===== END TEST verify_parser RECOVERED =====')

    def test_verify_parser_recovered(self):
        """
        Read recovered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains metadata entries.
        This test includes testing metadata entries as well
        """
        log.debug('===== START TEST verify_parser RECOVERED =====')
        in_file = self.open_file(FILE)
        parser = self.create_parser(PARTICLE_CLASSES, in_file)
        result = parser.get_records(RECORDS)

        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== END TEST verify_parser RECOVERED =====')

    def test_verify_parser_failure(self):
        """
        Read telemetered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains invalid entries.
        This test includes testing invalid entries as well
        """

        # Line 1: dcl_controller_timestamp: missing milliseconds.
        # Line 3: dcl_controller_timestamp: has non digit.
        # Line 5: date_time_string: missing #.
        # Line 7: date_time_string: has non digit.
        # Line 9: Missing M.
        # Line 11: metadata: missing closing bracket.
        # Line 13: M is a digit.
        # Line 15: zero_a2d: is a float.
        # Line 17: zero_a2d: has a not digit
        # Line 19: current_a2d: is a float.
        # Line 21: current_a2d: has a not digit
        # Line 23: measured_air_co2: is an int
        # Line 25: measured_air_co3: has a not digit
        # Line 27: avg_irga_temperature: is an int
        # Line 29: avg_irga_temperature: has a not digit
        # Line 31: humidity: is an int
        # Line 33: humidity: has a not digit
        # Line 35: humidity_temperature: is an int
        # Line 37: humidity_temperature: has a not digit
        # Line 39: gas_stream_pressure: is a float.
        # Line 41: gas_stream_pressure: has a not digit
        # Line 43: irga_detector_temperature: is an int
        # Line 45: irga_detector_temperature: has a not digit
        # Line 47: irga_source_temperature: is an int
        # Line 49: irga_source_temperature: has a not digit
        # Line 51: sensor type: missing sensor type
        # Line 53: sensor type: is a digit
        # Line 55: sensor type: is not A or W
        # Line 57: dcl_controller_timestamp: missing space
        # Line 59: line has space instead of comma separator.
        # Line 61: current_a2d: missing field
        # Line 63: irga_source_temperature: field doubled
        # Line 65: record ends with space then line feed  -- NO LONGER AN ERROR, bug_9989
        # Line 67: line is just a line feed
        # Line 69: suspect timestamp, followed by all hex ascii chars
        # Line 71: line has a tab instead of a comma separator.
        # Line 73: line has tab before line feed.  -- NO LONGER AN ERROR, bug_9989

        log.debug('===== START TEST failure verify_parser =====')
        in_file = self.open_file(FILE_FAILURE)
        parser = self.create_parser(PARTICLE_CLASSES, in_file)
        parser.get_records(RECORDS)

        self.assertTrue(self.exception_callback_value is not None)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], UnexpectedDataException))

        # bad records
        self.assertEqual(len(self.exception_callback_value), 5)

        in_file.close()
        log.debug('===== END TEST failure verify_parser =====')

    def test_verify_parser_failure_recovered(self):
        """
        Read recovered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains invalid entries.
        This test includes testing invalid entries as well
        """
        # Same errors as used above.

        log.debug('===== START TEST failure verify_parser RECOVERED =====')
        in_file = self.open_file(FILE_FAILURE)
        parser = self.create_parser(PARTICLE_CLASSES, in_file)
        parser.get_records(RECORDS)

        self.assertTrue(self.exception_callback_value is not None)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], UnexpectedDataException))

        # bad records
        self.assertEqual(len(self.exception_callback_value), 5)

        in_file.close()
        log.debug('===== END TEST failure verify_parser RECOVERED =====')

    def test_bug_9692(self):
        """
        Test to verify change made to dcl_file_common.py works with DCL
        timestamps containing seconds >59
        """
        in_file = self.open_file('20140217.pco2aA.log')
        parser = self.create_parser(PARTICLE_CLASSES, in_file)
        result = parser.get_records(10)

        self.assertEqual(len(result), 4)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()

    def test_bug_9989(self):
        """
        Test to verify the parser will accept records ending in <CR><CR><LF>
        These were found in files on the OMC
        """
        in_file = self.open_file('20150302.pco2a.log')
        parser = self.create_parser(PARTICLE_CLASSES, in_file)
        result = parser.get_records(500)

        self.assertEqual(len(result), 432)
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()
