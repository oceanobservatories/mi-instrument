"""
@package mi.dataset.parser.test
@file /mi/dataset/parser/testvel_/test3d_cd_dcl.py
@author Emily Hahn
@brief Test for the parser for the vel3d instrument series c,d through dcl dataset driver
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import os
from nose.plugins.attrib import attr
import re

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import UnexpectedDataException, SampleException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.parser.vel3d_cd_dcl import Vel3dCdDclParser
from mi.dataset.parser.common_regexes import FLOAT_REGEX, END_OF_LINE_REGEX

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'vel3d_cd', 'dcl', 'resource')


@attr('UNIT', group='mi')
class Vel3dCdDclParserUnitTestCase(ParserUnitTestCase):

    def test_simple_no_ts_telem(self):
        """
        Test a simple telemetered case without timestamps
        """
        with open(os.path.join(RESOURCE_PATH, 'first_no_timestamp.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            # we should only get 2 particles back, this ensures we skip the ignored record
            particles = parser.get_records(3)

            self.assert_particles(particles, "first_no_timestamp_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_simple_velocities_telem(self):
        """
        Test a simple set of system and velocity records for telemetered
        """
        with open(os.path.join(RESOURCE_PATH, 'no_timestamp_vel_telem.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(10)

            self.assert_particles(particles, "no_timestamp_vel_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_with_ts_telem(self):
        """
        Test a telemetered case with ascii timestamps
        """
        with open(os.path.join(RESOURCE_PATH, 'first_with_timestamp.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(12)

            self.assert_particles(particles, "first_with_timestamp_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_with_ts_recov(self):
        """
        Test a recovered case with ascii timestamps
        """
        with open(os.path.join(RESOURCE_PATH, 'first_with_timestamp.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(12)

            self.assert_particles(particles, "first_with_timestamp_recov.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_config_telem(self):
        """
        Test the 3 config record types.  Data is taken from velpt_ab, which has the same format records, since none
        were available in test files provided.
        """
        with open(os.path.join(RESOURCE_PATH, 'config_only.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(3)

            self.assert_particles(particles, "config_only_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_config_recov(self):
        """
        Test the 3 config record types.  Data is taken from velpt_ab, which has the same format records, since none
        were available in test files provided.
        """
        with open(os.path.join(RESOURCE_PATH, 'config_only.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(3)

            self.assert_particles(particles, "config_only_recov.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_no_ts_full_telem(self):
        """
        Test with the full no timestamp file, confirming we get the correct number of particles without exceptions
        """
        with open(os.path.join(RESOURCE_PATH, '20141007.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            # request more particles than are available
            particles = parser.get_records(1700)
            # confirm the correct number is received, expect 181 system records, 1440 velocity records, 1 data header
            self.assertEquals(len(particles), 1622)

            self.assertEqual(self.exception_callback_value, [])

    def test_with_ts_full_telem(self):
        """
        Test with the full timestamp file, confirming we get the correct number of particles without exceptions
        """
        with open(os.path.join(RESOURCE_PATH, '20141011.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            # request more particles than are available
            particles = parser.get_records(1700)
            # confirm the correct number is received, expect 181 system records, 1440 velocity records, 1 data header
            self.assertEquals(len(particles), 1622)

            self.assertEqual(self.exception_callback_value, [])

    def test_unexpected_data(self):
        """
        Test that unexpected data inserted between records causes an exception in the callback.  There is also
        an incomplete record at the end, which should cause another exception.
        """
        with open(os.path.join(RESOURCE_PATH, 'unexpected.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            # we should only get 2 particles back, this ensures we skip the ignored record
            particles = parser.get_records(3)

            self.assert_particles(particles, "first_no_timestamp_telem.yml", RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 2)
            # first exception due to unexpected data inserted before first system record
            self.assertIsInstance(self.exception_callback_value[0], UnexpectedDataException)
            # second exception from incomplete system record at end
            self.assertIsInstance(self.exception_callback_value[1], SampleException)

    def test_bad_checksum(self):
        """
        Test that a record with a bad checksum is skipped and raises an exception, and the file is continued
        to be parsed
        """
        with open(os.path.join(RESOURCE_PATH, 'bad_checksum.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(3)
            # we should only get 1 particles back
            self.assertEquals(len(particles), 1)

            self.assertEqual(len(self.exception_callback_value), 1)
            # first exception due to unexpected data inserted before first system record
            self.assertIsInstance(self.exception_callback_value[0], SampleException)

    def test_particle_velocity_group(self):
        """
        Test a case where there has been one full velocity record group, followed by an incomplete group.  Confirm
        timestamps are calculated correctly
        """
        with open(os.path.join(RESOURCE_PATH, 'partial_vel_group.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(15)

            self.assert_particles(particles, "partial_vel_group_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_partial_velocity_group_no_end(self):
        """
        Test a case where there is a partial velocity group without a terminating system record, and no previous
        velocity groups.  In this case the timestamp cannot be calculated and should cause an exception.
        """
        with open(os.path.join(RESOURCE_PATH, 'partial_vel_group_no_end.vel3d.log'), 'rb') as file_handle:
            parser = Vel3dCdDclParser(file_handle, self.exception_callback, is_telemetered=True)

            # ask for one more than expected
            particles = parser.get_records(4)
            # should get data header and two system records, but not create partial velocity group
            self.assertEquals(len(particles), 3)

            self.assertEqual(len(self.exception_callback_value), 1)
            # Exception from not being able to calculate timestamps
            self.assertIsInstance(self.exception_callback_value[0], SampleException)

    def fix_yml_float_params(self):
        """
        This helper tool was used to modify the yml files in response to ticket #8564
        """

        param_change_table = [
            ('battery_voltage', 'battery_voltage_dV', 10),
            ('speed_of_sound', 'sound_speed_dms', 10),
            ('heading', 'heading_decidegree', 10),
            ('pitch', 'pitch_decidegree', 10),
            ('roll', 'roll_decidegree', 10),
            ('seawater_pressure', 'seawater_pressure_mbar', 1000),
            ('temperature', 'temperature_centidegree', 100)
        ]

        for file_name in os.listdir(RESOURCE_PATH):

            if file_name.endswith('.yml'):

                with open(os.path.join(RESOURCE_PATH, file_name), 'rU') as in_file_id:

                    out_file_name = file_name + '.new'
                    log.info('fixing file %s', file_name)
                    log.info('creating file %s', out_file_name)

                    out_file_id = open(os.path.join(RESOURCE_PATH, out_file_name), 'w')

                    for line in in_file_id:
                        new_line = line

                        for param_name, new_name, mult in param_change_table:

                            param_regex = r'\s+' + param_name + r':\s+(' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX
                            match = re.match(param_regex, line)
                            if match is not None:
                                new_value = int(float(match.group(1)) * mult)
                                new_line = '    ' + new_name + ':  ' + str(new_value) + '\n'
                                log.info('%s', new_line)

                        out_file_id.write(new_line)

                    out_file_id.close()
