"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_phsen_abcdef.py
@author Joe Padula
@brief Test code for a Phsen_abcdef data parser
"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger

from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.phsen_abcdef import PhsenRecoveredParser
from mi.core.exceptions import SampleException

log = get_logger()

__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'phsen_abcdef', 'resource')


@attr('UNIT', group='mi')
class PhsenRecoveredParserUnitTestCase(ParserUnitTestCase):
    """
    Phsen ABCDEF Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """

        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'SAMI_P0080_180713_simple.txt'))

        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        particles = parser.get_records(20)

        self.assert_particles(particles, "SAMI_P0080_180713_simple.yml", RESOURCE_PATH)

        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_multiple.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        result = parser.get_records(3)

        self.assert_particles(result, 'SAMI_P0080_180713_multiple.yml', RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])

        stream_handle.close()

    def test_long(self):
        """
        Test with the full original file
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_orig.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)
        # request more particles than available, only 29 in file
        result = parser.get_records(32)
        self.assertEquals(len(result), 29)
        self.assertEquals(self.exception_callback_value, [])
        stream_handle.close()

    def test_invalid_num_fields_ph(self):
        """
        Test that the ph records have correct number of fields.
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_invalid_ph.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_invalid_num_fields_control(self):
        """
        Test that the generic control records have correct number of fields
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_invalid_control.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_invalid_num_fields_special_control(self):
        """
        Test that the special control records have correct number of fields
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_invalid_special_control.txt'))
        parser = PhsenRecoveredParser(self.config,  stream_handle, self.exception_callback)

        parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_recover(self):
        """
        Test that we can recover after receiving bad record
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_recover.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)
        parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_unknown_msg_type(self):
        """
        Test that we handle unsupported msg type
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_unknown_msg_type.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_alpha_type(self):
        """
        Test that we handle alpha msg type
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_alpha_type.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_alpha_field(self):
        """
        Test that we handle an alpha field
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_alpha_field.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_no_data_tag(self):
        """
        Test that we do not create a particle if the file does not contain the ':Data' tag
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_no_data_tag.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        result = parser.get_records(1)
        self.assertEqual(result, [])

        stream_handle.close()

    def test_integration_control(self):
        """
        Test with the integration control file
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_integration_control_ph.txt'))
        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        result = parser.get_records(12)
        self.assert_particles(result, "SAMI_P0080_180713_control_ph.yml", RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])

        stream_handle.close()

    def test_bug_3608(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """

        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'SAMI_P0080_160614_2.txt'))

        parser = PhsenRecoveredParser(self.config, stream_handle, self.exception_callback)

        particles = parser.get_records(5000)

        self.assertEqual(len(particles), 323)
        self.assertTrue(len(self.exception_callback_value) > 0)
