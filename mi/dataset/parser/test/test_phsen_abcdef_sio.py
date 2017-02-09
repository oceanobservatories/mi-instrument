#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_phsen_abcdef_sio.py
@author Emily Hahn
@brief Test code for a mflm Phsen abcdef sio data parser
"""
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import SampleException, UnexpectedDataException, RecoverableSampleException
from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.phsen_abcdef_sio import PhsenAbcdefSioParser

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'phsen_abcdef', 'sio', 'resource')

@attr('UNIT', group='mi')
class PhsenAbcdefSioParserUnitTestCase(ParserUnitTestCase):
    """
    Phsen Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['PhsenAbcdefSioDataParticle',
                                                     'PhsenAbcdefSioControlDataParticle']
        }

    def test_simple(self):
        """
        Read test data and assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.phsen.dat')) as stream_handle:

            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            result = parser.get_records(2)
            self.assert_particles(result, 'node59p1_1.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_get_many(self):
        """
        Read test data and pull out particles in different multiple calls to get_records.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'node59p1_2.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # 4 records in this file, get in 2 chunks
            result = parser.get_records(2)
            self.assertEqual(len(result), 2)
            result2 = parser.get_records(2)
            self.assertEqual(len(result2), 2)
            result.extend(result2)
            # make sure we don't get any more records
            result3 = parser.get_records(2)
            self.assertEqual(len(result3), 0)

            self.assert_particles(result, 'node59p1_2.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_long(self):
        """
        Test with the full file and make sure we get right number of records
        """

        with open(os.path.join(RESOURCE_PATH, 'node59p1_0.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # get all records in this file, contains 17 but one is corrupted
            result = parser.get_records(17)
            self.assertEqual(len(result), 16)

            # make sure we don't get any more records
            result2 = parser.get_records(2)
            self.assertEqual(len(result2), 0)

            # assert there was 1 exception, one sample was corrupted
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

    def test_control(self):
        """
        Test with a file with a control record, the IDD data file did not have any control records, the
        old format of data file had one though which was used to create a new file containing this record.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_control.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # get all records in this file
            result = parser.get_records(2)
            self.assertEqual(len(result), 2)

            self.assert_particles(result, 'node59p1_control.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_unexpected_id(self):
        """
        Test that an unexpected sio header id generates an exception in the callback
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_unexpected_id.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # get all records in this file
            result = parser.get_records(2)
            self.assertEqual(len(result), 2)

            self.assert_particles(result, 'node59p1_control.yml', RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 2)
            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))
            self.assert_(isinstance(self.exception_callback_value[1], UnexpectedDataException))

    def test_replaced_ref(self):
        """
        Test that a file with the reference light measurements with non-hex ascii chars succeeds but produced a
        null value there
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_replaced_ref.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # get all records in this file, there is only 1
            result = parser.get_records(2)
            self.assertEqual(len(result), 1)

            self.assert_particles(result, 'node59p1_repl_ref.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_replaced_light(self):
        """
        Test that a file with the light measurements with non-hex ascii chars succeeds but produced a
        null value there
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_replaced_lgt.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # get all records in this file, there is only 1
            result = parser.get_records(2)
            self.assertEqual(len(result), 1)

            self.assert_particles(result, 'node59p1_repl_lgt.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_short_control_record(self):
        """
        Test with a record that is too short
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_short_rec.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # get all records in this file, the only 1 is too short
            result = parser.get_records(2)
            self.assertEqual(len(result), 1)

            # first exception for short record, this messes up finding the second record in the same sio block and
            # gives a second unknown data exception
            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
            self.assertIsInstance(self.exception_callback_value[1], SampleException)

    def test_bad_checksum(self):
        """
        Test that a file with the bad checksum
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_bad_checksum.phsen.dat')) as stream_handle:
            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            # get all records in this file, there is only 1
            result = parser.get_records(2)
            self.assertEqual(len(result), 1)

            self.assert_particles(result, 'node59p1_bad_checksum.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_unexpected_data(self):
        """
        Test with a file that contains unexpected data
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_unexpected.phsen.dat')) as stream_handle:

            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            result = parser.get_records(2)
            result2 = parser.get_records(1)
            self.assertEqual(len(result2), 0)

            self.assert_particles(result, 'node59p1_1.yml', RESOURCE_PATH)

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], UnexpectedDataException)

    def test_control_fixed(self):
        """
        The only real control record is corrupted, make one that doesn't have non-ascii hex in it and confirm
        it is parsed correctly
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_control_fixed.phsen.dat')) as stream_handle:

            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            result = parser.get_records(2)
            self.assert_particles(result, 'node59p1_control_fixed.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_control_battery(self):
        """
        Make a up a battery record to test with and confirm it is parsed correctly
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_control_batt.phsen.dat')) as stream_handle:

            parser = PhsenAbcdefSioParser(self.config, stream_handle, self.exception_callback)

            result = parser.get_records(2)
            self.assert_particles(result, 'node59p1_control_batt.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])