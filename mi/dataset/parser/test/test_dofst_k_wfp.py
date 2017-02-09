#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_dofst_k_wfp
@file marine-integrations/mi/dataset/parser/test/test_dofst_k_wfp.py
@author Emily Hahn
@brief Test code for a dofst_k_wfp data parser
"""
import os
import struct
from StringIO import StringIO

import ntplib
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.dofst_k.wfp.resource import RESOURCE_PATH
from mi.dataset.parser.dofst_k_wfp import DofstKWfpParser
from mi.dataset.parser.dofst_k_wfp_particles import DofstKWfpRecoveredDataParticle
from mi.dataset.parser.dofst_k_wfp_particles import DofstKWfpRecoveredMetadataParticle
from mi.dataset.parser.dofst_k_wfp_particles import DofstKWfpTelemeteredDataParticle
from mi.dataset.parser.dofst_k_wfp_particles import DofstKWfpTelemeteredMetadataParticle
from mi.dataset.parser.wfp_c_file_common import StateKey
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class DofstKWfpParserUnitTestCase(ParserUnitTestCase):
    TEST_DATA = b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8\x00\x1a\x8c\x03\xe2" + \
                "\xc0\x00\x03\xeb\x0a\x81\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65\xff\xff" + \
                "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x52\x4e\x75\x82\x52\x4e\x76\x9a"

    TEST_DATA_PAD = b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8\x00\x1a\x8c\x03\xe2" + \
                    "\xc0\x00\x03\xeb\x0a\x81\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65\xff\xff" + \
                    "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x52\x4e\x75\x82\x52\x4e\x76\x9a\x0a"

    # not enough bytes for final timestamps
    TEST_DATA_BAD_TIME = b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8\x00\x1a\x8c\x03\xe2" + \
                         "\xc0\x00\x03\xeb\x0a\x81\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65\xff\xff" + \
                         "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x52\x4e\x75\x82\x52\x4e"

    TEST_DATA_BAD_SIZE = b"\x00\x1a\x88\x03\xe3\x3b\xc8\x00\x1a\x8c\x03\xe2" + \
                         "\xc0\x00\x03\xeb\x0a\x81\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65\xff\xff" + \
                         "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x52\x4e\x75\x82\x52\x4e\x76\x9a"

    TEST_DATA_BAD_EOP = b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8\x00\x1a\x8c\x03\xe2" + \
                        "\xc0\x00\x03\xeb\x0a\x81\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65" + \
                        "\xff\xff\xff\xff\x52\x4e\x75\x82\x52\x4e\x76\x9a"

    """
    dofst_k_wfp Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        self.exception_callback_value = exception

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_recovered = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dofst_k_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'instrument_data_particle_class': DofstKWfpRecoveredDataParticle,
                'metadata_particle_class': DofstKWfpRecoveredMetadataParticle
            }
        }

        self.config_telemetered = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dofst_k_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'instrument_data_particle_class': DofstKWfpTelemeteredDataParticle,
                'metadata_particle_class': DofstKWfpTelemeteredMetadataParticle
            }
        }

        self.recovered_start_state = {StateKey.POSITION: 0,
                                      StateKey.RECORDS_READ: 0,
                                      StateKey.METADATA_SENT: False}

        self.telemetered_start_state = {StateKey.POSITION: 0,
                                        StateKey.RECORDS_READ: 0,
                                        StateKey.METADATA_SENT: False}

        # Initialize this for later use.
        self.parser = None

        # Define test data particles and their associated timestamps which will be
        # compared with returned results
        timefields = struct.unpack('>II', '\x52\x4e\x75\x82\x52\x4e\x76\x9a')
        start_time = int(timefields[0])
        end_time = int(timefields[1])
        # even though there are only 3 samples in TEST_DATA, there are 270 samples in the original file,
        # so this needs to be used to determine the time increment for each time sample
        time_increment_3 = float(end_time - start_time) / 3.0
        time_increment_270 = float(end_time - start_time) / 270.0

        self.start_timestamp = self.calc_timestamp(start_time, time_increment_3, 0)
        self.start_timestamp_long = self.calc_timestamp(start_time, time_increment_270, 0)
        self.timestamp_2 = self.calc_timestamp(start_time, time_increment_3, 1)
        self.timestamp_2_long = self.calc_timestamp(start_time, time_increment_270, 1)
        timestamp_3 = self.calc_timestamp(start_time, time_increment_3, 2)
        timestamp_last = self.calc_timestamp(start_time, time_increment_270, 269)

        RMP = DofstKWfpRecoveredMetadataParticle
        RDP = DofstKWfpRecoveredDataParticle
        TMP = DofstKWfpTelemeteredMetadataParticle
        TDP = DofstKWfpTelemeteredDataParticle

        self.recov_particle_meta = RMP((b"\x52\x4e\x75\x82\x52\x4e\x76\x9a", 3.0),
                                       internal_timestamp=self.start_timestamp)
        self.recov_particle_meta_long = RMP((b"\x52\x4e\x75\x82\x52\x4e\x76\x9a", 270.0),
                                            internal_timestamp=self.start_timestamp_long)
        self.recov_particle_a = RDP(b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8",
                                    internal_timestamp=self.start_timestamp)
        self.recov_particle_a_long = RDP(b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8",
                                         internal_timestamp=self.start_timestamp_long)
        self.recov_particle_b = RDP(b"\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81",
                                    internal_timestamp=self.timestamp_2)
        self.recov_particle_b_long = RDP(b"\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81",
                                         internal_timestamp=self.timestamp_2_long)
        self.recov_particle_c = RDP(b"\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65",
                                    internal_timestamp=timestamp_3)
        self.recov_particle_last = RDP(b"\x00\x1a\x8f\x03\xe5\x91\x00\x03\xeb\x0bS",
                                       internal_timestamp=timestamp_last)

        self.telem_particle_meta = TMP((b"\x52\x4e\x75\x82\x52\x4e\x76\x9a", 3.0),
                                       internal_timestamp=self.start_timestamp)
        self.telem_particle_meta_long = TMP((b"\x52\x4e\x75\x82\x52\x4e\x76\x9a", 270.0),
                                            internal_timestamp=self.start_timestamp_long)
        self.telem_particle_a = TDP(b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8",
                                    internal_timestamp=self.start_timestamp)
        self.telem_particle_a_long = TDP(b"\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8",
                                         internal_timestamp=self.start_timestamp_long)
        self.telem_particle_b = TDP(b"\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81",
                                    internal_timestamp=self.timestamp_2)
        self.telem_particle_b_long = TDP(b"\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81",
                                         internal_timestamp=self.timestamp_2_long)
        self.telem_particle_c = TDP(b"\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65",
                                    internal_timestamp=timestamp_3)
        self.telem_particle_last = TDP(b"\x00\x1a\x8f\x03\xe5\x91\x00\x03\xeb\x0bS",
                                       internal_timestamp=timestamp_last)

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None

    def calc_timestamp(self, start, increment, sample_idx):
        new_time = start + (increment*sample_idx)
        return float(ntplib.system_to_ntp_time(new_time))

    def assert_result(self, result, position, particle, ingested, rec_read, metadata_sent):
        self.assertEqual(result, [particle])
        self.assertEqual(self.file_ingested_value, ingested)

        self.assertEqual(self.parser._state[StateKey.POSITION], position)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], position)

        self.assertEqual(self.parser._state[StateKey.METADATA_SENT], metadata_sent)
        self.assertEqual(self.state_callback_value[StateKey.METADATA_SENT], metadata_sent)

        self.assertEqual(self.parser._state[StateKey.RECORDS_READ], rec_read)
        self.assertEqual(self.state_callback_value[StateKey.RECORDS_READ], rec_read)

        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], particle)

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        stream_handle = StringIO(DofstKWfpParserUnitTestCase.TEST_DATA)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        recovered_parser = DofstKWfpParser(
            self.config_recovered, self.recovered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback,
            len(DofstKWfpParserUnitTestCase.TEST_DATA))
        self.parser = recovered_parser

        # next get records
        recovered_result = recovered_parser.get_records(1)
        self.assert_result(recovered_result, 0, self.recov_particle_meta, False, 0, True)
        recovered_result = self.parser.get_records(1)
        self.assert_result(recovered_result, 11, self.recov_particle_a, False, 1, True)
        recovered_result = self.parser.get_records(1)
        self.assert_result(recovered_result, 22, self.recov_particle_b, False, 2, True)
        recovered_result = self.parser.get_records(1)
        self.assert_result(recovered_result, 33, self.recov_particle_c, True, 3, True)

        # no data left, dont move the position
        recovered_result = recovered_parser.get_records(1)
        self.assertEqual(recovered_result, [])
        self.assertEqual(recovered_parser._state[StateKey.POSITION], 33)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 33)
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], self.recov_particle_c)

        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        telemetered_parser = DofstKWfpParser(
            self.config_telemetered, self.telemetered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback,
            len(DofstKWfpParserUnitTestCase.TEST_DATA))
        self.parser = telemetered_parser

        # next get records
        telemetered_result = telemetered_parser.get_records(1)
        self.assert_result(telemetered_result, 0, self.telem_particle_meta, False, 0, True)
        telemetered_result = self.parser.get_records(1)
        self.assert_result(telemetered_result, 11, self.telem_particle_a, False, 1, True)
        telemetered_result = self.parser.get_records(1)
        self.assert_result(telemetered_result, 22, self.telem_particle_b, False, 2, True)
        telemetered_result = self.parser.get_records(1)
        self.assert_result(telemetered_result, 33, self.telem_particle_c, True, 3, True)

        # no data left, dont move the position
        telemetered_result = telemetered_parser.get_records(1)
        self.assertEqual(telemetered_result, [])
        self.assertEqual(telemetered_parser._state[StateKey.POSITION], 33)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 33)
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], self.telem_particle_c)

    def test_simple_pad(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        stream_handle = StringIO(DofstKWfpParserUnitTestCase.TEST_DATA_PAD)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        recovered_parser = DofstKWfpParser(
            self.config_recovered, self.recovered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback,
            len(DofstKWfpParserUnitTestCase.TEST_DATA_PAD))
        self.parser = recovered_parser

        # next get records
        recovered_result = recovered_parser.get_records(1)
        self.assert_result(recovered_result, 0, self.recov_particle_meta, False, 0, True)

        recovered_result = self.parser.get_records(1)
        self.assert_result(recovered_result, 11, self.recov_particle_a, False, 1, True)
        recovered_result = self.parser.get_records(1)
        self.assert_result(recovered_result, 22, self.recov_particle_b, False, 2, True)
        recovered_result = self.parser.get_records(1)
        self.assert_result(recovered_result, 33, self.recov_particle_c, True, 3, True)

        # no data left, dont move the position
        recovered_result = recovered_parser.get_records(1)
        self.assertEqual(recovered_result, [])
        self.assertEqual(recovered_parser._state[StateKey.POSITION], 33)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 33)
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], self.recov_particle_c)

        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        telemetered_parser = DofstKWfpParser(
            self.config_telemetered, self.telemetered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback,
            len(DofstKWfpParserUnitTestCase.TEST_DATA_PAD))
        self.parser = telemetered_parser

        # next get records
        telemetered_result = telemetered_parser.get_records(1)
        self.assert_result(telemetered_result, 0, self.telem_particle_meta, False, 0, True)
        telemetered_result = self.parser.get_records(1)
        self.assert_result(telemetered_result, 11, self.telem_particle_a, False, 1, True)
        telemetered_result = self.parser.get_records(1)
        self.assert_result(telemetered_result, 22, self.telem_particle_b, False, 2, True)
        telemetered_result = self.parser.get_records(1)
        self.assert_result(telemetered_result, 33, self.telem_particle_c, True, 3, True)

        # no data left, dont move the position
        telemetered_result = telemetered_parser.get_records(1)
        self.assertEqual(telemetered_result, [])
        self.assertEqual(telemetered_parser._state[StateKey.POSITION], 33)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 33)
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], self.telem_particle_c)

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        stream_handle = StringIO(DofstKWfpParserUnitTestCase.TEST_DATA)
        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        recovered_parser = DofstKWfpParser(
            self.config_recovered, self.recovered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback,
            len(DofstKWfpParserUnitTestCase.TEST_DATA))
        self.parser = recovered_parser
        # next get records
        # NOTE - while we ask for 4 records, the metadata is a special case
        # the number of records read is actually 3. See the IDD for details.
        recovered_result = recovered_parser.get_records(4)
        self.assertEqual(recovered_result, [self.recov_particle_meta, self.recov_particle_a,
                                            self.recov_particle_b, self.recov_particle_c])
        self.assertEqual(recovered_parser._state[StateKey.POSITION], 33)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 33)
        self.assertEqual(self.publish_callback_value[0], self.recov_particle_meta)
        self.assertEqual(self.publish_callback_value[1], self.recov_particle_a)
        self.assertEqual(self.publish_callback_value[2], self.recov_particle_b)
        self.assertEqual(self.publish_callback_value[3], self.recov_particle_c)
        self.assertEqual(self.file_ingested_value, True)
        self.assertEqual(recovered_parser._state[StateKey.RECORDS_READ], 3)
        self.assertEqual(self.state_callback_value[StateKey.RECORDS_READ], 3)
        self.assertEqual(recovered_parser._state[StateKey.METADATA_SENT], True)
        self.assertEqual(self.state_callback_value[StateKey.METADATA_SENT], True)

        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        telemetered_parser = DofstKWfpParser(
            self.config_telemetered, self.telemetered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback,
            len(DofstKWfpParserUnitTestCase.TEST_DATA))
        self.parser = telemetered_parser
        # next get records
        telemetered_result = telemetered_parser.get_records(4)
        self.assertEqual(telemetered_result, [self.telem_particle_meta, self.telem_particle_a,
                                              self.telem_particle_b, self.telem_particle_c])
        self.assertEqual(telemetered_parser._state[StateKey.POSITION], 33)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 33)
        self.assertEqual(self.publish_callback_value[0], self.telem_particle_meta)
        self.assertEqual(self.publish_callback_value[1], self.telem_particle_a)
        self.assertEqual(self.publish_callback_value[2], self.telem_particle_b)
        self.assertEqual(self.publish_callback_value[3], self.telem_particle_c)
        self.assertEqual(self.file_ingested_value, True)
        self.assertEqual(telemetered_parser._state[StateKey.RECORDS_READ], 3)
        self.assertEqual(self.state_callback_value[StateKey.RECORDS_READ], 3)
        self.assertEqual(telemetered_parser._state[StateKey.METADATA_SENT], True)
        self.assertEqual(self.state_callback_value[StateKey.METADATA_SENT], True)

    def test_long_stream(self):
        """
        Test a long stream
        """
        filepath = os.path.join(RESOURCE_PATH, 'C0000038.DAT')
        filesize = os.path.getsize(filepath)
        stream_handle = open(filepath)

        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        recovered_parser = DofstKWfpParser(
            self.config_recovered, self.recovered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback, filesize)
        self.parser = recovered_parser
        # next get records
        recovered_result = self.parser.get_records(271)
        self.assertEqual(recovered_result[0], self.recov_particle_meta_long)
        self.assertEqual(recovered_result[1], self.recov_particle_a_long)
        self.assertEqual(recovered_result[2], self.recov_particle_b_long)
        self.assertEqual(recovered_result[-1], self.recov_particle_last)
        self.assertEqual(recovered_parser._state[StateKey.POSITION], 2970)
        self.assertEqual(recovered_parser._state[StateKey.RECORDS_READ], 270)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 2970)
        self.assertEqual(self.state_callback_value[StateKey.RECORDS_READ], 270)
        self.assertEqual(self.publish_callback_value[-1], self.recov_particle_last)
        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        telemetered_parser = DofstKWfpParser(
            self.config_telemetered, self.telemetered_start_state, stream_handle,
            self.state_callback, self.pub_callback, self.exception_callback, filesize)
        self.parser = telemetered_parser
        # next get records
        telemetered_result = self.parser.get_records(271)
        self.assertEqual(telemetered_result[0], self.telem_particle_meta_long)
        self.assertEqual(telemetered_result[1], self.telem_particle_a_long)
        self.assertEqual(telemetered_result[2], self.telem_particle_b_long)
        self.assertEqual(telemetered_result[-1], self.telem_particle_last)
        self.assertEqual(telemetered_parser._state[StateKey.POSITION], 2970)
        self.assertEqual(telemetered_parser._state[StateKey.RECORDS_READ], 270)
        self.assertEqual(self.state_callback_value[StateKey.POSITION], 2970)
        self.assertEqual(self.state_callback_value[StateKey.RECORDS_READ], 270)
        self.assertEqual(self.publish_callback_value[-1], self.telem_particle_last)

    def test_bad_time_data(self):
        """
        If the timestamps are missing, raise a sample exception and do not parse the file
        """
        stream_handle = StringIO(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_TIME)
        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with self.assertRaises(SampleException):
            recovered_parser = DofstKWfpParser(
                self.config_recovered, self.recovered_start_state, stream_handle,
                self.state_callback, self.pub_callback, self.exception_callback,
                len(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_TIME))
        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with self.assertRaises(SampleException):
            telemetered_parser = DofstKWfpParser(
                self.config_telemetered, self.telemetered_start_state, stream_handle,
                self.state_callback, self.pub_callback, self.exception_callback,
                len(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_TIME))

    def test_bad_size_data(self):
        """
        If any of the data records in the file are not 11 bytes long, raise a sample exception
        and do not parse the file.
        """
        stream_handle = StringIO(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_SIZE)
        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with self.assertRaises(SampleException):
            recovered_parser = DofstKWfpParser(
                self.config_recovered, self.recovered_start_state, stream_handle,
                self.state_callback, self.pub_callback, self.exception_callback,
                len(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_SIZE))
        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with self.assertRaises(SampleException):
            telemetered_parser = DofstKWfpParser(
                self.config_telemetered, self.telemetered_start_state, stream_handle,
                self.state_callback, self.pub_callback, self.exception_callback,
                len(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_SIZE))

    def test_bad_eop_data(self):
        """
        If the last "data" record in the file is not 11 byes of 0xFF, raise a sample exception
        and do not parse the file.
        """
        stream_handle = StringIO(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_EOP)
        #********************************************
        # Test the "recovered" version of the parser
        #********************************************
        with self.assertRaises(SampleException):
            recovered_parser = DofstKWfpParser(
                self.config_recovered, self.recovered_start_state, stream_handle,
                self.state_callback, self.pub_callback, self.exception_callback,
                len(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_EOP))
        #**********************************************
        # Test the "telemetered" version of the parser
        #**********************************************
        with self.assertRaises(SampleException):
            telemetered_parser = DofstKWfpParser(
                self.config_telemetered, self.telemetered_start_state, stream_handle,
                self.state_callback, self.pub_callback, self.exception_callback,
                len(DofstKWfpParserUnitTestCase.TEST_DATA_BAD_EOP))
