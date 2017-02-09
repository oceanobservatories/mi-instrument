#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_mopak_o_dcl
@file marine-integrations/mi/dataset/parser/test/test_mopak_o_dcl.py
@author Emily Hahn
@brief Test code for a mopak_o_dcl data parser
"""
import calendar
import os
import struct
from datetime import datetime

import ntplib
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException, ConfigurationException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_stc_eng.stc.resource import RESOURCE_PATH
from mi.dataset.parser.mopak_o_dcl import \
    MopakODclParser, \
    MopakODclAccelParserRecoveredDataParticle, \
    MopakODclRateParserRecoveredDataParticle, \
    MopakParticleClassType
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class MopakODclParserUnitTestCase(ParserUnitTestCase):
    """
    MopakODcl Parser unit test suite
    """

    def setUp(self):
        ADP = MopakODclAccelParserRecoveredDataParticle
        RDP = MopakODclRateParserRecoveredDataParticle

        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.mopak_o_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['MopakODclAccelParserDataParticle',
                                                     'MopakODclRateParserDataParticle'],
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT:
                {MopakParticleClassType.ACCEL_PARTICLE_CLASS: ADP,
                 MopakParticleClassType.RATE_PARTICLE_CLASS: RDP}

        }

        # using the same file, and hence the same start time, so just convert the start time here
        file_datetime = datetime.strptime('20140120_140004', "%Y%m%d_%H%M%S")

        start_time_utc = calendar.timegm(file_datetime.timetuple()) + (file_datetime.microsecond / 1000000.0)

        # Define test data particles and their associated timestamps which will be
        # compared with returned results
        self._timer_start = 33456
        self.timestamp1 = self.timer_to_timestamp(b'\x00\x00\x82\xb0', start_time_utc, 0, self._timer_start)
        self.particle_a_accel = ADP(b"\xcb\xbd\xe6\xac<\xbd\xd9\nA\xbf\x83\xa4"
                                    "+;\xaf\xb4\x01\xbd\xf2o\xd4\xbd\xfe\x9d'>P"
                                    "\xfd\xfc>t\xd5\xc4>\xed\x10\xb2\x00\x00\x82\xb0\x16I",
                                    internal_timestamp=self.timestamp1)
        self.timestamp2 = self.timer_to_timestamp(b'\x00\x00\x9b\x1a', start_time_utc, 0, self._timer_start)
        self.particle_b_accel = ADP(b"\xcb\xbe\x17Q\x8e\xbd\xc7_\x8a\xbf\x85\xc3"
                                    "e\xbc\xebN\x18\xbd\x9a\x86P\xbd\xf4\xe4\xd4>"
                                    "T38>s\xc8\xb9>\xea\xce\xd0\x00\x00\x9b\x1a\x15\xa5",
                                    internal_timestamp=self.timestamp2)
        self.timestamp3 = self.timer_to_timestamp(b'\x00\x00\xb3\x84', start_time_utc, 0, self._timer_start)
        self.particle_c_accel = ADP(b"\xcb\xbe1\xeak\xbd\xae?\x8a\xbf\x86\x18"
                                    "\x8a\xbd~\xde\xf0\xbc\xb2\x1d\xec\xbd\xd7"
                                    "\xe4\x04>U\xbcW>p\xf3U>\xeaOh\x00\x00\xb3\x84\x15\xd8",
                                    internal_timestamp=self.timestamp3)
        self.timestamp4 = self.timer_to_timestamp(b'\x00\x00\xcb\xee', start_time_utc, 0, self._timer_start)
        self.particle_d_accel = ADP(b"\xcb\xbe8\xed\xf0\xbd\xa7\x98'\xbf\x88"
                                    "\x0e\xca\xbd\xeegZ<\xf63\xdc\xbd\xe6b\x8d"
                                    ">U\xa5U>l6p>\xe9\xfc\x8d\x00\x00\xcb\xee\x16e",
                                    internal_timestamp=self.timestamp4)
        self.timestamp5 = self.timer_to_timestamp(b'\x00\x00\xe4X', start_time_utc, 0, self._timer_start)
        self.particle_e_accel = ADP(b"\xcb\xbe9t\xb5\xbd\x89\xd1\x16\xbf\x87"
                                    "\r\x14\xbe\r\xca\x9d=\xa9\x85+\xbd\xf3\x1c"
                                    "\xcb>R9\x1b>f\xcen>\xead\xb4\x00\x00\xe4X\x13\x1e",
                                    internal_timestamp=self.timestamp5)

        self.timestamp6 = self.timer_to_timestamp(b'\x04ud\x1e', start_time_utc, 0, self._timer_start)
        self.particle_last_accel = ADP(b"\xcb=\xfd\xb6?=0\x84\xf6\xbf\x82\xff"
                                       "\xed>\x07$\x16\xbe\xaf\xf3\xb9=\x93\xb5"
                                       "\xad\xbd\x97\xcb8\xbeo\x0bI>\xf4_K\x04ud\x1e\x14\x87",
                                       internal_timestamp=self.timestamp6)

        # got a second file with rate particles in it after writing tests, so adding new tests but leaving
        # the old, resulting in many test particles

        # after this is for a new file with rate in it
        # using the same file, and hence the same start time, so just convert the start time here
        file_datetime = datetime.strptime('20140313_191853', "%Y%m%d_%H%M%S")

        start_time_utc = calendar.timegm(file_datetime.timetuple()) + (file_datetime.microsecond / 1000000.0)

        # first in larger file
        self._rate_long_timer_start = 11409586
        self.timestampa11 = self.timer_to_timestamp(b'\x00\xae\x18\xb2', start_time_utc, 0, self._rate_long_timer_start)
        self.particle_a11_accel = ADP(b"\xcb?(\xf4\x85?.\xf6k>\x9dq\x91\xba7r"
                                      "\x9b\xba\xca\x19T:\xff\xbc[\xbe\xfb\xd3"
                                      "\xdf\xbd\xc6\x0b\xbb\xbe\x7f\xa8T\x00\xae\x18\xb2\x15\xfa",
                                      internal_timestamp=self.timestampa11)

        self._first_rate_timer_start = 11903336
        # first in first_rate file
        self.timestampa1 = self.timer_to_timestamp(b'\x00\xb5\xa1h', start_time_utc, 0, self._first_rate_timer_start)
        self.particle_a1_accel = ADP(b"\xcb?(\xd3d?/\x0bd>\x9dxr\xba$eZ\xbbl"
                                     "\xaa\xea:\xed\xe7\xa6\xbe\xfb\xe1J\xbd"
                                     "\xc6\xfa\x90\xbe\x7f\xcc2\x00\xb5\xa1h\x16\x01",
                                     internal_timestamp=self.timestampa1)
        self.timestampb1 = self.timer_to_timestamp(b'\x00\xb5\xb9\xd2', start_time_utc, 0, self._first_rate_timer_start)
        self.particle_b1_accel = ADP(b"\xcb?))$?/(\x9b>\x9e\x15w\xb9\x92\xc0"
                                     "\x16\xbah\xb6\x0e:\xe5\x97\xf3\xbe\xfc"
                                     "\x044\xbd\xc6\xf5\x1b\xbe\x80ym\x00\xb5\xb9\xd2\x13\xb2",
                                     internal_timestamp=self.timestampb1)

        self.timestamp1r = self.timer_to_timestamp(b'\x00\xd0\xe7\xd4', start_time_utc, 0, self._first_rate_timer_start)
        self.particle_a_rate = RDP(b"\xcf\xbf\xffNJ?:\x90\x8b@\x1e\xde\xa8\xba"
                                   "\tU\xe8\xbb\x07Z\xf2:\xb8\xa9\xc7\x00\xd0\xe7\xd4\x0f\x98",
                                   internal_timestamp=self.timestamp1r)
        self.timestamp2r = self.timer_to_timestamp(b'\x00\xd1\x00>', start_time_utc, 0, self._first_rate_timer_start)
        self.particle_b_rate = RDP(b"\xcf\xbf\xffD\xa1?:\x92\x85@\x1e\xde\xcc:\xa3"
                                   "6\xf1\xba\xf7I@;\xc4\x05\x85\x00\xd1\x00>\r\xe0",
                                   internal_timestamp=self.timestamp2r)
        self.timestamp3r = self.timer_to_timestamp(b'\x00\xd1\x18\xa8', start_time_utc, 0, self._first_rate_timer_start)
        self.particle_c_rate = RDP(b"\xcf\xbf\xffC\xcb?:\x8dL@\x1e\xdf6\xb9\xf5"
                                   "\xb1:\xb9\xdf\x06\n;\x05\\a\x00\xd1\x18\xa8\r/",
                                   internal_timestamp=self.timestamp3r)
        # last in first_rate file
        self.timestamp4r = self.timer_to_timestamp(b'\x00\xd11\x12', start_time_utc, 0, self._first_rate_timer_start)
        self.particle_d_rate = RDP(b"\xcf\xbf\xffF/?:\x8a\x1d@\x1e\xe0.\xba\xd3"
                                   "9*\xba\x80\x1c{:?-\xe9\x00\xd11\x12\x0b\xf2",
                                   internal_timestamp=self.timestamp4r)

        # last in larger file
        self.timestamp8r = self.timer_to_timestamp(b'\x00\xd73(', start_time_utc, 0, self._rate_long_timer_start)
        self.particle_last_rate = RDP(
            b"\xcf\xbf\xffK ?:r\xd4@\x1e\xf4\xf09\xa7\x91"
            "\xb0\xb9\x9b\x82\x85;$\x1f\xc7\x00\xd73(\r\xec",
            internal_timestamp=self.timestamp8r)

        # uncomment the following to generate particles in yml format for driver testing results files
        # self.particle_to_yml(self.particle_a1_accel)
        # self.particle_to_yml(self.particle_b1_accel)
        # self.particle_to_yml(self.particle_a_rate)
        # self.particle_to_yml(self.particle_b_rate)
        # self.particle_to_yml(self.particle_c_rate)
        # self.particle_to_yml(self.particle_d_rate)

    def timer_to_timestamp(self, timer, start_time_utc, rollover_count, timer_start):
        """
        convert a timer value to a ntp formatted timestamp
        """
        fields = struct.unpack('>I', timer)
        # if the timer has rolled over, multiply by the maximum value for timer so the time keeps increasing
        rollover_offset = rollover_count * 4294967296
        # make sure the timer starts at 0 for the file by subtracting the first timer
        # divide timer by 62500 to go from counts to seconds
        offset_secs = float(int(fields[0]) + rollover_offset - timer_start) / 62500.0
        # add in the utc start time
        time_secs = float(start_time_utc) + offset_secs
        # convert to ntp64
        return float(ntplib.system_to_ntp_time(time_secs))

    def particle_to_yml(self, particle):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml files here.
        """
        particle_dict = particle.generate_dict()
        # open write append, if you want to start from scratch manually delete this file
        fid = open('particle.yml', 'a')
        fid.write('  - _index: 0\n')
        fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))
        fid.write('    particle_object: %s\n' % particle.__class__.__name__)
        fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
        for val in particle_dict.get('values'):
            if isinstance(val.get('value'), float):
                fid.write('    %s: %16.20f\n' % (val.get('value_id'), val.get('value')))
            else:
                fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'first.mopak.log'))
        parser = MopakODclParser(self.config, stream_handle,
                                 '20140120_140004.mopak.log',
                                 self.exception_callback)
        # next get acceleration records
        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_a_accel)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_b_accel)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_c_accel)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_d_accel)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_e_accel)
        self.assertEqual(self.exception_callback_value, [])

        # no data left, don't move the position
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(self.exception_callback_value, [])

    def test_simple_rate(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'first_rate.mopak.log'))
        parser = MopakODclParser(self.config, stream_handle,
                                 '20140313_191853.mopak.log',
                                 self.exception_callback)

        # next get accel and rate records
        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_a1_accel)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_b1_accel)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_a_rate)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_b_rate)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)
        self.assertEqual(result[0], self.particle_c_rate)
        self.assertEqual(self.exception_callback_value, [])

        result = parser.get_records(1)

        self.assertEqual(result[0], self.particle_d_rate)
        self.assertEqual(self.exception_callback_value, [])

        # no data left, don't move the position
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(self.exception_callback_value, [])

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'first.mopak.log'))
        parser = MopakODclParser(self.config, stream_handle,
                                 '20140120_140004.mopak.log',
                                 self.exception_callback)
        # next get accel records
        result = parser.get_records(5)
        self.assertEqual(result, [self.particle_a_accel,
                                  self.particle_b_accel,
                                  self.particle_c_accel,
                                  self.particle_d_accel,
                                  self.particle_e_accel])
        self.assertEqual(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        Test a long (normal length file)
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          '20140120_140004_extradata.mopak.log'))
        parser = MopakODclParser(self.config, stream_handle,
                                 '20140120_140004.mopak.log',
                                 self.exception_callback)

        result = parser.get_records(11964)
        self.assertEqual(result[0], self.particle_a_accel)
        self.assertEqual(result[-1], self.particle_last_accel)
        self.assertEqual(self.exception_callback_value, [])

    def test_long_stream_yml(self):
        """
        Verify an entire file against a yaml result file.
        """
        with open(os.path.join(RESOURCE_PATH, '20140120_140004.mopak.log'), 'rb') as stream_handle:
            parser = MopakODclParser(self.config, stream_handle,
                                     '20140120_140004.mopak.log',
                                     self.exception_callback)

            particles = parser.get_records(10)

            self.assert_particles(particles, 'first_mopak_recov.result.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_long_stream_rate(self):
        """
        Test a long (normal length file) with accel and rate particles
        """
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          '20140313_191853.3dmgx3.log'))
        parser = MopakODclParser(self.config, stream_handle,
                                 '20140313_191853.3dmgx3.log',
                                 self.exception_callback)
        result = parser.get_records(148)
        self.assertEqual(result[0], self.particle_a11_accel)
        self.assertEqual(result[-1], self.particle_last_rate)
        self.assertEqual(self.exception_callback_value, [])

    def test_non_data_exception(self):
        """
        Test that we get a sample exception from non data being found in the file
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'noise.mopak.log'))
        parser = MopakODclParser(self.config, stream_handle,
                                 '20140120_140004.mopak.log',
                                 self.exception_callback)

        # next get accel records
        result = parser.get_records(5)
        self.assertEqual(result, [self.particle_a_accel,
                                  self.particle_b_accel,
                                  self.particle_c_accel,
                                  self.particle_d_accel,
                                  self.particle_e_accel])

        self.assertEqual(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

    def test_bad_config(self):
        """
        This tests that the parser raises a Configuration Exception if the
        required configuration items are not present
        """

        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'first.mopak.log'))

        config = {}

        with self.assertRaises(ConfigurationException):
            MopakODclParser(config, stream_handle,
                            '20140120_140004.mopak.log',
                            self.exception_callback)

    def test_bad_checksum(self):
        """
        This tests that the parser raises a Configuration Exception if the
        required configuration items are not present
        """

        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          '20140313_191853_bad_chksum.3dmgx3.log'))

        parser = MopakODclParser(self.config, stream_handle,
                                 '20140313_191853.3dmgx3.log',
                                 self.exception_callback)

        result = parser.get_records(10)

        self.assertEqual(len(result), 5)
        self.assertEqual(len(self.exception_callback_value), 2)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))
        self.assert_(isinstance(self.exception_callback_value[1], SampleException))
