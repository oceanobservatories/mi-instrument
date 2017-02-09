#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_vel3d_k_wfp
@file marine-integrations/mi/dataset/parser/test/test_vel3d_k_wfp.py
@author Steve Myerson (Raytheon)
@brief Test code for a vel3d_k_wfp data parser
"""

import os
from StringIO import StringIO

import ntplib
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException, RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.vel3d_k.wfp.resource import RESOURCE_PATH
from mi.dataset.parser.vel3d_k_wfp import \
    Vel3dKWfpParser, \
    Vel3dKWfpInstrumentParticle, \
    Vel3dKWfpMetadataParticle, \
    Vel3dKWfpStringParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


FILE_HEADER_SIZE = 4
DATA_RECORD_SIZE = 90
TIME_RECORD_SIZE = 8
TIME_ON = 1393266602.0  # time_on from the data file
SAMPLE_RATE = .5        # data records sample rate

RECORD_1_DATA = \
    '\x00\x00\xE6\xA0' \
    '\xA5\x0A\x15\x10\x50\x00\x56\x21\xEC\xF1\x02\x44' \
    '\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x10\xA6\x0E\x98\x3A' \
    '\xC4\x08\x00\x00\x00\x00\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00' \
    '\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE' \
    '\x83\x00\x80\xFF\xCE\x3E\xDA\x29\x1A\x01\x00\x00\xFC\xF7\x00\x00' \
    '\x00\x00\x72\x32\x4A\xD6\xE5\x01\x40\x3F\x3E\x14\x1E\x1A' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

RECORD_4_DATA = \
    '\x00\x00\xE6\xA0' \
    '\xA5\x0A\x15\x10\x50\x00\x56\x21\xEC\xF1\x02\x44' \
    '\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x10\xA6\x0E\x98\x3A' \
    '\xC4\x08\x00\x00\x00\x00\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00' \
    '\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE' \
    '\x83\x00\x80\xFF\xCE\x3E\xDA\x29\x1A\x01\x00\x00\xFC\xF7\x00\x00' \
    '\x00\x00\x72\x32\x4A\xD6\xE5\x01\x40\x3F\x3E\x14\x1E\x1A' \
    '\xA5\x0A' \
    '\x15\x10\x50\x00\xF3\x82\x89\x53\x02\x44\xC4\x86\x01\x00\xEE\x00' \
    '\x72\x01\x18\x12\x1E\x10\x2E\x22\x98\x3A\xC4\x08\x00\x00\x00\x00' \
    '\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00\x01\x30\xE6\x00\x96\x00' \
    '\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE\x83\x00\x80\xFF\xCE\x3E' \
    '\xDA\x29\x1A\x01\x00\x00\xFD\xF7\x00\x00\x00\x00\x14\xEF\xBD\xFB' \
    '\xE8\x86\x40\x3F\x3E\x08\x1A\x0D' \
    '\xA5\x0A\x15\x10\x50\x00\x87\x97' \
    '\x1D\x68\x02\x44\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x11' \
    '\xA6\x0E\x98\x3A\xC4\x08\x00\x00\x00\x00\xD1\x06\x7D\x03\x8E\x01' \
    '\x00\x00\x90\x00\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5B\x00' \
    '\xA3\xFF\x28\xFE\x91\x00\x86\xFF\xD8\x3F\xDA\x29\x1A\x01\x00\x00' \
    '\xFC\xF7\x00\x00\x00\x00\xD9\xD5\xE3\x01\x53\xAB\x40\x3E\x3C\x16' \
    '\x20\x15' \
    '\xA5\x0A\x15\x10\x50\x00\xF2\x03\x88\xD4\x02\x44\xC4\x86' \
    '\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x11\x2E\x22\x98\x3A\xC4\x08' \
    '\x00\x00\x00\x00\xD1\x06\x7D\x03\x8E\x01\x00\x00\x90\x00\x01\x30' \
    '\xE6\x00\x96\x00\x88\x13\x7B\x00\x5B\x00\xA3\xFF\x28\xFE\x91\x00' \
    '\x86\xFF\xD8\x3F\xDA\x29\x1A\x01\x00\x00\xFD\xF7\x00\x00\x00\x00' \
    '\x34\x04\x09\xEF\xC2\xEA\x40\x3F\x3E\x17\x10\x11' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

#
# First record has an extra byte before the sync.
# Second record is missing the sync.
#
RECORD_4_MISSING_SYNC = \
    '\x00\x00\xE6\xA0' \
    '\xBB\xA5\x0A\x15\x10\x50\x00\x56\x21\xEC\xF1\x02\x44' \
    '\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x10\xA6\x0E\x98\x3A' \
    '\xC4\x08\x00\x00\x00\x00\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00' \
    '\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE' \
    '\x83\x00\x80\xFF\xCE\x3E\xDA\x29\x1A\x01\x00\x00\xFC\xF7\x00\x00' \
    '\x00\x00\x72\x32\x4A\xD6\xE5\x01\x40\x3F\x3E\x14\x1E\x1A' \
    '\x5A\x0A' \
    '\x15\x10\x50\x00\xF3\x82\x89\x53\x02\x44\xC4\x86\x01\x00\xEE\x00' \
    '\x72\x01\x18\x12\x1E\x10\x2E\x22\x98\x3A\xC4\x08\x00\x00\x00\x00' \
    '\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00\x01\x30\xE6\x00\x96\x00' \
    '\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE\x83\x00\x80\xFF\xCE\x3E' \
    '\xDA\x29\x1A\x01\x00\x00\xFD\xF7\x00\x00\x00\x00\x14\xEF\xBD\xFB' \
    '\xE8\x86\x40\x3F\x3E\x08\x1A\x0D' \
    '\xA5\x0A\x15\x10\x50\x00\x87\x97' \
    '\x1D\x68\x02\x44\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x11' \
    '\xA6\x0E\x98\x3A\xC4\x08\x00\x00\x00\x00\xD1\x06\x7D\x03\x8E\x01' \
    '\x00\x00\x90\x00\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5B\x00' \
    '\xA3\xFF\x28\xFE\x91\x00\x86\xFF\xD8\x3F\xDA\x29\x1A\x01\x00\x00' \
    '\xFC\xF7\x00\x00\x00\x00\xD9\xD5\xE3\x01\x53\xAB\x40\x3E\x3C\x16' \
    '\x20\x15' \
    '\xA5\x0A\x15\x10\x50\x00\xF2\x03\x88\xD4\x02\x44\xC4\x86' \
    '\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x11\x2E\x22\x98\x3A\xC4\x08' \
    '\x00\x00\x00\x00\xD1\x06\x7D\x03\x8E\x01\x00\x00\x90\x00\x01\x30' \
    '\xE6\x00\x96\x00\x88\x13\x7B\x00\x5B\x00\xA3\xFF\x28\xFE\x91\x00' \
    '\x86\xFF\xD8\x3F\xDA\x29\x1A\x01\x00\x00\xFD\xF7\x00\x00\x00\x00' \
    '\x34\x04\x09\xEF\xC2\xEA\x40\x3F\x3E\x17\x10\x11' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

# Family changed from 0x10 to 0x11.
# Header checksum changed from 0xF1EC to 0xF2EC.
RECORD_INVALID_FAMILY = \
    '\x00\x00\xE6\xA0' \
    '\xA5\x0A\x15\x11\x50\x00\x56\x21\xEC\xF2\x02\x44' \
    '\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x10\xA6\x0E\x98\x3A' \
    '\xC4\x08\x00\x00\x00\x00\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00' \
    '\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE' \
    '\x83\x00\x80\xFF\xCE\x3E\xDA\x29\x1A\x01\x00\x00\xFC\xF7\x00\x00' \
    '\x00\x00\x72\x32\x4A\xD6\xE5\x01\x40\x3F\x3E\x14\x1E\x1A' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

# First record has an invalid header checksum.
# Changed from 0xF1EC to 0xECF1.
RECORD_INVALID_HEADER_CHECKSUM = \
    '\x00\x00\xE6\xA0' \
    '\xA5\x0A\x15\x10\x50\x00\x56\x21\xF1\xEC\x02\x44' \
    '\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x10\xA6\x0E\x98\x3A' \
    '\xC4\x08\x00\x00\x00\x00\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00' \
    '\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE' \
    '\x83\x00\x80\xFF\xCE\x3E\xDA\x29\x1A\x01\x00\x00\xFC\xF7\x00\x00' \
    '\x00\x00\x72\x32\x4A\xD6\xE5\x01\x40\x3F\x3E\x14\x1E\x1A' \
    '\xA5\x0A' \
    '\x15\x10\x50\x00\xF3\x82\x89\x53\x02\x44\xC4\x86\x01\x00\xEE\x00' \
    '\x72\x01\x18\x12\x1E\x10\x2E\x22\x98\x3A\xC4\x08\x00\x00\x00\x00' \
    '\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00\x01\x30\xE6\x00\x96\x00' \
    '\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE\x83\x00\x80\xFF\xCE\x3E' \
    '\xDA\x29\x1A\x01\x00\x00\xFD\xF7\x00\x00\x00\x00\x14\xEF\xBD\xFB' \
    '\xE8\x86\x40\x3F\x3E\x08\x1A\x0D' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

# First record has an invalid payload checksum.
# Changed from 0x2156 to 0x2155 which also causes the header checksum
# to change from 0xF1EC to 0xF1EB.
RECORD_INVALID_PAYLOAD_CHECKSUM = \
    '\x00\x00\xE6\xA0' \
    '\xA5\x0A\x15\x10\x50\x00\x55\x21\xEB\xF1\x02\x44' \
    '\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x10\xA6\x0E\x98\x3A' \
    '\xC4\x08\x00\x00\x00\x00\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00' \
    '\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE' \
    '\x83\x00\x80\xFF\xCE\x3E\xDA\x29\x1A\x01\x00\x00\xFC\xF7\x00\x00' \
    '\x00\x00\x72\x32\x4A\xD6\xE5\x01\x40\x3F\x3E\x14\x1E\x1A' \
    '\xA5\x0A' \
    '\x15\x10\x50\x00\xF3\x82\x89\x53\x02\x44\xC4\x86\x01\x00\xEE\x00' \
    '\x72\x01\x18\x12\x1E\x10\x2E\x22\x98\x3A\xC4\x08\x00\x00\x00\x00' \
    '\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00\x01\x30\xE6\x00\x96\x00' \
    '\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE\x83\x00\x80\xFF\xCE\x3E' \
    '\xDA\x29\x1A\x01\x00\x00\xFD\xF7\x00\x00\x00\x00\x14\xEF\xBD\xFB' \
    '\xE8\x86\x40\x3F\x3E\x08\x1A\x0D' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

# ID changed from 0x15 to 0x14.
# Header checksum changed from 0xF1EC to 0xF1EB.
RECORD_INVALID_ID = \
    '\x00\x00\xE6\xA0' \
    '\xA5\x0A\x14\x10\x50\x00\x56\x21\xEB\xF1\x02\x44' \
    '\xC4\x86\x01\x00\xEE\x00\x72\x01\x18\x12\x1E\x10\xA6\x0E\x98\x3A' \
    '\xC4\x08\x00\x00\x00\x00\xFF\x06\x7D\x03\xB9\x01\x00\x00\x90\x00' \
    '\x01\x30\xE6\x00\x96\x00\x88\x13\x7B\x00\x5C\x00\xA3\xFF\x29\xFE' \
    '\x83\x00\x80\xFF\xCE\x3E\xDA\x29\x1A\x01\x00\x00\xFC\xF7\x00\x00' \
    '\x00\x00\x72\x32\x4A\xD6\xE5\x01\x40\x3F\x3E\x14\x1E\x1A' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

# Fabricated record.
STRING_RECORD = \
    '\x00\x00\xE6\xA0' \
    '\xA5\x0A\xA0\x10\x22\x00\x28\x62\x1B\x33' \
    '\xCC' \
    'If u cn rd ths, u cn b a prgrmmr\x00' \
    '\x53\x0B\x8F\xAA\x53\x0B\x91\x03'

RECORD_1_FIELDS = (2, 68, 100036, 238, 114, 1, 24, 18, 30, 16, 3750, 15000,
                   2244, 0, 1791, 893, 441, 0, 144, 12289, 230, 150, 5000,
                   123, 92, -93, -471, 131, -128, 16078, 10714, 282, 0, -4, -9,
                   0, 12914, -10678, 485, 64, 63, 62, 20, 30, 26, 21)

RECORD_2_FIELDS = (2, 68, 100036, 238, 114, 1, 24, 18, 30, 16, 8750, 15000,
                   2244, 0, 1791, 893, 441, 0, 144, 12289, 230, 150, 5000,
                   123, 92, -93, -471, 131, -128, 16078, 10714, 282, 0, -3, -9,
                   0, -4332, -1091, -31000, 64, 63, 62, 8, 26, 13, 21)

RECORD_3_FIELDS = (2, 68, 100036, 238, 114, 1, 24, 18, 30, 17, 3750, 15000,
                   2244, 0, 1745, 893, 398, 0, 144, 12289, 230, 150, 5000,
                   123, 91, -93, -472, 145, -122, 16344, 10714, 282, 0, -4, -9,
                   0, -10791, 483, -21677, 64, 62, 60, 22, 32, 21, 21)

RECORD_4_FIELDS = (2, 68, 100036, 238, 114, 1, 24, 18, 30, 17, 8750, 15000,
                   2244, 0, 1745, 893, 398, 0, 144, 12289, 230, 150, 5000,
                   123, 91, -93, -472, 145, -122, 16344, 10714, 282, 0, -3, -9,
                   0, 1076, -4343, -5438, 64, 63, 62, 23, 16, 17, 21)

STRING_FIELDS = (204, 'If u cn rd ths, u cn b a prgrmmr')

TIME_1_FIELDS = (1393266602, 1393266947)
TIME_4_FIELDS = (1393266602, 1393266947)

# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class Vel3dKWfpParserUnitTestCase(ParserUnitTestCase):
    """
    vel3d_k_wfp Parser unit test suite
    """

    def create_expected_results(self):
        """
        This function creates the expected data particle results.
        """

        # These records are at time t=0
        time_stamp = TIME_ON
        ntp_time = ntplib.system_to_ntp_time(time_stamp)
        self.expected_particle1 = Vel3dKWfpInstrumentParticle(
            RECORD_1_FIELDS, internal_timestamp=ntp_time)
        self.expected_particle2_header_checksum = Vel3dKWfpInstrumentParticle(
            RECORD_2_FIELDS, internal_timestamp=ntp_time)
        self.expected_time = Vel3dKWfpMetadataParticle(
            TIME_1_FIELDS, internal_timestamp=ntp_time)
        self.expected_string_particle = Vel3dKWfpStringParticle(
            STRING_FIELDS, internal_timestamp=ntp_time)

        # These records are at time t=1
        time_stamp += SAMPLE_RATE
        ntp_time = ntplib.system_to_ntp_time(time_stamp)
        self.expected_particle2 = Vel3dKWfpInstrumentParticle(
            RECORD_2_FIELDS, internal_timestamp=ntp_time)
        self.expected_particle3_missing = Vel3dKWfpInstrumentParticle(
            RECORD_3_FIELDS, internal_timestamp=ntp_time)

        # These records are at time t=2
        time_stamp += SAMPLE_RATE
        ntp_time = ntplib.system_to_ntp_time(time_stamp)
        self.expected_particle3 = Vel3dKWfpInstrumentParticle(
            RECORD_3_FIELDS, internal_timestamp=ntp_time)
        self.expected_particle4_missing = Vel3dKWfpInstrumentParticle(
            RECORD_4_FIELDS, internal_timestamp=ntp_time)

        # These records are at time t=3
        time_stamp += SAMPLE_RATE
        ntp_time = ntplib.system_to_ntp_time(time_stamp)
        self.expected_particle4 = Vel3dKWfpInstrumentParticle(
            RECORD_4_FIELDS, internal_timestamp=ntp_time)

    def create_parser(self, file_handle):
        """
        This function creates a Vel3d_k_Wfp parser.
        """
        parser = Vel3dKWfpParser(self.config, file_handle, self.exception_callback)
        return parser

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_k_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS:
                ['Vel3dKWfpInstrumentParticle',
                 'Vel3dKWfpMetadataParticle',
                 'Vel3dKWfpStringParticle']
        }

        # Define test data particles and their associated timestamps which will be
        # compared with returned results

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None
        self.state = None

        self.maxDiff = None
        self.create_expected_results()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        log.debug("=================== START MANY ======================")
        log.debug("Many length %d", len(RECORD_4_DATA))
        input_file = StringIO(RECORD_4_DATA)
        self.parser = self.create_parser(input_file)

        log.debug("MANY VERIFY RECORD 1")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_particle1)

        self.verify_contents(result, self.expected_particle1)

        log.debug("MANY VERIFY RECORDS 2-4")
        result = self.parser.get_records(3)
        self.assertEqual(result, [self.expected_particle2,
                         self.expected_particle3, self.expected_particle4])

        log.debug("MANY VERIFY TIME RECORD")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_time)

        log.debug("=================== END MANY ======================")

    def test_invalid_family(self):
        """
        Ensure that a Sample Exception is raised when the family field is invalid.
        """
        log.debug("================= START INVALID FAMILY ======================")
        log.debug("Invalid Family length %d", len(RECORD_INVALID_FAMILY))
        input_file = StringIO(RECORD_INVALID_FAMILY)
        self.parser = self.create_parser(input_file)

        log.debug("INVALID FAMILY GET 1 RECORD")
        result = self.parser.get_records(1)
        self.assertEquals(len(result), 0)
        self.assertEquals(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        log.debug("================= END INVALID FAMILY ======================")

    def test_invalid_header_checksum(self):
        """
        Ensure that a data record is skipped when the header checksum is invalid.
        This should result in a non-data exception.
        """
        log.debug("============== START INVALID HEADER CHECKSUM ==================")
        log.debug("Invalid Header Checksum length %d",
                  len(RECORD_INVALID_HEADER_CHECKSUM))
        input_file = StringIO(RECORD_INVALID_HEADER_CHECKSUM)
        self.parser = self.create_parser(input_file)

        log.debug("INVALID HEADER CHECKSUM GET 1 RECORD")
        result = self.parser.get_records(1)

        # Per IDD processing should stop once a header with
        # and invalid checksum is encountered.  No data can
        # be trusted at that point

        self.assertEquals(len(result), 0)
        self.assertEquals(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        log.debug("============== END INVALID HEADER CHECKSUM ==================")

    def test_invalid_payload_checksum(self):
        """
        Ensure that a data record is skipped when the payload checksum is invalid.
        This should result in a non-data exception.
        """
        log.debug("============== START INVALID PAYLOAD CHECKSUM ==================")
        log.debug("Invalid Payload Checksum length %d",
                 len(RECORD_INVALID_PAYLOAD_CHECKSUM))
        input_file = StringIO(RECORD_INVALID_PAYLOAD_CHECKSUM)
        self.parser = self.create_parser(input_file)

        log.debug("INVALID PAYLOAD CHECKSUM GET 1 RECORD")
        result = self.parser.get_records(1)

        log.debug("INVALID PAYLOAD CHECKSUM VERIFY CONTENTS")
        self.verify_contents(result, self.expected_particle2_header_checksum)

        self.assertEquals(len(result), 1)
        self.assertEquals(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        log.debug("============== END INVALID PAYLOAD CHECKSUM ==================")

    def test_invalid_id(self):
        """
        Ensure that a Sample Exception is raised when the ID field is invalid.
        """
        log.debug("================= START INVALID ID ======================")
        log.debug("Invalid ID length %d", len(RECORD_INVALID_ID))
        input_file = StringIO(RECORD_INVALID_ID)
        self.parser = self.create_parser(input_file)

        log.debug("INVALID ID GET 1 RECORD")
        result = self.parser.get_records(1)
        self.assertEquals(len(result), 0)
        self.assertEquals(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        log.debug("================= END INVALID ID ======================")

    def test_missing_sync(self):
        """
        Ensure that bad data is skipped when a sync is missing.
        There should be 2 pieces of non-data:
          A single byte at the start of the file.
          An entire data record (2nd record in the file).
        """
        log.debug("=================== START MISSING SYNC ======================")
        log.debug("Missing Sync length %d", len(RECORD_4_MISSING_SYNC))
        input_file = StringIO(RECORD_4_MISSING_SYNC)
        self.parser = self.create_parser(input_file)

        log.debug("MISSING GET 3 RECORDS")
        result = self.parser.get_records(3)

        # Per IDD processing should stop once a header with
        # and invalid sync byte is encountered.

        self.assertEquals(len(result), 0)
        self.assertEquals(len(self.exception_callback_value), 1)
        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        log.debug("=================== END MISSING SYNC ======================")

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        File is valid.  Has 1 velocity record.
        """
        log.debug("=================== START SIMPLE ======================")
        log.debug("Simple length %d", len(RECORD_1_DATA))
        input_file = StringIO(RECORD_1_DATA)
        self.parser = self.create_parser(input_file)

        log.debug("SIMPLE VERIFY RECORD 1")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_particle1)

        log.debug("SIMPLE VERIFY TIME RECORD")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_time)

        log.debug("=================== END SIMPLE ======================")

    def test_string_record(self):
        """
        This function verifies that a string record can be processed.
        """
        log.debug("=================== START STRING ======================")
        log.debug("String record length %d", len(STRING_RECORD))
        input_file = StringIO(STRING_RECORD)
        self.parser = self.create_parser(input_file)

        log.debug("STRING VERIFY RECORD 1")
        result = self.parser.get_records(1)
        self.verify_contents(result, self.expected_string_particle)

        log.debug("=================== END STRING ======================")

    def verify_contents(self, actual_particle, expected_particle):
        # log.debug('==== ACT raw %s', actual_particle.raw_data)
        # log.debug('==== EXP raw %s', expected_particle.raw_data)

        self.assertEqual(actual_particle, [expected_particle])

    def test_real_file(self):
        """
        Ensure that data is skipped when flag record is too short.
        This should raise an exception indicating that end of file was
        reached while reading the Flag record.
        """
        log.info("START REAL FILE")

        with open(os.path.join(RESOURCE_PATH, 'A0000010_5.DAT')) as file_handle:

            self.parser = self.create_parser(file_handle)

            particles = self.parser.get_records(1000)

            self.assert_particles(particles, 'A0000010_5_1_5.yml', RESOURCE_PATH)
            self.assertEquals(len(self.exception_callback_value), 0)

        log.info("END REAL FILE 4")
