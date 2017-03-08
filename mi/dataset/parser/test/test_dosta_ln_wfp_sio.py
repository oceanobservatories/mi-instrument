#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_dosta_ln_wfp_sio.py
@author Christopher Fortin
@brief Test code for a dosta_ln_wfp_sio data parser
"""

import os
import struct

import ntplib
from nose.plugins.attrib import attr

from mi.core.exceptions import UnexpectedDataException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.dosta_ln.wfp_sio.resource import RESOURCE_PATH
from mi.dataset.parser.dosta_ln_wfp_sio import DostaLnWfpSioParser, DostaLnWfpSioDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class DostaLnWfpSioParserUnitTestCase(ParserUnitTestCase):

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_ln_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaLnWfpSioDataParticle'
        }

        # the hex characters used to create the expected particles below were extracted
        # from the first 4 E records in the file node58p1_0.we_wfp.dat by hand
        # and used here to verify the correct raw data was used to create the particles
        self.timestamp_1a = self.timestamp_to_ntp(b'\x52\x04\xCC\x2D')
        self.particle_1a = DostaLnWfpSioDataParticle(
            b'\x52\x04\xCC\x2D\x00\x00\x00\x00\x41\x3B\x6F\xD2\x00\x00\x00'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x37\x00\x60\x02\x3E',
            internal_timestamp=self.timestamp_1a)

        self.timestamp_1b = self.timestamp_to_ntp(b'\x52\x04\xCD\x70')
        self.particle_1b = DostaLnWfpSioDataParticle(
            b'\x52\x04\xCD\x70\x43\x66\x2F\x90\x41\x32\xDE\x01\x45\x7D\xA7'
            b'\x85\x43\x13\x9F\x7D\x3F\xBF\xBE\x77\x00\x37\x00\x61\x02\x3C',
            internal_timestamp=self.timestamp_1b)

        self.timestamp_1c = self.timestamp_to_ntp(b'\x52\x04\xCE\xB0')
        self.particle_1c = DostaLnWfpSioDataParticle(
            b'\x52\x04\xCE\xB0\x43\x6D\xEA\x30\x41\x2F\xE5\xC9\x45\x78\x56'
            b'\x66\x43\x12\x94\x39\x3F\xBF\x9D\xB2\x00\x37\x00\x73\x02\x3B',
            internal_timestamp=self.timestamp_1c)

        self.timestamp_1d = self.timestamp_to_ntp(b'\x52\x04\xCF\xF0')

        self.particle_1d = DostaLnWfpSioDataParticle(
            b'\x52\x04\xCF\xF0\x43\x6E\x7C\x78\x41\x2E\xF4\xF1\x45\x73\x1B'
            b'\x0A\x43\x11\x9F\x7D\x3F\xBF\x7C\xEE\x00\x37\x00\x5E\x02\x3B',
            internal_timestamp=self.timestamp_1d)

    def timestamp_to_ntp(self, hex_timestamp):
        fields = struct.unpack('>I', hex_timestamp)
        timestamp = float(fields[0])
        return ntplib.system_to_ntp_time(timestamp)

    def test_simple(self):
        """
        Read test data from the file and pull out data particles one at a time.
        Assert that the results are those we expected.
        This test only verifies the raw data in the particle is correct
        """

        log.debug('-Starting test_simple')
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node58p1_0.we_wfp.dat'))
        self.parser = DostaLnWfpSioParser(self.config, self.stream_handle, self.exception_callback)

        result = self.parser.get_records(1)
        self.assertEqual(result, [self.particle_1a])

        result = self.parser.get_records(1)
        self.assertEqual(result, [self.particle_1b])

        result = self.parser.get_records(1)
        self.assertEqual(result, [self.particle_1c])

        result = self.parser.get_records(1)
        self.assertEqual(result, [self.particle_1d])
        self.assertEquals(self.exception_callback_value, [])

        self.stream_handle.close()

    def test_get_many(self):
        """
        Read test data from the file and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('--Starting test_get_many')
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node58p1_0.we_wfp.dat'))
        self.parser = DostaLnWfpSioParser(self.config, self.stream_handle, self.exception_callback)

        result = self.parser.get_records(4)
        self.assertEqual(result,
                         [self.particle_1a, self.particle_1b, self.particle_1c, self.particle_1d])
        self.assertEquals(self.exception_callback_value, [])

        self.stream_handle.close()
    
    def test_long_stream(self):
        """
        Test a long stream 
        """

        log.debug('--Starting test_long_stream')
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node58p1_0.we_wfp.dat'))
        self.stream_handle.seek(0)
        self.parser = DostaLnWfpSioParser(self.config, self.stream_handle, self.exception_callback)

        result = self.parser.get_records(100)

        self.assert_particles(result, 'node58p1_0.we_wfp.yml', RESOURCE_PATH)
        self.assertEquals(self.exception_callback_value, [])

        self.stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that the bad record ( in this case a currupt status message ) causes a sample exception
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'node58p1_BADFLAGS.dat'))
        log.debug('--Starting test_bad_data')
        self.parser = DostaLnWfpSioParser(self.config, self.stream_handle, self.exception_callback)
        self.parser.get_records(1)
        self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

    def test_bad_e_record(self):
        """
        Ensure that the bad record causes a sample exception. The file 'bad_e_record.dat'
        includes a record containing one byte less than the expected 30 for the
        dosta_ln_wfp_sio. The 'Number of Data Bytes' and the 'CRC Checksum' values in the
        SIO Mule header have been modified accordingly.
        """
        self.stream_handle = open(os.path.join(RESOURCE_PATH, 'bad_e_record.dat'))

        self.parser = DostaLnWfpSioParser(self.config, self.stream_handle, self.exception_callback)
        self.parser.get_records(1)
        self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write('  - _index: %d\n' % (i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.5f\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """

        #ADCP_data_20130702.PD0 has one record in it
        fid = open(os.path.join(RESOURCE_PATH, 'node58p1_0.we_wfp.dat'), 'rb')

        stream_handle = fid
        parser = DostaLnWfpSioParser(self.config, stream_handle,
                                     self.exception_callback)

        particles = parser.get_records(100)

        self.particle_to_yml(particles, 'node58p1_0.we_wfp.yml')
        fid.close()

