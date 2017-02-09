#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_sio_eng_sio_mule
@file marine-integrations/mi/dataset/parser/test/test_sio_eng_sio_mule.py
@author Mike Nicoletti
@brief Test code for a sio_eng_sio_mule data parser
"""

from nose.plugins.attrib import attr
import os

from mi.core.exceptions import SampleException, UnexpectedDataException

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.sio_eng_sio import SioEngSioParser

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'sio_eng', 'sio', 'resource')


@attr('UNIT', group='mi')
class SioEngSioMuleParserUnitTestCase(ParserUnitTestCase):
    """
    sio_eng_sio_mule Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.telem_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.sio_eng_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'SioEngSioTelemeteredDataParticle'
        }

        self.recov_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.sio_eng_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'SioEngSioRecoveredDataParticle'
        }

    def test_simple(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.status.dat')) as stream_handle:

            parser = SioEngSioParser(self.telem_config, stream_handle, self.exception_callback)

            # request one more particle than expected
            result = parser.get_records(5)
            self.assert_particles(result, 'node59p1_1.yml', RESOURCE_PATH)

            result = parser.get_records(5)
            log.debug('second get records got back %s records', len(result))

        self.assertEqual(self.exception_callback_value, [])

    def test_simple_recov(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        This serves as both the simple and long recovered tests, since it uses the full recovered example file
        """
        with open(os.path.join(RESOURCE_PATH, 'STA15908.DAT')) as stream_handle:

            parser = SioEngSioParser(self.recov_config, stream_handle, self.exception_callback)

            result = parser.get_records(24)
            self.assert_particles(result, 'STA15908.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.status.dat')) as stream_handle:

            parser = SioEngSioParser(self.telem_config, stream_handle, self.exception_callback)

            # split up getting records, start with 2
            result = parser.get_records(2)
            self.assertEqual(len(result), 2)
            # only 2 remaining, request 3 and make sure we only get 2
            result2 = parser.get_records(3)
            self.assertEqual(len(result2), 2)
            # shouldn't get any more assert this is 0
            result3 = parser.get_records(1)
            self.assertEqual(len(result3), 0)

            # combine result and result2 to get the whole file
            result.extend(result2)
            self.assert_particles(result, 'node59p1_1.yml', RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_get_with_other_sio_id(self):
        """
        Test with a file that includes an ID other than the expected sio IDs, which should generate an exception
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.id.status.dat')) as stream_handle:

            parser = SioEngSioParser(self.telem_config, stream_handle, self.exception_callback)

            result = parser.get_records(4)
            self.assert_particles(result, 'node59p1_1.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

    def test_get_with_extra_data(self):
        """
        Test with a file that includes unexpected data parses correctly and generates an exception
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.extra.status.dat')) as stream_handle:

            parser = SioEngSioParser(self.telem_config, stream_handle, self.exception_callback)

            result = parser.get_records(4)
            self.assert_particles(result, 'node59p1_1.yml', RESOURCE_PATH)

            self.assertEquals(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

    def test_long_stream(self):
        """
        Test a long stream 
        """
        # test with the flmb file
        with open(os.path.join(RESOURCE_PATH, 'node59p1_0.status.dat')) as stream_handle:

            parser = SioEngSioParser(self.telem_config, stream_handle, self.exception_callback)

            result = parser.get_records(9)
            self.assert_particles(result, 'node59p1_0.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        # test with the hypm file
        with open(os.path.join(RESOURCE_PATH, 'node58p1_0.status.dat')) as stream_handle:

            parser = SioEngSioParser(self.telem_config, stream_handle, self.exception_callback)

            result = parser.get_records(4)
            self.assert_particles(result, 'node58p1_0.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_telem(self):
        """
        Test a file that has had a section of the 2nd CS data replaced with "BAD DATA"
        """
        with open(os.path.join(RESOURCE_PATH, 'node59p1_1.bad.status.dat')) as stream_handle:
            parser = SioEngSioParser(self.telem_config, stream_handle, self.exception_callback)

            result = parser.get_records(4)
            # 2nd particle has bad data, only 3 particles left
            self.assertEqual(len(result), 3)

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], SampleException))

    def test_bad_recov(self):
        """
        Test a file that has has sections of CS data replaced with letters
        """
        with open(os.path.join(RESOURCE_PATH, 'STA15908_BAD.DAT')) as stream_handle:
            parser = SioEngSioParser(self.recov_config, stream_handle, self.exception_callback)

            result = parser.get_records(24)
            # first particle has bad data, only 23 particles left
            self.assertEqual(len(result), 23)

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], SampleException))