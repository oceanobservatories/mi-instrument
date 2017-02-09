#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_wc_wm_cspp
@file marine-integrations/mi/dataset/parser/test/test_wc_wm_cspp.py
@author Jeff Roy
@brief Test code for a wc_wm_cspp data parser

wc_wm_cspp is based on cspp_base.py
test_dosta_abcdjm_cspp.py fully tests all of the capabilities of the
base parser.  That level of testing is omitted from this test suite
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.wc_wm.cspp.resource import RESOURCE_PATH
from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.wc_wm_cspp import \
    WcWmCsppParser, \
    WcWmEngRecoveredDataParticle, \
    WcWmEngTelemeteredDataParticle, \
    WcWmMetadataRecoveredDataParticle, \
    WcWmMetadataTelemeteredDataParticle, \
    WcWmDataTypeKey
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class WcWmCsppParserUnitTestCase(ParserUnitTestCase):
    """
    wc_wm_cspp Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            WcWmDataTypeKey.WC_WM_CSPP_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcWmMetadataTelemeteredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcWmEngTelemeteredDataParticle,
                }
            },
            WcWmDataTypeKey.WC_WM_CSPP_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcWmMetadataRecoveredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcWmEngRecoveredDataParticle,
                }
            },
        }

        self.file_ingested_value = None

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
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        Be sure to verify the results by eye before trusting!
        """

        fid = open(os.path.join(RESOURCE_PATH, '11079364_WC_WM.txt'), 'rU')

        stream_handle = fid
        parser = WcWmCsppParser(self.config.get(WcWmDataTypeKey.WC_WM_CSPP_RECOVERED),
                                stream_handle,
                                self.exception_callback)

        particles = parser.get_records(20)

        self.particle_to_yml(particles, '11079364_WC_WM_recov.yml')
        fid.close()

    def test_simple_recov(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_WM.txt')
        stream_handle = open(file_path, 'rU')

        parser = WcWmCsppParser(self.config.get(WcWmDataTypeKey.WC_WM_CSPP_RECOVERED),
                                stream_handle,
                                self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '11079364_WC_WM_recov.yml', RESOURCE_PATH)

    def test_simple_telem(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_WM.txt')
        stream_handle = open(file_path, 'rU')

        parser = WcWmCsppParser(self.config.get(WcWmDataTypeKey.WC_WM_CSPP_TELEMETERED),
                                stream_handle,
                                self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '11079364_WC_WM_telem.yml', RESOURCE_PATH)

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_WM.txt')
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcWmCsppParser(self.config.get(WcWmDataTypeKey.WC_WM_CSPP_TELEMETERED),
                                stream_handle,
                                self.exception_callback)

        # try to get 2000 particles, there are more data records in the file
        # so should get 2000 including the meta data
        particles = parser.get_records(2000)

        log.debug("*** test_get_many Num particles %s", len(particles))
        self.assertEqual(len(particles), 2000)

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # the 4th data record in this file are corrupted and will be ignored

        file_path = os.path.join(RESOURCE_PATH, '11079364_BAD_WC_WM.txt')
        stream_handle = open(file_path, 'rU')

        parser = WcWmCsppParser(self.config.get(WcWmDataTypeKey.WC_WM_CSPP_RECOVERED),
                                stream_handle,
                                self.exception_callback)

        parser.get_records(20)

        # log.info('##Exception value %s', self.exception_callback_value)

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        stream_handle.close()
