#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_ctdpf_j_cspp.py
@author Joe Padula modified by C. Goodrich for uFrame
@brief Test code for a ctdpf_j_cspp data parser

ctdpf_j_cspp is based on cspp_base.py
test_dosta_abcdjm_cspp.py fully tests all of the capabilities of the
base parser.  That level of testing is omitted from this test suite
"""

import os

from nose.plugins.attrib import attr
from mi.core.common import BaseEnum
from mi.core.log import get_logger

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY

from mi.dataset.parser.ctdpf_j_cspp import \
    CtdpfJCsppParser, \
    CtdpfJCsppInstrumentTelemeteredDataParticle, \
    CtdpfJCsppMetadataTelemeteredDataParticle, \
    CtdpfJCsppInstrumentRecoveredDataParticle, \
    CtdpfJCsppMetadataRecoveredDataParticle

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

log = get_logger()


class DataTypeKey(BaseEnum):
    CTDPF_J_CSPP_RECOVERED = 'ctdpf_j_cspp_recovered'
    CTDPF_J_CSPP_TELEMETERED = 'ctdpf_j_cspp_telemetered'

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'ctdpf_j', 'cspp', 'resource')

RECOVERED_SAMPLE_DATA = '11079364_PPB_CTD.txt'
TELEMETERED_SAMPLE_DATA = '11079364_PPD_CTD.txt'


@attr('UNIT', group='mi')
class CtdpfJCsppParserUnitTestCase(ParserUnitTestCase):
    """
    ctdpf_j_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataTypeKey.CTDPF_J_CSPP_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_j_cspp',
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: CtdpfJCsppMetadataTelemeteredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: CtdpfJCsppInstrumentTelemeteredDataParticle,
                }
            },
            DataTypeKey.CTDPF_J_CSPP_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_j_cspp',
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: CtdpfJCsppMetadataRecoveredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: CtdpfJCsppInstrumentRecoveredDataParticle,
                }
            },
        }

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write(" particle_object: 'MULTIPLE'\n")
        fid.write(" particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write(' - _index: %d\n' % (i+1))

            fid.write('   particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('   particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('   internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('   %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("   %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('   %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """

        fid = open(os.path.join(RESOURCE_PATH, TELEMETERED_SAMPLE_DATA), 'rU')

        stream_handle = fid
        parser = CtdpfJCsppParser(self.config.get(DataTypeKey.CTDPF_J_CSPP_TELEMETERED),
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(20)

        self.particle_to_yml(particles, '11079364_PPD_CTD_telem.yml')
        fid.close()

    def test_simple(self):
        """
        Read test data and pull out 20 data particles.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, RECOVERED_SAMPLE_DATA)
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = CtdpfJCsppParser(self.config.get(DataTypeKey.CTDPF_J_CSPP_RECOVERED),
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '11079364_PPB_CTD_recov.yml', RESOURCE_PATH)

        stream_handle.close()

        # Now do the same for the telemetered version
        file_path = os.path.join(RESOURCE_PATH, TELEMETERED_SAMPLE_DATA)
        stream_handle = open(file_path, 'rU')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = CtdpfJCsppParser(self.config.get(DataTypeKey.CTDPF_J_CSPP_TELEMETERED),
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '11079364_PPD_CTD_telem.yml', RESOURCE_PATH)

        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles
        Assert that we have the correct number of particles
        """
        file_path = os.path.join(RESOURCE_PATH, RECOVERED_SAMPLE_DATA)
        stream_handle = open(file_path, 'rU')

        parser = CtdpfJCsppParser(self.config.get(DataTypeKey.CTDPF_J_CSPP_RECOVERED),
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(3404)

        log.debug("*** test_get_many Num particles %s", len(particles))
        self.assertEqual(len(particles), 3404)

        stream_handle.close()

        # Now do the same for the telemetered version
        file_path = os.path.join(RESOURCE_PATH, TELEMETERED_SAMPLE_DATA)
        stream_handle = open(file_path, 'rU')

        parser = CtdpfJCsppParser(self.config.get(DataTypeKey.CTDPF_J_CSPP_TELEMETERED),
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(218)

        log.debug("*** test_get_many Num particles %s", len(particles))
        self.assertEqual(len(particles), 218)

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists and a RecoverableSampleException is thrown.
        Note: every other data record has bad data (float instead of int, extra column etc.)
        """

        file_path = os.path.join(RESOURCE_PATH, '11079364_BAD_PPB_CTD.txt')
        stream_handle = open(file_path, 'rU')

        log.debug(self.exception_callback_value)

        parser = CtdpfJCsppParser(self.config.get(DataTypeKey.CTDPF_J_CSPP_RECOVERED),
                                  stream_handle,
                                  self.exception_callback)

        parser.get_records(1)

        log.debug("Exception callback value: %s", self.exception_callback_value)

        self.assertTrue(self.exception_callback_value is not None)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        self.assertEqual(len(self.exception_callback_value), 12)
        stream_handle.close()
