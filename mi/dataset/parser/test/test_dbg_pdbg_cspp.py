#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_dbg_pdbg_cspp.py
@author Jeff Roy
@brief Test code for a dbg_pdbg_cspp data parser

dbg_pdbg_cspp is based on cspp_base.py
test_dosta_abcdjm_cspp.py fully tests all of the capabilities of the
base parser.  That level of testing is omitted from this test suite
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase, BASE_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY

from mi.dataset.parser.dbg_pdbg_cspp import \
    DbgPdbgCsppParser, \
    DbgPdbgRecoveredBatteryParticle, \
    DbgPdbgTelemeteredBatteryParticle, \
    DbgPdbgRecoveredGpsParticle, \
    DbgPdbgTelemeteredGpsParticle, \
    DbgPdbgMetadataRecoveredDataParticle, \
    DbgPdbgMetadataTelemeteredDataParticle, \
    DbgPdbgDataTypeKey, \
    BATTERY_STATUS_CLASS_KEY, \
    GPS_ADJUSTMENT_CLASS_KEY

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'dbg_pdbg', 'cspp', 'resource')


@attr('UNIT', group='mi')
class DbgPdbgCsppParserUnitTestCase(ParserUnitTestCase):
    """
    dbg_pdbg_cspp Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DbgPdbgDataTypeKey.DBG_PDBG_CSPP_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: DbgPdbgMetadataTelemeteredDataParticle,
                    BATTERY_STATUS_CLASS_KEY: DbgPdbgTelemeteredBatteryParticle,
                    GPS_ADJUSTMENT_CLASS_KEY: DbgPdbgTelemeteredGpsParticle
                }
            },
            DbgPdbgDataTypeKey.DBG_PDBG_CSPP_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: DbgPdbgMetadataRecoveredDataParticle,
                    BATTERY_STATUS_CLASS_KEY: DbgPdbgRecoveredBatteryParticle,
                    GPS_ADJUSTMENT_CLASS_KEY: DbgPdbgRecoveredGpsParticle
                }
            },
        }

    def test_simple(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.

        Because most of these files are ignored and there are only a few
        records of useful data in each one test_simple is the primary test
        There is no need for a test_get_many or other tests to get more particles
        """
        file_path = os.path.join(RESOURCE_PATH, '01554008_DBG_PDBG.txt')
        stream_handle = open(file_path, 'r')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = DbgPdbgCsppParser(self.config.get(DbgPdbgDataTypeKey.DBG_PDBG_CSPP_RECOVERED),
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(8)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '01554008_DBG_PDBG_recov.yml', RESOURCE_PATH)

        stream_handle.close()

    def test_simple_telem(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.

        Because most of these files are ignored and there are only a few
        records of useful data in each one test_simple is the primary test
        There is no need for a test_get_many or other tests to get more particles
        """
        file_path = os.path.join(RESOURCE_PATH, '01554008_DBG_PDBG.txt')
        stream_handle = open(file_path, 'r')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = DbgPdbgCsppParser(self.config.get(DbgPdbgDataTypeKey.DBG_PDBG_CSPP_TELEMETERED),
                                   stream_handle,
                                   self.exception_callback)

        particles = parser.get_records(8)

        log.debug("*** test_simple Num particles %s", len(particles))

        self.assert_particles(particles, '01554008_DBG_PDBG_telem.yml', RESOURCE_PATH)

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # the first useful record in this file is corrupted and will be ignored
        # we expect to get the metadata particle with the
        # timestamp from the 2nd data record and all of the valid engineering
        # data records

        file_path = os.path.join(RESOURCE_PATH, '01554008_BAD_DBG_PDBG.txt')
        stream_handle = open(file_path, 'r')

        log.info(self.exception_callback_value)

        parser = DbgPdbgCsppParser(self.config.get(DbgPdbgDataTypeKey.DBG_PDBG_CSPP_RECOVERED),
                                   stream_handle,
                                   self.exception_callback)

        # 18 particles
        particles = parser.get_records(7)

        self.assert_particles(particles, 'DBG_PDBG_bad_data_records.yml', RESOURCE_PATH)

        stream_handle.close()

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))


