"""
@package mi.dataset.parser.test.test_parad_j_cspp
@file marine-integrations/mi/dataset/parser/test/test_parad_j_cspp.py
@author Joe Padula
@brief Test code for a parad_j_cspp data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH, ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY

from mi.dataset.parser.parad_j_cspp import \
    ParadJCsppParser, \
    ParadJCsppInstrumentTelemeteredDataParticle, \
    ParadJCsppMetadataTelemeteredDataParticle, \
    ParadJCsppInstrumentRecoveredDataParticle, \
    ParadJCsppMetadataRecoveredDataParticle

log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH,
                             'parad_j', 'cspp', 'resource')

O_MODE = 'rU'   # Universal Open mode


@attr('UNIT', group='mi')
class ParadJCsppParserUnitTestCase(ParserUnitTestCase):
    """
    parad_j_cspp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._telemetered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.parad_j_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: ParadJCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: ParadJCsppInstrumentTelemeteredDataParticle,
            }
        }

        self._recovered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.parad_j_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: ParadJCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: ParadJCsppInstrumentRecoveredDataParticle,
            }
        }

        # Define test data particles and their associated timestamps which will be 
        # compared with returned results

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
                    fid.write('    %s: \'%s\'\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """

        fid = open(os.path.join(RESOURCE_PATH, '11079364_PPD_PARS.txt'), O_MODE)

        stream_handle = fid
        parser = ParadJCsppParser(self._telemetered_parser_config,
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(20)

        self.particle_to_yml(particles, '11079364_PPD_PARS_telem.yml')
        fid.close()

    def test_simple(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """

        # Recovered
        file_path = os.path.join(RESOURCE_PATH, '11079364_PPB_PARS.txt')
        stream_handle = open(file_path, O_MODE)

        parser = ParadJCsppParser(self._recovered_parser_config,
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        # check all the values against expected results.

        self.assert_particles(particles, "11079364_PPB_PARS_recov.yml", RESOURCE_PATH)

        stream_handle.close()

       # Telemetered
        file_path = os.path.join(RESOURCE_PATH, '11079364_PPD_PARS.txt')
        stream_handle = open(file_path, O_MODE)

        parser = ParadJCsppParser(self._telemetered_parser_config,
                                  stream_handle,
                                  self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        # check all the values against expected results.

        self.assert_particles(particles, "11079364_PPD_PARS_telem.yml", RESOURCE_PATH)

        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        file_path = os.path.join(RESOURCE_PATH, '11079364_PPB_PARS.txt')
        stream_handle = open(file_path, O_MODE)

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = ParadJCsppParser(self._recovered_parser_config,
                                  stream_handle,
                                  self.exception_callback)

        # try to get 2000 particles, there are only 194 data records
        # so should get 195 including the meta data
        particles = parser.get_records(2000)

        log.debug("*** test_get_many Num particles %s", len(particles))
        self.assertEqual(len(particles), 195)

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists and a RecoverableSampleException is thrown.
        """

        file_path = os.path.join(RESOURCE_PATH, '11079364_BAD_PPB_PARS.txt')
        stream_handle = open(file_path, O_MODE)

        log.debug(self.exception_callback_value)

        parser = ParadJCsppParser(self._recovered_parser_config,
                                  stream_handle,
                                  self.exception_callback)

        parser.get_records(1)

        log.debug("Exception callback value: %s", self.exception_callback_value)

        self.assertTrue(self.exception_callback_value is not None)
        # 14 bad records
        self.assertEqual(len(self.exception_callback_value), 14)
        stream_handle.close()

    def test_additional_column(self):
        """
        Ensure that additional column of data will cause an exception.
        """

        file_path = os.path.join(RESOURCE_PATH, '11079364_PPB_PARS_ADDED_COLUMN.txt')
        stream_handle = open(file_path, O_MODE)

        log.debug(self.exception_callback_value)

        parser = ParadJCsppParser(self._recovered_parser_config,
                                  stream_handle,
                                  self.exception_callback)

        parser.get_records(1)

        log.debug("Exception callback value: %s", self.exception_callback_value)

        self.assertTrue(self.exception_callback_value is not None)

        stream_handle.close()