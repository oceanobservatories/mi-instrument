#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi/dataset/parser/test/test_ctdmo_ghqr_imodem.py
@author Mark Worden
@brief Test code for a ctdmo_ghqr_imodem data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import ConfigurationException, UnexpectedDataException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.ctdmo_ghqr.imodem.resource import RESOURCE_PATH
from mi.dataset.parser.ctdmo_ghqr_imodem import CtdmoGhqrImodemParser, \
    CtdmoGhqrImodemParticleClassKey, \
    CtdmoGhqrImodemMetadataTelemeteredDataParticle, \
    CtdmoGhqrImodemMetadataRecoveredDataParticle, \
    CtdmoGhqrImodemInstrumentTelemeteredDataParticle, \
    CtdmoGhqrImodemInstrumentRecoveredDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class CtdmoGhqrImodemParserUnitTestCase(ParserUnitTestCase):
    """
    Cg_stc_eng_stc Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._telemetered_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE:
                'mi.dataset.parser.ctdmo_ghqr_imodem',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                CtdmoGhqrImodemParticleClassKey.METADATA_PARTICLE_CLASS:
                    CtdmoGhqrImodemMetadataTelemeteredDataParticle,
                CtdmoGhqrImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    CtdmoGhqrImodemInstrumentTelemeteredDataParticle,
            }
        }

        self._recovered_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE:
                'mi.dataset.parser.ctdmo_ghqr_imodem',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                CtdmoGhqrImodemParticleClassKey.METADATA_PARTICLE_CLASS:
                    CtdmoGhqrImodemMetadataRecoveredDataParticle,
                CtdmoGhqrImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    CtdmoGhqrImodemInstrumentRecoveredDataParticle,
            }
        }

    def test_happy_path(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST HAPPY PATH =====')

        with open(os.path.join(RESOURCE_PATH, 'ctdmo01_20140712_120719.DAT'), 'r') as file_handle:
            parser = CtdmoGhqrImodemParser(self._telemetered_config, file_handle, self.exception_callback)

            particles = parser.get_records(1000)

            self.assertEqual(self.exception_callback_value, [])

            self.particle_to_yml(particles, 'ctdmo01_20140712_120719_telem.yml')

            self.assert_particles(particles, "ctdmo01_20140712_120719_telem.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, 'ctdmo01_20140712_120719.DAT'), 'r') as file_handle:
            parser = CtdmoGhqrImodemParser(self._recovered_config, file_handle, self.exception_callback)

            particles = parser.get_records(1000)

            self.assertEqual(self.exception_callback_value, [])

            self.particle_to_yml(particles, 'ctdmo01_20140712_120719_recov.yml')

            self.assert_particles(particles, "ctdmo01_20140712_120719_recov.yml", RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

        log.debug('===== END TEST HAPPY PATH =====')

    def test_bad_config(self):
        """
        The test ensures that a ConfigurationException is raised when providing the
        parser invalid configuration
        """
        log.debug('===== START TEST BAD CONFIG =====')

        with self.assertRaises(ConfigurationException):
            with open(os.path.join(RESOURCE_PATH, 'ctdmo01_20140712_120719.DAT'), 'r') as file_handle:

                config = {
                    DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_imodem',
                    DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                    DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: None
                }

                CtdmoGhqrImodemParser(config, file_handle, self.exception_callback)

        log.debug('===== END TEST BAD CONFIG =====')

    def test_unexpected_data(self):
        """
        This test verifies that an unexpected data exception is reported when unexpected data
        is found.
        """
        log.debug('===== START TEST UNEXPECTED DATA =====')

        with open(os.path.join(RESOURCE_PATH, 'unexpected_data.DAT'), 'r') as file_handle:

            parser = CtdmoGhqrImodemParser(self._telemetered_config, file_handle, self.exception_callback)

            parser.get_records(1)

            self.assertEqual(len(self.exception_callback_value), 2)

            for exception in self.exception_callback_value:
                self.assertIsInstance(exception, UnexpectedDataException)

        log.debug('===== END TEST UNEXPECTED DATA =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')


        log.debug('===== END TEST NO PARTICLES =====')

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
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                elif val.get('value') is None:
                    fid.write("    %s: !!null\n" % (val.get('value_id')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()
