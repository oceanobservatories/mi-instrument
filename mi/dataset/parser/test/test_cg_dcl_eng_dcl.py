#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi/dataset/parser/test/test_cg_dcl_eng_dcl.py
@author Mark Worden
@brief Test code for a cg_dcl_eng_dcl data parser
"""
import os

from nose.plugins.attrib import attr

from mi.core.exceptions import ConfigurationException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_dcl_eng.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.cg_dcl_eng_dcl import CgDclEngDclParser, ParticleClassTypes, \
    CgDclEngDclMsgCountsRecoveredDataParticle, CgDclEngDclMsgCountsTelemeteredDataParticle, \
    CgDclEngDclCpuUptimeRecoveredDataParticle, CgDclEngDclCpuUptimeTelemeteredDataParticle, \
    CgDclEngDclErrorRecoveredDataParticle, CgDclEngDclErrorTelemeteredDataParticle, \
    CgDclEngDclGpsRecoveredDataParticle, CgDclEngDclGpsTelemeteredDataParticle, \
    CgDclEngDclPpsRecoveredDataParticle, CgDclEngDclPpsTelemeteredDataParticle, \
    CgDclEngDclSupervRecoveredDataParticle, CgDclEngDclSupervTelemeteredDataParticle, \
    CgDclEngDclDlogMgrRecoveredDataParticle, CgDclEngDclDlogMgrTelemeteredDataParticle, \
    CgDclEngDclDlogStatusRecoveredDataParticle, CgDclEngDclDlogStatusTelemeteredDataParticle, \
    CgDclEngDclStatusRecoveredDataParticle, CgDclEngDclStatusTelemeteredDataParticle, \
    CgDclEngDclDlogAarmRecoveredDataParticle, CgDclEngDclDlogAarmTelemeteredDataParticle
from mi.dataset.parser.utilities import particle_to_yml
from mi.dataset.test.test_parser import ParserUnitTestCase


log = get_logger()


@attr('UNIT', group='mi')
class CgParserUnitTestCase(ParserUnitTestCase):
    """
    cg_dcl_eng_dcl parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._exceptions_detected = 0

        self._recovered_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS:
                    CgDclEngDclMsgCountsRecoveredDataParticle,
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS:
                    CgDclEngDclCpuUptimeRecoveredDataParticle,
                ParticleClassTypes.ERROR_PARTICLE_CLASS:
                    CgDclEngDclErrorRecoveredDataParticle,
                ParticleClassTypes.GPS_PARTICLE_CLASS:
                    CgDclEngDclGpsRecoveredDataParticle,
                ParticleClassTypes.PPS_PARTICLE_CLASS:
                    CgDclEngDclPpsRecoveredDataParticle,
                ParticleClassTypes.SUPERV_PARTICLE_CLASS:
                    CgDclEngDclSupervRecoveredDataParticle,
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS:
                    CgDclEngDclDlogMgrRecoveredDataParticle,
                ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS:
                    CgDclEngDclDlogStatusRecoveredDataParticle,
                ParticleClassTypes.STATUS_PARTICLE_CLASS:
                    CgDclEngDclStatusRecoveredDataParticle,
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS:
                    CgDclEngDclDlogAarmRecoveredDataParticle,
                }
        }

        self._telemetered_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS:
                    CgDclEngDclMsgCountsTelemeteredDataParticle,
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS:
                    CgDclEngDclCpuUptimeTelemeteredDataParticle,
                ParticleClassTypes.ERROR_PARTICLE_CLASS:
                    CgDclEngDclErrorTelemeteredDataParticle,
                ParticleClassTypes.GPS_PARTICLE_CLASS:
                    CgDclEngDclGpsTelemeteredDataParticle,
                ParticleClassTypes.PPS_PARTICLE_CLASS:
                    CgDclEngDclPpsTelemeteredDataParticle,
                ParticleClassTypes.SUPERV_PARTICLE_CLASS:
                    CgDclEngDclSupervTelemeteredDataParticle,
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS:
                    CgDclEngDclDlogMgrTelemeteredDataParticle,
                ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS:
                    CgDclEngDclDlogStatusTelemeteredDataParticle,
                ParticleClassTypes.STATUS_PARTICLE_CLASS:
                    CgDclEngDclStatusTelemeteredDataParticle,
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS:
                    CgDclEngDclDlogAarmTelemeteredDataParticle,
                }
        }

        self._bad_recovered_config_1 = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        }

        self._bad_recovered_config_2 = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS:
                    CgDclEngDclMsgCountsRecoveredDataParticle,
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS:
                    CgDclEngDclCpuUptimeRecoveredDataParticle,
                ParticleClassTypes.GPS_PARTICLE_CLASS:
                    CgDclEngDclGpsRecoveredDataParticle,
                ParticleClassTypes.PPS_PARTICLE_CLASS:
                    CgDclEngDclPpsRecoveredDataParticle,
                ParticleClassTypes.SUPERV_PARTICLE_CLASS:
                    CgDclEngDclSupervRecoveredDataParticle,
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS:
                    CgDclEngDclDlogMgrRecoveredDataParticle,
                ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS:
                    CgDclEngDclDlogStatusRecoveredDataParticle,
                ParticleClassTypes.STATUS_PARTICLE_CLASS:
                    CgDclEngDclStatusRecoveredDataParticle,
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS:
                    CgDclEngDclDlogAarmRecoveredDataParticle,
                }
        }

        self._bad_telemetered_config_1 = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        }

        self._bad_telemetered_config_2 = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS:
                    CgDclEngDclMsgCountsTelemeteredDataParticle,
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS:
                    CgDclEngDclCpuUptimeTelemeteredDataParticle,
                ParticleClassTypes.GPS_PARTICLE_CLASS:
                    CgDclEngDclGpsTelemeteredDataParticle,
                ParticleClassTypes.ERROR_PARTICLE_CLASS:
                    CgDclEngDclErrorTelemeteredDataParticle,
                ParticleClassTypes.PPS_PARTICLE_CLASS:
                    CgDclEngDclPpsTelemeteredDataParticle,
                ParticleClassTypes.SUPERV_PARTICLE_CLASS:
                    CgDclEngDclSupervTelemeteredDataParticle,
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS:
                    CgDclEngDclDlogMgrTelemeteredDataParticle,
                ParticleClassTypes.STATUS_PARTICLE_CLASS:
                    CgDclEngDclStatusTelemeteredDataParticle,
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS:
                    CgDclEngDclDlogAarmTelemeteredDataParticle,
                }
        }

    def create_yml(self, particles, filename):
        particle_to_yml(particles, os.path.join(RESOURCE_PATH, filename))

    def exception_callback(self, exception):
        log.debug("Exception received: %s", exception)
        self._exceptions_detected += 1

    def test_happy_path(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated in processing the input files.
        """
        log.debug('===== START TEST HAPPY PATH =====')

        with open(os.path.join(RESOURCE_PATH, 'DCL26_20131220.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(100)
            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'recov.DCL26_20131220.syslog.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140915.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(100)
            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'recov.20140915.syslog.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, 'DCL26_20131220.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(100)
            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'telem.DCL26_20131220.syslog.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140915.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(100)
            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'telem.20140915.syslog.yml', RESOURCE_PATH)

        log.debug('===== END TEST HAPPY PATH =====')

    def test_invalid_fields(self):
        """
        This text method will ensure that invalid file records are found and reported.
        """
        log.debug('===== START TEST INVALID FIELDS =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(50)
            log.debug("Result: %s", particles)

            self.assertEqual(len(particles), 13)
            # PR 13713 - now preventing exceptions before extracting sample
            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'recov.invalid.syslog.yml', RESOURCE_PATH)

        self._exceptions_detected = 0

        with open(os.path.join(RESOURCE_PATH, 'invalid.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(50)
            log.debug("Result: %s", particles)

            self.assertEqual(len(particles), 13)
            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'telem.invalid.syslog.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID FIELDS =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file has no valid records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(1)
            log.debug("Result: %s", len(particles))

            self.assertEqual(len(particles), 0)
            self.assertEqual(self._exceptions_detected, 0)

        with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(1)
            log.debug("Result: %s", len(particles))

            self.assertEqual(len(particles), 0)
            self.assertEqual(self._exceptions_detected, 0)

        log.debug('===== END TEST NO PARTICLES =====')

    def test_bad_configuration(self):
        """
        Verify that no particles are produced if the input file has no valid records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with self.assertRaises(ConfigurationException):
            with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:

                CgDclEngDclParser(self._bad_recovered_config_1, file_handle,
                                  self.exception_callback)

        with self.assertRaises(ConfigurationException):
            with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:

                CgDclEngDclParser(self._bad_recovered_config_2, file_handle,
                                  self.exception_callback)

        with self.assertRaises(ConfigurationException):
            with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:

                CgDclEngDclParser(self._bad_telemetered_config_1, file_handle,
                                  self.exception_callback)

        with self.assertRaises(ConfigurationException):
            with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:

                CgDclEngDclParser(self._bad_telemetered_config_2, file_handle,
                                  self.exception_callback)

    def test_bug_1271_fix(self):
        """
        TBD
        """
        log.debug('===== START TEST BUG 1271 FIX =====')

        with open(os.path.join(RESOURCE_PATH, '20140626.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(100)

            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'recov.20140626.syslog.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140626.syslog.log')) as file_handle:
            parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                       self.exception_callback)

            particles = parser.get_records(100)

            self.assertEqual(self._exceptions_detected, 0)
            self.assert_particles(particles, 'telem.20140626.syslog.yml', RESOURCE_PATH)
