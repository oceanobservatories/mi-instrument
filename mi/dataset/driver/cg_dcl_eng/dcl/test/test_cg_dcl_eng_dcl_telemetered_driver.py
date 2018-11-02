#!/usr/bin/env python

import os
import unittest
import yaml

from mi.core.log import log
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.cg_dcl_eng.dcl.cg_dcl_eng_dcl_telemetered_driver import parse
from mi.dataset.driver.cg_dcl_eng.dcl.resource import RESOURCE_PATH

<<<<<<< HEAD
__author__ = 'dmergens'
=======
__author__ = 'mworden'
>>>>>>> 592de749f594c712e1d6def6dd1405e44882e62f


class DriverTest(unittest.TestCase):

    # TODO - move this to the unittest base class
    def assertNoParticleRegression(self, filepath, data_handler):
        """
        Compares particles with previous run. If no YAML file exists, creates one.
        :param filepath:  fully qualified name of the input file (that was parsed)
        :param data_handler:  ParticleDataHandler returned from parse()
        :return:
        """
        yaml_file = os.path.splitext(filepath)[0] + '.yml'
        particles = data_handler._samples
        if os.path.isfile(yaml_file):
            with open(yaml_file, 'r') as stream:
                prev_particles = yaml.load(stream)
                # particle key names should match
                self.assertListEqual(sorted(prev_particles.keys()), sorted(particles.keys()))
                # compare number of samples across one of the particle keys
<<<<<<< HEAD
                for p in prev_particles.keys():
                    log.debug('%s: %d %d', p, len(prev_particles[p]), len(particles[p]))
                    self.assertEqual(len(prev_particles[p]), len(particles[p]))
=======
                self.assertEqual(len(sorted(prev_particles.items())[0][1]), len(sorted(particles.items())[0][1]))
>>>>>>> 592de749f594c712e1d6def6dd1405e44882e62f
        else:
            with open(yaml_file, 'w') as stream:
                log.warn('creating yaml output file for regression testing - commit %s', yaml_file)
                yaml.dump(particles, stream, default_flow_style=False)

    def test_one(self):
        # log.setLevel('DEBUG')

        source_file_path = os.path.join(RESOURCE_PATH, '20140915.syslog.log')

        particle_data_handler = ParticleDataHandler()  # does this do anything?

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.info("SAMPLES: %s", particle_data_handler._samples)
        log.info("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)
        self.assertNoParticleRegression(source_file_path, particle_data_handler)

    def test_13694(self):
        source_file_path = os.path.join(RESOURCE_PATH, '20181010-bad.syslog.log')
        particle_data_handler = ParticleDataHandler()
        particle_data_handler = parse(None,
                                      source_file_path=source_file_path,
                                      particle_data_handler=particle_data_handler)

        log.debug('SAMPLES: %s', particle_data_handler._samples)
        log.debug('FAILURE: %s', particle_data_handler._failure)

<<<<<<< HEAD
        self.assertEquals(particle_data_handler._failure, True)
=======
        self.assertEquals(particle_data_handler._failure, False)
>>>>>>> 592de749f594c712e1d6def6dd1405e44882e62f
        self.assertNoParticleRegression(source_file_path, particle_data_handler)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()

    test = DriverTest('test_one')
    test.test_13694()
