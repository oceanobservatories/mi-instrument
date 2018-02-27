#!/usr/bin/env python

import os
import unittest

from mi.logging import log
from mi.dataset.driver.ctdav_n.auv.ctdav_n_auv_telemetered_driver import parse as parse_telemetered
from mi.dataset.driver.ctdav_n.auv.ctdav_n_auv_recovered_driver import parse as parse_recovered
from mi.dataset.driver.ctdav_n.auv.resource import RESOURCE_PATH
from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'Rene Gelinas'


class DriverTest(unittest.TestCase):

    source_file_path = os.path.join(RESOURCE_PATH, 'subset_reduced.csv')

    def test_telemetered_deprecation(self):

        particle_data_handler = parse_telemetered(None, self.source_file_path, ParticleDataHandler())

        log.info("SAMPLES: %s", particle_data_handler._samples)
        log.info("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)

    def test_recovered_deprecation(self):

        particle_data_handler = parse_recovered(None, self.source_file_path, ParticleDataHandler())

        log.info("SAMPLES: %s", particle_data_handler._samples)
        log.info("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = DriverTest('deprecation_tests')
    test.test_telemetered_deprecation()
    test.test_recovered_deprecation()
