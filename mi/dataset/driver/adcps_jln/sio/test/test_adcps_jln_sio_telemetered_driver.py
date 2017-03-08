#!/usr/bin/env python
import os
import unittest

from mi.core.log import get_logger
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.adcps_jln.sio.adcps_jln_sio_telemetered_driver import parse
from mi.dataset.driver.adcps_jln.sio.resource import RESOURCE_PATH

__author__ = 'Joe Padula'

log = get_logger()


class SampleTest(unittest.TestCase):
    def test_one(self):

        source_file_path = os.path.join(RESOURCE_PATH, 'node59p1_2.adcps.dat')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()