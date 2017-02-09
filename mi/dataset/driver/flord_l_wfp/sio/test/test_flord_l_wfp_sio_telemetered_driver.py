#!/usr/bin/env python

from mi.core.log import get_logger
from mi.dataset.driver.flord_l_wfp.sio.resource import RESOURCE_PATH

import unittest
import os
from mi.dataset.driver.flord_l_wfp.sio.flord_l_wfp_sio_telemetered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'mworden'

log = get_logger()


class DriverTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join(RESOURCE_PATH, 'node58p1_0.we_wfp.dat')

        particle_data_handler = ParticleDataHandler()
        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)

    def test_two(self):

        source_file_path = os.path.join(RESOURCE_PATH, 'node17p1_33.we_wfp-new-header.dat')

        particle_data_handler = ParticleDataHandler()
        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
    test = DriverTest('test_two')
    test.test_two()
