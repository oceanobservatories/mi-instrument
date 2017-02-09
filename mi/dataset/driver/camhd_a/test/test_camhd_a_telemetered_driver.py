#!/usr/bin/env python

import os
import unittest

from mi.logging import log
from mi.dataset.driver.camhd_a.camhd_a_telemetered_driver import parse
from mi.dataset.driver.camhd_a.resource import RESOURCE_PATH
from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'rronquillo'


class DriverTest(unittest.TestCase):

    source_file_path = os.path.join(
        RESOURCE_PATH, 'RS03ASHS-PN03B-06-CAMHDA301', '2015',
        '11', '19', 'CAMHDA301-20151119T210000Z.mp4')

    def test_one(self):

        particle_data_handler = parse(None, self.source_file_path,
                                       ParticleDataHandler())

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()

