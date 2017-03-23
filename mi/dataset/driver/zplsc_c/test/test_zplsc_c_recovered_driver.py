#!/usr/bin/env python

import os
import unittest

from mi.logging import log
from mi.dataset.driver.zplsc_c.zplsc_c_recovered_driver import parse
from mi.dataset.driver.zplsc_c.resource import RESOURCE_PATH
from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'Rene Gelinas'


class DriverTest(unittest.TestCase):

    source_file_path = os.path.join(RESOURCE_PATH, '15100520-Test.01A')

    def test_one(self):

        particle_data_handler = parse(None, self.source_file_path, ParticleDataHandler())

        log.info("SAMPLES: %s", particle_data_handler._samples)
        log.info("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)

if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
