#!/usr/bin/env python

import os
import unittest

from mi.logging import log
from mi.dataset.driver.zplsc_c.dcl.zplsc_c_dcl_telemetered_driver import parse
from mi.dataset.driver.zplsc_c.dcl.resource import RESOURCE_PATH
from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'rronquillo'


class DriverTest(unittest.TestCase):

    source_file_path = os.path.join(RESOURCE_PATH, '20150407.zplsc.log')

    def test_one(self):

        particle_data_handler = parse(None, self.source_file_path,
                                       ParticleDataHandler())

        log.info("SAMPLES: %s", particle_data_handler._samples)
        log.info("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)

if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()

