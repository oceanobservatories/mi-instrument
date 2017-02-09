#!/usr/bin/env python

import os
import unittest

from mi.core.log import get_logger
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.adcpt_acfgm.dcl.pd0.adcpt_acfgm_dcl_pd0_recovered_driver import parse
from mi.dataset.driver.adcpt_acfgm.dcl.pd0.resource import RESOURCE_PATH

__author__ = 'Jeff Roy'

log = get_logger()


class SampleTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join(RESOURCE_PATH, '20140424.adcpt.log')
        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()
