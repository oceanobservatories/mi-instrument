#!/usr/bin/env python

import os
import unittest

from mi.logging import log
from mi.dataset.parser.camhd_a import CamhdAParser
from mi.dataset.driver.camhd_a.resource import RESOURCE_PATH
from mi.dataset.driver.camhd_a.camhd_a_telemetered_driver import parse
from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'rronquillo'


class DriverTest(unittest.TestCase):

    source_file_path = os.path.join(RESOURCE_PATH, 'CAMHDA301-20190104T000000Z.log')

    def test_find_matching_mp4(self):
        log_file = '/rsn_cabled/rsn_data/DVT_Data/camhda301/logs/CAMHDA301-20160628T000000Z.log'
        expected_mp4_file = 'RS03ASHS/PN03B/06-CAMHDA301/2016/06/28/CAMHDA301-20160628T000000Z.mp4'
        mp4_file = CamhdAParser.find_matching_mp4(log_file, 'CAMHDA301', '20160628')
        self.assertEqual(mp4_file, expected_mp4_file)

    def test_one(self):

        particle_data_handler = parse(None, self.source_file_path, ParticleDataHandler())

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)
        self.assertEquals(len(particle_data_handler._samples), 1)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()

