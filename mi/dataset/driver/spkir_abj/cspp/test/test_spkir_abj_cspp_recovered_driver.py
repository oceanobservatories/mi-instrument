
"""
@package mi.dataset.driver.spkir_abj.cspp.test
@file mi/dataset/driver/spikr_abj/cspp/test/test_spkir_abj_cspp_recovered_driver.py
@author Mark Worden
@brief Minimal test code to exercise the driver parse method for spkir_abj_cspp recovered

Release notes:

Initial Release
"""

import os
import unittest

from mi.core.log import get_logger
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.spkir_abj.cspp.resource import RESOURCE_PATH
from mi.dataset.driver.spkir_abj.cspp.spkir_abj_cspp_recovered_driver import parse

__author__ = 'mworden'
log = get_logger()


class DriverTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join(RESOURCE_PATH, '11079419_PPB_OCR.txt')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
