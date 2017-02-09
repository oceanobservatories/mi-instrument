#!/usr/bin/env python

"""
@package mi.dataset.driver.fuelcell_eng.dcl.test
@file mi-dataset/mi/dataset/driver/fuelcell_eng/dcl/test_fuelcell_eng_dcl_telemetered_driver.py
@author Chris Goodrich
@brief Sample test for test_fuelcell_eng_dcl_telemetered_driver

Release notes:

Initial Release
"""

import os
import unittest

from mi.core.log import get_logger
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.fuelcell_eng.dcl.fuelcell_eng_dcl_telemetered_driver import parse
from mi.dataset.driver.fuelcell_eng.dcl.resource import RESOURCE_PATH

log = get_logger()


class SampleTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join(RESOURCE_PATH, '20141207s.pwrsys.log')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()
