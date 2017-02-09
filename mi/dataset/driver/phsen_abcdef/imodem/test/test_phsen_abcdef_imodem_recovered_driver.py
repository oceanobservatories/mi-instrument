
from mi.core.log import get_logger


import re
import unittest
import os
from mi.dataset.driver.phsen_abcdef.imodem.phsen_abcdef_imodem_recovered_driver import parse
from mi.dataset.driver.phsen_abcdef.imodem.resource import RESOURCE_PATH

from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'Joe Padula'
log = get_logger()


class SampleTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join(RESOURCE_PATH, 'phsen1_20140730_190554.DAT')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()
