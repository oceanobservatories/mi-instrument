import os
import unittest

from mi.core.log import get_logger
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.plims_a.plims_a_adc_driver import \
    parse as parse_telemetered
from mi.dataset.driver.plims_a.plims_a_adc_recovered_driver import \
    parse as parse_recovered
from mi.dataset.driver.plims_a.resource import RESOURCE_PATH

__author__ = 'Joffrey Peters'
log = get_logger()


class PlimsAAdcDriverTest(unittest.TestCase):

    def test_telemetered(self):

        source_file_path = os.path.join(RESOURCE_PATH, 'D20231021T175900_IFCB199.adc')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse_telemetered(None, source_file_path, particle_data_handler)

        log.debug("Telemetered samples: %s", particle_data_handler._samples)
        log.debug("Telemetered failures: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)

    def test_recovered(self):

        source_file_path = os.path.join(RESOURCE_PATH, 'D20231021T175900_IFCB199.adc')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse_recovered(None, source_file_path, particle_data_handler)

        log.debug("Recovered samples: %s", particle_data_handler._samples)
        log.debug("Recovered failures: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)

if __name__ == '__main__':
    test = PlimsAAdcDriverTest('test_one')
    test.test_one()
