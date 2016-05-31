"""
@package mi.instrument.uw.bars_no_hyd.ooicore.test.test_driver
@file mi/instrument/uw/bars_no_hyd/ooicore/test/test_driver.py
@author Steve Foley
@brief Test cases for TRHPH instrument with no hydrogen sensors driver
"""

__author__ = 'Kirk Hunt'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger

log = get_logger()

from mi.idk.unit_test import InstrumentDriverTestCase

from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import DriverConfigKey

from mi.instrument.uw.bars.ooicore.driver import Parameter
from mi.instrument.uw.bars.ooicore.driver import DataParticleType

from mi.instrument.uw.bars.ooicore.test.test_driver import DriverUnitTest

from mi.instrument.uw.bars_no_hyd.ooicore.driver import BarsNoHydProtocol

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.uw.bars_no_hyd.ooicore.driver',
    driver_class="BarsNoHydInstrumentDriver",
    instrument_agent_resource_id='QN341A',
    instrument_agent_name='uw_bars_ooicore',
    instrument_agent_packet_config=DataParticleType(),
    driver_startup_config={
        DriverConfigKey.PARAMETERS: {Parameter.CYCLE_TIME: 20,
                                     Parameter.METADATA_POWERUP: 0,
                                     Parameter.METADATA_RESTART: 0,
                                     Parameter.RUN_ACQUIRE_STATUS_INTERVAL: '00:10:00'}}
)

###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class BarsNoHydDriverUnitTest(DriverUnitTest):
    def setUp(self):
        DriverUnitTest.setUp(self)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, self.VALID_SAMPLE_01, self.assert_particle_sample, True)
        self.assert_particle_published(driver, self.VALID_SAMPLE_02, self.assert_particle_sample_2, True)