"""
@package mi.instrument.uw.bars.ooicore.test.test_driver
@file /Users/foley/sandbox/ooici/marine-integrations/mi/instrument/uw/bars/ooicore/driver.py
@author Steve Foley
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u
       $ bin/test_driver -i
       $ bin/test_driver -q

   * From pyon
       $ bin/nosetests -s -v /Users/foley/sandbox/ooici/marine-integrations/mi/instrument/uw/bars/ooicore
       $ bin/nosetests -s -v /Users/foley/sandbox/ooici/marine-integrations/mi/instrument/uw/bars/ooicore -a UNIT
       $ bin/nosetests -s -v /Users/foley/sandbox/ooici/marine-integrations/mi/instrument/uw/bars/ooicore -a INT
       $ bin/nosetests -s -v /Users/foley/sandbox/ooici/marine-integrations/mi/instrument/uw/bars/ooicore -a QUAL
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger

log = get_logger()

from mi.core.instrument.instrument_driver import ResourceAgentState

from mi.idk.unit_test import InstrumentDriverTestCase, ParameterTestConfigKey
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import AgentCapabilityType

from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConfigKey

from mi.core.exceptions import SampleException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException

from mi.instrument.uw.bars.ooicore.driver import Protocol, MENU, BarsStatusParticleKey, BarsStatusParticle, ScheduledJob
from mi.instrument.uw.bars.ooicore.driver import InstrumentDriver
from mi.instrument.uw.bars.ooicore.driver import ProtocolState
from mi.instrument.uw.bars.ooicore.driver import ProtocolEvent
from mi.instrument.uw.bars.ooicore.driver import Parameter
from mi.instrument.uw.bars.ooicore.driver import Command
from mi.instrument.uw.bars.ooicore.driver import Capability
from mi.instrument.uw.bars.ooicore.driver import SubMenu
from mi.instrument.uw.bars.ooicore.driver import Prompt
from mi.instrument.uw.bars.ooicore.driver import BarsDataParticleKey
from mi.instrument.uw.bars.ooicore.driver import DataParticleType


###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.uw.bars.ooicore.driver',
    driver_class="InstrumentDriver",
    instrument_agent_resource_id='QN341A',
    instrument_agent_name='uw_bars_ooicore',
    instrument_agent_packet_config=DataParticleType(),
    driver_startup_config={
        DriverConfigKey.PARAMETERS: {Parameter.CYCLE_TIME: 20,
                                     Parameter.METADATA_POWERUP: 0,
                                     Parameter.METADATA_RESTART: 0,
                                     Parameter.RUN_ACQUIRE_STATUS_INTERVAL: '00:10:00'}}
)

#################################### RULES ####################################
#                                                                             #
# Common capabilities in the base class                                       #
#                                                                             #
# Instrument specific stuff in the derived class                              #
#                                                                             #
# Generator spits out either stubs or comments describing test this here,     #
# test that there.                                                            #
#                                                                             #
# Qualification tests are driven through the instrument_agent                 #
#                                                                             #
###############################################################################

STARTUP_TIMEOUT = 120
EXECUTE_TIMEOUT = 60

CYCLE_TIME_VALUE = 20
EH_ISOLATION_AMP_POWER_VALUE = 1
HYDROGEN_POWER_VALUE = 1
INST_AMP_POWER_VALUE = 1
METADATA_POWERUP_VALUE = 0
METADATA_RESTART_VALUE = 0
REFERENCE_TEMP_POWER_VALUE = 1
RES_SENSOR_POWER_VALUE = 1
VERBOSE_VALUE = 0

# newline.
NEWLINE = '\r\n'


###############################################################################
#                           DRIVER TEST MIXIN        		                  #
#     Defines a set of constants and assert methods used for data particle    #
#     verification 														      #
#                                                                             #
#  In python mixin classes are classes designed such that they wouldn't be    #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.									  #
###############################################################################
class TRHPHMixinSub(DriverTestMixin):

    """
    Mixin class used for storing data particle constants and common data assertion methods.
    """

    InstrumentDriver = InstrumentDriver
    # Create some short names for the parameter test config
    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    DA = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES

    INVALID_SAMPLE = "This is an invalid sample; it had better cause an exception."
    VALID_SAMPLE_01 = "0.010  0.020  0.030  0.040  0.021  0.042  1.999  1.173  20.75  0.016   24.7   9.3" + NEWLINE
    VALID_SAMPLE_02 = "0.090  0.080  0.070  0.060  0.025  0.045  2.999  1.173  20.75  0.019   27.4   8.5" + NEWLINE

    VALID_STATUS_01 = "System Name:  Temperature Resistivity Probe - TRHPH" + NEWLINE + \
    "System Owner: Consortium for Ocean Leadership" + NEWLINE + \
    "            University of Washington, OOI-RCN" + NEWLINE + \
    "Contact Info: Giora Proskurowski, 206-685-3507" + NEWLINE + \
    "System Serial #: 002" + NEWLINE + \
    "Software Version 1.25, Last Update April 14, 2012" + NEWLINE + \
    "Made by Ocean Engineering Services, March 2012 (206-543-9688)" + NEWLINE + \
    NEWLINE + \
    "This System is presently setup with the following Parameters:" + NEWLINE +\
    "1  = Eprom Status (0 means not setup yet, 1 means ready to use)" + NEWLINE +\
    "20 = Cycle Time (Actual Time in Seconds or Minutes)" + NEWLINE +\
    "0  = Minutes or Seconds Cycle Time (0 = Seconds mode, 1 = Minutes mode)" + NEWLINE +\
    "55 = Power Control Word (Power Control Word = 1+2+4+16+32 = 55 if all On)" + NEWLINE +\
    "1  = Res Power Status (0 = Off, 1 = On)" + NEWLINE +\
    "1  = Thermocouple & Hydrogen Amp Power Status (0 = Off, 1 = On)" + NEWLINE +\
    "1  = eh Amp Power Status (0 = Off, 1 = On)" + NEWLINE +\
    "1  = Hydrogen Sensor Power Status (0 = Off, 1 = On)" + NEWLINE +\
    "1  = Reference Temperature Power Status (0 = Off, 1 = On)" + NEWLINE +\
    "0  = Metadata Print Status on Power up (0 = No Print on Powerup, 1 = Print)" + NEWLINE +\
    "0  = Metadata Print Status on Restart Data Collection (0 = No Print on Restart Data, 1 = Print)" + NEWLINE

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.AUTOSAMPLE]},
        Capability.ACQUIRE_STATUS: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
    }

    ###
    #  Parameter and Type Definitions
    ###
    _driver_parameters = {
        # Parameters defined in the IOS

        Parameter.CYCLE_TIME: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 20, VALUE: 20},
        Parameter.VERBOSE: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0, REQUIRED: False},
        Parameter.METADATA_POWERUP: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        Parameter.METADATA_RESTART: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        Parameter.RES_SENSOR_POWER: {TYPE: int, READONLY: True, DA: False, STARTUP: False, DEFAULT: 1, VALUE: 1},
        Parameter.INST_AMP_POWER: {TYPE: int, READONLY: True, DA: False, STARTUP: False, DEFAULT: 1, VALUE: 1},
        Parameter.EH_ISOLATION_AMP_POWER: {TYPE: int, READONLY: True, DA: False, STARTUP: False, DEFAULT: 1, VALUE: 1},
        Parameter.HYDROGEN_POWER: {TYPE: int, READONLY: True, DA: False, STARTUP: False, DEFAULT: 1, VALUE: 1},
        Parameter.REFERENCE_TEMP_POWER: {TYPE: int, READONLY: True, DA: False, STARTUP: False, DEFAULT: 1, VALUE: 1},
        Parameter.RUN_ACQUIRE_STATUS_INTERVAL: {TYPE: str, READONLY: False, DA: False, STARTUP: True, DEFAULT: '00:00:00', VALUE: '12:00:00'},

    }

    _status_parameters = {
        BarsStatusParticleKey.SYSTEM_INFO: {TYPE: unicode, VALUE:
                                            "System Name:  Temperature Resistivity Probe - TRHPH" + NEWLINE +
                                            "System Owner: Consortium for Ocean Leadership" + NEWLINE +
                                            "            University of Washington, OOI-RCN" + NEWLINE +
                                            "Contact Info: Giora Proskurowski, 206-685-3507" + NEWLINE +
                                            "System Serial #: 002" + NEWLINE +
                                            "Software Version 1.25, Last Update April 14, 2012" + NEWLINE +
                                            "Made by Ocean Engineering Services, March 2012 (206-543-9688)" + NEWLINE,
                                            REQUIRED: True},
        BarsStatusParticleKey.EPROM_STATUS: {TYPE: int, VALUE: 1, REQUIRED: True},
        BarsStatusParticleKey.CYCLE_TIME: {TYPE: int, VALUE: 20, REQUIRED: True},
        BarsStatusParticleKey.CYCLE_TIME_UNIT: {TYPE: int, VALUE: 0, REQUIRED: True},
        BarsStatusParticleKey.POWER_CONTROL_WORD: {TYPE: int, VALUE: 55, REQUIRED: True},
        BarsStatusParticleKey.RES_POWER: {TYPE: int, VALUE: 1, REQUIRED: True},
        BarsStatusParticleKey.THERMO_HYDRO_AMP_POWER: {TYPE: int, VALUE: 1, REQUIRED: True},
        BarsStatusParticleKey.EH_AMP_POWER: {TYPE: int, VALUE: 1, REQUIRED: True},
        BarsStatusParticleKey.HYDRO_SENSOR_POWER: {TYPE: int, VALUE: 1, REQUIRED: True},
        BarsStatusParticleKey.REF_TEMP_POWER: {TYPE: int, VALUE: 1, REQUIRED: True},
        BarsStatusParticleKey.METADATA_ON_POWERUP: {TYPE: int, VALUE: 0, REQUIRED: True},
        BarsStatusParticleKey.METADATA_ON_RESTART: {TYPE: int, VALUE: 0, REQUIRED: True},

    }

    _sample_parameters_2 = {
        BarsDataParticleKey.RESISTIVITY_5: {TYPE: float, VALUE: 0.090, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_X1: {TYPE: float, VALUE: 0.080, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_X5: {TYPE: float, VALUE: 0.070, REQUIRED: True},

        # At the moment, none of the BARS/TRHPH instruments have hydrogen sensors attached and are therefore not
        # producing such data. Commenting this out to prevent useless data from being produced in the data particle,
        # but keeping it around in case we need it again in the future
        # BarsDataParticleKey.HYDROGEN_5: {TYPE: float, VALUE: 0.060, REQUIRED: True},
        # BarsDataParticleKey.HYDROGEN_X1: {TYPE: float, VALUE: 0.025, REQUIRED: True},
        # BarsDataParticleKey.HYDROGEN_X5: {TYPE: float, VALUE: 0.045, REQUIRED: True},
        # BarsDataParticleKey.EH_SENSOR: {TYPE: float, VALUE: 2.999, REQUIRED: True},

        BarsDataParticleKey.REFERENCE_TEMP_VOLTS: {TYPE: float, VALUE: 1.173, REQUIRED: True},
        BarsDataParticleKey.REFERENCE_TEMP_DEG_C: {TYPE: float, VALUE: 20.75, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_TEMP_VOLTS: {TYPE: float, VALUE: 0.019, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_TEMP_DEG_C: {TYPE: float, VALUE: 27.4, REQUIRED: True},
        BarsDataParticleKey.BATTERY_VOLTAGE: {TYPE: float, VALUE: 8.5, REQUIRED: True},

    }

    _sample_parameters = {
        BarsDataParticleKey.RESISTIVITY_5: {TYPE: float, VALUE: 0.010, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_X1: {TYPE: float, VALUE: 0.020, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_X5: {TYPE: float, VALUE: 0.030, REQUIRED: True},

        # At the moment, none of the BARS/TRHPH instruments have hydrogen sensors attached and are therefore not
        # producing such data. Commenting this out to prevent useless data from being produced in the data particle,
        # but keeping it around in case we need it again in the future
        # BarsDataParticleKey.HYDROGEN_5: {TYPE: float, VALUE: 0.040, REQUIRED: True},
        # BarsDataParticleKey.HYDROGEN_X1: {TYPE: float, VALUE: 0.021, REQUIRED: True},
        # BarsDataParticleKey.HYDROGEN_X5: {TYPE: float, VALUE: 0.042, REQUIRED: True},
        # BarsDataParticleKey.EH_SENSOR: {TYPE: float, VALUE: 1.999, REQUIRED: True},

        BarsDataParticleKey.REFERENCE_TEMP_VOLTS: {TYPE: float, VALUE: 1.173, REQUIRED: True},
        BarsDataParticleKey.REFERENCE_TEMP_DEG_C: {TYPE: float, VALUE: 20.75, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_TEMP_VOLTS: {TYPE: float, VALUE: 0.016, REQUIRED: True},
        BarsDataParticleKey.RESISTIVITY_TEMP_DEG_C: {TYPE: float, VALUE: 24.7, REQUIRED: True},
        BarsDataParticleKey.BATTERY_VOLTAGE: {TYPE: float, VALUE: 9.3, REQUIRED: True},

    }

    def assert_status_particle(self, status_particle, verify_values=False):
        """
        Verify sample particle
        @param status_particle:  TRHPHDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(BarsStatusParticleKey, self._status_parameters)
        self.assert_data_particle_header(status_particle, DataParticleType.TRHPH_STATUS,
                                         require_instrument_timestamp=False)
        self.assert_data_particle_parameters(status_particle, self._status_parameters, verify_values)

    def assert_particle_sample(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  TRHPHDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(BarsDataParticleKey, self._sample_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.TRHPH_PARSED,
                                         require_instrument_timestamp=False)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters, verify_values)

    def assert_particle_sample_2(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  TRHPHDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(BarsDataParticleKey, self._sample_parameters_2)
        self.assert_data_particle_header(data_particle, DataParticleType.TRHPH_PARSED,
                                         require_instrument_timestamp=False)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters_2, verify_values)

    def assert_driver_parameters(self, current_parameters, verify_values=False):
        """
        Verify that all driver parameters are correct and potentially verify values.
        @param current_parameters: driver parameters read from the driver instance
        @param verify_values: should we verify values against definition?
        """
        self.assert_parameters(current_parameters, self._driver_parameters, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, TRHPHMixinSub):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(Command())
        self.assert_enum_has_no_duplicates(SubMenu())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(BarsDataParticleKey())

        # Test capabilities for duplicates, then verify that capabilities is a subset of protocol events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(Protocol.sieve_function)
        self.assert_chunker_sample(chunker, self.VALID_SAMPLE_01)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_SAMPLE_01)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_SAMPLE_01)
        self.assert_chunker_combined_sample(chunker, self.VALID_SAMPLE_01)

        self.assert_chunker_sample(chunker, self.VALID_SAMPLE_02)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_SAMPLE_02)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_SAMPLE_02)
        self.assert_chunker_combined_sample(chunker, self.VALID_SAMPLE_02)

        self.assert_chunker_sample(chunker, self.VALID_SAMPLE_02)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_SAMPLE_02)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_SAMPLE_02)
        self.assert_chunker_combined_sample(chunker, self.VALID_SAMPLE_02)

        self.assert_chunker_sample(chunker, self.VALID_STATUS_01)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_STATUS_01)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_STATUS_01)
        self.assert_chunker_combined_sample(chunker, self.VALID_STATUS_01)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, self.VALID_SAMPLE_01, self.assert_particle_sample, True)
        self.assert_particle_published(driver, self.VALID_SAMPLE_02, self.assert_particle_sample_2, True)

        # validate status particles
        self.assert_particle_published(driver, self.VALID_STATUS_01, self.assert_status_particle, True)

    def test_corrupt_data_sample(self):
        # garbage is not okay
        particle = BarsStatusParticle(self.VALID_STATUS_01.replace('20 =', 'bar'), port_timestamp=3558720820.531179)
        with self.assertRaises(SampleException):
            particle.generate()

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock()
        protocol = Protocol(MENU, Prompt, NEWLINE, mock_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities),
                          sorted(protocol._filter_capabilities(test_capabilities)))

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected.  All states defined in this dict must
        also be defined in the protocol FSM.
        """
        capabilities = {
            ProtocolState.COMMAND: [ProtocolEvent.GET,
                                    ProtocolEvent.SET,
                                    ProtocolEvent.START_DIRECT,
                                    ProtocolEvent.START_AUTOSAMPLE,
                                    ProtocolEvent.ACQUIRE_STATUS],

            ProtocolState.AUTOSAMPLE: [ProtocolEvent.STOP_AUTOSAMPLE,
                                       ProtocolEvent.SCHEDULED_ACQUIRE_STATUS],

            ProtocolState.DIRECT_ACCESS: [ProtocolEvent.STOP_DIRECT,
                                          ProtocolEvent.EXECUTE_DIRECT,
                                          ProtocolEvent.SCHEDULED_ACQUIRE_STATUS],

            ProtocolState.UNKNOWN: [ProtocolEvent.DISCOVER]
        }
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)

    def test_to_seconds(self):
        """
        Test to second conversion.
        """
        self.assertEquals(240, Protocol._to_seconds(4, 1))
        self.assertEquals(59, Protocol._to_seconds(59, 0))
        self.assertEquals(60, Protocol._to_seconds(60, 0))
        self.assertEquals(3600, Protocol._to_seconds(60, 1))
        self.assertEquals(3660, Protocol._to_seconds(61, 1))
        self.assertRaises(InstrumentProtocolException,
                          Protocol._to_seconds, 1, 2)
        self.assertRaises(InstrumentProtocolException,
                          Protocol._to_seconds, 1, "foo")

    def test_from_seconds(self):
        self.assertEquals((1, 23), Protocol._from_seconds(23))
        self.assertEquals((1, 59), Protocol._from_seconds(59))
        self.assertEquals((2, 1), Protocol._from_seconds(60))
        self.assertEquals((2, 2), Protocol._from_seconds(120))
        self.assertEquals((2, 1), Protocol._from_seconds(119))
        self.assertRaises(InstrumentParameterException,
                          Protocol._from_seconds, 3601)
        self.assertRaises(InstrumentParameterException,
                          Protocol._from_seconds, 14)



###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(InstrumentDriverIntegrationTestCase, TRHPHMixinSub):
    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

    def assert_acquire_status(self):
        """
        Verify all status particles generated
        """
        self.clear_events()
        self.assert_async_particle_generation(DataParticleType.TRHPH_STATUS, self.assert_status_particle, timeout=200)

    def test_scheduled_acquire_status(self):
        """
        Verify we can schedule an acquire status event
        """
        self.assert_scheduled_event(ScheduledJob.ACQUIRE_STATUS, self.assert_acquire_status,
                                    autosample_command=ProtocolEvent.START_AUTOSAMPLE, delay=200)

    def test_connect(self):
        """
        Test configuring and connecting to the device through the port
        agent. Discover device state.
        """
        self.assert_initialize_driver()

    def test_direct_access(self):
        """
        Verify we can enter the direct access state
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_state_change(ProtocolState.COMMAND, 3)

        self.assert_driver_command(ProtocolEvent.START_DIRECT)
        self.assert_state_change(ProtocolState.DIRECT_ACCESS, 3)
        self.assert_driver_command(ProtocolEvent.STOP_DIRECT)
        self.assert_state_change(ProtocolState.COMMAND, 3)

    def test_state_transition(self):
        """
        Tests to see if we can make transition to different states
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_state_change(ProtocolState.COMMAND, 3)

        # Test transition to auto sample
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE)
        self.assert_state_change(ProtocolState.AUTOSAMPLE, 3)

        # Test transition back to command state
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_state_change(ProtocolState.COMMAND, 10)

        # Test transition to direct access state
        self.assert_driver_command(ProtocolEvent.START_DIRECT)
        self.assert_state_change(ProtocolState.DIRECT_ACCESS, 3)

        # Test transition back to command state
        self.assert_driver_command(ProtocolEvent.STOP_DIRECT)
        self.assert_state_change(ProtocolState.COMMAND, 3)

    def test_parameters(self):
        """
        Test driver parameters and verify their type.  Startup parameters also verify the parameter
        value.  This test confirms that parameters are being read/converted properly and that
        the startup has been applied.
        """
        self.assert_initialize_driver()
        reply = self.driver_client.cmd_dvr('get_resource', Parameter.ALL)
        self.assert_driver_parameters(reply, True)

        # verify we can set read/write parameters
        self.assert_set(Parameter.CYCLE_TIME, 30)

        self.assert_set(Parameter.RUN_ACQUIRE_STATUS_INTERVAL, '00:00:30', startup=True, no_get=True)
        self.assert_get(Parameter.RUN_ACQUIRE_STATUS_INTERVAL, '00:00:30')
        self.assert_set_exception(Parameter.RUN_ACQUIRE_STATUS_INTERVAL, "00:00:00")


    def test_out_of_range(self):
        """
        Verify setting driver parameters with out of range values will cause
        instrument parameter exceptions.
        """
        self.assert_initialize_driver()
        self.assert_set_exception(Parameter.CYCLE_TIME, 14)
        self.assert_set_exception(Parameter.CYCLE_TIME, 3601)

        # verify we can set read/write parameters
        self.assert_set(Parameter.CYCLE_TIME, 30)

    def test_readonly_set(self):
        # verify we cannot set read only parameters
        self.assert_initialize_driver()
        self.assert_set_exception(Parameter.VERBOSE, VERBOSE_VALUE)
        self.assert_set_exception(Parameter.METADATA_POWERUP, METADATA_POWERUP_VALUE)
        self.assert_set_exception(Parameter.METADATA_RESTART, METADATA_RESTART_VALUE)
        self.assert_set_exception(Parameter.RES_SENSOR_POWER, RES_SENSOR_POWER_VALUE)
        self.assert_set_exception(Parameter.INST_AMP_POWER, INST_AMP_POWER_VALUE)
        self.assert_set_exception(Parameter.EH_ISOLATION_AMP_POWER, EH_ISOLATION_AMP_POWER_VALUE)
        self.assert_set_exception(Parameter.HYDROGEN_POWER, HYDROGEN_POWER_VALUE)
        self.assert_set_exception(Parameter.REFERENCE_TEMP_POWER, REFERENCE_TEMP_POWER_VALUE)

    def test_get_params(self):
        """
        Test get driver parameters and verify their initial values.
        """
        self.assert_initialize_driver()
        self.assert_get(Parameter.CYCLE_TIME, CYCLE_TIME_VALUE)
        self.assert_get(Parameter.EH_ISOLATION_AMP_POWER, EH_ISOLATION_AMP_POWER_VALUE)
        self.assert_get(Parameter.HYDROGEN_POWER, HYDROGEN_POWER_VALUE)
        self.assert_get(Parameter.INST_AMP_POWER, INST_AMP_POWER_VALUE)
        self.assert_get(Parameter.METADATA_POWERUP, METADATA_POWERUP_VALUE)
        self.assert_get(Parameter.METADATA_RESTART, METADATA_RESTART_VALUE)
        self.assert_get(Parameter.REFERENCE_TEMP_POWER, REFERENCE_TEMP_POWER_VALUE)
        self.assert_get(Parameter.RES_SENSOR_POWER, RES_SENSOR_POWER_VALUE)
        self.assert_get(Parameter.VERBOSE, VERBOSE_VALUE)

    def test_autosample_on(self):
        """
        @brief Test for turning auto sample data on
        """
        self.assert_initialize_driver()
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE)
        self.assert_async_particle_generation(DataParticleType.TRHPH_PARSED, self.assert_particle_sample,
                                              particle_count=3, timeout=70)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND)

    def test_status(self):
        """
        Test acquire status command which generates a status particle
        """
        self.assert_initialize_driver()

        # test acquire_status particles
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS,
                                        DataParticleType.TRHPH_STATUS,
                                        self.assert_status_particle,
                                        delay=60)

    def test_bogus_set(self):
        # verify we cannot set bogus param and bogus value
        self.assert_initialize_driver()
        self.assert_set_exception(Parameter.CYCLE_TIME)
        self.assert_set_exception('bogus param')


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class DriverQualificationTest(InstrumentDriverQualificationTestCase, TRHPHMixinSub):
    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)

    def test_direct_access_telnet_mode(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports direct access to the physical
        instrument. (telnet mode)
        """
        self.assert_direct_access_start_telnet(timeout=1200)
        self.assertTrue(self.tcp_client)

        self.tcp_client.send_data("\r")
        result = self.tcp_client.expect(Prompt.MAIN_MENU, max_retries=20)
        self.assertTrue(result)
        log.debug("Successfully sent return and got Command prompt")

        # Select Menu option 2 to change data collection parameters
        self.tcp_client.send_data("2\r")

        result = self.tcp_client.expect(Prompt.CHANGE_PARAM_MENU, max_retries=20)
        self.assertTrue(result)

        # Select SubMenu option 1 to change the Cycle Time
        self.tcp_client.send_data("1\r")

        result = self.tcp_client.expect(Prompt.CYCLE_TIME_PROMPT, max_retries=20)
        self.assertTrue(result)

        # Select SubMenu option 1 for Seconds( 1 for Seconds, 2 for Minutes)
        self.tcp_client.send_data("1\r")
        result = self.tcp_client.expect(Prompt.CYCLE_TIME_SEC_VALUE_PROMPT, max_retries=20)
        self.assertTrue(result)

        # Enter 16 for Cycle Time
        self.tcp_client.send_data("16\r")
        result = self.tcp_client.expect(Prompt.CHANGE_PARAM_MENU, max_retries=20)
        self.assertTrue(result)

        # Select SubMenu option 3 to change Print Status of Metadata on Powerup
        self.tcp_client.send_data("3\r")

        result = self.tcp_client.expect(Prompt.METADATA_PROMPT, max_retries=20)
        self.assertTrue(result)

        # Enter 2 for Yes option
        self.tcp_client.send_data("2\r")
        result = self.tcp_client.expect(Prompt.CHANGE_PARAM_MENU, max_retries=20)
        self.assertTrue(result)

        # Select SubMenu option 4 to change Print Status of Metadata on Restart
        self.tcp_client.send_data("4\r")

        result = self.tcp_client.expect(Prompt.METADATA_PROMPT, max_retries=20)
        self.assertTrue(result)

        # Enter 2 for Yes option
        self.tcp_client.send_data("2\r")
        result = self.tcp_client.expect(Prompt.CHANGE_PARAM_MENU, max_retries=20)
        self.assertTrue(result)

        # Send a Back Menu Command (9) to return to the main Menu
        self.tcp_client.send_data("9\r")
        result = self.tcp_client.expect(Prompt.MAIN_MENU, max_retries=20)
        self.assertTrue(result)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_get_parameter(Parameter.CYCLE_TIME, 20)
        self.assert_get_parameter(Parameter.METADATA_POWERUP, 0)
        self.assert_get_parameter(Parameter.METADATA_RESTART, 0)

    def test_discover(self):
        """
        over-ridden because the driver will always go to command mode
        during the discover process after a reset.

        """
        # Verify the agent is in command mode
        self.assert_enter_command_mode()

        # Now reset and try to discover.  This will stop the driver and cause it to re-discover which
        # will always go back to command for this instrument
        self.assert_reset()
        self.assert_discover(ResourceAgentState.COMMAND)

    def test_sample_particles(self):
        """
        Start and stop autosample and verify data particle
        """
        self.assert_sample_autosample(self.assert_particle_sample, DataParticleType.TRHPH_PARSED)

    def test_status_particles(self):
        """
        Verify status particle in Command state
        """
        self.assert_enter_command_mode()
        self.assert_particle_polled(Capability.ACQUIRE_STATUS, self.assert_status_particle,
                                    DataParticleType.TRHPH_STATUS, timeout=200)

    def test_get_capabilities(self):
        """
        @brief Verify that the correct capabilities are returned from get_capabilities
        at various driver/agent states.
        """

        ##################
        #  Command Mode
        ##################
        capabilities = {
            AgentCapabilityType.AGENT_COMMAND: self._common_agent_commands(ResourceAgentState.COMMAND),
            AgentCapabilityType.AGENT_PARAMETER: self._common_agent_parameters(),
            AgentCapabilityType.RESOURCE_COMMAND: [
                ProtocolEvent.GET,
                ProtocolEvent.SET,
                ProtocolEvent.START_AUTOSAMPLE,
                ProtocolEvent.ACQUIRE_STATUS],
            AgentCapabilityType.RESOURCE_INTERFACE: None,
            AgentCapabilityType.RESOURCE_PARAMETER: self._driver_parameters.keys()}

        self.assert_enter_command_mode()
        self.assert_capabilities(capabilities)

        ##################
        #  Streaming Mode
        ##################
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.STREAMING)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = [ProtocolEvent.STOP_AUTOSAMPLE]

        self.assert_start_autosample()
        self.assert_capabilities(capabilities)
        self.assert_stop_autosample()

        ##################
        #  DA Mode
        ##################
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.DIRECT_ACCESS)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = []

        self.assert_direct_access_start_telnet()
        self.assert_capabilities(capabilities)
        self.assert_direct_access_stop_telnet()

        #######################
        #  Uninitialized Mode
        #######################
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.UNINITIALIZED)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = []
        capabilities[AgentCapabilityType.RESOURCE_INTERFACE] = []
        capabilities[AgentCapabilityType.RESOURCE_PARAMETER] = []

        self.assert_reset()
        self.assert_capabilities(capabilities)

    def test_get_set_parameters(self):
        """
        verify that all parameters can be get and set properly
        """
        self.assert_enter_command_mode()

        self.assert_get_parameter(Parameter.CYCLE_TIME, 20)
        self.assert_set_parameter(Parameter.CYCLE_TIME, 16)

        self.assert_get_parameter(Parameter.VERBOSE, 0)
        self.assert_get_parameter(Parameter.METADATA_POWERUP, 0)
        self.assert_get_parameter(Parameter.METADATA_RESTART, 0)
        self.assert_get_parameter(Parameter.RES_SENSOR_POWER, 1)
        self.assert_get_parameter(Parameter.INST_AMP_POWER, 1)
        self.assert_get_parameter(Parameter.EH_ISOLATION_AMP_POWER, 1)
        self.assert_get_parameter(Parameter.HYDROGEN_POWER, 1)
        self.assert_get_parameter(Parameter.REFERENCE_TEMP_POWER, 1)
        self.assert_get_parameter(Parameter.RUN_ACQUIRE_STATUS_INTERVAL, "00:10:00")


        self.assert_reset()

    def test_direct_access_telnet_closed(self):
        """
        Test that we can properly handle the situation when a direct access
        session is launched, the telnet is closed, then direct access is stopped.
        Override test_direct_access_telnet_closed() in idk/unit_test.py to increase
        timeout parameter to 600 for assert_state_change().
        """
        self.assert_enter_command_mode()
        self.assert_direct_access_start_telnet(timeout=600)
        self.assertTrue(self.tcp_client)
        self.tcp_client.disconnect()
        self.assert_state_change(ResourceAgentState.COMMAND, DriverProtocolState.COMMAND, 600)