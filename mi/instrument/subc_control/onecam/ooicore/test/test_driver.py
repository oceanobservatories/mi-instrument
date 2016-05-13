"""
@package mi.instrument.subc_control.onecam.ooicore.test.test_driver
@file m-instrument/mi/instrument/subc_control/onecam/ooicore/driver.py
@author Tapana Gupta
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u [-t testname]
       $ bin/test_driver -i [-t testname]
       $ bin/test_driver -q [-t testname]
"""

__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger

log = get_logger()

# MI imports.
from mi.idk.unit_test import InstrumentDriverTestCase, ParameterTestConfigKey
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import DriverTestMixin

from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import DriverConfigKey

from mi.instrument.subc_control.onecam.ooicore.driver import InstrumentDriver, Command
from mi.instrument.subc_control.onecam.ooicore.driver import DataParticleType
from mi.instrument.subc_control.onecam.ooicore.driver import CAMHDAdreadStatusParticleKey
from mi.instrument.subc_control.onecam.ooicore.driver import ProtocolState
from mi.instrument.subc_control.onecam.ooicore.driver import ProtocolEvent
from mi.instrument.subc_control.onecam.ooicore.driver import Capability
from mi.instrument.subc_control.onecam.ooicore.driver import Parameter
from mi.instrument.subc_control.onecam.ooicore.driver import CAMHDProtocol
from mi.instrument.subc_control.onecam.ooicore.driver import Prompt
from mi.instrument.subc_control.onecam.ooicore.driver import NEWLINE

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.subc_control.onecam.ooicore.driver',
    driver_class="InstrumentDriver",
    instrument_agent_resource_id = '4RKRRG',
    instrument_agent_name = 'subc_control_onecam_ooicore',
    instrument_agent_packet_config = DataParticleType(),

    driver_startup_config = {
        DriverConfigKey.PARAMETERS: {}
    }
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

###
#   Driver constant definitions
###

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
class DriverTestMixinSub(DriverTestMixin):

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

    VALID_ADREAD_RESPONSE = 'ADVAL{"time": 1373996308, "data": [{"name": "abc", "val": 12.3, "units": "degrees"}, {"name": "def", "val": 45.6, "units": "minutes"}]}' + NEWLINE

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.ACQUIRE_STATUS: {STATES: [ProtocolState.COMMAND]},
        Capability.GET_STATUS_STREAMING: {STATES: [ProtocolState.COMMAND, ProtocolState.STREAMING]},
        Capability.START_STREAMING: {STATES: [ProtocolState.COMMAND]},
        Capability.STOP_STREAMING: {STATES: [ProtocolState.STREAMING]},

    }

    ###
    #  Parameter and Type Definitions
    ###
    _driver_parameters = {
        # Parameters defined in the IOS

        Parameter.ENDPOINT: {TYPE: str, READONLY: True, DA: False, STARTUP: False, DEFAULT: '128.95.97.233', VALUE: '128.95.97.233'},
        Parameter.PAN_POSITION: {TYPE: float, READONLY: False, DA: False, STARTUP: False, DEFAULT: 180.0, VALUE: 180.0},
        Parameter.TILT_POSITION: {TYPE: float, READONLY: False, DA: False, STARTUP: False, DEFAULT: 90.0, VALUE: 90.0},
        Parameter.PAN_TILT_SPEED: {TYPE: float, READONLY: False, DA: False, STARTUP: False, DEFAULT: 10.0, VALUE: 10.0},
        Parameter.HEADING: {TYPE: float, READONLY: True, DA: False, STARTUP: False, DEFAULT: 0.0, VALUE: 0.0},
        Parameter.PITCH: {TYPE: float, READONLY: True, DA: False, STARTUP: False, DEFAULT: 0.0, VALUE: 0.0},
        Parameter.LIGHT_1_LEVEL: {TYPE: int, READONLY: False, DA: False, STARTUP: False, DEFAULT: 50, VALUE: 50},
        Parameter.LIGHT_2_LEVEL: {TYPE: int, READONLY: False, DA: False, STARTUP: False, DEFAULT: 50, VALUE: 50},
        Parameter.ZOOM_LEVEL: {TYPE: int, READONLY: False, DA: False, STARTUP: False, DEFAULT: 0, VALUE: 0},
        Parameter.LASERS_STATE: {TYPE: str, READONLY: False, DA: False, STARTUP: False, DEFAULT: 'off', VALUE: 'off'},
        Parameter.STATUS_INTERVAL: {TYPE: str, READONLY: False, DA: False, STARTUP: False, DEFAULT: '00:00:00', VALUE: '00:00:00'},
        Parameter.ELEMENTAL_IP_ADDRESS: {TYPE: str, READONLY: False, DA: False, STARTUP: False, DEFAULT: '209.124.182.238', VALUE: '209.124.182.238'},
        Parameter.OUTPUT_GROUP_ID: {TYPE: int, READONLY: False, DA: False, STARTUP: False, DEFAULT: 27, VALUE: 27},

    }

    _adread_parameters = {

        CAMHDAdreadStatusParticleKey.CHANNEL_NAME: {TYPE: list, VALUE: ['abc', 'def'], REQUIRED: True},
        CAMHDAdreadStatusParticleKey.CHANNEL_VALUE: {TYPE: list, VALUE: [12.3, 45.6], REQUIRED: True},
        CAMHDAdreadStatusParticleKey.VALUE_UNITS: {TYPE: list, VALUE: ['degrees', 'minutes'], REQUIRED: True},

    }

    def assert_adread_status_particle(self, data_particle, verify_values=False):
        """
        Verify CAMDS health status data particle
        @param data_particle: CAMDS health status DataParticle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_keys(CAMHDAdreadStatusParticleKey, self._adread_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.ADREAD_STATUS)
        self.assert_data_particle_parameters(data_particle, self._adread_parameters)

###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
#                                                                             #
#   These tests are especially useful for testing parsers and other data      #
#   handling.  The tests generally focus on small segments of code, like a    #
#   single function call, but more complex code using Mock objects.  However  #
#   if you find yourself mocking too much maybe it is better as an            #
#   integration test.                                                         #
#                                                                             #
#   Unit tests do not start up external processes like the port agent or      #
#   driver process.                                                           #
###############################################################################
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, DriverTestMixinSub):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilites
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(Command())

        # Test capabilites for duplicates, them verify that capabilities is a subset of proto events
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
        chunker = StringChunker(CAMHDProtocol.sieve_function)

        self.assert_chunker_sample(chunker, self.VALID_ADREAD_RESPONSE)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, self.VALID_ADREAD_RESPONSE, self.assert_adread_status_particle, True)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        my_event_callback = Mock()
        protocol = CAMHDProtocol(Prompt, NEWLINE, my_event_callback)
        driver_capabilities = Capability.list()
        test_capabilities = Capability.list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(driver_capabilities, protocol._filter_capabilities(test_capabilities))

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected.  All states defined in this dict must
        also be defined in the protocol FSM.
        """
        capabilities = {
            ProtocolState.UNKNOWN: ['DRIVER_EVENT_DISCOVER'],
            ProtocolState.COMMAND: ['DRIVER_EVENT_ACQUIRE_STATUS',
                                    'DRIVER_EVENT_GET',
                                    'DRIVER_EVENT_SET',
                                    'DRIVER_EVENT_START_STREAMING',
                                    'DRIVER_EVENT_START_DIRECT'],
            ProtocolState.STREAMING: ['DRIVER_EVENT_STOP_STREAMING',
                                      'DRIVER_EVENT_GET',
                                      'DRIVER_EVENT_SET',
                                      'DRIVER_EVENT_GET_STATUS_STREAMING'],
            ProtocolState.DIRECT_ACCESS: ['DRIVER_EVENT_STOP_DIRECT', 'EXECUTE_DIRECT']
        }

        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(InstrumentDriverIntegrationTestCase):
    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for doing final testing of ion      #
# integration.  The generally aren't used for instrument debugging and should #
# be tackled after all unit and integration tests are complete                #
###############################################################################
@attr('QUAL', group='mi')
class DriverQualificationTest(InstrumentDriverQualificationTestCase):
    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)
