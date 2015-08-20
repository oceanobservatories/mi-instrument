"""
@package mi.instrument.subc_control.onecam.ooicore.test.test_driver
@file marine-integrations/mi/instrument/subc_control/onecam/ooicore/driver.py
@author Richard Han
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u [-t testname]
       $ bin/test_driver -i [-t testname]
       $ bin/test_driver -q [-t testname]
"""

__author__ = 'Richard Han'
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
from mi.instrument.subc_control.onecam.ooicore.driver import ProtocolState
from mi.instrument.subc_control.onecam.ooicore.driver import ProtocolEvent
from mi.instrument.subc_control.onecam.ooicore.driver import Capability
from mi.instrument.subc_control.onecam.ooicore.driver import Parameter
from mi.instrument.subc_control.onecam.ooicore.driver import Protocol
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
        DriverConfigKey.PARAMETERS: {Parameter.PICTURE_INTERVAL: 0,
                                     Parameter.RECORD_INTERVAL: 0,
                                     Parameter.SLEEP_INTERVAL: 0,
                                     Parameter.STOP_BIT: 1,
                                     Parameter.BYTE_SIZE: 8,
                                     Parameter.DATA_FLOW_CONTROL:'None',
                                     Parameter.SERIAL_BAUD_RATE: 9600,
                                     Parameter.INPUT_BUFFER_SIZE:1200,
                                     Parameter.OUTPUT_BUFFER_SIZE:1200,
                                     Parameter.PARITY: 0,}
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

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.AUTOSAMPLE]},
        Capability.ACQUIRE_SAMPLE: {STATES: [ProtocolState.COMMAND]}
    }

    ###
    #  Parameter and Type Definitions
    ###
    _driver_parameters = {
        # Parameters defined in the IOS

        Parameter.BYTE_SIZE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 8, VALUE: 8},
        Parameter.DATA_FLOW_CONTROL: {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: 'None', VALUE: 'None'},
        Parameter.STOP_BIT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1, VALUE: 1},
        Parameter.INPUT_BUFFER_SIZE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1024, VALUE: 1024},
        Parameter.OUTPUT_BUFFER_SIZE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1024, VALUE: 1024},
        Parameter.PARITY: {TYPE: int, READONLY: False, DA: False, STARTUP: True, DEFAULT: 0, VALUE: 0},
        Parameter.SERIAL_BAUD_RATE: {TYPE: int, READONLY: False, DA: False, STARTUP: True, DEFAULT: 9600, VALUE: 9600},
        Parameter.PICTURE_INTERVAL: {TYPE: int, READONLY: False, DA: False, STARTUP: True, DEFAULT: 0, VALUE: 0},
        Parameter.SLEEP_INTERVAL: {TYPE: int, READONLY: False, DA: False, STARTUP: True, DEFAULT: 0, VALUE: 0},
        Parameter.RECORD_INTERVAL: {TYPE: int, READONLY: False, DA: False, STARTUP: True, DEFAULT: 0, VALUE: 0},
    }

    def assertSampleDataParticle(self, data_particle):
        '''
        Verify a particle is a know particle to this driver and verify the particle is
        correct
        @param data_particle: Data particle of unkown type produced by the driver
        '''
        # if (isinstance(data_particle, RawDataParticle)):
        #     self.assert_particle_raw(data_particle)
        # else:
        #     log.error("Unknown Particle Detected: %s" % data_particle)
        #     self.assertFalse(True)
        pass


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
        chunker = StringChunker(Protocol.sieve_function)


    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)


    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock()
        protocol = Protocol(Prompt, NEWLINE, mock_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities),
                          sorted(protocol._filter_capabilities(test_capabilities)))


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

    def test_direct_access_telnet_mode(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports direct access to the physical instrument. (telnet mode)
        """
        self.assert_direct_access_start_telnet()
        self.assertTrue(self.tcp_client)

        ###
        #   Add instrument specific code here.
        ###

        self.assert_direct_access_stop_telnet()


    def test_poll(self):
        '''
        No polling for a single sample
        '''


    def test_autosample(self):
        '''
        start and stop autosample and verify data particle
        '''


    def test_get_set_parameters(self):
        '''
        verify that all parameters can be get set properly, this includes
        ensuring that read only parameters fail on set.
        '''
        self.assert_enter_command_mode()


    def test_get_capabilities(self):
        """
        @brief Walk through all driver protocol states and verify capabilities
        returned by get_current_capabilities
        """
        self.assert_enter_command_mode()
