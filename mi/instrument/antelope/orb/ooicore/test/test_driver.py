"""
@package mi.instrument.antelope.orb.ooicore.test.test_driver
@file marine-integrations/mi/instrument/antelope/orb/ooicore/test/test_driver.py
@author Pete Cable
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u [-t testname]
       $ bin/test_driver -i [-t testname]
       $ bin/test_driver -q [-t testname]
"""

import time

import ntplib
from mock import Mock
from nose.plugins.attrib import attr
import os
import shutil

from mi.core.instrument.port_agent_client import PortAgentPacket
from mi.core.log import get_logger
from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import ParameterTestConfigKey
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.instrument.antelope.orb.ooicore.driver import Capability, ProtocolState, InstrumentDriver, Protocol, ProtocolEvent, Parameter

__author__ = 'Pete Cable'
__license__ = 'Apache 2.0'

log = get_logger()

antelope_startup_config = {
        DriverConfigKey.PARAMETERS: {
            Parameter.DB_ADDR: '127.0.0.1',
            Parameter.DB_PORT: 5432,
            Parameter.FLUSH_INTERVAL: 60,
            Parameter.REFDES: 'test',
            Parameter.SOURCE_REGEX: '.*',
            Parameter.FILE_LOCATION: './antelope_data',
        }
}

# ##
# Driver parameters for the tests
# ##
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.antelope.orb.ooicore.driver',
    driver_class="InstrumentDriver",
    instrument_agent_resource_id='NCC1701',
    instrument_agent_name='antelope_orb_ooicore',
    instrument_agent_packet_config=[],
    driver_startup_config=antelope_startup_config
)

GO_ACTIVE_TIMEOUT = 180

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
###############################################################################
#                           DRIVER TEST MIXIN                                 #
#     Defines a set of constants and assert methods used for data particle    #
#     verification                                                            #
#                                                                             #
#  In python mixin classes are classes designed such that they wouldn't be    #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.                                      #
###############################################################################


class AntelopeTestMixinSub(DriverTestMixin):
    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    DA = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES

    _driver_parameters = {
        # Parameters defined in the IOS
        Parameter.DB_ADDR: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '127.0.0.1', VALUE: '127.0.0.1'},
        Parameter.DB_PORT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 5432, VALUE: 5432},
        Parameter.FLUSH_INTERVAL: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 60, VALUE: 60},
        Parameter.REFDES: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: 'test', VALUE: 'test'},
        Parameter.SOURCE_REGEX: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '.*', VALUE: '.*'},
        Parameter.FILE_LOCATION: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: './antelope_data', VALUE: './antelope_data'},
    }

    _samples = []

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.AUTOSAMPLE]},
        Capability.GET: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.SET: {STATES: [ProtocolState.COMMAND]},
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.CLEAR_WRITE_ERROR: {STATES: [ProtocolState.WRITE_ERROR]}
    }

    _capabilities = {
        ProtocolState.UNKNOWN: ['DRIVER_EVENT_DISCOVER'],
        ProtocolState.COMMAND: ['DRIVER_EVENT_GET',
                                'DRIVER_EVENT_SET',
                                'DRIVER_EVENT_START_AUTOSAMPLE'],
        ProtocolState.AUTOSAMPLE: ['DRIVER_EVENT_GET',
                                   'DRIVER_EVENT_STOP_AUTOSAMPLE',
                                   'PROTOCOL_EVENT_FLUSH',
                                   'PROTOCOL_EVENT_PROCESS_WRITE_ERROR'],
        ProtocolState.WRITE_ERROR: ['PROTOCOL_EVENT_CLEAR_WRITE_ERROR'],
    }

    def assert_driver_parameters(self, current_parameters, verify_values=False):
        """
        Verify that all driver parameters are correct and potentially verify values.
        @param current_parameters: driver parameters read from the driver instance
        @param verify_values: should we verify values against definition?
        """
        self.assert_parameters(current_parameters, self._driver_parameters, verify_values)

    def assert_particle(self, data_particle, particle_type, particle_keys, sample_data, verify_values=False):
        """
        Verify sample particle
        @param data_particle: data particle
        @param particle_type: particle type
        @param particle_keys: particle data keys
        @param sample_data: sample values to verify against
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_keys(particle_keys, sample_data)
        self.assert_data_particle_header(data_particle, particle_type, require_instrument_timestamp=True)
        self.assert_data_particle_parameters(data_particle, sample_data, verify_values)

    def _create_port_agent_packet(self, data_item):
        ts = ntplib.system_to_ntp_time(time.time())
        port_agent_packet = PortAgentPacket()
        port_agent_packet.attach_data(data_item)
        port_agent_packet.attach_timestamp(ts)
        port_agent_packet.pack_header()
        return port_agent_packet

    def _send_port_agent_packet(self, driver, data_item):
        driver._protocol.got_data(self._create_port_agent_packet(data_item))


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
# noinspection PyProtectedMember,PyUnusedLocal,PyUnresolvedReferences
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, AntelopeTestMixinSub):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_connect(self, initial_protocol_state=ProtocolState.COMMAND):
        """
        Verify we can initialize the driver.  Set up mock events for other tests.
        @param initial_protocol_state: target protocol state for driver
        @return: driver instance
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, initial_protocol_state)
        driver._protocol.set_init_params(antelope_startup_config)
        driver._protocol._init_params()
        return driver

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        driver = self.test_connect()

    def test_corrupt_data(self):
        """
        Verify corrupt data generates a SampleException
        """
        driver = self.test_connect()

    def test_start_stop_autosample(self):
        """
        Test starting/stopping autosample, verify state transitions
        """
        driver = self.test_connect()
        driver._protocol._build_persistent_dict()

        driver._protocol._protocol_fsm.on_event(ProtocolEvent.START_AUTOSAMPLE)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.AUTOSAMPLE)

        driver._protocol._protocol_fsm.on_event(ProtocolEvent.STOP_AUTOSAMPLE)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.COMMAND)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion. Also
        do a little extra validation for the Capabilities
        """
        self.assert_enum_has_no_duplicates(ProtocolState)
        self.assert_enum_has_no_duplicates(ProtocolEvent)
        self.assert_enum_has_no_duplicates(Parameter)
        # self.assert_enum_has_no_duplicates(InstrumentCommand())

        # Test capabilities for duplicates, them verify that capabilities is a subset of protocol events
        self.assert_enum_has_no_duplicates(Capability)
        self.assert_enum_complete(Capability, ProtocolEvent)

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected. All states defined in this dict must
        also be defined in the protocol FSM.
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, self._capabilities)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock()
        protocol = Protocol(mock_callback)
        driver_capabilities = Capability.list()
        test_capabilities = Capability.list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities),
                          sorted(protocol._filter_capabilities(test_capabilities)))

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(InstrumentDriverIntegrationTestCase, AntelopeTestMixinSub):
    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

    def test_connect(self):
        self.assert_initialize_driver()

    def test_get(self):
        self.assert_initialize_driver()
        for param in self._driver_parameters:
            self.assert_get(param, self._driver_parameters[param][self.VALUE])

    def test_set(self):
        """
        Test all set commands. Verify all exception cases.
        """
        self.assert_initialize_driver()

    def test_startup_parameters(self):
        new_values = {

        }

        self.assert_initialize_driver()
        self.assert_startup_parameters(self.assert_driver_parameters, new_values,
                                       self.test_config.driver_startup_config[DriverConfigKey.PARAMETERS])

    def test_incomplete_config(self):
        """
        Break our startup config, then verify the driver raises an exception
        """
        # grab the old config
        startup_params = self.test_config.driver_startup_config[DriverConfigKey.PARAMETERS]
        # old_value = startup_params[Parameter.LEVELING_TIMEOUT]
        # failed = False
        #
        # try:
        #     # delete a required parameter
        #     del (startup_params[Parameter.LEVELING_TIMEOUT])
        #     # re-init to take our broken config
        #     self.init_driver_process_client()
        #     self.assert_initialize_driver()
        #     failed = True
        # except ResourceError as e:
        #     log.info('Exception thrown, test should pass: %r', e)
        # finally:
        #     startup_params[Parameter.LEVELING_TIMEOUT] = old_value
        #
        # if failed:
        #     self.fail('Failed to throw exception on missing parameter')

    def test_autosample(self):
        """
        Test for turning data on
        """
        self.assert_initialize_driver()
        self.assert_driver_command(Capability.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE)

        # autosample for 10 seconds, then count the samples...
        time.sleep(10)
        self.assert_driver_command(Capability.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)
        time.sleep(10)

        # Search through the list of events for the data particle.  Extract the filename from the
        # metadata and assert that a file with that name was created.  Then clean up the data file.
        events = self.events
        file_location_created = False
        for event in self.events:
            if event['type'] == 'DRIVER_ASYNC_EVENT_SAMPLE':
                particle = event['value']
                particle_values = particle['values']
                for particle_value in particle_values:
                    if particle_value['value_id'] == 'filename':
                        filename = particle_value['value']
                        file_exists = os.path.exists(filename)
                        self.assertTrue(file_exists, 'creation of antelope data file: ' + filename)
                        if file_exists:
                            file_location_created = True

        if file_location_created:
            shutil.rmtree(self._driver_parameters[Parameter.FILE_LOCATION][self.VALUE])