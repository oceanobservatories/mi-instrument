"""
@package mi.instrument.KML.CAMDS.test.test_driver
@file marine-integrations/mi/instrument/KML/CAMDS/test/test_driver.py
@author Sung Ahn
@brief Test Driver for CAMDS
Release notes:

"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import time as time
import unittest
from nose.plugins.attrib import attr
from mi.core.log import get_logger

log = get_logger()

# MI imports.
from mi.idk.unit_test import AgentCapabilityType

from mi.instrument.kml.test.test_driver import KMLUnitTest
from mi.instrument.kml.test.test_driver import KMLIntegrationTest
from mi.instrument.kml.test.test_driver import KMLQualificationTest
from mi.instrument.kml.test.test_driver import KMLPublicationTest

from mi.instrument.kml.particles import DataParticleType
from mi.instrument.kml.driver import KMLProtocolState
from mi.instrument.kml.driver import KMLProtocolEvent
from mi.instrument.kml.driver import KMLParameter
from mi.instrument.kml.driver import ParameterIndex

from mi.core.exceptions import InstrumentCommandException

from mi.core.instrument.instrument_driver import ResourceAgentState


# ################################### RULES ####################################
# #
# Common capabilities in the base class                                       #
# #
# Instrument specific stuff in the derived class                              #
# #
# Generator spits out either stubs or comments describing test this here,     #
# test that there.                                                            #
# #
# Qualification tests are driven through the instrument_agent                 #
# #
# ##############################################################################

class CAMParameterAltValue():
    # Values that are valid, but not the ones we want to use,
    # used for testing to verify that we are setting good values.
    #

    # Probably best NOT to tweek this one.
    SERIAL_FLOW_CONTROL = '11110'
    BANNER = 1
    SAVE_NVRAM_TO_RECORDER = True  # Immutable.
    SLEEP_ENABLE = 1
    POLLED_MODE = True
    PITCH = 1
    ROLL = 1


# ##############################################################################
# UNIT TESTS                                   #
# ##############################################################################
@attr('UNIT', group='mi')
class CAMDriverUnitTest(KMLUnitTest):
    def setUp(self):
        KMLUnitTest.setUp(self)


# ##############################################################################
# INTEGRATION TESTS                                #
# ##############################################################################
@attr('INT', group='mi')
class CAMDriverIntegrationTest(KMLIntegrationTest):
    def setUp(self):
        KMLIntegrationTest.setUp(self)

    # ##
    # Add instrument specific integration tests
    ###
    def test_parameters(self):
        """
        Test driver parameters and verify their type.  Startup parameters also verify the parameter
        value.  This test confirms that parameters are being read/converted properly and that
        the startup has been applied.
        """
        self.assert_initialize_driver()
        reply = self.driver_client.cmd_dvr('get_resource', KMLParameter.ALL)
        log.error("Sung get_resource %s", repr(reply))

        self.assert_driver_parameters(reply, True)

    def test_break(self):
        self.assert_initialize_driver()
        self.assert_driver_command(KMLProtocolEvent.START_AUTOSAMPLE, state=KMLProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_driver_command(KMLProtocolEvent.STOP_AUTOSAMPLE, state=KMLProtocolState.COMMAND, delay=10)

    #@unittest.skip('It takes many hours for this test')
    def test_commands(self):
        """
        Run instrument commands from both command and streaming mode.
        """
        self.assert_initialize_driver()
        ####
        # First test in command mode
        ####

        self.assert_driver_command(KMLProtocolEvent.START_AUTOSAMPLE, state=KMLProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_driver_command(KMLProtocolEvent.STOP_AUTOSAMPLE, state=KMLProtocolState.COMMAND, delay=1)
        self.assert_driver_command(KMLProtocolEvent.ACQUIRE_SAMPLE)
        self.assert_driver_command(KMLProtocolEvent.LAMP_ON)
        self.assert_driver_command(KMLProtocolEvent.LAMP_OFF)
        self.assert_driver_command(KMLProtocolEvent.LASER_1_ON)
        self.assert_driver_command(KMLProtocolEvent.LASER_1_OFF)
        self.assert_driver_command(KMLProtocolEvent.LASER_BOTH_ON)
        self.assert_driver_command(KMLProtocolEvent.LASER_BOTH_OFF)
        self.assert_driver_command(KMLProtocolEvent.ACQUIRE_STATUS)
        self.assert_driver_command(KMLProtocolEvent.LASER_2_ON)
        self.assert_driver_command(KMLProtocolEvent.LASER_2_OFF)
        self.assert_driver_command(KMLProtocolEvent.LASER_BOTH_ON)
        self.assert_driver_command(KMLProtocolEvent.LASER_BOTH_OFF)

        ####
        # Test a bad command
        ####
        self.assert_driver_command_exception('ima_bad_command', exception_class=InstrumentCommandException)


# ##############################################################################
# QUALIFICATION TESTS                              #
# Device specific qualification tests are for doing final testing of ion      #
# integration.  The generally aren't used for instrument debugging and should #
# be tackled after all unit and integration tests are complete                #
###############################################################################
@attr('QUAL', group='mi')
class CAMDriverQualificationTest(KMLQualificationTest):
    def setUp(self):
        KMLQualificationTest.setUp(self)

    @unittest.skip('It takes time for this test')
    def test_get_capabilities(self):
        """
        @brief Verify that the correct capabilities are returned from get_capabilities
        at various driver/agent states.
        """
        self.assert_enter_command_mode()

        ##################
        #  Command Mode
        ##################
        capabilities = {
            AgentCapabilityType.AGENT_COMMAND: self._common_agent_commands(ResourceAgentState.COMMAND),
            AgentCapabilityType.AGENT_PARAMETER: self._common_agent_parameters(),
            AgentCapabilityType.RESOURCE_COMMAND: [
                KMLProtocolEvent.ACQUIRE_SAMPLE,
                KMLProtocolEvent.START_AUTOSAMPLE,
                KMLProtocolEvent.LAMP_ON,
                KMLProtocolEvent.LAMP_ON,
                KMLProtocolEvent.ACQUIRE_STATUS,
            ],
            AgentCapabilityType.RESOURCE_INTERFACE: None,
            AgentCapabilityType.RESOURCE_PARAMETER: self._driver_parameters.keys()
        }

        self.assert_capabilities(capabilities)

        ##################
        #  Streaming Mode
        ##################

        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.STREAMING)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = [
            KMLProtocolEvent.STOP_AUTOSAMPLE,
            KMLProtocolEvent.LASER_1_OFF,
        ]
        self.assert_start_autosample()
        self.assert_capabilities(capabilities)
        self.assert_stop_autosample()

        ##################
        #  DA Mode
        ##################

        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.DIRECT_ACCESS)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = [
        ]

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

    @unittest.skip('It takes many hours for this test')
    def test_startup_params_first_pass(self):
        """
        Verify that startup parameters are applied correctly. Generally this
        happens in the driver discovery method.  We have two identical versions
        of this test so it is run twice.  First time to check and CHANGE, then
        the second time to check again.

        since nose orders the tests by ascii value this should run second.
        """
        self.assert_enter_command_mode()

        for k in self._driver_parameters.keys():
            if self.VALUE in self._driver_parameters[k]:
                if not self._driver_parameters[k][self.READONLY]:
                    self.assert_get_parameter(k, self._driver_parameters[k][self.VALUE])
                    log.debug("VERIFYING %s is set to %s appropriately ", k,
                              str(self._driver_parameters[k][self.VALUE]))

        self.assert_set_parameter(KMLParameter.CAMERA_MODE, 9)
        self.assert_set_parameter(KMLParameter.CAMERA_GAIN, 255)
        self.assert_set_parameter(KMLParameter.FOCUS_POSITION, 100)
        self.assert_set_parameter(KMLParameter.FRAME_RATE, 30)
        self.assert_set_parameter(KMLParameter.IRIS_POSITION, 8)
        self.assert_set_parameter(KMLParameter.PAN_SPEED, 50)
        self.assert_set_parameter(KMLParameter.PAN_POSITION, 90)

###############################################################################
#                             PUBLICATION TESTS                               #
# Device specific publication tests are for                                    #
# testing device specific capabilities                                        #
###############################################################################
@attr('PUB', group='mi')
class CAMDriverPublicationTest(KMLPublicationTest):
    def setUp(self):
        KMLPublicationTest.setUp(self)


