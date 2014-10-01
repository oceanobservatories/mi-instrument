from mock import Mock
from nose.plugins.attrib import attr
from mi.core.log import get_logger

from mi.instrument.virtual.driver import ProtocolState
from mi.instrument.virtual.driver import ProtocolEvent
from mi.instrument.virtual.driver import InstrumentDriver
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.instrument.instrument_driver import DriverProtocolState

import unittest


__author__ = 'Pete Cable'
__license__ = 'Apache 2.0'

log = get_logger()
GO_ACTIVE_TIMEOUT = 180


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
class DriverUnitTest(unittest.TestCase):

    def test_connect(self, initial_protocol_state=ProtocolState.COMMAND):
        """
        Verify we can initialize the driver.  Set up mock events for other tests.
        @param initial_protocol_state: target protocol state for driver
        @return: driver instance
        """
        driver = InstrumentDriver(Mock())
        self.assert_initialize_driver(driver, initial_protocol_state)
        return driver


    def test_autosample(self):
        driver = self.test_connect(ProtocolState.COMMAND)
        driver._protocol._protocol_fsm.on_event(ProtocolEvent.START_AUTOSAMPLE)
        self.assertEqual(driver.get_resource_state(), ProtocolState.AUTOSAMPLE)


    def assert_initialize_driver(self, driver, initial_protocol_state=ProtocolState.AUTOSAMPLE):
        """
        Initialize an instrument driver with a mock port agent.  This will allow us to test the
        got data method.  Will the instrument, using test mode, through it's connection state
        machine.  End result, the driver will be in test mode and the connection state will be
        connected.
        @param driver: Instrument driver instance.
        @param initial_protocol_state: the state to force the driver too
        """
        mock_port_agent = Mock()

        # Put the driver into test mode
        driver.set_test_mode(True)

        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.UNCONFIGURED)

        # Now configure the driver with the mock_port_agent, verifying
        # that the driver transitions to that state
        config = {'mock_port_agent': mock_port_agent}
        driver.configure(config=config)

        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.DISCONNECTED)

        # Invoke the connect method of the driver: should connect to mock
        # port agent.  Verify that the connection FSM transitions to CONNECTED,
        # (which means that the FSM should now be reporting the ProtocolState).
        driver.connect()
        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverProtocolState.UNKNOWN)

        # Force the instrument into a known state
        self.assert_force_state(driver, initial_protocol_state)

    def assert_force_state(self, driver, protocol_state):
        """
        For the driver state to protocol_state
        @param driver: Instrument driver instance.
        @param protocol_state: State to transistion to
        """
        driver.test_force_state(state=protocol_state)
        current_state = driver.get_resource_state()
        self.assertEqual(current_state, protocol_state)