"""
@package mi.instrument.wetlabs.fluorometer.flord_d.test.test_driver
@file marine-integrations/mi/instrument/wetlabs/fluorometer/flort_d/driver.py
@author Tapana Gupta
@brief Test cases for flord_d driver

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

import gevent

from mock import Mock
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import InstrumentDriverUnitTestCase

from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import ParameterTestConfigKey
from mi.idk.unit_test import AgentCapabilityType

from mi.core.time_tools import get_timestamp_delayed

from mi.core.instrument.chunker import StringChunker

from mi.instrument.wetlabs.fluorometer.flort_d.test.test_driver import DriverTestMixinSub

from mi.instrument.wetlabs.fluorometer.flord_d.driver import InstrumentDriver
from mi.instrument.wetlabs.fluorometer.flord_d.driver import FlordProtocol

from mi.instrument.wetlabs.fluorometer.flort_d.driver import FlordDMNU_Particle, FlordDSample_Particle
from mi.instrument.wetlabs.fluorometer.flort_d.driver import DataParticleType
from mi.instrument.wetlabs.fluorometer.flort_d.driver import InstrumentCommand
from mi.instrument.wetlabs.fluorometer.flort_d.driver import ProtocolState
from mi.instrument.wetlabs.fluorometer.flort_d.driver import ProtocolEvent
from mi.instrument.wetlabs.fluorometer.flort_d.driver import Capability
from mi.instrument.wetlabs.fluorometer.flort_d.driver import Parameter

from mi.instrument.wetlabs.fluorometer.flort_d.driver import Prompt

from mi.instrument.wetlabs.fluorometer.flort_d.driver import FlordDMNU_ParticleKey
from mi.instrument.wetlabs.fluorometer.flort_d.driver import FlordDSample_ParticleKey
from mi.instrument.wetlabs.fluorometer.flort_d.driver import NEWLINE

from mi.core.instrument.instrument_driver import DriverProtocolState, DriverConfigKey, ResourceAgentState

# SAMPLE DATA FOR TESTING
from mi.instrument.wetlabs.fluorometer.flord_d.test.sample_data import SAMPLE_MNU_RESPONSE
from mi.instrument.wetlabs.fluorometer.flord_d.test.sample_data import SAMPLE_SAMPLE_RESPONSE
from mi.instrument.wetlabs.fluorometer.flord_d.test.sample_data import SAMPLE_MET_RESPONSE

from mi.core.exceptions import InstrumentCommandException, SampleException

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.wetlabs.fluorometer.flord_d.driver',
    driver_class="FlordInstrumentDriver",

    instrument_agent_resource_id='3DLE2A',
    instrument_agent_name='wetlabs_fluorometer_flord_d',
    instrument_agent_packet_config=DataParticleType(),

    driver_startup_config={
        DriverConfigKey.PARAMETERS: {Parameter.RUN_WIPER_INTERVAL: '00:10:00',
                                     Parameter.RUN_CLOCK_SYNC_INTERVAL: '00:10:00',
                                     Parameter.RUN_ACQUIRE_STATUS_INTERVAL: '00:10:00'}}
)

#################################### RULES ####################################
#                                                                             #
# Common capabilities in the base class                                       #
#                                                                             #
# Instrument specific in the derived class                                    #
#                                                                             #
# Generator spits out either stubs or comments describing test this here,     #
# test that there.                                                            #
#                                                                             #
# Qualification tests are driven through the instrument_agent                 #
#                                                                             #
###############################################################################

###############################################################################
#                           DRIVER TEST MIXIN        		                  #
#     Defines a set of constants and assert methods used for data particle    #
#     verification 														      #
#                                                                             #
#  In python, mixin classes are classes designed such that they wouldn't be   #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.									  #
###############################################################################


class FlordDriverTestMixinSub(DriverTestMixinSub):
    """
    Mixin class used for storing data particle constance and common data assertion methods.
    """

    #Create some short names for the parameter test config
    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    DA = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES
    _Driver = InstrumentDriver

    _flordD_mnu_parameters = {
        FlordDMNU_ParticleKey.SERIAL_NUM: {TYPE: unicode, VALUE: 'BBFL2W-993', REQUIRED: True},
        FlordDMNU_ParticleKey.FIRMWARE_VER: {TYPE: unicode, VALUE: 'Triplet5.20', REQUIRED: True},
        FlordDMNU_ParticleKey.AVE: {TYPE: int, VALUE: 1, REQUIRED: True},
        FlordDMNU_ParticleKey.PKT: {TYPE: int, VALUE: 0, REQUIRED: True},
        FlordDMNU_ParticleKey.M1D: {TYPE: int, VALUE: 0, REQUIRED: True},
        FlordDMNU_ParticleKey.M2D: {TYPE: int, VALUE: 0, REQUIRED: True},
        FlordDMNU_ParticleKey.M1S: {TYPE: float, VALUE: 1.000E+00, REQUIRED: True},
        FlordDMNU_ParticleKey.M2S: {TYPE: float, VALUE: 1.000E+00, REQUIRED: True},
        FlordDMNU_ParticleKey.SEQ: {TYPE: int, VALUE: 0, REQUIRED: True},
        FlordDMNU_ParticleKey.RAT: {TYPE: int, VALUE: 19200, REQUIRED: True},
        FlordDMNU_ParticleKey.SET: {TYPE: int, VALUE: 0, REQUIRED: True},
        FlordDMNU_ParticleKey.REC: {TYPE: int, VALUE: 1, REQUIRED: True},
        FlordDMNU_ParticleKey.MAN: {TYPE: int, VALUE: 0, REQUIRED: True},
        FlordDMNU_ParticleKey.INT: {TYPE: unicode, VALUE: '00:00:10', REQUIRED: True},
        FlordDMNU_ParticleKey.DAT: {TYPE: unicode, VALUE: '07/11/13', REQUIRED: True},
        FlordDMNU_ParticleKey.CLK: {TYPE: unicode, VALUE: '12:48:34', REQUIRED: True},
        FlordDMNU_ParticleKey.MST: {TYPE: unicode, VALUE: '12:48:31', REQUIRED: True},
        FlordDMNU_ParticleKey.MEM: {TYPE: int, VALUE: 4095, REQUIRED: True}
    }

    _flordD_sample_parameters = {
        FlordDSample_ParticleKey.date_string: {TYPE: unicode, VALUE: '07/16/13', REQUIRED: True},
        FlordDSample_ParticleKey.time_string: {TYPE: unicode, VALUE: '09:33:06', REQUIRED: True},
        FlordDSample_ParticleKey.wave_beta: {TYPE: int, VALUE: 700, REQUIRED: True},
        FlordDSample_ParticleKey.raw_sig_beta: {TYPE: int, VALUE: 4130, REQUIRED: True},
        FlordDSample_ParticleKey.wave_chl: {TYPE: int, VALUE: 695, REQUIRED: True},
        FlordDSample_ParticleKey.raw_sig_chl: {TYPE: int, VALUE: 1018, REQUIRED: True},
        FlordDSample_ParticleKey.raw_temp: {TYPE: int, VALUE: 525, REQUIRED: True},
        FlordDSample_ParticleKey.SIG_1_OFFSET: {TYPE: float, VALUE: 0, REQUIRED: True},
        FlordDSample_ParticleKey.SIG_2_OFFSET: {TYPE: float, VALUE: 0, REQUIRED: True},
        FlordDSample_ParticleKey.SIG_1_SCALE_FACTOR: {TYPE: int, VALUE: 0, REQUIRED: True},
        FlordDSample_ParticleKey.SIG_2_SCALE_FACTOR: {TYPE: int, VALUE: 0, REQUIRED: True}
    }

    # #
    # Driver Parameter Methods
    # #
    def assert_particle_mnu(self, data_particle, verify_values=False):
        """
        Verify flortd_mnu particle
        @param data_particle:  FlortDMNU_ParticleKey data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(FlordDMNU_ParticleKey, self._flordD_mnu_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.FLORDD_MNU)
        self.assert_data_particle_parameters(data_particle, self._flordD_mnu_parameters, verify_values)

    def assert_particle_sample(self, data_particle, verify_values=False):
        """
        Verify flortd_sample particle
        @param data_particle:  FlortDSample_ParticleKey data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(FlordDSample_ParticleKey, self._flordD_sample_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.FLORDD_SAMPLE)
        self.assert_data_particle_parameters(data_particle, self._flordD_sample_parameters, verify_values)


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
class DriverUnitTest(InstrumentDriverUnitTestCase, FlordDriverTestMixinSub):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the capabilities
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(InstrumentCommand())

        # Test capabilities for duplicates, them verify that capabilities is a subset of protocol events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_driver_schema(self):
        """
        Get the driver schema and verify it is configured properly
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(FlordProtocol.sieve_function)

        self.assert_chunker_sample(chunker, SAMPLE_MNU_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_MNU_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_MNU_RESPONSE, 128)
        self.assert_chunker_combined_sample(chunker, SAMPLE_MNU_RESPONSE)

        self.assert_chunker_sample(chunker, SAMPLE_MET_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_MET_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_MET_RESPONSE, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_MET_RESPONSE)

        self.assert_chunker_sample(chunker, SAMPLE_SAMPLE_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_SAMPLE_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_SAMPLE_RESPONSE, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_SAMPLE_RESPONSE)

    def test_corrupt_data_sample(self):
        particle = FlordDMNU_Particle(SAMPLE_MNU_RESPONSE.replace('Ave 1', 'Ave foo'))
        with self.assertRaises(SampleException):
            particle.generate()

        particle = FlordDSample_Particle(SAMPLE_SAMPLE_RESPONSE.replace('700', 'foo'))
        with self.assertRaises(SampleException):
            particle.generate()

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, SAMPLE_MNU_RESPONSE, self.assert_particle_mnu, True)
        self.assert_particle_published(driver, SAMPLE_SAMPLE_RESPONSE, self.assert_particle_sample, True)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock(spec="PortAgentClient")
        protocol = FlordProtocol(Prompt, NEWLINE, mock_callback)
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
            ProtocolState.UNKNOWN:      [ProtocolEvent.DISCOVER],

            ProtocolState.COMMAND:      [ProtocolEvent.GET,
                                         ProtocolEvent.SET,
                                         ProtocolEvent.START_DIRECT,
                                         ProtocolEvent.START_AUTOSAMPLE,
                                         ProtocolEvent.ACQUIRE_STATUS,
                                         ProtocolEvent.RUN_WIPER,
                                         ProtocolEvent.ACQUIRE_SAMPLE,
                                         ProtocolEvent.CLOCK_SYNC],

            ProtocolState.AUTOSAMPLE:   [ProtocolEvent.STOP_AUTOSAMPLE,
                                         ProtocolEvent.RUN_WIPER_SCHEDULED,
                                         ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                         ProtocolEvent.SCHEDULED_ACQUIRE_STATUS,
                                         ProtocolEvent.GET],

            ProtocolState.DIRECT_ACCESS: [ProtocolEvent.STOP_DIRECT,
                                          ProtocolEvent.EXECUTE_DIRECT]
        }

        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)

    def test_command_response(self):
        """
        Test response with no errors
        Test the general command response will raise an exception if the command is not recognized by
        the instrument
        """
        mock_callback = Mock()
        protocol = FlordProtocol(Prompt, NEWLINE, mock_callback)

        #test response with no errors
        protocol._parse_command_response(SAMPLE_MNU_RESPONSE, None)

        #test response with 'unrecognized command'
        response = False
        try:
            protocol._parse_command_response('unrecognized command', None)
        except InstrumentCommandException:
            response = True
        finally:
            self.assertTrue(response)

        #test correct response with error
        response = False
        try:
            protocol._parse_command_response(SAMPLE_MET_RESPONSE + NEWLINE + 'unrecognized command', None)
        except InstrumentCommandException:
            response = True
        finally:
            self.assertTrue(response)

    def test_run_wiper_response(self):
        """
        Test response with no errors
        Test the run wiper response will raise an exception:
        1. if the command is not recognized by
        2. the status of the wiper is bad
        """
        mock_callback = Mock()
        protocol = FlordProtocol(Prompt, NEWLINE, mock_callback)

        #test response with no errors
        protocol._parse_run_wiper_response('mvs 1', None)

        #test response with 'unrecognized command'
        response = False
        try:
            protocol._parse_run_wiper_response('unrecognized command', None)
        except InstrumentCommandException:
            response = True
        finally:
            self.assertTrue(response)

        #test response with error
        response = False
        try:
            protocol._parse_run_wiper_response("mvs 0" + NEWLINE, None)
        except InstrumentCommandException:
            response = True
        finally:
            self.assertTrue(response)

    def test_discover_state(self):
        """
        Test discovering the instrument in the COMMAND state and in the AUTOSAMPLE state
        """
        mock_callback = Mock()
        protocol = FlordProtocol(Prompt, NEWLINE, mock_callback)

        #COMMAND state
        protocol._linebuf = SAMPLE_MNU_RESPONSE
        protocol._promptbuf = SAMPLE_MNU_RESPONSE
        next_state, next_agent_state = protocol._handler_unknown_discover()
        self.assertEqual(next_state, DriverProtocolState.COMMAND)
        self.assertEqual(next_agent_state, ResourceAgentState.IDLE)

        #AUTOSAMPLE state
        protocol._linebuf = SAMPLE_SAMPLE_RESPONSE
        protocol._promptbuf = SAMPLE_SAMPLE_RESPONSE
        next_state, next_agent_state = protocol._handler_unknown_discover()
        self.assertEqual(next_state, DriverProtocolState.AUTOSAMPLE)
        self.assertEqual(next_agent_state, ResourceAgentState.STREAMING)

    def test_create_commands(self):
        """
        Test creating different types of commands
        1. command with no end of line
        2. simple command with no parameters
        3. command with parameter
        """
        #create the operator commands
        mock_callback = Mock()
        protocol = FlordProtocol(Prompt, NEWLINE, mock_callback)

        #!!!!!
        cmd = protocol._build_no_eol_command('!!!!!')
        self.assertEqual(cmd, '!!!!!')
        #$met
        cmd = protocol._build_simple_command('$met')
        self.assertEqual(cmd, '$met' + NEWLINE)
        #$mnu
        cmd = protocol._build_simple_command('$mnu')
        self.assertEqual(cmd, '$mnu' + NEWLINE)
        #$run
        cmd = protocol._build_simple_command('$run')
        self.assertEqual(cmd, '$run' + NEWLINE)

        #parameters
        cmd = protocol._build_single_parameter_command('$ave', Parameter.MEASUREMENTS_PER_REPORTED, 14)
        self.assertEqual(cmd, '$ave 14' + NEWLINE)
        cmd = protocol._build_single_parameter_command('$m2d', Parameter.MEASUREMENT_2_DARK_COUNT, 34)
        self.assertEqual(cmd, '$m2d 34' + NEWLINE)
        cmd = protocol._build_single_parameter_command('$m1s', Parameter.MEASUREMENT_1_SLOPE, 23.1341)
        self.assertEqual(cmd, '$m1s 23.1341' + NEWLINE)
        cmd = protocol._build_single_parameter_command('$dat', Parameter.DATE, '041014')
        self.assertEqual(cmd, '$dat 041014' + NEWLINE)
        cmd = protocol._build_single_parameter_command('$clk', Parameter.TIME, '010034')
        self.assertEqual(cmd, '$clk 010034' + NEWLINE)


