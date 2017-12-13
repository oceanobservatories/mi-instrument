"""
@package mi.instrument.seabird.sbe16plus_v2.dosta.test.test_driver
@file mi-instrument/mi/instrument/seabird/sbe16_plus_v2/dosta/test/test_driver.py
@author Dan Mergens
@brief Test cases for dosta attached to a ctdbp_no

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u
       $ bin/test_driver -i
       $ bin/test_driver -q

"""
__author__ = 'Dan Mergens'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr

from mi.core.log import get_logger

from mi.idk.unit_test import \
    DriverTestMixin, \
    InstrumentDriverTestCase, \
    InstrumentDriverUnitTestCase, \
    ParameterTestConfigKey

from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.chunker import StringChunker

from mi.instrument.seabird.sbe16plus_v2.driver import ProtocolState
from mi.instrument.seabird.sbe16plus_v2.driver import ProtocolEvent
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Parameter  # shares the CTD parameters, read-only

from mi.instrument.seabird.sbe16plus_v2.dosta.driver import \
    Capability, \
    DataParticleType, \
    DoSampleParticleKey, \
    InstrumentDriver, \
    Protocol

from mi.instrument.seabird.sbe16plus_v2.driver import NEWLINE

log = get_logger()

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.seabird.sbe16plus_v2.dosta.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='JI22B5',
    instrument_agent_name='seabird_sbe16plus_v2_dosta',
    instrument_agent_packet_config=DataParticleType(),

    driver_startup_config={DriverConfigKey.PARAMETERS: {
        Parameter.VOLT1: True,
        Parameter.OPTODE: True,
    }}
)


###############################################################################
#                                    RULES                                    #
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
class UtilMixin(DriverTestMixin):
    # InstrumentDriver = InstrumentDriver

    # Create some short names for the parameter test config
    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    DA = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES

    ###
    #  Instrument output (driver input) Definitions
    ###
    VALID_SAMPLE = "04570F0A1E910828FC47BC59F199952C64C9" + NEWLINE

    ###
    #  Parameter and Type Definitions
    ###
    _driver_capabilities = {
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
    }

    _driver_parameters = {
        # DOSTA specific parameters
        Parameter.OPTODE: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT1: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
    }

    _do_sample = {
        DoSampleParticleKey.EXT_VOLT0: {TYPE: int, VALUE: 23025, REQUIRED: True},
        DoSampleParticleKey.OXY_CALPHASE: {TYPE: int, VALUE: 39317, REQUIRED: True},
        DoSampleParticleKey.OXYGEN: {TYPE: int, VALUE: 2909385, REQUIRED: True},
        DoSampleParticleKey.OXY_TEMP: {TYPE: int, VALUE: 39317, REQUIRED: True},  # VOLT1
    }

    def assert_particle_sample(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  SBE19DataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(DoSampleParticleKey, self._do_sample)
        self.assert_data_particle_header(data_particle, DataParticleType.DO_SAMPLE, require_instrument_timestamp=False)
        self.assert_data_particle_parameters(data_particle, self._do_sample, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, UtilMixin):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(Capability())

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
        chunker = StringChunker(Protocol.sieve_function)

        self.assert_chunker_sample(chunker, self.VALID_SAMPLE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_SAMPLE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_SAMPLE)
        self.assert_chunker_combined_sample(chunker, self.VALID_SAMPLE)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, self.VALID_SAMPLE, self.assert_particle_sample, True)

    def test_capabilities(self):
        """
        This driver has no FSM and must not publish any capabilities.
        """
        capabilities = {
            ProtocolState.UNKNOWN: [ProtocolEvent.DISCOVER],
            ProtocolState.COMMAND: [ProtocolEvent.GET],
        }

        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)


