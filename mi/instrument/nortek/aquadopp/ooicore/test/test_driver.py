"""
@package mi.instrument.nortek.aquadopp.ooicore.test.test_driver
@author Rachel Manoni
@brief Test cases for ooicore driver
"""
from mock import Mock
from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.idk.unit_test import InstrumentDriverTestCase, ParameterTestConfigKey
from mi.instrument.nortek import common
from mi.instrument.nortek.aquadopp.ooicore.driver import Protocol, InstrumentDriver
from mi.instrument.nortek.driver import ProtocolState, ProtocolEvent, Parameter, \
    EngineeringParameter, InstrumentPrompts
from mi.instrument.nortek.particles import NortekEngIdDataParticleKey, AquadoppVelocityDataParticleKey, \
    AquadoppDataParticleType, AquadoppVelocityDataParticle
from mi.instrument.nortek.test.test_driver import DriverTestMixinSub, user_config_sample
from mi.instrument.nortek.test.test_driver import NortekUnitTest, NortekIntTest
from mi.instrument.nortek.user_configuration import UserConfiguration
from mi.instrument.nortek.vector.ooicore.test.test_driver import bad_sample


__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.nortek.aquadopp.ooicore.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='nortek_aquadopp_dw_ooicore',
    instrument_agent_name='nortek_aquadopp_dw_ooicore_agent',
    instrument_agent_packet_config=None,
    driver_startup_config={
        DriverConfigKey.PARAMETERS: {
            Parameter.DEPLOYMENT_NAME: 'test',
            Parameter.COMMENTS: 'this is a test',
            #update the following two parameters to allow for faster collecting of samples during testing
            Parameter.AVG_INTERVAL: 1,
            Parameter.MEASUREMENT_INTERVAL: 1}}
)


def eng_id_sample():
    sample_as_hex = "415144"
    return sample_as_hex.decode('hex')

eng_id_particle = [{DataParticleKey.VALUE_ID: NortekEngIdDataParticleKey.ID, DataParticleKey.VALUE: "AQD 8493      "}]


def velocity_sample():
    sample_as_hex = "a5011500101926221211000000009300f83b810628017f01002d0000e3094c0122ff9afe1e1416006093"
    return sample_as_hex.decode('hex')

velocity_particle = [{'value_id': AquadoppVelocityDataParticleKey.TIMESTAMP, 'value': '2012-11-26 22:10:19'},
                     {'value_id': AquadoppVelocityDataParticleKey.ERROR, 'value': 0},
                     {'value_id': AquadoppVelocityDataParticleKey.ANALOG1, 'value': 0},
                     {'value_id': AquadoppVelocityDataParticleKey.BATTERY_VOLTAGE, 'value': 147},
                     {'value_id': AquadoppVelocityDataParticleKey.SOUND_SPEED_ANALOG2, 'value': 15352},
                     {'value_id': AquadoppVelocityDataParticleKey.HEADING, 'value': 1665},
                     {'value_id': AquadoppVelocityDataParticleKey.PITCH, 'value': 296},
                     {'value_id': AquadoppVelocityDataParticleKey.ROLL, 'value': 383},
                     {'value_id': AquadoppVelocityDataParticleKey.STATUS, 'value': 45},
                     {'value_id': AquadoppVelocityDataParticleKey.PRESSURE, 'value': 0},
                     {'value_id': AquadoppVelocityDataParticleKey.TEMPERATURE, 'value': 2531},
                     {'value_id': AquadoppVelocityDataParticleKey.VELOCITY_BEAM1, 'value': 332},
                     {'value_id': AquadoppVelocityDataParticleKey.VELOCITY_BEAM2, 'value': -222},
                     {'value_id': AquadoppVelocityDataParticleKey.VELOCITY_BEAM3, 'value': -358},
                     {'value_id': AquadoppVelocityDataParticleKey.AMPLITUDE_BEAM1, 'value': 30},
                     {'value_id': AquadoppVelocityDataParticleKey.AMPLITUDE_BEAM2, 'value': 20},
                     {'value_id': AquadoppVelocityDataParticleKey.AMPLITUDE_BEAM3, 'value': 22}]


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
class AquadoppDriverTestMixinSub(DriverTestMixinSub):
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

    #this particle can be used for both the velocity particle and the diagnostic particle
    _sample_velocity_diagnostic = {
        AquadoppVelocityDataParticleKey.TIMESTAMP: {TYPE: unicode, VALUE: '', REQUIRED: True},
        AquadoppVelocityDataParticleKey.ERROR: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.ANALOG1: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.BATTERY_VOLTAGE: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.SOUND_SPEED_ANALOG2: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.HEADING: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.PITCH: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.ROLL: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.PRESSURE: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.STATUS: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.TEMPERATURE: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.VELOCITY_BEAM1: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.VELOCITY_BEAM2: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.VELOCITY_BEAM3: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.AMPLITUDE_BEAM1: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.AMPLITUDE_BEAM2: {TYPE: int, VALUE: 0, REQUIRED: True},
        AquadoppVelocityDataParticleKey.AMPLITUDE_BEAM3: {TYPE: int, VALUE: 0, REQUIRED: True}
    }

    def assert_particle_velocity(self, data_particle, verify_values=False):
        """
        Verify velpt_velocity_data
        @param data_particle AquadoppVelocityDataParticleKey data particle
        @param verify_values bool, should we verify parameter values
        """
        self.assert_data_particle_keys(AquadoppVelocityDataParticleKey, self._sample_velocity_diagnostic)
        self.assert_data_particle_header(data_particle, AquadoppDataParticleType.VELOCITY)
        self.assert_data_particle_parameters(data_particle, self._sample_velocity_diagnostic, verify_values)


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
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class DriverUnitTest(NortekUnitTest):
    def setUp(self):
        NortekUnitTest.setUp(self)

    def test_driver_enums(self):
        """
        Verify driver specific enums have no duplicates
        Base unit test driver will test enums specific for the base class.
        """
        self.assert_enum_has_no_duplicates(AquadoppDataParticleType())

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected.  All states defined in this dict must
        also be defined in the protocol FSM.
        """

        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, self._capabilities)

    def test_velocity_sample_format(self):
        """
        Verify driver can get velocity sample data out in a reasonable format.
        Parsed is all we care about...raw is tested in the base DataParticle tests
        """
        port_timestamp = 3555423720.711772
        driver_timestamp = 3555423722.711772
        internal_timestamp = 3562956619.0

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: AquadoppDataParticleType.VELOCITY,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.INTERNAL_TIMESTAMP: internal_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: velocity_particle}

        self.compare_parsed_data_particle(AquadoppVelocityDataParticle, velocity_sample(), expected_particle)

    def test_chunker(self):
        """
        Verify the chunker can parse each sample type
        1. complete data structure
        2. fragmented data structure
        3. combined data structure
        4. data structure with noise
        """
        chunker = StringChunker(Protocol.sieve_function)

        self.assert_chunker_sample(chunker, velocity_sample())
        self.assert_chunker_fragmented_sample(chunker, velocity_sample())
        self.assert_chunker_combined_sample(chunker, velocity_sample())
        self.assert_chunker_sample_with_noise(chunker, velocity_sample())

    def test_corrupt_data_structures(self):
        """
        Verify when generating the particle, if the particle is corrupt, an exception is raised
        """
        particle = AquadoppVelocityDataParticle(bad_sample(), port_timestamp=3558720820.531179)

        with self.assertRaises(SampleException):
            particle.generate()

    def test_update_params(self):
        protocol = Protocol(InstrumentPrompts, common.NEWLINE, Mock())
        sample = user_config_sample()
        protocol._do_cmd_resp = Mock(return_value=sample)
        protocol._update_params()
        c1 = protocol._param_dict.get_config()

        result = protocol._create_set_output(protocol._param_dict)
        protocol._do_cmd_resp = Mock(return_value=result)
        protocol._update_params()
        c2 = protocol._param_dict.get_config()
        self.assertEqual(c1, c2)


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class IntFromIDK(NortekIntTest, AquadoppDriverTestMixinSub):

    def setUp(self):
        NortekIntTest.setUp(self)

    def test_acquire_sample(self):
        """
        Verify acquire sample command and events.
        1. initialize the instrument to COMMAND state
        2. command the driver to ACQUIRE SAMPLE
        3. verify the particle coming in
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE)
        self.assert_async_particle_generation(AquadoppDataParticleType.VELOCITY, self.assert_particle_velocity, timeout=common.TIMEOUT)

    def test_command_autosample(self):
        """
        Verify autosample command and events.
        1. initialize the instrument to COMMAND state
        2. command the instrument to AUTOSAMPLE state
        3. verify the particle coming in and the sampling is continuous (gather several samples)
        4. stop AUTOSAMPLE
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_async_particle_generation(AquadoppDataParticleType.VELOCITY, self.assert_particle_velocity,
                                              particle_count=4, timeout=common.TIMEOUT)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

    def test_parameters(self):
        """
        Verify that we can set the parameters

        1. Cannot set read only parameters
        2. Can set read/write parameters
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)

        #test read/write parameter
        self.assert_set(Parameter.BLANKING_DISTANCE, 50)
        self.assert_set(Parameter.TIMING_CONTROL_REGISTER, 131)
        self.assert_set(Parameter.COMPASS_UPDATE_RATE, 2)
        self.assert_set(Parameter.COORDINATE_SYSTEM, 1)
        self.assert_set(Parameter.VELOCITY_ADJ_TABLE, 'bu0ePTk9Uz1uPYg9oj27PdQ97T0GPh4+Nj5OPmU+fT6TPqo+wD7WPuw+Aj8'
                                          'XPyw/QT9VP2k/fT+RP6Q/uD/KP90/8D8CQBRAJkA3QElAWkBrQHxAjECcQK'
                                          'xAvEDMQNtA6kD5QAhBF0ElQTNBQkFPQV1BakF4QYVBkkGeQatBt0HDQc9B20'
                                          'HnQfJB/UEIQhNCHkIoQjNCPUJHQlFCW0JkQm5Cd0KAQolCkUKaQqJCqkKyQrpC',)

        #these need to update simultaneously
        #self.assert_set(Parameter.MEASUREMENT_INTERVAL, 61)
        #self.assert_set(Parameter.AVG_INTERVAL, 61)

        #test read only parameters (includes immutable, when not startup)
        self.assert_set_exception(EngineeringParameter.CLOCK_SYNC_INTERVAL, '12:00:00')
        self.assert_set_exception(EngineeringParameter.ACQUIRE_STATUS_INTERVAL, '12:00:00')
        self.assert_set_exception(Parameter.TRANSMIT_PULSE_LENGTH, 20)
        self.assert_set_exception(Parameter.TIME_BETWEEN_PINGS, 45)
        self.assert_set_exception(Parameter.NUMBER_PINGS, 1)
        self.assert_set_exception(Parameter.RECEIVE_LENGTH, 8)
        self.assert_set_exception(Parameter.TIME_BETWEEN_BURST_SEQUENCES, 1)
        self.assert_set_exception(Parameter.USER_NUMBER_BEAMS, 4)
        self.assert_set_exception(Parameter.POWER_CONTROL_REGISTER, 1)
        self.assert_set_exception(Parameter.NUMBER_BINS, 2)
        self.assert_set_exception(Parameter.BIN_LENGTH, 8)
        self.assert_set_exception(Parameter.ADJUSTMENT_SOUND_SPEED, 16658)
        self.assert_set_exception(Parameter.DEPLOYMENT_NAME, 'test')
        self.assert_set_exception(Parameter.WRAP_MODE, 0)
        self.assert_set_exception(Parameter.CLOCK_DEPLOY, 123)
        self.assert_set_exception(Parameter.DIAGNOSTIC_INTERVAL, 10801)
        self.assert_set_exception(Parameter.MODE, 49)
        self.assert_set_exception(Parameter.NUMBER_SAMPLES_DIAGNOSTIC, 2)
        self.assert_set_exception(Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC, 2)
        self.assert_set_exception(Parameter.NUMBER_PINGS_DIAGNOSTIC, 2)
        self.assert_set_exception(Parameter.MODE_TEST, 5)
        self.assert_set_exception(Parameter.ANALOG_INPUT_ADDR, '123')
        self.assert_set_exception(Parameter.SW_VERSION, 'blah')
        self.assert_set_exception(Parameter.COMMENTS, 'hello there')
        self.assert_set_exception(Parameter.WAVE_MEASUREMENT_MODE, 3)
        # self.assert_set_exception(Parameter.DYN_PERCENTAGE_POSITION, 3)
        # self.assert_set_exception(Parameter.WAVE_TRANSMIT_PULSE,3 )
        # self.assert_set_exception(Parameter.WAVE_BLANKING_DISTANCE, 3)
        # self.assert_set_exception(Parameter.WAVE_CELL_SIZE, 3)
        # self.assert_set_exception(Parameter.NUMBER_DIAG_SAMPLES, 1)
        self.assert_set_exception(Parameter.NUMBER_SAMPLES_PER_BURST, 4)
        self.assert_set_exception(Parameter.ANALOG_OUTPUT_SCALE, 234)
        self.assert_set_exception(Parameter.CORRELATION_THRESHOLD, 1234)
        self.assert_set_exception(Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG, 1)
        # self.assert_set_exception(Parameter.QUAL_CONSTANTS, 'consts')
