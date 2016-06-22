"""
@package mi.instrument.nortek.vector.ooicore.test.test_driver
@file /Users/Bill/WorkSpace/marine-integrations/mi/instrument/nortek/vector/ooicore/driver.py
@author Bill Bollenbacher
@brief Test cases for ooicore driver
"""
import time
import ntplib

from nose.plugins.attrib import attr

from mi.instrument.nortek import common
from ooi.logging import log

from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import ParameterTestConfigKey
from mi.instrument.nortek.test.test_driver import NortekUnitTest, NortekIntTest, DriverTestMixinSub, bad_sample
from mi.core.instrument.instrument_driver import DriverConfigKey
from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue
from mi.core.instrument.chunker import StringChunker
from mi.core.exceptions import SampleException
from mi.core.time_tools import timegm_to_float
from mi.instrument.nortek.driver import ProtocolEvent, Parameter, EngineeringParameter, ProtocolState
from mi.instrument.nortek.vector.ooicore.driver import Protocol
from mi.instrument.nortek.particles import (VectorDataParticleType, VectorVelocityDataParticleKey,
                                            VectorVelocityHeaderDataParticleKey, VectorSystemDataParticleKey,
                                            VectorVelocityHeaderDataParticle, VectorVelocityDataParticle,
                                            VectorSystemDataParticle)


__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.nortek.vector.ooicore.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='nortek_vector_dw_ooicore',
    instrument_agent_name='nortek_vector_dw_ooicore_agent',
    instrument_agent_packet_config=VectorDataParticleType(),
    driver_startup_config={
        DriverConfigKey.PARAMETERS: {}}
)

VID = DataParticleKey.VALUE_ID
VAL = DataParticleKey.VALUE


# velocity data particle & sample
def velocity_sample():
    sample_as_hex = "a51000db00008f10000049f041f72303303132120918d8f7"
    return sample_as_hex.decode('hex')

# these values checkout against the sample above
velocity_particle = [{VID: VectorVelocityDataParticleKey.ANALOG_INPUT2, VAL: 0},
                     {VID: VectorVelocityDataParticleKey.COUNT, VAL: 219},
                     {VID: VectorVelocityDataParticleKey.PRESSURE, VAL: 4239},
                     {VID: VectorVelocityDataParticleKey.ANALOG_INPUT1, VAL: 0},
                     {VID: VectorVelocityDataParticleKey.VELOCITY_BEAM1, VAL: -4023},
                     {VID: VectorVelocityDataParticleKey.VELOCITY_BEAM2, VAL: -2239},
                     {VID: VectorVelocityDataParticleKey.VELOCITY_BEAM3, VAL: 803},
                     {VID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM1, VAL: 48},
                     {VID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM2, VAL: 49},
                     {VID: VectorVelocityDataParticleKey.AMPLITUDE_BEAM3, VAL: 50},
                     {VID: VectorVelocityDataParticleKey.CORRELATION_BEAM1, VAL: 18},
                     {VID: VectorVelocityDataParticleKey.CORRELATION_BEAM2, VAL: 9},
                     {VID: VectorVelocityDataParticleKey.CORRELATION_BEAM3, VAL: 24}]


# velocity header data particle & sample
def velocity_header_sample():
    sample_as_hex = "a512150012491711121270032f2f2e0002090d0000000000000000000000000000000000000000005d70"
    return sample_as_hex.decode('hex')

# these values checkout against the sample above
velocity_header_particle = [{VID: VectorVelocityHeaderDataParticleKey.TIMESTAMP, VAL: '2012-12-17 11:12:49'},
                            {VID: VectorVelocityHeaderDataParticleKey.NUMBER_OF_RECORDS, VAL: 880},
                            {VID: VectorVelocityHeaderDataParticleKey.NOISE1, VAL: 47},
                            {VID: VectorVelocityHeaderDataParticleKey.NOISE2, VAL: 47},
                            {VID: VectorVelocityHeaderDataParticleKey.NOISE3, VAL: 46},
                            {VID: VectorVelocityHeaderDataParticleKey.CORRELATION1, VAL: 2},
                            {VID: VectorVelocityHeaderDataParticleKey.CORRELATION2, VAL: 9},
                            {VID: VectorVelocityHeaderDataParticleKey.CORRELATION3, VAL: 13}]


# system data particle & sample
def system_sample():
    sample_as_hex = "a5110e0003261317121294007c3b83041301cdfe0a08007b0000e4d9"
    return sample_as_hex.decode('hex')

# these values checkout against the sample above
system_particle = [{VID: VectorSystemDataParticleKey.TIMESTAMP, VAL: '2012-12-13 17:03:26'},
                   {VID: VectorSystemDataParticleKey.BATTERY, VAL: 148},
                   {VID: VectorSystemDataParticleKey.SOUND_SPEED, VAL: 15228},
                   {VID: VectorSystemDataParticleKey.HEADING, VAL: 1155},
                   {VID: VectorSystemDataParticleKey.PITCH, VAL: 275},
                   {VID: VectorSystemDataParticleKey.ROLL, VAL: -307},
                   {VID: VectorSystemDataParticleKey.TEMPERATURE, VAL: 2058},
                   {VID: VectorSystemDataParticleKey.ERROR, VAL: 0},
                   {VID: VectorSystemDataParticleKey.STATUS, VAL: 123},
                   {VID: VectorSystemDataParticleKey.ANALOG_INPUT, VAL: 0}]


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
#  In python, mixin classes are classes designed such that they wouldn't be   #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.                                      #
###############################################################################
class VectorDriverTestMixinSub(DriverTestMixinSub):
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

    _sample_parameters_01 = {
        VectorVelocityDataParticleKey.ANALOG_INPUT2: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.COUNT: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.PRESSURE: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.ANALOG_INPUT1: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.VELOCITY_BEAM1: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.VELOCITY_BEAM2: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.VELOCITY_BEAM3: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.AMPLITUDE_BEAM1: {TYPE: int, VALUE: 1, REQUIRED: True},
        VectorVelocityDataParticleKey.AMPLITUDE_BEAM2: {TYPE: int, VALUE: 1, REQUIRED: True},
        VectorVelocityDataParticleKey.AMPLITUDE_BEAM3: {TYPE: int, VALUE: 1, REQUIRED: True},
        VectorVelocityDataParticleKey.CORRELATION_BEAM1: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.CORRELATION_BEAM2: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityDataParticleKey.CORRELATION_BEAM3: {TYPE: int, VALUE: 0, REQUIRED: True}
    }

    _sample_parameters_02 = {
        VectorVelocityHeaderDataParticleKey.TIMESTAMP: {TYPE: unicode, VALUE: '', REQUIRED: True},
        VectorVelocityHeaderDataParticleKey.NUMBER_OF_RECORDS: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityHeaderDataParticleKey.NOISE1: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityHeaderDataParticleKey.NOISE2: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityHeaderDataParticleKey.NOISE3: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityHeaderDataParticleKey.CORRELATION1: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityHeaderDataParticleKey.CORRELATION2: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorVelocityHeaderDataParticleKey.CORRELATION3: {TYPE: int, VALUE: 0, REQUIRED: True}
    }

    _system_data_parameter = {
        VectorSystemDataParticleKey.TIMESTAMP: {TYPE: unicode, VALUE: '', REQUIRED: True},
        VectorSystemDataParticleKey.BATTERY: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.SOUND_SPEED: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.HEADING: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.PITCH: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.ROLL: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.TEMPERATURE: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.ERROR: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.STATUS: {TYPE: int, VALUE: 0, REQUIRED: True},
        VectorSystemDataParticleKey.ANALOG_INPUT: {TYPE: int, VALUE: 0, REQUIRED: True}
    }

    def assert_particle_sample(self, data_particle, verify_values=False):
        """
        Verify vel3d_cd_sample particle
        @param data_particle  VectorVelocityDataParticleKey data particle
        @param verify_values  bool, should we verify parameter values
        """

        self.assert_data_particle_keys(VectorVelocityDataParticleKey, self._sample_parameters_01)
        log.debug('asserted keys')
        self.assert_data_particle_header(data_particle, VectorDataParticleType.VELOCITY)
        log.debug('asserted header')
        self.assert_data_particle_parameters(data_particle, self._sample_parameters_01, verify_values)
        log.debug('asserted particle params')

    def assert_particle_velocity(self, data_particle, verify_values=False):
        """
        Verify veld3d_cd_velocity particle
        @param data_particle  VectorVelocityHeaderDataParticleKey data particle
        @param verify_values  bool, should we verify parameter values
        """

        self.assert_data_particle_keys(VectorVelocityHeaderDataParticleKey, self._sample_parameters_02)
        self.assert_data_particle_header(data_particle, VectorDataParticleType.VELOCITY_HEADER)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters_02, verify_values)

    def assert_particle_system(self, data_particle, verify_values=False):
        """
        Verify vel3d_cd_system particle
        @param data_particle  VectorSystemDataParticleKey data particle
        @param verify_values  bool, should we verify parameter values
        """

        self.assert_data_particle_keys(VectorSystemDataParticleKey, self._system_data_parameter)
        self.assert_data_particle_header(data_particle, VectorDataParticleType.SYSTEM)
        self.assert_data_particle_parameters(data_particle, self._system_data_parameter, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class UnitFromIDK(NortekUnitTest):
    def setUp(self):
        NortekUnitTest.setUp(self)

    def test_driver_enums(self):
        """
        Verify driver specific enums have no duplicates
        Base unit test driver will test enums specific for the base class.
        """
        self.assert_enum_has_no_duplicates(VectorDataParticleType())

    def test_velocity_header_sample_format(self):
        """
        Verify driver can get velocity_header sample data out in a
        reasonable format. Parsed is all we care about...raw is tested in the
        base DataParticle tests
        """

        port_timestamp = 3555423720.711772
        driver_timestamp = 3555423722.711772
        text_timestamp = time.strptime('17/12/2012 11:12:49', "%d/%m/%Y %H:%M:%S")
        internal_timestamp = ntplib.system_to_ntp_time(timegm_to_float(text_timestamp))

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: VectorDataParticleType.VELOCITY_HEADER,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.INTERNAL_TIMESTAMP: internal_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: velocity_header_particle
        }

        self.compare_parsed_data_particle(VectorVelocityHeaderDataParticle,
                                          velocity_header_sample(),
                                          expected_particle)

    def test_velocity_sample_format(self):
        """
        Verify driver can get velocity sample data out in a reasonable
        format. Parsed is all we care about...raw is tested in the base
        DataParticle tests
        """

        port_timestamp = 3555423720.711772
        driver_timestamp = 3555423722.711772

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: VectorDataParticleType.VELOCITY,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: velocity_particle
        }

        self.compare_parsed_data_particle(VectorVelocityDataParticle,
                                          velocity_sample(),
                                          expected_particle)

    def test_system_sample_format(self):
        """
        Verify driver can get system sample data out in a reasonable
        format. Parsed is all we care about...raw is tested in the base
        DataParticle tests
        """
        port_timestamp = 3555423720.711772
        driver_timestamp = 3555423722.711772
        text_timestamp = time.strptime('13/12/2012 17:03:26', "%d/%m/%Y %H:%M:%S")
        internal_timestamp = ntplib.system_to_ntp_time(timegm_to_float(text_timestamp))

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: VectorDataParticleType.SYSTEM,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.INTERNAL_TIMESTAMP: internal_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: system_particle
        }

        self.compare_parsed_data_particle(VectorSystemDataParticle,
                                          system_sample(),
                                          expected_particle)

    def test_chunker(self):
        """
        Verify the chunker can parse each sample type
        1. complete data structure
        2. fragmented data structure
        3. combined data structure
        4. data structure with noise
        """
        chunker = StringChunker(Protocol.sieve_function)

        # test complete data structures
        self.assert_chunker_sample(chunker, velocity_sample())
        self.assert_chunker_sample(chunker, system_sample())
        self.assert_chunker_sample(chunker, velocity_header_sample())

        # test fragmented data structures
        self.assert_chunker_fragmented_sample(chunker, velocity_sample())
        self.assert_chunker_fragmented_sample(chunker, system_sample())
        self.assert_chunker_fragmented_sample(chunker, velocity_header_sample())

        # test combined data structures
        self.assert_chunker_combined_sample(chunker, velocity_sample())
        self.assert_chunker_combined_sample(chunker, system_sample())
        self.assert_chunker_combined_sample(chunker, velocity_header_sample())

        # test data structures with noise
        self.assert_chunker_sample_with_noise(chunker, velocity_sample())
        self.assert_chunker_sample_with_noise(chunker, system_sample())
        self.assert_chunker_sample_with_noise(chunker, velocity_header_sample())

    def test_corrupt_data_structures(self):
        """
        Verify when generating the particle, if the particle is corrupt, an exception is raised
        """
        particle = VectorVelocityHeaderDataParticle(bad_sample(), port_timestamp=3558720820.531179)
        with self.assertRaises(SampleException):
            particle.generate()

        particle = VectorSystemDataParticle(bad_sample(), port_timestamp=3558720820.531179)
        with self.assertRaises(SampleException):
            particle.generate()

        particle = VectorVelocityDataParticle(bad_sample(), port_timestamp=3558720820.531179)
        with self.assertRaises(SampleException):
            particle.generate()


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class IntFromIDK(NortekIntTest, VectorDriverTestMixinSub):

    def setUp(self):
        NortekIntTest.setUp(self)

    def test_acquire_sample(self):
        """
        Test acquire sample command and events.

        1. initialize the instrument to COMMAND state
        2. command the instrument to ACQUIRE SAMPLE
        3. verify the particle coming in
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE)
        self.assert_async_particle_generation(VectorDataParticleType.VELOCITY, self.assert_particle_sample, timeout=common.TIMEOUT)

    def test_command_autosample(self):
        """
        Test autosample command and events.

        1. initialize the instrument to COMMAND state
        2. command the instrument to AUTOSAMPLE
        3. verify the particle coming in
        4. command the instrument back to COMMAND state
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)

        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)

        self.assert_async_particle_generation(VectorDataParticleType.USER_CONFIG, self.assert_particle_user)
        self.assert_async_particle_generation(VectorDataParticleType.VELOCITY_HEADER, self.assert_particle_velocity)
        self.assert_async_particle_generation(VectorDataParticleType.SYSTEM, self.assert_particle_system)
        self.assert_async_particle_generation(VectorDataParticleType.VELOCITY, self.assert_particle_sample, timeout=45)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

    def test_parameters(self):
        """
        Verify that we can set the parameters

        1. Cannot set read only parameters
        2. Can set read/write parameters
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)

        #test read/write parameter
        self.assert_set(Parameter.TRANSMIT_PULSE_LENGTH, 2)
        self.assert_set(Parameter.RECEIVE_LENGTH, 8)
        self.assert_set(Parameter.TIME_BETWEEN_BURST_SEQUENCES, 44)
        self.assert_set(Parameter.TIMING_CONTROL_REGISTER, 131)
        self.assert_set(Parameter.COORDINATE_SYSTEM, 0)
        self.assert_set(Parameter.BIN_LENGTH, 8)
        self.assert_set(Parameter.ADJUSTMENT_SOUND_SPEED, 16658)
        self.assert_set(Parameter.VELOCITY_ADJ_TABLE, 'B50ePTk9Uz1uPYg9oj27PdQ97T0GPh4+Nj5OPmU+fT6TPqo+wD7WPuw+Aj8'
                                          'XPyw/QT9VP2k/fT+RP6Q/uD/KP90/8D8CQBRAJkA3QElAWkBrQHxAjECcQK'
                                          'xAvEDMQNtA6kD5QAhBF0ElQTNBQkFPQV1BakF4QYVBkkGeQatBt0HDQc9B20'
                                          'HnQfJB/UEIQhNCHkIoQjNCPUJHQlFCW0JkQm5Cd0KAQolCkUKaQqJCqkKyQrpC',)

        #test read only parameters (includes immutable, when not startup)
        self.assert_set_exception(EngineeringParameter.CLOCK_SYNC_INTERVAL, '12:00:00')
        self.assert_set_exception(EngineeringParameter.ACQUIRE_STATUS_INTERVAL, '12:00:00')
        self.assert_set_exception(Parameter.BLANKING_DISTANCE, 5)
        self.assert_set_exception(Parameter.TIME_BETWEEN_PINGS, 45)
        self.assert_set_exception(Parameter.NUMBER_PINGS, 1)
        self.assert_set_exception(Parameter.AVG_INTERVAL, 65)
        self.assert_set_exception(Parameter.USER_NUMBER_BEAMS, 4)
        self.assert_set_exception(Parameter.POWER_CONTROL_REGISTER, 1)
        self.assert_set_exception(Parameter.COMPASS_UPDATE_RATE, 2)
        self.assert_set_exception(Parameter.NUMBER_BINS, 2)
        self.assert_set_exception(Parameter.MEASUREMENT_INTERVAL, 601)
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
