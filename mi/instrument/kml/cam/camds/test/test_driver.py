"""
@package mi.instrument.kml.cam.camds.driver
@file marine-integrations/mi/instrument/kml/cam/camds/test/test_driver.py
@author Sung Ahn
@brief Test Driver for CAMDS
Release notes:

"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import copy
import time

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.instrument.chunker import StringChunker
from mi.core.log import get_logger

log = get_logger()

from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase

from mi.instrument.kml.cam.camds.driver import DataParticleType, CamdsDiskStatusKey, CamdsHealthStatusKey

from mi.idk.unit_test import DriverTestMixin

from mi.idk.unit_test import DriverStartupConfigKey

from mi.instrument.kml.cam.camds.driver import Parameter, ParameterIndex
from mi.instrument.kml.cam.camds.driver import CAMDSPrompt, InstrumentDriver, CAMDSProtocol
from mi.instrument.kml.cam.camds.driver import ScheduledJob
from mi.instrument.kml.cam.camds.driver import InstrumentCmds, ProtocolState, ProtocolEvent, Capability

from mi.idk.unit_test import InstrumentDriverTestCase, ParameterTestConfigKey

from mi.core.common import BaseEnum

NEWLINE = '\r\n'

# ##
# Driver parameters for tests
###

InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.kml.cam.camds.driver',
    driver_class="InstrumentDriver",
    instrument_agent_resource_id='HTWZMW',
    instrument_agent_preload_id='IA7',
    instrument_agent_name='kml cam',
    instrument_agent_packet_config=DataParticleType(),

    driver_startup_config={
        DriverStartupConfigKey.PARAMETERS: {
            Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]: Parameter.ACQUIRE_STATUS_INTERVAL[
                ParameterIndex.DEFAULT_DATA],
            Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]: Parameter.AUTO_CAPTURE_DURATION[
                ParameterIndex.DEFAULT_DATA],
            Parameter.CAMERA_GAIN[ParameterIndex.KEY]: Parameter.CAMERA_GAIN[ParameterIndex.DEFAULT_DATA],
            Parameter.CAMERA_MODE[ParameterIndex.KEY]: Parameter.CAMERA_MODE[ParameterIndex.DEFAULT_DATA],
            Parameter.COMPRESSION_RATIO[ParameterIndex.KEY]: Parameter.COMPRESSION_RATIO[ParameterIndex.DEFAULT_DATA],
            Parameter.FOCUS_POSITION[ParameterIndex.KEY]: Parameter.FOCUS_POSITION[ParameterIndex.DEFAULT_DATA],
            Parameter.FOCUS_SPEED[ParameterIndex.KEY]: Parameter.FOCUS_SPEED[ParameterIndex.DEFAULT_DATA],
            Parameter.FRAME_RATE[ParameterIndex.KEY]: Parameter.FRAME_RATE[ParameterIndex.DEFAULT_DATA],
            Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY]: Parameter.IMAGE_RESOLUTION[ParameterIndex.DEFAULT_DATA],
            Parameter.IRIS_POSITION[ParameterIndex.KEY]: Parameter.IRIS_POSITION[ParameterIndex.DEFAULT_DATA],

            Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]: Parameter.LAMP_BRIGHTNESS[ParameterIndex.DEFAULT_DATA],
            Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY]: Parameter.NETWORK_DRIVE_LOCATION[
                ParameterIndex.DEFAULT_DATA],
            Parameter.NTP_SETTING[ParameterIndex.KEY]: Parameter.NTP_SETTING[ParameterIndex.DEFAULT_DATA],
            Parameter.PAN_POSITION[ParameterIndex.KEY]: Parameter.PAN_POSITION[ParameterIndex.DEFAULT_DATA],
            Parameter.PAN_SPEED[ParameterIndex.KEY]: Parameter.PAN_SPEED[ParameterIndex.DEFAULT_DATA],
            Parameter.PRESET_NUMBER[ParameterIndex.KEY]: Parameter.PRESET_NUMBER[ParameterIndex.DEFAULT_DATA],
            Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]: Parameter.SAMPLE_INTERVAL[ParameterIndex.DEFAULT_DATA],
            Parameter.SHUTTER_SPEED[ParameterIndex.KEY]: Parameter.SHUTTER_SPEED[ParameterIndex.DEFAULT_DATA],
            Parameter.SOFT_END_STOPS[ParameterIndex.KEY]: Parameter.SOFT_END_STOPS[ParameterIndex.DEFAULT_DATA],
            Parameter.TILT_POSITION[ParameterIndex.KEY]: Parameter.TILT_POSITION[ParameterIndex.DEFAULT_DATA],
            Parameter.TILT_SPEED[ParameterIndex.KEY]: Parameter.TILT_SPEED[ParameterIndex.DEFAULT_DATA],

            Parameter.VIDEO_FORWARDING[ParameterIndex.KEY]: Parameter.VIDEO_FORWARDING[ParameterIndex.DEFAULT_DATA],
            Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]: Parameter.VIDEO_FORWARDING_TIMEOUT[
                ParameterIndex.DEFAULT_DATA],
            Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY]: Parameter.WHEN_DISK_IS_FULL[ParameterIndex.DEFAULT_DATA],
            Parameter.ZOOM_POSITION[ParameterIndex.KEY]: Parameter.ZOOM_POSITION[ParameterIndex.DEFAULT_DATA],
            Parameter.ZOOM_SPEED[ParameterIndex.KEY]: Parameter.ZOOM_SPEED[ParameterIndex.DEFAULT_DATA]
        },
        DriverStartupConfigKey.SCHEDULER: {
            ScheduledJob.VIDEO_FORWARDING: {},
            ScheduledJob.SAMPLE: {},
            ScheduledJob.STATUS: {},
            ScheduledJob.STOP_CAPTURE: {}
        }
    }
)


class TeledynePrompt(BaseEnum):
    """
    Device i/o prompts..
    """
    COMMAND = '\r\n>\r\n>'
    ERR = 'ERR:'


###################################################################

###
#   Driver constant definitions
###

###############################################################################
#                           DATA PARTICLE TEST MIXIN                          #
#     Defines a set of assert methods used for data particle verification     #
#                                                                             #
#  In python mixin classes are classes designed such that they wouldn't be    #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.
###############################################################################
class CAMDSMixin(DriverTestMixin):
    """
    Mixin class used for storing data particle constance
    and common data assertion methods.
    """
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
    # Parameter and Type Definitions
    ###
    _driver_parameters = {

        Parameter.CAMERA_GAIN[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.CAMERA_GAIN[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.CAMERA_GAIN[ParameterIndex.D_DEFAULT]},
        Parameter.CAMERA_MODE[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.CAMERA_MODE[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.CAMERA_MODE[ParameterIndex.D_DEFAULT]},
        Parameter.COMPRESSION_RATIO[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.COMPRESSION_RATIO[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.COMPRESSION_RATIO[ParameterIndex.D_DEFAULT]},
        Parameter.FOCUS_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.FOCUS_POSITION[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.FOCUS_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.FOCUS_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.FOCUS_SPEED[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.FOCUS_SPEED[ParameterIndex.D_DEFAULT]},
        Parameter.FRAME_RATE[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.FRAME_RATE[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.FRAME_RATE[ParameterIndex.D_DEFAULT]},
        Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.IMAGE_RESOLUTION[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.IMAGE_RESOLUTION[ParameterIndex.D_DEFAULT]},
        Parameter.IRIS_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.IRIS_POSITION[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.IRIS_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.LAMP_BRIGHTNESS[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.LAMP_BRIGHTNESS[ParameterIndex.D_DEFAULT]},
        Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.D_DEFAULT]},
        Parameter.NTP_SETTING[ParameterIndex.KEY]:
            {TYPE: str, READONLY: True, DA: True, STARTUP: False,
             DEFAULT: None, VALUE: None},
        Parameter.PAN_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.PAN_POSITION[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.PAN_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.PAN_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.PAN_SPEED[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.PAN_SPEED[ParameterIndex.D_DEFAULT]},
        Parameter.SHUTTER_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.SHUTTER_SPEED[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.SHUTTER_SPEED[ParameterIndex.D_DEFAULT]},
        Parameter.SOFT_END_STOPS[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.SOFT_END_STOPS[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.SOFT_END_STOPS[ParameterIndex.D_DEFAULT]},
        Parameter.TILT_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.TILT_POSITION[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.TILT_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.TILT_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.TILT_SPEED[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.TILT_SPEED[ParameterIndex.D_DEFAULT]},
        Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY]:
            {TYPE: str, READONLY: True, DA: True, STARTUP: False,
             DEFAULT: None, VALUE: None},
        Parameter.ZOOM_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.ZOOM_POSITION[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.ZOOM_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.ZOOM_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.ZOOM_SPEED[ParameterIndex.D_DEFAULT],
             VALUE: Parameter.ZOOM_SPEED[ParameterIndex.D_DEFAULT]},

        # Engineering parameters
        Parameter.PRESET_NUMBER[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.PRESET_NUMBER[ParameterIndex.DEFAULT_DATA],
             VALUE: Parameter.PRESET_NUMBER[ParameterIndex.D_DEFAULT]},
        Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.DEFAULT_DATA],
             VALUE: Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.D_DEFAULT]},
        Parameter.VIDEO_FORWARDING[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.VIDEO_FORWARDING[ParameterIndex.DEFAULT_DATA],
             VALUE: Parameter.VIDEO_FORWARDING[ParameterIndex.D_DEFAULT]},
        Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.DEFAULT_DATA],
             VALUE: Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.D_DEFAULT]},
        Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.SAMPLE_INTERVAL[ParameterIndex.DEFAULT_DATA],
             VALUE: Parameter.SAMPLE_INTERVAL[ParameterIndex.D_DEFAULT]},
        Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: False,
             DEFAULT: Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.DEFAULT_DATA],
             VALUE: Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.D_DEFAULT]}
    }

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.STOP_CAPTURE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.ACQUIRE_STATUS: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.GOTO_PRESET: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.SET_PRESET: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LAMP_OFF: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LAMP_ON: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LASER_1_OFF: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LASER_1_ON: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LASER_2_OFF: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LASER_2_ON: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LASER_BOTH_OFF: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.LASER_BOTH_ON: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.ACQUIRE_SAMPLE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.EXECUTE_AUTO_CAPTURE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
    }

    size_1 = chr(0x01)
    size_2 = chr(0x02)
    size_3 = chr(0x03)
    size_5 = chr(0x05)
    size_6 = chr(0x06)
    size_9 = chr(0x09)
    size_A = chr(0x0A)
    size_C = chr(0x0C)

    size_4 = chr(0x04)
    size_7 = chr(0x07)
    size_8 = chr(0x08)
    size_B = chr(0x0B)
    _ACK = chr(0x06)

    _health_data = '<' + size_7 + ':' + size_6 + ':' + 'HS' + size_1 + size_2 + size_3 + '>'

    _health_dict = {
        CamdsHealthStatusKey.humidity: {'type': int, 'value': 2},
        CamdsHealthStatusKey.temp: {'type': int, 'value': 1},
        CamdsHealthStatusKey.error: {'type': int, 'value': 3}
    }

    _disk_data = '<' + size_B + ':' + size_6 + ':' + 'GC' + size_1 + size_2 + \
                 size_3 + size_4 + size_5 + size_6 + size_7 + '>'

    _disk_status_dict = {
        CamdsDiskStatusKey.disk_remaining: {'type': int, 'value': 100},
        CamdsDiskStatusKey.image_on_disk: {'type': int, 'value': 3},
        CamdsDiskStatusKey.image_remaining: {'type': int, 'value': 1029},
        CamdsDiskStatusKey.size: {'type': int, 'value': 1543},

    }

    # Driver Parameter Methods
    ###
    def assert_driver_parameters(self, current_parameters, verify_values=False):
        """
        Verify that all driver parameters are correct and potentially verify values.
        @param current_parameters: driver parameters read from the driver instance
        @param verify_values: should we verify values against definition?
        """
        log.debug("assert_driver_parameters current_parameters = " + str(current_parameters))
        temp_parameters = copy.deepcopy(self._driver_parameters)
        temp_parameters.update(self._driver_parameters)
        self.assert_parameters(current_parameters, temp_parameters, verify_values)

    def assert_health_data(self, data_particle, verify_values=True):
        """
        Verify CAMDS health status data particle
        @param data_particle: CAMDS health status DataParticle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, DataParticleType.CAMDS_HEALTH_STATUS)
        self.assert_data_particle_parameters(data_particle, self._health_dict)  # , verify_values

    def assert_disk_data(self, data_particle, verify_values=True):
        """
        Verify CAMDS disk status data particle
        @param data_particle: CAMDS disk status data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, DataParticleType.CAMDS_DISK_STATUS)
        self.assert_data_particle_parameters(data_particle, self._disk_status_dict)  # , verify_values


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, CAMDSMixin):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_driver_schema(self):
        """
         get the driver schema and verify it is configured properly
         """
        temp_parameters = copy.deepcopy(self._driver_parameters)
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, temp_parameters, self._driver_capabilities)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles

        self.assert_particle_published(driver, self._health_data, self.assert_health_data, True)
        self.assert_particle_published(driver, self._disk_data, self.assert_disk_data, True)

    def test_driver_parameters(self):
        """
        Verify the set of parameters known by the driver
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, ProtocolState.COMMAND)

        expected_parameters = sorted(self._driver_parameters.keys())

        expected_parameters = sorted(expected_parameters)
        reported_parameters = sorted(driver.get_resource(Parameter.ALL))

        self.assertEqual(reported_parameters, expected_parameters)

        # Verify the parameter definitions
        self.assert_driver_parameter_definition(driver, self._driver_parameters)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """

        self.assert_enum_has_no_duplicates(InstrumentCmds())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ScheduledJob())
        # Test capabilities for duplicates, them verify that capabilities is a subset of proto events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(CAMDSProtocol.sieve_function)

        self.assert_chunker_sample(chunker, self._health_data)
        self.assert_chunker_sample_with_noise(chunker, self._health_data)
        self.assert_chunker_fragmented_sample(chunker, self._health_data, 5)
        self.assert_chunker_combined_sample(chunker, self._health_data)

        self.assert_chunker_sample(chunker, self._disk_data)
        self.assert_chunker_sample_with_noise(chunker, self._disk_data)
        self.assert_chunker_fragmented_sample(chunker, self._disk_data, 6)
        self.assert_chunker_combined_sample(chunker, self._disk_data)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        my_event_callback = Mock(spec="UNKNOWN WHAT SHOULD GO HERE FOR evt_callback")
        protocol = CAMDSProtocol(CAMDSPrompt, NEWLINE, my_event_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(driver_capabilities, protocol._filter_capabilities(test_capabilities))

    def test_set(self):
        params = [
            (Parameter.CAMERA_GAIN, 1, '<\x04:GS:\x01>'),
            (Parameter.CAMERA_GAIN, 2, '<\x04:GS:\x02>'),
            (Parameter.CAMERA_GAIN, 3, '<\x04:GS:\x03>'),
            (Parameter.CAMERA_GAIN, 4, '<\x04:GS:\x04>'),
            (Parameter.CAMERA_GAIN, 5, '<\x04:GS:\x05>'),
            (Parameter.CAMERA_GAIN, 32, '<\x04:GS:\x20>'),
            (Parameter.CAMERA_GAIN, 255, '<\x04:GS:\xff>'),
            (Parameter.CAMERA_MODE, 0, '<\x04:SV:\x00>'),
            (Parameter.CAMERA_MODE, 9, '<\x04:SV:\x09>'),
            (Parameter.CAMERA_MODE, 10, '<\x04:SV:\x0a>'),
            (Parameter.CAMERA_MODE, 11, '<\x04:SV:\x0b>'),
            (Parameter.FRAME_RATE, 1, '<\x04:FR:\x01>'),
            (Parameter.FRAME_RATE, 5, '<\x04:FR:\x05>'),
            (Parameter.FRAME_RATE, 10, '<\x04:FR:\x0a>'),
            (Parameter.FRAME_RATE, 20, '<\x04:FR:\x14>'),
            (Parameter.FRAME_RATE, 30, '<\x04:FR:\x1e>'),
            (Parameter.IMAGE_RESOLUTION, 1, '<\x04:SD:\x01>'),
            (Parameter.IMAGE_RESOLUTION, 2, '<\x04:SD:\x02>'),
            (Parameter.IMAGE_RESOLUTION, 4, '<\x04:SD:\x04>'),
            (Parameter.IMAGE_RESOLUTION, 8, '<\x04:SD:\x08>'),
            (Parameter.IMAGE_RESOLUTION, 16, '<\x04:SD:\x10>'),
            (Parameter.IMAGE_RESOLUTION, 32, '<\x04:SD:\x20>'),
            (Parameter.IMAGE_RESOLUTION, 64, '<\x04:SD:\x40>'),
            (Parameter.IMAGE_RESOLUTION, 100, '<\x04:SD:\x64>'),
            (Parameter.PAN_SPEED, 1, '<\x04:DS:\x01>'),
            (Parameter.PAN_SPEED, 2, '<\x04:DS:\x02>'),
            (Parameter.PAN_SPEED, 4, '<\x04:DS:\x04>'),
            (Parameter.PAN_SPEED, 8, '<\x04:DS:\x08>'),
            (Parameter.PAN_SPEED, 16, '<\x04:DS:\x10>'),
            (Parameter.PAN_SPEED, 32, '<\x04:DS:\x20>'),
            (Parameter.PAN_SPEED, 50, '<\x04:DS:\x32>'),
            (Parameter.PAN_SPEED, 64, '<\x04:DS:\x40>'),
            (Parameter.PAN_SPEED, 100, '<\x04:DS:\x64>'),
            (Parameter.COMPRESSION_RATIO, 1, '<\x04:CD:\x01>'),
            (Parameter.COMPRESSION_RATIO, 2, '<\x04:CD:\x02>'),
            (Parameter.COMPRESSION_RATIO, 4, '<\x04:CD:\x04>'),
            (Parameter.COMPRESSION_RATIO, 8, '<\x04:CD:\x08>'),
            (Parameter.COMPRESSION_RATIO, 16, '<\x04:CD:\x10>'),
            (Parameter.COMPRESSION_RATIO, 32, '<\x04:CD:\x20>'),
            (Parameter.COMPRESSION_RATIO, 64, '<\x04:CD:\x40>'),
            (Parameter.COMPRESSION_RATIO, 100, '<\x04:CD:\x64>'),
            (Parameter.FOCUS_POSITION, 0, '<\x04:FG:\x00>'),
            (Parameter.FOCUS_POSITION, 100, '<\x04:FG:\x64>'),
            (Parameter.FOCUS_POSITION, 200, '<\x04:FG:\xc8>'),
            (Parameter.PAN_POSITION, 0, '<\x06:PP:000>'),
            (Parameter.PAN_POSITION, 45, '<\x06:PP:045>'),
            (Parameter.PAN_POSITION, 90, '<\x06:PP:090>'),
            (Parameter.SHUTTER_SPEED, '25:3', '<\x05:ET:\x19\x03>'),
            (Parameter.SHUTTER_SPEED, '6:7', '<\x05:ET:\x06\x07>'),
            (Parameter.SHUTTER_SPEED, '255:255', '<\x05:ET:\xff\xff>'),
            (Parameter.TILT_POSITION, 0, '<\x06:TP:000>'),
            (Parameter.TILT_POSITION, 45, '<\x06:TP:045>'),
            (Parameter.TILT_POSITION, 90, '<\x06:TP:090>'),
            (Parameter.TILT_SPEED, 0, '<\x04:TA:\x00>'),
            (Parameter.TILT_SPEED, 50, '<\x04:TA:\x32>'),
            (Parameter.TILT_SPEED, 100, '<\x04:TA:\x64>'),
            (Parameter.ZOOM_SPEED, 0, '<\x04:ZX:\x00>'),
            (Parameter.FOCUS_SPEED, 0, '<\x04:FX:\x00>'),
            (Parameter.ZOOM_POSITION, 100, '<\x04:ZG:d>'),
            (Parameter.PAN_SPEED, 50, '<\x04:DS:2>'),
            (Parameter.PAN_POSITION, 90, '<\x06:PP:090>'),
            (Parameter.CAMERA_MODE, 9, '<\x04:SV:\t>'),
            (Parameter.TILT_SPEED, 50, '<\x04:TA:2>'),
            (Parameter.IRIS_POSITION, 8, '<\x04:IG:\x08>'),
            (Parameter.SOFT_END_STOPS, 1, '<\x04:ES:\x01>'),
            (Parameter.FOCUS_POSITION, 100, '<\x04:FG:d>'),
            (Parameter.COMPRESSION_RATIO, 100, '<\x04:CD:d>'),
            (Parameter.NETWORK_DRIVE_LOCATION, 0, '<\x04:FL:\x00>'),
            (Parameter.LAMP_BRIGHTNESS, '3:50', '<\x05:BF:\x032>'),

        ]

        for param, input_value, output_value in params:
            key = param[ParameterIndex.KEY]

            self.assertEqual(output_value, self._build_set_command(key, input_value))

    def _build_set_command(self, param, val):
        """
        Build handler for set commands. param=val followed by newline.
        String val constructed by param dict formatting function.
        @param param the parameter key to set.
        @param val the parameter value to set.
        @return The set command to be sent to the device.
        @throws InstrumentParameterException if the parameter is not valid or
        if the formatting function could not accept the value passed.
        """

        try:

            if param in [Parameter.PAN_POSITION[ParameterIndex.KEY],
                         Parameter.TILT_POSITION[ParameterIndex.KEY]]:

                if not isinstance(val, int) or val > 999:
                    raise Exception('The desired value for %s must be an integer less than 999: %s'
                                    % (param, val))

                val = '%03d' % val

            elif isinstance(val, basestring):
                val = ''.join(chr(int(x)) for x in val.split(':'))

            else:
                val = chr(val)

            if param == Parameter.NTP_SETTING[ParameterIndex.KEY]:
                val = val + Parameter.NTP_SETTING[ParameterIndex.DEFAULT_DATA]

            data_size = len(val) + 3
            param_tuple = getattr(Parameter, param)

            set_cmd = '<%s:%s:%s>' % (chr(data_size), param_tuple[ParameterIndex.SET], val)

        except KeyError:
            raise Exception('Unknown driver parameter. %s' % param)

        return set_cmd


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(InstrumentDriverIntegrationTestCase, CAMDSMixin):
    _tested = {}

    def setUp(self):
        self.port_agents = {}
        InstrumentDriverIntegrationTestCase.setUp(self)

    def assert_disk_status(self, data_particle, verify_values=True):
        """
        Verify a disk status particle
        @param data_particle: CAMDS disk status particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, DataParticleType.CAMDS_DISK_STATUS)
        self.assert_data_particle_parameters(data_particle, self._disk_status_dict)  # , verify_values

    def assert_health_status(self, data_particle, verify_values=True):
        """
        Verify a health status particle
        @param data_particle: CAMDS health status particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, DataParticleType.CAMDS_HEALTH_STATUS)
        self.assert_data_particle_parameters(data_particle, self._health_dict)  # , verify_values

    def assert_sample_meta(self, data_particle, verify_values=True):
        """
        Verify an image meta particle
        @param data_particle: CAMDS image meta data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, DataParticleType.CAMDS_IMAGE_METADATA)

    def assert_acquire_status(self):
        """
        Check data stream types for acquire_status()
        """
        self.assert_async_particle_generation(DataParticleType.CAMDS_DISK_STATUS, self.assert_disk_status,
                                              timeout=60)
        self.assert_async_particle_generation(DataParticleType.CAMDS_HEALTH_STATUS,
                                              self.assert_health_status, timeout=60)

    def assert_acquire_sample(self):
        """
        Check data stream types for acquire_status()
        """
        self.assert_async_particle_generation(DataParticleType.CAMDS_IMAGE_METADATA, self.assert_sample_meta,
                                              timeout=60)

    def test_connection(self):
        log.debug("######## Starting test_connection ##########")
        self.assert_initialize_driver()

    # Overwritten method
    def test_driver_process(self):
        """
        Test for correct launch of driver process and communications, including asynchronous driver events.
        Overridden to support multiple port agents.
        """
        log.info("Ensuring driver process was started properly ...")

        # Verify processes exist.
        self.assertNotEqual(self.driver_process, None)
        drv_pid = self.driver_process.getpid()
        self.assertTrue(isinstance(drv_pid, int))

        self.assertNotEqual(self.port_agents, None)
        for port_agent in self.port_agents.values():
            pagent_pid = port_agent.get_pid()
            self.assertTrue(isinstance(pagent_pid, int))

        # Send a test message to the process interface, confirm result.
        reply = self.driver_client.cmd_dvr('process_echo')
        self.assert_(reply.startswith('ping from resource ppid:'))

        reply = self.driver_client.cmd_dvr('driver_ping', 'foo')
        self.assert_(reply.startswith('driver_ping: foo'))

        # Test the event thread publishes and client side picks up events.
        events = [
            'I am important event #1!',
            'And I am important event #2!'
        ]
        self.driver_client.cmd_dvr('test_events', events=events)
        time.sleep(1)

        # Confirm the events received are as expected.
        self.assertEqual(self.events, events)

        # Test the exception mechanism.
        # with self.assertRaises(ResourceError):
        #     exception_str = 'Oh no, something bad happened!'
        #     self.driver_client.cmd_dvr('test_exceptions', exception_str)

    # Set bulk params and test auto sampling
    def test_autosample_particle_generation(self):
        """
        Test that we can generate particles when in autosample
        """
        self.assert_initialize_driver()

        params = {
            Parameter.CAMERA_GAIN: 255,
            Parameter.CAMERA_MODE: 9,
            Parameter.FRAME_RATE: 30,
            Parameter.IMAGE_RESOLUTION: 1,
            Parameter.PAN_SPEED: 50,
            Parameter.COMPRESSION_RATIO: 100,
            Parameter.FOCUS_POSITION: 100,
            Parameter.PAN_POSITION: 90,
            Parameter.SHUTTER_SPEED: '255:255',
            Parameter.TILT_POSITION: 90,
            Parameter.TILT_SPEED: 50,
        }
        self.assert_set_bulk(params)

        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=10)

    # test commands in different modes

    def test_commands(self):

        """
        Run instrument commands from both command and streaming mode.
        """
        self.assert_initialize_driver()
        ####
        # First test in command mode
        ####
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE,
                                   delay=20)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, delay=2)
        self.assert_acquire_status()

        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE, delay=2)
        self.assert_acquire_sample()
        self.assert_driver_command(ProtocolEvent.GOTO_PRESET)
        self.assert_driver_command(ProtocolEvent.SET_PRESET)
        self.assert_driver_command(ProtocolEvent.STOP_FORWARD)
        self.assert_driver_command(ProtocolEvent.LAMP_ON)
        self.assert_driver_command(ProtocolEvent.LAMP_OFF)
        self.assert_driver_command(ProtocolEvent.LASER_1_ON)
        self.assert_driver_command(ProtocolEvent.LASER_2_ON)
        self.assert_driver_command(ProtocolEvent.LASER_1_OFF)
        self.assert_driver_command(ProtocolEvent.LASER_2_OFF)
        self.assert_driver_command(ProtocolEvent.LASER_BOTH_ON)
        self.assert_driver_command(ProtocolEvent.LASER_BOTH_OFF)

        # ####
        # # Test in streaming mode
        # ####
        # # Put us in streaming
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, delay=2)
        self.assert_acquire_status()

        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE, delay=2)
        self.assert_acquire_sample()
        self.assert_driver_command(ProtocolEvent.GOTO_PRESET)
        self.assert_driver_command(ProtocolEvent.SET_PRESET)
        self.assert_driver_command(ProtocolEvent.STOP_FORWARD)
        self.assert_driver_command(ProtocolEvent.LAMP_ON)
        self.assert_driver_command(ProtocolEvent.LAMP_OFF)
        self.assert_driver_command(ProtocolEvent.LASER_1_ON)
        self.assert_driver_command(ProtocolEvent.LASER_2_ON)
        self.assert_driver_command(ProtocolEvent.LASER_1_OFF)
        self.assert_driver_command(ProtocolEvent.LASER_2_OFF)
        self.assert_driver_command(ProtocolEvent.LASER_BOTH_ON)
        self.assert_driver_command(ProtocolEvent.LASER_BOTH_OFF)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

    def test_scheduled_acquire_status_command(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        self.assert_initialize_driver()
        self.assert_set(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY], '00:00:07')
        time.sleep(15)
        self.assert_acquire_status()

        self.assert_set(Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY], '00:00:00')
        self.assert_current_state(ProtocolState.COMMAND)

    def test_scheduled_acquire_status_autosample(self):
        """
        Verify the scheduled acquire status is triggered and functions as expected
        """

        self.assert_initialize_driver()
        self.assert_current_state(ProtocolState.COMMAND)
        self.assert_set(Parameter.SAMPLE_INTERVAL, '00:00:04')
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)
        time.sleep(10)
        self.assert_acquire_sample()
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_current_state(ProtocolState.COMMAND)
        self.assert_set(Parameter.SAMPLE_INTERVAL, '00:00:00')
        self.assert_current_state(ProtocolState.COMMAND)

    def test_scheduled_capture(self):
        """
        Verify the scheduled acquire status is triggered and functions as expected
        """

        self.assert_initialize_driver()
        self.assert_current_state(ProtocolState.COMMAND)
        self.assert_set(Parameter.AUTO_CAPTURE_DURATION, '00:00:02')
        self.assert_driver_command(InstrumentCmds.START_CAPTURE)
        time.sleep(1)
        self.assert_acquire_sample()
        time.sleep(2)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_acquire_status(self):
        """
        Verify the acquire_status command is functional
        """

        self.assert_initialize_driver()
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS)
        self.assert_acquire_status()

###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
