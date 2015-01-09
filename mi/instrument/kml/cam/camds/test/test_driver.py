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
import base64
import datetime as dt
from nose.plugins.attrib import attr
from mock import Mock
from mi.core.instrument.chunker import StringChunker

import time

from mi.core.log import get_logger

log = get_logger()

from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import InstrumentDriverPublicationTestCase

from mi.instrument.kml.cam.camds.driver import DataParticleType, CAMDS_DISK_STATUS_KEY, CAMDS_HEALTH_STATUS_KEY, \
                                        CAMDS_VIDEO, CAMDS_VIDEO_KEY

from mi.idk.unit_test import DriverTestMixin

from mi.idk.unit_test import DriverStartupConfigKey

from mi.instrument.kml.cam.camds.driver import Parameter, ParameterIndex
from mi.instrument.kml.cam.camds.driver import CAMDSPrompt, InstrumentDriver, CAMDSProtocol
from mi.instrument.kml.cam.camds.driver import ScheduledJob
from mi.instrument.kml.cam.camds.driver import InstrumentCmds, ProtocolState, ProtocolEvent, Capability

from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.port_agent_client import PortAgentClient
from mi.core.port_agent_process import PortAgentProcess

from mi.idk.comm_config import ConfigTypes
from mi.idk.unit_test import InstrumentDriverTestCase, LOCALHOST, ParameterTestConfigKey

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
            Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]: Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.DEFAULT_DATA],
            Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]: Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.DEFAULT_DATA],
            Parameter.CAMERA_GAIN[ParameterIndex.KEY]: Parameter.CAMERA_GAIN[ParameterIndex.DEFAULT_DATA],
            Parameter.CAMERA_MODE[ParameterIndex.KEY]: Parameter.CAMERA_MODE[ParameterIndex.DEFAULT_DATA],
            Parameter.COMPRESSION_RATIO[ParameterIndex.KEY]: Parameter.COMPRESSION_RATIO[ParameterIndex.DEFAULT_DATA],
            Parameter.FOCUS_POSITION[ParameterIndex.KEY]: Parameter.FOCUS_POSITION[ParameterIndex.DEFAULT_DATA],
            Parameter.FOCUS_SPEED[ParameterIndex.KEY]: Parameter.FOCUS_SPEED[ParameterIndex.DEFAULT_DATA],
            Parameter.FRAME_RATE[ParameterIndex.KEY]: Parameter.FRAME_RATE[ParameterIndex.DEFAULT_DATA],
            Parameter.IMAGE_RESOLUTION[ParameterIndex.KEY]: Parameter.IMAGE_RESOLUTION[ParameterIndex.DEFAULT_DATA],
            Parameter.IRIS_POSITION[ParameterIndex.KEY]: Parameter.IRIS_POSITION[ParameterIndex.DEFAULT_DATA],

            Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]: Parameter.LAMP_BRIGHTNESS[ParameterIndex.DEFAULT_DATA],
            Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY]: Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.DEFAULT_DATA],
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
            Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]: Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.DEFAULT_DATA],
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
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
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
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.FOCUS_POSITION[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.FOCUS_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.FOCUS_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
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
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.IRIS_POSITION[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.IRIS_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.LAMP_BRIGHTNESS[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.LAMP_BRIGHTNESS[ParameterIndex.D_DEFAULT]},
        Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.NETWORK_DRIVE_LOCATION[ParameterIndex.D_DEFAULT]},
        Parameter.NTP_SETTING[ParameterIndex.KEY]:
            {TYPE: str, READONLY: True, DA: True, STARTUP: False,
             DEFAULT: Parameter.NTP_SETTING[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.NTP_SETTING[ParameterIndex.D_DEFAULT]},
        Parameter.PAN_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.PAN_POSITION[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.PAN_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.PAN_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.PAN_SPEED[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.PAN_SPEED[ParameterIndex.D_DEFAULT]},
        Parameter.SHUTTER_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.SHUTTER_SPEED[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.SHUTTER_SPEED[ParameterIndex.D_DEFAULT]},
        Parameter.SOFT_END_STOPS[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.SOFT_END_STOPS[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.SOFT_END_STOPS[ParameterIndex.D_DEFAULT]},
        Parameter.TILT_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.TILT_POSITION[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.TILT_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.TILT_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.TILT_SPEED[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.TILT_SPEED[ParameterIndex.D_DEFAULT]},
        Parameter.WHEN_DISK_IS_FULL[ParameterIndex.KEY]:
            {TYPE: str, READONLY: True, DA: True, STARTUP: False,
             DEFAULT: Parameter.WHEN_DISK_IS_FULL[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.WHEN_DISK_IS_FULL[ParameterIndex.D_DEFAULT]},
        Parameter.ZOOM_POSITION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.ZOOM_POSITION[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.ZOOM_POSITION[ParameterIndex.D_DEFAULT]},
        Parameter.ZOOM_SPEED[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True,
             DEFAULT: Parameter.ZOOM_SPEED[ParameterIndex.D_DEFAULT],
                                    VALUE: Parameter.ZOOM_SPEED[ParameterIndex.D_DEFAULT]},

        # Engineering parameters
        Parameter.PRESET_NUMBER[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.PRESET_NUMBER[ParameterIndex.DEFAULT_DATA],
                                    VALUE: Parameter.PRESET_NUMBER[ParameterIndex.D_DEFAULT]},
        Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.DEFAULT_DATA],
                                    VALUE: Parameter.ACQUIRE_STATUS_INTERVAL[ParameterIndex.D_DEFAULT]},
        Parameter.VIDEO_FORWARDING[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.VIDEO_FORWARDING[ParameterIndex.DEFAULT_DATA],
                                    VALUE: Parameter.VIDEO_FORWARDING[ParameterIndex.D_DEFAULT]},
        Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.DEFAULT_DATA],
                                    VALUE: Parameter.VIDEO_FORWARDING_TIMEOUT[ParameterIndex.D_DEFAULT]},
        Parameter.SAMPLE_INTERVAL[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.SAMPLE_INTERVAL[ParameterIndex.DEFAULT_DATA],
                                    VALUE: Parameter.SAMPLE_INTERVAL[ParameterIndex.D_DEFAULT]},
        Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.KEY]:
            {TYPE: str, READONLY: False, DA: False, STARTUP: True,
             DEFAULT: Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.DEFAULT_DATA],
                                    VALUE: Parameter.AUTO_CAPTURE_DURATION[ParameterIndex.D_DEFAULT]}
    }

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.STOP_CAPTURE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        #Capability.START_CAPTURE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
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

    VIDEO_STREAM = (
        "\x7f\x7f\x68\x08\x00\x06\x12\x00\x4d\x00\x8e\x00\xb0\x03\x42\x05\xd4\x06\x00\x00\x32\x28\xc8\x41\x00\x35\x04\x64\x01\x00\x80\x0c" +
        "\xc0\x02\x01\x40\x11\x00\xd0\x07\x00\x01\x00\x00\x00\x00\x00\x00\x7d\x3d\xeb\x0f\x10\x0d\x01\x05\x32\x00\xc6\x00\x82\x00\x00\x06" +
        "\xff\x23\xe5\x09\x00\x00\xff\x00\x0c\x48\x00\x00\x14\x80\x00\x05\x00\x0d\x03\x0f\x15\x21\x02\x2e\x00\x00\x00\xf3\x05\x00\x00\x65" +
        "\x14\xcf\xed\x2f\xee\x23\x00\x02\x08" +
        "\x00\x00\x00\x00\x00\x00\x74\xa9\x58\x4f\x4f\x00\x00\x00\x00\x00\x00\x00\x04\x90\x51\xf2\xff\xff\x00\x00\x00\x00\x00\x14\x0d\x03" +
        "\x0f\x15\x21\x02\x2e\x00\x01\x13\x00\xf4\xff\xb3\x00\x4d\x00\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80\x00\x80" +
        "\x00\x80\x00\x80\x00\x80\x00\x80\x00" +
        "\x02\x4d\x59\x57\x5d\x0f\x0d\x15\x0a\x07\x04\x08\x09\x07\x04\x10\x04\x07\x08\x0b\x09\x05\x09\x0b\x06\x07\x05\x0b\x09\x07\x0b\x05" +
        "\x06\x05\x08\x07\x09\x09\x0d\x03\x06\x06\x03\x08\x0a\x0a\x0b\x07\x07\x05\x0a\x06\x09\x04\x04\x02\x06\x06\x07\x05\x06\x05\x02\x03" +
        "\x0a\x04\x04\x0c\x07\x07\x07\x0d\x0c\x07\x05\x02\x0a\x0b\x09\x04\x07\x06\x02\x06\x0b\x0a\x0e\x06\x0a\x03\x07\x02\x07\x04\x02\x06" +
        "\x09\x04\x0a\x08\x04\x04\x04\x06\x0b" +
        "\x05\x03\x0b\x04\x02\x06\x0a\x06\x04\x05\x02\x07\x05\x0a\x0c\x05\x07\x07\x07\x0e\x05\x06\x0d\x06\x07\x09\x04\x02\x04\x08\x07\x09" +
        "\x06\x09\x06\x0b\x07\x05\x07\x11\x02\x07\x07\x03\x04\x04\x08\x0a\x03\x04\x07\x09\x09\x08\x06\x03\x02\x07\x09\x07\x04\x07\x07\x06" +
        "\x04\x09\x04\x06\x03\x07\x06\x0a\x04\x04\x03\x0d\x06\x0d\x0e\x09\x05\x06\x07\x04\x03\x04\x04\x08\x02\x09\x04\x0d\x04\x04\x09\x03" +
        "\x02\x07\x07\x0a\x03\x04\x04\x01\x06" +
        "\x09\x04\x0b\x0a\x0a\x06\x09\x07\x09\x07\x06\x05\x08\x09\x0c\x02\x0a\x03\x02\x07\x04\x08\x07\x05\x06\x09\x09\x06\x06\x06\x0a\x04" +
        "\x06\x05\x0c\x06\x02\x05\x0c\x04\x08\x04\x07\x03\x09\x09\x08\x06\x06\x04\x06\x05\x02\x04\x0b\x04\x0b\x0b\x0e\x03\x05\x09\x0c\x05" +
        "\x03\x02\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x61\x5d\x71\x63\x2f\x2f\x38\x33" +
        "\x29\x2a\x30\x2e\x28\x2a\x30\x2e\x28" +
        "\x29\x30\x2e\x28\x29\x2f\x2e\x29\x29\x2f\x2e\x28\x29\x2f\x2e\x28\x2a\x2f\x2e\x28\x2a\x2f\x2e\x28\x29\x2e\x2d\x28\x29\x30\x2e\x28" +
        "\x29\x30\x2e\x28\x2a\x2f\x2e\x28\x2a\x30\x2e\x28\x2a\x30\x2e\x28\x2a\x2f\x2e\x28\x29\x2f\x2e\x28\x29\x2f\x2e\x27\x29\x2f\x2e\x28" +
        "\x29\x2f\x2e\x28\x2a\x2f\x2d\x28\x29\x2f\x2e\x28\x2a\x2f\x2d\x29\x2a\x2f\x2e\x28\x2a\x30\x2e\x28\x2a\x2f\x2e\x28\x2a\x2f\x2e\x28" +
        "\x29\x2f\x2e\x27\x29\x30\x2e\x28\x2a" +
        "\x2f\x2f\x28\x2a\x2f\x2e\x28\x29\x30\x2e\x28\x29\x30\x2e\x29\x29\x2f\x2e\x27\x29\x2f\x2d\x28\x29\x30\x2e\x28\x29\x2f\x2e\x28\x29" +
        "\x30\x2d\x28\x29\x2e\x2d\x28\x29\x2f\x2e\x28\x2a\x30\x2f\x28\x29\x30\x2d\x28\x29\x2f\x2d\x28\x29\x2f\x2e\x28\x2a\x2f\x2e\x28\x29" +
        "\x2f\x2d\x28\x29\x2f\x2d\x28\x29\x2f\x2e\x28\x29\x30\x2e\x28\x29\x2f\x2e\x28\x29\x2e\x2e\x28\x29\x2f\x2e\x29\x29\x30\x2e\x28\x29" +
        "\x30\x2e\x28\x2a\x30\x2e\x28\x29\x2f" +
        "\x2d\x28\x29\x2f\x2d\x28\x2a\x2f\x2e\x28\x29\x2f\x2d\x28\x29\x2f\x2e\x28\x29\x2f\x2e\x28\x2a\x2f\x2e\x28\x29\x2e\x2d\x29\x29\x2f" +
        "\x2e\x28\x29\x2e\x2d\x28\x29\x2f\x2e\x28\x2a\x2f\x2e\x28\x29\x2f\x2e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x04\x64\x64\x64\x64\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x7d\xf2\x2f\x20")

    VIDEO_STREAM_LEN =len(VIDEO_STREAM)



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
        CAMDS_HEALTH_STATUS_KEY.humidity: {'type': int, 'value': 2},
        CAMDS_HEALTH_STATUS_KEY.temp: {'type': int, 'value': 1},
        CAMDS_HEALTH_STATUS_KEY.error: {'type': int, 'value': 3}
    }

    _video_dict = {
        CAMDS_VIDEO_KEY.CAMDS_VIDEO_BINARY: {'type': unicode, 'value': base64.b32encode(VIDEO_STREAM)},
    }
    #_disk_data = '<\x0B:\x06:GC\x01\x02\x03\x04\x05\x06>'
    _disk_data = '<' + size_B + ':' + size_6 + ':' + 'GC' + size_1 + size_2 + \
                  size_3 + size_4 + size_5 + size_6 + size_7+ '>'

    _disk_status_dict = {
        CAMDS_DISK_STATUS_KEY.disk_remaining: {'type': int, 'value':100},
        CAMDS_DISK_STATUS_KEY.image_on_disk: {'type': int, 'value': 3},
        CAMDS_DISK_STATUS_KEY.image_remaining: {'type': int, 'value':1029 },
        CAMDS_DISK_STATUS_KEY.size: {'type': int, 'value': 1543},

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

    def assert_initialize_driver(self, driver, initial_protocol_state=DriverProtocolState.COMMAND):
        """
        OVERWRITE
        Initialize an instrument driver with a mock port agent.  This will allow us to test the
        got data method.  Will the instrument, using test mode, through it's connection state
        machine.  End result, the driver will be in test mode and the connection state will be
        connected.
        @param driver: Instrument driver instance.
        @param initial_protocol_state: the state to force the driver too
        """
        # Put the driver into test mode
        driver.set_test_mode(True)

        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.UNCONFIGURED)

        # Now configure the driver with the mock_port_agent, verifying
        # that the driver transitions to that state
        config = {'Driver': {'mock_port_agent': Mock(spec=PortAgentClient)},
                  'Stream': {'mock_port_agent': Mock(spec=PortAgentClient)}}
        driver.configure(config=config['Driver'])

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

    def test_video_stream(self):
        port_agent_package = {
            'type': 7,
            'length':self.VIDEO_STREAM_LEN ,
            'checksum': 7,
            'raw': self.VIDEO_STREAM
        }
        video_stream = self.generate_raw_stream(port_agent_package)
        self.assert_data_particle_header(video_stream, DataParticleType.CAMDS_VIDEO)

    def generate_raw_stream(self, port_agent_packet):
        """
        Publish raw data
        @param: port_agent_packet port agent packet containing raw
        """
        particle = CAMDS_VIDEO(port_agent_packet,
                                           port_timestamp=round(2.675, 2))

        parsed_sample = particle.generate()
        return parsed_sample

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

        #Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(driver_capabilities, protocol._filter_capabilities(test_capabilities))


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

    def create_multi_comm_config(self, comm_config):
        result = {}
        for name, config in comm_config.configs.items():
            if config.method() == ConfigTypes.ETHERNET:
                result[name] = self.create_ethernet_comm_config(config)
            elif config.method() == ConfigTypes.SERIAL:
                result[name] = self.create_serial_comm_config(config)
            elif config.method() == ConfigTypes.RSN:
                result[name] = self.create_rsn_comm_config(config)
        return result

    def port_agent_config(self):
        """
        return the port agent configuration
        """
        comm_config = self.get_comm_config()
        method = comm_config.method()
        config = {}

        if method == ConfigTypes.SERIAL:
            config = self.create_serial_comm_config(comm_config)
        elif method == ConfigTypes.ETHERNET:
            config = self.create_ethernet_comm_config(comm_config)
        elif method == ConfigTypes.MULTI:
            config = self.create_multi_comm_config(comm_config)

        config['instrument_type'] = comm_config.method()

        if comm_config.sniffer_prefix:
            config['telnet_sniffer_prefix'] = comm_config.sniffer_prefix
        if comm_config.sniffer_suffix:
            config['telnet_sniffer_suffix'] = comm_config.sniffer_suffix

        return config

    def init_port_agent(self):
        """
        @brief Launch the driver process and driver client.  This is used in the
        integration and qualification tests.  The port agent abstracts the physical
        interface with the instrument.
        @return return the pid to the logger process
        """
        if self.port_agents:
            log.error("Port agent already initialized")
            return

        config = self.port_agent_config()
        log.debug("port agent config: %s", config)

        port_agents = {}

        if config['instrument_type'] != ConfigTypes.MULTI:
            config = {'only one port agent here!': config}
        for name, each in config.items():
            if type(each) != dict:
                continue
            port_agent_host = each.get('device_addr')
            if port_agent_host is not None:
                port_agent = PortAgentProcess.launch_process(each, timeout=60, test_mode=True)
                port = port_agent.get_data_port()
                pid = port_agent.get_pid()
                if port_agent_host == LOCALHOST:
                    log.info('Started port agent pid %s listening at port %s' % (pid, port))
                else:
                    log.info("Connecting to port agent on host: %s, port: %s", port_agent_host, port)
                port_agents[name] = port_agent

        self.addCleanup(self.stop_port_agent)
        self.port_agents = port_agents

    def stop_port_agent(self):
        """
        Stop the port agent.
        """
        if self.port_agents:
            log.debug("found port agents, now stop them")
            for agent in self.port_agents.values():
                agent.stop()
        self.port_agents = {}

    def port_agent_comm_config(self):
        config = {}
        for name, each in self.port_agents.items():
            port = each.get_data_port()
            cmd_port = each.get_command_port()

            config[name] = {
                'addr': each._config['port_agent_addr'],
                'port': port,
                'cmd_port': cmd_port
            }
        return config


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

    #Set bulk params and test auto sampling
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

    # @unittest.skip('It takes many hours for this test')
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
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, delay = 2)
        self.assert_acquire_status ()

        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE, delay = 2)
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
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, delay =2)
        self.assert_acquire_status ()

        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE, delay = 2)
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

    # @unittest.skip('It takes many hours for this test')
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

    #@unittest.skip('It takes time')
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
@attr('QUAL', group='mi')
class DriverQualificationTest(InstrumentDriverQualificationTestCase, CAMDSMixin):
    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)

    def init_port_agent(self):
        """
        @brief Launch the driver process and driver client.  This is used in the
        integration and qualification tests.  The port agent abstracts the physical
        interface with the instrument.
        @return return the pid to the logger process
        """
        if self.port_agent:
            return

        config = self.port_agent_config()
        port_agents = {}

        if config['instrument_type'] != ConfigTypes.MULTI:
            config = {'only one port agent here!': config}
        for name, each in config.items():
            if type(each) != dict:
                continue
            port_agent_host = each.get('device_addr')
            if port_agent_host is not None:
                port_agent = PortAgentProcess.launch_process(each, timeout=60, test_mode=True)

                port = port_agent.get_data_port()
                pid = port_agent.get_pid()

                if port_agent_host == LOCALHOST:
                    log.info('Started port agent pid %s listening at port %s' % (pid, port))
                else:
                    log.info("Connecting to port agent on host: %s, port: %s", port_agent_host, port)
                port_agents[name] = port_agent

        self.addCleanup(self.stop_port_agent)
        self.port_agents = port_agents

    def stop_port_agent(self):
        """
        Stop the port agent.
        """
        if self.port_agents:
            for agent in self.port_agents.values():
                agent.stop()
        self.port_agents = {}

    def port_agent_comm_config(self):
        config = {}
        for name, each in self.port_agents.items():
            port = each.get_data_port()
            cmd_port = each.get_command_port()

            config[name] = {
                'addr': each._config['port_agent_addr'],
                'port': port,
                'cmd_port': cmd_port
            }
        return config

    def init_instrument_agent_client(self):

        # Driver config
        driver_config = {
            'dvr_mod': self.test_config.driver_module,
            'dvr_cls': self.test_config.driver_class,
            'workdir': self.test_config.working_dir,
            'process_type': (self.test_config.driver_process_type,),
            'comms_config': self.port_agent_comm_config(),
            'startup_config': self.test_config.driver_startup_config
        }

        # Create agent config.
        agent_config = {
            'driver_config': driver_config,
            'stream_config': self.data_subscribers.stream_config,
            'agent': {'resource_id': self.test_config.agent_resource_id},
            'test_mode': True  # Enable a poison pill. If the spawning process dies
            ## shutdown the daemon process.
        }

        log.debug("Agent Config: %s", agent_config)

        # Start instrument agent client.
        self.instrument_agent_manager.start_client(
            name=self.test_config.agent_name,
            module=self.test_config.agent_module,
            cls=self.test_config.agent_class,
            config=agent_config,
            resource_id=self.test_config.agent_resource_id,
            deploy_file=self.test_config.container_deploy_file
        )

        self.instrument_agent_client = self.instrument_agent_manager.instrument_agent_client

    # Direct access to master
    def test_direct_access_telnet_mode_master(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports direct access
          to the physical instrument. (telnet mode)
        """

        self.assert_enter_command_mode()
        self.assert_set_parameter(Parameter.CAMERA_GAIN, 9)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)

        self.tcp_client.send_data("<\x03:GS:\x08>")

        self.tcp_client.expect(TeledynePrompt.COMMAND)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_enter_command_mode()
        # Direct access is true, it should be set before
        self.assert_get_parameter(Parameter.CAMERA_GAIN, 9)

    # Direct access to slave
    def test_direct_access_telnet_mode_slave(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports direct
          access to the physical instrument. (telnet mode)
        """

        self.assert_enter_command_mode()
        self.assert_set_parameter(Parameter.CAMERA_MODE, 9)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)

        self.tcp_client.send_data("<\x03:SV:\x0A>")

        self.tcp_client.expect(TeledynePrompt.COMMAND)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_enter_command_mode()
        # Direct access is true, it should be set before
        self.assert_get_parameter(Parameter.CAMERA_MODE, 9)


###############################################################################
#                             PUBLICATION TESTS                               #
# Device specific publication tests are for                                   #
# testing device specific capabilities                                        #
###############################################################################
@attr('PUB', group='mi')
class DriverPublicationTest(InstrumentDriverPublicationTestCase):
    pass
