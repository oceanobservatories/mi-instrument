"""
@package mi.instrument.teledyne.workhorse.vadcp.test
@file marine-integrations/mi/instrument/teledyne/workhorse/vadcp/test/test_driver.py
@author Sung Ahn
@brief Test Driver for the VADCP
Release notes:

"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import functools
import copy
import datetime as dt
import time

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger
log = get_logger()

# core
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.instrument.port_agent_client import PortAgentClient
from mi.core.port_agent_process import PortAgentProcess
from mi.core.exceptions import ResourceError
# idk
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import DriverStartupConfigKey
from mi.idk.comm_config import ConfigTypes
from mi.idk.unit_test import InstrumentDriverTestCase, LOCALHOST, ParameterTestConfigKey
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
# workhorse
from mi.instrument.teledyne.workhorse.driver import WorkhorseScheduledJob
from mi.instrument.teledyne.workhorse.driver import WorkhorseCapability
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocolState
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocolEvent
from mi.instrument.teledyne.workhorse.driver import WorkhorsePrompt
from mi.instrument.teledyne.workhorse.driver import WorkhorseParameter
from mi.instrument.teledyne.workhorse.driver import WorkhorseInstrumentCmds
from mi.instrument.teledyne.workhorse.driver import NEWLINE
from mi.instrument.teledyne.workhorse.test.test_driver import WorkhorseDriverUnitTest
from mi.instrument.teledyne.workhorse.test.test_driver import WorkhorseDriverIntegrationTest
from mi.instrument.teledyne.workhorse.test.test_driver import WorkhorseDriverQualificationTest
from mi.instrument.teledyne.workhorse.test.test_data import RSN_SAMPLE_RAW_DATA, VADCP_SLAVE_PS0_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import RSN_CALIBRATION_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import RSN_PS0_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import PT2_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import PT4_RAW_DATA
# particles
from mi.instrument.teledyne.workhorse.particles import VADCPDataParticleType
from mi.instrument.teledyne.workhorse.particles import AdcpCompassCalibrationKey
from mi.instrument.teledyne.workhorse.particles import AdcpSystemConfigurationKey
from mi.instrument.teledyne.workhorse.particles import AdcpPd0ParsedKey
from mi.instrument.teledyne.workhorse.particles import AdcpAncillarySystemDataKey
from mi.instrument.teledyne.workhorse.particles import AdcpTransmitPathKey
# vadcp
from mi.instrument.teledyne.workhorse.vadcp.driver import InstrumentDriver, Protocol, SlaveProtocol


# ##
# Driver parameters for tests
###

InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.teledyne.workhorse.vadcp.driver',
    driver_class="InstrumentDriver",
    instrument_agent_resource_id='HTWZMW',
    instrument_agent_preload_id='IA7',
    instrument_agent_name='teledyne_workhorse_monitor VADCP',
    instrument_agent_packet_config=VADCPDataParticleType,

    driver_startup_config={
        DriverStartupConfigKey.PARAMETERS: {
            WorkhorseParameter.SERIAL_FLOW_CONTROL: '11110',
            WorkhorseParameter.BANNER: False,
            WorkhorseParameter.INSTRUMENT_ID: 0,
            WorkhorseParameter.SLEEP_ENABLE: 0,
            WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: True,
            WorkhorseParameter.POLLED_MODE: False,
            WorkhorseParameter.XMIT_POWER: 255,
            WorkhorseParameter.SPEED_OF_SOUND: 1485,
            WorkhorseParameter.PITCH: 0,
            WorkhorseParameter.ROLL: 0,
            WorkhorseParameter.SALINITY: 35,
            WorkhorseParameter.COORDINATE_TRANSFORMATION: '00111',
            WorkhorseParameter.TIME_PER_ENSEMBLE: '00:00:00.00',
            WorkhorseParameter.TIME_PER_PING: '00:01.00',
            WorkhorseParameter.FALSE_TARGET_THRESHOLD: '050,001',
            WorkhorseParameter.BANDWIDTH_CONTROL: 0,
            WorkhorseParameter.CORRELATION_THRESHOLD: 64,
            WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: '111100000',
            WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: 2000,
            WorkhorseParameter.BLANK_AFTER_TRANSMIT: 88,
            WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: 0,
            WorkhorseParameter.RECEIVER_GAIN_SELECT: 1,
            WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: 22,
            WorkhorseParameter.PINGS_PER_ENSEMBLE: 1,
            WorkhorseParameter.DEPTH_CELL_SIZE: 100,
            WorkhorseParameter.TRANSMIT_LENGTH: 0,
            WorkhorseParameter.PING_WEIGHT: 0,
            WorkhorseParameter.AMBIGUITY_VELOCITY: 175,
            WorkhorseParameter.LATENCY_TRIGGER: 0,
            WorkhorseParameter.HEADING_ALIGNMENT: +00000,
            WorkhorseParameter.HEADING_BIAS: +00000,
            WorkhorseParameter.TRANSDUCER_DEPTH: 2000,
            WorkhorseParameter.DATA_STREAM_SELECTION: 0,
            WorkhorseParameter.ENSEMBLE_PER_BURST: 0,
            WorkhorseParameter.SAMPLE_AMBIENT_SOUND: 0,
            WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: '00:00:00',

            WorkhorseParameter.SYNC_PING_ENSEMBLE: '001',
            WorkhorseParameter.RDS3_MODE_SEL: 1,
            WorkhorseParameter.SYNCH_DELAY: 100,

            # Slave
            WorkhorseParameter.SERIAL_FLOW_CONTROL + '_5th': '11110',
            WorkhorseParameter.BANNER + '_5th': False,
            WorkhorseParameter.INSTRUMENT_ID + '_5th': 0,
            WorkhorseParameter.SLEEP_ENABLE + '_5th': 0,
            WorkhorseParameter.SAVE_NVRAM_TO_RECORDER + '_5th': True,
            WorkhorseParameter.POLLED_MODE + '_5th': False,
            WorkhorseParameter.XMIT_POWER + '_5th': 255,
            WorkhorseParameter.SPEED_OF_SOUND + '_5th': 1485,
            WorkhorseParameter.PITCH + '_5th': 0,
            WorkhorseParameter.ROLL + '_5th': 0,
            WorkhorseParameter.SALINITY + '_5th': 35,
            WorkhorseParameter.COORDINATE_TRANSFORMATION + '_5th': '00111',
            WorkhorseParameter.TIME_PER_ENSEMBLE + '_5th': '00:00:00.00',
            WorkhorseParameter.FALSE_TARGET_THRESHOLD + '_5th': '050,001',
            WorkhorseParameter.BANDWIDTH_CONTROL + '_5th': 0,
            WorkhorseParameter.CORRELATION_THRESHOLD + '_5th': 64,
            WorkhorseParameter.SERIAL_OUT_FW_SWITCHES + '_5th': '111100000',
            WorkhorseParameter.ERROR_VELOCITY_THRESHOLD + '_5th': 2000,
            WorkhorseParameter.BLANK_AFTER_TRANSMIT + '_5th': 83,
            WorkhorseParameter.CLIP_DATA_PAST_BOTTOM + '_5th': 0,
            WorkhorseParameter.RECEIVER_GAIN_SELECT + '_5th': 1,
            WorkhorseParameter.NUMBER_OF_DEPTH_CELLS + '_5th': 22,
            WorkhorseParameter.PINGS_PER_ENSEMBLE + '_5th': 1,
            WorkhorseParameter.DEPTH_CELL_SIZE + '_5th': 94,
            WorkhorseParameter.TRANSMIT_LENGTH + '_5th': 0,
            WorkhorseParameter.PING_WEIGHT + '_5th': 0,
            WorkhorseParameter.AMBIGUITY_VELOCITY + '_5th': 175,
            WorkhorseParameter.LATENCY_TRIGGER + '_5th': 0,
            WorkhorseParameter.HEADING_ALIGNMENT + '_5th': +00000,
            WorkhorseParameter.HEADING_BIAS + '_5th': +00000,
            WorkhorseParameter.TRANSDUCER_DEPTH + '_5th': 2000,
            WorkhorseParameter.DATA_STREAM_SELECTION + '_5th': 0,
            WorkhorseParameter.ENSEMBLE_PER_BURST + '_5th': 0,
            WorkhorseParameter.SAMPLE_AMBIENT_SOUND + '_5th': 0,
            WorkhorseParameter.BUFFERED_OUTPUT_PERIOD + '_5th': '00:00:00',

            WorkhorseParameter.SYNC_PING_ENSEMBLE + '_5th': '001',
            WorkhorseParameter.RDS3_MODE_SEL + '_5th': 2,
            WorkhorseParameter.SYNCH_DELAY + '_5th': 0,

            WorkhorseParameter.CLOCK_SYNCH_INTERVAL: '00:00:00',
            WorkhorseParameter.GET_STATUS_INTERVAL: '00:00:00',
        },
        DriverStartupConfigKey.SCHEDULER: {
            WorkhorseScheduledJob.GET_CALIBRATION: {},
            WorkhorseScheduledJob.GET_CONFIGURATION: {},
            WorkhorseScheduledJob.CLOCK_SYNC: {}
        }
    }
)


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
class VADCPMixin(DriverTestMixin):
    """
    Mixin class used for storing data particle constance
    and common data assertion methods.
    """
    _driver_class = InstrumentDriver

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
    _vadcp_driver_parameters = {
        WorkhorseParameter.SERIAL_DATA_OUT: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '000 000 000',
                                             VALUE: '000 000 000'},
        WorkhorseParameter.SERIAL_FLOW_CONTROL: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '11110',
                                                 VALUE: '11110'},
        WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True,
                                                    VALUE: True},
        WorkhorseParameter.TIME: {TYPE: str, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: {TYPE: str, READONLY: True, DA: True, STARTUP: True,
                                                    DEFAULT: '111100000',
                                                    VALUE: '111100000'},
        WorkhorseParameter.BANNER: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        WorkhorseParameter.INSTRUMENT_ID: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.SLEEP_ENABLE: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.POLLED_MODE: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False,
                                         VALUE: False},
        WorkhorseParameter.XMIT_POWER: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 255, VALUE: 255},
        WorkhorseParameter.SPEED_OF_SOUND: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1485,
                                            VALUE: 1485},
        WorkhorseParameter.PITCH: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.ROLL: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.SALINITY: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 35, VALUE: 35},
        WorkhorseParameter.COORDINATE_TRANSFORMATION: {TYPE: str, READONLY: True, DA: True, STARTUP: True,
                                                       DEFAULT: '00111',
                                                       VALUE: '00111'},
        WorkhorseParameter.SENSOR_SOURCE: {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: "1111101",
                                           VALUE: "1111101"},
        WorkhorseParameter.TIME_PER_ENSEMBLE: {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                               VALUE: '00:00:00.00'},
        WorkhorseParameter.TIME_OF_FIRST_PING: {TYPE: str, READONLY: True, DA: False, STARTUP: False, DEFAULT: False},

        WorkhorseParameter.TIME_PER_PING: {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: '00:01.00',
                                           VALUE: '00:01.00'},
        WorkhorseParameter.FALSE_TARGET_THRESHOLD: {TYPE: str, READONLY: False, DA: True, STARTUP: True,
                                                    DEFAULT: '050,001',
                                                    VALUE: '050,001'},
        WorkhorseParameter.BANDWIDTH_CONTROL: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                               VALUE: 0},
        WorkhorseParameter.CORRELATION_THRESHOLD: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 64,
                                                   VALUE: 64},
        WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                                      DEFAULT: 2000,
                                                      VALUE: 2000},
        WorkhorseParameter.BLANK_AFTER_TRANSMIT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 88,
                                                  VALUE: 88},
        WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: {TYPE: bool, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                                   VALUE: 0},
        WorkhorseParameter.RECEIVER_GAIN_SELECT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1,
                                                  VALUE: 1},
        WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 220,
                                                   VALUE: 220},
        WorkhorseParameter.PINGS_PER_ENSEMBLE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1,
                                                VALUE: 1},
        WorkhorseParameter.DEPTH_CELL_SIZE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 100,
                                             VALUE: 100},
        WorkhorseParameter.TRANSMIT_LENGTH: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                             VALUE: 0},
        WorkhorseParameter.PING_WEIGHT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: False, VALUE: 0},
        WorkhorseParameter.AMBIGUITY_VELOCITY: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 175,
                                                VALUE: 175},
        WorkhorseParameter.LATENCY_TRIGGER: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.HEADING_ALIGNMENT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: +00000,
                                               VALUE: +00000},
        WorkhorseParameter.HEADING_BIAS: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: +00000,
                                          VALUE: +00000},
        WorkhorseParameter.TRANSDUCER_DEPTH: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 2000,
                                              VALUE: 2000},
        WorkhorseParameter.DATA_STREAM_SELECTION: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                   VALUE: 0},
        WorkhorseParameter.ENSEMBLE_PER_BURST: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                VALUE: 0},
        WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: {TYPE: str, READONLY: True, DA: True, STARTUP: True,
                                                    DEFAULT: '00:00:00',
                                                    VALUE: '00:00:00'},
        WorkhorseParameter.SAMPLE_AMBIENT_SOUND: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                  VALUE: 0},
        WorkhorseParameter.SYNC_PING_ENSEMBLE: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '001',
                                                VALUE: '001'},
        WorkhorseParameter.RDS3_MODE_SEL: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 1, VALUE: 1},
        WorkhorseParameter.SLAVE_TIMEOUT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.SYNCH_DELAY: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 100, VALUE: 100},

        WorkhorseParameter.CLOCK_SYNCH_INTERVAL: {TYPE: str, READONLY: False, DA: False, STARTUP: True,
                                                  DEFAULT: '00:00:00',
                                                  VALUE: '00:00:00'},
        WorkhorseParameter.GET_STATUS_INTERVAL: {TYPE: str, READONLY: False, DA: False, STARTUP: True,
                                                 DEFAULT: '00:00:00',
                                                 VALUE: '00:00:00'}
    }

    _driver_parameters_slave = {
        WorkhorseParameter.SERIAL_DATA_OUT: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '000 000 000',
                                             VALUE: '000 000 000'},
        WorkhorseParameter.SERIAL_FLOW_CONTROL: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '11110',
                                                 VALUE: '11110'},
        WorkhorseParameter.SAVE_NVRAM_TO_RECORDER: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True,
                                                    VALUE: True},
        WorkhorseParameter.TIME: {TYPE: str, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        WorkhorseParameter.SERIAL_OUT_FW_SWITCHES: {TYPE: str, READONLY: True, DA: True, STARTUP: True,
                                                    DEFAULT: '111100000',
                                                    VALUE: '111100000'},
        WorkhorseParameter.BANNER: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        WorkhorseParameter.INSTRUMENT_ID: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.SLEEP_ENABLE: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.POLLED_MODE: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False,
                                         VALUE: False},
        WorkhorseParameter.XMIT_POWER: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 255, VALUE: 255},
        WorkhorseParameter.SPEED_OF_SOUND: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1485,
                                            VALUE: 1485},
        WorkhorseParameter.PITCH: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.ROLL: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.SALINITY: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 35, VALUE: 35},
        WorkhorseParameter.COORDINATE_TRANSFORMATION: {TYPE: str, READONLY: True, DA: True, STARTUP: True,
                                                       DEFAULT: '00111',
                                                       VALUE: '00111'},
        WorkhorseParameter.SENSOR_SOURCE: {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: "1111101",
                                           VALUE: "1111101"},
        WorkhorseParameter.TIME_PER_ENSEMBLE: {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                               VALUE: '00:00:00.00'},
        WorkhorseParameter.TIME_OF_FIRST_PING: {TYPE: str, READONLY: True, DA: False, STARTUP: False, DEFAULT: False},
        WorkhorseParameter.TIME_PER_PING: {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: '00:00.00',
                                           VALUE: '00:00.00'},
        WorkhorseParameter.FALSE_TARGET_THRESHOLD: {TYPE: str, READONLY: False, DA: True, STARTUP: True,
                                                    DEFAULT: '050,001',
                                                    VALUE: '050,001'},
        WorkhorseParameter.BANDWIDTH_CONTROL: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                               VALUE: 0},
        WorkhorseParameter.CORRELATION_THRESHOLD: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 64,
                                                   VALUE: 64},
        WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                                      DEFAULT: 2000,
                                                      VALUE: 2000},
        WorkhorseParameter.BLANK_AFTER_TRANSMIT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 83,
                                                  VALUE: 83},
        WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: {TYPE: bool, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                                   VALUE: 0},
        WorkhorseParameter.RECEIVER_GAIN_SELECT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1,
                                                  VALUE: 1},
        WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 220,
                                                   VALUE: 220},
        WorkhorseParameter.PINGS_PER_ENSEMBLE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1,
                                                VALUE: 1},
        WorkhorseParameter.DEPTH_CELL_SIZE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 94,
                                             VALUE: 94},
        WorkhorseParameter.TRANSMIT_LENGTH: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                             VALUE: 0},
        WorkhorseParameter.PING_WEIGHT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: False, VALUE: 0},
        WorkhorseParameter.AMBIGUITY_VELOCITY: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 175,
                                                VALUE: 175},
        WorkhorseParameter.LATENCY_TRIGGER: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.HEADING_ALIGNMENT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: +00000,
                                               VALUE: +00000},
        WorkhorseParameter.HEADING_BIAS: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: +00000,
                                          VALUE: +00000},
        WorkhorseParameter.TRANSDUCER_DEPTH: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 2000,
                                              VALUE: 2000},
        WorkhorseParameter.DATA_STREAM_SELECTION: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                   VALUE: 0},
        WorkhorseParameter.ENSEMBLE_PER_BURST: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                VALUE: 0},
        WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: {TYPE: str, READONLY: True, DA: True, STARTUP: True,
                                                    DEFAULT: '00:00:00',
                                                    VALUE: '00:00:00'},
        WorkhorseParameter.SAMPLE_AMBIENT_SOUND: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                  VALUE: 0},
        WorkhorseParameter.SYNC_PING_ENSEMBLE: {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '001',
                                                VALUE: '001'},
        WorkhorseParameter.RDS3_MODE_SEL: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 2, VALUE: 2},
        WorkhorseParameter.SLAVE_TIMEOUT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        WorkhorseParameter.SYNCH_DELAY: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 100, VALUE: 100},
    }

    for key in _driver_parameters_slave.keys():
        _vadcp_driver_parameters[key + '_5th'] = _driver_parameters_slave[key]

    _driver_capabilities = {
        # capabilities defined in the IOS
        WorkhorseCapability.START_AUTOSAMPLE: {
            STATES: [WorkhorseProtocolState.COMMAND, WorkhorseProtocolState.AUTOSAMPLE]},
        WorkhorseCapability.STOP_AUTOSAMPLE: {
            STATES: [WorkhorseProtocolState.COMMAND, WorkhorseProtocolState.AUTOSAMPLE]},
        WorkhorseCapability.CLOCK_SYNC: {STATES: [WorkhorseProtocolState.COMMAND]},
        WorkhorseCapability.RUN_TEST: {STATES: [WorkhorseProtocolState.COMMAND]},
        WorkhorseCapability.ACQUIRE_STATUS: {STATES: [WorkhorseProtocolState.COMMAND]},
    }

    _calibration_data_parameters = {
        AdcpCompassCalibrationKey.FLUXGATE_CALIBRATION_TIMESTAMP: {'type': float, 'value': 1347639932.0},
        AdcpCompassCalibrationKey.S_INVERSE_BX: {'type': list, 'value': [0.39218, 0.3966, -0.031681, 0.0064332]},
        AdcpCompassCalibrationKey.S_INVERSE_BY: {'type': list, 'value': [-0.02432, -0.010376, -0.0022428, -0.60628]},
        AdcpCompassCalibrationKey.S_INVERSE_BZ: {'type': list, 'value': [0.22453, -0.21972, -0.2799, -0.0024339]},
        AdcpCompassCalibrationKey.S_INVERSE_ERR: {'type': list, 'value': [0.46514, -0.40455, 0.69083, -0.014291]},
        AdcpCompassCalibrationKey.COIL_OFFSET: {'type': list, 'value': [34233.0, 34449.0, 34389.0, 34698.0]},
        AdcpCompassCalibrationKey.ELECTRICAL_NULL: {'type': float, 'value': 34285.0},
        AdcpCompassCalibrationKey.TILT_CALIBRATION_TIMESTAMP: {'type': float, 'value': 1347639285.0},
        AdcpCompassCalibrationKey.CALIBRATION_TEMP: {'type': float, 'value': 24.4},
        AdcpCompassCalibrationKey.ROLL_UP_DOWN: {'type': list,
                                                 'value': [7.4612e-07, -3.1727e-05, -3.0054e-07, 3.219e-05]},
        AdcpCompassCalibrationKey.PITCH_UP_DOWN: {'type': list,
                                                  'value': [-3.1639e-05, -6.3505e-07, -3.1965e-05, -1.4881e-07]},
        AdcpCompassCalibrationKey.OFFSET_UP_DOWN: {'type': list, 'value': [32808.0, 32568.0, 32279.0, 33047.0]},
        AdcpCompassCalibrationKey.TILT_NULL: {'type': float, 'value': 33500.0}
    }

    _system_configuration_data_parameters_VADCP = {
        AdcpSystemConfigurationKey.SERIAL_NUMBER: {'type': unicode, 'value': "61247"},
        AdcpSystemConfigurationKey.TRANSDUCER_FREQUENCY: {'type': int, 'value': 614400},
        AdcpSystemConfigurationKey.CONFIGURATION: {'type': unicode, 'value': "4 BEAM, JANUS"},
        AdcpSystemConfigurationKey.MATCH_LAYER: {'type': unicode, 'value': "10"},
        AdcpSystemConfigurationKey.BEAM_ANGLE: {'type': int, 'value': 20},
        AdcpSystemConfigurationKey.BEAM_PATTERN: {'type': unicode, 'value': "CONVEX"},
        AdcpSystemConfigurationKey.ORIENTATION: {'type': unicode, 'value': "UP"},
        AdcpSystemConfigurationKey.SENSORS: {'type': unicode,
                                             'value': "HEADING  TILT 1  TILT 2  TEMPERATURE"},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_c3: {'type': float, 'value': 0},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_c2: {'type': float, 'value': 0},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_c1: {'type': float, 'value': 0},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_OFFSET: {'type': float, 'value': 0},
        AdcpSystemConfigurationKey.TEMPERATURE_SENSOR_OFFSET: {'type': float, 'value': 0.03},
        AdcpSystemConfigurationKey.CPU_FIRMWARE: {'type': unicode, 'value': "50.40 [0]"},
        AdcpSystemConfigurationKey.BOOT_CODE_REQUIRED: {'type': unicode, 'value': "1.16"},
        AdcpSystemConfigurationKey.BOOT_CODE_ACTUAL: {'type': unicode, 'value': "1.16"},
        AdcpSystemConfigurationKey.DEMOD_1_VERSION: {'type': unicode, 'value': "ad48"},
        AdcpSystemConfigurationKey.DEMOD_1_TYPE: {'type': unicode, 'value': "1f"},
        AdcpSystemConfigurationKey.DEMOD_2_VERSION: {'type': unicode, 'value': "ad48"},
        AdcpSystemConfigurationKey.DEMOD_2_TYPE: {'type': unicode, 'value': "1f"},
        AdcpSystemConfigurationKey.POWER_TIMING_VERSION: {'type': unicode, 'value': "85d3"},
        AdcpSystemConfigurationKey.POWER_TIMING_TYPE: {'type': unicode, 'value': "6"},
        AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS: {'type': unicode,
                                                          'value': "9A  00 00 06 83 8B 94  09 CPU727-2011-00E,"
                                                                   "B8  00 00 06 B2 B7 C6  09 DSP727-2001-03H,"
                                                                   "3B  00 00 06 B3 32 FD  09 PIO727-3000-00G,"
                                                                   "40  00 00 06 B2 D8 57  09 REC727-1000-03E"}
    }

    _pd0_parameters_base = {
        AdcpPd0ParsedKey.HEADER_ID: {'type': int, 'value': 127},
        AdcpPd0ParsedKey.DATA_SOURCE_ID: {'type': int, 'value': 127},
        AdcpPd0ParsedKey.NUM_BYTES: {'type': int, 'value': 26632},
        AdcpPd0ParsedKey.NUM_DATA_TYPES: {'type': int, 'value': 6},
        AdcpPd0ParsedKey.OFFSET_DATA_TYPES: {'type': list, 'value': [18, 77, 142, 944, 1346, 1748, 2150]},
        AdcpPd0ParsedKey.FIXED_LEADER_ID: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.FIRMWARE_VERSION: {'type': int, 'value': 50},
        AdcpPd0ParsedKey.FIRMWARE_REVISION: {'type': int, 'value': 40},
        AdcpPd0ParsedKey.SYSCONFIG_FREQUENCY: {'type': int, 'value': 150},
        AdcpPd0ParsedKey.SYSCONFIG_BEAM_PATTERN: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SYSCONFIG_SENSOR_CONFIG: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SYSCONFIG_HEAD_ATTACHED: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SYSCONFIG_VERTICAL_ORIENTATION: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.DATA_FLAG: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.LAG_LENGTH: {'type': int, 'value': 53},
        AdcpPd0ParsedKey.NUM_BEAMS: {'type': int, 'value': 4},
        AdcpPd0ParsedKey.NUM_CELLS: {'type': int, 'value': 100},
        AdcpPd0ParsedKey.PINGS_PER_ENSEMBLE: {'type': int, 'value': 256},
        AdcpPd0ParsedKey.DEPTH_CELL_LENGTH: {'type': int, 'value': 32780},
        AdcpPd0ParsedKey.BLANK_AFTER_TRANSMIT: {'type': int, 'value': 49154},
        AdcpPd0ParsedKey.SIGNAL_PROCESSING_MODE: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.LOW_CORR_THRESHOLD: {'type': int, 'value': 64},
        AdcpPd0ParsedKey.NUM_CODE_REPETITIONS: {'type': int, 'value': 17},
        AdcpPd0ParsedKey.PERCENT_GOOD_MIN: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ERROR_VEL_THRESHOLD: {'type': int, 'value': 53255},
        AdcpPd0ParsedKey.TIME_PER_PING_MINUTES: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.TIME_PER_PING_SECONDS: {'type': float, 'value': 1.0},
        AdcpPd0ParsedKey.COORD_TRANSFORM_TYPE: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.COORD_TRANSFORM_TILTS: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.COORD_TRANSFORM_BEAMS: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.COORD_TRANSFORM_MAPPING: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.HEADING_ALIGNMENT: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.HEADING_BIAS: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SENSOR_SOURCE_SPEED: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_SOURCE_DEPTH: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_SOURCE_HEADING: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_SOURCE_PITCH: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_SOURCE_ROLL: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_SOURCE_CONDUCTIVITY: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SENSOR_SOURCE_TEMPERATURE: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_AVAILABLE_DEPTH: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_AVAILABLE_HEADING: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_AVAILABLE_PITCH: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_AVAILABLE_ROLL: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SENSOR_AVAILABLE_CONDUCTIVITY: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SENSOR_AVAILABLE_TEMPERATURE: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.BIN_1_DISTANCE: {'type': int, 'value': 60175},
        AdcpPd0ParsedKey.TRANSMIT_PULSE_LENGTH: {'type': int, 'value': 4109},
        AdcpPd0ParsedKey.REFERENCE_LAYER_START: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.REFERENCE_LAYER_STOP: {'type': int, 'value': 5},
        AdcpPd0ParsedKey.FALSE_TARGET_THRESHOLD: {'type': int, 'value': 50},
        AdcpPd0ParsedKey.LOW_LATENCY_TRIGGER: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.TRANSMIT_LAG_DISTANCE: {'type': int, 'value': 50688},
        AdcpPd0ParsedKey.CPU_BOARD_SERIAL_NUMBER: {'type': str, 'value': '9367487254980977929'},
        AdcpPd0ParsedKey.SYSTEM_BANDWIDTH: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SYSTEM_POWER: {'type': int, 'value': 255},
        AdcpPd0ParsedKey.SERIAL_NUMBER: {'type': str, 'value': '206045184'},
        AdcpPd0ParsedKey.BEAM_ANGLE: {'type': int, 'value': 20},
        AdcpPd0ParsedKey.VARIABLE_LEADER_ID: {'type': int, 'value': 128},
        AdcpPd0ParsedKey.ENSEMBLE_NUMBER: {'type': int, 'value': 5},
        AdcpPd0ParsedKey.ENSEMBLE_START_TIME: {'type': float, 'value': 3595104000},
        AdcpPd0ParsedKey.REAL_TIME_CLOCK: {'type': list, 'value': [13, 3, 15, 21, 33, 2, 46]},
        AdcpPd0ParsedKey.ENSEMBLE_NUMBER_INCREMENT: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.BIT_RESULT_DEMOD_0: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.BIT_RESULT_DEMOD_1: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.BIT_RESULT_TIMING: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SPEED_OF_SOUND: {'type': int, 'value': 1523},
        AdcpPd0ParsedKey.TRANSDUCER_DEPTH: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.HEADING: {'type': int, 'value': 5221},
        AdcpPd0ParsedKey.PITCH: {'type': int, 'value': -4657},
        AdcpPd0ParsedKey.ROLL: {'type': int, 'value': -4561},
        AdcpPd0ParsedKey.SALINITY: {'type': int, 'value': 35},
        AdcpPd0ParsedKey.TEMPERATURE: {'type': int, 'value': 2050},
        AdcpPd0ParsedKey.MPT_MINUTES: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.MPT_SECONDS: {'type': float, 'value': 0.0},
        AdcpPd0ParsedKey.HEADING_STDEV: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.PITCH_STDEV: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ROLL_STDEV: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ADC_TRANSMIT_CURRENT: {'type': int, 'value': 116},
        AdcpPd0ParsedKey.ADC_TRANSMIT_VOLTAGE: {'type': int, 'value': 169},
        AdcpPd0ParsedKey.ADC_AMBIENT_TEMP: {'type': int, 'value': 88},
        AdcpPd0ParsedKey.ADC_PRESSURE_PLUS: {'type': int, 'value': 79},
        AdcpPd0ParsedKey.ADC_PRESSURE_MINUS: {'type': int, 'value': 79},
        AdcpPd0ParsedKey.ADC_ATTITUDE_TEMP: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ADC_ATTITUDE: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ADC_CONTAMINATION_SENSOR: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.BUS_ERROR_EXCEPTION: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ADDRESS_ERROR_EXCEPTION: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ILLEGAL_INSTRUCTION_EXCEPTION: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ZERO_DIVIDE_INSTRUCTION: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.EMULATOR_EXCEPTION: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.UNASSIGNED_EXCEPTION: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.WATCHDOG_RESTART_OCCURRED: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.BATTERY_SAVER_POWER: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.PINGING: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.COLD_WAKEUP_OCCURRED: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.UNKNOWN_WAKEUP_OCCURRED: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.CLOCK_READ_ERROR: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.UNEXPECTED_ALARM: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.CLOCK_JUMP_FORWARD: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.CLOCK_JUMP_BACKWARD: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.POWER_FAIL: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SPURIOUS_DSP_INTERRUPT: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SPURIOUS_UART_INTERRUPT: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SPURIOUS_CLOCK_INTERRUPT: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.LEVEL_7_INTERRUPT: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ABSOLUTE_PRESSURE: {'type': int, 'value': 4294963793},
        AdcpPd0ParsedKey.PRESSURE_VARIANCE: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.VELOCITY_DATA_ID: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_ID: {'type': int, 'value': 2},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM1: {'type': list,
                                                       'value': [19801, 1796, 1800, 1797, 1288, 1539, 1290, 1543,
                                                                 1028, 1797, 1538, 775, 1034, 1283, 1029, 1799, 1801,
                                                                 1545, 519, 772, 519, 1033, 1028, 1286, 521, 519,
                                                                 1545, 1801, 522, 1286, 1030, 1032, 1542, 1035, 1283,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM2: {'type': list,
                                                       'value': [22365, 2057, 2825, 2825, 1801, 2058, 1545, 1286,
                                                                 3079, 522, 1547, 519, 2052, 2820, 519, 1806, 1026,
                                                                 1547, 1795, 1801, 2311, 1030, 781, 1796, 1037, 1802,
                                                                 1035, 1798, 770, 2313, 1292, 1031, 1030, 2830, 523,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM3: {'type': list,
                                                       'value': [3853, 1796, 1289, 1803, 2317, 2571, 1028, 1282,
                                                                 1799, 2825, 2574, 1026, 1028, 518, 1290, 1286, 1032,
                                                                 1797, 1028, 2312, 1031, 775, 1549, 772, 1028, 772,
                                                                 2570, 1288, 1796, 1542, 1538, 777, 1282, 773, 0, 0,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM4: {'type': list,
                                                       'value': [5386, 4100, 2822, 1286, 774, 1799, 518, 778, 3340,
                                                                 1031, 1546, 1545, 1547, 2566, 3077, 3334, 1801,
                                                                 1809, 2058, 1539, 1798, 1546, 3593, 1032, 2307,
                                                                 1025, 1545, 2316, 2055, 1546, 1292, 2312, 1035,
                                                                 2316, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_ID: {'type': int, 'value': 3},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM1: {'type': list,
                                                'value': [24925, 10538, 10281, 10537, 10282, 10281, 10281, 10282,
                                                          10282, 10281, 10281, 10281, 10538, 10282, 10281, 10282,
                                                          10281, 10537, 10281, 10281, 10281, 10281, 10281, 10281,
                                                          10281, 10281, 10281, 10281, 10281, 10282, 10281, 10282,
                                                          10537, 10281, 10281, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM2: {'type': list,
                                                'value': [29027, 12334, 12334, 12078, 12078, 11821, 12334, 12334,
                                                          12078, 12078, 12078, 12078, 12078, 12078, 12078, 12079,
                                                          12334, 12078, 12334, 12333, 12078, 12333, 12078, 12077,
                                                          12078, 12078, 12078, 12334, 12077, 12078, 12078, 12078,
                                                          12078, 12078, 12078, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM3: {'type': list,
                                                'value': [12079, 10282, 10281, 10281, 10282, 10281, 10282, 10282,
                                                          10281, 10025, 10282, 10282, 10282, 10282, 10025, 10282,
                                                          10281, 10025, 10281, 10281, 10282, 10281, 10282, 10281,
                                                          10281, 10281, 10537, 10282, 10281, 10281, 10281, 10281,
                                                          10281, 10282, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM4: {'type': list,
                                                'value': [14387, 12334, 12078, 12078, 12078, 12334, 12078, 12334,
                                                          12078, 12078, 12077, 12077, 12334, 12078, 12334, 12078,
                                                          12334, 12077, 12078, 11821, 12335, 12077, 12078, 12077,
                                                          12334, 11822, 12334, 12334, 12077, 12077, 12078, 11821,
                                                          11821, 12078, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_ID: {'type': int, 'value': 4},
        AdcpPd0ParsedKey.CHECKSUM: {'type': int, 'value': 8239}
    }

    _coordinate_transformation_earth_parameters = {
        # Earth Coordinates
        AdcpPd0ParsedKey.WATER_VELOCITY_EAST: {'type': list,
                                               'value': [128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                         128, 128, 128]},
        AdcpPd0ParsedKey.WATER_VELOCITY_NORTH: {'type': list,
                                                'value': [128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                          128, 128, 128]},
        AdcpPd0ParsedKey.WATER_VELOCITY_UP: {'type': list,
                                             'value': [128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                       128, 128, 128]},
        AdcpPd0ParsedKey.ERROR_VELOCITY: {'type': list,
                                          'value': [128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                    128, 128, 128, 128, 128, 128, 128, 128]},
        AdcpPd0ParsedKey.PERCENT_GOOD_3BEAM: {'type': list,
                                              'value': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_TRANSFORMS_REJECT: {'type': list,
                                                     'value': [25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                               25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                               25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                               25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                               25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                               25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                               25600, 25600, 25600, 25600, 25600, 25600, 25600]},
        AdcpPd0ParsedKey.PERCENT_BAD_BEAMS: {'type': list,
                                             'value': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                       0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_4BEAM: {'type': list,
                                              'value': [25600, 25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                        25600, 25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                        25600, 25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                        25600, 25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                        25600, 25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                        25600, 25600, 25600, 25600, 25600, 25600, 25600, 25600,
                                                        25600]},
    }

    # blue
    _coordinate_transformation_beam_parameters = {
        # Beam Coordinates
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM1: {'type': list,
                                              'value': [25700, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM2: {'type': list,
                                              'value': [25700, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM3: {'type': list,
                                              'value': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM4: {'type': list,
                                              'value': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.BEAM_1_VELOCITY: {'type': list,
                                           'value': [4864, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128]},
        AdcpPd0ParsedKey.BEAM_2_VELOCITY: {'type': list,
                                           'value': [62719, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128]},
        AdcpPd0ParsedKey.BEAM_3_VELOCITY: {'type': list,
                                           'value': [45824, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128]},
        AdcpPd0ParsedKey.BEAM_4_VELOCITY: {'type': list,
                                           'value': [19712, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
                                                     128, 128, 128, 128, 128, 128, 128, 128, 128]},
    }

    _pd0_parameters = dict(_pd0_parameters_base.items() +
                           _coordinate_transformation_beam_parameters.items())
    _pd0_parameters_earth = dict(_pd0_parameters_base.items() +
                                 _coordinate_transformation_earth_parameters.items())

    _pt2_dict = {
        AdcpAncillarySystemDataKey.ADCP_AMBIENT_CURRENT: {'type': float, 'value': "20.32"},
        AdcpAncillarySystemDataKey.ADCP_ATTITUDE_TEMP: {'type': float, 'value': "24.65"},
        AdcpAncillarySystemDataKey.ADCP_INTERNAL_MOISTURE: {'type': unicode, 'value': "8F0Ah"}
    }

    _pt4_dict = {
        AdcpTransmitPathKey.ADCP_TRANSIT_CURRENT: {'type': float, 'value': "2.0"},
        AdcpTransmitPathKey.ADCP_TRANSIT_VOLTAGE: {'type': float, 'value': "60.1"},
        AdcpTransmitPathKey.ADCP_TRANSIT_IMPEDANCE: {'type': float, 'value': "29.8"},
        AdcpTransmitPathKey.ADCP_TRANSIT_TEST_RESULT: {'type': unicode, 'value': "$0 ... PASS"},

    }

    # Driver Parameter Methods
    ###
    # def assert_driver_parameters(self, current_parameters, verify_values=False):
    #     """
    #     Verify that all driver parameters are correct and potentially verify values.
    #     @param current_parameters: driver parameters read from the driver instance
    #     @param verify_values: should we verify values against definition?
    #     """
    #     log.debug("assert_driver_parameters current_parameters = " + str(current_parameters))
    #     temp_parameters = copy.deepcopy(self._driver_parameters)
    #     temp_parameters.update(self._driver_parameters_slave)
    #     self.assert_parameters(current_parameters, temp_parameters, verify_values)

    ###
    # Data Particle Parameters Methods
    ###
    def assert_sample_data_particle(self, data_particle):
        """
        Verify a particle is a know particle to this driver and verify the particle is  correct
        @param data_particle: Data particle of unknown type produced by the driver
        """

        if isinstance(data_particle, VADCPDataParticleType.ADCP_PD0_PARSED_BEAM):
            self.assert_particle_pd0_data(data_particle)
        elif isinstance(data_particle, VADCPDataParticleType.ADCP_SYSTEM_CONFIGURATION):
            self.assert_particle_system_configuration(data_particle)
        elif isinstance(data_particle, VADCPDataParticleType.ADCP_COMPASS_CALIBRATION):
            self.assert_particle_compass_calibration(data_particle)
        else:
            log.error("Unknown Particle Detected: %s" % data_particle)
            self.assertFalse(True)

    def assert_particle_compass_calibration(self, data_particle, verify_values=True):
        """
        Verify an adcpt calibration data particle
        @param data_particle: ADCPT_CalibrationDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_COMPASS_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_data_parameters, verify_values)

    def assert_particle_4b_system_configuration(self, data_particle, verify_values=True):
        """
        Verify an adcpt fd data particle
        @param data_particle: ADCPT_FDDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_SYSTEM_CONFIGURATION)
        self.assert_data_particle_parameters(data_particle, self._system_configuration_data_parameters,
                                             verify_values)

    def assert_particle_5b_system_configuration(self, data_particle, verify_values=True):
        """
        Verify an adcpt fd data particle
        @param data_particle: ADCPT_FDDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_SYSTEM_CONFIGURATION)
        self.assert_data_particle_parameters(data_particle, self._system_configuration_data_parameters_VADCP,
                                             verify_values)

    def assert_particle_pd0_data(self, data_particle, verify_values=True):
        """
        Verify an adcpt ps0 data particle
        @param data_particle: ADCPT_PS0DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_PD0_BEAM_MASTER)
        self.assert_data_particle_parameters(data_particle, self._pd0_parameters)  # , verify_values

    def assert_particle_pd0_data_earth(self, data_particle, verify_values=True):
        """
        Verify an adcpt ps0 data particle
        @param data_particle: ADCPT_PS0DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_PD0_PARSED_EARTH)
        self.assert_data_particle_parameters(data_particle, self._pd0_parameters_earth)  # , verify_values

    def assert_particle_pt2_data(self, data_particle, verify_values=True):
        """
        Verify an adcpt pt2 data particle
        @param data_particle: ADCPT_PT2 DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_ANCILLARY_SYSTEM_DATA)
        self.assert_data_particle_parameters(data_particle, self._pt2_dict)  # , verify_values

    def assert_particle_pt4_data(self, data_particle, verify_values=True):
        """
        Verify an adcpt pt4 data particle
        @param data_particle: ADCPT_PT4 DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_TRANSMIT_PATH)
        self.assert_data_particle_parameters(data_particle, self._pt4_dict)  # , verify_values


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class VadcpDriverUnitTest(WorkhorseDriverUnitTest, VADCPMixin):
    def setUp(self):
        self._driver_class = InstrumentDriver
        log.error(self._driver_class)
        WorkhorseDriverUnitTest.setUp(self)
        self.maxDiff = None

    def assert_initialize_driver(self, driver, initial_protocol_state=WorkhorseProtocolState.COMMAND):
        """
        OVERRIDE
        Initialize an instrument driver with a mock port agent.  This will allow us to test the
        got data method.  Will the instrument, using test mode, through it's connection state
        machine.  End result, the driver will be in test mode and the connection state will be
        connected.
        @param driver: Instrument driver instance.
        @param initial_protocol_state: the state to force the driver too
        """
        # Put the driver into test mode
        driver.set_test_mode(True)

        # disable autoconnect
        driver._autoconnect = False

        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.UNCONFIGURED)

        # Now configure the driver with the mock_port_agent, verifying
        # that the driver transitions to that state
        config = {'4Beam': {'mock_port_agent': Mock(spec=PortAgentClient)},
                  '5thBeam': {'mock_port_agent': Mock(spec=PortAgentClient)}}
        driver.configure(config)

        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.DISCONNECTED)

        # Invoke the connect method of the driver: should connect to mock
        # port agent.  Verify that the connection FSM transitions to CONNECTED,
        # (which means that the FSM should now be reporting the WorkhorseProtocolState).
        driver.connect()
        current_state = driver.get_resource_state()
        self.assertEqual(current_state, WorkhorseProtocolState.UNKNOWN)

        # Force the instrument into a known state
        self.assert_force_state(driver, initial_protocol_state)

    def test_recover_autosample(self):
        """
        Overrides base class, not applicable to VADCP
        :return:
        """

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        temp_parameters = copy.deepcopy(self._vadcp_driver_parameters)
        temp_parameters.update(self._driver_parameters_slave)

        driver = self._driver_class(self._got_data_event_callback)
        self.assert_driver_schema(driver, temp_parameters, self._driver_capabilities)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = self._driver_class(self._got_data_event_callback)
        self.assert_initialize_driver(driver, initial_protocol_state=WorkhorseProtocolState.AUTOSAMPLE)

        got_data = driver._protocol.got_data
        driver._protocol.got_data = functools.partial(got_data, connection=SlaveProtocol.FOURBEAM)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, RSN_CALIBRATION_RAW_DATA, self.assert_particle_compass_calibration, True)
        self.assert_particle_published(driver, RSN_PS0_RAW_DATA, self.assert_particle_4b_system_configuration, True)
        self.assert_particle_published(driver, RSN_SAMPLE_RAW_DATA, self.assert_particle_pd0_data, True)
        self.assert_particle_published(driver, PT2_RAW_DATA, self.assert_particle_pt2_data, True)
        self.assert_particle_published(driver, PT4_RAW_DATA, self.assert_particle_pt4_data, True)

        driver._protocol.got_data = functools.partial(got_data, connection=SlaveProtocol.FIFTHBEAM)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, RSN_CALIBRATION_RAW_DATA, self.assert_particle_compass_calibration, True)
        self.assert_particle_published(driver, VADCP_SLAVE_PS0_RAW_DATA, self.assert_particle_5b_system_configuration,
                                       True)
        self.assert_particle_published(driver, RSN_SAMPLE_RAW_DATA, self.assert_particle_pd0_data, True)
        self.assert_particle_published(driver, PT2_RAW_DATA, self.assert_particle_pt2_data, True)
        self.assert_particle_published(driver, PT4_RAW_DATA, self.assert_particle_pt4_data, True)

    def test_driver_parameters(self):
        """
        Verify the set of parameters known by the driver
        """
        driver = self._driver_class(self._got_data_event_callback)
        self.assert_initialize_driver(driver, WorkhorseProtocolState.COMMAND)

        expected_parameters_master = sorted(self._vadcp_driver_parameters)
        expected_parameters_slave = sorted(self._driver_parameters_slave)

        expected_parameters = sorted(expected_parameters_master + expected_parameters_slave)
        reported_parameters = sorted(driver.get_resource(WorkhorseParameter.ALL))
        self.assertEqual(reported_parameters, expected_parameters)

        # Verify the parameter definitions
        d = {}
        d.update(self._vadcp_driver_parameters)
        d.update(self._driver_parameters_slave)
        self.assert_driver_parameter_definition(driver, d)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """

        self.assert_enum_has_no_duplicates(WorkhorseInstrumentCmds)
        self.assert_enum_has_no_duplicates(WorkhorseProtocolState)
        self.assert_enum_has_no_duplicates(WorkhorseProtocolEvent)
        self.assert_enum_has_no_duplicates(WorkhorseParameter)
        self.assert_enum_has_no_duplicates(VADCPDataParticleType)
        self.assert_enum_has_no_duplicates(WorkhorseScheduledJob)
        # Test capabilities for duplicates, them verify that capabilities is a subset of proto events
        self.assert_enum_has_no_duplicates(WorkhorseCapability)
        self.assert_enum_complete(WorkhorseCapability, WorkhorseProtocolEvent)

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(Protocol.sieve_function)

        self.assert_chunker_sample(chunker, RSN_SAMPLE_RAW_DATA)
        self.assert_chunker_sample_with_noise(chunker, RSN_SAMPLE_RAW_DATA)
        self.assert_chunker_fragmented_sample(chunker, RSN_SAMPLE_RAW_DATA, 32)
        self.assert_chunker_combined_sample(chunker, RSN_SAMPLE_RAW_DATA)

        self.assert_chunker_sample(chunker, RSN_PS0_RAW_DATA)
        self.assert_chunker_sample_with_noise(chunker, RSN_PS0_RAW_DATA)
        self.assert_chunker_fragmented_sample(chunker, RSN_PS0_RAW_DATA, 32)
        self.assert_chunker_combined_sample(chunker, RSN_PS0_RAW_DATA)

        self.assert_chunker_sample(chunker, RSN_CALIBRATION_RAW_DATA)
        self.assert_chunker_sample_with_noise(chunker, RSN_CALIBRATION_RAW_DATA)
        self.assert_chunker_fragmented_sample(chunker, RSN_CALIBRATION_RAW_DATA, 32)
        self.assert_chunker_combined_sample(chunker, RSN_CALIBRATION_RAW_DATA)

        self.assert_chunker_sample(chunker, PT2_RAW_DATA)
        self.assert_chunker_sample_with_noise(chunker, PT2_RAW_DATA)
        self.assert_chunker_fragmented_sample(chunker, PT2_RAW_DATA, 32)
        self.assert_chunker_combined_sample(chunker, PT2_RAW_DATA)

        self.assert_chunker_sample(chunker, PT4_RAW_DATA)
        self.assert_chunker_sample_with_noise(chunker, PT4_RAW_DATA)
        self.assert_chunker_fragmented_sample(chunker, PT4_RAW_DATA, 32)
        self.assert_chunker_combined_sample(chunker, PT4_RAW_DATA)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        my_event_callback = Mock(spec="UNKNOWN WHAT SHOULD GO HERE FOR evt_callback")
        protocol = Protocol(WorkhorsePrompt, NEWLINE, my_event_callback, [])
        driver_capabilities = WorkhorseCapability().list()
        test_capabilities = WorkhorseCapability().list()

        # Add a bogus capability that will be filtered out.
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
class VadcpDriverIntegrationTest(WorkhorseDriverIntegrationTest, VADCPMixin):
    _tested = {}

    def setUp(self):
        self.port_agents = {}
        self._driver_parameters = self._vadcp_driver_parameters
        InstrumentDriverIntegrationTestCase.setUp(self)

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

    def assert_VADCP_TRANSMIT_data(self, data_particle, verify_values=True):
        """
        Verify an adcpt pT4 data particle
        @param data_particle: ADCPT_PT4DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_TRANSMIT_PATH)
        self.assert_data_particle_parameters(data_particle, self._pt4_dict)  # , verify_values

    def assert_VADCP_ANCILLARY_data(self, data_particle, verify_values=True):
        """
        Verify an adcpt PT2 data particle
        @param data_particle: ADCPT_PT2DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_ANCILLARY_SYSTEM_DATA)
        self.assert_data_particle_parameters(data_particle, self._pt2_dict, verify_values)

    def assert_VADCP_Calibration(self, data_particle, verify_values=True):
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_COMPASS_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_data_parameters, verify_values)

    def assert_acquire_status(self):
        """
        Overridden to verify additional data particles for VADCP
        """
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_4BEAM_SYSTEM_CONFIGURATION,
                                              self.assert_particle_system_configuration, timeout=60)
        self.assert_async_particle_generation(VADCPDataParticleType.ADCP_COMPASS_CALIBRATION,
                                              self.assert_particle_compass_calibration, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.ADCP_ANCILLARY_SYSTEM_DATA,
                                              self.assert_particle_pt2_data, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.ADCP_TRANSMIT_PATH,
                                              self.assert_particle_pt4_data, timeout=10)

        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_5THBEAM_SYSTEM_CONFIGURATION,
                                              self.assert_particle_system_configuration, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_COMPASS_CALIBRATION,
                                              self.assert_VADCP_Calibration, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_ANCILLARY_SYSTEM_DATA,
                                              self.assert_VADCP_ANCILLARY_data, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_TRANSMIT_PATH,
                                              self.assert_particle_pt4_data, timeout=10)

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
        with self.assertRaises(ResourceError):
            exception_str = 'Oh no, something bad happened!'
            self.driver_client.cmd_dvr('test_exceptions', exception_str)

    # Set bulk params and test auto sampling
    def test_autosample_particle_generation(self):
        """
        Test that we can generate particles when in autosample
        """
        self.assert_initialize_driver()
        self.assert_driver_command(WorkhorseProtocolEvent.START_AUTOSAMPLE, state=WorkhorseProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_PD0_BEAM_MASTER,
                                              self.assert_particle_pd0_data, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_PD0_BEAM_SLAVE,
                                              self.assert_particle_pd0_data, timeout=10)
        self.assert_driver_command(WorkhorseProtocolEvent.STOP_AUTOSAMPLE, state=WorkhorseProtocolState.COMMAND,
                                   delay=1)

    def assert_set_ranges(self, param, good, bad):
        # master
        for each in good:
            self.assert_set(param, each)
        for each in bad:
            self.assert_set_exception(param, each)
        self.assert_set(param, self._driver_parameters[param][self.VALUE])

        # slave
        param += '_5th'
        for each in good:
            self.assert_set(param, each)
        for each in bad:
            self.assert_set_exception(param, each)
        self.assert_set(param, self._driver_parameters_slave[param][self.VALUE])

    def test_set_read_only(self):
        self.assert_initialize_driver()

        for param in [WorkhorseParameter.HEADING_ALIGNMENT,
                      WorkhorseParameter.ENSEMBLE_PER_BURST,
                      WorkhorseParameter.LATENCY_TRIGGER,
                      WorkhorseParameter.DATA_STREAM_SELECTION,
                      WorkhorseParameter.BUFFERED_OUTPUT_PERIOD]:
            self.assert_set_exception(param)
            self.assert_set_exception(param + '_5th')


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class QualFromIDK(WorkhorseDriverQualificationTest, VADCPMixin):
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

    def create_multi_comm_config(self, comm_config):
        result = {}
        for name, config in comm_config.configs.items():
            if config.method() == ConfigTypes.TCP:
                result[name] = self.create_ethernet_comm_config(config)
            elif config.method() == ConfigTypes.SERIAL:
                result[name] = self.create_serial_comm_config(config)
            elif config.method() == ConfigTypes.RSN:
                result[name] = self.create_rsn_comm_config(config)
        return result

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
        self.assert_set_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1487)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)

        self.tcp_client.send_data("%smaster::EC1488%s" % (NEWLINE, NEWLINE))

        self.tcp_client.expect(WorkhorsePrompt.COMMAND)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_enter_command_mode()
        # Direct access is true, it should be set before
        self.assert_get_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1487)

    # Direct access to slave
    def test_direct_access_telnet_mode_slave(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports direct
          access to the physical instrument. (telnet mode)
        """

        self.assert_enter_command_mode()
        self.assert_set_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1487)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)

        self.tcp_client.send_data("%sslave::EC1488%s" % (NEWLINE, NEWLINE))

        self.tcp_client.expect(WorkhorsePrompt.COMMAND)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_enter_command_mode()
        # Direct access is true, it should be set before
        self.assert_get_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1487)

    def test_recover_from_TG(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports direct access
         to the physical instrument. (telnet mode)
        """

        self.assert_enter_command_mode()

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)
        today_plus_1month = (dt.datetime.utcnow() + dt.timedelta(days=31)).strftime("%Y/%m/%d,%H:%m:%S")

        self.tcp_client.send_data("%sTG%s%s" % (NEWLINE, today_plus_1month, NEWLINE))

        self.tcp_client.expect(WorkhorsePrompt.COMMAND)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_enter_command_mode()

        self.assert_get_parameter(WorkhorseParameter.TIME_OF_FIRST_PING, '****/**/**,**:**:**')
