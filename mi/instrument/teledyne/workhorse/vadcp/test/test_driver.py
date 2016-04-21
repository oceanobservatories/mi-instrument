"""
@package mi.instrument.teledyne.workhorse.vadcp.test
@file marine-integrations/mi/instrument/teledyne/workhorse/vadcp/test/test_driver.py
@author Sung Ahn
@brief Test Driver for the VADCP
Release notes:

"""
import ntplib
from mi.core.instrument.data_particle import CommonDataParticleType

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
from mi.core.instrument.port_agent_client import PortAgentClient, PortAgentPacket
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
from mi.instrument.teledyne.workhorse.particles import VADCPDataParticleType, WorkhorseDataParticleType
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
            WorkhorseParameter.HEADING_ALIGNMENT: 0,
            WorkhorseParameter.HEADING_BIAS: 0,
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
            WorkhorseParameter.HEADING_ALIGNMENT + '_5th': 0,
            WorkhorseParameter.HEADING_BIAS + '_5th': 0,
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
        WorkhorseParameter.TIME: {TYPE: str, READONLY: True, DA: False, STARTUP: False, DEFAULT: False},
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
        WorkhorseParameter.HEADING_ALIGNMENT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                               VALUE: 0},
        WorkhorseParameter.HEADING_BIAS: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                          VALUE: 0},
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
        WorkhorseParameter.TIME: {TYPE: str, READONLY: True, DA: False, STARTUP: False, DEFAULT: False},
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
        WorkhorseParameter.HEADING_ALIGNMENT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                               VALUE: 0},
        WorkhorseParameter.HEADING_BIAS: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                          VALUE: 0},
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
        WorkhorseParameter.SYNCH_DELAY: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
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

    _calibration_data_parameters_VADCP = {
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
        AdcpCompassCalibrationKey.OFFSET_UP_DOWN: {'type': list, 'value': [33001.0, 33895.0, 32008.0, 34533.0]},
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
        AdcpPd0ParsedKey.NUM_CELLS: {'type': int, 'value': 100},
        AdcpPd0ParsedKey.DEPTH_CELL_LENGTH: {'type': int, 'value': 3200},
        AdcpPd0ParsedKey.BIN_1_DISTANCE: {'type': int, 'value': 4075},
        AdcpPd0ParsedKey.ENSEMBLE_NUMBER: {'type': int, 'value': 5},
        AdcpPd0ParsedKey.HEADING: {'type': int, 'value': 5221},
        AdcpPd0ParsedKey.PITCH: {'type': int, 'value': -4657},
        AdcpPd0ParsedKey.ROLL: {'type': int, 'value': -4561},
        AdcpPd0ParsedKey.SALINITY: {'type': int, 'value': 35},
        AdcpPd0ParsedKey.TEMPERATURE: {'type': int, 'value': 2050},
        AdcpPd0ParsedKey.TRANSDUCER_DEPTH: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ABSOLUTE_PRESSURE: {'type': int, 'value': 4294963793},
        AdcpPd0ParsedKey.SYSCONFIG_VERTICAL_ORIENTATION: {'type': int, 'value': 1},

    }

    _beam_parameters = {
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM1: {'type': list,
                                                       'value': [77, 15, 7, 7, 7, 5, 7, 7, 5, 9, 6, 10, 5, 4, 6, 5,
                                                                 4, 7, 7, 11, 6, 10, 3, 4, 4, 4, 5, 2, 4, 5, 7, 5,
                                                                 7, 4, 6, 7, 2, 4, 3, 9, 2, 4, 4, 3, 4, 6, 5, 3, 2,
                                                                 4, 2, 3, 6, 10, 7, 5, 2, 7, 5, 6, 4, 6, 4, 3, 6, 5,
                                                                 4, 3, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM2: {'type': list,
                                                       'value': [89, 13, 4, 4, 8, 9, 5, 11, 8, 13, 3, 11, 10, 4, 7,
                                                                 2, 4, 7, 5, 9, 2, 14, 7, 2, 10, 4, 3, 6, 5, 10, 7,
                                                                 6, 9, 8, 9, 5, 7, 4, 4, 8, 7, 7, 9, 7, 4, 13, 6, 4,
                                                                 9, 4, 7, 4, 9, 10, 9, 8, 10, 4, 6, 6, 6, 2, 8, 9,
                                                                 6, 2, 11, 5, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0, 0]},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM3: {'type': list,
                                                       'value': [87, 21, 8, 16, 11, 11, 11, 5, 7, 3, 8, 7, 6, 2, 5,
                                                                 3, 12, 13, 2, 4, 6, 6, 2, 6, 8, 6, 11, 10, 2, 12, 7,
                                                                 13, 4, 7, 6, 7, 7, 8, 7, 6, 9, 7, 4, 6, 3, 14, 7, 4,
                                                                 4, 9, 7, 4, 4, 6, 7, 9, 3, 8, 9, 6, 5, 5, 4, 9, 4,
                                                                 4, 11, 9, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0]},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM4: {'type': list,
                                                       'value': [93, 10, 9, 4, 9, 6, 9, 6, 9, 6, 10, 7, 9, 6, 6, 10,
                                                                 7, 12, 10, 7, 11, 10, 7, 9, 4, 11, 4, 6, 7, 5, 14,
                                                                 6, 2, 9, 11, 17, 3, 10, 9, 3, 7, 6, 6, 10, 13, 9, 4,
                                                                 8, 13, 3, 10, 1, 11, 9, 6, 12, 2, 7, 9, 10, 12, 12,
                                                                 7, 8, 6, 11, 14, 12, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM1: {'type': list,
                                                'value': [97, 47, 41, 40, 40, 40, 41, 40, 40, 40, 40, 40, 40, 40, 40,
                                                          40, 40, 40, 40, 39, 40, 40, 40, 40, 41, 40, 40, 40, 40, 39,
                                                          40, 40, 40, 40, 41, 39, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                                          40, 40, 40, 40, 40, 40, 40, 40, 41, 40, 40, 40, 40, 40, 40,
                                                          40, 40, 40, 40, 41, 40, 40, 40, 40, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM2: {'type': list,
                                                'value': [93, 47, 42, 42, 41, 41, 41, 41, 42, 42, 41, 41, 41, 42, 42,
                                                          42, 42, 41, 41, 41, 41, 42, 41, 42, 42, 42, 42, 42, 41, 41,
                                                          42, 42, 41, 41, 41, 41, 41, 41, 41, 41, 41, 42, 41, 41, 41,
                                                          42, 41, 41, 41, 41, 41, 41, 41, 41, 41, 42, 41, 41, 42, 41,
                                                          41, 41, 42, 41, 41, 41, 41, 42, 41, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM3: {'type': list,
                                                'value': [113, 56, 48, 48, 48, 47, 47, 47, 47, 47, 46, 48, 48, 47,
                                                          48, 48, 47, 47, 47, 47, 47, 47, 47, 47, 47, 48, 47, 47, 47,
                                                          48, 47, 47, 48, 48, 47, 47, 48, 47, 48, 46, 47, 48, 48, 47,
                                                          47, 47, 47, 47, 47, 48, 47, 46, 47, 48, 48, 48, 47, 47, 47,
                                                          47, 47, 47, 47, 46, 47, 46, 47, 47, 47, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM4: {'type': list,
                                                'value': [99, 51, 46, 46, 46, 46, 46, 46, 46, 46, 45, 46, 46, 46, 46,
                                                          46, 46, 46, 46, 46, 46, 45, 46, 45, 46, 46, 46, 46, 46, 46,
                                                          47, 46, 46, 46, 46, 45, 46, 46, 45, 45, 46, 47, 45, 45, 46,
                                                          46, 45, 45, 46, 46, 46, 46, 46, 46, 46, 46, 45, 45, 46, 45,
                                                          46, 46, 46, 45, 46, 45, 46, 46, 46, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM1: {'type': list,
                                              'value': [100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM2: {'type': list,
                                              'value': [100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM3: {'type': list,
                                              'value': [100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM4: {'type': list,
                                              'value': [100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},

        AdcpPd0ParsedKey.BEAM_1_VELOCITY: {'type': list,
                                           'value': [19, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768]},
        AdcpPd0ParsedKey.BEAM_2_VELOCITY: {'type': list,
                                           'value': [-12, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768]},
        AdcpPd0ParsedKey.BEAM_3_VELOCITY: {'type': list,
                                           'value': [179, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768]},
        AdcpPd0ParsedKey.BEAM_4_VELOCITY: {'type': list,
                                           'value': [77, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768]},
    }

    _beam_parameters_slave = {
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_BEAM5: {'type': list,
                                                       'value': [77, 15, 7, 7, 7, 5, 7, 7, 5, 9, 6, 10, 5, 4, 6, 5,
                                                                 4, 7, 7, 11, 6, 10, 3, 4, 4, 4, 5, 2, 4, 5, 7, 5,
                                                                 7, 4, 6, 7, 2, 4, 3, 9, 2, 4, 4, 3, 4, 6, 5, 3, 2,
                                                                 4, 2, 3, 6, 10, 7, 5, 2, 7, 5, 6, 4, 6, 4, 3, 6, 5,
                                                                 4, 3, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
        AdcpPd0ParsedKey.ECHO_INTENSITY_BEAM5: {'type': list,
                                                'value': [97, 47, 41, 40, 40, 40, 41, 40, 40, 40, 40, 40, 40, 40, 40,
                                                          40, 40, 40, 40, 39, 40, 40, 40, 40, 41, 40, 40, 40, 40, 39,
                                                          40, 40, 40, 40, 41, 39, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                                          40, 40, 40, 40, 40, 40, 40, 40, 41, 40, 40, 40, 40, 40, 40,
                                                          40, 40, 40, 40, 41, 40, 40, 40, 40, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                          0, 0, 0]},
        AdcpPd0ParsedKey.PERCENT_GOOD_BEAM5: {'type': list,
                                              'value': [100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},

        AdcpPd0ParsedKey.BEAM_5_VELOCITY: {'type': list,
                                           'value': [19, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                                     -32768, -32768, -32768, -32768]},
    }

    _pd0_parameters = dict(_pd0_parameters_base.items() +
                           _beam_parameters.items())
    _pd0_parameters_slave = dict(_pd0_parameters_base.items() +
                           _beam_parameters_slave.items())

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

    def assert_particle_compass_calibration(self, data_particle, verify_values=False):
        """
        Verify an adcpt calibration data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_COMPASS_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_data_parameters, verify_values)

    def assert_particle_compass_calibration_slave(self, data_particle, verify_values=False):
        """
        Verify an adcp calibration data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_COMPASS_CALIBRATION_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._calibration_data_parameters, verify_values)

    def assert_particle_system_configuration_master(self, data_particle, verify_values=False):
        """
        Verify an adcpt fd data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.ADCP_SYSTEM_CONFIGURATION)
        self.assert_data_particle_parameters(data_particle, self._system_configuration_data_parameters,
                                             verify_values)

    def assert_particle_system_configuration_slave(self, data_particle, verify_values=False):
        """
        Verify an adcpt fd data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_SYSTEM_CONFIGURATION_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._system_configuration_data_parameters_VADCP,
                                             verify_values)

    def assert_particle_pt2_data_slave(self, data_particle, verify_values=False):
        """
        Verify an adcpt pt2 data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_ANCILLARY_SYSTEM_DATA_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._pt2_dict, verify_values)

    def assert_particle_pt4_data_slave(self, data_particle, verify_values=False):
        """
        Verify an adcpt pt4 data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_TRANSMIT_PATH_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._pt4_dict, verify_values)

    def assert_particle_pd0_data_master(self, data_particle, verify_values=False):
        """
        Verify an adcp pd0 data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_PD0_BEAM_MASTER)
        self.assert_data_particle_parameters(data_particle, self._pd0_parameters, verify_values)

    def assert_particle_pd0_data_slave(self, data_particle, verify_values=False):
        """
        Verify an adcp pd0 data particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_PD0_BEAM_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._pd0_parameters_slave, verify_values)

    def assert_particle_pd0_engineering_slave(self, data_particle, verify_values=False):
        """
        Verify an adcp pd0 engineering particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_PD0_ENGINEERING_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._pd0_engineering_parameters, verify_values)

    def assert_particle_pd0_config_slave(self, data_particle, verify_values=False):
        """
        Verify an adcp pd0 config particle
        @param data_particle: data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_PD0_CONFIG_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._pd0_config_parameters, verify_values)

    def assert_vadcp_pd0_particles_published(self, driver, sample_data, verify_values=False):
        """
        Verify that we can send data through the port agent and the the correct particles
        are generated.

        Create a port agent packet, send it through got_data, then finally grab the data particle
        from the data particle queue and verify it using the passed in assert method.
        @param driver: instrument driver with mock port agent client
        @param sample_data: the byte string we want to send to the driver
        @param particle_assert_method: assert method to validate the data particle.
        @param verify_values: Should we validate values?
        """
        ts = ntplib.system_to_ntp_time(time.time())

        log.debug("Sample to publish: %r", sample_data)
        # Create and populate the port agent packet.
        port_agent_packet = PortAgentPacket()
        port_agent_packet.attach_data(sample_data)
        port_agent_packet.attach_timestamp(ts)
        port_agent_packet.pack_header()

        self.clear_data_particle_queue()

        # Push the data into the driver
        driver._protocol.got_data(port_agent_packet)

        # Find all particles of the correct data particle types (not raw)
        particles = []
        streams = []
        for p in self._data_particle_received:
            stream_type = p.get('stream_name')
            self.assertIsNotNone(stream_type)
            streams.append(stream_type)
            if stream_type != CommonDataParticleType.RAW:
                particles.append(p)

        log.debug("Non raw particles: %r ", particles)
        self.assertGreaterEqual(len(particles), 1)

        for p in particles:
            stream_type = p.get('stream_name')
            if VADCPDataParticleType.VADCP_PD0_BEAM_MASTER in streams:
                if stream_type == VADCPDataParticleType.VADCP_PD0_BEAM_MASTER:
                    self.assert_particle_pd0_data_master(p, verify_values)
                elif stream_type == WorkhorseDataParticleType.ADCP_PD0_ENGINEERING:
                    self.assert_particle_pd0_engineering(p, verify_values)
                elif stream_type == WorkhorseDataParticleType.ADCP_PD0_CONFIG:
                    self.assert_particle_pd0_config(p, verify_values)
                else:
                    raise AssertionError('Received invalid particle type from PD0: %r' % stream_type)
            else:
                if stream_type == VADCPDataParticleType.VADCP_PD0_BEAM_SLAVE:
                    self.assert_particle_pd0_data_slave(p, verify_values)
                elif stream_type == VADCPDataParticleType.VADCP_PD0_ENGINEERING_SLAVE:
                    self.assert_particle_pd0_engineering_slave(p, verify_values)
                elif stream_type == VADCPDataParticleType.VADCP_PD0_CONFIG_SLAVE:
                    self.assert_particle_pd0_config_slave(p, verify_values)
                else:
                    raise AssertionError('Received invalid particle type from PD0: %r' % stream_type)


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
        # (which means that the FSM should now be reporting the ProtocolState).
        driver.connect()
        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.INST_DISCONNECTED)

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

        # Start validating data particles
        self.assert_particle_published(driver, RSN_CALIBRATION_RAW_DATA, self.assert_particle_compass_calibration, True)
        self.assert_particle_published(driver, RSN_PS0_RAW_DATA, self.assert_particle_system_configuration_master, True)
        self.assert_particle_published(driver, PT2_RAW_DATA, self.assert_particle_pt2_data, True)
        self.assert_particle_published(driver, PT4_RAW_DATA, self.assert_particle_pt4_data, True)

        self.assert_vadcp_pd0_particles_published(driver, RSN_SAMPLE_RAW_DATA, True)

        driver._protocol.got_data = functools.partial(got_data, connection=SlaveProtocol.FIFTHBEAM)

        # Start validating data particles
        self.assert_particle_published(driver, RSN_CALIBRATION_RAW_DATA,
                                       self.assert_particle_compass_calibration_slave, True)
        self.assert_particle_published(driver, VADCP_SLAVE_PS0_RAW_DATA,
                                       self.assert_particle_system_configuration_slave, True)
        self.assert_particle_published(driver, PT2_RAW_DATA, self.assert_particle_pt2_data_slave, True)
        self.assert_particle_published(driver, PT4_RAW_DATA, self.assert_particle_pt4_data_slave, True)

        self.assert_vadcp_pd0_particles_published(driver, RSN_SAMPLE_RAW_DATA, True)

    def test_driver_parameters(self):
        """
        Verify the set of parameters known by the driver
        """
        driver = self._driver_class(self._got_data_event_callback)
        self.assert_initialize_driver(driver, WorkhorseProtocolState.COMMAND)

        expected_parameters = sorted(self._vadcp_driver_parameters)
        reported_parameters = sorted(driver.get_resource(WorkhorseParameter.ALL))
        self.assertEqual(reported_parameters, expected_parameters)

        # Verify the parameter definitions
        d = {}
        d.update(self._vadcp_driver_parameters)
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
        my_event_callback = Mock()
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

    def assert_VADCP_ANCILLARY_data(self, data_particle, verify_values=False):
        """
        Verify an adcpt PT2 data particle
        @param data_particle: ADCPT_PT2DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_ANCILLARY_SYSTEM_DATA_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._pt2_dict, verify_values)

    def assert_VADCP_Calibration(self, data_particle, verify_values=False):
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_COMPASS_CALIBRATION_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._calibration_data_parameters_VADCP, verify_values)

    def assert_particle_system_configuration_5th(self, data_particle, verify_values=False):
        """
        Verify an adcp fd data particle
        @param data_particle: ADCPT_FDDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, VADCPDataParticleType.VADCP_SYSTEM_CONFIGURATION_SLAVE)
        self.assert_data_particle_parameters(data_particle, self._system_configuration_data_parameters_VADCP,
                                             verify_values)

    def assert_acquire_status(self):
        """
        Overridden to verify additional data particles for VADCP
        """
        self.assert_async_particle_generation(VADCPDataParticleType.ADCP_SYSTEM_CONFIGURATION,
                                              self.assert_particle_system_configuration, timeout=60)
        self.assert_async_particle_generation(VADCPDataParticleType.ADCP_COMPASS_CALIBRATION,
                                              self.assert_particle_compass_calibration, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.ADCP_ANCILLARY_SYSTEM_DATA,
                                              self.assert_particle_pt2_data, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.ADCP_TRANSMIT_PATH,
                                              self.assert_particle_pt4_data, timeout=10)

        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_SYSTEM_CONFIGURATION_SLAVE,
                                              self.assert_particle_system_configuration_5th, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_COMPASS_CALIBRATION_SLAVE,
                                              self.assert_VADCP_Calibration, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_ANCILLARY_SYSTEM_DATA_SLAVE,
                                              self.assert_VADCP_ANCILLARY_data, timeout=10)
        self.assert_async_particle_generation(VADCPDataParticleType.VADCP_TRANSMIT_PATH_SLAVE,
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
        self.assert_set(param, self._driver_parameters[param][self.VALUE])

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
