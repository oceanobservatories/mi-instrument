"""
@package mi.instrument.teledyne.workhorse.driver
@file marine-integrations/mi/instrument/teledyne/workhorse/ADCP/test/test_driver.py
@author Sung Ahn
@brief Test Driver for the ADCP
Release notes:
Generic test Driver for ADCPS-K, ADCPS-I, ADCPT-B and ADCPT-DE
"""
import time
import copy
import unittest

from mi.core.log import get_logger

log = get_logger()

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.chunker import StringChunker

from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import AgentCapabilityType

from mi.instrument.teledyne.workhorse.test.test_data import RSN_SAMPLE_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import PT2_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import PT4_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import RSN_CALIBRATION_RAW_DATA
from mi.instrument.teledyne.workhorse.test.test_data import RSN_PS0_RAW_DATA

from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import ParameterTestConfigKey
from mi.idk.unit_test import DriverStartupConfigKey

from mi.instrument.teledyne.workhorse.driver import WorkhorseParameter
from mi.instrument.teledyne.workhorse.driver import WorkhorsePrompt
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocolEvent
from mi.instrument.teledyne.workhorse.driver import NEWLINE
from mi.instrument.teledyne.workhorse.driver import WorkhorseScheduledJob
from mi.instrument.teledyne.workhorse.driver import WorkhorseCapability
from mi.instrument.teledyne.workhorse.driver import WorkhorseInstrumentCmds
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocol
from mi.instrument.teledyne.workhorse.driver import WorkhorseProtocolState
from mi.instrument.teledyne.workhorse.particles import WorkhorseDataParticleType
from mi.instrument.teledyne.workhorse.particles import AdcpCompassCalibrationKey
from mi.instrument.teledyne.workhorse.particles import AdcpSystemConfigurationKey
from mi.instrument.teledyne.workhorse.particles import AdcpPd0ParsedKey
from mi.instrument.teledyne.workhorse.particles import AdcpAncillarySystemDataKey
from mi.instrument.teledyne.workhorse.particles import AdcpTransmitPathKey
from mi.instrument.teledyne.workhorse.adcp.driver import InstrumentDriver

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'


###
# Driver parameters for tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.teledyne.workhorse.adcp.driver',
    driver_class="InstrumentDriver",
    instrument_agent_resource_id='HTWZMW',
    instrument_agent_preload_id='IA7',
    instrument_agent_name='teledyne_workhorse_monitor ADCP',
    instrument_agent_packet_config=WorkhorseDataParticleType(),
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
            WorkhorseParameter.BLANK_AFTER_TRANSMIT: 704,
            WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: 0,
            WorkhorseParameter.RECEIVER_GAIN_SELECT: 1,
            WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: 100,
            WorkhorseParameter.PINGS_PER_ENSEMBLE: 1,
            WorkhorseParameter.DEPTH_CELL_SIZE: 800,
            WorkhorseParameter.TRANSMIT_LENGTH: 0,
            WorkhorseParameter.PING_WEIGHT: 0,
            WorkhorseParameter.AMBIGUITY_VELOCITY: 175,
            WorkhorseParameter.LATENCY_TRIGGER: 0,
            WorkhorseParameter.HEADING_ALIGNMENT: 0,
            WorkhorseParameter.HEADING_BIAS: 0,
            WorkhorseParameter.TRANSDUCER_DEPTH: 8000,
            WorkhorseParameter.DATA_STREAM_SELECTION: 0,
            WorkhorseParameter.ENSEMBLE_PER_BURST: 0,
            WorkhorseParameter.SAMPLE_AMBIENT_SOUND: 0,
            WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: '00:00:00',
        },
        DriverStartupConfigKey.SCHEDULER: {
            WorkhorseScheduledJob.GET_CALIBRATION: {},
            WorkhorseScheduledJob.GET_CONFIGURATION: {},
            WorkhorseScheduledJob.CLOCK_SYNC: {}
        }
    }
)


###############################################################################
# DATA PARTICLE TEST MIXIN                          #
#     Defines a set of assert methods used for data particle verification     #
#                                                                             #
#  In python mixin classes are classes designed such that they wouldn't be    #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.
###############################################################################
class ADCPTMixin(DriverTestMixin):
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
        WorkhorseParameter.SERIAL_DATA_OUT:
            {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '000 000 000', VALUE: '000 000 000'},
        WorkhorseParameter.SERIAL_FLOW_CONTROL:
            {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '11110', VALUE: '11110'},
        WorkhorseParameter.SAVE_NVRAM_TO_RECORDER:
            {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        WorkhorseParameter.TIME: {TYPE: str, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        WorkhorseParameter.SERIAL_OUT_FW_SWITCHES:
            {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '111100000', VALUE: '111100000'},
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
        WorkhorseParameter.COORDINATE_TRANSFORMATION:
            {TYPE: str, READONLY: True, DA: True, STARTUP: True, DEFAULT: '00111', VALUE: '00111'},
        WorkhorseParameter.SENSOR_SOURCE:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: "1111101", VALUE: "1111101"},
        WorkhorseParameter.TIME_PER_ENSEMBLE:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: False, VALUE: '00:00:00.00'},
        WorkhorseParameter.TIME_OF_FIRST_PING: {TYPE: str, READONLY: True, DA: False, STARTUP: False, DEFAULT: False},
        WorkhorseParameter.TIME_PER_PING:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: '00:01.00', VALUE: '00:01.00'},
        WorkhorseParameter.FALSE_TARGET_THRESHOLD:
            {TYPE: str, READONLY: False, DA: True, STARTUP: True, DEFAULT: '050,001', VALUE: '050,001'},
        WorkhorseParameter.BANDWIDTH_CONTROL: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                               VALUE: 0},
        WorkhorseParameter.CORRELATION_THRESHOLD: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 64,
                                                   VALUE: 64},
        WorkhorseParameter.ERROR_VELOCITY_THRESHOLD: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                                      DEFAULT: 2000,
                                                      VALUE: 2000},
        WorkhorseParameter.BLANK_AFTER_TRANSMIT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 704,
                                                  VALUE: 704},
        WorkhorseParameter.CLIP_DATA_PAST_BOTTOM: {TYPE: bool, READONLY: False, DA: True, STARTUP: True, DEFAULT: False,
                                                   VALUE: 0},
        WorkhorseParameter.RECEIVER_GAIN_SELECT: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1,
                                                  VALUE: 1},
        WorkhorseParameter.NUMBER_OF_DEPTH_CELLS: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 100,
                                                   VALUE: 100},
        WorkhorseParameter.PINGS_PER_ENSEMBLE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 1,
                                                VALUE: 1},
        WorkhorseParameter.DEPTH_CELL_SIZE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 800,
                                             VALUE: 800},
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
        WorkhorseParameter.TRANSDUCER_DEPTH: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 8000,
                                              VALUE: 8000},
        WorkhorseParameter.DATA_STREAM_SELECTION: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                   VALUE: 0},
        WorkhorseParameter.ENSEMBLE_PER_BURST: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                VALUE: 0},
        WorkhorseParameter.BUFFERED_OUTPUT_PERIOD: {TYPE: str, READONLY: True, DA: True, STARTUP: True,
                                                    DEFAULT: '00:00:00',
                                                    VALUE: '00:00:00'},
        WorkhorseParameter.SAMPLE_AMBIENT_SOUND: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0,
                                                  VALUE: 0},
        # Engineering parameter
        WorkhorseParameter.CLOCK_SYNCH_INTERVAL: {TYPE: str, READONLY: False, DA: False, STARTUP: True,
                                                  DEFAULT: '00:00:00',
                                                  VALUE: '00:00:00'},
        WorkhorseParameter.GET_STATUS_INTERVAL: {TYPE: str, READONLY: False, DA: False, STARTUP: True,
                                                 DEFAULT: '00:00:00',
                                                 VALUE: '00:00:00'}
    }

    _driver_capabilities = {
        # capabilities defined in the IOS
        WorkhorseCapability.START_AUTOSAMPLE: {STATES: [WorkhorseProtocolState.COMMAND]},
        WorkhorseCapability.STOP_AUTOSAMPLE: {STATES: [WorkhorseProtocolState.AUTOSAMPLE]},
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

    _system_configuration_data_parameters = {
        AdcpSystemConfigurationKey.SERIAL_NUMBER: {'type': unicode, 'value': "18444"},
        AdcpSystemConfigurationKey.TRANSDUCER_FREQUENCY: {'type': int, 'value': 76800},
        AdcpSystemConfigurationKey.CONFIGURATION: {'type': unicode, 'value': "4 BEAM, JANUS"},
        AdcpSystemConfigurationKey.MATCH_LAYER: {'type': unicode, 'value': "10"},
        AdcpSystemConfigurationKey.BEAM_ANGLE: {'type': int, 'value': 20},
        AdcpSystemConfigurationKey.BEAM_PATTERN: {'type': unicode, 'value': "CONVEX"},
        AdcpSystemConfigurationKey.ORIENTATION: {'type': unicode, 'value': "UP"},
        AdcpSystemConfigurationKey.SENSORS: {'type': unicode,
                                             'value': "HEADING  TILT 1  TILT 2  DEPTH  TEMPERATURE  PRESSURE"},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_c3: {'type': float, 'value': -1.927850E-11},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_c2: {'type': float, 'value': +1.281892E-06},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_c1: {'type': float, 'value': +1.375793E+00},
        AdcpSystemConfigurationKey.PRESSURE_COEFF_OFFSET: {'type': float, 'value': 13.38634},
        AdcpSystemConfigurationKey.TEMPERATURE_SENSOR_OFFSET: {'type': float, 'value': -0.01},
        AdcpSystemConfigurationKey.CPU_FIRMWARE: {'type': unicode, 'value': "50.40 [0]"},
        AdcpSystemConfigurationKey.BOOT_CODE_REQUIRED: {'type': unicode, 'value': "1.16"},
        AdcpSystemConfigurationKey.BOOT_CODE_ACTUAL: {'type': unicode, 'value': "1.16"},
        AdcpSystemConfigurationKey.DEMOD_1_VERSION: {'type': unicode, 'value': "ad48"},
        AdcpSystemConfigurationKey.DEMOD_1_TYPE: {'type': unicode, 'value': "1f"},
        AdcpSystemConfigurationKey.DEMOD_2_VERSION: {'type': unicode, 'value': "ad48"},
        AdcpSystemConfigurationKey.DEMOD_2_TYPE: {'type': unicode, 'value': "1f"},
        AdcpSystemConfigurationKey.POWER_TIMING_VERSION: {'type': unicode, 'value': "85d3"},
        AdcpSystemConfigurationKey.POWER_TIMING_TYPE: {'type': unicode, 'value': "7"},
        AdcpSystemConfigurationKey.BOARD_SERIAL_NUMBERS: {'type': unicode,
                                                          'value': u"72  00 00 06 FE BC D8  09 HPA727-3009-00B \n" +
                                                                   "81  00 00 06 F5 CD 9E  09 REC727-1004-06A\n" +
                                                                   "A5  00 00 06 FF 1C 79  09 HPI727-3007-00A\n" +
                                                                   "82  00 00 06 FF 23 E5  09 CPU727-2011-00E\n" +
                                                                   "07  00 00 06 F6 05 15  09 TUN727-1005-06A\n" +
                                                                   "DB  00 00 06 F5 CB 5D  09 DSP727-2001-06H"}
    }

    _pd0_parameters_base = {
        AdcpPd0ParsedKey.HEADER_ID: {'type': int, 'value': 127},
        AdcpPd0ParsedKey.DATA_SOURCE_ID: {'type': int, 'value': 127},
        AdcpPd0ParsedKey.FIRMWARE_VERSION: {'type': int, 'value': 50},
        AdcpPd0ParsedKey.FIRMWARE_REVISION: {'type': int, 'value': 40},
        AdcpPd0ParsedKey.SYSCONFIG_FREQUENCY: {'type': int, 'value': 75},
        AdcpPd0ParsedKey.SYSCONFIG_BEAM_PATTERN: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SYSCONFIG_SENSOR_CONFIG: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SYSCONFIG_HEAD_ATTACHED: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.SYSCONFIG_VERTICAL_ORIENTATION: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.DATA_FLAG: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.LAG_LENGTH: {'type': int, 'value': 53},
        AdcpPd0ParsedKey.NUM_BEAMS: {'type': int, 'value': 4},
        AdcpPd0ParsedKey.NUM_CELLS: {'type': int, 'value': 100},
        AdcpPd0ParsedKey.PINGS_PER_ENSEMBLE: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.DEPTH_CELL_LENGTH: {'type': int, 'value': 3200},
        AdcpPd0ParsedKey.BLANK_AFTER_TRANSMIT: {'type': int, 'value': 704},
        AdcpPd0ParsedKey.SIGNAL_PROCESSING_MODE: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.LOW_CORR_THRESHOLD: {'type': int, 'value': 64},
        AdcpPd0ParsedKey.NUM_CODE_REPETITIONS: {'type': int, 'value': 17},
        AdcpPd0ParsedKey.PERCENT_GOOD_MIN: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.ERROR_VEL_THRESHOLD: {'type': int, 'value': 2000},
        AdcpPd0ParsedKey.TIME_PER_PING_MINUTES: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.TIME_PER_PING_SECONDS: {'type': float, 'value': 1.0},
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
        AdcpPd0ParsedKey.BIN_1_DISTANCE: {'type': int, 'value': 4075},
        AdcpPd0ParsedKey.TRANSMIT_PULSE_LENGTH: {'type': int, 'value': 3344},
        AdcpPd0ParsedKey.REFERENCE_LAYER_START: {'type': int, 'value': 1},
        AdcpPd0ParsedKey.REFERENCE_LAYER_STOP: {'type': int, 'value': 5},
        AdcpPd0ParsedKey.FALSE_TARGET_THRESHOLD: {'type': int, 'value': 50},
        AdcpPd0ParsedKey.LOW_LATENCY_TRIGGER: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.TRANSMIT_LAG_DISTANCE: {'type': int, 'value': 198},
        AdcpPd0ParsedKey.CPU_BOARD_SERIAL_NUMBER: {'type': str, 'value': '713015694232387714'},
        AdcpPd0ParsedKey.SYSTEM_BANDWIDTH: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.SYSTEM_POWER: {'type': int, 'value': 255},
        AdcpPd0ParsedKey.SERIAL_NUMBER: {'type': str, 'value': '18444'},
        AdcpPd0ParsedKey.BEAM_ANGLE: {'type': int, 'value': 20},
        AdcpPd0ParsedKey.ENSEMBLE_NUMBER: {'type': int, 'value': 5},
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
        # TODO: These should be removed from the particle
        AdcpPd0ParsedKey.NUM_BYTES: {'type': int, 'value': 2152},
        AdcpPd0ParsedKey.NUM_DATA_TYPES: {'type': int, 'value': 6},
        AdcpPd0ParsedKey.FIXED_LEADER_ID: {'type': int, 'value': 0},
        AdcpPd0ParsedKey.OFFSET_DATA_TYPES: {'type': tuple, 'value': (18, 77, 142, 944, 1346, 1748)},
        AdcpPd0ParsedKey.VARIABLE_LEADER_ID: {'type': int, 'value': 128},
        AdcpPd0ParsedKey.ENSEMBLE_START_TIME: {'type': float, 'value': 3572371982.46},
        AdcpPd0ParsedKey.REAL_TIME_CLOCK: {'type': tuple, 'value': (20, 13, 3, 15, 21, 33, 2, 46)},
        AdcpPd0ParsedKey.VELOCITY_DATA_ID: {'type': int, 'value': 256},
        AdcpPd0ParsedKey.CORRELATION_MAGNITUDE_ID: {'type': int, 'value': 512},
        AdcpPd0ParsedKey.ECHO_INTENSITY_ID: {'type': int, 'value': 768},
        AdcpPd0ParsedKey.PERCENT_GOOD_ID: {'type': int, 'value': 1024},
        AdcpPd0ParsedKey.CHECKSUM: {'type': int, 'value': 8239},
        # TODO: END
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
    }

    beam_parameters = {
        # Beam Coordinates
        AdcpPd0ParsedKey.COORD_TRANSFORM_TYPE: {'type': int, 'value': 0},
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

    earth_parameters = {
        # Earth Coordinates
        AdcpPd0ParsedKey.COORD_TRANSFORM_TYPE: {'type': int, 'value': 3},
        AdcpPd0ParsedKey.WATER_VELOCITY_EAST: beam_parameters[AdcpPd0ParsedKey.BEAM_1_VELOCITY],
        AdcpPd0ParsedKey.WATER_VELOCITY_NORTH: beam_parameters[AdcpPd0ParsedKey.BEAM_2_VELOCITY],
        AdcpPd0ParsedKey.WATER_VELOCITY_UP: beam_parameters[AdcpPd0ParsedKey.BEAM_3_VELOCITY],
        AdcpPd0ParsedKey.ERROR_VELOCITY: beam_parameters[AdcpPd0ParsedKey.BEAM_4_VELOCITY],
        AdcpPd0ParsedKey.PERCENT_GOOD_3BEAM: beam_parameters[AdcpPd0ParsedKey.PERCENT_GOOD_BEAM1],
        AdcpPd0ParsedKey.PERCENT_TRANSFORMS_REJECT: beam_parameters[AdcpPd0ParsedKey.PERCENT_GOOD_BEAM2],
        AdcpPd0ParsedKey.PERCENT_BAD_BEAMS: beam_parameters[AdcpPd0ParsedKey.PERCENT_GOOD_BEAM3],
        AdcpPd0ParsedKey.PERCENT_GOOD_4BEAM: beam_parameters[AdcpPd0ParsedKey.PERCENT_GOOD_BEAM4]
    }

    _pd0_parameters = dict(_pd0_parameters_base.items() + beam_parameters.items())
    _pd0_parameters_earth = dict(_pd0_parameters_base.items() + earth_parameters.items())
    _pd0_parameters_earth[AdcpPd0ParsedKey.CHECKSUM] = {'type': int, 'value': 8263}

    _pt2_dict = {
        AdcpAncillarySystemDataKey.ADCP_AMBIENT_CURRENT: {'type': float, 'value': 20.32},
        AdcpAncillarySystemDataKey.ADCP_ATTITUDE_TEMP: {'type': float, 'value': 24.65},
        AdcpAncillarySystemDataKey.ADCP_INTERNAL_MOISTURE: {'type': unicode, 'value': "8F0Ah"}
    }

    _pt4_dict = {
        AdcpTransmitPathKey.ADCP_TRANSIT_CURRENT: {'type': float, 'value': 2.0},
        AdcpTransmitPathKey.ADCP_TRANSIT_VOLTAGE: {'type': float, 'value': 60.1},
        AdcpTransmitPathKey.ADCP_TRANSIT_IMPEDANCE: {'type': float, 'value': 29.8},
        AdcpTransmitPathKey.ADCP_TRANSIT_TEST_RESULT: {'type': unicode, 'value': "$0 ... PASS"},
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
        self.assert_parameters(current_parameters, self._driver_parameters, verify_values)

    ###
    # Data Particle Parameters Methods
    ###
    def assert_sample_data_particle(self, data_particle):
        """
        Verify a particle is a know particle to this driver and verify the particle is  correct
        @param data_particle: Data particle of unknown type produced by the driver
        """
        if isinstance(data_particle, WorkhorseDataParticleType.ADCP_PD0_PARSED_BEAM):
            self.assert_particle_pd0_data(data_particle)
        elif isinstance(data_particle, WorkhorseDataParticleType.ADCP_SYSTEM_CONFIGURATION):
            self.assert_particle_system_configuration(data_particle)
        elif isinstance(data_particle, WorkhorseDataParticleType.ADCP_COMPASS_CALIBRATION):
            self.assert_particle_compass_calibration(data_particle)
        else:
            log.error("Unknown Particle Detected: %s" % data_particle)
            self.assertFalse(True)

    def assert_particle_compass_calibration(self, data_particle, verify_values=False):
        """
        Verify an adcp calibration data particle
        @param data_particle: ADCPT_CalibrationDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_COMPASS_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_data_parameters, verify_values)

    def assert_particle_system_configuration(self, data_particle, verify_values=False):
        """
        Verify an adcp fd data particle
        @param data_particle: ADCPT_FDDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_SYSTEM_CONFIGURATION)
        self.assert_data_particle_parameters(data_particle, self._system_configuration_data_parameters, verify_values)

    def assert_particle_pd0_data(self, data_particle, verify_values=False):
        """
        Verify an adcp ps0 data particle
        @param data_particle: ADCP_PS0DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_PD0_PARSED_BEAM)
        self.assert_data_particle_parameters(data_particle, self._pd0_parameters, verify_values)

    def assert_particle_pd0_data_earth(self, data_particle, verify_values=False):
        """
        Verify an adcpt ps0 data particle
        @param data_particle: ADCPT_PS0DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_PD0_PARSED_EARTH, True)
        self.assert_data_particle_parameters(data_particle, self._pd0_parameters_earth, verify_values)

    def assert_particle_pt2_data(self, data_particle, verify_values=False):
        """
        Verify an adcpt pt2 data particle
        @param data_particle: ADCPT_PT2 DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_ANCILLARY_SYSTEM_DATA)
        self.assert_data_particle_parameters(data_particle, self._pt2_dict, verify_values)

    def assert_particle_pt4_data(self, data_particle, verify_values=False):
        """
        Verify an adcpt pt4 data particle
        @param data_particle: ADCPT_PT4 DataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_TRANSMIT_PATH)
        self.assert_data_particle_parameters(data_particle, self._pt4_dict, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class WorkhorseDriverUnitTest(InstrumentDriverUnitTestCase, ADCPTMixin):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Create a earth-transform particle from our beam transform data
        rsn_sample_raw_data_earth = bytearray(copy.copy(RSN_SAMPLE_RAW_DATA))
        rsn_sample_raw_data_earth[43] = 24  # set the transform type to 3
        rsn_sample_raw_data_earth[-2] = 71  # checksum
        rsn_sample_raw_data_earth[-1] = 32  # checksum
        rsn_sample_raw_data_earth = str(rsn_sample_raw_data_earth)

        self.assert_particle_published(driver, RSN_CALIBRATION_RAW_DATA, self.assert_particle_compass_calibration, True)
        self.assert_particle_published(driver, RSN_PS0_RAW_DATA, self.assert_particle_system_configuration, True)
        self.assert_particle_published(driver, RSN_SAMPLE_RAW_DATA, self.assert_particle_pd0_data, True)
        self.assert_particle_published(driver, rsn_sample_raw_data_earth, self.assert_particle_pd0_data_earth, True)
        self.assert_particle_published(driver, PT2_RAW_DATA, self.assert_particle_pt2_data, True)
        self.assert_particle_published(driver, PT4_RAW_DATA, self.assert_particle_pt4_data, True)

    def test_driver_parameters(self):
        """
        Verify the set of parameters known by the driver
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, WorkhorseProtocolState.COMMAND)

        expected_parameters = sorted(self._driver_parameters.keys())
        reported_parameters = sorted(driver.get_resource(WorkhorseParameter.ALL))

        log.debug("*** Expected Parameters: %s" % expected_parameters)
        log.debug("*** Reported Parameters: %s" % reported_parameters)

        self.assertEqual(reported_parameters, expected_parameters)

        # Verify the parameter definitions
        self.assert_driver_parameter_definition(driver, self._driver_parameters)

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected.  All states defined in this dict must
        also be defined in the protocol FSM.
        """

        capabilities = {
            WorkhorseProtocolState.UNKNOWN: ['DRIVER_EVENT_DISCOVER'],
            WorkhorseProtocolState.COMMAND: ['DRIVER_EVENT_CLOCK_SYNC',
                                             'DRIVER_EVENT_GET',
                                             'DRIVER_EVENT_SET',
                                             'DRIVER_EVENT_START_AUTOSAMPLE',
                                             'DRIVER_EVENT_START_DIRECT',
                                             'DRIVER_EVENT_ACQUIRE_STATUS',
                                             'DRIVER_EVENT_RUN_TEST',
                                             'PROTOCOL_EVENT_SCHEDULED_CLOCK_SYNC',
                                             'PROTOCOL_EVENT_SCHEDULED_GET_STATUS'],
            WorkhorseProtocolState.AUTOSAMPLE: ['DRIVER_EVENT_STOP_AUTOSAMPLE',
                                                'DRIVER_EVENT_GET',
                                                'PROTOCOL_EVENT_SCHEDULED_CLOCK_SYNC',
                                                'PROTOCOL_EVENT_SCHEDULED_GET_STATUS'],
            WorkhorseProtocolState.DIRECT_ACCESS: ['DRIVER_EVENT_STOP_DIRECT',
                                                   'EXECUTE_DIRECT'
                                                   ]
        }
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """

        self.assert_enum_has_no_duplicates(WorkhorseInstrumentCmds())
        self.assert_enum_has_no_duplicates(WorkhorseProtocolState())
        self.assert_enum_has_no_duplicates(WorkhorseProtocolEvent())
        self.assert_enum_has_no_duplicates(WorkhorseParameter())
        self.assert_enum_has_no_duplicates(WorkhorseDataParticleType())
        self.assert_enum_has_no_duplicates(WorkhorseScheduledJob())
        # Test capabilities for duplicates, then verify that capabilities is a subset of protocol events
        self.assert_enum_has_no_duplicates(WorkhorseCapability())
        self.assert_enum_complete(WorkhorseCapability(), WorkhorseProtocolEvent())

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(WorkhorseProtocol.sieve_function)

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
        protocol = WorkhorseProtocol(WorkhorsePrompt, NEWLINE, my_event_callback)
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
class WorkhorseDriverIntegrationTest(InstrumentDriverIntegrationTestCase, ADCPTMixin):
    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

    def test_startup_driver(self):
        self.assert_initialize_driver()

    def test_parameters(self):
        """
        Test driver parameters and verify their type.  Startup parameters also verify the parameter
        value.  This test confirms that parameters are being read/converted properly and that
        the startup has been applied.
        """
        self.assert_initialize_driver()
        reply = self.driver_client.cmd_dvr('get_resource', WorkhorseParameter.ALL)
        self.assert_driver_parameters(reply, True)

    def test_bulk_set(self):
        self.assert_initialize_driver()
        params = {
            WorkhorseParameter.XMIT_POWER: 128,
            WorkhorseParameter.SPEED_OF_SOUND: 1500,
            WorkhorseParameter.PITCH: 5,
            WorkhorseParameter.ROLL: 5,
            WorkhorseParameter.SALINITY: 36,
            WorkhorseParameter.FALSE_TARGET_THRESHOLD: '051,001',
            WorkhorseParameter.BANDWIDTH_CONTROL: 1,
            WorkhorseParameter.BLANK_AFTER_TRANSMIT: 88,
            WorkhorseParameter.TRANSMIT_LENGTH: 1,
            WorkhorseParameter.PING_WEIGHT: 1,
            WorkhorseParameter.AMBIGUITY_VELOCITY: 176,
            WorkhorseParameter.TRANSDUCER_DEPTH: 8001,
        }
        self.assert_set_bulk(params)

    def test_acquire_status(self):
        self.assert_initialize_driver()
        self.assert_driver_command(WorkhorseProtocolEvent.ACQUIRE_STATUS)
        self.assert_acquire_status()

    def test_autosample_particle_generation(self):
        self.assert_initialize_driver()
        self.assert_driver_command(WorkhorseProtocolEvent.START_AUTOSAMPLE, state=WorkhorseProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_async_particle_generation(WorkhorseDataParticleType.ADCP_PD0_PARSED_BEAM,
                                              self.assert_particle_pd0_data,
                                              timeout=40)
        self.assert_driver_command(WorkhorseProtocolEvent.STOP_AUTOSAMPLE, state=WorkhorseProtocolState.COMMAND,
                                   delay=10)

    def test_scheduled_interval_clock_sync_command(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        self.assert_initialize_driver()
        self.assert_set(WorkhorseParameter.CLOCK_SYNCH_INTERVAL, '00:00:04')
        time.sleep(10)

        self.assert_set(WorkhorseParameter.CLOCK_SYNCH_INTERVAL, '00:00:00')
        self.assert_current_state(WorkhorseProtocolState.COMMAND)

    def test_scheduled_interval_acquire_status_command(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        self.assert_initialize_driver()
        self.assert_set(WorkhorseParameter.GET_STATUS_INTERVAL, '00:00:04')
        # clear the event list
        self.events = []
        # sleep just longer than the status interval
        time.sleep(6)
        # assert we get the desired status particles
        self.assert_acquire_status()

        self.assert_set(WorkhorseParameter.GET_STATUS_INTERVAL, '00:00:00')

        # clear the event list
        self.events = []
        # sleep just longer than the status interval
        time.sleep(6)

        failed = False
        try:
            # assert that we get the desired status particles
            # this should raise an assertion error
            self.assert_acquire_status()
            failed = True
        except AssertionError:
            pass
        self.assertFalse(failed)

    def test_scheduled_acquire_status_autosample(self):
        """
        Verify the scheduled acquire status is triggered and functions as expected
        """

        self.assert_initialize_driver()
        self.assert_current_state(WorkhorseProtocolState.COMMAND)
        self.assert_set(WorkhorseParameter.GET_STATUS_INTERVAL, '00:00:04')
        self.assert_driver_command(WorkhorseProtocolEvent.START_AUTOSAMPLE)
        self.assert_current_state(WorkhorseProtocolState.AUTOSAMPLE)
        time.sleep(6)
        self.assert_acquire_status()
        self.assert_driver_command(WorkhorseProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_current_state(WorkhorseProtocolState.COMMAND)
        self.assert_set(WorkhorseParameter.GET_STATUS_INTERVAL, '00:00:00')
        self.assert_current_state(WorkhorseProtocolState.COMMAND)

    def test_scheduled_clock_sync_autosample(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected

        NOTE: this test requires review of the log to verify clock sync was
        executed in autosample
        """

        self.assert_initialize_driver()
        self.assert_current_state(WorkhorseProtocolState.COMMAND)
        self.assert_set(WorkhorseParameter.CLOCK_SYNCH_INTERVAL, '00:00:04')
        self.assert_driver_command(WorkhorseProtocolEvent.START_AUTOSAMPLE)
        self.assert_current_state(WorkhorseProtocolState.AUTOSAMPLE)
        time.sleep(20)
        self.assert_driver_command(WorkhorseProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_current_state(WorkhorseProtocolState.COMMAND)
        self.assert_set(WorkhorseParameter.CLOCK_SYNCH_INTERVAL, '00:00:00')
        self.assert_current_state(WorkhorseProtocolState.COMMAND)

    def test_set_read_only(self):
        self.assert_initialize_driver()

        self.assert_set_exception(WorkhorseParameter.HEADING_ALIGNMENT, 10000)
        self.assert_set_exception(WorkhorseParameter.HEADING_ALIGNMENT, 40000)
        self.assert_set_exception(WorkhorseParameter.ENSEMBLE_PER_BURST, 600)
        self.assert_set_exception(WorkhorseParameter.ENSEMBLE_PER_BURST, 70000)
        self.assert_set_exception(WorkhorseParameter.LATENCY_TRIGGER, 1)
        self.assert_set_exception(WorkhorseParameter.DATA_STREAM_SELECTION, 10)
        self.assert_set_exception(WorkhorseParameter.DATA_STREAM_SELECTION, 19)
        self.assert_set_exception(WorkhorseParameter.BUFFERED_OUTPUT_PERIOD, "00:00:11")

    def test_break(self):
        self.assert_initialize_driver()
        self.assert_driver_command(WorkhorseProtocolEvent.START_AUTOSAMPLE,
                                   state=WorkhorseProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_driver_command(WorkhorseProtocolEvent.STOP_AUTOSAMPLE,
                                   state=WorkhorseProtocolState.COMMAND,
                                   delay=1)

    def test_commands(self):
        """
        Run instrument commands from both command and streaming mode.
        """
        self.assert_initialize_driver()
        ####
        # First test in command mode
        ####

        self.assert_driver_command(WorkhorseProtocolEvent.START_AUTOSAMPLE,
                                   state=WorkhorseProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_driver_command(WorkhorseProtocolEvent.STOP_AUTOSAMPLE,
                                   state=WorkhorseProtocolState.COMMAND,
                                   delay=1)
        self.assert_driver_command(WorkhorseProtocolEvent.CLOCK_SYNC)
        self.assert_driver_command(WorkhorseProtocolEvent.SCHEDULED_CLOCK_SYNC)
        self.assert_driver_command(WorkhorseProtocolEvent.RUN_TEST, regex='^  Ambient  Temperature =')
        self.assert_driver_command(WorkhorseProtocolEvent.ACQUIRE_STATUS)

        ####
        # Test in streaming mode
        ####
        # Put us in streaming
        self.assert_driver_command(WorkhorseProtocolEvent.START_AUTOSAMPLE,
                                   state=WorkhorseProtocolState.AUTOSAMPLE,
                                   delay=1)
        self.assert_driver_command(WorkhorseProtocolEvent.SCHEDULED_CLOCK_SYNC)
        self.assert_driver_command(WorkhorseProtocolEvent.STOP_AUTOSAMPLE,
                                   state=WorkhorseProtocolState.COMMAND,
                                   delay=1)

    def test_startup_params(self):
        """
        Verify that startup parameters are applied correctly. Generally this
        happens in the driver discovery method.

        since nose orders the tests by ascii value this should run first.
        """
        self.assert_initialize_driver()

        new_values = {
            WorkhorseParameter.PITCH: 1,
            WorkhorseParameter.ROLL: 1
        }

        self.assert_startup_parameters(self.assert_driver_parameters, new_values)

    # This will be called by test_set_range()
    def test_set_range_xmit_power(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # XMIT_POWER:  -- Int 0-255
        self.assert_set(WorkhorseParameter.XMIT_POWER, 0)
        self.assert_set(WorkhorseParameter.XMIT_POWER, 128)
        self.assert_set(WorkhorseParameter.XMIT_POWER, 254)

        self.assert_set_exception(WorkhorseParameter.XMIT_POWER, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.XMIT_POWER, 256)
        self.assert_set_exception(WorkhorseParameter.XMIT_POWER, -1)
        self.assert_set_exception(WorkhorseParameter.XMIT_POWER, 3.1415926)
        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.XMIT_POWER,
                        self._driver_parameters[WorkhorseParameter.XMIT_POWER][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_speed_of_sound(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # SPEED_OF_SOUND:  -- Int 1485 (1400 - 1600)
        self.assert_set(WorkhorseParameter.SPEED_OF_SOUND, 1400)
        self.assert_set(WorkhorseParameter.SPEED_OF_SOUND, 1450)
        self.assert_set(WorkhorseParameter.SPEED_OF_SOUND, 1500)
        self.assert_set(WorkhorseParameter.SPEED_OF_SOUND, 1550)
        self.assert_set(WorkhorseParameter.SPEED_OF_SOUND, 1600)

        self.assert_set_exception(WorkhorseParameter.SPEED_OF_SOUND, 0)
        self.assert_set_exception(WorkhorseParameter.SPEED_OF_SOUND, 1399)

        self.assert_set_exception(WorkhorseParameter.SPEED_OF_SOUND, 1601)
        self.assert_set_exception(WorkhorseParameter.SPEED_OF_SOUND, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.SPEED_OF_SOUND, -256)
        self.assert_set_exception(WorkhorseParameter.SPEED_OF_SOUND, -1)
        self.assert_set_exception(WorkhorseParameter.SPEED_OF_SOUND, 3.1415926)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.SPEED_OF_SOUND,
                        self._driver_parameters[WorkhorseParameter.SPEED_OF_SOUND][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_salinity(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # SALINITY:  -- Int (0 - 40)
        self.assert_set(WorkhorseParameter.SALINITY, 1)
        self.assert_set(WorkhorseParameter.SALINITY, 10)
        self.assert_set(WorkhorseParameter.SALINITY, 20)
        self.assert_set(WorkhorseParameter.SALINITY, 30)
        self.assert_set(WorkhorseParameter.SALINITY, 40)

        self.assert_set_exception(WorkhorseParameter.SALINITY, "LEROY JENKINS")

        # AssertionError: Unexpected exception: ES no value match (40 != -1)
        self.assert_set_exception(WorkhorseParameter.SALINITY, -1)

        # AssertionError: Unexpected exception: ES no value match (35 != 41)
        self.assert_set_exception(WorkhorseParameter.SALINITY, 41)

        self.assert_set_exception(WorkhorseParameter.SALINITY, 3.1415926)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.SALINITY, self._driver_parameters[WorkhorseParameter.SALINITY][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_sensor_source(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # SENSOR_SOURCE:  -- (0/1) for 7 positions.
        # note it lacks capability to have a 1 in the #6 position
        self.assert_set(WorkhorseParameter.SENSOR_SOURCE, "0000000")
        self.assert_set(WorkhorseParameter.SENSOR_SOURCE, "1111101")
        self.assert_set(WorkhorseParameter.SENSOR_SOURCE, "1010101")
        self.assert_set(WorkhorseParameter.SENSOR_SOURCE, "0101000")
        self.assert_set(WorkhorseParameter.SENSOR_SOURCE, "1100100")

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.SENSOR_SOURCE, "1111101")

        self.assert_set_exception(WorkhorseParameter.SENSOR_SOURCE, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.SENSOR_SOURCE, 2)
        self.assert_set_exception(WorkhorseParameter.SENSOR_SOURCE, -1)
        self.assert_set_exception(WorkhorseParameter.SENSOR_SOURCE, "1111112")
        self.assert_set_exception(WorkhorseParameter.SENSOR_SOURCE, "11111112")
        self.assert_set_exception(WorkhorseParameter.SENSOR_SOURCE, 3.1415926)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.SENSOR_SOURCE,
                        self._driver_parameters[WorkhorseParameter.SENSOR_SOURCE][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_time_per_ensemble(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # TIME_PER_ENSEMBLE:  -- String 01:00:00.00 (hrs:min:sec.sec/100)
        self.assert_set(WorkhorseParameter.TIME_PER_ENSEMBLE, "00:00:00.00")
        self.assert_set(WorkhorseParameter.TIME_PER_ENSEMBLE, "00:00:01.00")
        self.assert_set(WorkhorseParameter.TIME_PER_ENSEMBLE, "00:01:00.00")

        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, '30:30:30.30')
        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, '59:59:59.99')
        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, 2)
        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, -1)
        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, '99:99:99.99')
        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, '-1:-1:-1.+1')
        self.assert_set_exception(WorkhorseParameter.TIME_PER_ENSEMBLE, 3.1415926)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.TIME_PER_ENSEMBLE,
                        self._driver_parameters[WorkhorseParameter.TIME_PER_ENSEMBLE][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_pitch(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # PITCH:  -- Int -6000 to 6000
        self.assert_set(WorkhorseParameter.PITCH, -6000)
        self.assert_set(WorkhorseParameter.PITCH, -4000)
        self.assert_set(WorkhorseParameter.PITCH, -2000)
        self.assert_set(WorkhorseParameter.PITCH, -1)
        self.assert_set(WorkhorseParameter.PITCH, 0)
        self.assert_set(WorkhorseParameter.PITCH, 1)
        self.assert_set(WorkhorseParameter.PITCH, 2000)
        self.assert_set(WorkhorseParameter.PITCH, 4000)
        self.assert_set(WorkhorseParameter.PITCH, 6000)

        self.assert_set_exception(WorkhorseParameter.PITCH, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.PITCH, -6001)
        self.assert_set_exception(WorkhorseParameter.PITCH, 6001)
        self.assert_set_exception(WorkhorseParameter.PITCH, 3.1415926)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.PITCH, self._driver_parameters[WorkhorseParameter.PITCH][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_roll(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # ROLL:  -- Int -6000 to 6000
        self.assert_set(WorkhorseParameter.ROLL, -6000)
        self.assert_set(WorkhorseParameter.ROLL, -4000)
        self.assert_set(WorkhorseParameter.ROLL, -2000)
        self.assert_set(WorkhorseParameter.ROLL, -1)
        self.assert_set(WorkhorseParameter.ROLL, 0)
        self.assert_set(WorkhorseParameter.ROLL, 1)
        self.assert_set(WorkhorseParameter.ROLL, 2000)
        self.assert_set(WorkhorseParameter.ROLL, 4000)
        self.assert_set(WorkhorseParameter.ROLL, 6000)

        self.assert_set_exception(WorkhorseParameter.ROLL, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.ROLL, -6001)
        self.assert_set_exception(WorkhorseParameter.ROLL, 6001)
        self.assert_set_exception(WorkhorseParameter.ROLL, 3.1415926)
        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.ROLL, self._driver_parameters[WorkhorseParameter.ROLL][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_time_per_ping(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # TIME_PER_PING: '00:01.00'
        self.assert_set(WorkhorseParameter.TIME_PER_PING, '01:00.00')
        self.assert_set(WorkhorseParameter.TIME_PER_PING, '59:59.99')
        self.assert_set(WorkhorseParameter.TIME_PER_PING, '30:30.30')

        self.assert_set_exception(WorkhorseParameter.TIME_PER_PING, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.TIME_PER_PING, 2)
        self.assert_set_exception(WorkhorseParameter.TIME_PER_PING, -1)
        self.assert_set_exception(WorkhorseParameter.TIME_PER_PING, '99:99.99')
        self.assert_set_exception(WorkhorseParameter.TIME_PER_PING, '-1:-1.+1')
        self.assert_set_exception(WorkhorseParameter.TIME_PER_PING, 3.1415926)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.TIME_PER_PING,
                        self._driver_parameters[WorkhorseParameter.TIME_PER_PING][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_false_target_threshold(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # FALSE_TARGET_THRESHOLD: string of 0-255,0-255
        self.assert_set(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "000,000")
        self.assert_set(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "255,000")
        self.assert_set(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "000,255")
        self.assert_set(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "255,255")

        self.assert_set_exception(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "256,000")
        self.assert_set_exception(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "256,255")
        self.assert_set_exception(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "000,256")
        self.assert_set_exception(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "255,256")
        self.assert_set_exception(WorkhorseParameter.FALSE_TARGET_THRESHOLD, -1)

        self.assert_set_exception(WorkhorseParameter.FALSE_TARGET_THRESHOLD, "LEROY JENKINS")

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.FALSE_TARGET_THRESHOLD,
                        self._driver_parameters[WorkhorseParameter.FALSE_TARGET_THRESHOLD][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_bandwidth_control(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # BANDWIDTH_CONTROL: 0/1,
        self.assert_set(WorkhorseParameter.BANDWIDTH_CONTROL, 1)

        self.assert_set_exception(WorkhorseParameter.BANDWIDTH_CONTROL, -1)
        self.assert_set_exception(WorkhorseParameter.BANDWIDTH_CONTROL, 2)
        self.assert_set_exception(WorkhorseParameter.BANDWIDTH_CONTROL, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.BANDWIDTH_CONTROL, 3.1415926)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.BANDWIDTH_CONTROL,
                        self._driver_parameters[WorkhorseParameter.BANDWIDTH_CONTROL][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_correlation_threshold(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # CORRELATION_THRESHOLD: int 064, 0 - 255
        self.assert_set(WorkhorseParameter.CORRELATION_THRESHOLD, 50)
        self.assert_set(WorkhorseParameter.CORRELATION_THRESHOLD, 100)
        self.assert_set(WorkhorseParameter.CORRELATION_THRESHOLD, 150)
        self.assert_set(WorkhorseParameter.CORRELATION_THRESHOLD, 200)
        self.assert_set(WorkhorseParameter.CORRELATION_THRESHOLD, 255)

        self.assert_set_exception(WorkhorseParameter.CORRELATION_THRESHOLD, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.CORRELATION_THRESHOLD, -256)
        self.assert_set_exception(WorkhorseParameter.CORRELATION_THRESHOLD, -1)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.CORRELATION_THRESHOLD,
                        self._driver_parameters[WorkhorseParameter.CORRELATION_THRESHOLD][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_error_velocity_threshold(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # ERROR_VELOCITY_THRESHOLD: int (0-5000 mm/s) NOTE it enforces 0-9999
        # decimals are truncated to ints
        self.assert_set(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, 0)
        self.assert_set(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, 128)
        self.assert_set(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, 2000)
        self.assert_set(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, 5000)

        self.assert_set_exception(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, -1)
        self.assert_set_exception(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, 10000)
        self.assert_set_exception(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, -3.1415926)
        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD,
                        self._driver_parameters[WorkhorseParameter.ERROR_VELOCITY_THRESHOLD][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_blank_after_transmit(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # BLANK_AFTER_TRANSMIT: int 704, (0 - 9999)
        self.assert_set(WorkhorseParameter.BLANK_AFTER_TRANSMIT, 0)
        self.assert_set(WorkhorseParameter.BLANK_AFTER_TRANSMIT, 128)
        self.assert_set(WorkhorseParameter.BLANK_AFTER_TRANSMIT, 9999)

        self.assert_set_exception(WorkhorseParameter.BLANK_AFTER_TRANSMIT, "LEROY JENKINS")
        self.assert_set_exception(WorkhorseParameter.BLANK_AFTER_TRANSMIT, -1)
        self.assert_set_exception(WorkhorseParameter.BLANK_AFTER_TRANSMIT, 10000)
        self.assert_set_exception(WorkhorseParameter.BLANK_AFTER_TRANSMIT, -3.1415926)
        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.BLANK_AFTER_TRANSMIT,
                        self._driver_parameters[WorkhorseParameter.BLANK_AFTER_TRANSMIT][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_clip_data_past_bottom(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # CLIP_DATA_PAST_BOTTOM: True/False,
        self.assert_set(WorkhorseParameter.CLIP_DATA_PAST_BOTTOM, True)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.CLIP_DATA_PAST_BOTTOM,
                        self._driver_parameters[WorkhorseParameter.CLIP_DATA_PAST_BOTTOM][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_receiver_gain_select(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # RECEIVER_GAIN_SELECT: (0/1),
        self.assert_set(WorkhorseParameter.RECEIVER_GAIN_SELECT, 0)
        self.assert_set(WorkhorseParameter.RECEIVER_GAIN_SELECT, 1)

        self.assert_set_exception(WorkhorseParameter.RECEIVER_GAIN_SELECT, 2)
        self.assert_set_exception(WorkhorseParameter.RECEIVER_GAIN_SELECT, -1)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.RECEIVER_GAIN_SELECT,
                        self._driver_parameters[WorkhorseParameter.RECEIVER_GAIN_SELECT][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_number_of_depth_cells(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # NUMBER_OF_DEPTH_CELLS:  -- int (1-255) 100,
        self.assert_set(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, 1)
        self.assert_set(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, 128)

        self.assert_set_exception(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, 256)
        self.assert_set_exception(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, 0)
        self.assert_set_exception(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, -1)

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS,
                        self._driver_parameters[WorkhorseParameter.NUMBER_OF_DEPTH_CELLS][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_pings_per_ensemble(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # PINGS_PER_ENSEMBLE: -- int  (0-16384) 1,
        self.assert_set(WorkhorseParameter.PINGS_PER_ENSEMBLE, 0)
        self.assert_set(WorkhorseParameter.PINGS_PER_ENSEMBLE, 16384)

        self.assert_set_exception(WorkhorseParameter.PINGS_PER_ENSEMBLE, 16385)
        self.assert_set_exception(WorkhorseParameter.PINGS_PER_ENSEMBLE, -1)
        self.assert_set_exception(WorkhorseParameter.PINGS_PER_ENSEMBLE, 32767)
        self.assert_set_exception(WorkhorseParameter.PINGS_PER_ENSEMBLE, "LEROY JENKINS")
        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.PINGS_PER_ENSEMBLE,
                        self._driver_parameters[WorkhorseParameter.PINGS_PER_ENSEMBLE][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_depth_cell_size(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # DEPTH_CELL_SIZE: int 80 - 3200
        self.assert_set(WorkhorseParameter.DEPTH_CELL_SIZE, 80)

        self.assert_set_exception(WorkhorseParameter.DEPTH_CELL_SIZE, 3201)
        self.assert_set_exception(WorkhorseParameter.DEPTH_CELL_SIZE, -1)
        self.assert_set_exception(WorkhorseParameter.DEPTH_CELL_SIZE, 2)
        self.assert_set_exception(WorkhorseParameter.DEPTH_CELL_SIZE, 3.1415926)
        self.assert_set_exception(WorkhorseParameter.DEPTH_CELL_SIZE, "LEROY JENKINS")
        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.DEPTH_CELL_SIZE,
                        self._driver_parameters[WorkhorseParameter.DEPTH_CELL_SIZE][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_transmit_length(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # TRANSMIT_LENGTH: int 0 to 3200
        self.assert_set(WorkhorseParameter.TRANSMIT_LENGTH, 80)
        self.assert_set(WorkhorseParameter.TRANSMIT_LENGTH, 3200)

        self.assert_set_exception(WorkhorseParameter.TRANSMIT_LENGTH, 3201)
        self.assert_set_exception(WorkhorseParameter.TRANSMIT_LENGTH, -1)
        self.assert_set_exception(WorkhorseParameter.TRANSMIT_LENGTH, "LEROY JENKINS")

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.TRANSMIT_LENGTH,
                        self._driver_parameters[WorkhorseParameter.TRANSMIT_LENGTH][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_ping_weight(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # PING_WEIGHT: (0/1),
        self.assert_set(WorkhorseParameter.PING_WEIGHT, 0)
        self.assert_set(WorkhorseParameter.PING_WEIGHT, 1)

        self.assert_set_exception(WorkhorseParameter.PING_WEIGHT, 2)
        self.assert_set_exception(WorkhorseParameter.PING_WEIGHT, -1)
        self.assert_set_exception(WorkhorseParameter.PING_WEIGHT, 3.1415926)
        self.assert_set_exception(WorkhorseParameter.PING_WEIGHT, "LEROY JENKINS")
        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.PING_WEIGHT,
                        self._driver_parameters[WorkhorseParameter.PING_WEIGHT][self.VALUE])

    # This will be called by test_set_range()
    def test_set_range_ambiguity_velocity(self):
        ###
        #   test get set of a variety of parameter ranges
        ###
        self.assert_initialize_driver()
        # AMBIGUITY_VELOCITY: int 2 - 700
        self.assert_set(WorkhorseParameter.AMBIGUITY_VELOCITY, 2)
        self.assert_set(WorkhorseParameter.AMBIGUITY_VELOCITY, 333)
        self.assert_set(WorkhorseParameter.AMBIGUITY_VELOCITY, 700)

        self.assert_set_exception(WorkhorseParameter.AMBIGUITY_VELOCITY, 0)
        self.assert_set_exception(WorkhorseParameter.AMBIGUITY_VELOCITY, 1)
        self.assert_set_exception(WorkhorseParameter.AMBIGUITY_VELOCITY, -1)
        self.assert_set_exception(WorkhorseParameter.AMBIGUITY_VELOCITY, "LEROY JENKINS")

        #
        # Reset to good value.
        #
        self.assert_set(WorkhorseParameter.AMBIGUITY_VELOCITY,
                        self._driver_parameters[WorkhorseParameter.AMBIGUITY_VELOCITY][self.VALUE])

    # HELPERS
    def _is_time_set(self, time_param, expected_time, time_format="%d %b %Y %H:%M:%S", tolerance=5):
        """
        Verify is what we expect it to be within a given tolerance
        @param time_param: driver parameter
        @param expected_time: what the time should be in seconds since unix epoch or formatted time string
        @param time_format: date time format
        @param tolerance: how close to the set time should the get be?
        """
        result_time = self.assert_get(time_param)

        log.debug("RESULT TIME = " + str(result_time))
        log.debug("TIME FORMAT = " + time_format)
        result_time_struct = time.strptime(result_time, time_format)
        converted_time = time.mktime(result_time_struct)

        if isinstance(expected_time, float):
            expected_time_struct = time.localtime(expected_time)
        else:
            expected_time_struct = time.strptime(expected_time, time_format)

        log.debug("Current Time: %s, Expected Time: %s", time.strftime("%d %b %y %H:%M:%S", result_time_struct),
                  time.strftime("%d %b %y %H:%M:%S", expected_time_struct))

        log.debug("Current Time: %s, Expected Time: %s, Tolerance: %s",
                  converted_time, time.mktime(expected_time_struct), tolerance)

        # Verify the clock is set within the tolerance
        return abs(converted_time - time.mktime(expected_time_struct)) <= tolerance

    def assert_acquire_status(self):
        """
        Assert that Acquire_status return the following ASYNC particles
        """
        self.assert_async_particle_generation(WorkhorseDataParticleType.ADCP_COMPASS_CALIBRATION,
                                              self.assert_particle_compass_calibration,
                                              timeout=3)
        self.assert_async_particle_generation(WorkhorseDataParticleType.ADCP_ANCILLARY_SYSTEM_DATA,
                                              self.assert_particle_pt2_data,
                                              timeout=3)
        self.assert_async_particle_generation(WorkhorseDataParticleType.ADCP_TRANSMIT_PATH,
                                              self.assert_particle_pt4_data,
                                              timeout=3)
        self.assert_async_particle_generation(WorkhorseDataParticleType.ADCP_SYSTEM_CONFIGURATION,
                                              self.assert_particle_system_configuration,
                                              timeout=3)

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
# QUALIFICATION TESTS                                                         #
# Device specific qualification tests are for doing final testing of ion      #
# integration.  The generally aren't used for instrument debugging and should #
# be tackled after all unit and integration tests are complete                #
###############################################################################
@attr('QUAL', group='mi')
class WorkhorseDriverQualificationTest(InstrumentDriverQualificationTestCase):
    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)

    def assert_configuration(self, data_particle, verify_values=False):
        """
        Verify assert_compass_calibration particle
        @param data_particle:  ADCP_COMPASS_CALIBRATION data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(AdcpSystemConfigurationKey, self._system_configuration_data_parameters)
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_SYSTEM_CONFIGURATION)
        self.assert_data_particle_parameters(data_particle, self._system_configuration_data_parameters, verify_values)

    def assert_compass_calibration(self, data_particle, verify_values=False):
        """
        Verify assert_compass_calibration particle
        @param data_particle:  ADCP_COMPASS_CALIBRATION data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(AdcpCompassCalibrationKey, self._calibration_data_parameters)
        self.assert_data_particle_header(data_particle, WorkhorseDataParticleType.ADCP_COMPASS_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_data_parameters, verify_values)

    # need to override this because we are slow and dont feel like modifying the base class lightly
    def assert_set_parameter(self, name, value, verify=True):
        """
        verify that parameters are set correctly.  Assumes we are in command mode.
        """
        setparams = {name: value}
        getparams = [name]

        self.instrument_agent_client.set_resource(setparams, timeout=300)

        if verify:
            result = self.instrument_agent_client.get_resource(getparams, timeout=300)
            self.assertEqual(result[name], value)

    def test_direct_access_telnet_mode(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports
         direct access to the physical instrument. (telnet mode)
        """

        self.assert_enter_command_mode()
        self.assert_set_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1487)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)

        self.tcp_client.send_data("%sEC1488%s" % (NEWLINE, NEWLINE))

        self.tcp_client.expect(WorkhorsePrompt.COMMAND)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_enter_command_mode()
        # Direct access is true, it should be set before
        self.assert_get_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1487)

    # Only test when time is sync in startup
    def _test_execute_clock_sync(self):
        """
        Verify we can synchronize the instrument internal clock
        """
        self.assert_enter_command_mode()

        self.assert_execute_resource(WorkhorseProtocolEvent.CLOCK_SYNC)

        # Now verify that at least the date matches
        check_new_params = self.instrument_agent_client.get_resource([WorkhorseParameter.TIME], timeout=45)

        instrument_time = time.mktime(
            time.strptime(check_new_params.get(WorkhorseParameter.TIME).lower(), "%Y/%m/%d,%H:%M:%S %Z"))

        self.assertLessEqual(abs(instrument_time - time.mktime(time.gmtime())), 45)

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
                WorkhorseProtocolEvent.CLOCK_SYNC,
                WorkhorseProtocolEvent.START_AUTOSAMPLE,
                WorkhorseProtocolEvent.RUN_TEST,
                WorkhorseProtocolEvent.ACQUIRE_STATUS,
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
            WorkhorseProtocolEvent.STOP_AUTOSAMPLE,
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

        self.assert_set_parameter(WorkhorseParameter.XMIT_POWER, 250)
        self.assert_set_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1480)
        self.assert_set_parameter(WorkhorseParameter.PITCH, 1)
        self.assert_set_parameter(WorkhorseParameter.ROLL, 1)
        self.assert_set_parameter(WorkhorseParameter.SALINITY, 36)
        self.assert_set_parameter(WorkhorseParameter.TRANSDUCER_DEPTH, 6000, False)
        self.assert_set_parameter(WorkhorseParameter.TRANSDUCER_DEPTH, 0)

        self.assert_set_parameter(WorkhorseParameter.TIME_PER_ENSEMBLE, '00:00:01.00')
        self.assert_set_parameter(WorkhorseParameter.TIME_PER_ENSEMBLE, '01:00:00.00')

        self.assert_set_parameter(WorkhorseParameter.FALSE_TARGET_THRESHOLD, '049,002')
        self.assert_set_parameter(WorkhorseParameter.BANDWIDTH_CONTROL, 1)
        self.assert_set_parameter(WorkhorseParameter.CORRELATION_THRESHOLD, 63)

        self.assert_set_parameter(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, 1999)
        self.assert_set_parameter(WorkhorseParameter.BLANK_AFTER_TRANSMIT, 714)

        self.assert_set_parameter(WorkhorseParameter.CLIP_DATA_PAST_BOTTOM, 1)
        self.assert_set_parameter(WorkhorseParameter.RECEIVER_GAIN_SELECT, 0)
        self.assert_set_parameter(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, 99)
        self.assert_set_parameter(WorkhorseParameter.PINGS_PER_ENSEMBLE, 0)
        self.assert_set_parameter(WorkhorseParameter.DEPTH_CELL_SIZE, 790)

        self.assert_set_parameter(WorkhorseParameter.TRANSMIT_LENGTH, 1)
        self.assert_set_parameter(WorkhorseParameter.PING_WEIGHT, 1)
        self.assert_set_parameter(WorkhorseParameter.AMBIGUITY_VELOCITY, 176)

    def test_startup_params_second_pass(self):
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

        # Change these values anyway just in case it ran first.
        self.assert_set_parameter(WorkhorseParameter.XMIT_POWER, 250)
        self.assert_set_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1480)
        self.assert_set_parameter(WorkhorseParameter.SPEED_OF_SOUND, 1500)
        self.assert_set_parameter(WorkhorseParameter.PITCH, 1)
        self.assert_set_parameter(WorkhorseParameter.ROLL, 1)
        self.assert_set_parameter(WorkhorseParameter.SALINITY, 36)
        self.assert_set_parameter(WorkhorseParameter.TIME_PER_ENSEMBLE, '00:00:01.00')
        self.assert_set_parameter(WorkhorseParameter.TIME_PER_PING, '00:02.00')
        self.assert_set_parameter(WorkhorseParameter.FALSE_TARGET_THRESHOLD, '049,002')
        self.assert_set_parameter(WorkhorseParameter.BANDWIDTH_CONTROL, 1)
        self.assert_set_parameter(WorkhorseParameter.CORRELATION_THRESHOLD, 63)
        self.assert_set_parameter(WorkhorseParameter.ERROR_VELOCITY_THRESHOLD, 1999)
        self.assert_set_parameter(WorkhorseParameter.BLANK_AFTER_TRANSMIT, 714)
        self.assert_set_parameter(WorkhorseParameter.BLANK_AFTER_TRANSMIT, 352)
        self.assert_set_parameter(WorkhorseParameter.CLIP_DATA_PAST_BOTTOM, 1)
        self.assert_set_parameter(WorkhorseParameter.RECEIVER_GAIN_SELECT, 0)
        self.assert_set_parameter(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, 99)
        self.assert_set_parameter(WorkhorseParameter.NUMBER_OF_DEPTH_CELLS, 30)
        self.assert_set_parameter(WorkhorseParameter.PINGS_PER_ENSEMBLE, 0)
        self.assert_set_parameter(WorkhorseParameter.PINGS_PER_ENSEMBLE, 1)
        self.assert_set_parameter(WorkhorseParameter.DEPTH_CELL_SIZE, 790)
        self.assert_set_parameter(WorkhorseParameter.TRANSMIT_LENGTH, 1)
        self.assert_set_parameter(WorkhorseParameter.PING_WEIGHT, 1)
        self.assert_set_parameter(WorkhorseParameter.AMBIGUITY_VELOCITY, 176)
