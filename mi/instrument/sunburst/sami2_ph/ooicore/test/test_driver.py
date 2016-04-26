"""
@package mi.instrument.sunburst.sami2_ph.ooicore.test.test_driver
@file marine-integrations/mi/instrument/sunburst/sami2_ph/ooicore/driver.py
@author Kevin Stiemke
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u [-t testname]
       $ bin/test_driver -i [-t testname]
       $ bin/test_driver -q [-t testname]
"""

__author__ = 'Kevin Stiemke'
__license__ = 'Apache 2.0'

import unittest
import time
import copy

import mock
from mock import Mock
from nose.plugins.attrib import attr
from mi.core.log import get_logger


log = get_logger()

# MI imports.
from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import ParameterTestConfigKey

from mi.idk.unit_test import AgentCapabilityType

from mi.core.instrument.chunker import StringChunker

from mi.core.instrument.instrument_driver import ResourceAgentEvent
from mi.core.instrument.instrument_driver import ResourceAgentState

from mi.instrument.sunburst.driver import Prompt
from mi.instrument.sunburst.driver import SAMI_NEWLINE
from mi.instrument.sunburst.sami2_ph.ooicore.driver import Capability
from mi.instrument.sunburst.sami2_ph.ooicore.driver import DataParticleType
from mi.instrument.sunburst.sami2_ph.ooicore.driver import InstrumentCommand
from mi.instrument.sunburst.sami2_ph.ooicore.driver import InstrumentDriver
from mi.instrument.sunburst.sami2_ph.ooicore.driver import Parameter
from mi.instrument.sunburst.sami2_ph.ooicore.driver import PhsenConfigDataParticleKey
from mi.instrument.sunburst.sami2_ph.ooicore.driver import PhsenSamiSampleDataParticleKey
from mi.instrument.sunburst.sami2_ph.ooicore.driver import ProtocolState
from mi.instrument.sunburst.sami2_ph.ooicore.driver import ProtocolEvent
from mi.instrument.sunburst.sami2_ph.ooicore.driver import Protocol
from mi.instrument.sunburst.test.test_driver import SamiMixin
from mi.instrument.sunburst.test.test_driver import SamiUnitTest
from mi.instrument.sunburst.test.test_driver import SamiIntegrationTest
from mi.instrument.sunburst.test.test_driver import SamiQualificationTest
from mi.instrument.sunburst.test.test_driver import PumpStatisticsContainer
from mi.instrument.sunburst.sami2_ph.ooicore.driver import ScheduledJob

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.sunburst.sami2_ph.ooicore.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='ZY4I90',
    instrument_agent_name='sunburst_sami2_ph_ooicore',
    instrument_agent_packet_config=DataParticleType(),

    driver_startup_config={}
)

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

###
#   Driver constant definitions
###
TYPE = ParameterTestConfigKey.TYPE
READONLY = ParameterTestConfigKey.READONLY
STARTUP = ParameterTestConfigKey.STARTUP
DA = ParameterTestConfigKey.DIRECT_ACCESS
VALUE = ParameterTestConfigKey.VALUE
REQUIRED = ParameterTestConfigKey.REQUIRED
DEFAULT = ParameterTestConfigKey.DEFAULT
STATES = ParameterTestConfigKey.STATES

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


class DriverTestMixinSub(SamiMixin):
    """
    Mixin class used for storing data particle constants and common data
    assertion methods.  Inherits from SAMI Instrument base Mixin class
    """

    ###
    #  Instrument output (driver input) Definitions
    ###
    # Configuration string received from the instrument via the L command
    # (clock set to 2014-01-01 00:00:00) with sampling set to start 540 days
    # (~18 months) later and stop 365 days after that. SAMI is set to run every
    # 60 minutes, but will be polled on a regular schedule rather than
    # autosampled.
    VALID_CONFIG_STRING = 'CDDD731D01E1338001E1338002000E100A0200000000110' + \
                          '0000000110000000011000000001107013704200108081004081008170000' + \
                          '0000000000000000000000000000000000000000000000000000000000000' + \
                          '0000000000000000000000000000000000000000000000000000000000000' + \
                          '00' + \
                          'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + \
                          'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + \
                          'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + \
                          'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + \
                          'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + SAMI_NEWLINE

    # Data records -- SAMI (response to the R or R0 command)
    VALID_DATA_SAMPLE = '*F8E70ACDDE9E4F06350BAA077C06A408040BAD077906A307' + \
                        'FE0BA80778069F08010BAA077C06A208020BAB077E06A208040BAB077906A' + \
                        '008010BAA06F806A107FE0BAE04EC06A707EF0BAF027C06A407E20BAA0126' + \
                        '069E07D60BAF00A806A207D60BAC008906A407DF0BAD009206A207E70BAB0' + \
                        '0C206A207F20BB0011306A707F80BAC019106A208000BAE022D069F08010B' + \
                        'AB02E006A008030BAD039706A308000BAB044706A208000BAA04E906A3080' + \
                        '30BAB056D06A408030BAA05DC069F08010BAF063406A608070BAE067406A2' + \
                        '08000BAC06AB069E07FF0BAD06D506A2080200000D650636CE' + SAMI_NEWLINE

    ###
    #  Parameter and Type Definitions
    ###

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.ACQUIRE_STATUS: {STATES: [ProtocolState.COMMAND,
                                             ProtocolState.AUTOSAMPLE]},
        Capability.ACQUIRE_SAMPLE: {STATES: [ProtocolState.COMMAND]},
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND,
                                               ProtocolState.AUTOSAMPLE]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.AUTOSAMPLE,
                                              ProtocolState.COMMAND]},
        Capability.SEAWATER_FLUSH_2750ML: {STATES: [ProtocolState.COMMAND]},
        Capability.REAGENT_FLUSH_50ML: {STATES: [ProtocolState.COMMAND]},
        Capability.SEAWATER_FLUSH: {STATES: [ProtocolState.COMMAND]},
        Capability.REAGENT_FLUSH: {STATES: [ProtocolState.COMMAND]}
    }

    _driver_parameters = {
        # Parameters defined in the PHSEN IOS. NOTE:these test values are
        # different than the PCO2's:/NOTE
        Parameter.LAUNCH_TIME: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                DEFAULT: 0x00000000, VALUE: 0xCDDD731D},
        Parameter.START_TIME_FROM_LAUNCH: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                           DEFAULT: 0x00000000, VALUE: 0x01E13380},
        Parameter.STOP_TIME_FROM_START: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                         DEFAULT: 0x01E13380, VALUE: 0x01E13380},
        Parameter.MODE_BITS: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                              DEFAULT: 0x02, VALUE: 0x02},
        Parameter.SAMI_SAMPLE_INTERVAL: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                         DEFAULT: 0x000E10, VALUE: 0x000E10},
        Parameter.SAMI_DRIVER_VERSION: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                        DEFAULT: 0x0A, VALUE: 0x0A},
        Parameter.SAMI_PARAMS_POINTER: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                        DEFAULT: 0x02, VALUE: 0x02},
        Parameter.DEVICE1_SAMPLE_INTERVAL: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                            DEFAULT: 0x000000, VALUE: 0x000000},
        Parameter.DEVICE1_DRIVER_VERSION: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                           DEFAULT: 0x00, VALUE: 0x00},
        Parameter.DEVICE1_PARAMS_POINTER: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                           DEFAULT: 0x11, VALUE: 0x11},
        Parameter.DEVICE2_SAMPLE_INTERVAL: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                            DEFAULT: 0x000000, VALUE: 0x000000},
        Parameter.DEVICE2_DRIVER_VERSION: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                           DEFAULT: 0x00, VALUE: 0x00},
        Parameter.DEVICE2_PARAMS_POINTER: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                           DEFAULT: 0x11, VALUE: 0x11},
        Parameter.DEVICE3_SAMPLE_INTERVAL: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                            DEFAULT: 0x000000, VALUE: 0x000000},
        Parameter.DEVICE3_DRIVER_VERSION: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                           DEFAULT: 0x00, VALUE: 0x00},
        Parameter.DEVICE3_PARAMS_POINTER: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                           DEFAULT: 0x11, VALUE: 0x11},
        Parameter.PRESTART_SAMPLE_INTERVAL: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                             DEFAULT: 0x000000, VALUE: 0x000000},
        Parameter.PRESTART_DRIVER_VERSION: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                            DEFAULT: 0x00, VALUE: 0x00},
        Parameter.PRESTART_PARAMS_POINTER: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                            DEFAULT: 0x11, VALUE: 0x11},
        Parameter.GLOBAL_CONFIGURATION: {TYPE: int, READONLY: True, DA: True, STARTUP: True,
                                         DEFAULT: 0x07, VALUE: 0x07},
        Parameter.NUMBER_SAMPLES_AVERAGED: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                            DEFAULT: 0x01, VALUE: 0x01},
        Parameter.NUMBER_FLUSHES: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                   DEFAULT: 0x37, VALUE: 0x37},
        Parameter.PUMP_ON_FLUSH: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                  DEFAULT: 0x04, VALUE: 0x04},
        Parameter.PUMP_OFF_FLUSH: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                   DEFAULT: 0x20, VALUE: 0x20},
        Parameter.NUMBER_REAGENT_PUMPS: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                         DEFAULT: 0x01, VALUE: 0x01},
        Parameter.VALVE_DELAY: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                DEFAULT: 0x08, VALUE: 0x08},
        Parameter.PUMP_ON_IND: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                DEFAULT: 0x08, VALUE: 0x08},
        Parameter.PV_OFF_IND: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                               DEFAULT: 0x10, VALUE: 0x10},
        Parameter.NUMBER_BLANKS: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                  DEFAULT: 0x04, VALUE: 0x04},
        Parameter.PUMP_MEASURE_T: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                   DEFAULT: 0x08, VALUE: 0x08},
        Parameter.PUMP_OFF_TO_MEASURE: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                        DEFAULT: 0x10, VALUE: 0x10},
        Parameter.MEASURE_TO_PUMP_ON: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                       DEFAULT: 0x08, VALUE: 0x08},
        Parameter.NUMBER_MEASUREMENTS: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                        DEFAULT: 0x17, VALUE: 0x17},
        Parameter.SALINITY_DELAY: {TYPE: int, READONLY: False, DA: True, STARTUP: True,
                                   DEFAULT: 0x00, VALUE: 0x00},
        Parameter.AUTO_SAMPLE_INTERVAL: {TYPE: int, READONLY: False, DA: False, STARTUP: False,
                                         DEFAULT: 0x38, VALUE: 3600},
        Parameter.REAGENT_FLUSH_DURATION: {TYPE: int, READONLY: False, DA: False, STARTUP: False,
                                           DEFAULT: 0x04, VALUE: 0x08, REQUIRED: True},
        Parameter.SEAWATER_FLUSH_DURATION: {TYPE: int, READONLY: False, DA: False, STARTUP: False,
                                            DEFAULT: 0x02, VALUE: 0x08, REQUIRED: True},
        Parameter.FLUSH_CYCLES: {TYPE: int, READONLY: False, DA: False, STARTUP: False,
                                 DEFAULT: 0x01, VALUE: 0x01, REQUIRED: True},
    }

    _sami_data_sample_parameters = {
        # SAMI pH sample (type 0x0A)
        PhsenSamiSampleDataParticleKey.UNIQUE_ID: {TYPE: int, VALUE: 0xF8, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.RECORD_LENGTH: {TYPE: int, VALUE: 0xE7, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.RECORD_TYPE: {TYPE: int, VALUE: 0x0A, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.RECORD_TIME: {TYPE: int, VALUE: 0xCDDE9E4F, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.START_THERMISTOR: {TYPE: int, VALUE: 0x0635, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.REF_MEASUREMENTS: {
            TYPE: list, VALUE:
            [0x0BAA, 0x077C, 0x06A4, 0x0804,
             0x0BAD, 0x0779, 0x06A3, 0x07FE,
             0x0BA8, 0x0778, 0x069F, 0x0801,
             0x0BAA, 0x077C, 0x06A2, 0x0802],
            REQUIRED: True},
        PhsenSamiSampleDataParticleKey.PH_MEASUREMENTS: {
            TYPE: list, VALUE:
            [0x0BAB, 0x077E, 0x06A2, 0x0804,
             0x0BAB, 0x0779, 0x06A0, 0x0801,
             0x0BAA, 0x06F8, 0x06A1, 0x07FE,
             0x0BAE, 0x04EC, 0x06A7, 0x07EF,
             0x0BAF, 0x027C, 0x06A4, 0x07E2,
             0x0BAA, 0x0126, 0x069E, 0x07D6,
             0x0BAF, 0x00A8, 0x06A2, 0x07D6,
             0x0BAC, 0x0089, 0x06A4, 0x07DF,
             0x0BAD, 0x0092, 0x06A2, 0x07E7,
             0x0BAB, 0x00C2, 0x06A2, 0x07F2,
             0x0BB0, 0x0113, 0x06A7, 0x07F8,
             0x0BAC, 0x0191, 0x06A2, 0x0800,
             0x0BAE, 0x022D, 0x069F, 0x0801,
             0x0BAB, 0x02E0, 0x06A0, 0x0803,
             0x0BAD, 0x0397, 0x06A3, 0x0800,
             0x0BAB, 0x0447, 0x06A2, 0x0800,
             0x0BAA, 0x04E9, 0x06A3, 0x0803,
             0x0BAB, 0x056D, 0x06A4, 0x0803,
             0x0BAA, 0x05DC, 0x069F, 0x0801,
             0x0BAF, 0x0634, 0x06A6, 0x0807,
             0x0BAE, 0x0674, 0x06A2, 0x0800,
             0x0BAC, 0x06AB, 0x069E, 0x07FF,
             0x0BAD, 0x06D5, 0x06A2, 0x0802],
            REQUIRED: True},
        PhsenSamiSampleDataParticleKey.VOLTAGE_BATTERY: {TYPE: int, VALUE: 0x0D65, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.END_THERMISTOR: {TYPE: int, VALUE: 0x0636, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.CHECKSUM: {TYPE: int, VALUE: 0xCE, REQUIRED: True},
        PhsenSamiSampleDataParticleKey.RESERVED_UNUSED: {TYPE: int, VALUE: 0x00, REQUIRED: False}
    }

    _configuration_parameters = {
        # Configuration settings NOTE:These test values are different than the
        # PCO2's and so are all included here:/NOTE
        PhsenConfigDataParticleKey.LAUNCH_TIME: {TYPE: int, VALUE: 0xCDDD731D, REQUIRED: True},
        PhsenConfigDataParticleKey.START_TIME_OFFSET: {TYPE: int, VALUE: 0x01E13380, REQUIRED: True},
        PhsenConfigDataParticleKey.RECORDING_TIME: {TYPE: int, VALUE: 0x01E13380, REQUIRED: True},
        PhsenConfigDataParticleKey.PMI_SAMPLE_SCHEDULE: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.SAMI_SAMPLE_SCHEDULE: {TYPE: int, VALUE: 1, REQUIRED: True},
        PhsenConfigDataParticleKey.SLOT1_FOLLOWS_SAMI_SCHEDULE: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.SLOT1_INDEPENDENT_SCHEDULE: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.SLOT2_FOLLOWS_SAMI_SCHEDULE: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.SLOT2_INDEPENDENT_SCHEDULE: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.SLOT3_FOLLOWS_SAMI_SCHEDULE: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.SLOT3_INDEPENDENT_SCHEDULE: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.TIMER_INTERVAL_SAMI: {TYPE: int, VALUE: 0x000E10, REQUIRED: True},
        PhsenConfigDataParticleKey.DRIVER_ID_SAMI: {TYPE: int, VALUE: 0x0A, REQUIRED: True},
        PhsenConfigDataParticleKey.PARAMETER_POINTER_SAMI: {TYPE: int, VALUE: 0x02, REQUIRED: True},
        PhsenConfigDataParticleKey.TIMER_INTERVAL_DEVICE1: {TYPE: int, VALUE: 0x000000, REQUIRED: True},
        PhsenConfigDataParticleKey.DRIVER_ID_DEVICE1: {TYPE: int, VALUE: 0x00, REQUIRED: True},
        PhsenConfigDataParticleKey.PARAMETER_POINTER_DEVICE1: {TYPE: int, VALUE: 0x11, REQUIRED: True},
        PhsenConfigDataParticleKey.TIMER_INTERVAL_DEVICE2: {TYPE: int, VALUE: 0x000000, REQUIRED: True},
        PhsenConfigDataParticleKey.DRIVER_ID_DEVICE2: {TYPE: int, VALUE: 0x00, REQUIRED: True},
        PhsenConfigDataParticleKey.PARAMETER_POINTER_DEVICE2: {TYPE: int, VALUE: 0x11, REQUIRED: True},
        PhsenConfigDataParticleKey.TIMER_INTERVAL_DEVICE3: {TYPE: int, VALUE: 0x000000, REQUIRED: True},
        PhsenConfigDataParticleKey.DRIVER_ID_DEVICE3: {TYPE: int, VALUE: 0x00, REQUIRED: True},
        PhsenConfigDataParticleKey.PARAMETER_POINTER_DEVICE3: {TYPE: int, VALUE: 0x11, REQUIRED: True},
        PhsenConfigDataParticleKey.TIMER_INTERVAL_PRESTART: {TYPE: int, VALUE: 0x000000, REQUIRED: True},
        PhsenConfigDataParticleKey.DRIVER_ID_PRESTART: {TYPE: int, VALUE: 0x00, REQUIRED: True},
        PhsenConfigDataParticleKey.PARAMETER_POINTER_PRESTART: {TYPE: int, VALUE: 0x11, REQUIRED: True},
        PhsenConfigDataParticleKey.USE_BAUD_RATE_57600: {TYPE: int, VALUE: 1, REQUIRED: True},
        PhsenConfigDataParticleKey.SEND_RECORD_TYPE: {TYPE: int, VALUE: 1, REQUIRED: True},
        PhsenConfigDataParticleKey.SEND_LIVE_RECORDS: {TYPE: int, VALUE: 1, REQUIRED: True},
        PhsenConfigDataParticleKey.EXTEND_GLOBAL_CONFIG: {TYPE: int, VALUE: 0, REQUIRED: True},
        PhsenConfigDataParticleKey.NUMBER_SAMPLES_AVERAGED: {TYPE: int, VALUE: 0x01, REQUIRED: True},
        PhsenConfigDataParticleKey.NUMBER_FLUSHES: {TYPE: int, VALUE: 0x37, REQUIRED: True},
        PhsenConfigDataParticleKey.PUMP_ON_FLUSH: {TYPE: int, VALUE: 0x04, REQUIRED: True},
        PhsenConfigDataParticleKey.PUMP_OFF_FLUSH: {TYPE: int, VALUE: 0x20, REQUIRED: True},
        PhsenConfigDataParticleKey.NUMBER_REAGENT_PUMPS: {TYPE: int, VALUE: 0x01, REQUIRED: True},
        PhsenConfigDataParticleKey.VALVE_DELAY: {TYPE: int, VALUE: 0x08, REQUIRED: True},
        PhsenConfigDataParticleKey.PUMP_ON_IND: {TYPE: int, VALUE: 0x08, REQUIRED: True},
        PhsenConfigDataParticleKey.PV_OFF_IND: {TYPE: int, VALUE: 0x10, REQUIRED: True},
        PhsenConfigDataParticleKey.NUMBER_BLANKS: {TYPE: int, VALUE: 0x04, REQUIRED: True},
        PhsenConfigDataParticleKey.PUMP_MEASURE_T: {TYPE: int, VALUE: 0x08, REQUIRED: True},
        PhsenConfigDataParticleKey.PUMP_OFF_TO_MEASURE: {TYPE: int, VALUE: 0x10, REQUIRED: True},
        PhsenConfigDataParticleKey.MEASURE_TO_PUMP_ON: {TYPE: int, VALUE: 0x08, REQUIRED: True},
        PhsenConfigDataParticleKey.NUMBER_MEASUREMENTS: {TYPE: int, VALUE: 0x17, REQUIRED: True},
        PhsenConfigDataParticleKey.SALINITY_DELAY: {TYPE: int, VALUE: 0x00, REQUIRED: True}
    }

    def assert_particle_sami_data_sample(self, data_particle, verify_values=False):
        """
        Verify sami_data_sample particle (Type 0A pH)
        @param data_particle: PhsenSamiSampleDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_keys(PhsenSamiSampleDataParticleKey,
                                       self._sami_data_sample_parameters)
        self.assert_data_particle_header(data_particle,
                                         DataParticleType.PHSEN_DATA_RECORD)
        self.assert_data_particle_parameters(data_particle,
                                             self._sami_data_sample_parameters,
                                             verify_values)

    def assert_particle_configuration(self, data_particle, verify_values=False):
        """
        Verify configuration particle
        @param data_particle: PhsenConfigDataParticle data particle
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_keys(PhsenConfigDataParticleKey,
                                       self._configuration_parameters)
        self.assert_data_particle_header(data_particle,
                                         DataParticleType.PHSEN_CONFIGURATION)
        self.assert_data_particle_parameters(data_particle,
                                             self._configuration_parameters,
                                             verify_values)


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
class DriverUnitTest(SamiUnitTest, DriverTestMixinSub):
    capabilities_test_dict = {
        ProtocolState.UNKNOWN: ['DRIVER_EVENT_DISCOVER'],
        ProtocolState.WAITING: ['DRIVER_EVENT_GET'],
        ProtocolState.COMMAND: ['DRIVER_EVENT_GET',
                                'DRIVER_EVENT_SET',
                                'DRIVER_EVENT_START_DIRECT',
                                'DRIVER_EVENT_ACQUIRE_STATUS',
                                'DRIVER_EVENT_ACQUIRE_SAMPLE',
                                'DRIVER_EVENT_START_AUTOSAMPLE',
                                'DRIVER_EVENT_SEAWATER_FLUSH_2750ML',
                                'DRIVER_EVENT_REAGENT_FLUSH_50ML',
                                'DRIVER_EVENT_SEAWATER_FLUSH',
                                'DRIVER_EVENT_REAGENT_FLUSH'],
        ProtocolState.SEAWATER_FLUSH_2750ML: ['PROTOCOL_EVENT_EXECUTE',
                                              'PROTOCOL_EVENT_SUCCESS',
                                              'PROTOCOL_EVENT_TIMEOUT',
                                              'DRIVER_EVENT_ACQUIRE_STATUS'],
        ProtocolState.REAGENT_FLUSH_50ML: ['PROTOCOL_EVENT_EXECUTE',
                                           'PROTOCOL_EVENT_SUCCESS',
                                           'PROTOCOL_EVENT_TIMEOUT',
                                           'DRIVER_EVENT_ACQUIRE_STATUS'],
        ProtocolState.SEAWATER_FLUSH: ['PROTOCOL_EVENT_EXECUTE',
                                       'PROTOCOL_EVENT_SUCCESS',
                                       'PROTOCOL_EVENT_TIMEOUT',
                                       'DRIVER_EVENT_ACQUIRE_STATUS'],
        ProtocolState.REAGENT_FLUSH: ['PROTOCOL_EVENT_EXECUTE',
                                      'PROTOCOL_EVENT_SUCCESS',
                                      'PROTOCOL_EVENT_TIMEOUT',
                                      'DRIVER_EVENT_ACQUIRE_STATUS'],
        ProtocolState.AUTOSAMPLE: ['DRIVER_EVENT_ACQUIRE_SAMPLE',
                                   'DRIVER_EVENT_STOP_AUTOSAMPLE',
                                   'DRIVER_EVENT_ACQUIRE_STATUS'],
        ProtocolState.DIRECT_ACCESS: ['EXECUTE_DIRECT',
                                      'DRIVER_EVENT_STOP_DIRECT'],
        ProtocolState.POLLED_SAMPLE: ['PROTOCOL_EVENT_EXECUTE',
                                      'PROTOCOL_EVENT_SUCCESS',
                                      'PROTOCOL_EVENT_TIMEOUT',
                                      'DRIVER_EVENT_ACQUIRE_STATUS'],
        ProtocolState.SCHEDULED_SAMPLE: ['PROTOCOL_EVENT_EXECUTE',
                                         'PROTOCOL_EVENT_SUCCESS',
                                         'PROTOCOL_EVENT_TIMEOUT',
                                         'DRIVER_EVENT_ACQUIRE_STATUS'],
    }

    def test_base_driver_enums(self):
        """
        Verify that all the SAMI Instrument driver enumerations have no
        duplicate values that might cause confusion. Also do a little
        extra validation for the Capabilities

        Extra enumeration tests are done in a specific subclass
        """

        # Test Enums defined in the base SAMI driver
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())

        # Test capabilities for duplicates, then verify that capabilities
        # is a subset of proto events

        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)

    def test_ph_driver_enums(self):
        """
        Verify that all the PH driver enumerations have no duplicate values
        that might cause confusion.
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(InstrumentCommand())

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(Protocol.sieve_function)

        for part in [self.VALID_STATUS_MESSAGE, self.VALID_DATA_SAMPLE, self.VALID_CONFIG_STRING]:
            self.assert_chunker_sample(chunker, part)
            self.assert_chunker_sample_with_noise(chunker, part)
            self.assert_chunker_fragmented_sample(chunker, part)
            self.assert_chunker_combined_sample(chunker, part)

        self.assert_chunker_sample(chunker, self.VALID_STATUS_MESSAGE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_STATUS_MESSAGE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_STATUS_MESSAGE)
        self.assert_chunker_combined_sample(chunker, self.VALID_STATUS_MESSAGE)

        self.assert_chunker_sample(chunker, self.VALID_DATA_SAMPLE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_DATA_SAMPLE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_DATA_SAMPLE)
        self.assert_chunker_combined_sample(chunker, self.VALID_DATA_SAMPLE)

        self.assert_chunker_sample(chunker, self.VALID_CONFIG_STRING)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_CONFIG_STRING)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_CONFIG_STRING)
        self.assert_chunker_combined_sample(chunker, self.VALID_CONFIG_STRING)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(
            driver, self.VALID_STATUS_MESSAGE, self.assert_particle_regular_status, True)
        self.assert_particle_published(
            driver, self.VALID_DATA_SAMPLE, self.assert_particle_sami_data_sample, True)
        self.assert_particle_published(
            driver, self.VALID_CONFIG_STRING, self.assert_particle_configuration, True)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock()
        protocol = Protocol(Prompt, SAMI_NEWLINE, mock_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities),
                          sorted(protocol._filter_capabilities(test_capabilities)))

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected. All states defined in
        this dict must also be defined in the protocol FSM. Note, the EXIT and
        ENTER DRIVER_EVENTS don't need to be listed here.
        """
        #
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, self.capabilities_test_dict)

    @unittest.skip('long running test, avoid for regular unit testing')
    def test_pump_commands(self):

        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        driver._protocol._connection.send.side_effect = self.send_newline_side_effect(driver._protocol)
        driver._protocol._protocol_fsm.current_state = ProtocolState.COMMAND
        for param in driver._protocol._param_dict.get_keys():
            log.debug('startup param = %s', param)
            driver._protocol._param_dict.set_default(param)

        driver._protocol._param_dict.set_value(Parameter.FLUSH_CYCLES, 0x3)
        driver._protocol._protocol_fsm.current_state = ProtocolState.SEAWATER_FLUSH_2750ML
        driver._protocol._handler_seawater_flush_execute_2750ml()
        call = mock.call('P01,02\r')
        driver._protocol._connection.send.assert_has_calls(call)
        command_count = driver._protocol._connection.send.mock_calls.count(call)
        log.debug('SEAWATER_FLUSH_2750ML command count = %s', command_count)
        self.assertEqual(165, command_count, 'SEAWATER_FLUSH_2750ML command count %s != 165' % command_count)
        driver._protocol._connection.send.reset_mock()

        driver._protocol._param_dict.set_value(Parameter.FLUSH_CYCLES, 0x5)
        driver._protocol._protocol_fsm.current_state = ProtocolState.REAGENT_FLUSH_50ML
        driver._protocol._handler_reagent_flush_execute_50ml()
        call1 = mock.call('P03,04\r')
        call2 = mock.call('P02,04\r')
        driver._protocol._connection.send.assert_has_calls([call1, call2])
        command_count = driver._protocol._connection.send.mock_calls.count(call1)
        log.debug('REAGENT_FLUSH_50ML reagent flush command count = %s', command_count)
        self.assertEqual(5, command_count, 'REAGENT_FLUSH_50ML reagent flush command count %s != 5' % command_count)
        command_count = driver._protocol._connection.send.mock_calls.count(call2)
        log.debug('REAGENT_FLUSH_50ML seawater flush command count = %s', command_count)
        self.assertEqual(5, command_count, 'REAGENT_FLUSH_50ML seawater flush command count %s != 5' % command_count)
        driver._protocol._connection.send.reset_mock()

        driver._protocol._param_dict.set_value(Parameter.SEAWATER_FLUSH_DURATION, 0x27)
        driver._protocol._protocol_fsm.current_state = ProtocolState.SEAWATER_FLUSH
        driver._protocol._handler_seawater_flush_execute()
        call = mock.call('P01,27\r')
        driver._protocol._connection.send.assert_has_calls([call])
        command_count = driver._protocol._connection.send.mock_calls.count(call)
        log.debug('SEAWATER_FLUSH command count = %s', command_count)
        self.assertEqual(1, command_count, 'SEAWATER_FLUSH command count %s != 1' % command_count)
        driver._protocol._connection.send.reset_mock()

        driver._protocol._param_dict.set_value(Parameter.REAGENT_FLUSH_DURATION, 0x77)
        driver._protocol._protocol_fsm.current_state = ProtocolState.REAGENT_FLUSH
        driver._protocol._handler_reagent_flush_execute()
        call = mock.call('P03,77\r')
        driver._protocol._connection.send.assert_has_calls(call)
        command_count = driver._protocol._connection.send.mock_calls.count(call)
        log.debug('REAGENT_FLUSH command count = %s', command_count)
        self.assertEqual(1, command_count, 'REAGENT_FLUSH command count %s != 1' % command_count)
        driver._protocol._connection.send.reset_mock()

    @unittest.skip('long running test, avoid for regular unit testing')
    def test_pump_timing(self):
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        driver._protocol._protocol_fsm.current_state = ProtocolState.COMMAND
        for param in driver._protocol._param_dict.get_keys():
            log.debug('startup param = %s', param)
            driver._protocol._param_dict.set_default(param)

        stats = PumpStatisticsContainer(self, ('P01', '02'))
        driver._protocol._do_cmd_resp_no_wakeup = Mock(side_effect=stats.side_effect)
        driver._protocol._protocol_fsm.current_state = ProtocolState.SEAWATER_FLUSH_2750ML
        driver._protocol._handler_seawater_flush_execute_2750ml()
        stats.assert_timing(2)

        stats = PumpStatisticsContainer(self, ('P03', '04'))
        driver._protocol._do_cmd_resp_no_wakeup = Mock(side_effect=stats.side_effect)
        driver._protocol._param_dict.set_value(Parameter.FLUSH_CYCLES, 0x5)
        driver._protocol._protocol_fsm.current_state = ProtocolState.REAGENT_FLUSH_50ML
        driver._protocol._handler_reagent_flush_execute_50ml()
        stats.assert_timing(1)

    @unittest.skip('long running test, avoid for regular unit testing')
    def test_waiting_discover(self):

        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_waiting_discover(driver)

    def test_autosample_timing(self):
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_autosample_timing(driver)


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(SamiIntegrationTest, DriverTestMixinSub):
    def test_startup_params(self):

        startup_values = {
            Parameter.NUMBER_SAMPLES_AVERAGED: 0x01,
            Parameter.NUMBER_FLUSHES: 0x37,
            Parameter.PUMP_ON_FLUSH: 0x04,
            Parameter.PUMP_OFF_FLUSH: 0x20,
            Parameter.NUMBER_REAGENT_PUMPS: 0x01,
            Parameter.VALVE_DELAY: 0x08,
            Parameter.PUMP_ON_IND: 0x08,
            Parameter.PV_OFF_IND: 0x10,
            Parameter.NUMBER_BLANKS: 0x04,
            Parameter.PUMP_MEASURE_T: 0x08,
            Parameter.PUMP_OFF_TO_MEASURE: 0x10,
            Parameter.MEASURE_TO_PUMP_ON: 0x08,
            Parameter.NUMBER_MEASUREMENTS: 0x17,
            Parameter.SALINITY_DELAY: 0x00,
            Parameter.AUTO_SAMPLE_INTERVAL: 3600,
            Parameter.REAGENT_FLUSH_DURATION: 0x04,
            Parameter.SEAWATER_FLUSH_DURATION: 0x02,
            Parameter.FLUSH_CYCLES: 1
        }

        new_values = {
            Parameter.NUMBER_SAMPLES_AVERAGED: 0x02,
            Parameter.NUMBER_FLUSHES: 0x38,
            Parameter.PUMP_ON_FLUSH: 0x05,
            Parameter.PUMP_OFF_FLUSH: 0x21,
            Parameter.NUMBER_REAGENT_PUMPS: 0x02,
            Parameter.VALVE_DELAY: 0x09,
            Parameter.PUMP_ON_IND: 0x09,
            Parameter.PV_OFF_IND: 0x11,
            Parameter.NUMBER_BLANKS: 0x05,
            Parameter.PUMP_MEASURE_T: 0x09,
            Parameter.PUMP_OFF_TO_MEASURE: 0x11,
            Parameter.MEASURE_TO_PUMP_ON: 0x09,
            Parameter.NUMBER_MEASUREMENTS: 0x18,
            Parameter.SALINITY_DELAY: 0x01,
            Parameter.AUTO_SAMPLE_INTERVAL: 600,
            Parameter.REAGENT_FLUSH_DURATION: 0x08,
            Parameter.SEAWATER_FLUSH_DURATION: 0x07,
            Parameter.FLUSH_CYCLES: 14
        }

        self.assert_initialize_driver()

        for (key, val) in startup_values.iteritems():
            self.assert_get(key, val)

        self.assert_set_bulk(new_values)

        self.driver_client.cmd_dvr('apply_startup_params')

        for (key, val) in startup_values.iteritems():
            self.assert_get(key, val)

    def test_set(self):
        self.assert_initialize_driver()
        self.assert_set(Parameter.AUTO_SAMPLE_INTERVAL, 77)

        self.assert_set(Parameter.NUMBER_SAMPLES_AVERAGED, 0x02)
        self.assert_set(Parameter.NUMBER_FLUSHES, 0x30)
        self.assert_set(Parameter.PUMP_ON_FLUSH, 0x05)
        self.assert_set(Parameter.PUMP_OFF_FLUSH, 0x25)
        self.assert_set(Parameter.NUMBER_REAGENT_PUMPS, 0x02)
        self.assert_set(Parameter.VALVE_DELAY, 0x0A)
        self.assert_set(Parameter.PUMP_ON_IND, 0x0A)
        self.assert_set(Parameter.PV_OFF_IND, 0x15)
        self.assert_set(Parameter.NUMBER_BLANKS, 0x07)
        self.assert_set(Parameter.PUMP_MEASURE_T, 0x0A)
        self.assert_set(Parameter.PUMP_OFF_TO_MEASURE, 0x05)
        self.assert_set(Parameter.MEASURE_TO_PUMP_ON, 0x07)
        self.assert_set(Parameter.NUMBER_MEASUREMENTS, 0xA0)
        self.assert_set(Parameter.SALINITY_DELAY, 0x05)
        self.assert_set(Parameter.REAGENT_FLUSH_DURATION, 1)
        self.assert_set(Parameter.SEAWATER_FLUSH_DURATION, 1)
        self.assert_set(Parameter.FLUSH_CYCLES, 14)

        self.assert_set_readonly(Parameter.START_TIME_FROM_LAUNCH, 84600)
        self.assert_set_readonly(Parameter.STOP_TIME_FROM_START, 84600)
        self.assert_set_readonly(Parameter.MODE_BITS, 10)
        self.assert_set_readonly(Parameter.SAMI_SAMPLE_INTERVAL, 1800)

    def test_bulk_set(self):
        self.assert_initialize_driver()

        new_values = {
            Parameter.AUTO_SAMPLE_INTERVAL: 77,
            Parameter.NUMBER_SAMPLES_AVERAGED: 0x02,
            Parameter.NUMBER_FLUSHES: 0x30,
            Parameter.PUMP_ON_FLUSH: 0x05,
            Parameter.PUMP_OFF_FLUSH: 0x25,
            Parameter.NUMBER_REAGENT_PUMPS: 0x02,
            Parameter.VALVE_DELAY: 0x0A,
            Parameter.PUMP_ON_IND: 0x0A,
            Parameter.PV_OFF_IND: 0x15,
            Parameter.NUMBER_BLANKS: 0x07,
            Parameter.PUMP_MEASURE_T: 0x0A,
            Parameter.PUMP_OFF_TO_MEASURE: 0x05,
            Parameter.MEASURE_TO_PUMP_ON: 0x07,
            Parameter.NUMBER_MEASUREMENTS: 0xA0,
            Parameter.SALINITY_DELAY: 0x05,
            Parameter.REAGENT_FLUSH_DURATION: 1,
            Parameter.SEAWATER_FLUSH_DURATION: 1,
            Parameter.FLUSH_CYCLES: 14
        }
        self.assert_set_bulk(new_values)

    def test_bad_parameters(self):
        self.assert_initialize_driver()

        self.assert_set_exception(Parameter.NUMBER_SAMPLES_AVERAGED, 2.0)
        self.assert_set_exception(Parameter.NUMBER_FLUSHES, 30.0)
        self.assert_set_exception(Parameter.PUMP_ON_FLUSH, 5.0)
        self.assert_set_exception(Parameter.PUMP_OFF_FLUSH, 25.0)
        self.assert_set_exception(Parameter.NUMBER_REAGENT_PUMPS, 2.0)
        self.assert_set_exception(Parameter.VALVE_DELAY, 10.0)
        self.assert_set_exception(Parameter.PUMP_ON_IND, 10.0)
        self.assert_set_exception(Parameter.PV_OFF_IND, 15.0)
        self.assert_set_exception(Parameter.NUMBER_BLANKS, 7.0)
        self.assert_set_exception(Parameter.PUMP_MEASURE_T, 10.0)
        self.assert_set_exception(Parameter.PUMP_OFF_TO_MEASURE, 5.0)
        self.assert_set_exception(Parameter.MEASURE_TO_PUMP_ON, 7.0)
        self.assert_set_exception(Parameter.NUMBER_MEASUREMENTS, 40.0)
        self.assert_set_exception(Parameter.SALINITY_DELAY, 5.0)

    def test_acquire_sample(self):
        self.assert_initialize_driver()
        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE)
        self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD, self.assert_particle_sami_data_sample,
                                              timeout=240)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_auto_sample(self):
        self.assert_initialize_driver()
        self.assert_set(Parameter.AUTO_SAMPLE_INTERVAL, 320)

        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.SCHEDULED_SAMPLE, delay=5)
        self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD, self.assert_particle_sami_data_sample,
                                              particle_count=3, timeout=1280)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=5)
        self.clear_events()

        # Now verify that no more particles get generated
        failed = False
        try:
            self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD,
                                                  self.assert_particle_sami_data_sample,
                                                  timeout=400)
            failed = True
        except AssertionError:
            pass
        self.assertFalse(failed)

        # Restart autosample
        self.clear_events()
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.SCHEDULED_SAMPLE, delay=5)
        self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD, self.assert_particle_sami_data_sample,
                                              particle_count=3, timeout=1280)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=5)

    def test_polled_sample_state(self):
        self.assert_initialize_driver()
        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE, state=ProtocolState.POLLED_SAMPLE, delay=5)
        self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD, self.assert_particle_sami_data_sample,
                                              timeout=240)

    def test_scheduled_sample_state(self):
        self.assert_initialize_driver()
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.SCHEDULED_SAMPLE, delay=5)
        self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD, self.assert_particle_sami_data_sample,
                                              timeout=240)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=5)

    def test_scheduled_device_status_auto_sample(self):
        """
        Verify the device status command can be triggered and run in autosample
        """
        self.assert_scheduled_event(ScheduledJob.ACQUIRE_STATUS, delay=160)
        self.clear_events()
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.SCHEDULED_SAMPLE, delay=5)
        self.assert_async_particle_generation(DataParticleType.PHSEN_CONFIGURATION, self.assert_particle_configuration,
                                              timeout=280)
        self.assert_async_particle_generation(DataParticleType.PHSEN_BATTERY_VOLTAGE,
                                              self.assert_particle_battery_voltage)
        self.assert_async_particle_generation(DataParticleType.PHSEN_THERMISTOR_VOLTAGE,
                                              self.assert_particle_thermistor_voltage)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

    def test_queued_command(self):
        """
        Verify status is queued while samples are being taken
        """
        self.assert_initialize_driver()

        # Queue sample and status
        self.clear_events()
        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS)
        self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD, self.assert_particle_sami_data_sample,
                                              timeout=240)
        self.assert_async_particle_generation(DataParticleType.PHSEN_REGULAR_STATUS,
                                              self.assert_particle_regular_status,
                                              timeout=240)

        self.assert_current_state(ProtocolState.COMMAND)

    def test_queued_autosample(self):
        """
        Verify commands are queued while samples are being taken
        """
        self.assert_initialize_driver()
        self.clear_events()
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.SCHEDULED_SAMPLE, delay=5)

        # Queue sample and status
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS)
        self.assert_async_particle_generation(DataParticleType.PHSEN_DATA_RECORD, self.assert_particle_sami_data_sample,
                                              timeout=240)
        self.assert_async_particle_generation(DataParticleType.PHSEN_REGULAR_STATUS,
                                              self.assert_particle_regular_status,
                                              timeout=240)

        self.assert_current_state(ProtocolState.AUTOSAMPLE)

    def test_acquire_status(self):
        self.assert_initialize_driver()
        self.clear_events()
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.PHSEN_REGULAR_STATUS,
                                        self.assert_particle_regular_status)
        self.assert_async_particle_generation(DataParticleType.PHSEN_CONFIGURATION, self.assert_particle_configuration)
        self.assert_async_particle_generation(DataParticleType.PHSEN_BATTERY_VOLTAGE, self.assert_particle_battery_voltage)
        self.assert_async_particle_generation(DataParticleType.PHSEN_THERMISTOR_VOLTAGE,
                                              self.assert_particle_thermistor_voltage)

    def test_scheduled_device_status_command(self):
        """
        Verify the device status command can be triggered and run in command
        """
        self.assert_scheduled_event(ScheduledJob.ACQUIRE_STATUS, delay=120)
        self.clear_events()
        self.assert_async_particle_generation(DataParticleType.PHSEN_CONFIGURATION, self.assert_particle_configuration,
                                              timeout=180)
        self.assert_async_particle_generation(DataParticleType.PHSEN_BATTERY_VOLTAGE, self.assert_particle_battery_voltage)
        self.assert_async_particle_generation(DataParticleType.PHSEN_THERMISTOR_VOLTAGE,
                                              self.assert_particle_thermistor_voltage)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_flush_pump(self):
        self.assert_initialize_driver()
        self.assert_driver_command(ProtocolEvent.SEAWATER_FLUSH_2750ML, delay=220.0)
        self.assert_driver_command(ProtocolEvent.REAGENT_FLUSH_50ML, delay=15.0)
        self.assert_driver_command(ProtocolEvent.SEAWATER_FLUSH, delay=15.0)
        self.assert_driver_command(ProtocolEvent.REAGENT_FLUSH, delay=15.0)


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for doing final testing of ion      #
# integration.  The generally aren't used for instrument debugging and should #
# be tackled after all unit and integration tests are complete                #
###############################################################################
@attr('QUAL', group='mi')
class DriverQualificationTest(SamiQualificationTest, DriverTestMixinSub):
    @unittest.skip("Runs for several hours to test default autosample rate of 60 minutes")
    def test_overnight(self):
        """
        Verify autosample at default rate
        """
        self.assert_enter_command_mode()

        self.assert_sample_autosample(self.assert_particle_sami_data_sample, DataParticleType.PHSEN_DATA_RECORD,
                                      timeout=14400)

    def test_direct_access_telnet_mode(self):
        """
        @brief This test manually tests that the Instrument Driver properly
        supports direct access to the physical instrument. (telnet mode)
        """

        self.assert_enter_command_mode()

        self.assert_set_parameter(Parameter.NUMBER_FLUSHES, 0x30)

        configuration_string = 'CF8F17F902C7EA0001E1338002000E100A0200000000000000000000000000000000000000000' + \
                               '70137042001080810040810081700000000000000000000000000000000000000000000000000' + \
                               '00000000000000000000000000000000000000000000000000000000000000000000000000000' + \
                               '0FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + \
                               'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + \
                               'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' + \
                               'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'

        self.assert_direct_access_start_telnet()
        self.assertTrue(self.tcp_client)

        # Erase memory
        self.tcp_client.send_data("E5A%s" % SAMI_NEWLINE)

        time.sleep(1)

        # Load a new configuration string changing X to X
        self.tcp_client.send_data("L5A%s" % SAMI_NEWLINE)

        time.sleep(1)

        self.tcp_client.send_data("%s00%s" % (configuration_string, SAMI_NEWLINE))

        time.sleep(1)

        # Check that configuration was changed
        self.tcp_client.send_data("L%s" % SAMI_NEWLINE)
        return_value = self.tcp_client.expect(configuration_string)
        self.assertTrue(return_value)

        ###
        #   Add instrument specific code here.
        ###

        self.assert_direct_access_stop_telnet()
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)

        self.assert_get_parameter(Parameter.NUMBER_FLUSHES, 0x30)

    def test_command_poll(self):
        self.assert_enter_command_mode()

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_SAMPLE, self.assert_particle_sami_data_sample,
                                    DataParticleType.PHSEN_DATA_RECORD, sample_count=1, timeout=240)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_regular_status,
                                    DataParticleType.PHSEN_REGULAR_STATUS, sample_count=1, timeout=10)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_configuration,
                                    DataParticleType.PHSEN_CONFIGURATION, sample_count=1, timeout=10)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_battery_voltage,
                                    DataParticleType.PHSEN_BATTERY_VOLTAGE, sample_count=1, timeout=10)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_thermistor_voltage,
                                    DataParticleType.PHSEN_THERMISTOR_VOLTAGE, sample_count=1, timeout=10)

        self.assert_resource_command(ProtocolEvent.SEAWATER_FLUSH_2750ML, delay=220,
                                     agent_state=ResourceAgentState.COMMAND, resource_state=ProtocolState.COMMAND)
        self.assert_resource_command(ProtocolEvent.REAGENT_FLUSH_50ML, delay=15, agent_state=ResourceAgentState.COMMAND,
                                     resource_state=ProtocolState.COMMAND)
        self.assert_resource_command(ProtocolEvent.SEAWATER_FLUSH, delay=15, agent_state=ResourceAgentState.COMMAND,
                                     resource_state=ProtocolState.COMMAND)
        self.assert_resource_command(ProtocolEvent.REAGENT_FLUSH, delay=15, agent_state=ResourceAgentState.COMMAND,
                                     resource_state=ProtocolState.COMMAND)

        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)

    def test_autosample_poll(self):
        self.assert_enter_command_mode()

        self.assert_start_autosample(timeout=240)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_SAMPLE, self.assert_particle_sami_data_sample,
                                    DataParticleType.PHSEN_DATA_RECORD, sample_count=1, timeout=240)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_regular_status,
                                    DataParticleType.PHSEN_REGULAR_STATUS, sample_count=1, timeout=10)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_configuration,
                                    DataParticleType.PHSEN_CONFIGURATION, sample_count=1, timeout=10)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_battery_voltage,
                                    DataParticleType.PHSEN_BATTERY_VOLTAGE, sample_count=1, timeout=10)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_thermistor_voltage,
                                    DataParticleType.PHSEN_THERMISTOR_VOLTAGE, sample_count=1, timeout=10)

        self.assert_stop_autosample()
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)

    def test_autosample(self):
        """
        Verify autosample works and data particles are created
        """
        self.assert_enter_command_mode()
        self.assert_set_parameter(Parameter.AUTO_SAMPLE_INTERVAL, 320)

        self.assert_sample_autosample(self.assert_particle_sami_data_sample, DataParticleType.PHSEN_DATA_RECORD)

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
                ProtocolEvent.START_AUTOSAMPLE,
                ProtocolEvent.ACQUIRE_STATUS,
                ProtocolEvent.ACQUIRE_SAMPLE,
                ProtocolEvent.SEAWATER_FLUSH_2750ML,
                ProtocolEvent.REAGENT_FLUSH_50ML,
                ProtocolEvent.SEAWATER_FLUSH,
                ProtocolEvent.REAGENT_FLUSH
            ],
            AgentCapabilityType.RESOURCE_INTERFACE: None,
            AgentCapabilityType.RESOURCE_PARAMETER: self._driver_parameters.keys()
        }

        self.assert_capabilities(capabilities)

        ##################
        #  DA Mode
        ##################

        da_capabilities = copy.deepcopy(capabilities)
        da_capabilities[AgentCapabilityType.AGENT_COMMAND] = [ResourceAgentEvent.GO_COMMAND]
        da_capabilities[AgentCapabilityType.RESOURCE_COMMAND] = []

        # Test direct access disconnect
        self.assert_direct_access_start_telnet(timeout=10)
        self.assertTrue(self.tcp_client)

        self.assert_capabilities(da_capabilities)
        self.tcp_client.disconnect()

        # Now do it again, but use the event to stop DA
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)
        self.assert_direct_access_start_telnet(timeout=10)
        self.assert_capabilities(da_capabilities)
        self.assert_direct_access_stop_telnet()

        ##################
        #  Command Mode
        ##################

        # We should be back in command mode from DA.
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)
        self.assert_capabilities(capabilities)

        ##################
        #  Streaming Mode
        ##################

        st_capabilities = copy.deepcopy(capabilities)
        st_capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.STREAMING)
        st_capabilities[AgentCapabilityType.RESOURCE_COMMAND] = [
            ProtocolEvent.STOP_AUTOSAMPLE,
            ProtocolEvent.ACQUIRE_STATUS,
            ProtocolEvent.ACQUIRE_SAMPLE
        ]

        self.assert_start_autosample(timeout=240)
        self.assert_capabilities(st_capabilities)
        self.assert_stop_autosample()

        ##################
        #  Command Mode
        ##################

        # We should be back in command mode from DA.
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)
        self.assert_capabilities(capabilities)

        #######################
        #  Uninitialized Mode
        #######################

        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.UNINITIALIZED)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = []
        capabilities[AgentCapabilityType.RESOURCE_INTERFACE] = []
        capabilities[AgentCapabilityType.RESOURCE_PARAMETER] = []

        self.assert_reset()
        self.assert_capabilities(capabilities)
