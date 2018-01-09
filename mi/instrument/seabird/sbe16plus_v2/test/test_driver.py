"""
@package mi.instrument.seabird.sbe16plus_v2.test.test_driver
@file mi/instrument/seabird/sbe16plus_v2/test/test_driver.py
@author David Everett
@brief Test cases for InstrumentDriver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u
       $ bin/test_driver -i
       $ bin/test_driver -q

   * From pyon
       $ bin/nosetests -s -v .../mi/instrument/seabird/sbe16plus_v2/ooicore
       $ bin/nosetests -s -v .../mi/instrument/seabird/sbe16plus_v2/ooicore -a UNIT
       $ bin/nosetests -s -v .../mi/instrument/seabird/sbe16plus_v2/ooicore -a INT
       $ bin/nosetests -s -v .../mi/instrument/seabird/sbe16plus_v2/ooicore -a QUAL
"""

import time
from mock import Mock
from nose.plugins.attrib import attr

from mi.idk.unit_test import \
    AgentCapabilityType, DriverTestMixin, InstrumentDriverUnitTestCase, InstrumentDriverIntegrationTestCase, \
    InstrumentDriverTestCase, InstrumentDriverQualificationTestCase, ParameterTestConfigKey

from mi.core.log import get_logger
from mi.core.time_tools import get_timestamp_delayed, timegm_to_float
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.instrument_driver import DriverEvent, ResourceAgentState

from mi.core.exceptions import \
    InstrumentParameterException, SampleException, InstrumentProtocolException, InstrumentCommandException

from mi.instrument.seabird.sbe16plus_v2.driver import \
    SBE16Protocol, Sbe16plusBaseParticle, SBE16DataParticle, SBE16StatusParticle, SBE16CalibrationParticle, \
    SBE16InstrumentDriver, DataParticleType, ConfirmedParameter, NEWLINE, SBE16DataParticleKey, \
    SBE16StatusParticleKey, SBE16CalibrationParticleKey, ProtocolState, ProtocolEvent, ScheduledJob, Capability, \
    Parameter, Command, Prompt

from mi.instrument.seabird.sbe16plus_v2.test.sample_particles import \
    VALID_STATUS_RESPONSE, VALID_SAMPLE, VALID_SAMPLE2, VALID_DCAL_STRAIN, VALID_DCAL_QUARTZ

__author__ = 'David Everett'
__license__ = 'Apache 2.0'

DEFAULT_CLOCK_DIFF = 5

log = get_logger()

InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.seabird.sbe16plus_v2.driver',
    driver_class="SBE16InstrumentDriver",
    instrument_agent_resource_id='3DLE2A',
    instrument_agent_name='seabird_sbe16plus_v2_ctdpf',
    instrument_agent_packet_config=DataParticleType(),
    driver_startup_config={}
)


class SeaBird16plusMixin(DriverTestMixin):
    InstrumentDriver = SBE16InstrumentDriver

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
    #  Parameter and Type Definitions
    ###
    _driver_parameters = {
        # Parameters defined in the IOS
        Parameter.ECHO: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.OUTPUT_EXEC_TAG: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.TXREALTIME: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.PUMP_MODE: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 2, VALUE: 2},
        Parameter.NCYCLES: {TYPE: int, READONLY: False, DA: False, STARTUP: True, DEFAULT: 4, VALUE: 4},
        Parameter.INTERVAL: {TYPE: int, READONLY: False, DA: False, STARTUP: True, VALUE: 10},
        Parameter.BIOWIPER: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.PTYPE: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 1, VALUE: 1},
        Parameter.VOLT0: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT1: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT2: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT3: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT4: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT5: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.DELAY_BEFORE_SAMPLE: {TYPE: float, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0.0, VALUE: 0.0},
        Parameter.DELAY_AFTER_SAMPLE: {TYPE: float, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0.0, VALUE: 0.0},
        Parameter.SBE63: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.SBE38: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.SBE50: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.WETLABS: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.GTD: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.OPTODE: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.SYNCMODE: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.OUTPUT_FORMAT: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        Parameter.LOGGING: {TYPE: bool, READONLY: True, DA: False, STARTUP: False},
        Parameter.DUAL_GTD: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
    }

    _driver_capabilities = {
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.ACQUIRE_SAMPLE: {STATES: [ProtocolState.COMMAND]},
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.AUTOSAMPLE]},
        Capability.CLOCK_SYNC: {STATES: [ProtocolState.COMMAND]},
        Capability.ACQUIRE_STATUS: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
    }

    _sample_parameters = {
        SBE16DataParticleKey.TEMP: {TYPE: int, VALUE: 264667, REQUIRED: True},
        SBE16DataParticleKey.CONDUCTIVITY: {TYPE: int, VALUE: 684940, REQUIRED: True},
        SBE16DataParticleKey.PRESSURE: {TYPE: int, VALUE: 8483962, REQUIRED: True},
        SBE16DataParticleKey.PRESSURE_TEMP: {TYPE: int, VALUE: 33964, REQUIRED: True},
        SBE16DataParticleKey.TIME: {TYPE: int, VALUE: 415133401, REQUIRED: True},
    }

    _status_parameters = {
        SBE16StatusParticleKey.FIRMWARE_VERSION: {TYPE: unicode, VALUE: '2.5.2', REQUIRED: True},
        SBE16StatusParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: '01906914', REQUIRED: True},
        SBE16StatusParticleKey.DATE_TIME: {TYPE: unicode, VALUE: '2014-03-20T09:09:06', REQUIRED: True},
        SBE16StatusParticleKey.VBATT: {TYPE: float, VALUE: 13.0, REQUIRED: True},
        SBE16StatusParticleKey.VLITH: {TYPE: float, VALUE: 8.6, REQUIRED: True},
        SBE16StatusParticleKey.IOPER: {TYPE: float, VALUE: 51.1, REQUIRED: True},
        SBE16StatusParticleKey.IPUMP: {TYPE: float, VALUE: 145.6, REQUIRED: True},
        SBE16StatusParticleKey.LOGGING_STATUS: {TYPE: unicode, VALUE: 'not logging', REQUIRED: True},
        SBE16StatusParticleKey.SAMPLES: {TYPE: int, VALUE: 15, REQUIRED: True},
        SBE16StatusParticleKey.MEM_FREE: {TYPE: int, VALUE: 2990809, REQUIRED: True},
        SBE16StatusParticleKey.SAMPLE_INTERVAL: {TYPE: int, VALUE: 10, REQUIRED: False},
        SBE16StatusParticleKey.MEASUREMENTS_PER_SAMPLE: {TYPE: int, VALUE: 4, REQUIRED: False},
        SBE16StatusParticleKey.PUMP_MODE: {TYPE: unicode, VALUE: 'no', REQUIRED: True},
        SBE16StatusParticleKey.DELAY_BEFORE_SAMPLING: {TYPE: float, VALUE: 15.0, REQUIRED: True},
        SBE16StatusParticleKey.DELAY_AFTER_SAMPLING: {TYPE: float, VALUE: 15.0, REQUIRED: True},
        SBE16StatusParticleKey.TX_REAL_TIME: {TYPE: int, VALUE: 1, REQUIRED: False},
        SBE16StatusParticleKey.BATTERY_CUTOFF: {TYPE: float, VALUE: 7.5, REQUIRED: True},
        SBE16StatusParticleKey.PRESSURE_SENSOR: {TYPE: unicode, VALUE: 'strain-0', REQUIRED: True},
        SBE16StatusParticleKey.RANGE: {TYPE: float, VALUE: 160, REQUIRED: False},
        SBE16StatusParticleKey.SBE38: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.SBE50: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.WETLABS: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.OPTODE: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.GAS_TENSION_DEVICE: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.EXT_VOLT_0: {TYPE: int, VALUE: 1, REQUIRED: True},
        SBE16StatusParticleKey.EXT_VOLT_1: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.EXT_VOLT_2: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.EXT_VOLT_3: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.EXT_VOLT_4: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.EXT_VOLT_5: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE16StatusParticleKey.ECHO_CHARACTERS: {TYPE: int, VALUE: 1, REQUIRED: True},
        SBE16StatusParticleKey.OUTPUT_FORMAT: {TYPE: unicode, VALUE: 'raw HEX', REQUIRED: True},
        SBE16StatusParticleKey.OUTPUT_SALINITY: {TYPE: int, VALUE: 0, REQUIRED: False},
        SBE16StatusParticleKey.OUTPUT_SOUND_VELOCITY: {TYPE: int, VALUE: 0, REQUIRED: False},
        SBE16StatusParticleKey.SERIAL_SYNC_MODE: {TYPE: int, VALUE: 0, REQUIRED: False},
    }

    _calibration_parameters_strain = {
        SBE16CalibrationParticleKey.PRES_SERIAL_NUMBER: {TYPE: unicode, VALUE: '3313899', REQUIRED: True},
        SBE16CalibrationParticleKey.PRES_RANGE: {TYPE: int, VALUE: 508, REQUIRED: True},
        SBE16CalibrationParticleKey.PRES_CAL_DATE: {TYPE: unicode, VALUE: '06-Oct-11', REQUIRED: True},
        SBE16CalibrationParticleKey.PA0: {TYPE: float, VALUE: -3.689246e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.PA1: {TYPE: float, VALUE: 1.545570e-03, REQUIRED: True},
        SBE16CalibrationParticleKey.PA2: {TYPE: float, VALUE: 6.733197e-12, REQUIRED: True},
        SBE16CalibrationParticleKey.PTCA0: {TYPE: float, VALUE: 5.249034e+05, REQUIRED: True},
        SBE16CalibrationParticleKey.PTCA1: {TYPE: float, VALUE: 1.423189e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.PTCA2: {TYPE: float, VALUE: -1.206562e-01, REQUIRED: True},
        SBE16CalibrationParticleKey.PTCB0: {TYPE: float, VALUE: 2.501288e+01, REQUIRED: True},
        SBE16CalibrationParticleKey.PTCB1: {TYPE: float, VALUE: -2.250000e-04, REQUIRED: True},
        SBE16CalibrationParticleKey.PTCB2: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.PTEMPA0: {TYPE: float, VALUE: -5.677620e+01, REQUIRED: True},
        SBE16CalibrationParticleKey.PTEMPA1: {TYPE: float, VALUE: 5.424624e+01, REQUIRED: True},
        SBE16CalibrationParticleKey.PTEMPA2: {TYPE: float, VALUE: -2.278113e-01, REQUIRED: True},
        SBE16CalibrationParticleKey.POFFSET: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.FIRMWARE_VERSION: {TYPE: unicode, VALUE: "SBE19plus", REQUIRED: True},
        SBE16CalibrationParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: '01906914', REQUIRED: True},
        SBE16CalibrationParticleKey.DATE_TIME: {TYPE: unicode, VALUE: "09-Oct-11", REQUIRED: True},
        SBE16CalibrationParticleKey.TEMP_CAL_DATE: {TYPE: unicode, VALUE: "09-Oct-11", REQUIRED: True},
        SBE16CalibrationParticleKey.TA0: {TYPE: float, VALUE: 1.254755e-03, REQUIRED: True},
        SBE16CalibrationParticleKey.TA1: {TYPE: float, VALUE: 2.758871e-04, REQUIRED: True},
        SBE16CalibrationParticleKey.TA2: {TYPE: float, VALUE: -1.368268e-06, REQUIRED: True},
        SBE16CalibrationParticleKey.TA3: {TYPE: float, VALUE: 1.910795e-07, REQUIRED: True},
        SBE16CalibrationParticleKey.TOFFSET: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.COND_CAL_DATE: {TYPE: unicode, VALUE: '09-Oct-11', REQUIRED: True},
        SBE16CalibrationParticleKey.CONDG: {TYPE: float, VALUE: -9.761799e-01, REQUIRED: True},
        SBE16CalibrationParticleKey.CONDH: {TYPE: float, VALUE: 1.369994e-01, REQUIRED: True},
        SBE16CalibrationParticleKey.CONDI: {TYPE: float, VALUE: -3.523860e-04, REQUIRED: True},
        SBE16CalibrationParticleKey.CONDJ: {TYPE: float, VALUE: 4.404252e-05, REQUIRED: True},
        SBE16CalibrationParticleKey.CPCOR: {TYPE: float, VALUE: -9.570000e-08, REQUIRED: True},
        SBE16CalibrationParticleKey.CTCOR: {TYPE: float, VALUE: 3.250000e-06, REQUIRED: True},
        SBE16CalibrationParticleKey.CSLOPE: {TYPE: float, VALUE: 1.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT0_OFFSET: {TYPE: float, VALUE: -4.650526e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT0_SLOPE: {TYPE: float, VALUE: 1.246381e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT1_OFFSET: {TYPE: float, VALUE: -4.618105e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT1_SLOPE: {TYPE: float, VALUE: 1.247197e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT2_OFFSET: {TYPE: float, VALUE: -4.659790e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT2_SLOPE: {TYPE: float, VALUE: 1.247601e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT3_OFFSET: {TYPE: float, VALUE: -4.502421e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT3_SLOPE: {TYPE: float, VALUE: 1.246911e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT4_OFFSET: {TYPE: float, VALUE: -4.589158e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT4_SLOPE: {TYPE: float, VALUE: 1.246346e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT5_OFFSET: {TYPE: float, VALUE: -4.609895e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT5_SLOPE: {TYPE: float, VALUE: 1.247868e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_FREQ: {TYPE: float, VALUE: 1.000008e+00, REQUIRED: True},
        # data will not be found in a STRAIN instrument response
        SBE16CalibrationParticleKey.PC1: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PC2: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PC3: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PD1: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PD2: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PT1: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PT2: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PT3: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PT4: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PSLOPE: {TYPE: float, VALUE: None, REQUIRED: False},
    }

    _calibration_parameters_quartz = {
        SBE16CalibrationParticleKey.PRES_SERIAL_NUMBER: {TYPE: unicode, VALUE: '3313899', REQUIRED: True},

        SBE16CalibrationParticleKey.PRES_CAL_DATE: {TYPE: unicode, VALUE: '06-Oct-11', REQUIRED: True},
        SBE16CalibrationParticleKey.PC1: {TYPE: float, VALUE: -4.642673e+03, REQUIRED: True},
        SBE16CalibrationParticleKey.PC2: {TYPE: float, VALUE: -4.611640e-03, REQUIRED: True},
        SBE16CalibrationParticleKey.PC3: {TYPE: float, VALUE: 8.921190e-04, REQUIRED: True},
        SBE16CalibrationParticleKey.PD1: {TYPE: float, VALUE: 7.024800e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.PD2: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.PT1: {TYPE: float, VALUE: 3.022595e+01, REQUIRED: True},
        SBE16CalibrationParticleKey.PT2: {TYPE: float, VALUE: -1.549720e-04, REQUIRED: True},
        SBE16CalibrationParticleKey.PT3: {TYPE: float, VALUE: 2.677750e-06, REQUIRED: True},
        SBE16CalibrationParticleKey.PT4: {TYPE: float, VALUE: 1.705490e-09, REQUIRED: True},
        SBE16CalibrationParticleKey.PSLOPE: {TYPE: float, VALUE: -1.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.POFFSET: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.FIRMWARE_VERSION: {TYPE: unicode, VALUE: "SBE19plus", REQUIRED: True},
        SBE16CalibrationParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: '01906914', REQUIRED: True},
        SBE16CalibrationParticleKey.DATE_TIME: {TYPE: unicode, VALUE: "09-Oct-11", REQUIRED: True},
        SBE16CalibrationParticleKey.TEMP_CAL_DATE: {TYPE: unicode, VALUE: "09-Oct-11", REQUIRED: True},
        SBE16CalibrationParticleKey.TA0: {TYPE: float, VALUE: 1.254755e-03, REQUIRED: True},
        SBE16CalibrationParticleKey.TA1: {TYPE: float, VALUE: 2.758871e-04, REQUIRED: True},
        SBE16CalibrationParticleKey.TA2: {TYPE: float, VALUE: -1.368268e-06, REQUIRED: True},
        SBE16CalibrationParticleKey.TA3: {TYPE: float, VALUE: 1.910795e-07, REQUIRED: True},
        SBE16CalibrationParticleKey.TOFFSET: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.COND_CAL_DATE: {TYPE: unicode, VALUE: '09-Oct-11', REQUIRED: True},
        SBE16CalibrationParticleKey.CONDG: {TYPE: float, VALUE: -9.761799e-01, REQUIRED: True},
        SBE16CalibrationParticleKey.CONDH: {TYPE: float, VALUE: 1.369994e-01, REQUIRED: True},
        SBE16CalibrationParticleKey.CONDI: {TYPE: float, VALUE: -3.523860e-04, REQUIRED: True},
        SBE16CalibrationParticleKey.CONDJ: {TYPE: float, VALUE: 4.404252e-05, REQUIRED: True},
        SBE16CalibrationParticleKey.CPCOR: {TYPE: float, VALUE: -9.570000e-08, REQUIRED: True},
        SBE16CalibrationParticleKey.CTCOR: {TYPE: float, VALUE: 3.250000e-06, REQUIRED: True},
        SBE16CalibrationParticleKey.CSLOPE: {TYPE: float, VALUE: 1.000000e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT0_OFFSET: {TYPE: float, VALUE: -4.650526e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT0_SLOPE: {TYPE: float, VALUE: 1.246381e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT1_OFFSET: {TYPE: float, VALUE: -4.618105e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT1_SLOPE: {TYPE: float, VALUE: 1.247197e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT2_OFFSET: {TYPE: float, VALUE: -4.659790e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT2_SLOPE: {TYPE: float, VALUE: 1.247601e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT3_OFFSET: {TYPE: float, VALUE: -4.502421e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT3_SLOPE: {TYPE: float, VALUE: 1.246911e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT4_OFFSET: {TYPE: float, VALUE: -4.589158e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT4_SLOPE: {TYPE: float, VALUE: 1.246346e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT5_OFFSET: {TYPE: float, VALUE: -4.609895e-02, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_VOLT5_SLOPE: {TYPE: float, VALUE: 1.247868e+00, REQUIRED: True},
        SBE16CalibrationParticleKey.EXT_FREQ: {TYPE: float, VALUE: 1.000008e+00, REQUIRED: True},
        # data will not be found in a QUARTZ instrument response
        SBE16CalibrationParticleKey.PA0: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PA1: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PA2: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTCA0: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTCA1: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTCA2: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTCB0: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTCB1: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTCB2: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTEMPA0: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTEMPA1: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PTEMPA2: {TYPE: float, VALUE: None, REQUIRED: False},
        SBE16CalibrationParticleKey.PRES_RANGE: {TYPE: int, VALUE: None, REQUIRED: False},
    }

    ###
    #   Driver Parameter Methods
    ###
    def assert_driver_parameters(self, current_parameters, verify_values=False):
        """
        Verify that all driver parameters are correct and potentially verify values.
        @param current_parameters: driver parameters read from the driver instance
        @param verify_values: should we verify values against definition?
        """
        self.assert_parameters(current_parameters, self._driver_parameters, verify_values)

    def assert_particle_sample(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  SBE16DataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE16DataParticleKey, self._sample_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.CTD_PARSED, require_instrument_timestamp=True)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters, verify_values)

    def assert_particle_status(self, data_particle, verify_values=False):
        """
        Verify status particle
        @param data_particle:  SBE16StatusParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE16StatusParticleKey, self._status_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_STATUS)
        self.assert_data_particle_parameters(data_particle, self._status_parameters, verify_values)

    def assert_particle_calibration_quartz(self, data_particle, verify_values=False):
        """
        Verify calibration particle
        @param data_particle:  SBE16CalibrationParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE16CalibrationParticleKey, self._calibration_parameters_quartz)
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_parameters_quartz, verify_values)

    def assert_particle_calibration_strain(self, data_particle, verify_values=False):
        """
        Verify calibration particle
        @param data_particle:  SBE16CalibrationParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE16CalibrationParticleKey, self._calibration_parameters_strain)
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_parameters_strain, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class Sbe16plusUnitTestCase(InstrumentDriverUnitTestCase, SeaBird16plusMixin):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_corrupt_data(self):
        """
        Verify corrupt data generates a SampleException
        """
        for sample, p_type in [(VALID_DCAL_STRAIN, SBE16CalibrationParticle),
                               (VALID_DCAL_QUARTZ, SBE16CalibrationParticle),
                               (VALID_STATUS_RESPONSE, SBE16StatusParticle),
                               (VALID_SAMPLE, SBE16DataParticle),
                               (VALID_SAMPLE2, SBE16DataParticle)]:
                sample = sample[:8] + 'GARBAGE123123124' + sample[8:]
                with self.assertRaises(SampleException):
                    p_type(sample).generate()

    def test_combined_samples(self):
        """
        Verify combined samples produce the correct number of chunks
        """
        chunker = StringChunker(SBE16Protocol.sieve_function)
        ts = self.get_ntp_timestamp()
        my_samples = [(VALID_SAMPLE + VALID_STATUS_RESPONSE + VALID_SAMPLE, 3),
                      (VALID_SAMPLE2 + VALID_SAMPLE2 + VALID_DCAL_QUARTZ + VALID_DCAL_STRAIN, 4)]

        for data, num_samples in my_samples:
            chunker.add_chunk(data, ts)
            results = []
            while True:
                timestamp, result = chunker.get_next_data()
                if result:
                    results.append(result)
                    self.assertTrue(result in data)
                    self.assertEqual(timestamp, ts)
                else:
                    break

            self.assertEqual(len(results), num_samples)

    def test_sbetime2unixtime(self):
        """
        Verify the sbetime2unixtime method works as expected.
        """
        value = time.gmtime(Sbe16plusBaseParticle.sbetime2unixtime(0))
        self.assertEqual("2000-01-01 00:00:00", time.strftime("%Y-%m-%d %H:%M:%S", value))

        value = time.gmtime(Sbe16plusBaseParticle.sbetime2unixtime(5))
        self.assertEqual("2000-01-01 00:00:05", time.strftime("%Y-%m-%d %H:%M:%S", value))

        value = time.gmtime(Sbe16plusBaseParticle.sbetime2unixtime(604800))
        self.assertEqual("2000-01-08 00:00:00", time.strftime("%Y-%m-%d %H:%M:%S", value))

        value = time.gmtime(Sbe16plusBaseParticle.sbetime2unixtime(-1))
        self.assertEqual("1999-12-31 23:59:59", time.strftime("%Y-%m-%d %H:%M:%S", value))

    def test_base_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """
        self.assert_enum_has_no_duplicates(Command())
        self.assert_enum_has_no_duplicates(ScheduledJob())
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())

        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_complete(ConfirmedParameter(), Parameter())

        # Test capabilities for duplicates, them verify that capabilities is a subset of proto events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(SBE16Protocol.sieve_function)

        self.assert_chunker_sample(chunker, VALID_SAMPLE)
        self.assert_chunker_sample_with_noise(chunker, VALID_SAMPLE)
        self.assert_chunker_fragmented_sample(chunker, VALID_SAMPLE)
        self.assert_chunker_combined_sample(chunker, VALID_SAMPLE)

        self.assert_chunker_sample(chunker, VALID_SAMPLE2)
        self.assert_chunker_sample_with_noise(chunker, VALID_SAMPLE2)
        self.assert_chunker_fragmented_sample(chunker, VALID_SAMPLE2)
        self.assert_chunker_combined_sample(chunker, VALID_SAMPLE2)

        self.assert_chunker_sample(chunker, VALID_DCAL_QUARTZ)
        self.assert_chunker_sample_with_noise(chunker, VALID_DCAL_QUARTZ)
        self.assert_chunker_fragmented_sample(chunker, VALID_DCAL_QUARTZ, 64)
        self.assert_chunker_combined_sample(chunker, VALID_DCAL_QUARTZ)

        self.assert_chunker_sample(chunker, VALID_DCAL_STRAIN)
        self.assert_chunker_sample_with_noise(chunker, VALID_DCAL_STRAIN)
        self.assert_chunker_fragmented_sample(chunker, VALID_DCAL_STRAIN, 64)
        self.assert_chunker_combined_sample(chunker, VALID_DCAL_STRAIN)

        # self.assert_chunker_sample(chunker, VALID_STATUS_RESPONSE)
        # self.assert_chunker_sample_with_noise(chunker, VALID_STATUS_RESPONSE)
        # self.assert_chunker_fragmented_sample(chunker, VALID_STATUS_RESPONSE, 64)
        # self.assert_chunker_combined_sample(chunker, VALID_STATUS_RESPONSE)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, VALID_SAMPLE, self.assert_particle_sample, True)
        self.assert_particle_published(driver, VALID_SAMPLE2, self.assert_particle_sample, True)
        self.assert_particle_published(driver, VALID_DCAL_STRAIN, self.assert_particle_calibration_strain, True)
        self.assert_particle_published(driver, VALID_DCAL_QUARTZ, self.assert_particle_calibration_quartz, True)
        self.assert_particle_published(driver, VALID_STATUS_RESPONSE, self.assert_particle_status, True)

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected.  All states defined in this dict must
        also be defined in the protocol FSM.
        """
        capabilities = {
            ProtocolState.UNKNOWN: [ProtocolEvent.DISCOVER],
            ProtocolState.COMMAND: [ProtocolEvent.ACQUIRE_SAMPLE,
                                    ProtocolEvent.ACQUIRE_STATUS,
                                    ProtocolEvent.CLOCK_SYNC,
                                    ProtocolEvent.GET,
                                    ProtocolEvent.SET,
                                    ProtocolEvent.START_AUTOSAMPLE,
                                    ProtocolEvent.START_DIRECT,
                                    ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                    ProtocolEvent.SCHEDULED_ACQUIRED_STATUS],
            ProtocolState.AUTOSAMPLE: [ProtocolEvent.GET,
                                       ProtocolEvent.STOP_AUTOSAMPLE,
                                       ProtocolEvent.SCHEDULED_ACQUIRED_STATUS,
                                       ProtocolEvent.SCHEDULED_CLOCK_SYNC],
            ProtocolState.DIRECT_ACCESS: [ProtocolEvent.STOP_DIRECT,
                                          ProtocolEvent.EXECUTE_DIRECT],
            ProtocolState.ACQUIRING_SAMPLE: [],
            ProtocolState.ACQUIRING_STATUS: []
        }

        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)

    def test_parse_set_response(self):
        """
        Test response from set commands.
        """
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, ProtocolState.COMMAND)

        response = "Not an error"
        driver._protocol._parse_set_response(response, Prompt.EXECUTED)
        driver._protocol._parse_set_response(response, Prompt.COMMAND)

        with self.assertRaises(InstrumentProtocolException):
            driver._protocol._parse_set_response(response, Prompt.BAD_COMMAND)

        response = "<ERROR type='INVALID ARGUMENT' msg='out of range'/>"
        with self.assertRaises(InstrumentParameterException):
            driver._protocol._parse_set_response(response, Prompt.EXECUTED)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        my_event_callback = Mock()
        protocol = SBE16Protocol(Prompt, NEWLINE, my_event_callback)
        driver_capabilities = Capability.list()
        test_capabilities = Capability.list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(driver_capabilities, protocol._filter_capabilities(test_capabilities))


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)       #
###############################################################################
@attr('INT', group='mi')
class Sbe16plusIntegrationTestCase(InstrumentDriverIntegrationTestCase, SeaBird16plusMixin):
    """
    Integration tests for the sbe16 driver. This class tests and shows
    use patterns for the sbe16 driver as a zmq driver process.
    """

    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

    def assert_set_clock(self, time_param, time_override=None, time_format="%d %b %Y %H:%M:%S",
                         tolerance=DEFAULT_CLOCK_DIFF):
        """
        Verify that we can set the clock
        @param time_param: driver parameter
        @param time_override: use this time instead of current time.
        @param time_format: date time format
        @param tolerance: how close to the set time should the get be?
        """
        # Some seabirds tick the clock the instant you set it.  So you set
        # time 1, the get would be time 2.  Others do it correctly and wait
        # for a second before ticking. Hence the default tolerance of 1.
        if time_override is None:
            set_time = get_timestamp_delayed(time_format)
        else:
            set_time = time.strftime(time_format, time.localtime(time_override))

        self.assert_set(time_param, set_time, no_get=True, startup=True)
        self.assertTrue(self._is_time_set(time_param, set_time, time_format, tolerance))

    def _is_time_set(self, time_param, expected_time, time_format="%d %b %Y %H:%M:%S", tolerance=DEFAULT_CLOCK_DIFF):
        """
        Verify is what we expect it to be within a given tolerance
        @param time_param: driver parameter
        @param expected_time: what the time should be in seconds since unix epoch or formatted time string
        @param time_format: date time format
        @param tolerance: how close to the set time should the get be?
        """
        log.debug("Expected time unformatted: %s", expected_time)

        result_time = self.assert_get(time_param)
        result_time_struct = time.strptime(result_time, time_format)
        converted_time = timegm_to_float(result_time_struct)

        if isinstance(expected_time, float):
            expected_time_struct = time.localtime(expected_time)
        else:
            expected_time_struct = time.strptime(expected_time, time_format)

        log.debug("Current Time: %s, Expected Time: %s", time.strftime("%d %b %y %H:%M:%S", result_time_struct),
                  time.strftime("%d %b %y %H:%M:%S", expected_time_struct))

        log.debug("Current Time: %s, Expected Time: %s, Tolerance: %s",
                  converted_time, timegm_to_float(expected_time_struct), tolerance)

        # Verify the clock is set within the tolerance
        return abs(converted_time - timegm_to_float(expected_time_struct)) <= tolerance

    def assert_clock_set(self, time_param, sync_clock_cmd=DriverEvent.ACQUIRE_STATUS, timeout=60,
                         tolerance=DEFAULT_CLOCK_DIFF):
        """
        Verify the clock is set to at least the current date
        """
        log.debug("verify clock is set to the current time")

        timeout_time = time.time() + timeout

        while not self._is_time_set(time_param, timegm_to_float(time.gmtime()), tolerance=tolerance):
            log.debug("time isn't current. sleep for a bit")

            # Run acquire status command to set clock parameter
            self.assert_driver_command(sync_clock_cmd)

            log.debug("T: %s T: %s", time.time(), timeout_time)
            time.sleep(5)
            self.assertLess(time.time(), timeout_time, msg="Timeout waiting for clock sync event")

    def test_parameters(self):
        """
        Test driver parameters and verify their type.  Startup parameters also verify the parameter
        value.  This test confirms that parameters are being read/converted properly and that
        the startup has been applied.
        """
        self.assert_initialize_driver()
        reply = self.driver_client.cmd_dvr('get_resource', Parameter.ALL)
        self.assert_driver_parameters(reply, True)

    def test_set(self):
        """
        Test all set commands. Verify all exception cases.
        """
        self.assert_initialize_driver()

        # Verify we can set all parameters in bulk
        new_values = {
            Parameter.INTERVAL: 20,
            Parameter.PUMP_MODE: 0,
            Parameter.NCYCLES: 6
        }
        self.assert_set_bulk(new_values)

        # Pump Mode
        # x=0: No pump.
        # x=1: Run pump for 0.5 sec before each sample.
        # x=2: Run pump during each sample.
        self.assert_set(Parameter.PUMP_MODE, 0)
        self.assert_set(Parameter.PUMP_MODE, 1)
        self.assert_set(Parameter.PUMP_MODE, 2)
        self.assert_set_exception(Parameter.PUMP_MODE, -1)
        self.assert_set_exception(Parameter.PUMP_MODE, 3)
        self.assert_set_exception(Parameter.PUMP_MODE, 'bad')

        # NCYCLE Range 1 - 100
        self.assert_set(Parameter.NCYCLES, 1)
        self.assert_set(Parameter.NCYCLES, 100)
        self.assert_set_exception(Parameter.NCYCLES, 0)
        self.assert_set_exception(Parameter.NCYCLES, 101)
        self.assert_set_exception(Parameter.NCYCLES, -1)
        self.assert_set_exception(Parameter.NCYCLES, 0.1)
        self.assert_set_exception(Parameter.NCYCLES, 'bad')

        # SampleInterval Range 10 - 14,400
        self.assert_set(Parameter.INTERVAL, 10)
        self.assert_set(Parameter.INTERVAL, 14400)
        self.assert_set_exception(Parameter.INTERVAL, 9)
        self.assert_set_exception(Parameter.INTERVAL, 14401)
        self.assert_set_exception(Parameter.INTERVAL, -1)
        self.assert_set_exception(Parameter.INTERVAL, 0.1)
        self.assert_set_exception(Parameter.INTERVAL, 'bad')

        # Read only parameters
        self.assert_set_readonly(Parameter.ECHO, False)
        self.assert_set_readonly(Parameter.OUTPUT_EXEC_TAG, False)
        self.assert_set_readonly(Parameter.TXREALTIME, False)
        self.assert_set_readonly(Parameter.BIOWIPER, False)
        self.assert_set_readonly(Parameter.PTYPE, 1)
        self.assert_set_readonly(Parameter.VOLT0, False)
        self.assert_set_readonly(Parameter.VOLT1, False)
        self.assert_set_readonly(Parameter.VOLT2, False)
        self.assert_set_readonly(Parameter.VOLT3, False)
        self.assert_set_readonly(Parameter.VOLT4, False)
        self.assert_set_readonly(Parameter.VOLT5, False)
        self.assert_set_readonly(Parameter.DELAY_BEFORE_SAMPLE, 1)
        self.assert_set_readonly(Parameter.DELAY_AFTER_SAMPLE, 1)
        self.assert_set_readonly(Parameter.SBE63, False)
        self.assert_set_readonly(Parameter.SBE38, False)
        self.assert_set_readonly(Parameter.SBE50, False)
        self.assert_set_readonly(Parameter.WETLABS, False)
        self.assert_set_readonly(Parameter.GTD, False)
        self.assert_set_readonly(Parameter.OPTODE, False)
        self.assert_set_readonly(Parameter.SYNCMODE, False)
        self.assert_set_readonly(Parameter.SYNCWAIT, 1)
        self.assert_set_readonly(Parameter.OUTPUT_FORMAT, 1)
        self.assert_set_readonly(Parameter.LOGGING, False)

    def test_startup_params(self):
        """
        Verify that startup parameters are applied correctly. Generally this
        happens in the driver discovery method.
        """

        # Explicitly verify these values after discover.  They should match
        # what the startup values should be
        get_values = {
            Parameter.INTERVAL: 10,
            Parameter.PUMP_MODE: 2,
            Parameter.NCYCLES: 4
        }

        # Change the values of these parameters to something before the
        # driver is reinitalized.  They should be blown away on reinit.
        new_values = {
            Parameter.INTERVAL: 20,
            Parameter.PUMP_MODE: 0,
            Parameter.NCYCLES: 6
        }

        self.assert_initialize_driver()
        self.assert_startup_parameters(self.assert_driver_parameters, new_values, get_values)

        # Start autosample and try again
        self.assert_set_bulk(new_values)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_startup_parameters(self.assert_driver_parameters)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

    def test_commands(self):
        """
        Run instrument commands from both command and streaming mode.
        """
        self.assert_initialize_driver()

        ####
        # First test in command mode
        ####
        self.assert_driver_command(ProtocolEvent.CLOCK_SYNC)
        self.assert_driver_command(ProtocolEvent.SCHEDULED_CLOCK_SYNC)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, regex=r'serial sync mode')

        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, regex=r'serial sync mode')

        ####
        # Test in streaming mode
        ####
        # Put us in streaming
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)

        self.assert_driver_command(ProtocolEvent.SCHEDULED_CLOCK_SYNC)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, regex=r'serial sync mode')

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

        ####
        # Test a bad command
        ####
        self.assert_driver_command_exception('ima_bad_command', exception_class=InstrumentCommandException)

    def test_autosample(self):
        """
        Verify that we can enter streaming and that all particles are produced
        properly.

        Because we have to test for three different data particles we can't use
        the common assert_sample_autosample method
        """
        self.assert_initialize_driver()
        self.assert_set(Parameter.INTERVAL, 10)

        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_async_particle_generation(DataParticleType.CTD_PARSED, self.assert_particle_sample, timeout=60)

        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_STATUS,
                                        self.assert_particle_status)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

    def test_polled(self):
        """
        Test that we can generate particles with commands
        """
        self.assert_initialize_driver()

        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_STATUS,
                                        self.assert_particle_status)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_SAMPLE, DataParticleType.CTD_PARSED,
                                        self.assert_particle_sample)

    ###
    #   Test scheduled events
    ###
    def assert_calibration_coefficients(self):
        """
        Verify a calibration particle was generated
        """
        self.clear_events()
        self.assert_async_particle_generation(DataParticleType.DEVICE_CALIBRATION,
                                              self.assert_particle_calibration_strain, timeout=120)

    def assert_acquire_status(self):
        """
        Verify a status particle was generated
        """
        self.clear_events()
        self.assert_async_particle_generation(DataParticleType.DEVICE_STATUS, self.assert_particle_status, timeout=120)

    def test_scheduled_device_status_command(self):
        """
        Verify the device status command can be triggered and run in command
        """
        self.assert_scheduled_event(ScheduledJob.ACQUIRE_STATUS, self.assert_acquire_status, delay=120)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_scheduled_device_status_autosample(self):
        """
        Verify the device status command can be triggered and run in autosample
        """
        self.assert_scheduled_event(ScheduledJob.ACQUIRE_STATUS, self.assert_acquire_status,
                                    autosample_command=ProtocolEvent.START_AUTOSAMPLE, delay=180)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)

    def test_scheduled_clock_sync_command(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        timeout = 120
        self.assert_scheduled_event(ScheduledJob.CLOCK_SYNC, delay=timeout)
        self.assert_current_state(ProtocolState.COMMAND)

        # Set the clock to some time in the past
        # Need an easy way to do this now that DATE_TIME is read only
        # self.assert_set_clock(Parameter.DATE_TIME, time_override=SBE_EPOCH)

        # Check the clock until it is set correctly (by a scheduled event)
        # self.assert_clock_set(Parameter.DATE_TIME, sync_clock_cmd=ProtocolEvent.GET_CONFIGURATION, timeout=timeout)

    def test_scheduled_clock_sync_autosample(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        timeout = 240
        self.assert_scheduled_event(ScheduledJob.CLOCK_SYNC, delay=timeout)
        self.assert_current_state(ProtocolState.COMMAND)

        # Set the clock to some time in the past
        # Need an easy way to do this now that DATE_TIME is read only
        # self.assert_set_clock(Parameter.DATE_TIME, time_override=SBE_EPOCH)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE)

        # Check the clock until it is set correctly (by a scheduled event)
        # self.assert_clock_set(Parameter.DATE_TIME, sync_clock_cmd=ProtocolEvent.GET_CONFIGURATION, timeout=timeout,
        # tolerance=10)

    def assert_cycle(self):
        self.assert_current_state(ProtocolState.COMMAND)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

        self.assert_async_particle_generation(DataParticleType.CTD_PARSED, self.assert_particle_sample,
                                              particle_count=6, timeout=60)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_STATUS,
                                        self.assert_particle_status)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_discover(self):
        """
        Verify we can discover from both command and auto sample modes
        """
        self.assert_initialize_driver()
        self.assert_cycle()
        self.assert_cycle()

    def test_metadata(self):
        metadata = self.driver_client.cmd_dvr('get_config_metadata')
        self.assertEqual(metadata, None)  # must be connected
        self.assert_initialize_driver()
        metadata = self.driver_client.cmd_dvr('get_config_metadata')
        log.debug("Metadata: %s", metadata)
        self.assertTrue(isinstance(metadata, basestring))


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################

@attr('QUAL', group='mi')
class Sbe16plusQualTestCase(InstrumentDriverQualificationTestCase, SeaBird16plusMixin):
    """Qualification Test Container"""

    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)

    def test_autosample(self):
        """
        Verify autosample works and data particles are created
        """
        self.assert_enter_command_mode()
        self.assert_set_parameter(Parameter.INTERVAL, 10)

        self.assert_start_autosample()
        self.assert_particle_async(DataParticleType.CTD_PARSED, self.assert_particle_sample)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status,
                                    DataParticleType.DEVICE_STATUS, sample_count=1, timeout=20)

        # Stop autosample and do run a couple commands.
        self.assert_stop_autosample()

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status,
                                    DataParticleType.DEVICE_STATUS, sample_count=1)

        # Restart autosample and gather a couple samples
        self.assert_sample_autosample(self.assert_particle_sample, DataParticleType.CTD_PARSED)

    def assert_cycle(self):
        self.assert_start_autosample()

        self.assert_particle_async(DataParticleType.CTD_PARSED, self.assert_particle_sample)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status,
                                    DataParticleType.DEVICE_STATUS, sample_count=1, timeout=20)

        self.assert_stop_autosample()

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status,
                                    DataParticleType.DEVICE_STATUS, sample_count=1)

    def test_cycle(self):
        """
        Verify we can bounce between command and streaming.  We try it a few times to see if we can find a timeout.
        """
        self.assert_enter_command_mode()

        self.assert_cycle()
        self.assert_cycle()
        self.assert_cycle()
        self.assert_cycle()

    def test_poll(self):
        """
        Verify that we can poll for a sample.  Take sample for this instrument
        Also poll for other engineering data streams.
        """
        self.assert_enter_command_mode()

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_SAMPLE, self.assert_particle_sample,
                                    DataParticleType.CTD_PARSED, sample_count=1)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status,
                                    DataParticleType.DEVICE_STATUS, sample_count=1)

    def test_direct_access_telnet_mode(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports direct access to the physical
        instrument. (telnet mode)
        """
        self.assert_enter_command_mode()
        self.assert_set_parameter(Parameter.INTERVAL, 10)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)
        self.tcp_client.send_data("%sampleinterval=97%s" % (NEWLINE, NEWLINE))
        self.tcp_client.expect(Prompt.EXECUTED)

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_enter_command_mode()
        self.assert_get_parameter(Parameter.INTERVAL, 10)

    def test_execute_clock_sync(self):
        """
        Verify we can synchronize the instrument internal clock
        """
        self.assert_enter_command_mode()

        self.assert_execute_resource(ProtocolEvent.CLOCK_SYNC)

        # get the time from the driver
        check_new_params = self.instrument_agent_client.get_resource([Parameter.DATE_TIME])
        # convert driver's time from formatted date/time string to seconds integer
        instrument_time = timegm_to_float(
            time.strptime(check_new_params.get(Parameter.DATE_TIME).lower(), "%d %b %Y %H:%M:%S"))

        # need to convert local machine's time to date/time string and back to seconds to 'drop' the DST attribute so
        # test passes
        # get time from local machine
        lt = time.strftime("%d %b %Y %H:%M:%S", time.gmtime(timegm_to_float(time.localtime())))
        # convert local time from formatted date/time string to seconds integer to drop DST
        local_time = timegm_to_float(time.strptime(lt, "%d %b %Y %H:%M:%S"))

        # Now verify that the time matches to within 15 seconds
        self.assertLessEqual(abs(instrument_time - local_time), 15)

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
                ProtocolEvent.GET,
                ProtocolEvent.SET,
                ProtocolEvent.CLOCK_SYNC,
                ProtocolEvent.ACQUIRE_STATUS,
                ProtocolEvent.START_AUTOSAMPLE,
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
            ProtocolEvent.GET,
            ProtocolEvent.STOP_AUTOSAMPLE,
            ProtocolEvent.ACQUIRE_STATUS]

        self.assert_start_autosample()
        self.assert_capabilities(capabilities)
        self.assert_stop_autosample()

        ##################
        #  DA Mode
        ##################

        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.DIRECT_ACCESS)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = self._common_da_resource_commands()

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
