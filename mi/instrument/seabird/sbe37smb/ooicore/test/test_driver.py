#!/usr/bin/env python

"""
@package ion.services.mi.instrument.sbe37.test.test_sbe37_driver
@file ion/services/mi/instrument/sbe37/test/test_sbe37_driver.py
@author Edward Hunter
@brief Test cases for InstrumentDriver
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Ensure the test class is monkey patched for gevent


import json

import gevent
from mock import Mock
from gevent import monkey;

monkey.patch_all()
from pprint import PrettyPrinter

# Standard lib imports
import time
import unittest

# 3rd party imports
from nose.plugins.attrib import attr


from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverConnectionState

from mi.core.exceptions import InstrumentTimeoutException
from mi.core.exceptions import SampleException

from mi.instrument.seabird.sbe37smb.ooicore.test.sample_data import *

from mi.instrument.seabird.sbe37smb.ooicore.driver import DataParticleType
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolState
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Capability
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37DataParticle
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37DataParticleKey
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37DeviceCalibrationParticle
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37DeviceCalibrationParticleKey
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37DeviceStatusParticleKey
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Driver

from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue

from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import ParameterTestConfigKey

from mi.instrument.seabird.test.test_driver import SeaBirdUnitTest
from mi.instrument.seabird.test.test_driver import SeaBirdIntegrationTest
from mi.instrument.seabird.test.test_driver import SeaBirdQualificationTest

# MI logger
from mi.core.log import get_logger ; log = get_logger()
# from interface.objects import AgentCommand

from mi.core.instrument.instrument_driver import DriverEvent

from mi.core.instrument.instrument_driver import DriverProtocolState

from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import ResourceAgentEvent


from mi.core.exceptions import BadRequest
from mi.core.exceptions import Conflict
from mi.core.exceptions import ResourceError

# from interface.objects import CapabilityType
# from interface.objects import AgentCapability
from mi.idk.unit_test import DriverStartupConfigKey

###
#   Driver parameters for the tests
###

# Create some short names for the parameter test config
TYPE = ParameterTestConfigKey.TYPE
READONLY = ParameterTestConfigKey.READONLY
STARTUP = ParameterTestConfigKey.STARTUP
DA = ParameterTestConfigKey.DIRECT_ACCESS
VALUE = ParameterTestConfigKey.VALUE
REQUIRED = ParameterTestConfigKey.REQUIRED
DEFAULT = ParameterTestConfigKey.DEFAULT

# Make tests verbose and provide stdout
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_process
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_config
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_connect
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_get_set
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_poll
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_autosample
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_test
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_errors
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_discover_autosample
# bin/nosetests -s -v mi/instrument/seabird/sbe37smb/ooicore/test/test_driver.py:SBEIntTestCase.test_lost_connection


## Initialize the test parameters
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.seabird.sbe37smb.ooicore.driver',
    driver_class="SBE37Driver",

    instrument_agent_resource_id = '123xyz',
    instrument_agent_preload_id = 'IA2',
    instrument_agent_name = 'Agent007',
    instrument_agent_packet_config = DataParticleType(),
    driver_startup_config = {
        DriverStartupConfigKey.PARAMETERS: {
            SBE37Parameter.INTERVAL: 1,
        },
    }
)
#

# Used to validate param config retrieved from driver.
PARAMS = {
    SBE37Parameter.OUTPUTSAL : bool,
    SBE37Parameter.OUTPUTSV : bool,
    SBE37Parameter.NAVG : int,
    SBE37Parameter.SAMPLENUM : int,
    SBE37Parameter.INTERVAL : int,
    SBE37Parameter.STORETIME : bool,
    SBE37Parameter.TXREALTIME : bool,
    SBE37Parameter.SYNCMODE : bool,
    SBE37Parameter.SYNCWAIT : int,
    SBE37Parameter.TCALDATE : tuple,
    SBE37Parameter.TA0 : float,
    SBE37Parameter.TA1 : float,
    SBE37Parameter.TA2 : float,
    SBE37Parameter.TA3 : float,
    SBE37Parameter.CCALDATE : tuple,
    SBE37Parameter.CG : float,
    SBE37Parameter.CH : float,
    SBE37Parameter.CI : float,
    SBE37Parameter.CJ : float,
    SBE37Parameter.WBOTC : float,
    SBE37Parameter.CTCOR : float,
    SBE37Parameter.CPCOR : float,
    SBE37Parameter.PCALDATE : tuple,
    SBE37Parameter.PA0 : float,
    SBE37Parameter.PA1 : float,
    SBE37Parameter.PA2 : float,
    SBE37Parameter.PTCA0 : float,
    SBE37Parameter.PTCA1 : float,
    SBE37Parameter.PTCA2 : float,
    SBE37Parameter.PTCB0 : float,
    SBE37Parameter.PTCB1 : float,
    SBE37Parameter.PTCB2 : float,
    SBE37Parameter.POFFSET : float,
    SBE37Parameter.RCALDATE : tuple,
    SBE37Parameter.RTCA0 : float,
    SBE37Parameter.RTCA1 : float,
    SBE37Parameter.RTCA2 : float
}



class SBEMixin(DriverTestMixin):
    '''
    Mixin class used for storing data particle constance and common data assertion methods.
    '''
    ###
    #  Parameter and Type Definitions
    ###
    _driver_parameters = {
        # DS parameters
        SBE37Parameter.OUTPUTSAL: {TYPE: bool, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        SBE37Parameter.OUTPUTSV: {TYPE: bool, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        SBE37Parameter.NAVG : {TYPE: int, READONLY: False, DA: True, STARTUP: False, REQUIRED: False},
        SBE37Parameter.SAMPLENUM : {TYPE: int, READONLY: False, DA: False, STARTUP: True, REQUIRED: False, VALUE: False},
        SBE37Parameter.INTERVAL : {TYPE: int, READONLY: False, DA: False, STARTUP: True, REQUIRED: False, VALUE: 1},
        SBE37Parameter.STORETIME : {TYPE: bool, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        SBE37Parameter.TXREALTIME : {TYPE: bool, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        SBE37Parameter.SYNCMODE : {TYPE: bool, READONLY: False, DA: False, STARTUP: False, DEFAULT: False},
        SBE37Parameter.SYNCWAIT : {TYPE: int, READONLY: False, DA: False, STARTUP: True, REQUIRED: False}, # may need a default , VALUE: 1
        # DC parameters
        SBE37Parameter.TCALDATE : {TYPE: tuple, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.TA0 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.TA1 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.TA2 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.TA3 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.CCALDATE : {TYPE: tuple, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.CG : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.CH : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.CI : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.CJ : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.WBOTC : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.CTCOR : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.CPCOR : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PCALDATE : {TYPE: tuple, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PA0 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PA1 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PA2 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PTCA0 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PTCA1 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PTCA2 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PTCB0 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PTCB1 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.PTCB2 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.POFFSET : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.RCALDATE : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.RTCA0 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.RTCA1 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},
        SBE37Parameter.RTCA2 : {TYPE: float, READONLY: False, DA: False, STARTUP: False, REQUIRED: False},          
    }

    _sample_parameters = {
        SBE37DataParticleKey.TEMP: {TYPE: float, VALUE: 55.9044, REQUIRED: False },
        SBE37DataParticleKey.CONDUCTIVITY: {TYPE: float, VALUE: 41.40609, REQUIRED: False },
        SBE37DataParticleKey.DEPTH: {TYPE: float, VALUE: 572.170, REQUIRED: False }
    }
    
    _device_calibration_parameters = {
        SBE37DeviceCalibrationParticleKey.TCALDATE:  {TYPE: tuple, VALUE: (8, 11, 2005), REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.TA0: {TYPE: float, VALUE: -2.572242e-04, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.TA1: {TYPE: float, VALUE: 3.138936e-04, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.TA2: {TYPE: float, VALUE: -9.717158e-06, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.TA3:  {TYPE: float, VALUE: 2.138735e-07, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.CCALDATE: {TYPE: tuple, VALUE: (8, 11, 2005), REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.G: {TYPE: float, VALUE: -9.870930e-01, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.H: {TYPE: float, VALUE: 1.417895e-01, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.I: {TYPE: float, VALUE: 1.334915e-04, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.J: {TYPE: float, VALUE: 3.339261e-05, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.CPCOR: {TYPE: float, VALUE: 9.570000e-08, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.CTCOR: {TYPE: float, VALUE: 3.250000e-06, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.WBOTC: {TYPE: float, VALUE: 1.202400e-05, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PCALDATE: {TYPE: tuple, VALUE: (12, 8, 2005), REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PRANGE: {TYPE: float, VALUE: 10847.1964958, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PSN: {TYPE: int, VALUE: 4955, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PA0: {TYPE: float, VALUE: 5.916199e+00, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PA1: {TYPE: float, VALUE: 4.851819e-01, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PA2: {TYPE: float, VALUE: 4.596432e-07, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PTCA0: {TYPE: float, VALUE: 2.762492e+02, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PTCA1: {TYPE: float, VALUE: 6.603433e-01, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PTCA2: {TYPE: float, VALUE: 5.756490e-03, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PTCSB0: {TYPE: float, VALUE: 2.461450e+01, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PTCSB1: {TYPE: float, VALUE: -9.000000e-04, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.PTCSB2: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.POFFSET: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.RTC: {TYPE: tuple, VALUE: (8, 11, 2005), REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.RTCA0: {TYPE: float, VALUE: 9.999862e-01, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.RTCA1: {TYPE: float, VALUE: 1.686132e-06, REQUIRED: False },
        SBE37DeviceCalibrationParticleKey.RTCA2: {TYPE: float, VALUE: -3.022745e-08, REQUIRED: False },
    }
    
    _device_status_parameters = {
        SBE37DeviceStatusParticleKey.SERIAL_NUMBER: {TYPE: int, VALUE: 2165, REQUIRED: False },
        SBE37DeviceStatusParticleKey.DATE_TIME: {TYPE: float, VALUE: 3569109103.0, REQUIRED: False },        
        SBE37DeviceStatusParticleKey.LOGGING: {TYPE: bool, VALUE: False, REQUIRED: False },
        SBE37DeviceStatusParticleKey.SAMPLE_INTERVAL: {TYPE: int, VALUE: 20208, REQUIRED: False },
        SBE37DeviceStatusParticleKey.SAMPLE_NUMBER: {TYPE: int, VALUE: 0, REQUIRED: False },
        SBE37DeviceStatusParticleKey.MEMORY_FREE: {TYPE: int, VALUE: 200000, REQUIRED: False },
        SBE37DeviceStatusParticleKey.TX_REALTIME: {TYPE: bool, VALUE: True, REQUIRED: False },
        SBE37DeviceStatusParticleKey.OUTPUT_SALINITY: {TYPE: bool, VALUE: False, REQUIRED: False },
        SBE37DeviceStatusParticleKey.OUTPUT_SOUND_VELOCITY: {TYPE: bool, VALUE: False, REQUIRED: False },
        SBE37DeviceStatusParticleKey.STORE_TIME: {TYPE: bool, VALUE: False, REQUIRED: False },
        SBE37DeviceStatusParticleKey.NUMBER_OF_SAMPLES_TO_AVERAGE: {TYPE: int, VALUE: 0, REQUIRED: False },
        SBE37DeviceStatusParticleKey.REFERENCE_PRESSURE: {TYPE: float, VALUE: 0.0, REQUIRED: False },
        SBE37DeviceStatusParticleKey.SERIAL_SYNC_MODE: {TYPE: bool, VALUE: False, REQUIRED: False },
        SBE37DeviceStatusParticleKey.SERIAL_SYNC_WAIT: {TYPE: int, VALUE: 0, REQUIRED: False },
        SBE37DeviceStatusParticleKey.INTERNAL_PUMP: {TYPE: bool, VALUE: True, REQUIRED: False },
        SBE37DeviceStatusParticleKey.TEMPERATURE: {TYPE: float, VALUE: 7.54, REQUIRED: False },
    }
    
    
    

    ###
    #   Driver Parameter Methods
    ###
    def assert_driver_parameters(self, current_parameters, verify_values = False):
        '''
        Verify that all driver parameters are correct and potentially verify values.
        @param current_parameters: driver parameters read from the driver instance
        @param verify_values: should we verify values against definition?
        '''
        self.assert_parameters(current_parameters, self._driver_parameters, verify_values)

    ###
    #   Data Particle Parameters Methods
    ###
    def assert_sample_data_particle(self, data_particle):
        '''
        Verify a particle is a know particle to this driver and verify the particle is
        correct
        @param data_particle: Data particle of unkown type produced by the driver
        '''
        if (isinstance(data_particle, SBE37DataParticle)):
            self.assert_particle_sample(data_particle)
        elif (isinstance(data_particle, SBE37DeviceCalibrationParticle)):
            self.assert_particle_device_calibration(data_particle)
        elif (isinstance(data_particle, SBE37DeviceStatusParticleKey)):
            self.assert_particle_device_status(data_particle)
        else:
            log.error("Unknown Particle Detected: %s" % data_particle)
            self.assertFalse(True)

    def assert_particle_sample(self, data_particle, verify_values = False):
        '''
        Verify a take sample data particle
        @param data_particle:  SBE26plusTideSampleDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_header(data_particle, DataParticleType.PARSED)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters, verify_values)

    def assert_particle_device_calibration(self, data_particle, verify_values = False):
        '''
        Verify a take sample data particle
        @param data_particle:  SBE26plusDeviceCalibrationDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._device_calibration_parameters, verify_values)

    def assert_particle_device_status(self, data_particle, verify_values = False):
        '''
        Verify a take sample data particle
        @param data_particle:  SBE26plusDeviceStatusDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_STATUS)
        self.assert_data_particle_parameters(data_particle, self._device_status_parameters, verify_values)



@attr('UNIT', group='mi')
class SBEUnitTestCase(SeaBirdUnitTest, SBEMixin):
    """
    Unit Test Container
    """
    
    def setUp(self):
        SeaBirdUnitTest.setUp(self)
        
    TEST_OVERLAY_CONFIG = [SBE37Parameter.SAMPLENUM, SBE37Parameter.INTERVAL]

    TEST_BASELINE_CONFIG = {
        SBE37Parameter.OUTPUTSAL : True,
        SBE37Parameter.OUTPUTSV : True,
        SBE37Parameter.NAVG : 1,
        SBE37Parameter.SAMPLENUM : 1,
        SBE37Parameter.INTERVAL : 1,
        SBE37Parameter.STORETIME : True,
        SBE37Parameter.TXREALTIME : True,
        SBE37Parameter.SYNCMODE : True,
        SBE37Parameter.SYNCWAIT : 1,
        SBE37Parameter.TCALDATE : (1,1),
        SBE37Parameter.TA0 : 1.0,
        SBE37Parameter.TA1 : 1.0,
        SBE37Parameter.TA2 : 1.0,
        SBE37Parameter.TA3 : 1.0,
        SBE37Parameter.CCALDATE : (1,1),
        SBE37Parameter.CG : 1.0,
        SBE37Parameter.CH : 1.0,
        SBE37Parameter.CI : 1.0,
        SBE37Parameter.CJ : 1.0,
        SBE37Parameter.WBOTC : 1.0,
        SBE37Parameter.CTCOR : 1.0,
        SBE37Parameter.CPCOR : 1.0,
        SBE37Parameter.PCALDATE : (1,1),
        SBE37Parameter.PA0 : 1.0,
        SBE37Parameter.PA1 : 1.0,
        SBE37Parameter.PA2 : 1.0,
        SBE37Parameter.PTCA0 : 1.0,
        SBE37Parameter.PTCA1 : 1.0,
        SBE37Parameter.PTCA2 : 1.0,
        SBE37Parameter.PTCB0 : 1.0,
        SBE37Parameter.PTCB1 : 1.0,
        SBE37Parameter.PTCB2 : 1.0,
        SBE37Parameter.POFFSET : 1.0,
        SBE37Parameter.RCALDATE : (1,1),
        SBE37Parameter.RTCA0 : 1.0,
        SBE37Parameter.RTCA1 : 1.0,
        SBE37Parameter.RTCA2 : 1.0
        }
    
    def test_zero_data(self):
        particle = SBE37DataParticle('#87.9140,5.42747, 556.864,   37.1829, 1506.961, 02 Jan 2001, 15:34:51',
                                     port_timestamp = 3558720820.531179)
        parsed = particle.generate()
        self.assertNotEquals(parsed, None)
        particle = SBE37DataParticle('#00.0000,5.42747, 556.864,   37.1829, 1506.961, 02 Jan 2001, 15:34:51',
                                     port_timestamp = 3558720820.531179)
        self.assertNotEquals(parsed, None)
        particle = SBE37DataParticle('#87.9140,0.00000, 556.864,   37.1829, 1506.961, 02 Jan 2001, 15:34:51',
                                     port_timestamp = 3558720820.531179)
        self.assertNotEquals(parsed, None)
        particle = SBE37DataParticle('#87.9140,5.42747, 000.000,   37.1829, 1506.961, 02 Jan 2001, 15:34:51',
                                     port_timestamp = 3558720820.531179)
        self.assertNotEquals(parsed, None)
        
        # garbage is not okay
        particle = SBE37DataParticle('#fo.oooo,5.42747, 556.864,   37.1829, 1506.961, 02 Jan 2001, 15:34:51',
                                     port_timestamp = 3558720820.531179)
        with self.assertRaises(SampleException):
            particle.generate()
        particle = SBE37DataParticle('#87.9140,f.ooooo, 556.864,   37.1829, 1506.961, 02 Jan 2001, 15:34:51',
                                     port_timestamp = 3558720820.531179)
        with self.assertRaises(SampleException):
            particle.generate()
        particle = SBE37DataParticle('#87.9140,5.42747, foo.ooo,   37.1829, 1506.961, 02 Jan 2001, 15:34:51',
                                     port_timestamp = 3558720820.531179)
        with self.assertRaises(SampleException):
            particle.generate()

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = SBE37Driver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)
        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, SAMPLE, self.assert_particle_sample, True)
        self.assert_particle_published(driver, SAMPLE_DC, self.assert_particle_device_calibration, True)
        self.assert_particle_published(driver, SAMPLE_DS, self.assert_particle_device_status, True)

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = SBE37Driver(self._got_data_event_callback)

        config = driver.get_config_metadata()
        self.assertIsNotNone(config)

        pp = PrettyPrinter()
        log.debug("Config: %s", pp.pformat(config))

    def test_is_logging(self):
        """
        Test the is logging method.
        """
        driver = SBE37Driver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)
        
        self.assertIsNotNone(driver._protocol._is_logging(SAMPLE_DS))
        self.assertFalse(driver._protocol._is_logging(SAMPLE_DS))
        
        # mock up the "second opinion" response for the method under test
        driver._protocol._do_cmd_resp = Mock(return_value="Something\nlogging data\nsomething else")
        self.assertTrue(driver._protocol._is_logging("Something\nlogging data\nsomething else"))
        
        # mock up the "second opinion" response for the method under test
        driver._protocol._do_cmd_resp = Mock(return_value="bad data")
        self.assertIsNone(driver._protocol._is_logging("bad data"))

        sample_and_ds = """
#61.1459,24.45520, 356.906,   23.0652, 1506.848, 18 Jul 2013, 15:08:45
SBE37-SMP V 2.6 SERIAL NO. 2165   18 Jul 2013  15:08:45
logging data
sample interval = 5 seconds
samplenumber = 0, free = 200000
transmit real-time data
do not output salinity with each sample
do not output sound velocity with each sample
do not store time with each sample
number of samples to average = 1
reference pressure = 0.0 db
serial sync mode disabled
wait time after serial sync sampling = 0 seconds
internal pump is installed
temperature = 7.54 deg C
WARNING: LOW BATTERY VOLTAGE!!
"""
        self.assertTrue(driver._protocol._is_logging(sample_and_ds))

###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)       #
###############################################################################

@attr('INT', group='mi')
class SBEIntTestCase(SeaBirdIntegrationTest, SBEMixin):
    """
    Integration tests for the sbe37 driver. This class tests and shows
    use patterns for the sbe37 driver as a zmq driver process.
    """    
    def setUp(self):
        SeaBirdIntegrationTest.setUp(self)


    def assertSampleDict(self, val):
        """
        Verify the value is an SBE37DataParticle with a few key fields or a
        dict with 'raw' and 'parsed' tags.
        """
        
        if (isinstance(val, SBE37DataParticle)):
            raw_dict = json.loads(val.generate_raw())
            parsed_dict = json.loads(val.generate_parsed())
        else:
            self.assertTrue(val['raw'])
            raw_dict = val['raw']
            self.assertTrue(val['parsed'])
            parsed_dict = val['parsed']
            
        self.assertTrue(raw_dict[DataParticleKey.STREAM_NAME],
                        DataParticleValue.RAW)
        self.assertTrue(raw_dict[DataParticleKey.PKT_FORMAT_ID],
                        DataParticleValue.JSON_DATA)
        self.assertTrue(raw_dict[DataParticleKey.PKT_VERSION], 1)
        self.assertTrue(isinstance(raw_dict[DataParticleKey.VALUES],
                        list))
        
        self.assertTrue(parsed_dict[DataParticleKey.STREAM_NAME],
                        DataParticleType.PARSED)
        self.assertTrue(parsed_dict[DataParticleKey.PKT_FORMAT_ID],
                        DataParticleValue.JSON_DATA)
        self.assertTrue(parsed_dict[DataParticleKey.PKT_VERSION], 1)
        self.assertTrue(isinstance(parsed_dict[DataParticleKey.VALUES],
                        list))
        
    def assertParamDict(self, pd, all_params=False):
        """
        Verify all device parameters exist and are correct type.
        """
        if all_params:
            self.assertEqual(set(pd.keys()), set(PARAMS.keys()))
            for (key, type_val) in PARAMS.iteritems():
                self.assertTrue(isinstance(pd[key], type_val))
        else:
            for (key, val) in pd.iteritems():
                self.assertTrue(PARAMS.has_key(key))
                self.assertTrue(isinstance(val, PARAMS[key]))
    
    def assertParamVals(self, params, correct_params):
        """
        Verify parameters take the correct values.
        """
        self.assertEqual(set(params.keys()), set(correct_params.keys()))
        for (key, val) in params.iteritems():
            correct_val = correct_params[key]
            if isinstance(val, float):
                # Verify to 5% of the larger value.
                max_val = max(abs(val), abs(correct_val))
                self.assertAlmostEqual(val, correct_val, delta=max_val*.01)

            else:
                # int, bool, str, or tuple of same
                self.assertEqual(val, correct_val)

    def test_configuration(self):
        """
        Test to configure the driver process for device comms and transition
        to disconnected state.
        """

        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self.driver_client.cmd_dvr('initialize')

        # Test the driver returned state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

    def test_connect(self):
        """
        Test configuring and connecting to the device through the port
        agent. Discover device state.
        """
        log.info("test_connect test started")

        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('discover_state')

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        self.assert_get(DriverParameter.ALL)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self.driver_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
        
    def test_get_set(self):
        """
        Test device parameter access.
        """

        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        reply = self.driver_client.cmd_dvr('connect')
                
        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)
                
        reply = self.driver_client.cmd_dvr('discover_state')

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Get all device parameters. Confirm all expected keys are retrived
        # and have correct type.
        reply = self.driver_client.cmd_dvr('get_resource', SBE37Parameter.ALL)
        self.assertParamDict(reply, True)

        # Remember original configuration.
        orig_config = reply
        
        # Grab a subset of parameters.
        params = [
            SBE37Parameter.TA0,
            SBE37Parameter.INTERVAL,
            SBE37Parameter.STORETIME,
            SBE37Parameter.TCALDATE
            ]
        reply = self.driver_client.cmd_dvr('get_resource', params)
        self.assertParamDict(reply)        

        # Remember the original subset.
        orig_params = reply
        
        # Construct new parameters to set.
        old_date = orig_params[SBE37Parameter.TCALDATE]
        new_params = {
            SBE37Parameter.TA0 : orig_params[SBE37Parameter.TA0] * 1.2,
            SBE37Parameter.INTERVAL : orig_params[SBE37Parameter.INTERVAL] + 1,
            SBE37Parameter.STORETIME : not orig_params[SBE37Parameter.STORETIME],
            SBE37Parameter.TCALDATE : (old_date[0], old_date[1], old_date[2] + 1)
        }

        # Set parameters and verify.
        reply = self.driver_client.cmd_dvr('set_resource', new_params)
        reply = self.driver_client.cmd_dvr('get_resource', params)
        self.assertParamVals(reply, new_params)
        
        # Restore original parameters and verify.
        reply = self.driver_client.cmd_dvr('set_resource', orig_params)
        reply = self.driver_client.cmd_dvr('get_resource', params)
        self.assertParamVals(reply, orig_params)

        # Retrieve the configuration and ensure it matches the original.
        # Remove samplenum as it is switched by autosample and storetime.
        reply = self.driver_client.cmd_dvr('get_resource', SBE37Parameter.ALL)
        reply.pop('SAMPLENUM')
        orig_config.pop('SAMPLENUM')
        self.assertParamVals(reply, orig_config)

        # Disconnect from the port agent.
        reply = self.driver_client.cmd_dvr('disconnect')
        
        # Test the driver is disconnected.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)
        
        # Deconfigure the driver.
        reply = self.driver_client.cmd_dvr('initialize')
        
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)        
    
    def test_autosample(self):
        """
        Test autosample mode.
        """
        
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('discover_state')

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)
        
        # Make sure the device parameters are set to sample frequently.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5
        }
        reply = self.driver_client.cmd_dvr('set_resource', params)
        
        reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.START_AUTOSAMPLE)

        # Test the driver is in autosample mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.AUTOSAMPLE)
        
        # Wait for a few samples to roll in.
        gevent.sleep(30)
        
        # Return to command mode. Catch timeouts and retry if necessary.
        count = 0
        while True:
            try:
                reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.STOP_AUTOSAMPLE)
            
            except InstrumentTimeoutException:
                count += 1
                if count >= 5:
                    self.fail('Could not wakeup device to leave autosample mode.')

            else:
                break

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Verify we received at least 2 samples.
        sample_events = [evt for evt in self.events if evt['type']==DriverAsyncEvent.SAMPLE]
        self.assertTrue(len(sample_events) >= 2)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self.driver_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

    @unittest.skip('Not supported by simulator and very long (> 5 min).')
    def test_test(self):
        """
        Test the hardware testing mode.
        """
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('discover_state')

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        start_time = time.time()
        reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.TEST)

        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.TEST)
        
        while state != SBE37ProtocolState.COMMAND:
            gevent.sleep(5)
            elapsed = time.time() - start_time
            log.info('Device testing %f seconds elapsed.' % elapsed)
            state = self.driver_client.cmd_dvr('get_resource_state')

        # Verify we received the test result and it passed.
        test_results = [evt for evt in self.events if evt['type']==DriverAsyncEvent.TEST_RESULT]
        self.assertTrue(len(test_results) == 1)
        self.assertEqual(test_results[0]['value']['success'], 'Passed')

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self.driver_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

    def test_errors(self):
        """
        Test response to erroneous commands and parameters.
        """
        
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Assert for an unknown driver command.
        with self.assertRaises(ResourceError):
            reply = self.driver_client.cmd_dvr('bogus_command')

        # Assert for a known command, invalid state.
        with self.assertRaises(Conflict):
            reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.ACQUIRE_SAMPLE)

        # Assert we forgot the comms parameter.
        with self.assertRaises(BadRequest):
            reply = self.driver_client.cmd_dvr('configure')

        # Assert we send a bad config object (not a dict).
        with self.assertRaises(BadRequest):
            BOGUS_CONFIG = 'not a config dict'            
            reply = self.driver_client.cmd_dvr('configure', BOGUS_CONFIG)
            
        # Assert we send a bad config object (missing addr value).
        with self.assertRaises(BadRequest):
            BOGUS_CONFIG = self.port_agent_comm_config().copy()
            BOGUS_CONFIG.pop('addr')
            reply = self.driver_client.cmd_dvr('configure', BOGUS_CONFIG)

        # Assert we send a bad config object (bad addr value).
        with self.assertRaises(BadRequest):
            BOGUS_CONFIG = self.port_agent_comm_config().copy()
            BOGUS_CONFIG['addr'] = ''
            reply = self.driver_client.cmd_dvr('configure', BOGUS_CONFIG)
        
        # Configure for comms.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Assert for a known command, invalid state.
        with self.assertRaises(Conflict):
            reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.ACQUIRE_SAMPLE)

        reply = self.driver_client.cmd_dvr('connect')
                
        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Assert for a known command, invalid state.
        with self.assertRaises(Conflict):
            reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.ACQUIRE_SAMPLE)
                
        reply = self.driver_client.cmd_dvr('discover_state')

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Poll for a sample and confirm result.
        reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.ACQUIRE_SAMPLE)
        self.assertIsNotNone(reply)

        # Assert for a known command, invalid state.
        with self.assertRaises(Conflict):
            reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.STOP_AUTOSAMPLE)
        
        # Assert for a known command, invalid state.
        with self.assertRaises(Conflict):
            reply = self.driver_client.cmd_dvr('connect')

        # Get all device parameters. Confirm all expected keys are retrived
        # and have correct type.
        reply = self.driver_client.cmd_dvr('get_resource', SBE37Parameter.ALL)
        self.assertParamDict(reply, True)
        
        # Assert get fails without a parameter.
        with self.assertRaises(BadRequest):
            reply = self.driver_client.cmd_dvr('get_resource')
            
        # Assert get fails without a bad parameter (not ALL or a list).
        with self.assertRaises(BadRequest):
            bogus_params = 'I am a bogus param list.'
            reply = self.driver_client.cmd_dvr('get_resource', bogus_params)
            
        # Assert get fails without a bad parameter (not ALL or a list).
        #with self.assertRaises(InvalidParameterValueError):
        with self.assertRaises(BadRequest):
            bogus_params = [
                'a bogus parameter name',
                SBE37Parameter.INTERVAL,
                SBE37Parameter.STORETIME,
                SBE37Parameter.TCALDATE
                ]
            reply = self.driver_client.cmd_dvr('get_resource', bogus_params)        
        
        # Assert we cannot set a bogus parameter.
        with self.assertRaises(BadRequest):
            bogus_params = {
                'a bogus parameter name' : 'bogus value'
            }
            reply = self.driver_client.cmd_dvr('set_resource', bogus_params)
            
        # Assert we cannot set a real parameter to a bogus value.
        with self.assertRaises(BadRequest):
            bogus_params = {
                SBE37Parameter.INTERVAL : 'bogus value'
            }
            reply = self.driver_client.cmd_dvr('set_resource', bogus_params)
        
        # Disconnect from the port agent.
        reply = self.driver_client.cmd_dvr('disconnect')
        
        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)
        
        # Deconfigure the driver.
        reply = self.driver_client.cmd_dvr('initialize')
        
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
    
    @unittest.skip('Not supported by simulator.')
    def test_discover_autosample(self):
        """
        Test the device can discover autosample mode.
        """
        
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('discover_state')

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)
        
        # Make sure the device parameters are set to sample frequently.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5
        }
        reply = self.driver_client.cmd_dvr('set_resource', params)
        
        reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.START_AUTOSAMPLE)

        # Test the driver is in autosample mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.AUTOSAMPLE)
    
        # Let a sample or two come in.
        gevent.sleep(30)
    
        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self.driver_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Wait briefly before we restart the comms.
        gevent.sleep(10)
    
        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        count = 0
        while True:
            try:        
                reply = self.driver_client.cmd_dvr('discover_state')

            except InstrumentTimeoutException:
                count += 1
                if count >=5:
                    self.fail('Could not discover device state.')

            else:
                break

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.AUTOSAMPLE)

        # Let a sample or two come in.
        # This device takes awhile to begin transmitting again after you
        # prompt it in autosample mode.
        gevent.sleep(30)

        # Return to command mode. Catch timeouts and retry if necessary.
        count = 0
        while True:
            try:
                reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.STOP_AUTOSAMPLE)
            
            except InstrumentTimeoutException:
                count += 1
                if count >= 5:
                    self.fail('Could not wakeup device to leave autosample mode.')

            else:
                break

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self.driver_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

    def test_polled_particle_generation(self):
        """
        Test that we can generate particles with commands
        """
        self.assert_initialize_driver()

        self.assert_particle_generation(SBE37ProtocolEvent.ACQUIRE_SAMPLE, DataParticleType.PARSED, self.assert_particle_sample)
        self.assert_particle_generation(SBE37ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_STATUS, self.assert_particle_device_status)
        self.assert_particle_generation(SBE37ProtocolEvent.ACQUIRE_CONFIGURATION, DataParticleType.DEVICE_CALIBRATION, self.assert_particle_device_calibration)

    def test_lost_connection(self):
        """
        Test that the driver responds correctly to lost connections.
        """
        
        # Test the driver is in state unconfigured.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('discover_state')

        # Test the driver is in command mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)
        
        # Make sure the device parameters are set to sample frequently.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5
        }
        reply = self.driver_client.cmd_dvr('set_resource', params)
        
        # Acquire a sample to know we're cooking with gas.
        reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.ACQUIRE_SAMPLE)
                
        # Stop the port agent out from under the driver.
        self.stop_port_agent()

        # Loop until we see the state change. This will cause the test
        # to timeout if it never happens.
        while True:
            state = self.driver_client.cmd_dvr('get_resource_state')
            if state == DriverConnectionState.DISCONNECTED:
                break
            else:
                gevent.sleep(1)

    def test_automatic_startup_params(self):
        """
        Verify that startup params are applied automatically when the driver is started.
        """
        self.assert_initialize_driver()
        self.assert_get(SBE37Parameter.INTERVAL, 1)

    def test_reachback_recovery(self):
        """
        Verify that reachback into old data.  Currently this is just spoofed in the driver.
        """
        self.assert_initialize_driver()
        recovery_start = 1
        recovery_end = 11

        # Make sure the device parameters are set to sample frequently.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5
        }
        reply = self.driver_client.cmd_dvr('set_resource', params)

        reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.START_AUTOSAMPLE)

        # Test the driver is in autosample mode.
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, SBE37ProtocolState.AUTOSAMPLE)

        reply = self.driver_client.cmd_dvr('execute_resource', SBE37Capability.GAP_RECOVERY, recovery_start, recovery_end)

        # Enough time for a sample event to roll in.
        gevent.sleep(12)

        samples = self.get_sample_events(DataParticleType.PARSED)

        recovered_particles = []

        for sample in samples:
            log.debug("Sample: %s", sample)
            value = sample.get('value')
            self.assertIsNotNone(value)

            particle = json.loads(value)
            self.assertIsNotNone(particle)

            log.debug("PA Timestamp: %s", particle['port_timestamp'])

            if particle['port_timestamp'] >= recovery_start and particle['port_timestamp'] <= recovery_end:
                recovered_particles.append(particle)

        # Ensure we detected all the samples we expect to recover.
        self.assertEqual(len(recovered_particles), 10)

###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################

@attr('QUAL', group='mi')
class SBEQualificationTestCase(SeaBirdQualificationTest, SBEMixin):
    """Qualification Test Container"""

    # Qualification tests live in the base class.  This class is extended
    # here so that when running this test from 'nosetests' all tests
    # (UNIT, INT, and QUAL) are run.
    def setUp(self):
        SeaBirdQualificationTest.setUp(self)

    def check_for_reused_values(self, obj):
        """
        @author Roger Unwin
        @brief  verifies that no two definitions resolve to the same value.
        @returns True if no reused values
        """
        match = 0
        outer_match = 0
        for i in [v for v in dir(obj) if not callable(getattr(obj,v))]:
            if i.startswith('_') == False:
                outer_match = outer_match + 1
                for j in [x for x in dir(obj) if not callable(getattr(obj,x))]:
                    if i.startswith('_') == False:
                        if getattr(obj, i) == getattr(obj, j):
                            match = match + 1
                            log.debug(str(i) + " == " + j + " (Looking for reused values)")

        # If this assert fails, then two of tte enumerations have an identical value...
        return match == outer_match

    @unittest.skip("Tested in the base class")
    def test_direct_access_telnet_mode_command(self):
        """
        @brief This test verifies that the Instrument Driver
               properly supports direct access to the physical
               instrument. (telnet mode)
        """
        ###
        # First test direct access and exit with a go command
        # call.  Also add a parameter change to verify DA
        # parameters are restored on DA exit.
        ###
        self.assert_enter_command_mode()
        self.assert_set_parameter(SBE37Parameter.INTERVAL, 10)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet()

        log.debug("DA Server Started.  Adjust DA Parameter.")
        self.tcp_client.send_data("%sinterval=97%s" % (NEWLINE, NEWLINE))
        self.tcp_client.send_data("%sds%s" % (NEWLINE, NEWLINE))
        self.tcp_client.expect("sample interval = 97")
        log.debug("DA Parameter Sample Interval Updated")

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_state_change(ResourceAgentState.COMMAND, SBE37ProtocolState.COMMAND, 10)
        self.assert_get_parameter(SBE37Parameter.INTERVAL, 10)

        ###
        # Test direct access inactivity timeout
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=30, session_timeout=90)
        self.assert_state_change(ResourceAgentState.COMMAND, SBE37ProtocolState.COMMAND, 60)

        ###
        # Test session timeout without activity
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=120, session_timeout=30)
        self.assert_state_change(ResourceAgentState.COMMAND, SBE37ProtocolState.COMMAND, 60)

        ###
        # Test direct access session timeout with activity
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=30, session_timeout=60)
        # Send some activity every 30 seconds to keep DA alive.
        for i in range(1, 2, 3):
            self.tcp_client.send_data(NEWLINE)
            log.debug("Sending a little keep alive communication, sleeping for 15 seconds")
            gevent.sleep(15)

        self.assert_state_change(ResourceAgentState.COMMAND, SBE37ProtocolState.COMMAND, 45)

        ###
        # Test direct access disconnect
        ###
        self.assert_direct_access_start_telnet()
        self.tcp_client.disconnect()
        self.assert_state_change(ResourceAgentState.COMMAND, SBE37ProtocolState.COMMAND, 30)

    def test_direct_access_telnet_mode_autosample(self):
        """
        @brief Same as the previous DA test except in this test
               we force the instrument into streaming when in
               DA.  Then we need to verify the transition back
               to the driver works as expected.
        """
        ###
        # First test direct access and exit with a go command
        # call.  Also add a parameter change to verify DA
        # parameters are restored on DA exit.
        ###
        self.assert_enter_command_mode()
        self.assert_set_parameter(SBE37Parameter.INTERVAL, 10)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet()

        log.debug("DA Server Started.  Adjust DA Parameter.")
        self.tcp_client.send_data("%sinterval=97%s" % (NEWLINE, NEWLINE))
        self.tcp_client.send_data("%sds%s" % (NEWLINE, NEWLINE))
        self.assertTrue(self.tcp_client.expect("sample interval = 97"))
        self.tcp_client.send_data("%sstartnow%s" % (NEWLINE, NEWLINE))
        gevent.sleep(3)
        log.debug("DA Parameter Sample Interval Updated")

        self.assert_direct_access_stop_telnet(timeout=60)

        # verify the setting got restored.
        self.assert_state_change(ResourceAgentState.STREAMING, SBE37ProtocolState.AUTOSAMPLE, 10)
        self.assert_get_parameter(SBE37Parameter.INTERVAL, 10)

        ###
        # Test direct access disconnect
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=600, session_timeout=600)
        log.debug("DA server started and connected")
        self.tcp_client.send_data("%sstartnow%s" % (NEWLINE, NEWLINE))
        gevent.sleep(3)
        log.debug("DA server autosample started")
        self.tcp_client.disconnect()
        log.debug("DA server tcp client disconnected")
        self.assert_state_change(ResourceAgentState.STREAMING, SBE37ProtocolState.AUTOSAMPLE, 90)


    @unittest.skip("Do not include until a good method is devised")
    def test_direct_access_virtual_serial_port_mode(self):
        """
        @brief This test verifies that the Instrument Driver
               properly supports direct access to the physical
               instrument. (virtual serial port mode)

        Status: Sample code for this test has yet to be written.
                WCB will implement next iteration

        UPDATE: Do not include for now. May include later as a
                good method is devised

        TODO:
        """
        pass

    def test_sbe37_parameter_enum(self):
        """
        @ brief ProtocolState enum test

            1. test that SBE37ProtocolState matches the expected enums from DriverProtocolState.
            2. test that multiple distinct states do not resolve back to the same string.
        """

        self.assertEqual(SBE37Parameter.ALL, DriverParameter.ALL)

        self.assertTrue(self.check_for_reused_values(DriverParameter))
        self.assertTrue(self.check_for_reused_values(SBE37Parameter))


    def test_protocol_event_enum(self):
        """
        @brief ProtocolState enum test

            1. test that SBE37ProtocolState matches the expected enums from DriverProtocolState.
            2. test that multiple distinct states do not resolve back to the same string.
        """

        self.assertEqual(SBE37ProtocolEvent.ENTER, DriverEvent.ENTER)
        self.assertEqual(SBE37ProtocolEvent.EXIT, DriverEvent.EXIT)
        self.assertEqual(SBE37ProtocolEvent.GET, DriverEvent.GET)
        self.assertEqual(SBE37ProtocolEvent.SET, DriverEvent.SET)
        self.assertEqual(SBE37ProtocolEvent.DISCOVER, DriverEvent.DISCOVER)
        self.assertEqual(SBE37ProtocolEvent.ACQUIRE_SAMPLE, DriverEvent.ACQUIRE_SAMPLE)
        self.assertEqual(SBE37ProtocolEvent.START_AUTOSAMPLE, DriverEvent.START_AUTOSAMPLE)
        self.assertEqual(SBE37ProtocolEvent.STOP_AUTOSAMPLE, DriverEvent.STOP_AUTOSAMPLE)
        self.assertEqual(SBE37ProtocolEvent.TEST, DriverEvent.TEST)
        self.assertEqual(SBE37ProtocolEvent.RUN_TEST, DriverEvent.RUN_TEST)
        self.assertEqual(SBE37ProtocolEvent.CALIBRATE, DriverEvent.CALIBRATE)
        self.assertEqual(SBE37ProtocolEvent.EXECUTE_DIRECT, DriverEvent.EXECUTE_DIRECT)
        self.assertEqual(SBE37ProtocolEvent.START_DIRECT, DriverEvent.START_DIRECT)
        self.assertEqual(SBE37ProtocolEvent.STOP_DIRECT, DriverEvent.STOP_DIRECT)

        self.assertTrue(self.check_for_reused_values(DriverEvent))
        self.assertTrue(self.check_for_reused_values(SBE37ProtocolEvent))


    def test_protocol_state_enum(self):
        """
        @ brief ProtocolState enum test

            1. test that SBE37ProtocolState matches the expected enums from DriverProtocolState.
            2. test that multiple distinct states do not resolve back to the same string.

        """

        self.assertEqual(SBE37ProtocolState.UNKNOWN, DriverProtocolState.UNKNOWN)
        self.assertEqual(SBE37ProtocolState.COMMAND, DriverProtocolState.COMMAND)
        self.assertEqual(SBE37ProtocolState.AUTOSAMPLE, DriverProtocolState.AUTOSAMPLE)
        self.assertEqual(SBE37ProtocolState.TEST, DriverProtocolState.TEST)
        self.assertEqual(SBE37ProtocolState.CALIBRATE, DriverProtocolState.CALIBRATE)
        self.assertEqual(SBE37ProtocolState.DIRECT_ACCESS, DriverProtocolState.DIRECT_ACCESS)


        #SBE37ProtocolState.UNKNOWN = SBE37ProtocolState.COMMAND
        #SBE37ProtocolState.UNKNOWN2 = SBE37ProtocolState.UNKNOWN

        self.assertTrue(self.check_for_reused_values(DriverProtocolState))
        self.assertTrue(self.check_for_reused_values(SBE37ProtocolState))


    @unittest.skip("Underlying method not yet implemented")
    def test_driver_memory_leaks(self):
        """
        @brief long running test that runs over a half hour, and looks for memory leaks.
               stub this out for now
        TODO: write test if time permits after all other tests are done.
        """
        pass

    @unittest.skip("SKIP for now.  This will come in around the time we split IA into 2 parts wet side dry side")
    def test_instrument_agent_data_decimation(self):
        """
        @brief This test verifies that the instrument driver,
               if required, can properly decimate sampling data.
                decimate here means send every 5th sample.

        """
        pass


    def assertParsedGranule(self, granule):
        
        # rdt = RecordDictionaryTool.load_from_granule(granule)
        # self.assert_('conductivity' in rdt)
        # self.assert_(rdt['conductivity'] is not None)
        # self.assertTrue(isinstance(rdt['conductivity'], numpy.ndarray))
        #
        # self.assert_('pressure' in rdt)
        # self.assert_(rdt['pressure'] is not None)
        # self.assertTrue(isinstance(rdt['pressure'], numpy.ndarray))
        #
        # self.assert_('temp' in rdt)
        # self.assert_(rdt['temp'] is not None)
        # self.assertTrue(isinstance(rdt['temp'], numpy.ndarray))
        pass
        
    def assertSampleDataParticle(self, val):
        """
        Verify the value for a sbe37 sample data particle

        {
          'quality_flag': 'ok',
          'preferred_timestamp': 'driver_timestamp',
          'stream_name': 'parsed',
          'pkt_format_id': 'JSON_Data',
          'pkt_version': 1,
          'driver_timestamp': 3559843883.8029947,
          'values': [
            {
              'value_id': 'temp',
              'value': 67.4448
            },
            {
              'value_id': 'conductivity',
              'value': 44.69101
            },
            {
              'value_id': 'pressure',
              'value': 865.096
            }
          ],
        }
        """

        if (isinstance(val, SBE37DataParticle)):
            sample_dict = json.loads(val.generate_parsed())
        else:
            sample_dict = val

        self.assertTrue(sample_dict[DataParticleKey.STREAM_NAME],
            DataParticleType.PARSED)
        self.assertTrue(sample_dict[DataParticleKey.PKT_FORMAT_ID],
            DataParticleValue.JSON_DATA)
        self.assertTrue(sample_dict[DataParticleKey.PKT_VERSION], 1)
        self.assertTrue(isinstance(sample_dict[DataParticleKey.VALUES],
            list))
        self.assertTrue(isinstance(sample_dict.get(DataParticleKey.DRIVER_TIMESTAMP), float))
        self.assertTrue(sample_dict.get(DataParticleKey.PREFERRED_TIMESTAMP))

        for x in sample_dict['values']:
            self.assertTrue(x['value_id'] in ['temp', 'conductivity', 'pressure'])
            self.assertTrue(isinstance(x['value'], float))


    def test_capabilities(self):
        """
        Test the ability to retrieve agent and resource parameter and command
        capabilities in various system states.
        """

        agt_cmds_all = [
            ResourceAgentEvent.INITIALIZE,
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.GO_ACTIVE,
            ResourceAgentEvent.GO_INACTIVE,
            ResourceAgentEvent.RUN,
            ResourceAgentEvent.CLEAR,
            ResourceAgentEvent.PAUSE,
            ResourceAgentEvent.RESUME,
            ResourceAgentEvent.GO_COMMAND,
            ResourceAgentEvent.GO_DIRECT_ACCESS
        ]

        agt_pars_all = ['example', 'streams', 'pubrate', 'alerts', 'driver_pid', 'driver_name', 'aggstatus']

        res_cmds_all =[
            SBE37ProtocolEvent.ACQUIRE_STATUS,
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE,
            SBE37ProtocolEvent.STOP_AUTOSAMPLE,
            SBE37ProtocolEvent.ACQUIRE_CONFIGURATION,
            SBE37ProtocolEvent.GAP_RECOVERY
        ]

        res_pars_all = PARAMS.keys()


        def sort_caps(caps_list):
            agt_cmds = []
            agt_pars = []
            res_cmds = []
            res_pars = []

            if len(caps_list)>0 and isinstance(caps_list[0], AgentCapability):
                agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
                agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
                res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
                res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]

            elif len(caps_list)>0 and isinstance(caps_list[0], dict):
                agt_cmds = [x['name'] for x in retval if x['cap_type']==CapabilityType.AGT_CMD]
                agt_pars = [x['name'] for x in retval if x['cap_type']==CapabilityType.AGT_PAR]
                res_cmds = [x['name'] for x in retval if x['cap_type']==CapabilityType.RES_CMD]
                res_pars = [x['name'] for x in retval if x['cap_type']==CapabilityType.RES_PAR]

            return agt_cmds, agt_pars, res_cmds, res_pars


        ##################################################################
        # UNINITIALIZED
        ##################################################################

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Get exposed capabilities in current state.
        retval = self.instrument_agent_client.get_capabilities()

        # Validate capabilities for state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_uninitialized = [
            ResourceAgentEvent.INITIALIZE
        ]

        log.debug("agt_pars: %r, agt_pars_all: %r", agt_pars, agt_pars_all)
        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self.instrument_agent_client.get_capabilities(False)

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self.instrument_agent_client.execute_agent(cmd)

        ##################################################################
        # INACTIVE
        ##################################################################

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Get exposed capabilities in current state.
        retval = self.instrument_agent_client.get_capabilities()

        # Validate capabilities for state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_inactive = [
            ResourceAgentEvent.GO_ACTIVE,
            ResourceAgentEvent.RESET
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_inactive)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self.instrument_agent_client.get_capabilities(False)

        # Validate all capabilities as read from state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self.instrument_agent_client.execute_agent(cmd)

        ##################################################################
        # IDLE
        ##################################################################

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        # Get exposed capabilities in current state.
        retval = self.instrument_agent_client.get_capabilities()

        # Validate capabilities for state IDLE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_idle = [
            ResourceAgentEvent.GO_INACTIVE,
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.RUN
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_idle)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states as read from IDLE.
        retval = self.instrument_agent_client.get_capabilities(False)

        # Validate all capabilities as read from state IDLE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self.instrument_agent_client.execute_agent(cmd)

        ##################################################################
        # COMMAND
        ##################################################################

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Get exposed capabilities in current state.
        retval = self.instrument_agent_client.get_capabilities()

        # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_command = [
            ResourceAgentEvent.CLEAR,
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.GO_DIRECT_ACCESS,
            ResourceAgentEvent.GO_INACTIVE,
            ResourceAgentEvent.PAUSE
        ]

        res_cmds_command = [
            SBE37ProtocolEvent.ACQUIRE_STATUS,
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE,
            SBE37ProtocolEvent.ACQUIRE_CONFIGURATION,
            SBE37ProtocolEvent.GAP_RECOVERY
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_pars, res_pars_all)

        # Get exposed capabilities in all states as read from state COMMAND.
        retval = self.instrument_agent_client.get_capabilities(False)

        # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)

        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self.instrument_agent_client.execute_resource(cmd)

        ##################################################################
        # STREAMING
        ##################################################################

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STREAMING)

        # Get exposed capabilities in current state.
        retval = self.instrument_agent_client.get_capabilities()

        # Validate capabilities of state STREAMING
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)


        agt_cmds_streaming = [
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.GO_INACTIVE
        ]

        res_cmds_streaming = [
            SBE37ProtocolEvent.STOP_AUTOSAMPLE,
            SBE37ProtocolEvent.GAP_RECOVERY
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_streaming)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_streaming)
        self.assertItemsEqual(res_pars, res_pars_all)

        # Get exposed capabilities in all states as read from state STREAMING.
        retval = self.instrument_agent_client.get_capabilities(False)

        # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)

        gevent.sleep(5)

        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self.instrument_agent_client.execute_resource(cmd, timeout=30)

        ##################################################################
        # COMMAND
        ##################################################################

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Get exposed capabilities in current state.
        retval = self.instrument_agent_client.get_capabilities()

        # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_pars, res_pars_all)

        # Get exposed capabilities in all states as read from state STREAMING.
        retval = self.instrument_agent_client.get_capabilities(False)

        # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self.instrument_agent_client.execute_agent(cmd)

        ##################################################################
        # UNINITIALIZED
        ##################################################################

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Get exposed capabilities in current state.
        retval = self.instrument_agent_client.get_capabilities()

        # Validate capabilities for state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self.instrument_agent_client.get_capabilities(False)

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

    def test_autosample(self):
        """
        Test instrument driver execute interface to start and stop streaming
        mode.
        """
        self.data_subscribers.start_data_subscribers()
        self.addCleanup(self.data_subscribers.stop_data_subscribers)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self.instrument_agent_client.execute_agent(cmd)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self.instrument_agent_client.execute_agent(cmd)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self.instrument_agent_client.execute_agent(cmd)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)


        # Make sure the sampling rate and transmission are sane.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5,
            SBE37Parameter.TXREALTIME : True
        }
        self.instrument_agent_client.set_resource(params)

        self.data_subscribers.clear_sample_queue(DataParticleType.PARSED)

        # Begin streaming.
        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self.instrument_agent_client.execute_resource(cmd, timeout=30)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STREAMING)

        # Assert we got 3 samples.
        samples = self.data_subscribers.get_samples(DataParticleType.PARSED, 3, timeout=30)
        self.assertGreaterEqual(len(samples), 3)

        # If we want to verify granules then this needs to be a publication test.  In the
        # IDK we overload the IA data handler to emit particles.
        #self.assertParsedGranule(samples.pop())
        #self.assertParsedGranule(samples.pop())
        #self.assertParsedGranule(samples.pop())
        self.assertSampleDataParticle(samples.pop())
        self.assertSampleDataParticle(samples.pop())
        self.assertSampleDataParticle(samples.pop())

        # Halt streaming.
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self.instrument_agent_client.execute_resource(cmd, timeout=30)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self.instrument_agent_client.execute_agent(cmd)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self.doCleanups()

    def assertParamDict(self, pd, all_params=False):
        """
        Verify all device parameters exist and are correct type.
        """
        if all_params:
            self.assertEqual(set(pd.keys()), set(PARAMS.keys()))
            for (key, type_val) in PARAMS.iteritems():
                if type_val == list or type_val == tuple:
                    self.assertTrue(isinstance(pd[key], (list, tuple)))
                else:
                    self.assertTrue(isinstance(pd[key], type_val))

        else:
            for (key, val) in pd.iteritems():
                self.assertTrue(PARAMS.has_key(key))
                self.assertTrue(isinstance(val, PARAMS[key]))

    def assertParamVals(self, params, correct_params):
        """
        Verify parameters take the correct values.
        """
        self.assertEqual(set(params.keys()), set(correct_params.keys()))
        for (key, val) in params.iteritems():
            correct_val = correct_params[key]
            if isinstance(val, float):
                # Verify to 5% of the larger value.
                max_val = max(abs(val), abs(correct_val))
                self.assertAlmostEqual(val, correct_val, delta=max_val*.01)

            elif isinstance(val, (list, tuple)):
                # list of tuple.
                self.assertEqual(list(val), list(correct_val))

            else:
                # int, bool, str.
                self.assertEqual(val, correct_val)

    @unittest.skip("PROBLEM WITH command=ResourceAgentEvent.GO_ACTIVE")
    def test_get_set(self):
        """
        Test instrument driver get and set interface.
        """
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()

        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Retrieve all resource parameters.
        reply = self.instrument_agent_client.get_resource(SBE37Parameter.ALL)
        self.assertParamDict(reply, True)
        orig_config = reply

        # Retrieve a subset of resource parameters.
        params = [
            SBE37Parameter.OUTPUTSV,
            SBE37Parameter.NAVG,
            SBE37Parameter.TA0
        ]
        reply = self.instrument_agent_client.get_resource(params)
        self.assertParamDict(reply)
        orig_params = reply

        # Set a subset of resource parameters.
        new_params = {
            SBE37Parameter.OUTPUTSV : not orig_params[SBE37Parameter.OUTPUTSV],
            SBE37Parameter.NAVG : orig_params[SBE37Parameter.NAVG] + 1,
            SBE37Parameter.TA0 : orig_params[SBE37Parameter.TA0] * 2
        }
        self.instrument_agent_client.set_resource(new_params)
        check_new_params = self.instrument_agent_client.get_resource(params)
        self.assertParamVals(check_new_params, new_params)

        # Reset the parameters back to their original values.
        self.instrument_agent_client.set_resource(orig_params)
        reply = self.instrument_agent_client.get_resource(SBE37Parameter.ALL)
        reply.pop(SBE37Parameter.SAMPLENUM)
        orig_config.pop(SBE37Parameter.SAMPLENUM)
        self.assertParamVals(reply, orig_config)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    @unittest.skip("PROBLEM WITH command=ResourceAgentEvent.GO_ACTIVE")
    def oldtest_poll(self):
        """
        Test observatory polling function.
        """
        # Set up all data subscriptions.  Stream names are defined
        # in the driver PACKET_CONFIG dictionary
        self.data_subscribers.start_data_subscribers()
        self.addCleanup(self.data_subscribers.stop_data_subscribers)

        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        ###
        # Poll for a few samples
        ###

        # make sure there aren't any junk samples in the parsed
        # data queue.
        self.data_subscribers.clear_sample_queue(DataParticleType.PARSED)
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        reply = self.instrument_agent_client.execute_resource(cmd)

        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        reply = self.instrument_agent_client.execute_resource(cmd)

        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        reply = self.instrument_agent_client.execute_resource(cmd)

        # Watch the parsed data queue and return once three samples
        # have been read or the default timeout has been reached.
        samples = self.data_subscribers.get_samples(DataParticleType.PARSED, 3)
        self.assertGreaterEqual(len(samples), 3)

        self.assertSampleDataParticle(samples.pop())
        self.assertSampleDataParticle(samples.pop())
        self.assertSampleDataParticle(samples.pop())

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self.doCleanups()

    def test_poll(self):
        '''
        Verify that we can poll for a sample.  Take sample for this instrument
        Also poll for other engineering data streams.
        '''
        self.assert_enter_command_mode()


        self.assert_particle_polled(SBE37ProtocolEvent.ACQUIRE_SAMPLE, self.assert_particle_sample, DataParticleType.PARSED)
        self.assert_particle_polled(SBE37ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_device_status, DataParticleType.DEVICE_STATUS)
        self.assert_particle_polled(SBE37ProtocolEvent.ACQUIRE_CONFIGURATION, self.assert_particle_device_calibration, DataParticleType.DEVICE_CALIBRATION)


    def test_instrument_driver_vs_invalid_commands(self):
        """
        @Author Edward Hunter
        @brief This test should send mal-formed, misspelled,
               missing parameter, or out of bounds parameters
               at the instrument driver in an attempt to
               confuse it.

               See: test_instrument_driver_to_physical_instrument_interoperability
               That test will provide the how-to of connecting.
               Once connected, send messed up commands.

               * negative testing


               Test illegal behavior and replies.
        """


        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)



        # Try to execute agent command with bogus command.
        with self.assertRaises(BadRequest):
            cmd = AgentCommand(command='BOGUS_COMMAND')
            retval = self.instrument_agent_client.execute_agent(cmd)


        # Can't go active in unitialized state.
        # Status 660 is state error.
        with self.assertRaises(Conflict):
            cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
            retval = self.instrument_agent_client.execute_agent(cmd)


        # Try to execute the resource, wrong state.
        with self.assertRaises(BadRequest):
            cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
            retval = self.instrument_agent_client.execute_agent(cmd)


        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)


        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)


        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self.instrument_agent_client.execute_agent(cmd)
        state = self.instrument_agent_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # OK, I can do this now.
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        reply = self.instrument_agent_client.execute_resource(cmd)
        self.assertTrue(reply.result)

        # 404 unknown agent command.
        with self.assertRaises(BadRequest):
            cmd = AgentCommand(command='kiss_edward')
            reply = self.instrument_agent_client.execute_agent(cmd)


        '''
        @todo this needs to be re-enabled eventually
        # 670 unknown driver command.
        cmd = AgentCommand(command='acquire_sample_please')
        retval = self.instrument_agent_client.execute(cmd)
        log.debug("retval = " + str(retval))

        # the return value will likely be changed in the future to return
        # to being 670... for now, lets make it work.
        #self.assertEqual(retval.status, 670)
        self.assertEqual(retval.status, -1)

        try:
            reply = self.instrument_agent_client.get_param('1234')
        except Exception as e:
            log.debug("InstrumentParameterException ERROR = " + str(e))

        #with self.assertRaises(XXXXXXXXXXXXXXXXXXXXXXXX):
        #    reply = self.instrument_agent_client.get_param('1234')

        # 630 Parameter error.
        #with self.assertRaises(InstParameterError):
        #    reply = self.instrument_agent_client.get_param('bogus bogus')

        cmd = AgentCommand(command='reset')
        retval = self.instrument_agent_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_agent_state')
        retval = self.instrument_agent_client.execute_agent(cmd)

        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)
        '''
        pass

    @unittest.skip("Needs to be fixed")
    def test_direct_access_config(self):
        """
        Verify that the configurations work when we go into direct access mode
        and jack with settings
        """
        # NAVG is direct access
        # INTERVAL has a default value of 1

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self.instrument_agent_client.execute_agent(cmd)
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self.instrument_agent_client.execute_agent(cmd)
        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self.instrument_agent_client.execute_agent(cmd)
        
        self.instrument_agent_client.set_resource({SBE37Parameter.NAVG:2})
        self.instrument_agent_client.set_resource({SBE37Parameter.SAMPLENUM:2})
        self.instrument_agent_client.set_resource({SBE37Parameter.INTERVAL:2})     

        params = [
            SBE37Parameter.SAMPLENUM,
            SBE37Parameter.NAVG,
            SBE37Parameter.INTERVAL
        ]
        reply = self.instrument_agent_client.get_resource(params)
        self.assertParamDict(reply)
        orig_params = reply
        self.assertEquals(reply[SBE37Parameter.NAVG], 2) # da param
        self.assertEquals(reply[SBE37Parameter.SAMPLENUM], 2) # non-da param
        self.assertEquals(reply[SBE37Parameter.INTERVAL], 2) # non-da param, w/default
        
        # go into direct access mode
        self.assert_direct_access_start_telnet()
        self.assertTrue(self.tcp_client)
        self.tcp_client.send_data("INTERVAL=20\r\n")
        self.tcp_client.send_data("NAVG=20\r\n")
        self.tcp_client.send_data("SAMPLENUM=20\r\n")
        self.assert_direct_access_stop_telnet()

        reply = self.instrument_agent_client.get_resource(params)
        self.assertParamDict(reply)
        orig_params = reply
        self.assertEquals(reply[SBE37Parameter.NAVG], 2) # da param
        self.assertEquals(reply[SBE37Parameter.SAMPLENUM], 20) # non-da param
        self.assertEquals(reply[SBE37Parameter.INTERVAL], 20) # non-da param, w/default

    def test_reset(self):
        """
        Overload base test because we are having issue with coming out of DA
        """
        self.assert_enter_command_mode()
        self.assert_reset()

        self.assert_enter_command_mode()
        self.assert_start_autosample()
        self.assert_reset()

    def test_instrument_agent_common_state_model_lifecycle(self):
        '''
        Skipping this common test for now because we are having issues coming out of DA
        @return:
        '''
        pass

    def test_discover(self):
        '''
        Skipping this common test because the simulator doesn't have state and can't remember that
        it was in streaming mode when we disconnect.
        '''
        pass

    def test_startup_params(self):
        """
        Verify that startup parameters are applied correctly when the driver is started.
        """
        # Startup the driver, verify the startup value and then change it.
        self.assert_enter_command_mode()
        self.assert_get_parameter(SBE37Parameter.INTERVAL, 1)
        self.assert_set_parameter(SBE37Parameter.INTERVAL, 10)

        # Reset the agent which brings the driver down
        self.assert_reset()

        # Now restart the driver and verify the value has reverted back to the startup value
        self.assert_enter_command_mode()
        self.assert_get_parameter(SBE37Parameter.INTERVAL, 1)
