"""
@package mi.instrument.seabird.sbe54tps.test.test_driver
@file mi/instrument/seabird/sbe54tps/test/test_driver.py
@author Roger Unwin
@brief Test cases for sbe54 driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u
       $ bin/test_driver -i
       $ bin/test_driver -q

   * From pyon
       $ bin/nosetests -s -v mi/instrument/seabird/sbe54tps/ooicore
       $ bin/nosetests -s -v mi/instrument/seabird/sbe54tps/ooicore -a UNIT
       $ bin/nosetests -s -v mi/instrument/seabird/sbe54tps/ooicore -a INT
       $ bin/nosetests -s -v mi/instrument/seabird/sbe54tps/ooicore -a QUAL
"""
import copy
from nose.plugins.attrib import attr
from mock import Mock
import time
import ntplib

from mi.core.log import get_logger
from mi.core.time_tools import timegm_to_float
from mi.idk.unit_test import DriverTestMixin, DriverStartupConfigKey, InstrumentDriverTestCase
from mi.idk.unit_test import ParameterTestConfigKey
from mi.idk.unit_test import AgentCapabilityType
from mi.instrument.seabird.sbe54tps.driver import SBE54PlusInstrumentDriver
from mi.instrument.seabird.sbe54tps.driver import ScheduledJob
from mi.instrument.seabird.sbe54tps.driver import ProtocolState
from mi.instrument.seabird.sbe54tps.driver import Parameter
from mi.instrument.seabird.sbe54tps.driver import ProtocolEvent
from mi.instrument.seabird.sbe54tps.driver import Capability
from mi.instrument.seabird.sbe54tps.driver import Prompt
from mi.instrument.seabird.sbe54tps.driver import Protocol
from mi.instrument.seabird.sbe54tps.driver import InstrumentCommands
from mi.instrument.seabird.sbe54tps.driver import SBE54tpsStatusDataParticleKey
from mi.instrument.seabird.sbe54tps.driver import SBE54tpsEventCounterDataParticleKey
from mi.instrument.seabird.sbe54tps.driver import SBE54tpsSampleDataParticleKey
from mi.instrument.seabird.sbe54tps.driver import SBE54tpsHardwareDataParticleKey
from mi.instrument.seabird.sbe54tps.driver import SBE54tpsConfigurationDataParticleKey
from mi.instrument.seabird.sbe54tps.driver import SBE54tpsSampleRefOscDataParticleKey
from mi.instrument.seabird.sbe54tps.driver import DataParticleType
from mi.instrument.seabird.test.test_driver import SeaBirdUnitTest
from mi.instrument.seabird.test.test_driver import SeaBirdIntegrationTest
from mi.instrument.seabird.test.test_driver import SeaBirdQualificationTest
from mi.instrument.seabird.sbe54tps.test.sample_data import *
from mi.core.instrument.instrument_driver import ResourceAgentEvent
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.exceptions import InstrumentCommandException
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.port_agent_client import PortAgentPacket

__author__ = 'Roger Unwin'
__license__ = 'Apache 2.0'


log = get_logger()

InstrumentDriverTestCase.initialize(
        driver_module='mi.instrument.seabird.sbe54tps.ooicore.driver',
        driver_class="InstrumentDriver",
        instrument_agent_resource_id='123xyz',
        instrument_agent_preload_id='IA7',
        instrument_agent_name='Agent007',
        instrument_agent_packet_config=DataParticleType(),

        driver_startup_config={
            DriverStartupConfigKey.PARAMETERS: {
                Parameter.SAMPLE_PERIOD: 15,
                Parameter.ENABLE_ALERTS: 1,
            },
            DriverStartupConfigKey.SCHEDULER: {
                ScheduledJob.ACQUIRE_STATUS: {},
                ScheduledJob.STATUS_DATA: {},
                ScheduledJob.HARDWARE_DATA: {},
                ScheduledJob.EVENT_COUNTER_DATA: {},
                ScheduledJob.CONFIGURATION_DATA: {},
                ScheduledJob.CLOCK_SYNC: {}
            }
        }
)


class SeaBird54tpsMixin(DriverTestMixin):
    """
    Mixin class used for storing data particle constance and common data assertion methods.
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
    #  Parameter and Type Definitions
    ###
    _driver_parameters = {
        # Parameters defined in the IOS
        Parameter.SAMPLE_PERIOD: {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 15, VALUE: 15},
        Parameter.TIME: {TYPE: str, READONLY: True, DA: False, STARTUP: False},
        Parameter.BATTERY_TYPE: {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 1, VALUE: 1},
        Parameter.ENABLE_ALERTS: {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: 1},
    }

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.CLOCK_SYNC: {STATES: [ProtocolState.COMMAND]},
        Capability.ACQUIRE_STATUS: {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},
        Capability.SAMPLE_REFERENCE_OSCILLATOR: {STATES: [ProtocolState.COMMAND]},
        Capability.TEST_EEPROM: {STATES: [ProtocolState.COMMAND]},
    }

    _prest_real_time_parameters = {
        SBE54tpsSampleDataParticleKey.SAMPLE_NUMBER: {TYPE: int, VALUE: 5947, REQUIRED: True},
        SBE54tpsSampleDataParticleKey.SAMPLE_TYPE: {TYPE: unicode, VALUE: 'Pressure', REQUIRED: True},
        SBE54tpsSampleDataParticleKey.INST_TIME: {TYPE: unicode, VALUE: '2012-11-07T12:21:25', REQUIRED: True},
        SBE54tpsSampleDataParticleKey.PRESSURE: {TYPE: float, VALUE: 13.9669, REQUIRED: True},
        SBE54tpsSampleDataParticleKey.PRESSURE_TEMP: {TYPE: float, VALUE: 18.9047, REQUIRED: True},
    }

    _prest_reference_oscillator_parameters = {
        SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT: {TYPE: int, VALUE: 125000, REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_MAX: {TYPE: int, VALUE: 150000, REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.SET_TIMEOUT_ICD: {TYPE: int, VALUE: 150000, REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.SAMPLE_NUMBER: {TYPE: int, VALUE: 1244, REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TYPE: {TYPE: unicode, VALUE: 'RefOsc', REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.SAMPLE_TIMESTAMP: {TYPE: unicode, VALUE: u'2013-01-30T15:36:53',
                                                               REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.REF_OSC_FREQ: {TYPE: float, VALUE: 5999995.955, REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.REF_ERROR_PPM: {TYPE: float, VALUE: 0.090, REQUIRED: True},
        SBE54tpsSampleRefOscDataParticleKey.PCB_TEMP_RAW: {TYPE: int, VALUE: 18413, REQUIRED: True},
    }

    _prest_configuration_data_parameters = {
        SBE54tpsConfigurationDataParticleKey.DEVICE_TYPE: {TYPE: unicode, VALUE: 'SBE54', REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.SERIAL_NUMBER: {TYPE: str, VALUE: '05400012', REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.ACQ_OSC_CAL_DATE: {TYPE: unicode, VALUE: '2012-02-20', REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.FRA0: {TYPE: float, VALUE: 5.999926E+06, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.FRA1: {TYPE: float, VALUE: 5.792290E-03, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.FRA2: {TYPE: float, VALUE: -1.195664E-07, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.FRA3: {TYPE: float, VALUE: 7.018589E-13, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PRESSURE_SERIAL_NUM: {TYPE: unicode, VALUE: '121451', REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PRESSURE_CAL_DATE: {TYPE: unicode, VALUE: '2011-06-01', REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PU0: {TYPE: float, VALUE: 5.820407E+00, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PY1: {TYPE: float, VALUE: -3.845374E+03, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PY2: {TYPE: float, VALUE: -1.078882E+04, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PY3: {TYPE: float, VALUE: 0.000000E+00, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PC1: {TYPE: float, VALUE: -2.700543E+04, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PC2: {TYPE: float, VALUE: -1.738438E+03, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PC3: {TYPE: float, VALUE: 7.629962E+04, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PD1: {TYPE: float, VALUE: 3.739600E-02, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PD2: {TYPE: float, VALUE: 0.000000E+00, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PT1: {TYPE: float, VALUE: 3.027306E+01, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PT2: {TYPE: float, VALUE: 2.231025E-01, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PT3: {TYPE: float, VALUE: 5.398972E+01, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PT4: {TYPE: float, VALUE: 1.455506E+02, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PRESSURE_OFFSET: {TYPE: float, VALUE: 0.000000E+00, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.PRESSURE_RANGE: {TYPE: float, VALUE: 6.000000E+03, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.BATTERY_TYPE: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.BAUD_RATE: {TYPE: int, VALUE: 9600, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.ENABLE_ALERTS: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.UPLOAD_TYPE: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE54tpsConfigurationDataParticleKey.SAMPLE_PERIOD: {TYPE: int, VALUE: 15, REQUIRED: True}
    }

    _prest_device_status_parameters = {
        SBE54tpsStatusDataParticleKey.DEVICE_TYPE: {TYPE: unicode, VALUE: 'SBE54', REQUIRED: True},
        SBE54tpsStatusDataParticleKey.SERIAL_NUMBER: {TYPE: str, VALUE: '05400012', REQUIRED: True},
        SBE54tpsStatusDataParticleKey.TIME: {TYPE: unicode, VALUE: '2012-11-06T10:55:44', REQUIRED: True},
        SBE54tpsStatusDataParticleKey.EVENT_COUNT: {TYPE: int, VALUE: 573},
        SBE54tpsStatusDataParticleKey.MAIN_SUPPLY_VOLTAGE: {TYPE: float, VALUE: 23.3, REQUIRED: True},
        SBE54tpsStatusDataParticleKey.NUMBER_OF_SAMPLES: {TYPE: int, VALUE: 22618, REQUIRED: True},
        SBE54tpsStatusDataParticleKey.BYTES_USED: {TYPE: int, VALUE: 341504, REQUIRED: True},
        SBE54tpsStatusDataParticleKey.BYTES_FREE: {TYPE: int, VALUE: 133876224, REQUIRED: True},
    }

    _prest_event_counter_parameters = {
        SBE54tpsEventCounterDataParticleKey.NUMBER_EVENTS: {TYPE: int, VALUE: 573},
        SBE54tpsEventCounterDataParticleKey.MAX_STACK: {TYPE: int, VALUE: 354},
        SBE54tpsEventCounterDataParticleKey.DEVICE_TYPE: {TYPE: unicode, VALUE: 'SBE54'},
        SBE54tpsEventCounterDataParticleKey.SERIAL_NUMBER: {TYPE: str, VALUE: '05400012'},
        SBE54tpsEventCounterDataParticleKey.POWER_ON_RESET: {TYPE: int, VALUE: 25},
        SBE54tpsEventCounterDataParticleKey.POWER_FAIL_RESET: {TYPE: int, VALUE: 25},
        SBE54tpsEventCounterDataParticleKey.SERIAL_BYTE_ERROR: {TYPE: int, VALUE: 9},
        SBE54tpsEventCounterDataParticleKey.COMMAND_BUFFER_OVERFLOW: {TYPE: int, VALUE: 1},
        SBE54tpsEventCounterDataParticleKey.SERIAL_RECEIVE_OVERFLOW: {TYPE: int, VALUE: 255},
        SBE54tpsEventCounterDataParticleKey.LOW_BATTERY: {TYPE: int, VALUE: 255},
        SBE54tpsEventCounterDataParticleKey.SIGNAL_ERROR: {TYPE: int, VALUE: 1},
        SBE54tpsEventCounterDataParticleKey.ERROR_10: {TYPE: int, VALUE: 1},
        SBE54tpsEventCounterDataParticleKey.ERROR_12: {TYPE: int, VALUE: 1},
    }

    _prest_hardware_data_parameters = {
        SBE54tpsHardwareDataParticleKey.DEVICE_TYPE: {TYPE: unicode, VALUE: 'SBE54', REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.SERIAL_NUMBER: {TYPE: str, VALUE: '05400012', REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.MANUFACTURER: {TYPE: unicode, VALUE: 'Sea-Bird Electronics, Inc',
                                                       REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.FIRMWARE_VERSION: {TYPE: unicode, VALUE: 'SBE54 V1.3-6MHZ', REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.FIRMWARE_DATE: {TYPE: unicode, VALUE: 'Mar 22 2007', REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.HARDWARE_VERSION: {TYPE: list, VALUE: ['41477A.1', '41478A.1T'],
                                                           REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.PCB_SERIAL_NUMBER: {TYPE: list, VALUE: ['NOT SET', 'NOT SET'], REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.PCB_TYPE: {TYPE: unicode, VALUE: '1', REQUIRED: True},
        SBE54tpsHardwareDataParticleKey.MANUFACTURE_DATE: {TYPE: unicode, VALUE: 'Jun 27 2007', REQUIRED: True},
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

    def assert_particle_real_time(self, data_particle, verify_values=False):
        """
        Verify prest_real_tim particle
        @param data_particle:  SBE54tpsSampleDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE54tpsSampleDataParticleKey, self._prest_real_time_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.PREST_REAL_TIME)
        self.assert_data_particle_parameters(data_particle, self._prest_real_time_parameters, verify_values)

    def assert_particle_reference_oscillator(self, data_particle, verify_values=False):
        """
        Verify prest_reference_oscillator particle
        @param data_particle:  SBE54tpsSampleRefOscDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE54tpsSampleRefOscDataParticleKey, self._prest_reference_oscillator_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.PREST_REFERENCE_OSCILLATOR)
        self.assert_data_particle_parameters(data_particle, self._prest_reference_oscillator_parameters, verify_values)

    def assert_particle_configuration_data(self, data_particle, verify_values=False):
        """
        Verify prest_configuration_data particle
        @param data_particle:  SBE54tpsSampleDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE54tpsConfigurationDataParticleKey, self._prest_configuration_data_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.PREST_CONFIGURATION_DATA)
        self.assert_data_particle_parameters(data_particle, self._prest_configuration_data_parameters, verify_values)

    def assert_particle_device_status(self, data_particle, verify_values=False):
        """
        Verify prest_device_status particle
        @param data_particle:  SBE54tpsStatusDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE54tpsStatusDataParticleKey, self._prest_device_status_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.PREST_DEVICE_STATUS)
        self.assert_data_particle_parameters(data_particle, self._prest_device_status_parameters, verify_values)

    def assert_particle_event_counter(self, data_particle, verify_values=False):
        """
        Verify prest_event_coutner particle
        @param data_particle:  SBE54tpsEventCounterDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE54tpsEventCounterDataParticleKey, self._prest_event_counter_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.PREST_EVENT_COUNTER)
        self.assert_data_particle_parameters(data_particle, self._prest_event_counter_parameters, verify_values)

    def assert_particle_hardware_data(self, data_particle, verify_values=False):
        """
        Verify prest_hardware_data particle
        @param data_particle:  SBE54tpsHardwareDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(SBE54tpsHardwareDataParticleKey, self._prest_hardware_data_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.PREST_HARDWARE_DATA)
        self.assert_data_particle_parameters(data_particle, self._prest_hardware_data_parameters, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class SeaBird54PlusUnitTest(SeaBirdUnitTest, SeaBird54tpsMixin):
    def setUp(self):
        SeaBirdUnitTest.setUp(self)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """
        self.assert_enum_has_no_duplicates(ScheduledJob())
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(InstrumentCommands())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())

        # Test capabilities for duplicates, them verify that capabilities is a subset of proto events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = SBE54PlusInstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(Protocol.sieve_function)

        self.assert_chunker_sample(chunker, SAMPLE_GETSD)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_GETSD)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_GETSD, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_GETSD)

        self.assert_chunker_sample(chunker, SAMPLE_GETCD)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_GETCD)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_GETCD, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_GETCD)

        self.assert_chunker_sample(chunker, SAMPLE_GETEC)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_GETEC)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_GETEC, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_GETEC)

        self.assert_chunker_sample(chunker, SAMPLE_GETHD)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_GETHD)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_GETHD, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_GETHD)

        self.assert_chunker_sample(chunker, SAMPLE_SAMPLE)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_SAMPLE)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_SAMPLE, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_SAMPLE)

        self.assert_chunker_sample(chunker, SAMPLE_REF_OSC)
        self.assert_chunker_sample_with_noise(chunker, SAMPLE_REF_OSC)
        self.assert_chunker_fragmented_sample(chunker, SAMPLE_REF_OSC, 32)
        self.assert_chunker_combined_sample(chunker, SAMPLE_REF_OSC)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = SBE54PlusInstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, SAMPLE_GETSD, self.assert_particle_device_status, True)
        self.assert_particle_published(driver, SAMPLE_GETCD, self.assert_particle_configuration_data, True)
        self.assert_particle_published(driver, SAMPLE_GETEC, self.assert_particle_event_counter, True)
        self.assert_particle_published(driver, SAMPLE_GETHD, self.assert_particle_hardware_data, True)
        self.assert_particle_published(driver, SAMPLE_SAMPLE, self.assert_particle_real_time, True)
        self.assert_particle_published(driver, SAMPLE_REF_OSC, self.assert_particle_reference_oscillator, True)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        my_event_callback = Mock(spec="UNKNOWN WHAT SHOULD GO HERE FOR evt_callback")
        protocol = Protocol(Prompt, NEWLINE, my_event_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(driver_capabilities, protocol._filter_capabilities(test_capabilities))

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected.  All states defined in this dict must
        also be defined in the protocol FSM.
        """
        capabilities = {
            ProtocolState.UNKNOWN: [ProtocolEvent.DISCOVER,
                                    ProtocolEvent.START_DIRECT],
            ProtocolState.COMMAND: [ProtocolEvent.ACQUIRE_STATUS,
                                    ProtocolEvent.CLOCK_SYNC,
                                    ProtocolEvent.SCHEDULED_ACQUIRE_STATUS,
                                    ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                    ProtocolEvent.GET,
                                    ProtocolEvent.SET,
                                    ProtocolEvent.INIT_PARAMS,
                                    ProtocolEvent.START_AUTOSAMPLE,
                                    ProtocolEvent.START_DIRECT,
                                    ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR,
                                    ProtocolEvent.RECOVER_AUTOSAMPLE,
                                    ProtocolEvent.TEST_EEPROM],
            ProtocolState.OSCILLATOR: [ProtocolEvent.ACQUIRE_OSCILLATOR_SAMPLE],
            ProtocolState.AUTOSAMPLE: [ProtocolEvent.GET,
                                       ProtocolEvent.INIT_PARAMS,
                                       ProtocolEvent.STOP_AUTOSAMPLE,
                                       ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                       ProtocolEvent.SCHEDULED_ACQUIRE_STATUS],
            ProtocolState.DIRECT_ACCESS: [ProtocolEvent.STOP_DIRECT,
                                          ProtocolEvent.EXECUTE_DIRECT]
        }

        driver = SBE54PlusInstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)

    def _create_port_agent_packet(self, data_item):
        ts = ntplib.system_to_ntp_time(time.time())
        port_agent_packet = PortAgentPacket()
        port_agent_packet.attach_data(data_item)
        port_agent_packet.attach_timestamp(ts)
        port_agent_packet.pack_header()
        return port_agent_packet

    def _send_port_agent_packet(self, driver, data_item):
        driver._protocol.got_data(self._create_port_agent_packet(data_item))

    def send_side_effect(self, driver):
        def inner(data):
            data = data.strip()
            log.debug('get data response for "%s"', data)
            response = self._responses.get(data)
            if response is None:
                log.warn('No response found for %r', data)
                response = Prompt.COMMAND
            log.debug("my_send: data: %s, my_response: %s", data, response)
            self._send_port_agent_packet(driver, response + NEWLINE)

        return inner

    _responses = {
        'GetSD': SAMPLE_GETSD + NEWLINE + Prompt.COMMAND,
        'GetCD': SAMPLE_GETCD + NEWLINE + Prompt.COMMAND,
        'SampleRefOsc': SAMPLE_REF_OSC + NEWLINE
    }

    def test_connect(self, initial_protocol_state=ProtocolState.COMMAND):
        """
        Verify we can initialize the driver.  Set up mock events for other tests.
        @param initial_protocol_state: target protocol state for driver
        @return: driver instance
        """
        startup_config = {
            DriverStartupConfigKey.PARAMETERS: {
                Parameter.SAMPLE_PERIOD: 15,
                Parameter.ENABLE_ALERTS: 1,
            },
            DriverStartupConfigKey.SCHEDULER: {
                ScheduledJob.ACQUIRE_STATUS: {},
                ScheduledJob.STATUS_DATA: {},
                ScheduledJob.HARDWARE_DATA: {},
                ScheduledJob.EVENT_COUNTER_DATA: {},
                ScheduledJob.CONFIGURATION_DATA: {},
                ScheduledJob.CLOCK_SYNC: {}
            }
        }

        driver = SBE54PlusInstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, initial_protocol_state)
        driver._protocol.set_init_params(startup_config)
        driver._connection.send.side_effect = self.send_side_effect(driver)
        driver._protocol._protocol_fsm.on_event_actual = driver._protocol._protocol_fsm.on_event
        driver._protocol._protocol_fsm.on_event = Mock()
        driver._protocol._protocol_fsm.on_event.side_effect = driver._protocol._protocol_fsm.on_event_actual
        driver._protocol._init_params()

        return driver

    def test_oscillator_sample_fail(self):
        """
        Tests both good and failed responses to an oscillator sample command.

        Normally the instrument returns a proper oscillator sample and remains
        in the command state.
        Bench testing showed that when storage is full it will instead
        NOT return an oscillator sample and go into autosample.

        This will test the driver to see if it ends up in the appropriate
        state for each type of response.

        @param self:
        @return:
        """

        driver = self.test_connect()

        # Test a normal oscillator sample
        driver._protocol._protocol_fsm.on_event(ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.OSCILLATOR)
        time.sleep(0.1)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.COMMAND)

        # Test a failed oscillator sample

        # Mock _do_cmd_resp to force timeout to 0.1
        def shorten_timeout(cmd, *args, **kwargs):
            kwargs['timeout'] = 0.1
            driver._protocol._do_cmd_resp_actual(cmd, *args, **kwargs)

        driver._protocol._do_cmd_resp_actual = driver._protocol._do_cmd_resp
        driver._protocol._do_cmd_resp = Mock()
        driver._protocol._do_cmd_resp.side_effect = shorten_timeout

        # Set response to failed oscillator sample
        self._responses['SampleRefOsc'] = SAMPLE_REF_OSC_FAIL + NEWLINE

        # Excercise driver
        driver._protocol._protocol_fsm.on_event(ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.OSCILLATOR)
        time.sleep(0.2)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.COMMAND)


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class SeaBird54PlusIntegrationTest(SeaBirdIntegrationTest, SeaBird54tpsMixin):
    def setUp(self):
        SeaBirdIntegrationTest.setUp(self)

    def test_set(self):
        """
        Test all set commands. Verify all exception cases.
        """
        self.assert_initialize_driver()

        # Sample Period.  integer 1 - 240
        self.assert_set(Parameter.SAMPLE_PERIOD, 1)
        self.assert_set(Parameter.SAMPLE_PERIOD, 240)
        self.assert_set_exception(Parameter.SAMPLE_PERIOD, 241)
        self.assert_set_exception(Parameter.SAMPLE_PERIOD, 0)
        self.assert_set_exception(Parameter.SAMPLE_PERIOD, -1)
        self.assert_set_exception(Parameter.SAMPLE_PERIOD, 0.2)
        self.assert_set_exception(Parameter.SAMPLE_PERIOD, "1")

        #   Read only parameters
        self.assert_set_readonly(Parameter.BATTERY_TYPE, 1)
        self.assert_set_readonly(Parameter.ENABLE_ALERTS, True)

    def test_commands(self):
        """
        Run instrument commands from both command and streaming mode.
        """
        self.assert_initialize_driver()

        ####
        # First test in command mode
        ####
        self.assert_driver_command(ProtocolEvent.CLOCK_SYNC)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, regex=r'StatusData DeviceType')
        self.assert_driver_command(ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR, regex=r'Ref osc warmup')
        self.assert_driver_command(ProtocolEvent.SCHEDULED_CLOCK_SYNC)

        ####
        # Test in streaming mode
        ####
        # Put us in streaming
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_driver_command(ProtocolEvent.SCHEDULED_CLOCK_SYNC)
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
        self.assert_set(Parameter.SAMPLE_PERIOD, 1)

        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_async_particle_generation(DataParticleType.PREST_REAL_TIME, self.assert_particle_real_time,
                                              timeout=120)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

    def test_polled(self):
        """
        Test that we can generate particles with commands
        """
        self.assert_initialize_driver()
        self.assert_particle_generation(ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR,
                                        DataParticleType.PREST_REFERENCE_OSCILLATOR,
                                        self.assert_particle_reference_oscillator)

    def test_apply_startup_params(self):
        """
        This test verifies that we can set the startup params
        from autosample mode.  It only verifies one parameter
        change because all parameters are tested above.
        """
        # Apply autosample happens for free when the driver fires up
        self.assert_initialize_driver()

        # Change something
        self.assert_set(Parameter.SAMPLE_PERIOD, 10)

        # Now try to apply params in Streaming
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE)
        self.driver_client.cmd_dvr('apply_startup_params')

        # All done.  Verify the startup parameter has been reset
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND)
        self.assert_get(Parameter.SAMPLE_PERIOD, 15)

    def test_startup_params(self):
        """
        Verify that startup parameters are applied correctly. Generally this
        happens in the driver discovery method.
        """

        # Explicitly verify these values after discover.  They should match
        # what the startup values should be
        get_values = {
            Parameter.SAMPLE_PERIOD: 15
        }

        # Change the values of these parameters to something before the
        # driver is re-initalized.  They should be blown away on reinit.
        new_values = {
            Parameter.SAMPLE_PERIOD: 5
        }

        self.assert_initialize_driver()
        self.assert_startup_parameters(self.assert_driver_parameters, new_values, get_values)

        # Start autosample and try again
        self.assert_set_bulk(new_values)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_startup_parameters(self.assert_driver_parameters)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

    def test_autosample_recovery(self):
        """
        Test to ensure the driver will right itself when the instrument
        times out in command mode and starts sampling again.
        """
        # Puts the instrument into command mode.
        self.assert_initialize_driver()
        self.assert_set(Parameter.SAMPLE_PERIOD, 1)

        # The instrument will return to streaming in 120 seconds.  We
        # wil watch for 200.
        timeout = time.time() + 200

        while time.time() < timeout:
            state = self.driver_client.cmd_dvr('get_resource_state')
            if state == ProtocolState.AUTOSAMPLE:
                return

            log.debug("current state %s. recheck in 5" % state)
            time.sleep(5)

        self.assertFalse(True, msg="Failed to transition to streaming after 200 seconds")

    def assert_acquire_status(self):
        """
        Verify a status particle was generated
        """
        self.clear_events()
        self.assert_async_particle_generation(DataParticleType.PREST_DEVICE_STATUS, self.assert_particle_device_status,
                                              timeout=120)
        self.assert_async_particle_generation(DataParticleType.PREST_CONFIGURATION_DATA,
                                              self.assert_particle_configuration_data, timeout=3)
        self.assert_async_particle_generation(DataParticleType.PREST_EVENT_COUNTER, self.assert_particle_event_counter,
                                              timeout=3)
        self.assert_async_particle_generation(DataParticleType.PREST_HARDWARE_DATA, self.assert_particle_hardware_data,
                                              timeout=3)

    def test_scheduled_device_status_command(self):
        """
        Verify the device status command can be triggered and run in command
        """
        self.assert_scheduled_event(ScheduledJob.ACQUIRE_STATUS, self.assert_acquire_status, delay=120)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_scheduled_acquire_status_autosample(self):
        """
        Verify the device status command can be triggered and run in autosample
        """
        self.assert_scheduled_event(ScheduledJob.ACQUIRE_STATUS, self.assert_acquire_status,
                                    autosample_command=ProtocolEvent.START_AUTOSAMPLE, delay=120)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)

    def assert_clock_sync(self):
        """
        Verify the clock is set to at least the current date
        """
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS)
        dt = self.assert_get(Parameter.TIME)
        lt = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(timegm_to_float(time.localtime())))
        self.assertTrue(lt[:12].upper() in dt.upper())

    def test_scheduled_clock_sync_command(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        self.assert_scheduled_event(ScheduledJob.CLOCK_SYNC, self.assert_clock_sync, delay=90)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_scheduled_clock_sync_autosample(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        self.assert_scheduled_event(ScheduledJob.CLOCK_SYNC, self.assert_clock_sync,
                                    autosample_command=ProtocolEvent.START_AUTOSAMPLE, delay=90)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)

    def assert_cycle(self):
        self.assert_current_state(ProtocolState.COMMAND)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

        self.assert_async_particle_generation(DataParticleType.PREST_REAL_TIME, self.assert_particle_real_time,
                                              timeout=120)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_current_state(ProtocolState.COMMAND)

    def test_discover(self):
        """
        Verify we can discover from both command and auto sample modes
        """
        self.assert_initialize_driver(final_state=ProtocolState.AUTOSAMPLE)
        self.assert_cycle()
        self.assert_cycle()


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class SeaBird54PlusQualificationTest(SeaBirdQualificationTest, SeaBird54tpsMixin):
    def setUp(self):
        SeaBirdQualificationTest.setUp(self)

    def test_autosample(self):
        """
        Verify autosample works and data particles are created
        """
        self.assert_enter_command_mode()
        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 1)

        self.assert_sample_autosample(self.assert_particle_real_time, DataParticleType.PREST_REAL_TIME)

    def test_poll(self):
        """
        Verify that we can poll for a sample.  Take sample for this instrument
        Also poll for other engineering data streams.
        """
        self.assert_enter_command_mode()

        self.assert_particle_polled(ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR,
                                    self.assert_particle_reference_oscillator,
                                    DataParticleType.PREST_REFERENCE_OSCILLATOR, sample_count=1, timeout=200)

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
        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 10)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet()

        log.debug("DA Server Started.  Adjust DA Parameter.")
        self.tcp_client.send_data("%ssetsamplePeriod=15%s" % (NEWLINE, NEWLINE))
        self.tcp_client.send_data("%sGetCD%s" % (NEWLINE, NEWLINE))
        self.tcp_client.expect("samplePeriod='15'")
        log.debug("DA Parameter Sample Interval Updated")

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 10)
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 10)

        ###
        # Test session timeout without activity
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=120, session_timeout=30)
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)

        ###
        # Test direct access session timeout with activity
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=30, session_timeout=60)
        # Send some activity every 30 seconds to keep DA alive.
        for i in range(1, 2, 3):
            self.tcp_client.send_data(NEWLINE)
            log.debug("Sending a little keep alive communication, sleeping for 15 seconds")
            time.sleep(15)

        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 45)

        ###
        # Test direct access disconnect
        ###
        self.assert_direct_access_start_telnet()
        self.tcp_client.disconnect()
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 30)

    def test_direct_access_telnet_mode_autosample(self):
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
        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 10)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet()

        log.debug("DA Server Started.  Adjust DA Parameter.")
        self.tcp_client.send_data("%ssetsamplePeriod=15%s" % (NEWLINE, NEWLINE))
        self.tcp_client.send_data("%sGetCD%s" % (NEWLINE, NEWLINE))
        self.tcp_client.expect("samplePeriod='15'")
        self.tcp_client.send_data("%sStart%s" % (NEWLINE, NEWLINE))
        time.sleep(3)

        log.debug("DA Parameter Sample Interval Updated")

        self.assert_direct_access_stop_telnet()

        # verify the setting got restored.
        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, 10)
        self.tcp_client.send_data("%sStart%s" % (NEWLINE, NEWLINE))
        time.sleep(3)
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 10)

        ###
        # Test session timeout without activity
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=120, session_timeout=30)
        self.tcp_client.send_data("%sStart%s" % (NEWLINE, NEWLINE))
        time.sleep(3)
        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, 60)

        ###
        # Test direct access session timeout with activity
        ###
        self.assert_direct_access_start_telnet(inactivity_timeout=30, session_timeout=60)
        self.tcp_client.send_data("%sStart%s" % (NEWLINE, NEWLINE))
        time.sleep(3)
        # Send some activity every 30 seconds to keep DA alive.
        for i in range(1, 2, 3):
            self.tcp_client.send_data(NEWLINE)
            log.debug("Sending a little keep alive communication, sleeping for 15 seconds")
            time.sleep(15)

        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, 75)

        ###
        # Test direct access disconnect
        ###
        self.assert_direct_access_start_telnet()
        self.tcp_client.send_data("%sStart%s" % (NEWLINE, NEWLINE))
        time.sleep(3)
        self.tcp_client.disconnect()
        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, 30)

    def test_direct_access_telnet_timeout(self):
        """
        Verify that DA times out as expected and transitions back to command mode.
        """
        self.assert_enter_command_mode()

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=30)
        self.assertTrue(self.tcp_client)

        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 90)

    def test_direct_access_telnet_disconnect(self):
        """
        Verify that a disconnection from the DA server transitions the agent back to
        command mode.
        """
        self.assert_enter_command_mode()

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)
        self.assertTrue(self.tcp_client)
        self.tcp_client.disconnect()

        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 30)

    def test_direct_access_telnet_autosample(self):
        """
        Verify we can handle an instrument state change while in DA
        """
        self.assert_enter_command_mode()

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet(timeout=600)

        self.tcp_client.send_data("%s%s" % (InstrumentCommands.START_LOGGING, NEWLINE))
        self.tcp_client.expect("S>")

        self.assert_sample_async(self.assert_particle_real_time, DataParticleType.PREST_REAL_TIME, 15)

        self.tcp_client.disconnect()

        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, 30)

    def test_sample_interval_set(self):
        """
        https://jira.oceanobservatories.org/tasks/browse/CISWMI-147
        There was an issue raised that the sample interval set wasn't behaving
        consistently.  This test is intended to replicate the error and verify
        the fix.
        """
        self.assert_enter_command_mode()

        log.debug("getting ready to set some parameters!  Start watching the sniffer")
        time.sleep(30)

        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 15)
        self.assert_start_autosample()
        self.assert_stop_autosample()
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 15)
        self.assert_direct_access_start_telnet()
        self.assert_direct_access_stop_telnet()
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 15)

        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 10)
        self.assert_start_autosample()
        self.assert_stop_autosample()
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 10)
        self.assert_direct_access_start_telnet()
        self.assert_direct_access_stop_telnet()
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 10)

        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 20)
        self.assert_start_autosample()
        self.assert_stop_autosample()
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 20)
        self.assert_direct_access_start_telnet()
        self.assert_direct_access_stop_telnet()
        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 20)

    def test_startup_params_first_pass(self):
        """
        Verify that startup parameters are applied correctly. Generally this
        happens in the driver discovery method.  We have two identical versions
        of this test so it is run twice.  First time to check and CHANGE, then
        the second time to check again.

        since nose orders the tests by ascii value this should run second.
        """
        self.assert_enter_command_mode()

        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 15)

        # Change these values anyway just in case it ran first.
        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 5)

    def test_startup_params_second_pass(self):
        """
        Verify that startup parameters are applied correctly. Generally this
        happens in the driver discovery method.  We have two identical versions
        of this test so it is run twice.  First time to check and CHANGE, then
        the second time to check again.

        since nose orders the tests by ascii value this should run second.
        """
        self.assert_enter_command_mode()

        self.assert_get_parameter(Parameter.SAMPLE_PERIOD, 15)

        # Change these values anyway just in case it ran first.
        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 5)

    def test_autosample_recovery(self):
        """
        @brief Verify that when the instrument automatically starts autosample
        the states are updated correctly
        """
        self.assert_enter_command_mode()
        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, 145)

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
                ProtocolEvent.CLOCK_SYNC,
                ProtocolEvent.SAMPLE_REFERENCE_OSCILLATOR,
                ProtocolEvent.TEST_EEPROM,
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
        ]

        self.assert_start_autosample()
        self.assert_capabilities(st_capabilities)
        self.assert_stop_autosample()

        ##################
        #  Command Mode
        ##################

        # We should be back in command mode from DA.
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 60)
        self.assert_capabilities(capabilities)

        #######################
        #  Streaming Recovery
        #######################

        # Command mode times out after 120 seconds.  This test will verify the agent states are correct
        self.assert_set_parameter(Parameter.SAMPLE_PERIOD, 1)
        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, 200)
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
