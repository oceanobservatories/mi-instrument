"""
@package mi.instrument.seabird.sbe16plus_v2.ooicore.test.test_driver
@file ion/services/mi/drivers/sbe16_plus_v2/test_sbe16_driver.py
@author Tapana Gupta
@brief Test cases for ctdbp_no driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u
       $ bin/test_driver -i
       $ bin/test_driver -q

"""
from mi.core.instrument.instrument_driver import DriverConfigKey

__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'

import unittest
import time

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger ; log = get_logger()

# MI imports.
from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import ParameterTestConfigKey
from mi.idk.unit_test import AgentCapabilityType

from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentCommandException

from mi.core.instrument.chunker import StringChunker

from mi.instrument.seabird.test.test_driver import SeaBirdUnitTest
from mi.instrument.seabird.test.test_driver import SeaBirdIntegrationTest
from mi.instrument.seabird.test.test_driver import SeaBirdQualificationTest
from mi.instrument.seabird.test.test_driver import SeaBirdPublicationTest

from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import ProtocolState
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import ProtocolEvent
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Capability
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Parameter
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import ConfirmedParameter
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Command
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SendOptodeCommand
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import ScheduledJob
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19DataParticleKey
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19StatusParticleKey
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19ConfigurationParticleKey
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import OptodeSettingsParticleKey
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Prompt
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import NEWLINE

from mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver import DataParticleType
from mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver import SBE16NOHardwareParticleKey
from mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver import SBE16NOCalibrationParticleKey
from mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver import SBE16NOProtocol
from mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver import InstrumentDriver

from mi.core.instrument.instrument_driver import ResourceAgentEvent
from mi.core.instrument.instrument_driver import ResourceAgentState

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id = 'JI22B5',
    instrument_agent_name = 'seabird_sbe16plus_v2_ctdbp_no',
    instrument_agent_packet_config = DataParticleType(),

    driver_startup_config = {DriverConfigKey.PARAMETERS:
            {Parameter.PTYPE: 3,
             Parameter.VOLT0: True,
             Parameter.VOLT1: True,
             Parameter.VOLT2: False,
             Parameter.VOLT3: False,
             Parameter.VOLT4: False,
             Parameter.VOLT5: False,
             Parameter.SBE38: False,
             Parameter.WETLABS: False,
             Parameter.GTD: False,
             Parameter.DUAL_GTD: False,
             Parameter.SBE63: False,
             Parameter.OPTODE: True,
             Parameter.OUTPUT_FORMAT: 0,
             Parameter.NUM_AVG_SAMPLES: 4,
             Parameter.MIN_COND_FREQ: 500,
             Parameter.PUMP_DELAY: 60,
             Parameter.AUTO_RUN: False,
             Parameter.IGNORE_SWITCH: True,
             Parameter.CLOCK_INTERVAL: '00:00:00',
             Parameter.STATUS_INTERVAL: '00:00:00'}}
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
class SBE16NOMixin(DriverTestMixin):

    InstrumentDriver = InstrumentDriver

    '''
    Mixin class used for storing data particle constants and common data assertion methods.
    '''

    # Create some short names for the parameter test config
    TYPE      = ParameterTestConfigKey.TYPE
    READONLY  = ParameterTestConfigKey.READONLY
    STARTUP   = ParameterTestConfigKey.STARTUP
    DA        = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE     = ParameterTestConfigKey.VALUE
    REQUIRED  = ParameterTestConfigKey.REQUIRED
    DEFAULT   = ParameterTestConfigKey.DEFAULT
    STATES    = ParameterTestConfigKey.STATES

    ###
    #  Instrument output (driver input) Definitions
    ###
    VALID_SAMPLE = "04570F0A1E910828FC47BC59F199952C64C9" + NEWLINE

    VALID_GETHD_RESPONSE =  "" + \
"<HardwareData DeviceType = 'SBE19plus' SerialNumber = '01907230'>" + NEWLINE + \
"   <Manufacturer>Sea-Bird Electronics, Inc.</Manufacturer>" + NEWLINE + \
"   <FirmwareVersion>2.5.2</FirmwareVersion>" + NEWLINE + \
"   <FirmwareDate>12 Mar 2013 11:50</FirmwareDate>" + NEWLINE + \
"   <CommandSetVersion>1.3</CommandSetVersion>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '49565' AssemblyNum = '41054H'/>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '43360' AssemblyNum = '41580B'/>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '49357' AssemblyNum = '41606'/>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '38072' AssemblyNum = '41057A'/>" + NEWLINE + \
"   <MfgDate>29-Oct-2012</MfgDate>" + NEWLINE + \
"   <InternalSensors>" + NEWLINE + \
"      <Sensor id = 'Main Temperature'>" + NEWLINE + \
"         <type>temperature0</type>" + NEWLINE + \
"         <SerialNumber>01907230</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'Main Conductivity'>" + NEWLINE + \
"         <type>conductivity-0</type>" + NEWLINE + \
"         <SerialNumber>01907230</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'Main Pressure'>" + NEWLINE + \
"         <type>quartzTC-0</type>" + NEWLINE + \
"         <SerialNumber>124969</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"   </InternalSensors>" + NEWLINE + \
"   <ExternalSensors>" + NEWLINE + \
"      <Sensor id = 'volt 0'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 1'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 2'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 3'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 4'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 5'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'serial'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"   </ExternalSensors>" + NEWLINE + \
"</HardwareData>" + NEWLINE

    VALID_GETCC_RESPONSE =  "" + \
"<CalibrationCoefficients DeviceType = 'SBE19plus' SerialNumber = '01907230'>" + NEWLINE + \
"   <Calibration format = 'TEMP1' id = 'Main Temperature'>" + NEWLINE + \
"      <SerialNum>01907230</SerialNum>" + NEWLINE + \
"      <CalDate>07-Dec-13</CalDate>" + NEWLINE + \
"      <TA0>1.272723e-03</TA0>" + NEWLINE + \
"      <TA1>2.687218e-04</TA1>" + NEWLINE + \
"      <TA2>-4.735777e-07</TA2>" + NEWLINE + \
"      <TA3>1.522571e-07</TA3>" + NEWLINE + \
"      <TOFFSET>0.000000e+00</TOFFSET>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'WBCOND0' id = 'Main Conductivity'>" + NEWLINE + \
"      <SerialNum>01907230</SerialNum>" + NEWLINE + \
"      <CalDate>07-Dec-13</CalDate>" + NEWLINE + \
"      <G>-9.931677e-01</G>" + NEWLINE + \
"      <H>1.391189e-01</H>" + NEWLINE + \
"      <I>-4.457962e-04</I>" + NEWLINE + \
"      <J>5.145191e-05</J>" + NEWLINE + \
"      <CPCOR>-9.570000e-08</CPCOR>" + NEWLINE + \
"      <CTCOR>3.250000e-06</CTCOR>" + NEWLINE + \
"      <CSLOPE>1.000000e+00</CSLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'QUARTZ0' id = 'Main Pressure'>" + NEWLINE + \
"      <SerialNum>124969</SerialNum>" + NEWLINE + \
"      <CalDate>05-Dec-13</CalDate>" + NEWLINE + \
"      <PC1>9.913353e+02</PC1>" + NEWLINE + \
"      <PC2>1.013600e-05</PC2>" + NEWLINE + \
"      <PC3>-1.182100e-04</PC3>" + NEWLINE + \
"      <PD1>3.107200e-02</PD1>" + NEWLINE + \
"      <PD2>0.000000e+00</PD2>" + NEWLINE + \
"      <PT1>2.767451e+01</PT1>" + NEWLINE + \
"      <PT2>-1.080330e-04</PT2>" + NEWLINE + \
"      <PT3>1.036700e-06</PT3>" + NEWLINE + \
"      <PT4>1.687490e-09</PT4>" + NEWLINE + \
"      <PSLOPE>1.000000e+00</PSLOPE>" + NEWLINE + \
"      <POFFSET>0.000000e+00</POFFSET>" + NEWLINE + \
"      <PRANGE>2.000000e+02</PRANGE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 0'>" + NEWLINE + \
"      <OFFSET>-4.719895e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.248055e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 1'>" + NEWLINE + \
"      <OFFSET>-4.677263e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.249706e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 2'>" + NEWLINE + \
"      <OFFSET>-4.673579e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.247281e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 3'>" + NEWLINE + \
"      <OFFSET>-4.665053e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.248687e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 4'>" + NEWLINE + \
"      <OFFSET>-4.620527e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.248225e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 5'>" + NEWLINE + \
"      <OFFSET>-4.645263e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.249040e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'FREQ0' id = 'external frequency channel'>" + NEWLINE + \
"      <EXTFREQSF>9.999944e-01</EXTFREQSF>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"</CalibrationCoefficients>" + NEWLINE

    VALID_GETCD_RESPONSE =  "" + \
"<ConfigurationData DeviceType = 'SBE19plus' SerialNumber = '01907230'>" + NEWLINE + \
"   <ProfileMode>" + NEWLINE + \
"      <ScansToAverage>4</ScansToAverage>" + NEWLINE + \
"      <MinimumCondFreq>500</MinimumCondFreq>" + NEWLINE + \
"      <PumpDelay>60</PumpDelay>" + NEWLINE + \
"      <AutoRun>no</AutoRun>" + NEWLINE + \
"      <IgnoreSwitch>yes</IgnoreSwitch>" + NEWLINE + \
"   </ProfileMode>" + NEWLINE + \
"   <Battery>" + NEWLINE + \
"      <Type>alkaline</Type>" + NEWLINE + \
"      <CutOff>7.5</CutOff>" + NEWLINE + \
"   </Battery>" + NEWLINE + \
"   <DataChannels>" + NEWLINE + \
"      <ExtVolt0>yes</ExtVolt0>" + NEWLINE + \
"      <ExtVolt1>yes</ExtVolt1>" + NEWLINE + \
"      <ExtVolt2>no</ExtVolt2>" + NEWLINE + \
"      <ExtVolt3>no</ExtVolt3>" + NEWLINE + \
"      <ExtVolt4>no</ExtVolt4>" + NEWLINE + \
"      <ExtVolt5>no</ExtVolt5>" + NEWLINE + \
"      <SBE38>no</SBE38>" + NEWLINE + \
"      <WETLABS>no</WETLABS>" + NEWLINE + \
"      <OPTODE>yes</OPTODE>" + NEWLINE + \
"      <SBE63>no</SBE63>" + NEWLINE + \
"      <GTD>no</GTD>" + NEWLINE + \
"   </DataChannels>" + NEWLINE + \
"   <EchoCharacters>yes</EchoCharacters>" + NEWLINE + \
"   <OutputExecutedTag>no</OutputExecutedTag>" + NEWLINE + \
"   <OutputFormat>raw HEX</OutputFormat>" + NEWLINE + \
"</ConfigurationData>" + NEWLINE

    VALID_GETSD_RESPONSE =  "" + \
"<StatusData DeviceType = 'SBE19plus' SerialNumber = '01907230'>" + NEWLINE + \
"   <DateTime>2014-05-08T21:58:38</DateTime>" + NEWLINE + \
"   <LoggingState>not logging</LoggingState>" + NEWLINE + \
"   <EventSummary numEvents = '3'/>" + NEWLINE + \
"   <Power>" + NEWLINE + \
"      <vMain>12.9</vMain>" + NEWLINE + \
"      <vLith>8.5</vLith>" + NEWLINE + \
"      <iMain>51.1</iMain>" + NEWLINE + \
"      <iPump> 0.4</iPump>" + NEWLINE + \
"      <iExt01> 0.4</iExt01>" + NEWLINE + \
"      <iSerial>46.9</iSerial>" + NEWLINE + \
"   </Power>" + NEWLINE + \
"   <MemorySummary>" + NEWLINE + \
"      <Bytes>1224</Bytes>" + NEWLINE + \
"      <Samples>68</Samples>" + NEWLINE + \
"      <SamplesFree>3655384</SamplesFree>" + NEWLINE + \
"      <SampleLength>18</SampleLength>" + NEWLINE + \
"      <Profiles>4</Profiles>" + NEWLINE + \
"   </MemorySummary>" + NEWLINE + \
"</StatusData>" + NEWLINE

    VALID_SEND_OPTODE_RESPONSE = "" + \
'Optode RX = Analog Output	4831	134	CalPhase' + NEWLINE + \
'Optode RX = CalPhase[Deg]	4831	134	30.050' + NEWLINE + \
'S>sendoptode=get enable temperature' + NEWLINE + \
'Sending Optode: get enable temperature' + NEWLINE + NEWLINE + \
'Optode RX = Enable Temperature	4831	134	No' + NEWLINE + \
'S>sendoptode=get enable text' + NEWLINE + \
'Sending Optode: get enable text' + NEWLINE + NEWLINE + \
'Optode RX = Enable Text 	4831	134	No' + NEWLINE + \
'S>sendoptode=get enable humiditycomp' + NEWLINE + \
'Sending Optode: get enable humiditycomp' + NEWLINE + NEWLINE + \
'Optode RX = Enable HumidityComp	 4831	134	Yes' + NEWLINE + \
'S>sendoptode=get enable airsaturation' + NEWLINE + \
'Sending Optode: get enable airsaturation' + NEWLINE + NEWLINE + \
'Optode RX = Enable AirSaturation	4831	134	No' + NEWLINE + \
'S>sendoptode=get enable rawdata' + NEWLINE + \
'Sending Optode: get enable rawdata' + NEWLINE + NEWLINE + \
'Optode RX = Enable Rawdata	4831	134	No' + NEWLINE + \
'S>sendoptode=get interval' + NEWLINE + \
'Sending Optode: get interval' + NEWLINE + NEWLINE + \
'Optode RX = Interval	4831	134	5.000' + NEWLINE + \
'S>sendoptode=get mode' + NEWLINE + \
'Sending Optode: get mode' + NEWLINE + NEWLINE + \
'Optode RX = Mode	4831	134	Smart Sensor Terminal' + NEWLINE


    VALID_DS_RESPONSE = 'SBE 19plus V 2.3  SERIAL NO. 6914    18 Apr 2014 19:14:13' + NEWLINE + \
        'vbatt = 23.3, vlith =  8.5, ioper =  62.1 ma, ipump =  71.7 ma, ' + NEWLINE + \
        'iext01 =   0.2 ma, iserial =  26.0 ma' + NEWLINE + \
        'status = not logging' + NEWLINE + \
        'number of scans to average = 4' + NEWLINE + \
        'samples = 1861, free = 3653591, casts = 7' + NEWLINE + \
        'mode = profile, minimum cond freq = 500, pump delay = 60 sec' + NEWLINE + \
        'autorun = no, ignore magnetic switch = yes' + NEWLINE + \
        'battery type = alkaline, battery cutoff =  7.5 volts' + NEWLINE + \
        'pressure sensor = quartz with temp comp, range = 508.0' + NEWLINE + \
        'SBE 38 = no, WETLABS = no, OPTODE = yes, SBE63 = no, Gas Tension Device = no' + NEWLINE + \
        'Ext Volt 0 = yes, Ext Volt 1 = yes' + NEWLINE + \
        'Ext Volt 2 = no, Ext Volt 3 = no' + NEWLINE + \
        'Ext Volt 4 = no, Ext Volt 5 = no' + NEWLINE + \
        'echo characters = no' + NEWLINE + \
        'output format = raw HEX' + NEWLINE

    ###
    #  Parameter and Type Definitions
    ###

    _sample_parameters = {
        SBE19DataParticleKey.TEMP: {TYPE: int, VALUE: 284431, REQUIRED: True },
        SBE19DataParticleKey.CONDUCTIVITY: {TYPE: int, VALUE: 663185, REQUIRED: True },
        SBE19DataParticleKey.PRESSURE: {TYPE: int, VALUE: 534780, REQUIRED: True },
        SBE19DataParticleKey.PRESSURE_TEMP: {TYPE: int, VALUE: 18364, REQUIRED: True },
        SBE19DataParticleKey.VOLT0: {TYPE: int, VALUE: 23025, REQUIRED: True },
        SBE19DataParticleKey.VOLT1: {TYPE: int, VALUE: 39317, REQUIRED: True },
        SBE19DataParticleKey.OXYGEN: {TYPE: int, VALUE: 2909385, REQUIRED: True },

    }

    _configuration_parameters = {
        SBE19ConfigurationParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True},
        SBE19ConfigurationParticleKey.SCANS_TO_AVERAGE: {TYPE: int, VALUE: 4, REQUIRED: True},
        SBE19ConfigurationParticleKey.MIN_COND_FREQ: {TYPE: int, VALUE: 500, REQUIRED: True},
        SBE19ConfigurationParticleKey.PUMP_DELAY: {TYPE: int, VALUE: 60, REQUIRED: True},
        SBE19ConfigurationParticleKey.AUTO_RUN: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.IGNORE_SWITCH: {TYPE: int, VALUE: 1, REQUIRED: True},
        SBE19ConfigurationParticleKey.BATTERY_TYPE: {TYPE: unicode, VALUE: "alkaline", REQUIRED: True},
        SBE19ConfigurationParticleKey.BATTERY_CUTOFF: {TYPE: float, VALUE: 7.5, REQUIRED: True},
        SBE19ConfigurationParticleKey.EXT_VOLT_0: {TYPE: int, VALUE: 1, REQUIRED: True},
        SBE19ConfigurationParticleKey.EXT_VOLT_1: {TYPE: int, VALUE: 1, REQUIRED: True},
        SBE19ConfigurationParticleKey.EXT_VOLT_2: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.EXT_VOLT_3: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.EXT_VOLT_4: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.EXT_VOLT_5: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.SBE38: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.WETLABS: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.OPTODE: {TYPE: int, VALUE: 1, REQUIRED: True},
        SBE19ConfigurationParticleKey.SBE63: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.GAS_TENSION_DEVICE: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.ECHO_CHARACTERS: {TYPE: int, VALUE: 1, REQUIRED: True},
        SBE19ConfigurationParticleKey.OUTPUT_EXECUTED_TAG: {TYPE: int, VALUE: 0, REQUIRED: True},
        SBE19ConfigurationParticleKey.OUTPUT_FORMAT: {TYPE: unicode, VALUE: "raw HEX", REQUIRED: True},
    }

    _status_parameters = {
        SBE19StatusParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True},
        SBE19StatusParticleKey.DATE_TIME: {TYPE: unicode, VALUE: "2014-05-08T21:58:38", REQUIRED: True},
        SBE19StatusParticleKey.LOGGING_STATE: {TYPE: unicode, VALUE: "not logging", REQUIRED: True},
        SBE19StatusParticleKey.NUMBER_OF_EVENTS: {TYPE: int, VALUE: 3, REQUIRED: True},
        SBE19StatusParticleKey.BATTERY_VOLTAGE_MAIN: {TYPE: float, VALUE: 12.9, REQUIRED: True},
        SBE19StatusParticleKey.BATTERY_VOLTAGE_LITHIUM: {TYPE: float, VALUE: 8.5, REQUIRED: True},
        SBE19StatusParticleKey.OPERATIONAL_CURRENT: {TYPE: float, VALUE: 51.1, REQUIRED: True},
        SBE19StatusParticleKey.PUMP_CURRENT: {TYPE: float, VALUE: 0.4, REQUIRED: True},
        SBE19StatusParticleKey.EXT_V01_CURRENT: {TYPE: float, VALUE: 0.4, REQUIRED: True},
        SBE19StatusParticleKey.SERIAL_CURRENT: {TYPE: float, VALUE: 46.9, REQUIRED: True},
        SBE19StatusParticleKey.MEMORY_FREE: {TYPE: int, VALUE: 1224, REQUIRED: True},
        SBE19StatusParticleKey.NUMBER_OF_SAMPLES: {TYPE: int, VALUE: 68, REQUIRED: True},
        SBE19StatusParticleKey.SAMPLES_FREE: {TYPE: int, VALUE: 3655384, REQUIRED: True},
        SBE19StatusParticleKey.SAMPLE_LENGTH: {TYPE: int, VALUE: 18, REQUIRED: True},
        SBE19StatusParticleKey.PROFILES: {TYPE: int, VALUE: 4, REQUIRED: True},
    }

    _hardware_parameters = {
        SBE16NOHardwareParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True},
        SBE16NOHardwareParticleKey.FIRMWARE_VERSION: {TYPE: unicode, VALUE: '2.5.2', REQUIRED: True},
        SBE16NOHardwareParticleKey.FIRMWARE_DATE: {TYPE: unicode, VALUE: '12 Mar 2013 11:50', REQUIRED: True},
        SBE16NOHardwareParticleKey.COMMAND_SET_VERSION: {TYPE: unicode, VALUE: '1.3', REQUIRED: True},
        SBE16NOHardwareParticleKey.PCB_SERIAL_NUMBER: {TYPE: list, VALUE: ['49565', '43360', '49357', '38072'], REQUIRED: True},
        SBE16NOHardwareParticleKey.ASSEMBLY_NUMBER: {TYPE: list, VALUE: ['41054H', '41580B', '41606', '41057A'], REQUIRED: True},
        SBE16NOHardwareParticleKey.MANUFACTURE_DATE: {TYPE: unicode, VALUE: '29-Oct-2012', REQUIRED: True},
        SBE16NOHardwareParticleKey.TEMPERATURE_SENSOR_SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True},
        SBE16NOHardwareParticleKey.CONDUCTIVITY_SENSOR_SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True},
        SBE16NOHardwareParticleKey.PRESSURE_SENSOR_SERIAL_NUMBER: {TYPE: unicode, VALUE: '124969', REQUIRED: True},
        SBE16NOHardwareParticleKey.PRESSURE_SENSOR_TYPE: {TYPE: unicode, VALUE: 'quartzTC-0', REQUIRED: True},
        SBE16NOHardwareParticleKey.VOLT0_TYPE: {TYPE: unicode, VALUE: 'not assigned', REQUIRED: True},
        SBE16NOHardwareParticleKey.VOLT0_SERIAL_NUMBER: {TYPE: unicode, VALUE: 'not assigned', REQUIRED: True},
        SBE16NOHardwareParticleKey.VOLT1_TYPE: {TYPE: unicode, VALUE: 'not assigned', REQUIRED: True},
        SBE16NOHardwareParticleKey.VOLT1_SERIAL_NUMBER: {TYPE: unicode, VALUE: 'not assigned', REQUIRED: True},
    }

    _calibration_parameters = {
        SBE16NOCalibrationParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True},
        SBE16NOCalibrationParticleKey.TEMP_SENSOR_SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True },
        SBE16NOCalibrationParticleKey.TEMP_CAL_DATE: {TYPE: unicode, VALUE: "07-Dec-13", REQUIRED: True},
        SBE16NOCalibrationParticleKey.TA0: {TYPE: float, VALUE: 1.272723e-03, REQUIRED: True},
        SBE16NOCalibrationParticleKey.TA1: {TYPE: float, VALUE: 2.687218e-04, REQUIRED: True},
        SBE16NOCalibrationParticleKey.TA2: {TYPE: float, VALUE: -4.735777e-07, REQUIRED: True},
        SBE16NOCalibrationParticleKey.TA3: {TYPE: float, VALUE: 1.522571e-07, REQUIRED: True},
        SBE16NOCalibrationParticleKey.TOFFSET: {TYPE: float, VALUE: 0.0, REQUIRED: True},
        SBE16NOCalibrationParticleKey.COND_SENSOR_SERIAL_NUMBER: {TYPE: unicode, VALUE: '01907230', REQUIRED: True },
        SBE16NOCalibrationParticleKey.COND_CAL_DATE: {TYPE: unicode, VALUE: '07-Dec-13', REQUIRED: True},
        SBE16NOCalibrationParticleKey.CONDG: {TYPE: float, VALUE: -9.931677e-01, REQUIRED: True},
        SBE16NOCalibrationParticleKey.CONDH: {TYPE: float, VALUE: 1.391189e-01, REQUIRED: True},
        SBE16NOCalibrationParticleKey.CONDI: {TYPE: float, VALUE: -4.457962e-04, REQUIRED: True},
        SBE16NOCalibrationParticleKey.CONDJ: {TYPE: float, VALUE: 5.145191e-05, REQUIRED: True},
        SBE16NOCalibrationParticleKey.CPCOR: {TYPE: float, VALUE: -9.570000e-08, REQUIRED: True},
        SBE16NOCalibrationParticleKey.CTCOR: {TYPE: float, VALUE: 3.250000e-06, REQUIRED: True},
        SBE16NOCalibrationParticleKey.CSLOPE: {TYPE: float, VALUE: 1.0, REQUIRED: True},
        SBE16NOCalibrationParticleKey.PRES_SERIAL_NUMBER: {TYPE: unicode, VALUE: '124969', REQUIRED: True },
        SBE16NOCalibrationParticleKey.PRES_CAL_DATE: {TYPE: unicode, VALUE: '05-Dec-13', REQUIRED: True },
        SBE16NOCalibrationParticleKey.PC1: {TYPE: float, VALUE: 9.913353e+02, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PC2: {TYPE: float, VALUE:1.013600e-05, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PC3: {TYPE: float, VALUE: -1.182100e-04, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PD1: {TYPE: float, VALUE: 3.107200e-02, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PD2: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PT1: {TYPE: float, VALUE: 2.767451e+01, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PT2: {TYPE: float, VALUE: -1.080330e-04, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PT3: {TYPE: float, VALUE: 1.036700e-06, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PT4: {TYPE: float, VALUE: 1.687490e-09, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PSLOPE: {TYPE: float, VALUE: 1.000000e+00, REQUIRED: True },
        SBE16NOCalibrationParticleKey.POFFSET: {TYPE: float, VALUE: 0.000000e+00, REQUIRED: True },
        SBE16NOCalibrationParticleKey.PRES_RANGE: {TYPE: int, VALUE: 2.000000e+02, REQUIRED: True },
        SBE16NOCalibrationParticleKey.EXT_VOLT0_OFFSET: {TYPE: float, VALUE: -4.719895e-02, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT0_SLOPE: {TYPE: float, VALUE: 1.248055e+00, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT1_OFFSET: {TYPE: float, VALUE: -4.677263e-02, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT1_SLOPE: {TYPE: float, VALUE: 1.249706e+00, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT2_OFFSET: {TYPE: float, VALUE: -4.673579e-02, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT2_SLOPE: {TYPE: float, VALUE: 1.247281e+00, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT3_OFFSET: {TYPE: float, VALUE: -4.665053e-02, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT3_SLOPE: {TYPE: float, VALUE: 1.248687e+00, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT4_OFFSET: {TYPE: float, VALUE: -4.620527e-02, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT4_SLOPE: {TYPE: float, VALUE: 1.248225e+00, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT5_OFFSET: {TYPE: float, VALUE: -4.645263e-02, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_VOLT5_SLOPE: {TYPE: float, VALUE: 1.249040e+00, REQUIRED: True},
        SBE16NOCalibrationParticleKey.EXT_FREQ: {TYPE: float, VALUE: 9.999944e-01, REQUIRED: True},
    }

    _send_optode_parameters = {
        OptodeSettingsParticleKey.ANALOG_OUTPUT: {TYPE: unicode, VALUE: 'CalPhase', REQUIRED: True},
        OptodeSettingsParticleKey.CALPHASE: {TYPE: float, VALUE: 30.050, REQUIRED: True},
        OptodeSettingsParticleKey.ENABLE_AIR_SAT: {TYPE: int, VALUE: 0, REQUIRED: True},
        OptodeSettingsParticleKey.ENABLE_RAW_DATA: {TYPE: int, VALUE: 0, REQUIRED: True},
        OptodeSettingsParticleKey.ENABLE_HUM_COMP: {TYPE: int, VALUE: 1, REQUIRED: True},
        OptodeSettingsParticleKey.ENABLE_TEMP: {TYPE: int, VALUE: 0, REQUIRED: True},
        OptodeSettingsParticleKey.ENABLE_TEXT: {TYPE: int, VALUE: 0, REQUIRED: True},
        OptodeSettingsParticleKey.INTERVAL: {TYPE: float, VALUE: 5.000, REQUIRED: True},
        OptodeSettingsParticleKey.MODE: {TYPE: unicode, VALUE: 'Smart Sensor Terminal', REQUIRED: True},

    }

    ###
    #  Parameter and Type Definitions
    ###
    _driver_parameters = {
        # Parameters defined in the IOS
        Parameter.DATE_TIME : {TYPE: str, READONLY: True, DA: False, STARTUP: False},
        Parameter.PTYPE : {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 3, VALUE: 3},
        Parameter.VOLT0 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT1 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.VOLT2 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.VOLT3 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.VOLT4 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.VOLT5 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.SBE38 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.WETLABS : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.GTD : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.DUAL_GTD : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.SBE63 : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.OPTODE : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.OUTPUT_FORMAT : {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 0, VALUE: 0},
        Parameter.NUM_AVG_SAMPLES : {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 4, VALUE: 4},
        Parameter.MIN_COND_FREQ : {TYPE: int, READONLY: True, DA: True, STARTUP: True, DEFAULT: 500, VALUE: 500},
        Parameter.PUMP_DELAY : {TYPE: int, READONLY: False, DA: True, STARTUP: True, DEFAULT: 60, VALUE: 60},
        Parameter.AUTO_RUN : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: False, VALUE: False},
        Parameter.IGNORE_SWITCH : {TYPE: bool, READONLY: True, DA: True, STARTUP: True, DEFAULT: True, VALUE: True},
        Parameter.LOGGING : {TYPE: bool, READONLY: True, DA: False, STARTUP: False},
        Parameter.CLOCK_INTERVAL : {TYPE: str, READONLY: False, DA: False, STARTUP: True, DEFAULT: '00:00:00', VALUE: '00:00:00'},
        Parameter.STATUS_INTERVAL : {TYPE: str, READONLY: False, DA: False, STARTUP: True, DEFAULT: '00:00:00', VALUE: '00:00:00'},
    }

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.ACQUIRE_SAMPLE : {STATES: [ProtocolState.COMMAND]},
        Capability.START_AUTOSAMPLE : {STATES: [ProtocolState.COMMAND]},
        Capability.STOP_AUTOSAMPLE : {STATES: [ProtocolState.AUTOSAMPLE]},
        Capability.CLOCK_SYNC : {STATES: [ProtocolState.COMMAND]},
        Capability.ACQUIRE_STATUS : {STATES: [ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE]},

    }

    def assert_driver_parameters(self, current_parameters, verify_values = False):
        """
        Verify that all driver parameters are correct and potentially verify values.
        @param current_parameters: driver parameters read from the driver instance
        @param verify_values: should we verify values against definition?
        """
        self.assert_parameters(current_parameters, self._driver_parameters, verify_values)

    def assert_particle_sample(self, data_particle, verify_values = False):
        '''
        Verify sample particle
        @param data_particle:  SBE19DataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_keys(SBE19DataParticleKey, self._sample_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.CTD_PARSED, require_instrument_timestamp=False)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters, verify_values)

    def assert_particle_hardware(self, data_particle, verify_values = False):
        '''
        Verify hardware particle
        @param data_particle:  SBE19HardwareParticle data particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_keys(SBE16NOHardwareParticleKey, self._hardware_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_HARDWARE)
        self.assert_data_particle_parameters(data_particle, self._hardware_parameters, verify_values)

    def assert_particle_calibration(self, data_particle, verify_values = False):
        '''
        Verify sample particle
        @param data_particle:  SBE19CalibrationParticle calibration particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_keys(SBE16NOCalibrationParticleKey, self._calibration_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_CALIBRATION)
        self.assert_data_particle_parameters(data_particle, self._calibration_parameters, verify_values)

    def assert_particle_status(self, data_particle, verify_values = False):
        '''
        Verify status particle
        @param data_particle:  SBE19StatusParticle status particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_keys(SBE19StatusParticleKey, self._status_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_STATUS)
        self.assert_data_particle_parameters(data_particle, self._status_parameters, verify_values)

    def assert_particle_configuration(self, data_particle, verify_values = False):
        '''
        Verify configuration particle
        @param data_particle:  SBE19ConfigurationParticle configuration particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_keys(SBE19ConfigurationParticleKey, self._configuration_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.DEVICE_CONFIGURATION)
        self.assert_data_particle_parameters(data_particle, self._configuration_parameters, verify_values)

    def assert_particle_send_optode(self, data_particle, verify_values = False):
        '''
        Verify send optode particle
        @param data_particle:  SBE19EventCounterParticle event counter particle
        @param verify_values:  bool, should we verify parameter values
        '''
        self.assert_data_particle_keys(OptodeSettingsParticleKey, self._send_optode_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.OPTODE_SETTINGS)
        self.assert_data_particle_parameters(data_particle, self._send_optode_parameters, verify_values)

###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class SBE16NOUnitTestCase(SeaBirdUnitTest, SBE16NOMixin):
    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilites
        """
        self.assert_enum_has_no_duplicates(ScheduledJob())
        self.assert_enum_has_no_duplicates(Command())
        self.assert_enum_has_no_duplicates(SendOptodeCommand())
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())

        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_complete(ConfirmedParameter(), Parameter())

        # Test capabilites for duplicates, then verify that capabilities is a subset of proto events
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
        chunker = StringChunker(SBE16NOProtocol.sieve_function)

        self.assert_chunker_sample(chunker, self.VALID_SAMPLE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_SAMPLE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_SAMPLE)
        self.assert_chunker_combined_sample(chunker, self.VALID_SAMPLE)

        self.assert_chunker_sample(chunker, self.VALID_GETHD_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_GETHD_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_GETHD_RESPONSE)
        self.assert_chunker_combined_sample(chunker, self.VALID_GETHD_RESPONSE)

        self.assert_chunker_sample(chunker, self.VALID_GETCC_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_GETCC_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_GETCC_RESPONSE)
        self.assert_chunker_combined_sample(chunker, self.VALID_GETCC_RESPONSE)

        self.assert_chunker_sample(chunker, self.VALID_GETSD_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_GETSD_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_GETSD_RESPONSE)
        self.assert_chunker_combined_sample(chunker, self.VALID_GETSD_RESPONSE)

        self.assert_chunker_sample(chunker, self.VALID_GETCD_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_GETCD_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_GETCD_RESPONSE)
        self.assert_chunker_combined_sample(chunker, self.VALID_GETCD_RESPONSE)

        self.assert_chunker_sample(chunker, self.VALID_SEND_OPTODE_RESPONSE)
        self.assert_chunker_sample_with_noise(chunker, self.VALID_SEND_OPTODE_RESPONSE)
        self.assert_chunker_fragmented_sample(chunker, self.VALID_SEND_OPTODE_RESPONSE)
        self.assert_chunker_combined_sample(chunker, self.VALID_SEND_OPTODE_RESPONSE)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles
        self.assert_particle_published(driver, self.VALID_SAMPLE, self.assert_particle_sample, True)
        self.assert_particle_published(driver, self.VALID_GETHD_RESPONSE, self.assert_particle_hardware, True)
        self.assert_particle_published(driver, self.VALID_GETCC_RESPONSE, self.assert_particle_calibration, True)
        self.assert_particle_published(driver, self.VALID_GETSD_RESPONSE, self.assert_particle_status, True)
        self.assert_particle_published(driver, self.VALID_GETCD_RESPONSE, self.assert_particle_configuration, True)
        self.assert_particle_published(driver, self.VALID_SEND_OPTODE_RESPONSE, self.assert_particle_send_optode, True)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        my_event_callback = Mock()
        protocol = SBE16NOProtocol(Prompt, NEWLINE, my_event_callback)
        driver_capabilities = Capability.list()
        test_capabilities = Capability.list()

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
            ProtocolState.UNKNOWN: ['DRIVER_EVENT_DISCOVER'],
            ProtocolState.COMMAND: ['DRIVER_EVENT_ACQUIRE_SAMPLE',
                                    'DRIVER_EVENT_ACQUIRE_STATUS',
                                    'PROTOCOL_EVENT_SCHEDULED_ACQUIRE_STATUS',
                                    'DRIVER_EVENT_CLOCK_SYNC',
                                    'DRIVER_EVENT_GET',
                                    'DRIVER_EVENT_SET',
                                    'DRIVER_EVENT_START_AUTOSAMPLE',
                                    'DRIVER_EVENT_START_DIRECT',
                                    'PROTOCOL_EVENT_GET_CONFIGURATION',
                                    'DRIVER_EVENT_SCHEDULED_CLOCK_SYNC'],
            ProtocolState.AUTOSAMPLE: ['DRIVER_EVENT_GET',
                                       'DRIVER_EVENT_STOP_AUTOSAMPLE',
                                       'PROTOCOL_EVENT_GET_CONFIGURATION',
                                       'DRIVER_EVENT_SCHEDULED_CLOCK_SYNC',
                                       'PROTOCOL_EVENT_SCHEDULED_ACQUIRE_STATUS',
                                       'DRIVER_EVENT_ACQUIRE_STATUS'],
            ProtocolState.DIRECT_ACCESS: ['DRIVER_EVENT_STOP_DIRECT', 'EXECUTE_DIRECT']
        }

        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)

    def test_parse_ds(self):
        """
        Verify that the DS command gets parsed correctly and check that the param dict gets updated
        """
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, ProtocolState.COMMAND)
        source = self.VALID_DS_RESPONSE

        baseline = driver._protocol._param_dict.get_current_timestamp()

        # First verify that parse ds sets all know parameters.
        driver._protocol._parse_dsdc_response(source, Prompt.COMMAND)

        # Set param dict values not parsed in from the instrument response
        driver._protocol._param_dict.set_value(Parameter.CLOCK_INTERVAL, "00:00:00")
        driver._protocol._param_dict.set_value(Parameter.STATUS_INTERVAL, "00:00:00")

        pd = driver._protocol._param_dict.get_all(baseline)
        log.debug("Param Dict Values: %s" % pd)
        log.debug("Param Sample: %s" % source)
        self.assert_driver_parameters(pd, True)

        # Now change some things and make sure they are parsed properly
        # Note:  Only checking parameters that can change.

        # Logging
        source = source.replace("= not logging", "= logging")
        log.debug("Param Sample: %s" % source)
        driver._protocol._parse_dsdc_response(source, Prompt.COMMAND)
        pd = driver._protocol._param_dict.get_all(baseline)
        self.assertTrue(pd.get(Parameter.LOGGING))

        # NAvg
        source = source.replace("scans to average = 4", "scans to average = 2")
        log.debug("Param Sample: %s" % source)
        driver._protocol._parse_dsdc_response(source, Prompt.COMMAND)
        pd = driver._protocol._param_dict.get_all(baseline)
        self.assertEqual(pd.get(Parameter.NUM_AVG_SAMPLES), 2)

        # Optode
        source = source.replace("OPTODE = yes", "OPTODE = no")
        log.debug("Param Sample: %s" % source)
        driver._protocol._parse_dsdc_response(source, Prompt.COMMAND)
        pd = driver._protocol._param_dict.get_all(baseline)
        self.assertFalse(pd.get(Parameter.OPTODE))

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


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class SBE16NOIntTestCase(SeaBirdIntegrationTest, SBE16NOMixin):

    def test_connection(self):
        self.assert_initialize_driver()

    def test_set(self):
        """
        Test all set commands. Verify all exception cases.
        """
        self.assert_initialize_driver()

        # Verify we can set all parameters in bulk
        new_values = {
            Parameter.PUMP_DELAY: 55,
            Parameter.NUM_AVG_SAMPLES: 2
        }
        self.assert_set_bulk(new_values)

        # Pump Delay: Range 0 - 600 seconds
        self.assert_set(Parameter.PUMP_DELAY, 0)
        self.assert_set(Parameter.PUMP_DELAY, 600)

        # Test bad values
        self.assert_set_exception(Parameter.PUMP_DELAY, -1)
        self.assert_set_exception(Parameter.PUMP_DELAY, 601)
        self.assert_set_exception(Parameter.PUMP_DELAY, 'bad')

        # Num Avg Samples: Range 1 - 32767
        self.assert_set(Parameter.NUM_AVG_SAMPLES, 1)
        self.assert_set(Parameter.NUM_AVG_SAMPLES, 32767)

        # Test bad values
        self.assert_set_exception(Parameter.NUM_AVG_SAMPLES, 0)
        self.assert_set_exception(Parameter.NUM_AVG_SAMPLES, 32768)
        self.assert_set_exception(Parameter.NUM_AVG_SAMPLES, 'bad')

        # Set params back to their default values
        self.assert_set(Parameter.PUMP_DELAY, 60)
        self.assert_set(Parameter.NUM_AVG_SAMPLES, 4)

        # Attempt to set Read only params
        self.assert_set_readonly(Parameter.DATE_TIME, '06032014113000')
        self.assert_set_readonly(Parameter.PTYPE, 1)
        self.assert_set_readonly(Parameter.VOLT0, False)
        self.assert_set_readonly(Parameter.VOLT1, False)
        self.assert_set_readonly(Parameter.VOLT2, True)
        self.assert_set_readonly(Parameter.VOLT3, True)
        self.assert_set_readonly(Parameter.VOLT4, True)
        self.assert_set_readonly(Parameter.VOLT5, True)
        self.assert_set_readonly(Parameter.SBE38, True)
        self.assert_set_readonly(Parameter.SBE63, True)
        self.assert_set_readonly(Parameter.WETLABS, True)
        self.assert_set_readonly(Parameter.GTD, True)
        self.assert_set_readonly(Parameter.DUAL_GTD, True)
        self.assert_set_readonly(Parameter.OPTODE, False)
        self.assert_set_readonly(Parameter.MIN_COND_FREQ, 400)
        self.assert_set_readonly(Parameter.OUTPUT_FORMAT, 1)
        self.assert_set_readonly(Parameter.LOGGING, True)
        self.assert_set_readonly(Parameter.AUTO_RUN, True)
        self.assert_set_readonly(Parameter.IGNORE_SWITCH, False)

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
        self.assert_driver_command(ProtocolEvent.ACQUIRE_SAMPLE)
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS)
        self.assert_driver_command(ProtocolEvent.GET_CONFIGURATION)

        # Invalid command/state transition: try to stop autosampling in command mode
        self.assert_driver_command_exception(ProtocolEvent.STOP_AUTOSAMPLE, exception_class=InstrumentCommandException)

        ####
        # Test in streaming mode
        ####
        # Put us in streaming
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)

        self.assert_driver_command(ProtocolEvent.SCHEDULED_CLOCK_SYNC)
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS)
        self.assert_driver_command(ProtocolEvent.GET_CONFIGURATION)

        # Invalid command/state transitions
        self.assert_driver_command_exception(ProtocolEvent.CLOCK_SYNC, exception_class=InstrumentCommandException)
        self.assert_driver_command_exception(ProtocolEvent.ACQUIRE_SAMPLE, exception_class=InstrumentCommandException)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

        ####
        # Test a bad command
        ####
        self.assert_driver_command_exception('ima_bad_command', exception_class=InstrumentCommandException)

    def test_parameters(self):
        """
        Test driver parameters and verify their type. Also verify the parameter values for startup
        params.  This test confirms that parameters are being read/converted properly and that
        the startup has been applied.
        """
        self.assert_initialize_driver()
        reply = self.driver_client.cmd_dvr('get_resource', Parameter.ALL)
        self.assert_driver_parameters(reply, True)

    def test_startup_params(self):
        """
        Verify that startup parameters are applied correctly. Generally this
        happens in the driver discovery method.
        """

        # Explicitly verify these values after discover.  They should match
        # what the startup values should be
        get_values = {
            Parameter.PTYPE: 3,
             Parameter.VOLT0: True,
             Parameter.VOLT1: True,
             Parameter.VOLT2: False,
             Parameter.VOLT3: False,
             Parameter.VOLT4: False,
             Parameter.VOLT5: False,
             Parameter.SBE38: False,
             Parameter.WETLABS: False,
             Parameter.GTD: False,
             Parameter.DUAL_GTD: False,
             Parameter.SBE63: False,
             Parameter.OPTODE: True,
             Parameter.OUTPUT_FORMAT: 0,
             Parameter.NUM_AVG_SAMPLES: 4,
             Parameter.MIN_COND_FREQ: 500,
             Parameter.PUMP_DELAY: 60,
             Parameter.AUTO_RUN: False,
             Parameter.IGNORE_SWITCH: True,
             Parameter.CLOCK_INTERVAL: '00:00:00',
             Parameter.STATUS_INTERVAL: '00:00:00'
        }

        # Change the values of these parameters to something before the
        # driver is reinitialized.  They should be blown away on reinit.
        new_values = {
            Parameter.PUMP_DELAY: 55,
            Parameter.NUM_AVG_SAMPLES: 2,
            Parameter.CLOCK_INTERVAL: '00:10:00',
            Parameter.STATUS_INTERVAL: '00:20:00'
        }

        self.assert_initialize_driver()
        self.assert_startup_parameters(self.assert_driver_parameters, new_values, get_values)

        self.assert_set_bulk(new_values)

        # Start autosample and try again
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=5)
        self.assert_startup_parameters(self.assert_driver_parameters)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

        #stop autosampling
        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=5)

    def test_status(self):
        self.assert_initialize_driver()

        # test acquire_status particles
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_STATUS, self.assert_particle_status)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_HARDWARE, self.assert_particle_hardware)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_CONFIGURATION, self.assert_particle_configuration)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_CALIBRATION, self.assert_particle_calibration)

        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.OPTODE_SETTINGS, self.assert_particle_send_optode)

    def test_configuration(self):
        self.assert_initialize_driver()

        # test get_configuration particle
        self.assert_particle_generation(ProtocolEvent.GET_CONFIGURATION, DataParticleType.DEVICE_CALIBRATION, self.assert_particle_calibration)

    def test_polled(self):
        """
        Test that we can generate particles with commands while in command mode
        """
        self.assert_initialize_driver()

        # test acquire_sample data particle
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_SAMPLE, DataParticleType.CTD_PARSED, self.assert_particle_sample)

    def test_autosample(self):
        """
        Verify that we can enter streaming and that all particles are produced
        properly.

        Because we have to test for many different data particles we can't use
        the common assert_sample_autosample method
        """
        self.assert_initialize_driver()

        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_async_particle_generation(DataParticleType.CTD_PARSED, self.assert_particle_sample, timeout=60)

        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_STATUS, self.assert_particle_status)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_HARDWARE, self.assert_particle_hardware)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_CONFIGURATION, self.assert_particle_configuration)
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS, DataParticleType.DEVICE_CALIBRATION, self.assert_particle_calibration)

        self.assert_particle_generation(ProtocolEvent.GET_CONFIGURATION, DataParticleType.DEVICE_CALIBRATION, self.assert_particle_calibration)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE, state=ProtocolState.COMMAND, delay=1)

    def test_scheduled_status_command(self):
        """
        Verify the device status command can be triggered and run in command
        """
        self.assert_initialize_driver()
        self.assert_set(Parameter.STATUS_INTERVAL, "00:00:20")

        # Verify that the event got scheduled
        self.assert_async_particle_generation(DataParticleType.DEVICE_STATUS, self.assert_particle_status, timeout=60)

        # Reset the interval
        self.assert_set(Parameter.STATUS_INTERVAL, "00:00:10")

        # Verify that the event got scheduled
        self.assert_async_particle_generation(DataParticleType.DEVICE_STATUS, self.assert_particle_status, timeout=30)

        # This should unschedule the acquire status event
        self.assert_set(Parameter.STATUS_INTERVAL, "00:00:00")

        # Now verify that no more status particles get generated, provide generous timeout
        failed = False

        try:
            self.assert_async_particle_generation(DataParticleType.DEVICE_STATUS, self.assert_particle_status, timeout=100)

            # We should never get here, failed should remain False
            failed = True
        except AssertionError:
            pass

        self.assertFalse(failed)

        self.assert_current_state(ProtocolState.COMMAND)

    def test_scheduled_status_autosample(self):
        """
        Verify the device status command can be triggered and run in autosample
        """
        self.assert_initialize_driver()
        self.assert_set(Parameter.STATUS_INTERVAL, "00:01:15")

        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

        #verify that the event got scheduled
        self.assert_async_particle_generation(DataParticleType.DEVICE_STATUS, self.assert_particle_status, timeout=90)

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)

    def test_scheduled_clock_sync_command(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        self.assert_initialize_driver()

        #Set the clock sync interval to 10 seconds
        self.assert_set(Parameter.CLOCK_INTERVAL, "00:00:10")
        # Verification: Search log for 'clock sync interval: 10'

        # Allow for a couple of clock syncs to happen
        time.sleep(25)
        # Verification: Search log for 'Performing Clock Sync', should be seen at 10 second intervals

        # Reset the interval
        self.assert_set(Parameter.CLOCK_INTERVAL, "00:00:20")
        # Verification: Search log for 'clock sync interval: 20'

        # Allow for a couple of clock syncs to happen
        time.sleep(50)
        # Verification: Search log for 'Performing Clock Sync', should be seen at 20 second intervals

        # Set the interval to 0 so that the event is unscheduled
        self.assert_set(Parameter.CLOCK_INTERVAL, "00:00:00")
        # Verification: Search log for 'Removed scheduler for clock sync'

        self.assert_current_state(ProtocolState.COMMAND)

    def test_scheduled_clock_sync_autosample(self):
        """
        Verify the scheduled clock sync is triggered and functions as expected
        """
        self.assert_initialize_driver()

        #Set the clock sync interval to 90 seconds
        self.assert_set(Parameter.CLOCK_INTERVAL, "00:01:30")
        # Verification: Search log for 'clock sync interval: 90'

        # Get into autosample mode
        self.assert_driver_command(ProtocolEvent.START_AUTOSAMPLE, state=ProtocolState.AUTOSAMPLE, delay=1)
        self.assert_current_state(ProtocolState.AUTOSAMPLE)

        # Allow for a clock sync to happen
        time.sleep(100)
        # Verification: Search log for 'Performing Clock Sync in autosample mode',
        # should be seen roughly 90 seconds after the interval was set

        self.assert_driver_command(ProtocolEvent.STOP_AUTOSAMPLE)


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class SBE16NOQualTestCase(SeaBirdQualificationTest, SBE16NOMixin):

    def setUp(self):
        SeaBirdQualificationTest.setUp(self)

    def test_direct_access_telnet_mode(self):
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
        self.assert_get_parameter(Parameter.OUTPUT_FORMAT, 0)

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet()
        self.tcp_client.send_data("%soutputformat=1%s" % (NEWLINE, NEWLINE))

        #need to sleep as the instrument needs time to apply the new param value
        time.sleep(5)

        # Verfy the param value got changed on the instrument
        self.tcp_client.send_data("%sGetCD%s" % (NEWLINE, NEWLINE))
        self.tcp_client.expect("<OutputFormat>converted HEX</OutputFormat>")
        self.assert_direct_access_stop_telnet()

        # verify the setting remained unchanged in the param dict
        self.assert_enter_command_mode()
        self.assert_get_parameter(Parameter.OUTPUT_FORMAT, 0)

    def test_direct_access_telnet_mode_autosample(self):
        """
        @brief Same as the previous DA test except in this test
               we force the instrument into streaming when in
               DA.  Then we need to verify the transition back
               to the driver works as expected.
        """
        self.assert_enter_command_mode()

        # go into direct access, and muck up a setting.
        self.assert_direct_access_start_telnet()
        self.assertTrue(self.tcp_client)

        #start logging
        self.tcp_client.send_data("%sstartnow%s" % (NEWLINE, NEWLINE))
        time.sleep(2)

        #verify we're logging
        self.tcp_client.send_data("%sGetSD%s" % (NEWLINE, NEWLINE))
        self.tcp_client.expect("<LoggingState>logging</LoggingState>")

        #Assert if stopping DA while autosampling, discover will put driver into Autosample state
        self.assert_direct_access_stop_telnet()
        self.assert_state_change(ResourceAgentState.STREAMING, ProtocolState.AUTOSAMPLE, timeout=10)

        #now stop autosampling
        self.assert_stop_autosample()

    def test_direct_access_telnet_timeout(self):
        """
        Verify that direct access times out as expected and the agent transitions back to command mode.
        """
        self.assert_enter_command_mode()

        # go into direct access
        self.assert_direct_access_start_telnet(timeout=30)
        self.assertTrue(self.tcp_client)

        self.assert_state_change(ResourceAgentState.IDLE, ProtocolState.COMMAND, 180)

    def test_direct_access_telnet_closed(self):
        """
        Verify that a disconnection from the DA server transitions the agent back to
        command mode.
        """
        self.assert_enter_command_mode()

        # go into direct access
        self.assert_direct_access_start_telnet(timeout=600)
        self.assertTrue(self.tcp_client)
        self.tcp_client.disconnect()

        self.assert_state_change(ResourceAgentState.IDLE, ProtocolState.COMMAND, 120)

    def test_poll(self):
        '''
        Verify that we can poll for a sample.  Take sample for this instrument
        Also poll for other engineering data streams.
        '''
        self.assert_enter_command_mode()

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_SAMPLE, self.assert_particle_sample, DataParticleType.CTD_PARSED, sample_count=1, timeout=30)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status, DataParticleType.DEVICE_STATUS, sample_count=1, timeout=90)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_hardware, DataParticleType.DEVICE_HARDWARE, sample_count=1, timeout=90)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_configuration, DataParticleType.DEVICE_CONFIGURATION, sample_count=1, timeout=90)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_calibration, DataParticleType.DEVICE_CALIBRATION, sample_count=1, timeout=90)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_send_optode, DataParticleType.OPTODE_SETTINGS, sample_count=1, timeout=90)

        self.assert_particle_polled(ProtocolEvent.GET_CONFIGURATION, self.assert_particle_calibration, DataParticleType.DEVICE_CALIBRATION, sample_count=1, timeout=30)

    def test_autosample(self):
        """
        Verify autosample works and data particles are created
        """
        self.assert_enter_command_mode()

        self.assert_start_autosample()
        self.assert_particle_async(DataParticleType.CTD_PARSED, self.assert_particle_sample)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status, DataParticleType.DEVICE_STATUS, sample_count=1, timeout=30)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_hardware, DataParticleType.DEVICE_HARDWARE, sample_count=1, timeout=30)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_configuration, DataParticleType.DEVICE_CONFIGURATION, sample_count=1, timeout=30)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_calibration, DataParticleType.DEVICE_CALIBRATION, sample_count=1, timeout=30)
        self.assert_particle_polled(ProtocolEvent.GET_CONFIGURATION, self.assert_particle_calibration, DataParticleType.DEVICE_CALIBRATION, sample_count=1, timeout=30)

        # Stop autosample and do run a couple commands.
        self.assert_stop_autosample()

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_status, DataParticleType.DEVICE_STATUS, sample_count=1, timeout=90)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_hardware, DataParticleType.DEVICE_HARDWARE, sample_count=1, timeout=90)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_configuration, DataParticleType.DEVICE_CONFIGURATION, sample_count=1, timeout=90)
        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_calibration, DataParticleType.DEVICE_CALIBRATION, sample_count=1, timeout=90)

        self.assert_particle_polled(ProtocolEvent.ACQUIRE_STATUS, self.assert_particle_send_optode, DataParticleType.OPTODE_SETTINGS, sample_count=1, timeout=90)

        self.assert_particle_polled(ProtocolEvent.GET_CONFIGURATION, self.assert_particle_calibration, DataParticleType.DEVICE_CALIBRATION, sample_count=1, timeout=30)

        # Restart autosample and gather a couple samples
        self.assert_sample_autosample(self.assert_particle_sample, DataParticleType.CTD_PARSED)

    def test_execute_clock_sync(self):
        """
        Verify we can synchronize the instrument internal clock
        """
        self.assert_enter_command_mode()

        # Perform a clock sync!
        self.assert_execute_resource(ProtocolEvent.CLOCK_SYNC)

        # Call discover so that the driver gets the updated DateTime value from the instrument
        self.assert_reset()
        self.assert_discover(ResourceAgentState.COMMAND)

        # get the time from the driver
        check_new_params = self.instrument_agent_client.get_resource([Parameter.DATE_TIME])

        # convert driver's time from formatted date/time string to seconds integer
        instrument_time = time.mktime(time.strptime(check_new_params.get(Parameter.DATE_TIME).lower(), "%d %b %Y %H:%M:%S"))

        # need to convert local machine's time to date/time string and back to seconds to 'drop' the DST attribute so test passes
        # get time from local machine
        lt = time.strftime("%d %b %Y %H:%M:%S", time.gmtime(time.mktime(time.localtime())))
        # convert local time from formatted date/time string to seconds integer to drop DST
        local_time = time.mktime(time.strptime(lt, "%d %b %Y %H:%M:%S"))

        # Now verify that the time matches to within 10 seconds
        # The instrument time will be slightly behind as assert_discover takes a few seconds to complete
        self.assertLessEqual(abs(instrument_time - local_time), 10)

    def test_get_set_parameters(self):
        '''
        verify that all parameters can be get set properly
        '''
        self.assert_enter_command_mode()

        #attempt to change some parameters
        self.assert_set_parameter(Parameter.NUM_AVG_SAMPLES, 2)
        self.assert_set_parameter(Parameter.PUMP_DELAY, 55)

        #get parameters and verify values
        self.assert_get_parameter(Parameter.NUM_AVG_SAMPLES, 2)
        self.assert_get_parameter(Parameter.PUMP_DELAY, 55)

        #set parameters back to their default values
        self.assert_set_parameter(Parameter.NUM_AVG_SAMPLES, 4)
        self.assert_set_parameter(Parameter.PUMP_DELAY, 60)

        #get parameters and verify values
        self.assert_get_parameter(Parameter.NUM_AVG_SAMPLES, 4)
        self.assert_get_parameter(Parameter.PUMP_DELAY, 60)

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
                ProtocolEvent.ACQUIRE_SAMPLE,
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
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] =  [
            ProtocolEvent.STOP_AUTOSAMPLE,
            ProtocolEvent.ACQUIRE_STATUS,
            ]

        self.assert_start_autosample()
        self.assert_capabilities(capabilities)
        self.assert_stop_autosample()

        ##################
        #  DA Mode
        ##################

        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.DIRECT_ACCESS)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = []

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


###############################################################################
#                             PUBLICATION TESTS                               #
# Device specific publication tests are for                                   #
# testing device specific capabilities                                        #
###############################################################################
@attr('PUB', group='mi')
class PubFromIDK():
    pass
