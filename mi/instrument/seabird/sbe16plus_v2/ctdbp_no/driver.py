"""
@package mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver
@file mi/instrument/seabird/sbe16plus_v2/ctdbp_no/driver.py
@author Tapana Gupta
@brief Driver class for sbe16plus V2 CTD instrument.
"""

import re
import time

from mi.core.log import get_logger
from mi.core.common import BaseEnum

from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType

from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import SampleException

from xml.dom.minidom import parseString

from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import OptodeCommands
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Parameter
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import Command
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SendOptodeCommand
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19Protocol
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19DataParticle
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19StatusParticle
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import SBE19ConfigurationParticle
from mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver import OptodeSettingsParticle

from mi.instrument.seabird.sbe16plus_v2.driver import \
    Prompt, SBE16InstrumentDriver, Sbe16plusBaseParticle, WAKEUP_TIMEOUT, NEWLINE, TIMEOUT, ProtocolState
from mi.core.instrument.protocol_param_dict import ParameterDictType, ParameterDictVisibility


__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'

log = get_logger()


class DataParticleType(BaseEnum):
    RAW = CommonDataParticleType.RAW
    CTD_PARSED = 'ctdbp_no_sample'
    DEVICE_STATUS = 'ctdbp_no_status'
    DEVICE_CALIBRATION = 'ctdbp_no_calibration_coefficients'
    DEVICE_HARDWARE = 'ctdbp_no_hardware'
    DEVICE_CONFIGURATION = 'ctdbp_no_configuration'
    OPTODE_SETTINGS = 'ctdbp_no_optode_settings'


class SBE16NODataParticle(SBE19DataParticle):
    """
    This data particle is identical to the corresponding one for CTDPF-Optode, except for the stream
    name, which we specify here
    """
    _data_particle_type = DataParticleType.CTD_PARSED


class SBE16NOConfigurationParticle(SBE19ConfigurationParticle):
    """
    This data particle is identical to the corresponding one for CTDPF-Optode, except for the stream
    name, which we specify here
    """
    _data_particle_type = DataParticleType.DEVICE_CONFIGURATION


class SBE16NOStatusParticle(SBE19StatusParticle):
    """
    This data particle is identical to the corresponding one for CTDPF-Optode, except for the stream
    name, which we specify here
    """
    _data_particle_type = DataParticleType.DEVICE_STATUS


class SBE16NOOptodeSettingsParticle(OptodeSettingsParticle):
    """
    This data particle is identical to the corresponding one for CTDPF-Optode, except for the stream
    name, which we specify here
    """
    _data_particle_type = DataParticleType.OPTODE_SETTINGS


class SBE16NOHardwareParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"
    FIRMWARE_VERSION = "firmware_version"
    FIRMWARE_DATE = "firmware_date"
    COMMAND_SET_VERSION = "command_set_version"
    PCB_SERIAL_NUMBER = "pcb_serial_number"
    ASSEMBLY_NUMBER = "assembly_number"
    MANUFACTURE_DATE = "manufacture_date"
    TEMPERATURE_SENSOR_SERIAL_NUMBER = 'temp_sensor_serial_number'
    CONDUCTIVITY_SENSOR_SERIAL_NUMBER = 'cond_sensor_serial_number'
    PRESSURE_SENSOR_TYPE = 'pressure_sensor_type'
    PRESSURE_SENSOR_SERIAL_NUMBER = 'quartz_pressure_sensor_serial_number'
    VOLT0_TYPE = 'volt0_type'
    VOLT0_SERIAL_NUMBER = 'volt0_serial_number'
    VOLT1_TYPE = 'volt1_type'
    VOLT1_SERIAL_NUMBER = 'volt1_serial_number'


class SBE16NOHardwareParticle(Sbe16plusBaseParticle):
    _data_particle_type = DataParticleType.DEVICE_HARDWARE

    @staticmethod
    def regex():
        """
        Regular expression to match a getHD response pattern
        @return: regex string
        """
        pattern = r'(<HardwareData.*?</HardwareData>)' + NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(SBE16NOHardwareParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        """
        Regular expression to match a getHD response pattern
        @return: regex string
        """
        pattern = r'(<HardwareData.*?</HardwareData>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        """
        get the compiled regex pattern
        @return: compiled re
        """
        return re.compile(SBE16NOHardwareParticle.resp_regex(), re.DOTALL)

    # noinspection PyPep8Naming
    def _build_parsed_values(self):
        """
        @throws SampleException If there is a problem with sample creation
        """

        SENSOR = "Sensor"
        TYPE = "type"
        ID = "id"
        PCB_SERIAL_NUMBER = "PCBSerialNum"
        ASSEMBLY_NUMBER = "AssemblyNum"
        SERIAL_NUMBER = "SerialNumber"
        FIRMWARE_VERSION = "FirmwareVersion"
        FIRMWARE_DATE = "FirmwareDate"
        COMMAND_SET_VERSION = "CommandSetVersion"
        PCB_ASSEMBLY = "PCBAssembly"
        MANUFACTURE_DATE = "MfgDate"
        INTERNAL_SENSORS = "InternalSensors"
        TEMPERATURE_SENSOR_ID = "Main Temperature"
        CONDUCTIVITY_SENSOR_ID = "Main Conductivity"
        PRESSURE_SENSOR_ID = "Main Pressure"
        EXTERNAL_SENSORS = "ExternalSensors"
        VOLT0 = "volt 0"
        VOLT1 = "volt 1"

        # check to make sure there is a correct match before continuing
        match = SBE16NOHardwareParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise SampleException("No regex match of parsed hardware data: [%s]" %
                                  self.raw_data)

        dom = parseString(self.raw_data)
        root = dom.documentElement
        log.debug("root.tagName = %s", root.tagName)
        serial_number = root.getAttribute(SERIAL_NUMBER)

        firmware_version = self._extract_xml_element_value(root, FIRMWARE_VERSION)
        firmware_date = self._extract_xml_element_value(root, FIRMWARE_DATE)
        command_set_version = self._extract_xml_element_value(root, COMMAND_SET_VERSION)
        manufacture_date = self._extract_xml_element_value(root, MANUFACTURE_DATE)

        pcb_assembly_elements = self._extract_xml_elements(root, PCB_ASSEMBLY)
        pcb_serial_number = []
        pcb_assembly = []
        for assembly in pcb_assembly_elements:
            pcb_serial_number.append(assembly.getAttribute(PCB_SERIAL_NUMBER))
            pcb_assembly.append(assembly.getAttribute(ASSEMBLY_NUMBER))

        temperature_sensor_serial_number = ""
        conductivity_sensor_serial_number = ""
        pressure_sensor_serial_number = ""
        pressure_sensor_type = ""
        volt0_serial_number = 0
        volt0_type = ""
        volt1_serial_number = 0
        volt1_type = ""

        internal_sensors_element = self._extract_xml_elements(root, INTERNAL_SENSORS)[0]
        sensors = self._extract_xml_elements(internal_sensors_element, SENSOR)

        for sensor in sensors:
            sensor_id = sensor.getAttribute(ID)
            if sensor_id == TEMPERATURE_SENSOR_ID:
                temperature_sensor_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
            elif sensor_id == CONDUCTIVITY_SENSOR_ID:
                conductivity_sensor_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
            elif sensor_id == PRESSURE_SENSOR_ID:
                pressure_sensor_serial_number = str(self._extract_xml_element_value(sensor, SERIAL_NUMBER))
                pressure_sensor_type = self._extract_xml_element_value(sensor, TYPE)

        external_sensors_element = self._extract_xml_elements(root, EXTERNAL_SENSORS)[0]
        sensors = self._extract_xml_elements(external_sensors_element, SENSOR)

        for sensor in sensors:
            sensor_id = sensor.getAttribute(ID)
            if sensor_id == VOLT0:
                volt0_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
                volt0_type = self._extract_xml_element_value(sensor, TYPE)
            elif sensor_id == VOLT1:
                volt1_serial_number = self._extract_xml_element_value(sensor, SERIAL_NUMBER)
                volt1_type = self._extract_xml_element_value(sensor, TYPE)

        result = [{DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: str(serial_number)},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.FIRMWARE_VERSION,
                   DataParticleKey.VALUE: firmware_version},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.FIRMWARE_DATE,
                   DataParticleKey.VALUE: firmware_date},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.COMMAND_SET_VERSION,
                   DataParticleKey.VALUE: command_set_version},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.MANUFACTURE_DATE,
                   DataParticleKey.VALUE: manufacture_date},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.PCB_SERIAL_NUMBER,
                   DataParticleKey.VALUE: pcb_serial_number},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.ASSEMBLY_NUMBER,
                   DataParticleKey.VALUE: pcb_assembly},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.TEMPERATURE_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: temperature_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.CONDUCTIVITY_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: conductivity_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.PRESSURE_SENSOR_SERIAL_NUMBER,
                   DataParticleKey.VALUE: pressure_sensor_serial_number},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.PRESSURE_SENSOR_TYPE,
                   DataParticleKey.VALUE: pressure_sensor_type},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.VOLT0_SERIAL_NUMBER,
                   DataParticleKey.VALUE: volt0_serial_number},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.VOLT0_TYPE,
                   DataParticleKey.VALUE: volt0_type},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.VOLT1_SERIAL_NUMBER,
                   DataParticleKey.VALUE: volt1_serial_number},
                  {DataParticleKey.VALUE_ID: SBE16NOHardwareParticleKey.VOLT1_TYPE,
                   DataParticleKey.VALUE: volt1_type}]

        return result


class SBE16NOCalibrationParticleKey(BaseEnum):
    SERIAL_NUMBER = "serial_number"

    TEMP_SENSOR_SERIAL_NUMBER = "temp_sensor_serial_number"
    TEMP_CAL_DATE = "calibration_date_temperature"
    TA0 = "temp_coeff_ta0"
    TA1 = "temp_coeff_ta1"
    TA2 = "temp_coeff_ta2"
    TA3 = "temp_coeff_ta3"
    TOFFSET = "temp_coeff_offset"

    COND_SENSOR_SERIAL_NUMBER = "cond_sensor_serial_number"
    COND_CAL_DATE = "calibration_date_conductivity"
    CONDG = "cond_coeff_cg"
    CONDH = "cond_coeff_ch"
    CONDI = "cond_coeff_ci"
    CONDJ = "cond_coeff_cj"
    CPCOR = "cond_coeff_cpcor"
    CTCOR = "cond_coeff_ctcor"
    CSLOPE = "cond_coeff_cslope"

    PRES_SERIAL_NUMBER = "pressure_sensor_serial_number"
    PRES_CAL_DATE = "calibration_date_pressure"
    PC1 = "press_coeff_pc1"
    PC2 = "press_coeff_pc2"
    PC3 = "press_coeff_pc3"
    PD1 = "press_coeff_pd1"
    PD2 = "press_coeff_pd2"
    PT1 = "press_coeff_pt1"
    PT2 = "press_coeff_pt2"
    PT3 = "press_coeff_pt3"
    PT4 = "press_coeff_pt4"
    PSLOPE = "press_coeff_pslope"
    POFFSET = "press_coeff_poffset"
    PRES_RANGE = "pressure_sensor_range"

    EXT_VOLT0_OFFSET = "ext_volt0_offset"
    EXT_VOLT0_SLOPE = "ext_volt0_slope"
    EXT_VOLT1_OFFSET = "ext_volt1_offset"
    EXT_VOLT1_SLOPE = "ext_volt1_slope"
    EXT_VOLT2_OFFSET = "ext_volt2_offset"
    EXT_VOLT2_SLOPE = "ext_volt2_slope"
    EXT_VOLT3_OFFSET = "ext_volt3_offset"
    EXT_VOLT3_SLOPE = "ext_volt3_slope"
    EXT_VOLT4_OFFSET = "ext_volt4_offset"
    EXT_VOLT4_SLOPE = "ext_volt4_slope"
    EXT_VOLT5_OFFSET = "ext_volt5_offset"
    EXT_VOLT5_SLOPE = "ext_volt5_slope"

    EXT_FREQ = "ext_freq_sf"


class SBE16NOCalibrationParticle(Sbe16plusBaseParticle):
    """
    Routines for parsing raw data into a data particle structure. Override
    the building of values, and the rest should come along for free.
    """
    _data_particle_type = DataParticleType.DEVICE_CALIBRATION

    @staticmethod
    def regex():
        pattern = r'(<CalibrationCoefficients.*?</CalibrationCoefficients>)' + NEWLINE
        return pattern

    @staticmethod
    def regex_compiled():
        return re.compile(SBE16NOCalibrationParticle.regex(), re.DOTALL)

    @staticmethod
    def resp_regex():
        pattern = r'(<CalibrationCoefficients.*?</CalibrationCoefficients>)'
        return pattern

    @staticmethod
    def resp_regex_compiled():
        return re.compile(SBE16NOCalibrationParticle.resp_regex(), re.DOTALL)

    def _map_param_to_xml_tag(self, parameter_name):
        map_param_to_tag = {SBE16NOCalibrationParticleKey.TEMP_SENSOR_SERIAL_NUMBER: "SerialNum",
                            SBE16NOCalibrationParticleKey.TEMP_CAL_DATE: "CalDate",
                            SBE16NOCalibrationParticleKey.TA0: "TA0",
                            SBE16NOCalibrationParticleKey.TA1: "TA1",
                            SBE16NOCalibrationParticleKey.TA2: "TA2",
                            SBE16NOCalibrationParticleKey.TA3: "TA3",
                            SBE16NOCalibrationParticleKey.TOFFSET: "TOFFSET",

                            SBE16NOCalibrationParticleKey.COND_SENSOR_SERIAL_NUMBER: "SerialNum",
                            SBE16NOCalibrationParticleKey.COND_CAL_DATE: "CalDate",
                            SBE16NOCalibrationParticleKey.CONDG: "G",
                            SBE16NOCalibrationParticleKey.CONDH: "H",
                            SBE16NOCalibrationParticleKey.CONDI: "I",
                            SBE16NOCalibrationParticleKey.CONDJ: "J",
                            SBE16NOCalibrationParticleKey.CPCOR: "CPCOR",
                            SBE16NOCalibrationParticleKey.CTCOR: "CTCOR",
                            SBE16NOCalibrationParticleKey.CSLOPE: "CSLOPE",

                            SBE16NOCalibrationParticleKey.PRES_SERIAL_NUMBER: "SerialNum",
                            SBE16NOCalibrationParticleKey.PRES_CAL_DATE: "CalDate",
                            SBE16NOCalibrationParticleKey.PC1: "PC1",
                            SBE16NOCalibrationParticleKey.PC2: "PC2",
                            SBE16NOCalibrationParticleKey.PC3: "PC3",
                            SBE16NOCalibrationParticleKey.PD1: "PD1",
                            SBE16NOCalibrationParticleKey.PD2: "PD2",
                            SBE16NOCalibrationParticleKey.PT1: "PT1",
                            SBE16NOCalibrationParticleKey.PT2: "PT2",
                            SBE16NOCalibrationParticleKey.PT3: "PT3",
                            SBE16NOCalibrationParticleKey.PT4: "PT4",
                            SBE16NOCalibrationParticleKey.PSLOPE: "PSLOPE",
                            SBE16NOCalibrationParticleKey.POFFSET: "POFFSET",
                            SBE16NOCalibrationParticleKey.PRES_RANGE: "PRANGE",

                            SBE16NOCalibrationParticleKey.EXT_VOLT0_OFFSET: "OFFSET",
                            SBE16NOCalibrationParticleKey.EXT_VOLT0_SLOPE: "SLOPE",
                            SBE16NOCalibrationParticleKey.EXT_VOLT1_OFFSET: "OFFSET",
                            SBE16NOCalibrationParticleKey.EXT_VOLT1_SLOPE: "SLOPE",
                            SBE16NOCalibrationParticleKey.EXT_VOLT2_OFFSET: "OFFSET",
                            SBE16NOCalibrationParticleKey.EXT_VOLT2_SLOPE: "SLOPE",
                            SBE16NOCalibrationParticleKey.EXT_VOLT3_OFFSET: "OFFSET",
                            SBE16NOCalibrationParticleKey.EXT_VOLT3_SLOPE: "SLOPE",
                            SBE16NOCalibrationParticleKey.EXT_VOLT4_OFFSET: "OFFSET",
                            SBE16NOCalibrationParticleKey.EXT_VOLT4_SLOPE: "SLOPE",
                            SBE16NOCalibrationParticleKey.EXT_VOLT5_OFFSET: "OFFSET",
                            SBE16NOCalibrationParticleKey.EXT_VOLT5_SLOPE: "SLOPE",

                            SBE16NOCalibrationParticleKey.EXT_FREQ: "EXTFREQSF"}

        return map_param_to_tag[parameter_name]

    # noinspection PyPep8Naming
    def _build_parsed_values(self):
        """
        Parse the output of the getCC command
        @throws SampleException If there is a problem with sample creation
        """

        SERIAL_NUMBER = "SerialNumber"
        CALIBRATION = "Calibration"
        ID = "id"
        TEMPERATURE_SENSOR_ID = "Main Temperature"
        CONDUCTIVITY_SENSOR_ID = "Main Conductivity"
        PRESSURE_SENSOR_ID = "Main Pressure"
        VOLT0 = "Volt 0"
        VOLT1 = "Volt 1"
        VOLT2 = "Volt 2"
        VOLT3 = "Volt 3"
        VOLT4 = "Volt 4"
        VOLT5 = "Volt 5"
        EXTERNAL_FREQUENCY_CHANNEL = "external frequency channel"

        # check to make sure there is a correct match before continuing
        match = SBE16NOCalibrationParticle.regex_compiled().match(self.raw_data)
        if not match:
            raise SampleException("No regex match of parsed calibration data: [%s]" %
                                  self.raw_data)

        dom = parseString(self.raw_data)
        root = dom.documentElement
        log.debug("root.tagName = %s", root.tagName)
        serial_number = root.getAttribute(SERIAL_NUMBER)
        result = [{DataParticleKey.VALUE_ID: SBE16NOCalibrationParticleKey.SERIAL_NUMBER,
                   DataParticleKey.VALUE: serial_number}]

        calibration_elements = self._extract_xml_elements(root, CALIBRATION)
        for calibration in calibration_elements:
            id_attr = calibration.getAttribute(ID)
            if id_attr == TEMPERATURE_SENSOR_ID:
                result.append(
                    self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.TEMP_SENSOR_SERIAL_NUMBER, str))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.TEMP_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.TA0))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.TA1))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.TA2))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.TA3))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.TOFFSET))
            elif id_attr == CONDUCTIVITY_SENSOR_ID:
                result.append(
                    self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.COND_SENSOR_SERIAL_NUMBER, str))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.COND_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.CONDG))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.CONDH))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.CONDI))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.CONDJ))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.CPCOR))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.CTCOR))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.CSLOPE))
            elif id_attr == PRESSURE_SENSOR_ID:
                result.append(
                    self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PRES_SERIAL_NUMBER, str))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PRES_CAL_DATE, str))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PC1))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PC2))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PC3))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PD1))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PD2))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PT1))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PT2))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PT3))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PT4))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PSLOPE))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.POFFSET))
                result.append(
                    self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.PRES_RANGE, self.float_to_int))
            elif id_attr == VOLT0:
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT0_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT0_SLOPE))
            elif id_attr == VOLT1:
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT1_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT1_SLOPE))
            elif id_attr == VOLT2:
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT2_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT2_SLOPE))
            elif id_attr == VOLT3:
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT3_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT3_SLOPE))
            elif id_attr == VOLT4:
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT4_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT4_SLOPE))
            elif id_attr == VOLT5:
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT5_OFFSET))
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_VOLT5_SLOPE))
            elif id_attr == EXTERNAL_FREQUENCY_CHANNEL:
                result.append(self._get_xml_parameter(calibration, SBE16NOCalibrationParticleKey.EXT_FREQ))

        return result


###############################################################################
# Seabird Electronics 16plus V2 NO Driver.
###############################################################################
class InstrumentDriver(SBE16InstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################
    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return Parameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################
    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = SBE16NOProtocol(Prompt, NEWLINE, self._driver_event)


###############################################################################
# Seabird Electronics 16plus V2 NO protocol.
###############################################################################
class SBE16NOProtocol(SBE19Protocol):
    """
    Instrument protocol class for SBE16 NO driver.
    Subclasses SBE16Protocol
    """
    def __init__(self, prompts, newline, driver_event):
        """
        SBE16Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The SBE16 newline.
        @param driver_event Driver process event callback.
        """
        SBE19Protocol.__init__(self, prompts, newline, driver_event)

    @staticmethod
    def sieve_function(raw_data):
        """ The method that splits samples
        Over-ride sieve function to handle additional particles.
        """
        matchers = []
        return_list = []

        matchers.append(SBE16NODataParticle.regex_compiled())
        matchers.append(SBE16NOHardwareParticle.regex_compiled())
        matchers.append(SBE16NOCalibrationParticle.regex_compiled())
        matchers.append(SBE16NOStatusParticle.regex_compiled())
        matchers.append(SBE16NOConfigurationParticle.regex_compiled())
        matchers.append(SBE16NOOptodeSettingsParticle.regex_compiled())
        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, timestamp):
        """
        Over-ride sieve function to handle additional particles.
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        if self._extract_sample(SBE16NODataParticle, SBE16NODataParticle.regex_compiled(), chunk, timestamp):
            self._sampling = True
            return

        for particle_class in SBE16NOHardwareParticle, \
                              SBE16NOCalibrationParticle, \
                              SBE16NOConfigurationParticle, \
                              SBE16NOStatusParticle, \
                              SBE16NOOptodeSettingsParticle:
            if self._extract_sample(particle_class, particle_class.regex_compiled(), chunk, timestamp):
                return

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_acquire_status_async(self, *args, **kwargs):
        """
        Get device status
        """
        next_state = ProtocolState.COMMAND
        result = []

        result.append(self._do_cmd_resp(Command.GET_SD, response_regex=SBE16NOStatusParticle.regex_compiled(),
                                   timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, response_regex=SBE16NOHardwareParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, response_regex=SBE16NOConfigurationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, response_regex=SBE16NOCalibrationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_command_acquire_status: GetEC Response: %s", result)

        # Reset the event counter right after getEC
        self._do_cmd_resp(Command.RESET_EC, timeout=TIMEOUT)

        # Now send commands to the Optode to get its status
        # Stop the optode first, need to send the command twice
        stop_command = "stop"
        start_command = "start"
        self._do_cmd_resp(OptodeCommands.SEND_OPTODE, stop_command, timeout=TIMEOUT)
        time.sleep(2)
        self._do_cmd_resp(OptodeCommands.SEND_OPTODE, stop_command, timeout=TIMEOUT)
        time.sleep(3)

        # Send all the 'sendoptode=' commands one by one
        optode_commands = SendOptodeCommand.list()
        for command in optode_commands:
            log.debug("Sending optode command: %s" % command)
            result.append(self._do_cmd_resp(OptodeCommands.SEND_OPTODE, command, timeout=TIMEOUT))
            log.debug("_handler_command_acquire_status: SendOptode Response: %s", result)

        # restart the optode
        self._do_cmd_resp(OptodeCommands.SEND_OPTODE, start_command, timeout=TIMEOUT)

        return next_state, (next_state, ''.join(result))

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        Acquire sample from SBE16.
        @retval next_state, (next_state, result) tuple
        """
        next_state = None
        timeout = time.time() + TIMEOUT

        self._do_cmd_resp(Command.TS, *args, **kwargs)

        particles = self.wait_for_particles(DataParticleType.CTD_PARSED, timeout)

        return next_state, (next_state, particles)

    def _handler_autosample_acquire_status(self, *args, **kwargs):
        """
        Get device status
        """
        next_state = None
        result = []

        # When in autosample this command requires two wakeups to get to the right prompt
        self._wakeup(timeout=WAKEUP_TIMEOUT)
        self._wakeup(timeout=WAKEUP_TIMEOUT)

        result.append(self._do_cmd_resp(Command.GET_SD, response_regex=SBE16NOStatusParticle.regex_compiled(),
                                   timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetSD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_HD, response_regex=SBE16NOHardwareParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetHD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CD, response_regex=SBE16NOConfigurationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetCD Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_CC, response_regex=SBE16NOCalibrationParticle.regex_compiled(),
                                    timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetCC Response: %s", result)
        result.append(self._do_cmd_resp(Command.GET_EC, timeout=TIMEOUT))
        log.debug("_handler_autosample_acquire_status: GetEC Response: %s", result)

        # Reset the event counter right after getEC
        self._do_cmd_no_resp(Command.RESET_EC)

        return next_state, (next_state, ''.join(result))

    ########################################################################
    # response handlers.
    ########################################################################
    def _validate_GetCD_response(self, response, prompt):
        """
        validation handler for GetCD command
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("GetCD command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('GetCD command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if not SBE16NOConfigurationParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetCD_response: GetCD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetCD command not recognized: %s.' % response)

        self._param_dict.update_many(response)

        return response

    def _validate_GetCC_response(self, response, prompt):
        """
        validation handler for GetCC command
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("GetCC command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('GetCC command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if not SBE16NOCalibrationParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetCC_response: GetCC command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetCC command not recognized: %s.' % response)

        return response

    def _validate_GetHD_response(self, response, prompt):
        """
        validation handler for GetHD command
        @param response command response string.
        @param prompt prompt following command response.
        @throws InstrumentProtocolException if command misunderstood.
        """
        error = self._find_error(response)

        if error:
            log.error("GetHD command encountered error; type='%s' msg='%s'", error[0], error[1])
            raise InstrumentProtocolException('GetHD command failure: type="%s" msg="%s"' % (error[0], error[1]))

        if not SBE16NOHardwareParticle.resp_regex_compiled().search(response):
            log.error('_validate_GetHD_response: GetHD command not recognized: %s.' % response)
            raise InstrumentProtocolException('GetHD command not recognized: %s.' % response)

        self._param_dict.update_many(response)

        return response

    def _build_ctd_specific_params(self):
        self._param_dict.add(Parameter.PTYPE,
                             r"<Sensor id = 'Main Pressure'>.*?<type>(.*?)</type>.*?</Sensor>",
                             self._pressure_sensor_to_int,
                             str,
                             type=ParameterDictType.INT,
                             display_name="Pressure Sensor Type",
                             range={'Strain Gauge': 1, 'Quartz with Temp Comp': 3},
                             startup_param=True,
                             direct_access=True,
                             default_value=3,
                             description="Sensor type: (1:strain gauge | 3:quartz with temp comp)",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             regex_flags=re.DOTALL)


def create_playback_protocol(callback):
    return SBE16NOProtocol(None, None, callback)
