#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/phsen_abcdef_dclpy
@author Nick Almonte
@brief Parser for the phsen_abcdef_dcl dataset driver
Release notes:

initial release
"""

import re

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import ONE_OR_MORE_WHITESPACE_REGEX
from mi.dataset.parser.utilities import convert_to_signed_int_16_bit, dcl_controller_timestamp_to_ntp_time

__author__ = 'Nick Almonte'
__license__ = 'Apache 2.0'

METADATA_PARTICLE_CLASS_KEY = 'metadata_particle_class'
# The key for the data particle class
DATA_PARTICLE_CLASS_KEY = 'data_particle_class'

log = get_logger()


def _calculate_working_record_checksum(working_record):
    """
    Calculates the checksum of the argument ascii-hex string
    @retval int - modulo integer checksum value of argument ascii-hex string
    """
    checksum = 0

    # strip off the leading * and ID characters of the log line (3 characters) and
    # strip off the trailing Checksum characters (2 characters)
    star_and_checksum_stripped_working_record = working_record[3:-2]

    working_record_length = len(star_and_checksum_stripped_working_record)

    for x in range(0, working_record_length, 2):
        value = star_and_checksum_stripped_working_record[x:x+2]
        checksum += int(value, 16)

    modulo_checksum = checksum % 256

    return modulo_checksum


class DataParticleType(BaseEnum):
    """
    The data particle types that a phsen_abcdef_dcl parser may generate
    """
    METADATA_RECOVERED = 'phsen_abcdef_dcl_metadata_recovered'
    INSTRUMENT_RECOVERED = 'phsen_abcdef_dcl_instrument_recovered'
    METADATA_TELEMETERED = 'phsen_abcdef_dcl_metadata'
    INSTRUMENT_TELEMETERED = 'phsen_abcdef_dcl_instrument'


class StateKey(BaseEnum):
    POSITION = 'position'  # hold the current file position
    START_OF_DATA = 'start_of_data'


class PhsenAbcdefDclMetadataDataParticle(DataParticle):

    def _build_parsed_values(self):
        """
        Extracts PHSEN ABCDEF DCL Metadata data from raw_data.

        @returns result a list of dictionaries of particle data
        """
        # extract the time from the raw_data tuple
        dcl_controller_timestamp = self.raw_data[0]

        # convert the time
        converted_time = dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp)
        # set the converted time to the particle internal timestamp
        self.set_internal_timestamp(converted_time)

        # extract the working_record string from the raw data tuple
        working_record = self.raw_data[1]

        # Per the IDD, voltage_battery data is optional and not guaranteed to be included in every CONTROL
        # data record. Nominal size of a metadata string without the voltage_battery data is 39 (including the #).
        # Voltage data adds 4 ascii characters to that, so raw_data greater than 41 contains voltage data,
        # anything smaller does not.
        if len(working_record) >= 41:
            have_voltage_battery_data = True
        else:
            have_voltage_battery_data = False

        # Begin saving particle data
        unique_id_ascii_hex = working_record[1:3]
        # convert 2 ascii (hex) chars to int
        unique_id_int = int(unique_id_ascii_hex, 16)

        record_type_ascii_hex = working_record[5:7]
        # convert 2 ascii (hex) chars to int
        record_type_int = int(record_type_ascii_hex, 16)

        record_time_ascii_hex = working_record[7:15]
        # convert 8 ascii (hex) chars to int
        record_time_int = int(record_time_ascii_hex, 16)

        # FLAGS
        flags_ascii_hex = working_record[15:19]
        # convert 4 ascii (hex) chars to list of binary data
        flags_ascii_int = int(flags_ascii_hex, 16)
        binary_list = [(flags_ascii_int >> x) & 0x1 for x in range(16)]

        clock_active = binary_list[0]
        recording_active = binary_list[1]
        record_end_on_time = binary_list[2]
        record_memory_full = binary_list[3]
        record_end_on_error = binary_list[4]
        data_download_ok = binary_list[5]
        flash_memory_open = binary_list[6]
        battery_low_prestart = binary_list[7]
        battery_low_measurement = binary_list[8]
        battery_low_blank = binary_list[9]
        battery_low_external = binary_list[10]
        external_device1_fault = binary_list[11]
        external_device2_fault = binary_list[12]
        external_device3_fault = binary_list[13]
        flash_erased = binary_list[14]
        power_on_invalid = binary_list[15]

        num_data_records_ascii_hex = working_record[19:25]
        # convert 6 ascii (hex) chars to int
        num_data_records_int = int(num_data_records_ascii_hex, 16)

        num_error_records_ascii_hex = working_record[25:31]
        # convert 6 ascii (hex) chars to int
        num_error_records_int = int(num_error_records_ascii_hex, 16)

        num_bytes_stored_ascii_hex = working_record[31:37]
        # convert 6 ascii (hex) chars to int
        num_bytes_stored_int = int(num_bytes_stored_ascii_hex, 16)

        calculated_checksum = _calculate_working_record_checksum(working_record)

        # Record may not have voltage data...
        if have_voltage_battery_data:
            voltage_battery_ascii_hex = working_record[37:41]
            # convert 4 ascii (hex) chars to int
            voltage_battery_int = int(voltage_battery_ascii_hex, 16)

            passed_checksum_ascii_hex = working_record[41:43]
            # convert 2 ascii (hex) chars to int
            passed_checksum_int = int(passed_checksum_ascii_hex, 16)

            # Per IDD, if the calculated checksum does not match the checksum in the record,
            # use a checksum of zero in the resultant particle
            if passed_checksum_int != calculated_checksum:
                checksum_final = 0
            else:
                checksum_final = 1
        else:
            voltage_battery_int = None

            passed_checksum_ascii_hex = working_record[37:39]
            # convert 2 ascii (hex) chars to int
            passed_checksum_int = int(passed_checksum_ascii_hex, 16)

            # Per IDD, if the calculated checksum does not match the checksum in the record,
            # use a checksum of zero in the resultant particle
            if passed_checksum_int != calculated_checksum:
                checksum_final = 0
            else:
                checksum_final = 1

        # ASSEMBLE THE RESULTANT PARTICLE..
        resultant_particle_data = [{DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.DCL_CONTROLLER_TIMESTAMP,
                                    DataParticleKey.VALUE: dcl_controller_timestamp},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.UNIQUE_ID,
                                    DataParticleKey.VALUE: unique_id_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.RECORD_TYPE,
                                    DataParticleKey.VALUE: record_type_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.RECORD_TIME,
                                    DataParticleKey.VALUE: record_time_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.CLOCK_ACTIVE,
                                    DataParticleKey.VALUE: clock_active},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.RECORDING_ACTIVE,
                                    DataParticleKey.VALUE: recording_active},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.RECORD_END_ON_TIME,
                                    DataParticleKey.VALUE: record_end_on_time},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.RECORD_MEMORY_FULL,
                                    DataParticleKey.VALUE: record_memory_full},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.RECORD_END_ON_ERROR,
                                    DataParticleKey.VALUE: record_end_on_error},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.DATA_DOWNLOAD_OK,
                                    DataParticleKey.VALUE: data_download_ok},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.FLASH_MEMORY_OPEN,
                                    DataParticleKey.VALUE: flash_memory_open},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_PRESTART,
                                    DataParticleKey.VALUE: battery_low_prestart},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_MEASUREMENT,
                                    DataParticleKey.VALUE: battery_low_measurement},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_BLANK,
                                    DataParticleKey.VALUE: battery_low_blank},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_EXTERNAL,
                                    DataParticleKey.VALUE: battery_low_external},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                                    DataParticleKey.VALUE: external_device1_fault},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                                    DataParticleKey.VALUE: external_device2_fault},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                                    DataParticleKey.VALUE: external_device3_fault},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.FLASH_ERASED,
                                    DataParticleKey.VALUE: flash_erased},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.POWER_ON_INVALID,
                                    DataParticleKey.VALUE: power_on_invalid},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.NUM_DATA_RECORDS,
                                    DataParticleKey.VALUE: num_data_records_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.NUM_ERROR_RECORDS,
                                    DataParticleKey.VALUE: num_error_records_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.NUM_BYTES_STORED,
                                    DataParticleKey.VALUE: num_bytes_stored_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.VOLTAGE_BATTERY,
                                    DataParticleKey.VALUE: voltage_battery_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclMetadataDataParticleKey.PASSED_CHECKSUM,
                                    DataParticleKey.VALUE: checksum_final}]

        return resultant_particle_data


class PhsenAbcdefDclMetadataDataParticleKey(BaseEnum):
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'
    UNIQUE_ID = 'unique_id'
    RECORD_TYPE = 'record_type'
    RECORD_TIME = 'record_time'
    CLOCK_ACTIVE = 'clock_active'
    RECORDING_ACTIVE = 'recording_active'
    RECORD_END_ON_TIME = 'record_end_on_time'
    RECORD_MEMORY_FULL = 'record_memory_full'
    RECORD_END_ON_ERROR = 'record_end_on_error'
    DATA_DOWNLOAD_OK = 'data_download_ok'
    FLASH_MEMORY_OPEN = 'flash_memory_open'
    BATTERY_LOW_PRESTART = 'battery_low_prestart'
    BATTERY_LOW_MEASUREMENT = 'battery_low_measurement'
    BATTERY_LOW_BLANK = 'battery_low_blank'
    BATTERY_LOW_EXTERNAL = 'battery_low_external'
    EXTERNAL_DEVICE1_FAULT = 'external_device1_fault'
    EXTERNAL_DEVICE2_FAULT = 'external_device2_fault'
    EXTERNAL_DEVICE3_FAULT = 'external_device3_fault'
    FLASH_ERASED = 'flash_erased'
    POWER_ON_INVALID = 'power_on_invalid'
    NUM_DATA_RECORDS = 'num_data_records'
    NUM_ERROR_RECORDS = 'num_error_records'
    NUM_BYTES_STORED = 'num_bytes_stored'
    VOLTAGE_BATTERY = 'voltage_battery'
    PASSED_CHECKSUM = 'passed_checksum'


class PhsenAbcdefDclInstrumentDataParticle(DataParticle):
    measurement_num_of_chars = 4

    def _create_light_measurements_array(self, working_record):
        """
        Creates a light measurement array from raw data for a PHSEN DCL Instrument record
        @returns list a list of light measurement values.  From the IDD: (an) array of 92 light measurements
                      (23 sets of 4 measurements)
        """
        light_measurements_list_int = []

        light_measurements_chunk = working_record[83:-14]
        light_measurements_ascii_hex = [light_measurements_chunk[i:i+self.measurement_num_of_chars]
                                        for i in range(0, len(light_measurements_chunk),
                                                       self.measurement_num_of_chars)]

        for ascii_hex_value in light_measurements_ascii_hex:
            light_measurements_int = convert_to_signed_int_16_bit(ascii_hex_value)
            light_measurements_list_int.append(light_measurements_int)

        return light_measurements_list_int

    def _create_reference_light_measurements_array(self, working_record):
        """
        Creates a reference light measurement array from raw data for a PHSEN DCL Instrument record
        @returns list a list of light measurement values.  From the IDD: (an) array of 16 measurements
                      (4 sets of 4 measurements)
        """
        reference_light_measurements_list_int = []

        reference_light_measurements_chunk = working_record[19:-382]
        reference_light_measurements_ascii_hex = [reference_light_measurements_chunk[i:i+self.measurement_num_of_chars]
                                                  for i in range(0, len(reference_light_measurements_chunk),
                                                                 self.measurement_num_of_chars)]

        for ascii_hex_value in reference_light_measurements_ascii_hex:
            reference_light_measurements_int = convert_to_signed_int_16_bit(ascii_hex_value)
            reference_light_measurements_list_int.append(reference_light_measurements_int)

        return reference_light_measurements_list_int

    def _build_parsed_values(self):
        """
        Extracts PHSEN ABCDEF DCL Instrument data from the raw_data tuple.

        @returns result a list of dictionaries of particle data
        """
        # extract the time from the raw_data tuple
        dcl_controller_timestamp = self.raw_data[0]

        # convert the time
        converted_time = dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp)
        # set the converted time to the particle internal timestamp
        self.set_internal_timestamp(converted_time)

        # extract the working_record string from the raw data tuple
        working_record = self.raw_data[1]

        # Begin saving particle data
        unique_id_ascii_hex = working_record[1:3]
        # convert 2 ascii (hex) chars to int
        unique_id_int = int(unique_id_ascii_hex, 16)

        record_type_ascii_hex = working_record[5:7]
        # convert 2 ascii (hex) chars to int
        record_type_int = int(record_type_ascii_hex, 16)

        record_time_ascii_hex = working_record[7:15]
        # convert 8 ascii (hex) chars to int
        record_time_int = int(record_time_ascii_hex, 16)

        thermistor_start_ascii_hex = working_record[15:19]
        # convert 4 ascii (hex) chars to int
        thermistor_start_int = int(thermistor_start_ascii_hex, 16)

        reference_light_measurements_list_int = self._create_reference_light_measurements_array(working_record)

        light_measurements_list_int = self._create_light_measurements_array(working_record)

        voltage_battery_ascii_hex = working_record[455:459]
        # convert 4 ascii (hex) chars to int
        voltage_battery_int = int(voltage_battery_ascii_hex, 16)

        thermistor_end_ascii_hex = working_record[459:463]
        # convert 4 ascii (hex) chars to int
        thermistor_end_int = int(thermistor_end_ascii_hex, 16)

        passed_checksum_ascii_hex = working_record[463:465]
        # convert 2 ascii (hex) chars to int
        passed_checksum_int = int(passed_checksum_ascii_hex, 16)

        calculated_checksum = _calculate_working_record_checksum(working_record)

        # Per IDD, if the calculated checksum does not match the checksum in the record,
        # use a checksum of zero in the resultant particle
        if passed_checksum_int != calculated_checksum:
            checksum_final = 0
        else:
            checksum_final = 1

        # ASSEMBLE THE RESULTANT PARTICLE..
        resultant_particle_data = [{DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.DCL_CONTROLLER_TIMESTAMP,
                                    DataParticleKey.VALUE: dcl_controller_timestamp},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.UNIQUE_ID,
                                    DataParticleKey.VALUE: unique_id_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.RECORD_TYPE,
                                    DataParticleKey.VALUE: record_type_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.RECORD_TIME,
                                    DataParticleKey.VALUE: record_time_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.THERMISTOR_START,
                                    DataParticleKey.VALUE: thermistor_start_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS,
                                    DataParticleKey.VALUE: reference_light_measurements_list_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.LIGHT_MEASUREMENTS,
                                    DataParticleKey.VALUE: light_measurements_list_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.VOLTAGE_BATTERY,
                                    DataParticleKey.VALUE: voltage_battery_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.THERMISTOR_END,
                                    DataParticleKey.VALUE: thermistor_end_int},
                                   {DataParticleKey.VALUE_ID:
                                    PhsenAbcdefDclInstrumentDataParticleKey.PASSED_CHECKSUM,
                                    DataParticleKey.VALUE: checksum_final}]

        return resultant_particle_data


class PhsenAbcdefDclInstrumentDataParticleKey(BaseEnum):
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'
    UNIQUE_ID = 'unique_id'
    RECORD_TYPE = 'record_type'
    RECORD_TIME = 'record_time'
    THERMISTOR_START = 'thermistor_start'
    REFERENCE_LIGHT_MEASUREMENTS = 'reference_light_measurements'
    LIGHT_MEASUREMENTS = 'light_measurements'
    VOLTAGE_BATTERY = 'voltage_battery'
    THERMISTOR_END = 'thermistor_end'
    PASSED_CHECKSUM = 'passed_checksum'


class PhsenAbcdefDclMetadataRecoveredDataParticle(PhsenAbcdefDclMetadataDataParticle):

    _data_particle_type = DataParticleType.METADATA_RECOVERED


class PhsenAbcdefDclMetadataTelemeteredDataParticle(PhsenAbcdefDclMetadataDataParticle):

    _data_particle_type = DataParticleType.METADATA_TELEMETERED


class PhsenAbcdefDclInstrumentRecoveredDataParticle(PhsenAbcdefDclInstrumentDataParticle):

    _data_particle_type = DataParticleType.INSTRUMENT_RECOVERED


class PhsenAbcdefDclInstrumentTelemeteredDataParticle(PhsenAbcdefDclInstrumentDataParticle):

    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED


class DataTypeEnum(BaseEnum):
    UNKNOWN = 0
    INSTRUMENT = 1
    CONTROL = 2


class PhsenAbcdefDclParser(SimpleParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        # whitespace regex
        self._whitespace_regex = re.compile(ONE_OR_MORE_WHITESPACE_REGEX)
        # instrument data regex: *
        self._instrument_data_regex = re.compile(r'\*')

        particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
        self._instrument_data_particle_class = particle_classes_dict.get('data_particle_class_key')
        self._metadata_particle_class = particle_classes_dict.get('metadata_particle_class_key')

        super(PhsenAbcdefDclParser, self).__init__(config, stream_handle, exception_callback)

        self.working_record = ""

        self.in_record = False

        self.latest_dcl_time = ""

        self.result_particle_list = []

    def _strip_logfile_line(self, logfile_line):
        """
        Strips any trailing newline and linefeed from the logfile line,
        and strips the leading DLC time from the logfile line
        """
        # strip off any trailing linefeed or newline hidden characters
        working_logfile_line = logfile_line.rstrip('\r\n')

        # strip off the preceding 24 characters (the DCL time) of the log line
        stripped_logfile_line = self._strip_time(working_logfile_line)

        return stripped_logfile_line

    def _strip_time(self, logfile_line):

        # strip off the leading 24 characters of the log line
        stripped_logfile_line = logfile_line[24:]

        # save off this DLC time in case this is the last DCL time recorded before the next record begins
        self.latest_dcl_time = logfile_line[:23]

        return stripped_logfile_line

    def _process_instrument_data(self, working_record):
        """
        Determines which particle to produce, calls extract_sample to create the given particle
        """
        log.debug("PhsenAbcdefDclParser._process_instrument_data(): aggregate working_record size %s is %s",
                  len(working_record), working_record)

        # this size includes the leading * character
        instrument_record_length = 465

        # this size includes the leading * character
        control_record_length_without_voltage_battery = 39

        # this size includes the leading * character
        control_record_length_with_voltage_battery = 43

        data_type = self._determine_data_type(working_record)

        if data_type is not DataTypeEnum.UNKNOWN:

            # Create a tuple for the particle composed of the working record and latest DCL time
            # The tuple allows for DCL time to be available when EXTERNAL calls each particle's
            # build_parse_values method
            particle_data = (self.latest_dcl_time, working_record)

            if data_type is DataTypeEnum.INSTRUMENT:

                # Per the IDD, if the candidate data is not the proper size, throw a recoverable exception
                if len(working_record) == instrument_record_length:

                    # Create particle mule (to be used later to create the instrument particle)
                    particle = self._extract_sample(self._instrument_data_particle_class,
                                                    None,
                                                    particle_data,
                                                    self.latest_dcl_time)

                    self._record_buffer.append(particle)
                else:
                    self._exception_callback(RecoverableSampleException(
                        "PhsenAbcdefDclParser._process_instrument_data(): "
                        "Throwing RecoverableSampleException, Size of data "
                        "record is not the length of an instrument data record"))

            elif data_type is DataTypeEnum.CONTROL:

                # Per the IDD, if the candidate data is not the proper size, throw a recoverable exception
                if len(working_record) == control_record_length_without_voltage_battery or \
                   len(working_record) == control_record_length_with_voltage_battery:

                    # Create particle mule (to be used later to create the metadata particle)
                    particle = self._extract_sample(self._metadata_particle_class,
                                                    None,
                                                    particle_data,
                                                    self.latest_dcl_time)

                    self._record_buffer.append(particle)
                else:
                    log.warn("PhsenAbcdefDclParser._process_instrument_data(): "
                             "Size of data record is not the length of a control data record")

                    self._exception_callback(RecoverableSampleException(
                        "PhsenAbcdefDclParser._process_instrument_data(): "
                        "Throwing RecoverableSampleException, Size of data "
                        "record is not the length of a control data record"))
        else:
            log.warn("PhsenAbcdefDclParser._process_instrument_data(): "
                     "Throwing RecoverableSampleException, Record is neither instrument or control")

            self._exception_callback(RecoverableSampleException("PhsenAbcdefDclParser._process_instrument_data(): "
                                                                "Data Type is neither Control or Instrument"))

    @staticmethod
    def _determine_data_type(working_record):

        # strip out the type from the working record
        type_ascii_hex = working_record[5:7]
        # convert to a 16 bit unsigned int
        type_int = int(type_ascii_hex, 16)

        # allowable Control record hex values are from the SAMI_error_info_control_records spreadsheet
        is_control_record = re.search(r'80|81|83|85|86|87|BE|BF|C0|C1|C2|C3|C4|C5|C6|FE|FF', type_ascii_hex)

        # Type checks, per values defined in the IDD
        if type_int == 10:
            return DataTypeEnum.INSTRUMENT
        elif is_control_record:
            return DataTypeEnum.CONTROL
        else:
            return DataTypeEnum.UNKNOWN

    def parse_file(self):
        """
        Parse data file line by line. If the line
        it is a valid data piece, build a particle, append to buffer
        """

        for line in self._stream_handle:

            is_bracket_present = re.search(r'\[', line)

            # check for a * in this line, signaling the start of a new record
            is_star_present = re.search(r'\*', line)

            # if this line has a bracket it should not be processed...
            if is_bracket_present:

                # if the line has a bracket AND data has been previously parsed...
                if self.in_record:

                    # if the aggregate working record is not empty,
                    # the working record is complete and a particle can now be created
                    if self.working_record:

                        # PROCESS WORKING STRING TO CREATE A PARTICLE
                        self._process_instrument_data(self.working_record)

                        # clear out the working record (the last string that was being built)
                        self.working_record = ""

                # if the line has a bracket and data has NOT been previously parsed,
                # do nothing (this is one of the first chunks seen by this parser)

            # if the line does NOT have a bracket, it contains instrument or control log data
            else:
                # if the * character is present this is the first piece of data for an instrument or control log
                if is_star_present:
                    # strip the trailing newlines and carriage returns from the string
                    # strip the leading DCL data/time data from the string
                    # save off the DLC time
                    stripped_logfile_line = self._strip_logfile_line(line)

                    # the working record should be empty when a new star is found
                    if len(self.working_record) > 0:
                        # clear the working record, it must contain bad or start of day data
                        # clear out the working record (the last string that was being built)
                        self.working_record = ""

                        log.warn("PhsenAbcdefDclParser.parse_chunks(): "
                                 "found a new star but working_record is non-zero length, "
                                 "throwing a RecoverableSample exception")
                        self._exception_callback(RecoverableSampleException(
                            "PhsenAbcdefDclParser.parse_chunks(): "
                            "found a new record to parse but "
                            "working_record is non-zero length, "
                            "throwing a RecoverableSample exception"))

                    # append time_stripped_logfile_line to working_record
                    self.working_record += stripped_logfile_line

                    # this is the first time a * has been found, set a flag
                    self.in_record = True

                # if there is no * character in this line,
                # it is the next part of an instrument or control log file
                # and will be appended to the previous portion of the log file
                else:
                    # strip the trailing newlines and carriage returns from the string
                    # strip the leading DCL data/time data from the string
                    # save off the DLC time
                    stripped_logfile_line = self._strip_logfile_line(line)

                    # append time_stripped_logfile_line to working_record
                    self.working_record += stripped_logfile_line

        # Per the IDD, it is possible for a single instrument data record to span multiple files, when the record is
        # being written out as the day changes. Since the software architecture does not support parsing a single
        # particle from multiple files, a recoverable sample exception should be issued in this case.
        if len(self.working_record) > 0:
            log.warn("PhsenAbcdefDclParser.parse_chunks(): "
                     "working_record is non-zero length, throwing a RecoverableSample exception")
    
            self._exception_callback(RecoverableSampleException("PhsenAbcdefDclParser.parse_chunks(): "
                                                                "working_record is non-zero length, "
                                                                "throwing a RecoverableSample exception"))
