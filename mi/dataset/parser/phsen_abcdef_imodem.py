"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/phsen_abcdef_imodem.py
@author Joe Padula
@brief Parser for the phsen_abcdef_imodem recovered and telemetered dataset
Release notes:

initial release
"""
__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

import ntplib
import re

from mi.core.common import BaseEnum
from mi.core.exceptions import ConfigurationException, UnexpectedDataException, \
    RecoverableSampleException
from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_parser import DataSetDriverConfigKeys, SimpleParser
from mi.dataset.parser.common_regexes import \
    ASCII_HEX_CHAR_REGEX, END_OF_LINE_REGEX, FLOAT_REGEX
from mi.dataset.parser.phsen_abcdef_imodem_particles import \
    PhsenAbcdefImodemDataParticleKey
from mi.dataset.parser.utilities import \
    formatted_timestamp_utc_time, \
    mac_timestamp_to_utc_timestamp

# The following constants are used to index into the string array of flag bit values
# after the numeric value is converted to a bit string and split into a array
# Example of flags:
# 0043: Flags
# converted to binary in a string: 0000000001000011
#
# The flags will be set as follows:
#    00000000 00000001 = Clock started
#    00000000 00000010 = Recording started
#    00000000 01000000 = Flash Open
#
# Note that the 0 bit, or rightmost bit, is actually the 15th index into the string
# array representation.

CLOCK_ACTIVE_FLAGS_INDEX = 15
RECORDING_ACTIVE_FLAGS_INDEX = 14
RECORD_END_ON_TIME_FLAGS_INDEX = 13
RECORD_MEMORY_FULL_FLAGS_INDEX = 12
RECORD_END_ON_ERROR_FLAGS_INDEX = 11
DATA_DOWNLOAD_OK_FLAGS_INDEX = 10
FLASH_MEMORY_OPEN_FLAGS_INDEX = 9
BATTERY_LOW_PRESTART_FLAGS_INDEX = 8
BATTERY_LOW_MEASUREMENT_FLAGS_INDEX = 7
BATTERY_LOW_BLANK_FLAGS_INDEX = 6
BATTERY_LOW_EXTERNAL_FLAGS_INDEX = 5
EXTERNAL_DEVICE1_FAULT_FLAGS_INDEX = 4
EXTERNAL_DEVICE2_FAULT_FLAGS_INDEX = 3
EXTERNAL_DEVICE3_FAULT_FLAGS_INDEX = 2
FLASH_ERASED_FLAGS_INDEX = 1
POWER_ON_INVALID_FLAGS_INDEX = 0

HEADER_BEGIN_REGEX = r'#UIMM Status' + END_OF_LINE_REGEX

DATA_BEGIN_REGEX = r'#Begin UIMM Data' + END_OF_LINE_REGEX
BLANK_LINE_REGEX = r'\s*' + END_OF_LINE_REGEX

# Metadata REGEX section

FILE_TIME_REGEX = r'#\w+_DateTime:\s+(?P<' + \
                  PhsenAbcdefImodemDataParticleKey.FILE_TIME + \
                  '>\d{8}\s+\d{6})' + END_OF_LINE_REGEX

INSTRUMENT_ID_REGEX = \
    r'#ID=(?P<' + \
    PhsenAbcdefImodemDataParticleKey.INSTRUMENT_ID + \
    '>\w+)' + END_OF_LINE_REGEX

SERIAL_NUMBER_REGEX = \
    r'#SN=(?P<' + \
    PhsenAbcdefImodemDataParticleKey.SERIAL_NUMBER + \
    '>\w+)' + END_OF_LINE_REGEX

VOLTAGE_FLT32_REGEX = \
    r'#Volts=(?P<' + \
    PhsenAbcdefImodemDataParticleKey.VOLTAGE_FLT32 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

HEADER_NUM_DATA_RECORDS_REGEX = \
    r'#Records=(?P<' + \
    PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS + \
    '>\d+)' + END_OF_LINE_REGEX

RECORD_LENGTH_REGEX = \
    r'#Length=(?P<' + \
    PhsenAbcdefImodemDataParticleKey.RECORD_LENGTH + \
    '>\d+)' + END_OF_LINE_REGEX

NUM_EVENTS_REGEX = \
    r'#Events=(?P<' + \
    PhsenAbcdefImodemDataParticleKey.NUM_EVENTS + \
    '>\d+)' + END_OF_LINE_REGEX

NUM_SAMPLES_REGEX = \
    r'#End UIMM Data,\s+(?P<' + \
    PhsenAbcdefImodemDataParticleKey.NUM_SAMPLES + \
    '>\d+)\s+samples written' + END_OF_LINE_REGEX

# REGEX common to Science (ph or Control) record types
RECORD_TIME_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{8}' + ')'
ID_REGEX = LEN_REGEX = CHECKSUM_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{2}' + ')'
BATTERY_VOLTAGE_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{4}' + ')'
UNUSED_DATA = ASCII_HEX_CHAR_REGEX + '{4}'

# Common regex for beginning of each science data record
# Note: there may be "Record[nnn]:" before the record start
RECORD_PREFIX_REGEX = '(?:Record.*)?'
RECORD_START_REGEX = '\*'
SCIENCE_DATA_RECORD_BEGIN_PART_REGEX = RECORD_PREFIX_REGEX + RECORD_START_REGEX + \
    ID_REGEX + LEN_REGEX

# The following constants are used to index into an MATCHER for Science records
ID_GROUP_INDEX = 1
LENGTH_GROUP_INDEX = 2
RECORD_TYPE_GROUP_INDEX = 3
RECORD_TIME_GROUP_INDEX = 4

#    pH Record
#
# BEGIN definition of regular expressions, matchers and group indices for pH
# data records.

PH_RECORD_TYPE_REGEX = r'(0A)'
LIGHT_MEASUREMENTS_REGEX = r'((?:' + ASCII_HEX_CHAR_REGEX + '){368})'
REF_LIGHT_MEASUREMENTS_REGEX = r'((?:' + ASCII_HEX_CHAR_REGEX + '){64})'
THERMISTOR_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{4}' + ')'

PH_REGEX = SCIENCE_DATA_RECORD_BEGIN_PART_REGEX + PH_RECORD_TYPE_REGEX + RECORD_TIME_REGEX + \
    THERMISTOR_REGEX + REF_LIGHT_MEASUREMENTS_REGEX + LIGHT_MEASUREMENTS_REGEX + \
    UNUSED_DATA + BATTERY_VOLTAGE_REGEX + THERMISTOR_REGEX + CHECKSUM_REGEX
PH_MATCHER = re.compile(PH_REGEX)

# The following constants are used to index into an PH_MATCHER match group
STARTING_THERMISTOR_GROUP_INDEX = 5
REF_LIGHT_MEASUREMENTS_GROUP_INDEX = 6
LIGHT_MEASUREMENTS_GROUP_INDEX = 7
PH_BATTERY_VOLTAGE_GROUP_INDEX = 8
ENDING_THERMISTOR_GROUP_INDEX = 9

# END definition of regular expressions, matchers and group indices for pH data records.

#    Control Record
#
# BEGIN definition of regular expressions, matchers and group indices for Control records.

FLAGS_REGEX = r'(\d{4})'
NUM_DATA_RECORDS_REGEX = NUM_ERROR_RECORDS_REGEX = NUM_BYTES_STORED_REGEX = \
    r'(' + ASCII_HEX_CHAR_REGEX + '{6}' + ')'

CONTROL_RECORD_TYPE_REGEX = r'(80|81|83|85|86|87|BE|BF|C2|C3|C4|C5|C6|FE|FF)'
CONTROL_REGEX = SCIENCE_DATA_RECORD_BEGIN_PART_REGEX + CONTROL_RECORD_TYPE_REGEX + \
    RECORD_TIME_REGEX + FLAGS_REGEX + NUM_DATA_RECORDS_REGEX + \
    NUM_ERROR_RECORDS_REGEX + NUM_BYTES_STORED_REGEX + CHECKSUM_REGEX
CONTROL_MATCHER = re.compile(CONTROL_REGEX)

CONTROL_WITH_BATTERY_VOLTAGE_RECORD_TYPE_REGEX = r'(C0|C1)'
CONTROL_WITH_BATTERY_VOLTAGE_REGEX = SCIENCE_DATA_RECORD_BEGIN_PART_REGEX + \
    CONTROL_WITH_BATTERY_VOLTAGE_RECORD_TYPE_REGEX + \
    RECORD_TIME_REGEX + FLAGS_REGEX + NUM_DATA_RECORDS_REGEX + \
    NUM_ERROR_RECORDS_REGEX + NUM_BYTES_STORED_REGEX + BATTERY_VOLTAGE_REGEX + CHECKSUM_REGEX
CONTROL_WITH_BATTERY_VOLTAGE_MATCHER = re.compile(CONTROL_WITH_BATTERY_VOLTAGE_REGEX)

# The following constants are used to index into an CONTROL_MATCHER or
# CONTROL_WITH_BATTERY_VOLTAGE_MATCHER match group
FLAGS_GROUP_INDEX = 5
NUM_DATA_RECORDS_GROUP_INDEX = 6
NUM_ERROR_RECORDS_GROUP_INDEX = 7
NUM_BYTES_STORED_GROUP_INDEX = 8
CONTROL_BATTERY_VOLTAGE_GROUP_INDEX = 9

# END definition of regular expressions, matchers and group indices for control records.


class MetadataMatchKey(BaseEnum):
    """
    An enum for the keys in the _metadata_matches_dict.
    """
    FILE_TIME_MATCH = 'file_time_match'
    INSTRUMENT_ID_MATCH = 'instrument_id_match'
    SERIAL_NUMBER_MATCH = 'serial_number_match'
    VOLTAGE_FLT32_MATCH = 'voltage_flt32_match'
    NUM_DATA_RECORDS_MATCH = 'num_data_records_match'
    RECORD_LENGTH_MATCH = 'record_length_match'
    NUM_EVENTS_MATCH = 'num_events_match'
    NUM_SAMPLES_MATCH = 'num_samples_match'


class PhsenAbcdefImodemParticleClassKey (BaseEnum):
    """
    An enum for the keys application to the phsen abcdef imodem particle classes.
    """
    INSTRUMENT_PARTICLE_CLASS = 'instrument_particle_class'
    CONTROL_PARTICLE_CLASS = 'control_particle_class'
    METADATA_PARTICLE_CLASS = 'metadata_particle_class'


class PhsenAbcdefImodemParser(SimpleParser):
    """
    Class used to parse the phsen_abcdef_imodem data set. This class extends the Parser class.
    """
    # Only certain control records have battery voltage
    _control_record_has_battery_voltage = False

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        super(PhsenAbcdefImodemParser, self).__init__(config,
                                                      stream_handle,
                                                      exception_callback)

        try:
            self._instrument_particle_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    PhsenAbcdefImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS]
            self._control_particle_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    PhsenAbcdefImodemParticleClassKey.CONTROL_PARTICLE_CLASS]
            self._metadata_particle_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    PhsenAbcdefImodemParticleClassKey.METADATA_PARTICLE_CLASS]
        except:
            raise ConfigurationException("Error configuring PhsenAbcdefImodemParser")

        # Construct the dictionary to save off the metadata record matches
        self._metadata_matches_dict = {
            MetadataMatchKey.FILE_TIME_MATCH: None,
            MetadataMatchKey.INSTRUMENT_ID_MATCH: None,
            MetadataMatchKey.SERIAL_NUMBER_MATCH: None,
            MetadataMatchKey.VOLTAGE_FLT32_MATCH: None,
            MetadataMatchKey.RECORD_LENGTH_MATCH: None,
            MetadataMatchKey.NUM_EVENTS_MATCH: None,
            MetadataMatchKey.NUM_SAMPLES_MATCH: None,
        }

        self._metadata_sample_generated = False

    @staticmethod
    def _populate_common_dict(common_match, common_dict):
        """
        Populate parameters that are common to all the types of particles.
        :param common_match: the match used to get the group
        :param common_dict: dict to be populated
        :return: the populated common dict
        """

        unique_id = common_match.group(ID_GROUP_INDEX)
        common_dict[PhsenAbcdefImodemDataParticleKey.UNIQUE_ID] = int(unique_id, 16)

        rec_type = common_match.group(RECORD_TYPE_GROUP_INDEX)
        common_dict[PhsenAbcdefImodemDataParticleKey.RECORD_TYPE] = int(rec_type, 16)

        rec_time = common_match.group(RECORD_TIME_GROUP_INDEX)
        common_dict[PhsenAbcdefImodemDataParticleKey.RECORD_TIME] = int(rec_time, 16)

        return common_dict

    @staticmethod
    def _populate_light_measurements(light_measurements, instrument_dict, dict_key):
        """
        Helper method for filling in the light measurements.
        :param light_measurements: light or reference light measurements
        :param instrument_dict: instrument_dict
        :param dict_key: either PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS or
                        PhsenAbcdefImodemDataParticleKey.REF_LIGHT_MEASUREMENTS
        :return: dict filled in light or reference light measurements
        """
        log.trace("entered _populate_light_measurements()")
        end_range = num_chars = 4
        last_light_measurement_start_index = len(light_measurements)-3
        for x in range(0, last_light_measurement_start_index, num_chars):
            light_measurement = light_measurements[x: end_range]
            log.trace("ascii-hex light_measurement[%s]: %s", x+1, light_measurement)
            # convert to int
            int_light_measurement = int(light_measurement, 16)

            # append int value to instrument_dict
            instrument_dict[dict_key].append(int_light_measurement)

            end_range += num_chars

        return instrument_dict[dict_key]

    @staticmethod
    def _calculate_passed_checksum(line, record_checksum):
        """
        Calculate the checksum of the argument ascii-hex string.
        :param line: the record that will be stripped
        :record_checksum: the checksum from the record
        :return: the modulo integer checksum value of argument ascii-hex string
        """

        log.trace("_calculate_passed_checksum(): string_length is %s, record is %s, record_checksum is %s",
                  len(line), line, record_checksum)
        checksum = 0

        # Strip off the leading part of the record, including the ID characters. This
        # will vary as some lines start with Record, and others don't. Also the
        # lines that start with Record are different, example Record[13], or
        # Record[135] so the starting point to slice will vary.
        #
        # Strip off the trailing Checksum characters and newline (3 characters).

        beg = line.find('*')
        start_slice = beg + 3   # Get past the ID
        stripped_record = line[start_slice:-3]
        log.trace("stripped line: %s", stripped_record)
        stripped_record_length = len(stripped_record)

        log.trace("_calculate_passed_checksum(): stripped record length is %s",
                  stripped_record_length)
        for x in range(0, stripped_record_length, 2):
            value = stripped_record[x:x+2]
            log.trace("value: %s", value)
            checksum += int(value, 16)

        # module of the checksum will give us the low order byte
        log.trace("modulo of calculated checksum: %s", checksum % 256)
        log.trace("record checksum: %s", record_checksum)
        if record_checksum == checksum % 256:
            passed_checksum = 1
        else:
            passed_checksum = 0

        return passed_checksum

    @staticmethod
    def _generate_internal_timestamp(record_dict):
        """
        Generate the internal timestamp from the given record time contained
        in the record dict. The time is seconds since 1/1/1904 (or Mac time).
        :param record_dict: dictionary containing the record_time parameter
        :return: the internal timestamp
        """

        utc_time = float(mac_timestamp_to_utc_timestamp(
            record_dict[PhsenAbcdefImodemDataParticleKey.RECORD_TIME]))

        return float(ntplib.system_to_ntp_time(utc_time))

    def _process_metadata_match_dict(self, key, particle_data):
        """
        Process the metadata record matches.
        :param key: a MetadataMatchKey
        :param particle_data: particle data for the key
        """

        group_dict = self._metadata_matches_dict[key].groupdict()

        if key == MetadataMatchKey.FILE_TIME_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.FILE_TIME] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.FILE_TIME]

        elif key == MetadataMatchKey.INSTRUMENT_ID_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.INSTRUMENT_ID] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.INSTRUMENT_ID]

        elif key == MetadataMatchKey.SERIAL_NUMBER_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.SERIAL_NUMBER] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.SERIAL_NUMBER]

        elif key == MetadataMatchKey.VOLTAGE_FLT32_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.VOLTAGE_FLT32] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.VOLTAGE_FLT32]

        elif key == MetadataMatchKey.NUM_DATA_RECORDS_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS]

        elif key == MetadataMatchKey.RECORD_LENGTH_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.RECORD_LENGTH] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.RECORD_LENGTH]

        elif key == MetadataMatchKey.NUM_EVENTS_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.NUM_EVENTS] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.NUM_EVENTS]

        elif key == MetadataMatchKey.NUM_SAMPLES_MATCH:

            particle_data[PhsenAbcdefImodemDataParticleKey.NUM_SAMPLES] = \
                group_dict[PhsenAbcdefImodemDataParticleKey.NUM_SAMPLES]

    def _generate_metadata_particle(self):
        """
        This function generates a metadata particle.
        """

        if self._metadata_matches_dict[MetadataMatchKey.FILE_TIME_MATCH] is None:
            message = "Unable to create metadata particle due to missing file time"
            log.warn(message)
            self._exception_callback(RecoverableSampleException(message))
        else:
            particle_data = dict()

            for key in self._metadata_matches_dict.keys():
                log.trace('key: %s, particle_data: %s', key, particle_data)

                if self._metadata_matches_dict[key]:
                    self._process_metadata_match_dict(key, particle_data)

            utc_time = formatted_timestamp_utc_time(
                particle_data[PhsenAbcdefImodemDataParticleKey.FILE_TIME],
                "%Y%m%d %H%M%S")
            ntp_timestamp = ntplib.system_to_ntp_time(utc_time)

            # Generate the metadata particle class and add the
            # result to the list of particles to be returned.
            particle = self._extract_sample(self._metadata_particle_class,
                                            None,
                                            particle_data,
                                            ntp_timestamp)
            if particle is not None:
                log.trace("Appending metadata particle to record buffer: %s", particle.generate())
                self._record_buffer.append(particle)

    @staticmethod
    def _create_empty_instrument_dict():

        instrument_dict = dict.fromkeys([PhsenAbcdefImodemDataParticleKey.UNIQUE_ID,
                                         PhsenAbcdefImodemDataParticleKey.RECORD_TYPE,
                                         PhsenAbcdefImodemDataParticleKey.RECORD_TIME,
                                         PhsenAbcdefImodemDataParticleKey.THERMISTOR_START,
                                         PhsenAbcdefImodemDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS,
                                         PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS,
                                         PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY,
                                         PhsenAbcdefImodemDataParticleKey.THERMISTOR_END,
                                         PhsenAbcdefImodemDataParticleKey.PASSED_CHECKSUM],
                                        None)

        return instrument_dict

    @staticmethod
    def _create_empty_control_dict():

        control_dict = dict.fromkeys([PhsenAbcdefImodemDataParticleKey.UNIQUE_ID,
                                      PhsenAbcdefImodemDataParticleKey.RECORD_TYPE,
                                      PhsenAbcdefImodemDataParticleKey.RECORD_TIME,
                                      PhsenAbcdefImodemDataParticleKey.CLOCK_ACTIVE,
                                      PhsenAbcdefImodemDataParticleKey.RECORDING_ACTIVE,
                                      PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_TIME,
                                      PhsenAbcdefImodemDataParticleKey.RECORD_MEMORY_FULL,
                                      PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_ERROR,
                                      PhsenAbcdefImodemDataParticleKey.DATA_DOWNLOAD_OK,
                                      PhsenAbcdefImodemDataParticleKey.FLASH_MEMORY_OPEN,
                                      PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_PRESTART,
                                      PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_MEASUREMENT,
                                      PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_BLANK,
                                      PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_EXTERNAL,
                                      PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                                      PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                                      PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                                      PhsenAbcdefImodemDataParticleKey.FLASH_ERASED,
                                      PhsenAbcdefImodemDataParticleKey.POWER_ON_INVALID,
                                      PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS,
                                      PhsenAbcdefImodemDataParticleKey.NUM_ERROR_RECORDS,
                                      PhsenAbcdefImodemDataParticleKey.NUM_BYTES_STORED,
                                      PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY,
                                      PhsenAbcdefImodemDataParticleKey.PASSED_CHECKSUM],
                                     None)
        return control_dict

    @staticmethod
    def _create_empty_metadata_dict():

        metadata_dict = dict.fromkeys([PhsenAbcdefImodemDataParticleKey.FILE_TIME,
                                       PhsenAbcdefImodemDataParticleKey.INSTRUMENT_ID,
                                       PhsenAbcdefImodemDataParticleKey.SERIAL_NUMBER,
                                       PhsenAbcdefImodemDataParticleKey.VOLTAGE_FLT32,
                                       PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS,
                                       PhsenAbcdefImodemDataParticleKey.RECORD_LENGTH,
                                       PhsenAbcdefImodemDataParticleKey.NUM_EVENTS,
                                       PhsenAbcdefImodemDataParticleKey.NUM_SAMPLES],
                                      None)
        return metadata_dict

    def _populate_control_dict(self, control_match, control_dict, line):
        """
        Fields from the control record are used to populate
        the control dictionary.
        """

        log.trace('entered _populate_control_dict()')
        common_dict = PhsenAbcdefImodemParser._populate_common_dict(control_match, control_dict)
        control_dict.update(common_dict)

        # Convert the flags group from ASCII-HEX to int
        log.trace('control_match.group(FLAGS_GROUP_INDEX): %s', control_match.group(FLAGS_GROUP_INDEX))
        int_flags = int(control_match.group(FLAGS_GROUP_INDEX), 16)
        log.trace('int_flags: %s', int_flags)

        # Convert the FLAGS integer value to a 16 bit binary value with leading 0s
        # as necessary. This is stored as a string array.
        #
        # Note that format returns a string, which will make the 15th index
        # into the string the right most bit, so we had to adjust the index into flags
        # accordingly.
        flags = format(int(int_flags), '016b')
        log.trace('flags: %s', flags)

        control_dict[PhsenAbcdefImodemDataParticleKey.CLOCK_ACTIVE] = \
            flags[CLOCK_ACTIVE_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.RECORDING_ACTIVE] = \
            flags[RECORDING_ACTIVE_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_TIME] = \
            flags[RECORD_END_ON_TIME_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.RECORD_MEMORY_FULL] = \
            flags[RECORD_MEMORY_FULL_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_ERROR] = \
            flags[RECORD_END_ON_ERROR_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.DATA_DOWNLOAD_OK] = \
            flags[DATA_DOWNLOAD_OK_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.FLASH_MEMORY_OPEN] = \
            flags[FLASH_MEMORY_OPEN_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_PRESTART] = \
            flags[BATTERY_LOW_PRESTART_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_MEASUREMENT] = \
            flags[BATTERY_LOW_MEASUREMENT_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_BLANK] = \
            flags[BATTERY_LOW_BLANK_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_EXTERNAL] = \
            flags[BATTERY_LOW_EXTERNAL_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE1_FAULT] = \
            flags[EXTERNAL_DEVICE1_FAULT_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE2_FAULT] = \
            flags[EXTERNAL_DEVICE2_FAULT_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE3_FAULT] = \
            flags[EXTERNAL_DEVICE3_FAULT_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.FLASH_ERASED] = \
            flags[FLASH_ERASED_FLAGS_INDEX]
        control_dict[PhsenAbcdefImodemDataParticleKey.POWER_ON_INVALID] = \
            flags[POWER_ON_INVALID_FLAGS_INDEX]
        # End of flags

        num_data_records = control_match.group(NUM_DATA_RECORDS_GROUP_INDEX)
        control_dict[PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS] = int(num_data_records, 16)

        num_error_records = control_match.group(NUM_ERROR_RECORDS_GROUP_INDEX)
        control_dict[PhsenAbcdefImodemDataParticleKey.NUM_ERROR_RECORDS] = int(num_error_records, 16)

        num_bytes_stored = control_match.group(NUM_BYTES_STORED_GROUP_INDEX)
        control_dict[PhsenAbcdefImodemDataParticleKey.NUM_BYTES_STORED] = int(num_bytes_stored, 16)

        # Not all have battery_voltage
        if self._control_record_has_battery_voltage:
            battery_voltage = control_match.group(CONTROL_BATTERY_VOLTAGE_GROUP_INDEX)
            control_dict[PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY] = int(battery_voltage, 16)

        # Checksum will always be the last group
        passed_checksum = PhsenAbcdefImodemParser._calculate_passed_checksum(
            line, int(control_match.group(control_match.lastindex), 16))
        control_dict[PhsenAbcdefImodemDataParticleKey.PASSED_CHECKSUM] = passed_checksum

    @staticmethod
    def _populate_instrument_dict(instrument_record_match, instrument_dict, line):
        """
        Fields from the pH record are used to populate
        the instrument dictionary.
        """

        log.trace("entered _populate_instrument_dict")
        common_dict = PhsenAbcdefImodemParser._populate_common_dict(instrument_record_match, instrument_dict)
        instrument_dict.update(common_dict)

        starting_thermistor = instrument_record_match.group(STARTING_THERMISTOR_GROUP_INDEX)
        instrument_dict[PhsenAbcdefImodemDataParticleKey.THERMISTOR_START] = int(starting_thermistor, 16)

        # Reference light measurements array
        ref_light_measurements = instrument_record_match.group(REF_LIGHT_MEASUREMENTS_GROUP_INDEX)
        log.trace("reference light_measurements: %s", ref_light_measurements)
        instrument_dict[PhsenAbcdefImodemDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS] = []

        instrument_dict[PhsenAbcdefImodemDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS] \
            = PhsenAbcdefImodemParser._populate_light_measurements(
                ref_light_measurements,
                instrument_dict,
                PhsenAbcdefImodemDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS)

        # Light measurements array
        light_measurements = instrument_record_match.group(LIGHT_MEASUREMENTS_GROUP_INDEX)
        log.trace("light_measurements: %s", light_measurements)
        instrument_dict[PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS] = []

        instrument_dict[PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS] \
            = PhsenAbcdefImodemParser._populate_light_measurements(
                light_measurements,
                instrument_dict,
                PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS)

        battery_voltage = instrument_record_match.group(PH_BATTERY_VOLTAGE_GROUP_INDEX)
        instrument_dict[PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY] = int(battery_voltage, 16)

        ending_thermistor = instrument_record_match.group(ENDING_THERMISTOR_GROUP_INDEX)
        instrument_dict[PhsenAbcdefImodemDataParticleKey.THERMISTOR_END] = int(ending_thermistor, 16)

        # Checksum will always be the last group.
        passed_checksum = PhsenAbcdefImodemParser._calculate_passed_checksum(
            line,
            int(instrument_record_match.group(instrument_record_match.lastindex), 16))
        instrument_dict[PhsenAbcdefImodemDataParticleKey.PASSED_CHECKSUM] = passed_checksum

    def _handle_non_match(self, line):
        """
        Handle any record lines that do not match any of the REGEX patterns.
        :param line: the record line from the file
        """

        # Check for other lines that can be ignored
        if (re.match(HEADER_BEGIN_REGEX, line) or
                re.match(BLANK_LINE_REGEX, line) or
                re.match(DATA_BEGIN_REGEX, line)):
            log.debug("Ignoring line: %s", line)

        else:
            # We found a line in the file that was unexpected.  This includes a record
            # with incorrect length. Report a UnexpectedDataException.
            msg_str = "Unexpected data in file, or num bytes does not match record_length, line: "
            log.warn(msg_str + "%s", line)
            message = msg_str + line
            self._exception_callback(UnexpectedDataException(message))

    def _check_for_metadata_match(self, line):
        """
        Check for each of the different match possibilities. If we have a match
        put it in the dictionary. Once a match is found, return True.
        :param line: the line to check
        :return: True if a metadata match was found, otherwise False
        """

        file_time_match = re.match(FILE_TIME_REGEX, line)
        if file_time_match:
            self._metadata_matches_dict[MetadataMatchKey.FILE_TIME_MATCH] = \
                file_time_match
            return True

        instrument_id_match = re.match(INSTRUMENT_ID_REGEX, line)
        if instrument_id_match:
            self._metadata_matches_dict[MetadataMatchKey.INSTRUMENT_ID_MATCH] = \
                instrument_id_match
            return True

        serial_number_match = re.match(SERIAL_NUMBER_REGEX, line)
        if serial_number_match:
            self._metadata_matches_dict[MetadataMatchKey.SERIAL_NUMBER_MATCH] = \
                serial_number_match
            return True

        voltage_flt32_match = re.match(VOLTAGE_FLT32_REGEX, line)
        if voltage_flt32_match:
            self._metadata_matches_dict[MetadataMatchKey.VOLTAGE_FLT32_MATCH] = \
                voltage_flt32_match
            return True

        num_data_records_match = re.match(HEADER_NUM_DATA_RECORDS_REGEX, line)
        if num_data_records_match:
            self._metadata_matches_dict[MetadataMatchKey.NUM_DATA_RECORDS_MATCH] = \
                num_data_records_match
            return True

        record_length_match = re.match(RECORD_LENGTH_REGEX, line)
        if record_length_match:
            self._metadata_matches_dict[MetadataMatchKey.RECORD_LENGTH_MATCH] = \
                record_length_match
            return True

        num_events_match = re.match(NUM_EVENTS_REGEX, line)
        if num_events_match:
            self._metadata_matches_dict[MetadataMatchKey.NUM_EVENTS_MATCH] = \
                num_events_match
            return True

        num_samples_match = re.match(NUM_SAMPLES_REGEX, line)
        if num_samples_match:
            self._metadata_matches_dict[MetadataMatchKey.NUM_SAMPLES_MATCH] = \
                num_samples_match
            return True

        # No metadata matches found
        return False

    def parse_file(self):
        """
        Parse the input file.
        """

        # Create some empty dictionaries which we will use to collect
        # data for the extract sample calls.
        control_dict = self._create_empty_control_dict()
        instrument_dict = self._create_empty_instrument_dict()

        line = self._stream_handle.readline()

        # Go through each line in the file
        while line:

            log.trace("line = %s", line)

            # Check for each of the different match possibilities

            control_with_battery_voltage_match \
                = CONTROL_WITH_BATTERY_VOLTAGE_MATCHER.match(line)
            control_match = CONTROL_MATCHER.match(line)

            ph_match = PH_MATCHER.match(line)

            if not self._check_for_metadata_match(line):

                # There are two control match possibilities
                if control_with_battery_voltage_match or control_match:

                    # If we found a control record with battery voltage,
                    # supply the match
                    if control_with_battery_voltage_match:

                        self._control_record_has_battery_voltage = True

                        log.trace("found control record with battery voltage, line: %s", line)
                        log.trace("control groups: %s", control_with_battery_voltage_match.groups())
                        log.trace("control group1: %s", control_with_battery_voltage_match.group(1))
                        log.trace("control group2: %s", control_with_battery_voltage_match.group(2))
                        log.trace("control group3: %s", control_with_battery_voltage_match.group(3))
                        log.trace("control group4: %s", control_with_battery_voltage_match.group(4))
                        log.trace("control group5: %s", control_with_battery_voltage_match.group(5))
                        log.trace("control group6: %s", control_with_battery_voltage_match.group(6))
                        log.trace("control group7: %s", control_with_battery_voltage_match.group(7))
                        log.trace("control group8: %s", control_with_battery_voltage_match.group(8))
                        log.trace("control group9: %s", control_with_battery_voltage_match.group(9))

                        self._populate_control_dict(control_with_battery_voltage_match,
                                                    control_dict, line)

                    else:
                        log.trace("found control record without battery voltage, line: %s", line)
                        log.trace("control groups: %s", control_match.groups())
                        log.trace("control group1: %s", control_match.group(1))
                        log.trace("control group2: %s", control_match.group(2))
                        log.trace("control group3: %s", control_match.group(3))
                        log.trace("control group4: %s", control_match.group(4))
                        log.trace("control group5: %s", control_match.group(5))
                        log.trace("control group6: %s", control_match.group(6))
                        log.trace("control group7: %s", control_match.group(7))
                        log.trace("control group8: %s", control_match.group(8))
                        log.trace("control group9: %s", control_match.group(9))

                        self._control_record_has_battery_voltage = False

                        # If we found a control record without battery voltage,
                        # supply that match
                        self._populate_control_dict(control_match, control_dict, line)

                    particle = self._extract_sample(
                        self._control_particle_class,
                        None,
                        control_dict,
                        PhsenAbcdefImodemParser._generate_internal_timestamp(control_dict))

                    log.trace("Appending control particle: %s", particle.generate())
                    self._record_buffer.append(particle)

                    # Recreate an empty control dictionary
                    control_dict = self._create_empty_control_dict()

                # Does the line contain pH data?
                elif ph_match:
                    log.trace("Found pH record, line: %s", line)
                    log.trace("pH groups %s", ph_match.groups())
                    log.trace("pH group1 id %s", ph_match.group(1))
                    log.trace("pH group2 len %s", ph_match.group(2))
                    log.trace("pH group3 type %s", ph_match.group(3))
                    log.trace("pH group4 time %s", ph_match.group(4))
                    log.trace("pH group5 thermistor start %s", ph_match.group(5))
                    log.trace("pH group6 ref light meas %s", ph_match.group(6))
                    log.trace("pH group7 light meas %s", ph_match.group(7))
                    log.trace("pH group8 voltage %s", ph_match.group(8))
                    log.trace("pH group9 end thermistor %s", ph_match.group(9))

                    self._populate_instrument_dict(ph_match, instrument_dict, line)

                    particle = self._extract_sample(
                        self._instrument_particle_class,
                        None,
                        instrument_dict,
                        PhsenAbcdefImodemParser._generate_internal_timestamp(instrument_dict))

                    log.trace("Appending instrument particle: %s", particle.generate())
                    self._record_buffer.append(particle)

                    # Recreate an empty instrument dictionary
                    instrument_dict = self._create_empty_instrument_dict()

                else:
                    self._handle_non_match(line)

            # See if we have all the metadata parameters
            if (None not in self._metadata_matches_dict.values() and
                    not self._metadata_sample_generated):
                # Attempt to generate metadata particle
                self._generate_metadata_particle()
                self._metadata_sample_generated = True

            # Get the next line
            line = self._stream_handle.readline()

        # If the metadata sample is not fully populated log a warning
        if not self._metadata_sample_generated:
            log.warn("Not all parameters are populated in the metadata particle")
            # Attempt to generate metadata particle with whatever parameters have been populated
            self._generate_metadata_particle()
