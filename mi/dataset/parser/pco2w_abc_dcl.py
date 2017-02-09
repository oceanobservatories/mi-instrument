"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/pco2w_abc_dcl.py
@author Joe Padula
@brief Parser for the pco2w_abc_dcl recovered and telemetered dataset driver
Release notes:

initial release
"""
__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

import re
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_utc_time, \
    dcl_controller_timestamp_to_ntp_time

import ntplib

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.parser.pco2w_abc import Pco2wAbcParser

log = get_logger()
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcDataParticleKey
from mi.dataset.parser.common_regexes import ONE_OR_MORE_WHITESPACE_REGEX, ASCII_HEX_CHAR_REGEX

# A regex to match a date in format YYYY/MM/DD, example 2014/05/07
DATE_REGEX = r'\d{4}/\d{2}/\d{2}'
DATE_REGEX_MATCHER = re.compile(DATE_REGEX)

# A regex to match a time in format HH:MM:SS.sss, example 01:00:40.102
TIME_REGEX = r'\d{2}:\d{2}:\d{2}.\d{3}'
TIME_REGEX_MATCHER = re.compile(TIME_REGEX)

DCL_CONTROLLER_TIMESTAMP_REGEX = r'(' + DATE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + TIME_REGEX + ')'

ID_REGEX = LEN_REGEX = CHECKSUM_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{2}' + ')'

# Common regex for beginning of instrument data record
INSTRUMENT_DATA_RECORD_REGEX = DCL_CONTROLLER_TIMESTAMP_REGEX + ONE_OR_MORE_WHITESPACE_REGEX \
    + '\*{1}' + ID_REGEX + LEN_REGEX

# Format of DCL Controller Timestamp in records
# Example: 2014/08/17 00:57:10.648
DCL_CONTROLLER_TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S.%f"

"""
*** BEGIN definition of regular expressions, matchers and group indices for CO2 (normal) and
CO2 (blank measurements) data records.

Example CO2 record with light measurements:
> 2014/08/17 01:05:32.967 *C22704D015B0C50040004C0D290177079C048205C6249B0044004C0D2F0178079E04810C5A07ED3B

Data Mapping:
2014/08/17 01:05:32.967 -> DCL controller timestamp
* -> instrument data
C2 -> ID
27 -> length
04 -> type (CO2 record - normal)
D015B0C5 -> time
0040004C0D290177079C048205C6249B0044004C0D2F0178079E0481 -> light measurements
0C5A -> battery voltage
07ED -> thermistor
3B -> checksum


Example CO2 record with blank light measurements:
> 2014/05/07 13:06:20.390 *C22705CF8FE0900040004C0D0C023407B1020A09870F0E0045004E0D10023607B2020A0C4A07E232

Data Mapping:
2014/05/07 13:06:20.390 -> DCL controller timestamp
* -> instrument data
C2 -> ID
27 -> length
05 -> type (CO2 record - blank)
CF8FE090 -> time
0040004C0D0C023407B1020A09870F0E0045004E0D10023607B2020A - blank light measurements
0C4A -> battery voltage
07E2 -> thermistor
32 -> checksum

"""

INSTRUMENT_RECORD_TYPE_REGEX = r'(04)'
INSTRUMENT_BLANK_RECORD_TYPE_REGEX = r'(05)'
RECORD_TIME_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{8}' + ')'
BATTERY_VOLTAGE_REGEX = THERMISTOR_REGEX = r'(' + ASCII_HEX_CHAR_REGEX + '{4}' + ')'
LIGHT_MEASUREMENTS_REGEX = r'((?:' + ASCII_HEX_CHAR_REGEX + '){56})'

INSTRUMENT_REGEX = INSTRUMENT_DATA_RECORD_REGEX + INSTRUMENT_RECORD_TYPE_REGEX + RECORD_TIME_REGEX + \
    LIGHT_MEASUREMENTS_REGEX + BATTERY_VOLTAGE_REGEX + THERMISTOR_REGEX + CHECKSUM_REGEX
INSTRUMENT_MATCHER = re.compile(INSTRUMENT_REGEX)

INSTRUMENT_BLANK_REGEX = INSTRUMENT_DATA_RECORD_REGEX + INSTRUMENT_BLANK_RECORD_TYPE_REGEX + RECORD_TIME_REGEX + \
    LIGHT_MEASUREMENTS_REGEX + BATTERY_VOLTAGE_REGEX + THERMISTOR_REGEX + CHECKSUM_REGEX
INSTRUMENT_BLANK_MATCHER = re.compile(INSTRUMENT_BLANK_REGEX)

# Override from base class, index values are different
DATE_TIME_GROUP_INDEX = 1
ID_GROUP_INDEX = 2
LENGTH_GROUP_INDEX = 3
RECORD_TYPE_GROUP_INDEX = 4
RECORD_TIME_GROUP_INDEX = 5
LIGHT_MEASUREMENTS_GROUP_INDEX = 6
CO2_BATTERY_VOLTAGE_GROUP_INDEX = 7
THERMISTOR_GROUP_INDEX = 8

"""
*** END definition of regular expressions, matchers and group indices for CO2 (normal) and
CO2 (blank measurements) data records.
"""

"""
*** BEGIN definition of regular expressions, matchers and group indices for Control records.

Example:
> 2014/09/11 04:53:01.971 *B61285CEA6132D004100000B000000000A31D2

Data Mapping:
2014/09/11 04:53:01.971 -> DCL controller timestamp
* -> instrument data
B6 -> ID
12 -> length
85 -> type (control record without battery voltage field)
CEA6132D -> time
0041 -> flags
00000B -> # records
000000 -> # errors
000A31 -> # bytes
D2 -> checksum

"""
FLAGS_REGEX = r'(\d{4})'
NUM_DATA_RECORDS_REGEX = NUM_ERROR_RECORDS_REGEX = NUM_BYTES_STORED_REGEX = \
    r'(' + ASCII_HEX_CHAR_REGEX + '{6}' + ')'

METADATA_RECORD_TYPE_REGEX = r'(80|81|83|85|86|87|BE|BF|C2|C3|C4|C5|C6|FE|FF)'
METADATA_REGEX = INSTRUMENT_DATA_RECORD_REGEX + METADATA_RECORD_TYPE_REGEX + \
    RECORD_TIME_REGEX + FLAGS_REGEX + \
    NUM_DATA_RECORDS_REGEX + NUM_ERROR_RECORDS_REGEX + NUM_BYTES_STORED_REGEX + CHECKSUM_REGEX
METADATA_MATCHER = re.compile(METADATA_REGEX)

METADATA_WITH_BATTERY_VOLTAGE_RECORD_TYPE_REGEX = r'(C0|C1)'
METADATA_WITH_BATTERY_VOLTAGE_REGEX = INSTRUMENT_DATA_RECORD_REGEX + METADATA_WITH_BATTERY_VOLTAGE_RECORD_TYPE_REGEX + \
    RECORD_TIME_REGEX + FLAGS_REGEX + \
    NUM_DATA_RECORDS_REGEX + NUM_ERROR_RECORDS_REGEX + NUM_BYTES_STORED_REGEX + BATTERY_VOLTAGE_REGEX + CHECKSUM_REGEX
METADATA_WITH_BATTERY_VOLTAGE_MATCHER = re.compile(METADATA_WITH_BATTERY_VOLTAGE_REGEX)

# Override Control indices from base class, because of the three extra fields in the pco2w_abc_dcl records.
FLAGS_GROUP_INDEX = 6
NUM_DATA_RECORDS_GROUP_INDEX = 7
NUM_ERROR_RECORDS_GROUP_INDEX = 8
NUM_BYTES_STORED_GROUP_INDEX = 9
CONTROL_BATTERY_VOLTAGE_GROUP_INDEX = 10

"""
*** END definition of regular expressions, matchers and group indices for control records.
"""

"""
*** BEGIN definition of regular expressions, matchers and group indices for power records.

Example:
> 2014/08/17 01:00:39.425 *C20711D015AFA14D

Data Mapping:
2014/08/17 01:00:39.425 -> DCL controller timestamp
* -> instrument data
C2 -> ID
07 -> length
11 -> type (power record)
D015AFA1 -> time
4D -> checksum

"""

POWER_RECORD_TYPE_REGEX = r'(11|21|31)'
POWER_REGEX = INSTRUMENT_DATA_RECORD_REGEX + POWER_RECORD_TYPE_REGEX + RECORD_TIME_REGEX + CHECKSUM_REGEX
POWER_MATCHER = re.compile(POWER_REGEX)


"""
*** BEGIN definition of regular expressions, matchers and group indices for DCL logging records.

Example:
> 2014/08/17 00:57:10.648 [pco2w1:DLOGP5]:Idle state, without initialize

Data Mapping:
2014/08/17 00:57:10.648 -> DCL Controller Timestamp
[ -> DCL logging type

"""
DCL_LOGGING_RECORD_REGEX = DCL_CONTROLLER_TIMESTAMP_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + '\[{1}'
DCL_LOGGING_MATCHER = re.compile(DCL_LOGGING_RECORD_REGEX)
"""
*** END definition of regular expressions, matchers and group indices for DCL logging records.
"""

"""
NOTE: records with different record type will be ignored and RecoverableSampleException thrown.
"""


class Pco2wAbcDclParser(Pco2wAbcParser):
    """
    Class used to parse the pco2w_abc_dcl data set. This class extends the Pco2wAbcParser.
    """
    # Only certain control records have battery voltage
    _control_record_has_battery_voltage = False

    @staticmethod
    def _create_dcl_extension_dict():
        """
        Creates a dictionary for the additional DCL parameters
        :return: an empty dictionary for the dcl items
        """
        # Load the additional DCL dictionary entries for this subclass
        dcl_dict = dict.fromkeys([Pco2wAbcDataParticleKey.DCL_CONTROLLER_TIMESTAMP,
                                  Pco2wAbcDataParticleKey.UNIQUE_ID,
                                  Pco2wAbcDataParticleKey.PASSED_CHECKSUM],
                                 None)
        return dcl_dict

    @staticmethod
    def _populate_common_dict(common_match, common_dict):
        """
        Populates parameters that are common to all the types of particles
        :param common_match: the match used to get the group
        :param common_dict: dict to be populated
        :return: the populated common dict
        """
        common_dict[Pco2wAbcDataParticleKey.DCL_CONTROLLER_TIMESTAMP] = \
            common_match.group(DATE_TIME_GROUP_INDEX)

        unique_id = common_match.group(ID_GROUP_INDEX)
        common_dict[Pco2wAbcDataParticleKey.UNIQUE_ID] = int(unique_id, 16)

        rec_type = common_match.group(RECORD_TYPE_GROUP_INDEX)
        common_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = int(rec_type, 16)

        rec_time = common_match.group(RECORD_TIME_GROUP_INDEX)
        common_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = int(rec_time, 16)

        return common_dict

    @staticmethod
    def _populate_light_measurements(light_measurements, instrument_dict, dict_key):
        """
        Helper method for filling in the light measurements (either normal or blank)
        :param light_measurements: normal or blank light measurements
        :param instrument_dict: instrument_dict or instrument_blank_dict
        :param dict_key: either Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS or
                        Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS
        :return: dict filled in normal or blank light measurements
        """

        end_range = num_chars = 4
        last_light_measurement_start_index = len(light_measurements)-3
        for x in range(0, last_light_measurement_start_index, num_chars):
            light_measurement = light_measurements[x: end_range]
            log.trace("ascii-hex light_measurement: %s", light_measurement)
            # convert to int
            int_light_measurement = int(light_measurement, 16)

            # append int value to instrument_dict
            instrument_dict[dict_key].append(int_light_measurement)

            end_range += num_chars

        return instrument_dict[dict_key]

    @staticmethod
    def _calculate_passed_checksum(line, record_checksum):
        """
        Calculates the checksum of the argument ascii-hex string
        :param line: the record that will be stripped
        :record_checksum: the checksum from the record
        :return: the modulo integer checksum value of argument ascii-hex string
        """

        log.trace("_calculate_passed_checksum(): string_length is %s, record is %s",
                  len(line), line)
        checksum = 0

        # Strip off the leading DCL Controller Timestamp, * and ID characters of the log line (27 characters) and
        # Strip off the trailing Checksum characters and newline (3 characters)
        stripped_record = line[27:-3]
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
        if record_checksum == checksum % 256:
            passed_checksum = 1
        else:
            passed_checksum = 0

        return passed_checksum

    @staticmethod
    def _generate_internal_timestamp(record_dict):
        """
        Generates the internal timestamp from the given DCL Controller Timestamp.
        :param record_dict: dictionary containing the dcl controller timestamp str parameter
        :return: the internal timestamp
        """

        return float(dcl_controller_timestamp_to_ntp_time(
            record_dict[Pco2wAbcDataParticleKey.DCL_CONTROLLER_TIMESTAMP]))

    @staticmethod
    def _create_empty_metadata_dict():
        """
        Overrides method in Pco2wAbcParser.
        """

        # Load the dictionary from the base class
        metadata_dict = Pco2wAbcParser._create_empty_metadata_dict()

        # Load the DCL dictionary entries for this subclass
        dcl_dict = Pco2wAbcDclParser._create_dcl_extension_dict()

        # Update the dict from base class with the dictionary entries that are unique
        # to this subclass
        metadata_dict.update(dcl_dict)

        return metadata_dict

    @staticmethod
    def _create_empty_power_dict():
        """
        Overrides method in Pco2wAbcParser.
        """

        power_dict = Pco2wAbcParser._create_empty_power_dict()

        # Load the dictionary entries for this subclass
        dcl_dict = Pco2wAbcDclParser._create_dcl_extension_dict()

        # Update the dict from base class with the dictionary entries that are unique
        # to this subclass
        power_dict.update(dcl_dict)

        return power_dict

    @staticmethod
    def _create_empty_instrument_dict():
        """
        Overrides method in Pco2wAbcParser.
        """

        instrument_dict = Pco2wAbcParser._create_empty_instrument_dict()

        # Load the dictionary entries for this subclass
        dcl_dict = Pco2wAbcDclParser._create_dcl_extension_dict()

        # Update the dict from base class with the dictionary entries that are unique
        # to this subclass
        instrument_dict.update(dcl_dict)

        return instrument_dict

    @staticmethod
    def _create_empty_instrument_blank_dict():
        """
        Overrides method in Pco2wAbcParser.
        """

        instrument_dict = Pco2wAbcParser._create_empty_instrument_blank_dict()

        # Load the dictionary entries for this subclass
        dcl_dict = Pco2wAbcDclParser._create_dcl_extension_dict()

        # Update the dict from base class with the dictionary entries that are unique
        # to this subclass
        instrument_dict.update(dcl_dict)

        return instrument_dict

    def _populate_metadata_dict(self, metadata_match, metadata_dict, line):
        """
        Fields from the control record are used to populate
        the metadata dictionary.
        """

        common_dict = Pco2wAbcDclParser._populate_common_dict(metadata_match, metadata_dict)
        metadata_dict.update(common_dict)

        # Convert the flags group from ASCII-HEX to int
        ascii_hex_flags = metadata_match.group(FLAGS_GROUP_INDEX)
        int_flags = int(ascii_hex_flags, 16)

        # Convert the FLAGS integer value to a 16 bit binary value with leading 0s
        # as necessary
        bit_string = format(int(int_flags), '016b')

        bit_flag_params = [Pco2wAbcDataParticleKey.POWER_ON_INVALID,
                           Pco2wAbcDataParticleKey.FLASH_ERASED,
                           Pco2wAbcDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                           Pco2wAbcDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                           Pco2wAbcDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                           Pco2wAbcDataParticleKey.BATTERY_LOW_EXTERNAL,
                           Pco2wAbcDataParticleKey.BATTERY_LOW_BLANK,
                           Pco2wAbcDataParticleKey.BATTERY_LOW_MEASUREMENT,
                           Pco2wAbcDataParticleKey.BATTERY_LOW_PRESTART,
                           Pco2wAbcDataParticleKey.FLASH_MEMORY_OPEN,
                           Pco2wAbcDataParticleKey.DATA_DOWNLOAD_OK,
                           Pco2wAbcDataParticleKey.RECORD_END_ON_ERROR,
                           Pco2wAbcDataParticleKey.RECORD_MEMORY_FULL,
                           Pco2wAbcDataParticleKey.RECORD_END_ON_TIME,
                           Pco2wAbcDataParticleKey.RECORDING_ACTIVE,
                           Pco2wAbcDataParticleKey.CLOCK_ACTIVE]

        index = 0

        for bit in bit_string:

            metadata_dict[bit_flag_params[index]] = bit
            index += 1

        num_data_records = metadata_match.group(NUM_DATA_RECORDS_GROUP_INDEX)
        metadata_dict[Pco2wAbcDataParticleKey.NUM_DATA_RECORDS] = int(num_data_records, 16)

        num_error_records = metadata_match.group(NUM_ERROR_RECORDS_GROUP_INDEX)
        metadata_dict[Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS] = int(num_error_records, 16)

        num_bytes_stored = metadata_match.group(NUM_BYTES_STORED_GROUP_INDEX)
        metadata_dict[Pco2wAbcDataParticleKey.NUM_BYTES_STORED] = int(num_bytes_stored, 16)

        # Not all have battery_voltage
        if self._control_record_has_battery_voltage:
            battery_voltage = metadata_match.group(CONTROL_BATTERY_VOLTAGE_GROUP_INDEX)
            metadata_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = int(battery_voltage, 16)

        # Checksum will always be the last group
        passed_checksum = Pco2wAbcDclParser._calculate_passed_checksum(
            line, int(metadata_match.group(metadata_match.lastindex), 16))
        metadata_dict[Pco2wAbcDataParticleKey.PASSED_CHECKSUM] = passed_checksum

    @staticmethod
    def _populate_power_dict(power_match, power_dict, line):
        """
        Fields from the power record are used to populate
        the power dictionary.
        """

        common_dict = Pco2wAbcDclParser._populate_common_dict(power_match, power_dict)
        power_dict.update(common_dict)

        passed_checksum = Pco2wAbcDclParser._calculate_passed_checksum(
            line, int(power_match.group(power_match.lastindex), 16))
        power_dict[Pco2wAbcDataParticleKey.PASSED_CHECKSUM] = passed_checksum

    @staticmethod
    def _populate_instrument_dict(instrument_record_match, instrument_dict, line):
        """
        Fields from the CO2 (normal) record are used to populate
        the instrument dictionary.
        """

        common_dict = Pco2wAbcDclParser._populate_common_dict(instrument_record_match, instrument_dict)
        instrument_dict.update(common_dict)

        # Light measurements array
        light_measurements = instrument_record_match.group(LIGHT_MEASUREMENTS_GROUP_INDEX)
        log.trace("light_measurements: %s", light_measurements)
        instrument_dict[Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS] = []

        instrument_dict[Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS] \
            = Pco2wAbcDclParser._populate_light_measurements(light_measurements,
                                                             instrument_dict,
                                                             Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS)

        battery_voltage = instrument_record_match.group(CO2_BATTERY_VOLTAGE_GROUP_INDEX)
        instrument_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = int(battery_voltage, 16)

        raw_thermistor = instrument_record_match.group(THERMISTOR_GROUP_INDEX)
        instrument_dict[Pco2wAbcDataParticleKey.THERMISTOR_RAW] = int(raw_thermistor, 16)

        # Checksum will always be the last group
        passed_checksum = Pco2wAbcDclParser._calculate_passed_checksum(
            line,
            int(instrument_record_match.group(instrument_record_match.lastindex), 16))

        instrument_dict[Pco2wAbcDataParticleKey.PASSED_CHECKSUM] = passed_checksum

    @staticmethod
    def _populate_instrument_blank_dict(instrument_blank_record_match, instrument_blank_dict, line):
        """
        Fields from the CO2 (blank) record are used to populate
        the instrument blank dictionary.
        """

        common_dict = Pco2wAbcDclParser._populate_common_dict(instrument_blank_record_match, instrument_blank_dict)
        instrument_blank_dict.update(common_dict)

        # Blank light measurements array
        light_measurements = instrument_blank_record_match.group(LIGHT_MEASUREMENTS_GROUP_INDEX)
        instrument_blank_dict[Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS] = []

        instrument_blank_dict[Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS] \
            = Pco2wAbcDclParser._populate_light_measurements(light_measurements,
                                                             instrument_blank_dict,
                                                             Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS)

        battery_voltage = instrument_blank_record_match.group(CO2_BATTERY_VOLTAGE_GROUP_INDEX)
        instrument_blank_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = int(battery_voltage, 16)

        raw_thermistor = instrument_blank_record_match.group(THERMISTOR_GROUP_INDEX)
        instrument_blank_dict[Pco2wAbcDataParticleKey.THERMISTOR_RAW] = int(raw_thermistor, 16)

        # Checksum will always be the last group
        passed_checksum = Pco2wAbcDclParser._calculate_passed_checksum(
            line,
            int(instrument_blank_record_match.group(instrument_blank_record_match.lastindex), 16))
        instrument_blank_dict[Pco2wAbcDataParticleKey.PASSED_CHECKSUM] = passed_checksum

    def parse_file(self):
        """
        Overrides method in Pco2wAbcParser.
        """

        # Create some empty dictionaries which we will use to collect
        # data for the extract sample calls.
        metadata_dict = self._create_empty_metadata_dict()
        power_dict = self._create_empty_power_dict()
        instrument_dict = self._create_empty_instrument_dict()
        instrument_blank_dict = self._create_empty_instrument_blank_dict()

        line = self._stream_handle.readline()

        # Go through each line in the file
        while line:

            log.trace("line = %s", line)

            # Check for each of the different match possibilities
            metadata_with_battery_voltage_match = \
                METADATA_WITH_BATTERY_VOLTAGE_MATCHER.match(line)
            metadata_match = METADATA_MATCHER.match(line)
            power_match = POWER_MATCHER.match(line)
            instrument_match = INSTRUMENT_MATCHER.match(line)
            instrument_blank_match = INSTRUMENT_BLANK_MATCHER.match(line)
            dcl_logging_match = DCL_LOGGING_MATCHER.match(line)

            # There are two metadata match possibilities
            if metadata_with_battery_voltage_match or metadata_match:

                # If we found a metadata record with battery voltage,
                # supply the match
                if metadata_with_battery_voltage_match:

                    self._control_record_has_battery_voltage = True

                    log.debug("found control record with battery voltage, line: %s", line)
                    log.debug("control groups %s", metadata_with_battery_voltage_match.groups())

                    self._populate_metadata_dict(metadata_with_battery_voltage_match,
                                                 metadata_dict, line)

                else:
                    log.debug("found control record without battery voltage, line: %s", line)
                    log.debug("control groups %s", metadata_match.groups())
                    self._control_record_has_battery_voltage = False

                    # If we found a metadata record without battery voltage,
                    # supply that match
                    self._populate_metadata_dict(metadata_match, metadata_dict, line)

                particle = self._extract_sample(self._metadata_class,
                                                None,
                                                metadata_dict,
                                                Pco2wAbcDclParser._generate_internal_timestamp(metadata_dict))

                log.trace("Appending metadata particle: %s", particle.generate())
                self._record_buffer.append(particle)

                # Recreate an empty metadata dictionary
                metadata_dict = self._create_empty_metadata_dict()

            elif power_match:
                log.debug("Found power record, line: %s", line)
                log.debug("power groups %s", power_match.groups())
                self._populate_power_dict(power_match, power_dict, line)

                particle = self._extract_sample(self._power_class,
                                                None,
                                                power_dict,
                                                Pco2wAbcDclParser._generate_internal_timestamp(power_dict))

                log.trace("Appending power particle: %s", particle.generate())
                self._record_buffer.append(particle)

                # Recreate an empty power dictionary
                power_dict = self._create_empty_power_dict()

            elif instrument_match:
                log.debug("Found instrument record, line: %s", line)
                log.debug("instrument groups %s", instrument_match.groups())
                self._populate_instrument_dict(instrument_match, instrument_dict, line)

                particle = self._extract_sample(self._instrument_class,
                                                None,
                                                instrument_dict,
                                                Pco2wAbcDclParser._generate_internal_timestamp(instrument_dict))

                log.trace("Appending instrument particle: %s", particle.generate())
                self._record_buffer.append(particle)

                # Recreate an empty instrument dictionary
                instrument_dict = self._create_empty_instrument_dict()

            elif instrument_blank_match:
                log.debug("Found instrument blank record, line: %s", line)
                log.debug("instrument blank groups %s", instrument_blank_match.groups())
                self._populate_instrument_blank_dict(instrument_blank_match, instrument_blank_dict, line)

                particle = self._extract_sample(self._instrument_blank_class,
                                                None,
                                                instrument_blank_dict,
                                                Pco2wAbcDclParser._generate_internal_timestamp(instrument_blank_dict))

                log.trace("Appending instrument blank particle: %s", particle.generate())
                self._record_buffer.append(particle)

                # Recreate an empty instrument blank dictionary
                instrument_blank_dict = self._create_empty_instrument_blank_dict()

            elif dcl_logging_match:
                # Nothing to do, we ignore the DCL logging records.
                log.trace("DCL logging record, line: %s", line)

            else:
                # We found a line in the file that was unexpected.  This includes a record
                # with incorrect length. Report a RecoverableSampleException.
                log.error("Unexpected data in file, or num bytes does not match record_length, line: %s", line)
                message = "Unexpected data in file, or num bytes does not match record_length, line: " + line
                self._exception_callback(RecoverableSampleException(message))

            # Get the next line
            line = self._stream_handle.readline()

        # Provide the indication that the file was parsed
        self._file_parsed = True
