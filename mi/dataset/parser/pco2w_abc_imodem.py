__author__ = 'mworden'

import ntplib
import re

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

log = get_logger()
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcDataParticleKey, \
    Pco2wAbcParticleClassKey
from mi.dataset.parser.common_regexes import FLOAT_REGEX, ASCII_HEX_CHAR_REGEX
from mi.dataset.parser.utilities import formatted_timestamp_utc_time, \
    sum_hex_digits

"""
Example file contents:
#UIMM Status
#7370_DateTime: 20140804 010552
#ID=23
#SN=70001687
#Volts=8.80
#Records=1
#Length=466
#Events=0
#Begin UIMM Data
Record[305]:*C22704CF8F37A30044004C0D0F024307B001FB09C80E7D0044004B0D0E024207B201FB0C5B07E357
#End UIMM Data, 1 samples written
"""

DATA_TIME_REGEX = \
    r'#\w+_DateTime:\s+(?P<' + Pco2wAbcDataParticleKey.FILE_TIME + \
    '>\d{8}\s\d{6})'
ID_REGEX = r'#ID=(?P<' + Pco2wAbcDataParticleKey.INSTRUMENT_ID + '>\d+)'
SN_REGEX = r'#SN=(?P<' + Pco2wAbcDataParticleKey.SERIAL_NUMBER + '>\d+)'
VOLTS_REGEX = \
    r'#Volts=(?P<' + Pco2wAbcDataParticleKey.VOLTAGE_FLT32 + \
    '>' + FLOAT_REGEX + ')'
RECORDS_REGEX = \
    r'#Records=(?P<' + \
    Pco2wAbcDataParticleKey.NUM_DATA_RECORDS + '>\d+)'
LENGTH_REGEX = \
    r'#Length=(?P<' + \
    Pco2wAbcDataParticleKey.RECORD_LENGTH + '>\d+)'
EVENTS_REGEX = \
    r'#Events=(?P<' + \
    Pco2wAbcDataParticleKey.NUM_EVENTS + '>\d+)'
NUM_SAMPLES_REGEX = \
    r'#End UIMM Data,\s+(?P<' + \
    Pco2wAbcDataParticleKey.NUM_SAMPLES + '>\d+) samples written'

RECORD_DATA_PARAM = 'record_data_param'

# Example:
# Record[305]:*C22704CF8F37A30044004C0D0F024307B001FB09C80E7D0044004B0D0E024207B201FB0C5B07E357
RECORD_REGEX = r'(?:Record\[\d+\]:)?\*(?P<' + RECORD_DATA_PARAM + '>' + \
               ASCII_HEX_CHAR_REGEX + '{16,80})'

CO2_TYPE_NORMAL = 4
CO2_TYPE_BLANK = 5
CO2_INSTRUMENT_TYPE_VALUES = (CO2_TYPE_NORMAL, CO2_TYPE_BLANK)
CO2_POWER_TYPE_VALUES = (17, 33, 49)
CO2_CONTROL_START = 128

LEN_CONTROL_RECORD_WITH_VOLTAGE = 40


class Pco2wAbcImodemParser(SimpleParser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        """
        This is the constructor used to create an Pco2wAbcImodemParser.
        :param config: a configuration dictionary for the parser
        :param stream_handle: the handle to the file stream
        :param exception_callback: the function to call upon
        detection of an exception
        """

        super(Pco2wAbcImodemParser, self).__init__(config,
                                                   stream_handle,
                                                   exception_callback)

        self._record_buffer = []
        self._metadata_class = config[
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS]
        self._power_class = config[
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS]
        self._instrument_class = config[
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS]
        self._instrument_blank_class = config[
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS]
        self._control_class = config[
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                Pco2wAbcParticleClassKey.CONTROL_PARTICLE_CLASS]

        self._metadata_dict = {
            Pco2wAbcDataParticleKey.FILE_TIME: None,
            Pco2wAbcDataParticleKey.INSTRUMENT_ID: None,
            Pco2wAbcDataParticleKey.SERIAL_NUMBER: None,
            Pco2wAbcDataParticleKey.VOLTAGE_FLT32: None,
            Pco2wAbcDataParticleKey.NUM_DATA_RECORDS: None,
            Pco2wAbcDataParticleKey.RECORD_LENGTH: None,
            Pco2wAbcDataParticleKey.NUM_EVENTS: None,
            Pco2wAbcDataParticleKey.NUM_SAMPLES: None,
        }

    def _check_for_metadata_match(self, line):
        """
        This method checks a line in the file for match against metadata
        :param line: line read via file stream
        :return:True if match found, False if match not found
        """

        date_time_match = re.match(DATA_TIME_REGEX, line)
        if date_time_match is not None:

            date_time_match_dict = date_time_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.FILE_TIME] = \
                date_time_match_dict[Pco2wAbcDataParticleKey.FILE_TIME]

            return True

        record_id_match = re.match(ID_REGEX, line)
        if record_id_match is not None:

            record_id_match_dict = record_id_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.INSTRUMENT_ID] = \
                record_id_match_dict[Pco2wAbcDataParticleKey.INSTRUMENT_ID]

            return True

        sn_match = re.match(SN_REGEX, line)
        if sn_match is not None:

            sn_match_dict = sn_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.SERIAL_NUMBER] = \
                sn_match_dict[Pco2wAbcDataParticleKey.SERIAL_NUMBER]

            return True

        volts_match = re.match(VOLTS_REGEX, line)
        if volts_match is not None:

            volts_match_dict = volts_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.VOLTAGE_FLT32] = \
                volts_match_dict[Pco2wAbcDataParticleKey.VOLTAGE_FLT32]

            return True

        records_match = re.match(RECORDS_REGEX, line)
        if records_match is not None:

            records_match_dict = records_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.NUM_DATA_RECORDS] = \
                records_match_dict[Pco2wAbcDataParticleKey.NUM_DATA_RECORDS]

            return True

        length_match = re.match(LENGTH_REGEX, line)
        if length_match is not None:

            length_match_dict = length_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.RECORD_LENGTH] = \
                length_match_dict[Pco2wAbcDataParticleKey.RECORD_LENGTH]

            return True

        events_match = re.match(EVENTS_REGEX, line)
        if events_match is not None:

            events_match_dict = events_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.NUM_EVENTS] = \
                events_match_dict[Pco2wAbcDataParticleKey.NUM_EVENTS]

            return True

        num_samples_match = re.match(NUM_SAMPLES_REGEX, line)
        if num_samples_match is not None:

            num_samples_match_dict = num_samples_match.groupdict()

            self._metadata_dict[Pco2wAbcDataParticleKey.NUM_SAMPLES] = \
                num_samples_match_dict[Pco2wAbcDataParticleKey.NUM_SAMPLES]

            return True

        return False

    def _process_common_record_data(self,
                                    record_id,
                                    record_type,
                                    record_timestamp,
                                    len_hex_data,
                                    record_data,
                                    data_dict):
        """
        This method performs processing common to most pco2w_abc_imodem data records
        :param record_id: The record ID
        :param record_type: The type of record
        :param record_timestamp: The timestamp for the record
        :param len_hex_data: The length of the ascii hex data to process
        :param record_data: The ascii hex record data
        :param data_dict: A dictionary to populate with common pco2w_abc_imodem
        particle key value pairs
        :return: True if the record data was found to have good data.  False if the
         record data was found to have invalid data
        """

        is_valid = True

        actual_len_record_data = len(record_data)
        expected_len_record_data = len_hex_data*2

        if actual_len_record_data != expected_len_record_data:
            is_valid = False
            message = "Length of record data does not match expected.  " + \
                      "Actual: %d, Expected: %d" % (actual_len_record_data,
                                                    expected_len_record_data)
            log.warn(message)
            self._exception_callback(RecoverableSampleException(message))

        data_dict[Pco2wAbcDataParticleKey.UNIQUE_ID] = record_id
        data_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = record_type
        data_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = record_timestamp

        expected_low_byte_checksum = record_data[expected_len_record_data-2:]

        hex_val = sum_hex_digits(record_data[:expected_len_record_data-2])

        # Convert the hex result to uppercase and grab the last byte of hex,
        # which is the low byte of the checksum
        actual_low_byte_checksum = str(hex_val).upper()[-2:]

        if actual_low_byte_checksum == expected_low_byte_checksum:
            data_dict[Pco2wAbcDataParticleKey.PASSED_CHECKSUM] = True
        else:
            data_dict[Pco2wAbcDataParticleKey.PASSED_CHECKSUM] = False
            log.debug("Low Byte Checksums expected %s, actual %s",
                      expected_low_byte_checksum,
                      actual_low_byte_checksum)

        return is_valid

    def _process_instrument_data(self,
                                 record_id,
                                 record_type,
                                 record_timestamp,
                                 len_hex_data,
                                 record_data):
        """
        This method process an instrument record
        :param record_id: The record ID
        :param record_type: The type of record
        :param record_timestamp: The timestamp for the record
        :param len_hex_data: The length of the ascii hex data to process
        :param record_data: The ascii hex record data
        :return: None
        """

        instrument_data_dict = dict()

        if self._process_common_record_data(record_id,
                                            record_type,
                                            record_timestamp,
                                            len_hex_data,
                                            record_data,
                                            instrument_data_dict):
            light_measurements = []

            """
            The offset into the light measurements is 12 ASCII HEX characters into
            record buffer.
            Light measurements consist of 14 instances of 4 ASCII HEX characters
            """
            offset = 12
            num_light_measurements = 14
            num_ascii_hex_chars = 4
            for index in range(offset,
                               offset+(num_light_measurements*num_ascii_hex_chars),
                               num_ascii_hex_chars):
                light_measurements.append(
                    int(record_data[index:index+num_ascii_hex_chars], 16))

            offset += num_light_measurements*num_ascii_hex_chars

            instrument_data_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
                int(record_data[offset:offset+4], 16)

            offset += 4
            instrument_data_dict[Pco2wAbcDataParticleKey.THERMISTOR_RAW] = \
                int(record_data[offset:offset+4], 16)

            if record_type == CO2_TYPE_NORMAL:
                instrument_data_dict[
                    Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS] = \
                    light_measurements
                particle = self._extract_sample(self._instrument_class,
                                                None,
                                                instrument_data_dict,
                                                None)
            else:
                instrument_data_dict[
                    Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS] = \
                    light_measurements
                particle = self._extract_sample(self._instrument_blank_class,
                                                None,
                                                instrument_data_dict,
                                                None)

            self._record_buffer.append(particle)

    def _process_power_data(self,
                            record_id,
                            record_type,
                            record_timestamp,
                            len_hex_data,
                            record_data):
        """
        This method process a power record
        :param record_id: The record ID
        :param record_type: The type of record
        :param record_timestamp: The timestamp for the record
        :param len_hex_data: The length of the ascii hex data to process
        :param record_data: The ascii hex record data
        :return: None
        """

        power_data_dict = dict()

        if self._process_common_record_data(record_id,
                                            record_type,
                                            record_timestamp,
                                            len_hex_data,
                                            record_data,
                                            power_data_dict):

            particle = self._extract_sample(self._power_class,
                                            None,
                                            power_data_dict,
                                            None)

            self._record_buffer.append(particle)

    def _process_control_data(self,
                              record_id,
                              record_type,
                              record_timestamp,
                              len_hex_data,
                              record_data):
        """
        This method process a control record
        :param record_id: The record ID
        :param record_type: The type of record
        :param record_timestamp: The timestamp for the record
        :param len_hex_data: The length of the ascii hex data to process
        :param record_data: The ascii hex record data
        :return: None
        """

        control_data_dict = dict()

        len_record_data = len(record_data)

        if self._process_common_record_data(record_id,
                                            record_type,
                                            record_timestamp,
                                            len_hex_data,
                                            record_data,
                                            control_data_dict):

            # Convert the ascii hex to an integer, format the result into a
            # binary string and zero fill up to 16 bits to ensure having a
            # 16 bit string
            bit_string = format(int(record_data[12:16], 16), 'b').zfill(16)

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
                control_data_dict[bit_flag_params[index]] = bit
                index += 1

            control_data_dict[Pco2wAbcDataParticleKey.NUM_DATA_RECORDS] = \
                int(record_data[16:22], 16)

            control_data_dict[Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS] = \
                int(record_data[22:28], 16)

            control_data_dict[Pco2wAbcDataParticleKey.NUM_BYTES_STORED] = \
                int(record_data[28:34], 16)

            # The voltage battery is optional
            if len_record_data == LEN_CONTROL_RECORD_WITH_VOLTAGE:
                control_data_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
                    int(record_data[34:38], 16)
            else:
                control_data_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
                    None

            particle = self._extract_sample(self._control_class,
                                            None,
                                            control_data_dict,
                                            None)

            self._record_buffer.append(particle)

    def _process_record_data(self, record_data):
        """
        This method processes record data calling additional methods to handle
        specific logic applicable to the record type.
        :param record_data: The record data to process
        :return: None
        """

        record_id = int(record_data[0:2], 16)

        len_hex_data = int(record_data[2:4], 16)

        record_type = int(record_data[4:6], 16)

        record_timestamp = int(record_data[6:14], 16)

        if record_type in CO2_INSTRUMENT_TYPE_VALUES:

            self._process_instrument_data(record_id,
                                          record_type,
                                          record_timestamp,
                                          len_hex_data,
                                          record_data[2:])

        elif record_type in CO2_POWER_TYPE_VALUES:

            self._process_power_data(record_id,
                                     record_type,
                                     record_timestamp,
                                     len_hex_data,
                                     record_data[2:])

        elif record_type >= CO2_CONTROL_START:

            self._process_control_data(record_id,
                                       record_type,
                                       record_timestamp,
                                       len_hex_data,
                                       record_data[2:])

        else:
            message = "Invalid record type: %s" % record_type
            log.warn(message)
            self._exception_callback(RecoverableSampleException(message))

    def _process_line(self, line):
        """
        This method processes a single line in the file.
        :param line: A line read from the file stream
        :return: None
        """

        if not self._check_for_metadata_match(line):

            record_match = re.match(RECORD_REGEX, line)

            if record_match:

                self._process_record_data(
                    record_match.groupdict().get(RECORD_DATA_PARAM))

    def _process_metadata_dict(self):
        """
        This method process a full metadata dictionary which results in
        the generation of a metadata particle
        :return: None
        """

        file_time = self._metadata_dict[Pco2wAbcDataParticleKey.FILE_TIME]

        if file_time is None:
            message = "Unable to create metadata particle due to missing file time"
            log.warn(message)
            self._exception_callback(RecoverableSampleException(message))

        else:
            utc_timestamp = formatted_timestamp_utc_time(file_time,
                                                         "%Y%m%d %H%M%S")

            ntp_timestamp = float(ntplib.system_to_ntp_time(utc_timestamp))

            particle = self._extract_sample(
                self._metadata_class, None,
                self._metadata_dict, ntp_timestamp)

            self._record_buffer.append(particle)

    def parse_file(self):
        """
        This method parses the contents of the file
        :return: None
        """

        line = self._stream_handle.readline()

        # Go through each line in the file
        while line:

            log.trace("Line: %s", line)

            self._process_line(line)

            line = self._stream_handle.readline()

        self._process_metadata_dict()
