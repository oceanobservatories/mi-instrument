#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/phsen_abdcef_sio.py
@author Emily Hahn
@brief Parser for the phsen abcdef sio dataset driver
Release notes:

initial release
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re
import binascii

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import SampleException, RecoverableSampleException, UnexpectedDataException
from mi.dataset.parser.common_regexes import ASCII_HEX_CHAR_REGEX
from mi.dataset.parser.sio_mule_common import SioParser, SIO_HEADER_MATCHER, SIO_BLOCK_END

# match the ascii hex ph records
# the data should be ascii hex, but may have non hex ascii characters, if this happens the
# value will be set to none
DATA_REGEX = b'(\^0A\r\*)(' + ASCII_HEX_CHAR_REGEX + '{4}0A)([\x00-\xFF]{8})([\x00-\xFF]{446}' + \
             ASCII_HEX_CHAR_REGEX + '{4})\r'
DATA_MATCHER = re.compile(DATA_REGEX)

# match the ascii hex control record, there is an optional 2 byte field at the end
# this also allows for non hex ascii characters in the timestamp, flags and number of records
CONTROL_REGEX = b'(\*)(' + ASCII_HEX_CHAR_REGEX + '{4}[8-9A-Fa-f]' + ASCII_HEX_CHAR_REGEX + \
                ')([\x00-\xFF]{32}' + ASCII_HEX_CHAR_REGEX + '{0,4})\r'
CONTROL_MATCHER = re.compile(CONTROL_REGEX)

# control messages are hex 80 or greater, so the first ascii char must be greater than 8 hex
CONTROL_ID_REGEX = b'[8-9A-Fa-f]' + ASCII_HEX_CHAR_REGEX
CONTROL_ID_MATCHER = re.compile(CONTROL_ID_REGEX)

TIMESTAMP_REGEX = ASCII_HEX_CHAR_REGEX + b'{8}'
TIMESTAMP_MATCHER = re.compile(TIMESTAMP_REGEX)

HEX_INT_REGEX = ASCII_HEX_CHAR_REGEX + b'{4}'
HEX_INT_MATCHER = re.compile(HEX_INT_REGEX)

# this occurs frequently at the end of ph messages, don't send an exception for this case
PH_EXTRA_END = b'?03\r'

PH_ID = '0A'
# the control message has an optional data or battery field for some control IDs
DATA_CONTROL_IDS = ['BF', 'FF']
BATT_CONTROL_IDS = ['C0', 'C1']

SIO_HEADER_BYTES = 33
NORMAL_CONTROL_LEN = 40
OPTIONAL_CONTROL_LEN = 44
MEASUREMENT_BYTES = 4


class DataParticleType(BaseEnum):
    SAMPLE = 'phsen_abcdef_sio_mule_instrument'
    CONTROL = 'phsen_abcdef_sio_mule_metadata'


class PhsenAbcdefSioCommonDataParticleKey(BaseEnum):
    CONTROLLER_TIMESTAMP = 'sio_controller_timestamp'
    UNIQUE_ID = 'unique_id'
    RECORD_TYPE = 'record_type'
    RECORD_TIME = 'record_time'
    PASSED_CHECKSUM = 'passed_checksum'


class PhsenAbcdefSioDataParticleKey(PhsenAbcdefSioCommonDataParticleKey):
    THERMISTOR_START = 'thermistor_start'
    REFERENCE_LIGHT_MEASUREMENTS = 'reference_light_measurements'
    LIGHT_MEASUREMENTS = 'light_measurements'
    VOLTAGE_BATTERY = 'voltage_battery'
    THERMISTOR_END = 'thermistor_end'


def encode_int_16(val_str):
    """
    Encode a hex string into an int
    @param val_str string containing hex value
    """
    return int(val_str, 16)


def encode_timestamp(timestamp_str):
    """
    Encode a hex value into an int if it matches the timestamp
    @param timestamp_str string containing hex timestamp value
    """
    timestamp_match = TIMESTAMP_MATCHER.match(timestamp_str)
    if not timestamp_match:
        return None
    else:
        return int(timestamp_str, 16)


class PhsenAbcdefSioDataParticle(DataParticle):
    """
    Class for parsing data from the mflm_phsen instrument
    """

    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        # raw data comes in as a tuple
        ts = self.raw_data[0]
        data_match = self.raw_data[1]

        # use the timestamp from the sio header as internal timestamp
        sec_since_1970 = int(ts, 16)
        self.set_internal_timestamp(unix_time=sec_since_1970)

        ref_meas = []
        previous_record_bytes = 4
        # 4 sets of 4 reference light measurements (16 total)
        for i in range(0, 16):
            start_idx = previous_record_bytes + i * MEASUREMENT_BYTES
            # confirm this contains only ascii hex chars
            if HEX_INT_MATCHER.match(data_match.group(4)[start_idx:start_idx + MEASUREMENT_BYTES]):
                this_ref = int(data_match.group(4)[start_idx:start_idx + MEASUREMENT_BYTES], 16)
                ref_meas.append(this_ref)
            else:
                # don't send an exception if a non ascii hex char is in this value
                ref_meas.append(None)

        light_meas = []
        n_outer_sets = 23
        n_inner_sets = 4
        previous_record_bytes = 68
        # 23 sets of 4 light measurements
        for i in range(0, n_outer_sets):
            for s in range(0, n_inner_sets):
                start_idx = previous_record_bytes + i * n_inner_sets * MEASUREMENT_BYTES + s * MEASUREMENT_BYTES
                # confirm this contains only ascii hex chars
                if HEX_INT_MATCHER.match(data_match.group(4)[start_idx:start_idx + MEASUREMENT_BYTES]):
                    this_meas = int(data_match.group(4)[start_idx:start_idx + MEASUREMENT_BYTES], 16)
                    light_meas.append(this_meas)
                else:
                    # don't send an exception if a non ascii hex char is in this value
                    light_meas.append(None)

        # calculate the checksum and compare with the received checksum
        passed_checksum = True
        try:
            chksum = int(data_match.group(0)[-3:-1], 16)
            sum_bytes = 0
            for i in range(7, 467, 2):
                sum_bytes += int(data_match.group(0)[i:i + 2], 16)
            calc_chksum = sum_bytes & 255
            if calc_chksum != chksum:
                passed_checksum = False
                log.debug('Calculated internal checksum %d does not match received %d', calc_chksum, chksum)
        except Exception as e:
            log.debug('Error calculating checksums: %s, setting passed checksum to False', e)
            passed_checksum = False

        result = [self._encode_value(PhsenAbcdefSioDataParticleKey.CONTROLLER_TIMESTAMP, ts, encode_int_16),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.UNIQUE_ID, data_match.group(2)[0:2], encode_int_16),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.RECORD_TYPE, data_match.group(2)[4:6],
                                     encode_int_16),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.RECORD_TIME, data_match.group(3), encode_timestamp),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.THERMISTOR_START, data_match.group(4)[0:4],
                                     encode_int_16),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS, ref_meas, list),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.LIGHT_MEASUREMENTS, light_meas, list),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.VOLTAGE_BATTERY, data_match.group(0)[-11:-7],
                                     encode_int_16),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.THERMISTOR_END, data_match.group(0)[-7:-3],
                                     encode_int_16),
                  self._encode_value(PhsenAbcdefSioDataParticleKey.PASSED_CHECKSUM, passed_checksum, int)]
        return result


class PhsenControlDataParticleKey(PhsenAbcdefSioCommonDataParticleKey):
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


class PhsenAbcdefSioControlDataParticle(DataParticle):
    """
    Class for parsing data from the mflm_phsen instrument
    """

    _data_particle_type = DataParticleType.CONTROL

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        # raw data is a tuple made of timestamp and the data match
        ts = self.raw_data[0]
        data_match = self.raw_data[1]

        # use the timestamp from the sio header as internal timestamp
        sec_since_1970 = int(ts, 16)
        self.set_internal_timestamp(unix_time=sec_since_1970)

        data_len = len(data_match.group(0))
        control_id = data_match.group(2)[4:6]
        if control_id in DATA_CONTROL_IDS or control_id in BATT_CONTROL_IDS:
            if data_len != OPTIONAL_CONTROL_LEN:
                raise RecoverableSampleException("PhsenControlDataParticle: for id %s size %d does not match %d" %
                                                 (control_id, data_len, OPTIONAL_CONTROL_LEN))
        elif data_len != NORMAL_CONTROL_LEN:
            raise RecoverableSampleException("PhsenControlDataParticle: for id %s size %d does not match %d" %
                                             (control_id, data_len, NORMAL_CONTROL_LEN))

        # calculate the checksum and compare with the received checksum
        passed_checksum = True
        try:
            chksum = int(data_match.group(0)[-3:-1], 16)
            sum_bytes = 0
            # subtract the 3 bytes for the '*' and unique ID, 2 for the checksum, and 1 for the last \r
            control_len = data_len - 6
            for i in range(3, control_len, 2):
                sum_bytes += int(data_match.group(0)[i:i + 2], 16)
            calc_chksum = sum_bytes & 255
            if calc_chksum != chksum:
                passed_checksum = False
                log.debug('Calculated internal checksum %d does not match received %d', calc_chksum, chksum)
        except Exception as e:
            log.debug('Error calculating checksums: %s, setting passed checksum to False', e)
            passed_checksum = False

        # turn the flag value from a hex-ascii value into a string of binary values
        try:
            flags = bin(int(data_match.group(3)[8:12], 16))[2:].zfill(16)
            valid_flags = True
        except ValueError:
            valid_flags = False

        result = [
            self._encode_value(PhsenControlDataParticleKey.CONTROLLER_TIMESTAMP, ts, encode_int_16),
            self._encode_value(PhsenControlDataParticleKey.UNIQUE_ID, data_match.group(2)[0:2], encode_int_16),
            self._encode_value(PhsenControlDataParticleKey.RECORD_TYPE, control_id, encode_int_16),
            self._encode_value(PhsenControlDataParticleKey.RECORD_TIME, data_match.group(3)[0:8], encode_timestamp)]

        # if the flag is valid, fill in the values, otherwise set to None
        if valid_flags:
            result.extend([
                self._encode_value(PhsenControlDataParticleKey.CLOCK_ACTIVE, flags[0], int),
                self._encode_value(PhsenControlDataParticleKey.RECORDING_ACTIVE, flags[1], int),
                self._encode_value(PhsenControlDataParticleKey.RECORD_END_ON_TIME, flags[2], int),
                self._encode_value(PhsenControlDataParticleKey.RECORD_MEMORY_FULL, flags[3], int),
                self._encode_value(PhsenControlDataParticleKey.RECORD_END_ON_ERROR, flags[4], int),
                self._encode_value(PhsenControlDataParticleKey.DATA_DOWNLOAD_OK, flags[5], int),
                self._encode_value(PhsenControlDataParticleKey.FLASH_MEMORY_OPEN, flags[6], int),
                self._encode_value(PhsenControlDataParticleKey.BATTERY_LOW_PRESTART, flags[7], int),
                self._encode_value(PhsenControlDataParticleKey.BATTERY_LOW_MEASUREMENT, flags[8], int),
                self._encode_value(PhsenControlDataParticleKey.BATTERY_LOW_BLANK, flags[9], int),
                self._encode_value(PhsenControlDataParticleKey.BATTERY_LOW_EXTERNAL, flags[10], int),
                self._encode_value(PhsenControlDataParticleKey.EXTERNAL_DEVICE1_FAULT, flags[11], int),
                self._encode_value(PhsenControlDataParticleKey.EXTERNAL_DEVICE2_FAULT, flags[12], int),
                self._encode_value(PhsenControlDataParticleKey.EXTERNAL_DEVICE3_FAULT, flags[13], int),
                self._encode_value(PhsenControlDataParticleKey.FLASH_ERASED, flags[14], int),
                self._encode_value(PhsenControlDataParticleKey.POWER_ON_INVALID, flags[15], int)])
        else:
            result.extend([
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.CLOCK_ACTIVE, DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.RECORDING_ACTIVE, DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.RECORD_END_ON_TIME,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.RECORD_MEMORY_FULL,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.RECORD_END_ON_ERROR,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.DATA_DOWNLOAD_OK, DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.FLASH_MEMORY_OPEN, DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.BATTERY_LOW_PRESTART,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.BATTERY_LOW_MEASUREMENT,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.BATTERY_LOW_BLANK, DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.BATTERY_LOW_EXTERNAL,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                 DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.FLASH_ERASED, DataParticleKey.VALUE: None},
                {DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.POWER_ON_INVALID, DataParticleKey.VALUE: None}])
        # these 3 may also have invalid hex values, allow for none when encoding
        # so exceptions are not thrown here
        result.extend([
            self._encode_value(PhsenControlDataParticleKey.NUM_DATA_RECORDS, data_match.group(3)[12:18],
                               PhsenAbcdefSioControlDataParticle.encode_int_16_or_none),
            self._encode_value(PhsenControlDataParticleKey.NUM_ERROR_RECORDS, data_match.group(3)[18:24],
                               PhsenAbcdefSioControlDataParticle.encode_int_16_or_none),
            self._encode_value(PhsenControlDataParticleKey.NUM_BYTES_STORED, data_match.group(3)[24:30],
                               PhsenAbcdefSioControlDataParticle.encode_int_16_or_none)])

        if control_id in BATT_CONTROL_IDS and HEX_INT_MATCHER.match(data_match.group(3)[30:34]):
            result.append(self._encode_value(PhsenControlDataParticleKey.VOLTAGE_BATTERY,
                                             data_match.group(3)[30:34], encode_int_16))
        else:
            result.append({DataParticleKey.VALUE_ID: PhsenControlDataParticleKey.VOLTAGE_BATTERY,
                           DataParticleKey.VALUE: None})
        result.append(self._encode_value(PhsenControlDataParticleKey.PASSED_CHECKSUM, passed_checksum, int))
        return result

    @staticmethod
    def encode_int_16_or_none(int_val):
        """
        Use to convert from hex-ascii to int when encoding data particle values,
        but it is not an error to not match, return None without failing encoding
        """
        result = None
        try:
            result = int(int_val, 16)
        except ValueError:
            # the result will stay at None if we fail the encoding, and no exception
            pass
        return result


class PhsenAbcdefSioParser(SioParser):

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        result_particles = []

        # non-data does not need to be handled here because for the single file
        # the data may be corrected and re-written later, it is just ignored until it matches
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()
        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:
            header_match = SIO_HEADER_MATCHER.match(chunk)

            if header_match.group(1) == 'PH':
                # start after the sio header
                index = header_match.end(0)
                last_index = index
                chunk_len = len(chunk)
                while index < chunk_len:
                    data_match = DATA_MATCHER.match(chunk[index:])
                    control_match = CONTROL_MATCHER.match(chunk[index:])
                    # check for any valid match and make sure no extra data was found between valid matches
                    if data_match or control_match or chunk[index] == SIO_BLOCK_END:
                        # if the indices don't match we have data that doesn't match
                        # exclude the expected possible ph end bytes
                        if last_index != index and chunk[last_index:index] != PH_EXTRA_END:
                            # we found bad data, send a sample exception but keep processing the file
                            msg = "unknown data found in chunk %s from %d to %d" % (chunk[1:32], last_index, index)
                            log.warning(msg)
                            self._exception_callback(SampleException(msg))
                            # stop processing this sio block, it is bad
                            break

                    if data_match:
                        log.debug('Found data match in chunk %s at index %d', chunk[1:32], index)
                        # particle-ize the data block received, return the record
                        sample = self._extract_sample(PhsenAbcdefSioDataParticle, None,
                                                      (header_match.group(3), data_match), None)
                        result_particles.append(sample)

                        index += len(data_match.group(0))
                        last_index = index
                    elif control_match:
                        log.debug('Found control match in chunk %s at index %d', chunk[1:32], index)
                        # particle-ize the data block received, return the record
                        sample = self._extract_sample(PhsenAbcdefSioControlDataParticle, None,
                                                      (header_match.group(3), control_match), None)
                        result_particles.append(sample)

                        index += len(control_match.group(0))
                        last_index = index
                    elif chunk[index] == SIO_BLOCK_END:
                        # found end of sio block marker, we are done with this chunk
                        break
                    else:
                        # we found extra data, warn on chunks of extra data not each byte
                        index += 1

            else:
                # we found unexpected sio instrument id
                msg = "Unexpected sio instrument header ID %s" % header_match.group(1)
                log.warning(msg)
                self._exception_callback(UnexpectedDataException(msg))

            # non-data does not need to be handled here because for the single file
            # the data may be corrected and re-written later, it is just ignored until it matches
            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()
            self.handle_non_data(non_data, non_end, start)

        return result_particles

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data found in the file
        :param non_data: Non data
        :param non_end: end index of non data
        :param start: start index of data
        """
        if non_data is not None and non_end <= start:
            message = "Found %d bytes of unexpected non-data %s" % (len(non_data), binascii.b2a_hex(non_data))
            log.warn(message)
            self._exception_callback(UnexpectedDataException(message))
