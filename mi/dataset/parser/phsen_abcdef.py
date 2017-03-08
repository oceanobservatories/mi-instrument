#!/usr/bin/env python

"""
@package mi.dataset.parser.phsen_abcdef
@file marine-integrations/mi/dataset/parser/phsen_abcdef.py
@author Joseph Padula
@brief Parser for the phsen_abcdef recovered dataset driver
Release notes:

initial release
"""

from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.core.exceptions import SampleException
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.utilities import mac_timestamp_to_utc_timestamp
log = get_logger()


__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'


START_OF_DATA = ':Data'

# This is an example of the input string for a PH record
#   10 -> Type is pH
#   3456975600 -> Time = 2013-07-18 7:00:00
#   2276 -> Starting Thermistor
#
#   Reference Light measurements
#   2955 2002 2436 2495
#   2962 1998 2440 2492
#   2960 2001 2440 2494
#   2964 2002 2444 2496

#   Light measurements
#   2962 2004 2438 2496
#   2960 2002 2437 2494
#   ...
#   2962 1585 2439 2171
#   0 -> Not Used
#   2857 -> Battery
#   2297 -> End Thermistor
#


PH_REFERENCE_MEASUREMENTS = 16
PH_LIGHT_MEASUREMENTS = 92
PH_FIELDS = 114
PH_TYPE = 10  # PH Records identifier

# Control type consists of Info (128 - 191) and Error (192 - 255). Some of these are excluded
# as they are a different length and are in BATTERY_CONTROL_TYPE and DATA_CONTROL_TYPE
CONTROL_TYPE = [128, 129, 131, 133, 134, 135, 190, 194, 195, 196, 197, 198, 254]
# Control msg with battery voltage field or extra data field
BATTERY_DATA_CONTROL_TYPE = [191, 192, 193, 255]

# Number of fields in the other control messages (not including battery or data type)
CONTROL_FIELDS = 6

# Control msg with battery or data field
BATTERY_DATA_CONTROL_FIELDS = 7

TIMESTAMP_FIELD = 1


class DataParticleType(BaseEnum):
    INSTRUMENT = 'phsen_abcdef_instrument'
    METADATA = 'phsen_abcdef_metadata'


class PhsenRecoveredDataParticleKey(BaseEnum):
    RECORD_TYPE = 'record_type'
    RECORD_TIME = 'record_time'
    VOLTAGE_BATTERY = 'voltage_battery'


class PhsenRecoveredInstrumentDataParticleKey(PhsenRecoveredDataParticleKey):
    THERMISTOR_START = 'thermistor_start'
    REFERENCE_LIGHT_MEASUREMENTS = 'reference_light_measurements'
    LIGHT_MEASUREMENTS = 'light_measurements'
    THERMISTOR_END = 'thermistor_end'


class PhsenRecoveredInstrumentDataParticle(DataParticle):
    """
    Class for parsing data from the phsen_abcdef ph data set
    """

    _data_particle_type = DataParticleType.INSTRUMENT

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(PhsenRecoveredInstrumentDataParticle, self).__init__(raw_data,
                                                                   port_timestamp,
                                                                   internal_timestamp,
                                                                   preferred_timestamp,
                                                                   quality_flag,
                                                                   new_sequence)

        # use the timestamp from the sio header as internal timestamp
        sec_since_1904 = int(self.raw_data[TIMESTAMP_FIELD])

        unix_time = mac_timestamp_to_utc_timestamp(sec_since_1904)
        self.set_internal_timestamp(unix_time=unix_time)

    def _build_parsed_values(self):
        """
        Take a record in the ph data format and turn it into
        a particle with the instrument tag.
        @throws SampleException If there is a problem with sample creation
        """
        record_type = self.raw_data[0]

        record_time = self.raw_data[1]

        starting_thermistor = self.raw_data[2]

        offset = 3
        reference_measurements = self.raw_data[offset:PH_REFERENCE_MEASUREMENTS + offset]

        offset += PH_REFERENCE_MEASUREMENTS
        light_measurements = self.raw_data[offset: PH_LIGHT_MEASUREMENTS + offset]

        battery_voltage, end_thermistor = self.raw_data[-2:]

        result = [self._encode_value(PhsenRecoveredDataParticleKey.RECORD_TYPE, record_type, int),
                  self._encode_value(PhsenRecoveredDataParticleKey.RECORD_TIME, record_time, int),
                  self._encode_value(PhsenRecoveredInstrumentDataParticleKey.THERMISTOR_START, starting_thermistor,
                                     int),
                  self._encode_value(PhsenRecoveredInstrumentDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS,
                                     reference_measurements, lambda x: [int(y) for y in x]),
                  self._encode_value(PhsenRecoveredInstrumentDataParticleKey.LIGHT_MEASUREMENTS, light_measurements,
                                     lambda x: [int(y) for y in x]),
                  self._encode_value(PhsenRecoveredDataParticleKey.VOLTAGE_BATTERY, battery_voltage, int),
                  self._encode_value(PhsenRecoveredInstrumentDataParticleKey.THERMISTOR_END, end_thermistor, int)]
        return result


class PhsenRecoveredMetadataDataParticleKey(PhsenRecoveredDataParticleKey):
    CLOCK_ACTIVE = 'clock_active'
    RECORDING_ACTIVE = 'recording_active'
    RECORD_END_TIME = 'record_end_on_time'
    RECORD_MEMORY_FULL = 'record_memory_full'
    RECORD_END_ON_ERROR = 'record_end_on_error'
    DATA_DOWNLOAD_OK = 'data_download_ok'
    FLASH_MEMORY_OPEN = 'flash_memory_open'
    BATTERY_LOW_PRESENT = 'battery_low_prestart'
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


class PhsenRecoveredMetadataDataParticle(DataParticle):
    """
    Class for parsing data from the phsen_abcdef control data set
    """

    _data_particle_type = DataParticleType.METADATA

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(PhsenRecoveredMetadataDataParticle, self).__init__(raw_data,
                                                                 port_timestamp,
                                                                 internal_timestamp,
                                                                 preferred_timestamp,
                                                                 quality_flag,
                                                                 new_sequence)

        # use the timestamp from the sio header as internal timestamp
        sec_since_1904 = int(self.raw_data[TIMESTAMP_FIELD])

        unix_time = mac_timestamp_to_utc_timestamp(sec_since_1904)
        self.set_internal_timestamp(unix_time=unix_time)

    def _build_parsed_values(self):
        """
        Take a record in the control data format and turn it into
        a particle with the metadata tag.
        @throws SampleException If there is a problem with sample creation
        """

        record_type, record_time, flags, num_data_records, num_error_records, num_bytes_stored = self.raw_data[:6]
        flags = int(flags)
        record_type = int(record_type)

        result = [self._encode_value(PhsenRecoveredDataParticleKey.RECORD_TYPE, record_type, int),
                  self._encode_value(PhsenRecoveredDataParticleKey.RECORD_TIME, record_time, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.CLOCK_ACTIVE, flags & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.RECORDING_ACTIVE, (flags >> 1) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.RECORD_END_TIME, (flags >> 2) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.RECORD_MEMORY_FULL, (flags >> 3) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.RECORD_END_ON_ERROR, (flags >> 4) & 0x1,
                                     int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.DATA_DOWNLOAD_OK, (flags >> 5) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.FLASH_MEMORY_OPEN, (flags >> 6) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.BATTERY_LOW_PRESENT, (flags >> 7) & 0x1,
                                     int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.BATTERY_LOW_MEASUREMENT, (flags >> 8) & 0x1,
                                     int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.BATTERY_LOW_BLANK, (flags >> 9) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.BATTERY_LOW_EXTERNAL, (flags >> 10) & 0x1,
                                     int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.EXTERNAL_DEVICE1_FAULT, (flags >> 11) & 0x1,
                                     int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.EXTERNAL_DEVICE2_FAULT, (flags >> 12) & 0x1,
                                     int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.EXTERNAL_DEVICE3_FAULT, (flags >> 13) & 0x1,
                                     int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.FLASH_ERASED, (flags >> 14) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.POWER_ON_INVALID, (flags >> 15) & 0x1, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.NUM_DATA_RECORDS, num_data_records, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.NUM_ERROR_RECORDS, num_error_records, int),
                  self._encode_value(PhsenRecoveredMetadataDataParticleKey.NUM_BYTES_STORED, num_bytes_stored, int)]

        if record_type in BATTERY_DATA_CONTROL_TYPE:
            battery_voltage = self.raw_data[BATTERY_DATA_CONTROL_FIELDS - 1]
            temp_result = self._encode_value(PhsenRecoveredDataParticleKey.VOLTAGE_BATTERY, battery_voltage, int)
            result.append(temp_result)
        else:
            # This will handle anything after NUM_ERROR_RECORDS, including data in type 191 and 255.
            result.append({DataParticleKey.VALUE_ID: PhsenRecoveredDataParticleKey.VOLTAGE_BATTERY,
                           DataParticleKey.VALUE: None})
        return result


class PhsenRecoveredParser(SimpleParser):

    def parse_file(self):
        """
        Parse data file line by line. If the line
        it is a valid data piece, build a particle, append to buffer
        """
        start_of_data = False

        for line in self._stream_handle:

            if not start_of_data:
                if line.rstrip() == START_OF_DATA:
                    start_of_data = True
                continue  # skip lines until Data start

            fields = line.split()
            count = len(fields)

            try:  # get the record type from first field
                record_type = int(fields[0])
            except ValueError:
                error_str = 'Invalid record type on line %s'
                log.warn(error_str, line)
                self._exception_callback(SampleException(error_str % line))
                continue  # skip to next line

            if record_type == PH_TYPE and count == PH_FIELDS:
                # particle-ize the data block received, return the record
                particle = self._extract_sample(PhsenRecoveredInstrumentDataParticle,
                                                None, fields, None)

                self._record_buffer.append(particle)

            elif record_type in BATTERY_DATA_CONTROL_TYPE and count == BATTERY_DATA_CONTROL_FIELDS:
                # particle-ize the data block received, return the record
                particle = self._extract_sample(PhsenRecoveredMetadataDataParticle,
                                                None, fields, None)

                self._record_buffer.append(particle)

            elif record_type in CONTROL_TYPE and count == CONTROL_FIELDS:
                particle = self._extract_sample(PhsenRecoveredMetadataDataParticle,
                                                None, fields, None)

                self._record_buffer.append(particle)

            else:
                error_str = 'Invalid record type or incorrect number of fields %s'
                log.warn(error_str, line)
                self._exception_callback(SampleException(error_str % line))

