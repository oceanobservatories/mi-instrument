__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

import re

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

log = get_logger()
from mi.dataset.dataset_parser import Parser
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcDataParticleKey, \
    Pco2wAbcParticleClassKey
from mi.dataset.parser.common_regexes import UNSIGNED_INT_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX

"""
*** BEGIN definition of regular expressions, matchers and group indices for instrument and instrument blank
data records.

Example instrument record with light measurements:
> 4	3466998237	44	38	2869	311	1877	613	1581	5139	42	38	2887	313	1879	614	3200	2248

Data Mapping:
4 -> record type (instrument data)
3466998237 -> record time in seconds since 1904 -> ?
44	38	2869	311	1877	613	1581	5139	42	38	2887	313	1879	614 -> light measurements
3200 -> battery voltage
2248 -> thermistor

Example instrument record with blank light measurements:
> 5	3466968983	41	36	2919	1566	1921	1763	8707	15056	42	38	2933	1575	1925	1766
3180	2138

Data Mapping:
5 -> record type (instrument blank data)
3466968983 -> record time seconds since 1904 -> 2013-11-10 22:56:23
41 36 2919 1566 1921 1763 8707 15056 42 38 2933 1575 1925 1766 -> blank light measurements
3180 -> battery voltage
2138 -> thermistor
"""
INSTRUMENT_RECORD_TYPE_REGEX = r'(4)'
INSTRUMENT_BLANK_RECORD_TYPE_REGEX = r'(5)'
RECORD_TIME_REGEX = BATTERY_VOLTAGE_REGEX = THERMISTOR_REGEX = r'(' + UNSIGNED_INT_REGEX + ')'
LIGHT_MEASUREMENTS_REGEX = r'((?:' + UNSIGNED_INT_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + '){14})'

INSTRUMENT_REGEX = r'^' + INSTRUMENT_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                   RECORD_TIME_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + LIGHT_MEASUREMENTS_REGEX + \
                   BATTERY_VOLTAGE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + THERMISTOR_REGEX
INSTRUMENT_MATCHER = re.compile(INSTRUMENT_REGEX)

INSTRUMENT_BLANK_REGEX = r'^' + INSTRUMENT_BLANK_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                         RECORD_TIME_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + LIGHT_MEASUREMENTS_REGEX + \
                         BATTERY_VOLTAGE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + THERMISTOR_REGEX
INSTRUMENT_BLANK_MATCHER = re.compile(INSTRUMENT_BLANK_REGEX)

# The following constants are used to index into an INSTRUMENT_MATCHER or
# INSTRUMENT_BLANK_MATCHER match group
RECORD_TYPE_GROUP_INDEX = 1
RECORD_TIME_GROUP_INDEX = 2
LIGHT_MEASUREMENTS_GROUP_INDEX = 3
INSTRUMENT_BATTERY_VOLTAGE_GROUP_INDEX = 4
THERMISTOR_GROUP_INDEX = 5
"""
*** END definition of regular expressions, matchers and group indices for instrument and instrument blank data records.
"""

"""
*** BEGIN definition of regular expressions, matchers and group indices for metadata records.

Metadata record types:
128 - launch record type
129 - start record type
131 - good shutdown record type
133 - handshake_on record type
135 - stopped by user record type

Example:
> 128	3466968623	65	1	0	512

Data Mapping:
128 -> launch record type
3466968623 -> seconds since 1904 -> 2013-11-10 22:50:23
65 -> flag = 0x41 = 01000001 -> erased, clock started
1 -> number of records
0 -> number of errors
512 ->number of bytes
"""
FLAGS_REGEX = r'(\d{2})'
NUM_DATA_RECORDS_REGEX = NUM_ERROR_RECORDS_REGEX = NUM_BYTES_STORED_REGEX = \
    r'(' + UNSIGNED_INT_REGEX + ')'

METADATA_RECORD_TYPE_REGEX = r'(1[2-9][8-9]|1[3-9][0-9]|2[0-4][0-9]|25[0-5])'
METADATA_REGEX = r'^' + METADATA_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                 RECORD_TIME_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + FLAGS_REGEX + \
                 ONE_OR_MORE_WHITESPACE_REGEX + NUM_DATA_RECORDS_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                 NUM_ERROR_RECORDS_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + NUM_BYTES_STORED_REGEX
METADATA_MATCHER = re.compile(METADATA_REGEX)

METADATA_WITH_BATTERY_VOLTAGE_RECORD_TYPE_REGEX = r'(192|193)'
METADATA_WITH_BATTERY_VOLTAGE_REGEX = \
    r'^' + METADATA_WITH_BATTERY_VOLTAGE_RECORD_TYPE_REGEX + \
    ONE_OR_MORE_WHITESPACE_REGEX + RECORD_TIME_REGEX + \
    ONE_OR_MORE_WHITESPACE_REGEX + FLAGS_REGEX + \
    ONE_OR_MORE_WHITESPACE_REGEX + NUM_DATA_RECORDS_REGEX + \
    ONE_OR_MORE_WHITESPACE_REGEX + \
    NUM_ERROR_RECORDS_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
    NUM_BYTES_STORED_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
    BATTERY_VOLTAGE_REGEX
METADATA_WITH_BATTERY_VOLTAGE_MATCHER = re.compile(METADATA_WITH_BATTERY_VOLTAGE_REGEX)

# The following constants are used to index into an METADATA_MATCHER or
# METADATA_WITH_BATTERY_VOLTAGE_MATCHER match group
FLAGS_GROUP_INDEX = 3
NUM_DATA_RECORDS_GROUP_INDEX = 4
NUM_ERROR_RECORDS_GROUP_INDEX = 5
NUM_BYTES_STORED_GROUP_INDEX = 6
METADATA_BATTERY_VOLTAGE_GROUP_INDEX = 7

"""
*** END definition of regular expressions, matchers and group indices for metadata records.
"""

"""
*** BEGIN definition of regular expressions, matchers and group indices for power records.

Power record type:

Example:
> 17	3466968635

Data Mapping:
17 -> power record type
3466968635 -> seconds since 1904 Sunday, November 10, 2013 5:50:35pm
"""

POWER_RECORD_TYPE_REGEX = r'(17|33|49)'
POWER_REGEX = r'^' + POWER_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
              RECORD_TIME_REGEX
POWER_MATCHER = re.compile(POWER_REGEX)
"""
*** END definition of regular expressions, matchers and group indices for power records.
"""

"""
NOTE: A record with record type 10 should not be seen since it is pH based and not CO2.
"""

"""
*** BEGIN definition of regular expressions and matchers for ignore handling.
"""
IGNORE_MARKER_END_REGEX = r'^:Data'
IGNORE_MARKER_END_MATCHER = re.compile(IGNORE_MARKER_END_REGEX)
"""
*** END definition of regular expressions and matchers for ignore handling.
"""


class Pco2wAbcParser(Parser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 state_callback=None,  # No longer used
                 publish_callback=None):  # No longer used

        self._file_parsed = False
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

        super(Pco2wAbcParser, self).__init__(config,
                                             stream_handle,
                                             None,  # State no longer used
                                             None,  # Sieve function no longer used
                                             state_callback,
                                             publish_callback,
                                             exception_callback)

    @staticmethod
    def _create_empty_metadata_dict():

        metadata_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                       Pco2wAbcDataParticleKey.RECORD_TIME,
                                       Pco2wAbcDataParticleKey.CLOCK_ACTIVE,
                                       Pco2wAbcDataParticleKey.RECORDING_ACTIVE,
                                       Pco2wAbcDataParticleKey.RECORD_END_ON_TIME,
                                       Pco2wAbcDataParticleKey.RECORD_MEMORY_FULL,
                                       Pco2wAbcDataParticleKey.RECORD_END_ON_ERROR,
                                       Pco2wAbcDataParticleKey.DATA_DOWNLOAD_OK,
                                       Pco2wAbcDataParticleKey.FLASH_MEMORY_OPEN,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_PRESTART,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_MEASUREMENT,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_BLANK,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_EXTERNAL,
                                       Pco2wAbcDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                                       Pco2wAbcDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                                       Pco2wAbcDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                                       Pco2wAbcDataParticleKey.FLASH_ERASED,
                                       Pco2wAbcDataParticleKey.POWER_ON_INVALID,
                                       Pco2wAbcDataParticleKey.NUM_DATA_RECORDS,
                                       Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS,
                                       Pco2wAbcDataParticleKey.NUM_BYTES_STORED,
                                       Pco2wAbcDataParticleKey.VOLTAGE_BATTERY],
                                      None)

        return metadata_dict

    @staticmethod
    def _create_empty_power_dict():

        power_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                    Pco2wAbcDataParticleKey.RECORD_TIME],
                                   None)

        return power_dict

    @staticmethod
    def _create_empty_instrument_dict():

        instrument_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                         Pco2wAbcDataParticleKey.RECORD_TIME,
                                         Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS,
                                         Pco2wAbcDataParticleKey.VOLTAGE_BATTERY,
                                         Pco2wAbcDataParticleKey.THERMISTOR_RAW],
                                        None)

        return instrument_dict

    @staticmethod
    def _create_empty_instrument_blank_dict():

        instrument_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                         Pco2wAbcDataParticleKey.RECORD_TIME,
                                         Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS,
                                         Pco2wAbcDataParticleKey.VOLTAGE_BATTERY,
                                         Pco2wAbcDataParticleKey.THERMISTOR_RAW],
                                        None)

        return instrument_dict

    @staticmethod
    def _fill_metadata_dict(metadata_match, metadata_dict):

        metadata_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
            metadata_match.group(RECORD_TYPE_GROUP_INDEX)
        metadata_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
            metadata_match.group(RECORD_TIME_GROUP_INDEX)

        # Convert the FLAGS integer value to a 16 bit binary value with leading 0s
        # as necessary
        bit_string = format(int(metadata_match.group(FLAGS_GROUP_INDEX)), '016b')

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

        metadata_dict[Pco2wAbcDataParticleKey.NUM_DATA_RECORDS] = \
            metadata_match.group(NUM_DATA_RECORDS_GROUP_INDEX)
        metadata_dict[Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS] = \
            metadata_match.group(NUM_ERROR_RECORDS_GROUP_INDEX)
        metadata_dict[Pco2wAbcDataParticleKey.NUM_BYTES_STORED] = \
            metadata_match.group(NUM_BYTES_STORED_GROUP_INDEX)


        if metadata_match.lastindex == METADATA_BATTERY_VOLTAGE_GROUP_INDEX:
            metadata_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
                metadata_match.group(METADATA_BATTERY_VOLTAGE_GROUP_INDEX)

    @staticmethod
    def _fill_power_dict(power_match, power_dict):

        power_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
            power_match.group(RECORD_TYPE_GROUP_INDEX)
        power_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
            power_match.group(RECORD_TIME_GROUP_INDEX)

    @staticmethod
    def _fill_instrument_dict(instrument_record_match, instrument_dict):

        instrument_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
            instrument_record_match.group(RECORD_TYPE_GROUP_INDEX)
        instrument_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
            instrument_record_match.group(RECORD_TIME_GROUP_INDEX)
        instrument_dict[Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS] = \
            map(int, instrument_record_match.group(LIGHT_MEASUREMENTS_GROUP_INDEX). \
            rstrip().split('\t'))
        instrument_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
            instrument_record_match.group(INSTRUMENT_BATTERY_VOLTAGE_GROUP_INDEX)
        instrument_dict[Pco2wAbcDataParticleKey.THERMISTOR_RAW] = \
            instrument_record_match.group(THERMISTOR_GROUP_INDEX)

    @staticmethod
    def _fill_instrument_blank_dict(instrument_blank_record_match, instrument_blank_dict):

        instrument_blank_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
            instrument_blank_record_match.group(RECORD_TYPE_GROUP_INDEX)
        instrument_blank_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
            instrument_blank_record_match.group(RECORD_TIME_GROUP_INDEX)
        instrument_blank_dict[Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS] = \
            map(int, instrument_blank_record_match.group(LIGHT_MEASUREMENTS_GROUP_INDEX). \
            rstrip().split('\t'))
        instrument_blank_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
            instrument_blank_record_match.group(INSTRUMENT_BATTERY_VOLTAGE_GROUP_INDEX)
        instrument_blank_dict[Pco2wAbcDataParticleKey.THERMISTOR_RAW] = \
            instrument_blank_record_match.group(THERMISTOR_GROUP_INDEX)

    def parse_file(self):

        # Let's create some empty dictionaries which we will use to collect
        # data for the extract sample calls.
        metadata_dict = Pco2wAbcParser._create_empty_metadata_dict()
        power_dict = Pco2wAbcParser._create_empty_power_dict()
        instrument_dict = Pco2wAbcParser._create_empty_instrument_dict()
        instrument_blank_dict = Pco2wAbcParser._create_empty_instrument_blank_dict()

        line = self._stream_handle.readline()

        found_ignore_marker_end = False

        # Go through each line in the file
        while line:

            # Did we already find the end of ignore data marker?
            # If not, then see if we found it
            if not found_ignore_marker_end:

                ignore_marker_end_match = IGNORE_MARKER_END_MATCHER.match(line)

                # If we found the marker that indicates to stop ignoring data,
                # set the indication that we found it
                if ignore_marker_end_match:
                    found_ignore_marker_end = True

            else:
                # OK.  We need to process lines now.

                # Let's check for each of the different match possibilities
                metadata_with_battery_voltage_match = \
                    METADATA_WITH_BATTERY_VOLTAGE_MATCHER.match(line)
                metadata_match = METADATA_MATCHER.match(line)
                power_match = POWER_MATCHER.match(line)
                instrument_match = INSTRUMENT_MATCHER.match(line)
                instrument_blank_match = INSTRUMENT_BLANK_MATCHER.match(line)

                # There are two metadata match possibilities
                if metadata_with_battery_voltage_match or metadata_match:

                    # If we found a metadata record with battery voltage, let's
                    # supply that match
                    if metadata_with_battery_voltage_match:

                        Pco2wAbcParser._fill_metadata_dict(
                            metadata_with_battery_voltage_match,
                            metadata_dict)

                    else:

                        # If we found a metadata record without battery voltage,
                        # let's supply that match
                        Pco2wAbcParser._fill_metadata_dict(
                            metadata_match, metadata_dict)

                    timestamp = float(metadata_dict[Pco2wAbcDataParticleKey.RECORD_TIME])

                    particle = self._extract_sample(self._metadata_class,
                                                    None,
                                                    metadata_dict,
                                                    timestamp)

                    log.debug("Appending metadata particle: %s", particle.generate())
                    self._record_buffer.append(particle)

                    # Recreate an empty metadata dictionary
                    metadata_dict = Pco2wAbcParser._create_empty_metadata_dict()

                elif power_match:

                    Pco2wAbcParser._fill_power_dict(power_match, power_dict)

                    timestamp = float(power_dict[Pco2wAbcDataParticleKey.RECORD_TIME])

                    particle = self._extract_sample(self._power_class,
                                                    None,
                                                    power_dict,
                                                    timestamp)

                    log.debug("Appending power particle: %s", particle.generate())
                    self._record_buffer.append(particle)

                    # Recreate an empty power dictionary
                    power_dict = Pco2wAbcParser._create_empty_power_dict()

                elif instrument_match:

                    Pco2wAbcParser._fill_instrument_dict(instrument_match, instrument_dict)

                    timestamp = float(instrument_dict[Pco2wAbcDataParticleKey.RECORD_TIME])

                    particle = self._extract_sample(self._instrument_class,
                                                    None,
                                                    instrument_dict,
                                                    timestamp)

                    log.debug("Appending instrument particle: %s", particle.generate())
                    self._record_buffer.append(particle)

                    # Recreate an empty instrument dictionary
                    instrument_dict = Pco2wAbcParser._create_empty_instrument_dict()

                elif instrument_blank_match:

                    Pco2wAbcParser._fill_instrument_blank_dict(
                        instrument_blank_match, instrument_blank_dict)

                    timestamp = float(instrument_blank_dict[Pco2wAbcDataParticleKey.RECORD_TIME])

                    particle = self._extract_sample(self._instrument_blank_class,
                                                    None,
                                                    instrument_blank_dict,
                                                    timestamp)

                    log.debug("Appending instrument blank particle: %s", particle.generate())
                    self._record_buffer.append(particle)

                    # Recreate an empty instrument blank dictionary
                    instrument_blank_dict = Pco2wAbcParser._create_empty_instrument_blank_dict()

                else:
                    # We found a line in the file that was unexpected.  Report a
                    # RecoverableSampleException
                    message = "Unexpected data in file, line: " + line
                    self._exception_callback(RecoverableSampleException(message))

            line = self._stream_handle.readline()

        # Let's provide an indication that the file was parsed
        self._file_parsed = True

    def get_records(self, num_records_requested=1):
        """
        Returns a list of particles that is  equal to the num_records_requested when there are that many particles
        are available or a list of particles less than the num_records_requested when there are fewer than
        num_records_requested available.
        """
        particles_to_return = []

        if num_records_requested > 0:

            # If the file was not read, let's parse it
            if self._file_parsed is False:
                self.parse_file()

            # Iterate through the particles returned, and pop them off from the beginning of the record
            # buffer to the end
            while len(particles_to_return) < num_records_requested and len(self._record_buffer) > 0:
                particles_to_return.append(self._record_buffer.pop(0))

        return particles_to_return
