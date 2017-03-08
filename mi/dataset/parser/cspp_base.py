#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/cspp_base.py
@author Mark Worden
@brief Base Parser for a cspp dataset driver
Release notes:

initial release
"""

import copy
import re
import string

from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.core.exceptions import DatasetParserException, \
    RecoverableSampleException, \
    ConfigurationException
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    FLOAT_REGEX, ASCII_HEX_CHAR_REGEX

log = get_logger()

__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

HEADER_PART_REGEX = r'(.*):\s+(.*)' + END_OF_LINE_REGEX
HEADER_PART_MATCHER = re.compile(HEADER_PART_REGEX)

TIMESTAMP_LINE_REGEX = r'Timestamp.*' + END_OF_LINE_REGEX
TIMESTAMP_LINE_MATCHER = re.compile(TIMESTAMP_LINE_REGEX)

# A regex to capture the y or n (used in Suspect Timestamp)
Y_OR_N_REGEX = r'[yYnN]'
# A regex to match against one or more tab characters

# a regex to match the 3 items that start a sample or status line:
# profiler timestamp, depth, suspect timestamp
SAMPLE_START_REGEX = FLOAT_REGEX + '\t' + FLOAT_REGEX + '\t' + Y_OR_N_REGEX + '\t'

# match the profiler timestamp, depth, suspect timestamp followed by all hex
# ascii chars
# not all cspp instruments will match this due to the depth and suspect timestamp
# being reversed, but those do not have ignore matchers so they will get caught
# by being expected data
HEX_ASCII_LINE_REGEX = SAMPLE_START_REGEX + ASCII_HEX_CHAR_REGEX + '*' + END_OF_LINE_REGEX
HEX_ASCII_LINE_MATCHER = re.compile(HEX_ASCII_LINE_REGEX)

# The following two keys are keys to be used with the PARTICLE_CLASSES_DICT
# The key for the metadata particle class
METADATA_PARTICLE_CLASS_KEY = 'metadata_particle_class'
# The key for the data particle class
DATA_PARTICLE_CLASS_KEY = 'data_particle_class'


def encode_y_or_n(val):
    if val == 'y' or val == 'Y':
        return 1
    else:
        return 0


class HeaderPartMatchesGroupNumber(BaseEnum):
    """
    An enum used to access header related match group values
    """
    HEADER_PART_MATCH_GROUP_KEY = 1
    HEADER_PART_MATCH_GROUP_VALUE = 2


class DefaultHeaderKey(BaseEnum):
    """
    An enum for the default set of header keys
    """
    SOURCE_FILE = 'Source File'
    PROCESSED = 'Processed'
    USING_VERSION = 'Using Version'
    DEVICE = 'Device'
    START_DATE = 'Start Date'

# The default set of header keys as a list
DEFAULT_HEADER_KEY_LIST = DefaultHeaderKey.list()


class CsppMetadataParserDataParticleKey(BaseEnum):
    """
    An enum for the base cspp metadata particle parameters
    """
    LAST_CHARACTER_CONTROLLER_ID = 'last_character_controller_id'
    DAY_OF_YEAR_NUMBER = 'day_of_year_number'
    FRACTION_OF_DAY = 'fraction_of_day'
    SOURCE_FILE = 'source_file'
    PROCESSING_TIME = 'processing_time'
    PREPROCESSING_SOFTWARE_VERSION = 'preprocessing_software_version'
    START_DATE = 'start_date'

# The following are used to index into encoding rules tuple structures.  The HEADER_DICTIONARY_KEY_INDEX
# is the same value as the DATA_MATCHES_GROUP_NUMBER_INDEX because one is used for the metadata and the other
# is used for the data record.
PARTICLE_KEY_INDEX = 0
HEADER_DICTIONARY_KEY_INDEX = 1
DATA_MATCHES_GROUP_NUMBER_INDEX = 1
TYPE_ENCODING_INDEX = 2

# A group of metadata particle encoding rules used to simplify encoding using a loop
METADATA_PARTICLE_ENCODING_RULES = [
    (CsppMetadataParserDataParticleKey.SOURCE_FILE, DefaultHeaderKey.SOURCE_FILE, str),
    (CsppMetadataParserDataParticleKey.PREPROCESSING_SOFTWARE_VERSION, DefaultHeaderKey.USING_VERSION, str),
    (CsppMetadataParserDataParticleKey.START_DATE, DefaultHeaderKey.START_DATE, str),
    (CsppMetadataParserDataParticleKey.PROCESSING_TIME, DefaultHeaderKey.PROCESSED, str)
]

# The following items are used to index into source file name string
LAST_CHARACTER_CONTROLLER_ID_SOURCE_FILE_CHAR_POSITION = 0
DAY_OF_YEAR_NUMBER_SOURCE_FILE_STARTING_CHAR_POSITION = 1
DAY_OF_YEAR_NUMBER_SOURCE_FILE_CHARS_END_RANGE = 4
FRACTION_OF_DAY_SOURCE_FILE_STARTING_CHAR_POSITION = 4
FRACTION_OF_DAY_SOURCE_FILE_CHARS_END_RANGE = 8


class MetadataRawDataKey(BaseEnum):
    """
    An enum used to index into a tuple of metadata parts
    """
    HEADER_DICTIONARY = 0
    DATA_MATCH = 1


class CsppMetadataDataParticle(DataParticle):
    """
    Class for parsing cspp metadata particle values
    """

    def _build_metadata_parsed_values(self):
        """
        This method builds and returns a list of encoded common metadata particle values from the raw_data which
        is expected to be regular expression match data.  This method would need to be overridden if the header
        items do not include what is in the DefaultHeaderKey enum
        @returns result list of metadata parsed values
        """

        # Initialize the results to return
        results = []

        # Grab the header data dictionary, which is the first item in the raw_data tuple
        header_dict = self.raw_data[MetadataRawDataKey.HEADER_DICTIONARY]

        # Grab the source file path from the match raw_data's
        source_file_path = header_dict[DefaultHeaderKey.SOURCE_FILE]

        # Split the source file path.  The regex below supports splitting on a Windows or unix/linux file path.
        source_file_name_parts = re.split(r'\\|/', source_file_path)
        # Obtain the list of source file name parts
        num_source_name_file_parts = len(source_file_name_parts)
        # Grab the last part of the source file name
        last_part_of_source_file_name = source_file_name_parts[num_source_name_file_parts - 1]

        # Encode the last character controller ID which consists of one character within the source file name
        results.append(self._encode_value(
            CsppMetadataParserDataParticleKey.LAST_CHARACTER_CONTROLLER_ID,
            last_part_of_source_file_name[LAST_CHARACTER_CONTROLLER_ID_SOURCE_FILE_CHAR_POSITION],
            str))

        # Encode the day of year number which consists of three characters within the source file name
        results.append(self._encode_value(
            CsppMetadataParserDataParticleKey.DAY_OF_YEAR_NUMBER,
            last_part_of_source_file_name[
                DAY_OF_YEAR_NUMBER_SOURCE_FILE_STARTING_CHAR_POSITION:DAY_OF_YEAR_NUMBER_SOURCE_FILE_CHARS_END_RANGE],
            int))

        # Encode the fraction of day which consists of four characters within the source file name
        results.append(self._encode_value(
            CsppMetadataParserDataParticleKey.FRACTION_OF_DAY,
            last_part_of_source_file_name[
                FRACTION_OF_DAY_SOURCE_FILE_STARTING_CHAR_POSITION:FRACTION_OF_DAY_SOURCE_FILE_CHARS_END_RANGE],
            int))

        # Iterate through a set of metadata particle encoding rules to encode the remaining parameters
        for name, index, encoding_func in METADATA_PARTICLE_ENCODING_RULES:
            results.append(self._encode_value(name, header_dict[index], encoding_func))

        log.debug('CsppMetadataDataParticle: particle=%s', results)
        return results


class CsppParser(SimpleParser):
    """
    Class for a common cspp data file parser
    """

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 data_record_regex,
                 header_key_list=None,
                 ignore_matcher=None):
        """
        This method is a constructor that will instantiate an CsppParser object.
        @param config The configuration for this CsppParser parser
        @param stream_handle The handle to the data stream containing the cspp data
        @param exception_callback The function to call to report exceptions
        @param data_record_regex The data regex that should be used to obtain data records
        @param header_key_list The list of header keys expected within a header
        @param ignore_matcher A matcher from a regex to use to ignore expected junk lines
        """

        self._data_record_matcher = None
        self._header_and_first_data_record_matcher = None
        self._ignore_matcher = ignore_matcher

        # Ensure that we have a data regex
        if data_record_regex is None:
            log.warn('A data_record_regex is required, but None was given')
            raise DatasetParserException("Must provide a data_record_regex")
        else:
            self._data_record_matcher = re.compile(data_record_regex)

        # Build up the header state dictionary using the default her key list ot one that was provided
        self._header_state = {}

        if header_key_list is None:
            header_key_list = DEFAULT_HEADER_KEY_LIST

        for header_key in header_key_list:
            self._header_state[header_key] = None

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
            # Set the metadata and data particle classes to be used later

            if METADATA_PARTICLE_CLASS_KEY in particle_classes_dict and \
               DATA_PARTICLE_CLASS_KEY in particle_classes_dict:

                self._data_particle_class = particle_classes_dict.get(DATA_PARTICLE_CLASS_KEY)
                self._metadata_particle_class = particle_classes_dict.get(METADATA_PARTICLE_CLASS_KEY)
            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
        else:
            log.warning('Configuration missing particle classes dict')
            raise ConfigurationException('Configuration missing particle classes dict')

        # Initialize the record buffer to an empty list
        self._record_buffer = []

        # Initialize the metadata flag
        self._metadata_extracted = False

        # Call the superclass constructor
        super(CsppParser, self).__init__(config,
                                         stream_handle,
                                         exception_callback)

    def _process_data_match(self, data_match, result_particles):
        """
        This method processes a data match.  It will extract a metadata particle and insert it into
         result_particles when we have not already extracted the metadata and all header values exist.
         This method will also extract a data particle and append it to the result_particles.
        @param data_match A regular expression match object for a cspp data record
        @param result_particles A list which should be updated to include any particles extracted
        """

        # Extract the data record particle
        data_particle = self._extract_sample(self._data_particle_class,
                                             None,
                                             data_match,
                                             None)

        # If we created a data particle, let's append the particle to the result particles
        # to return and increment the state data positioning
        if data_particle:

            if not self._metadata_extracted:
                # Once the first data particle is read, all available header lines will
                # have been read and inserted into the header state dictionary.
                # Only the source file is required to create a metadata particle.

                if self._header_state[DefaultHeaderKey.SOURCE_FILE] is not None:
                    metadata_particle = self._extract_sample(self._metadata_particle_class,
                                                             None,
                                                             (copy.copy(self._header_state),
                                                              data_match),
                                                             None)
                    if metadata_particle:
                        # We're going to insert the metadata particle so that it is
                        # the first in the list and set the position to 0, as it cannot
                        # have the same position as the non-metadata particle
                        result_particles.insert(0, metadata_particle)
                    else:
                        # metadata particle was not created successfully
                        log.warn('Unable to create metadata particle')
                        self._exception_callback(RecoverableSampleException(
                            'Unable to create metadata particle'))
                else:
                    # no source file path, don't create metadata particle
                    log.warn('No source file, not creating metadata particle')
                    self._exception_callback(RecoverableSampleException(
                        'No source file, not creating metadata particle'))

                # need to set metadata extracted to true so we don't keep creating
                # the metadata, even if it failed
                self._metadata_extracted = True

            result_particles.append(data_particle)

    def _process_header_part_match(self, header_part_match):
        """
        This method processes a header part match.  It will process one row within a cspp header
        that matched a provided regex.  The match groups should be processed and the _header_state
        will be updated  with the obtained header values.
        @param header_part_match A regular expression match object for a cspp header row
        """

        header_part_key = header_part_match.group(
            HeaderPartMatchesGroupNumber.HEADER_PART_MATCH_GROUP_KEY)
        header_part_value = header_part_match.group(
            HeaderPartMatchesGroupNumber.HEADER_PART_MATCH_GROUP_VALUE)

        if header_part_key in self._header_state.keys():
            self._header_state[header_part_key] = string.rstrip(header_part_value)

    def _process_line_not_containing_data_record_or_header_part(self, line):
        """
        This method processes a line that does not contain a data record or header.  This case is
        not applicable to "non_data".  For cspp file streams, we expect some lines in the file that
        we do not care about, and we will not consider them "non_data".
        @param line A regular expression match object for a cspp header row
        """

        if HEX_ASCII_LINE_MATCHER.match(line):
            # we found a line starting with the timestamp, depth, and
            # suspect timestamp, followed by all hex ascii chars
            log.warn('got hex ascii corrupted data %s ', line)
            self._exception_callback(RecoverableSampleException(
                "Found hex ascii corrupted data: %s" % line))

        # ignore the expected timestamp line and any lines matching the ignore regex,
        # otherwise data is unexpected
        elif not TIMESTAMP_LINE_MATCHER.match(line) and not \
                (self._ignore_matcher is not None and self._ignore_matcher.match(line)):
            # Unexpected data was found
            log.warn('got unrecognized row %r', line)
            self._exception_callback(RecoverableSampleException("Found an invalid line: %s" % line))

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        for line in self._stream_handle:

            data_match = self._data_record_matcher.match(line)

            # If we found a data match, let's process it
            if data_match is not None:
                self._process_data_match(data_match, self._record_buffer)

            else:
                # Check for head part match
                header_part_match = HEADER_PART_MATCHER.match(line)

                if header_part_match is not None:
                    self._process_header_part_match(header_part_match)

                else:
                    self._process_line_not_containing_data_record_or_header_part(line)

