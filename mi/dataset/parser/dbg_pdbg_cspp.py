#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/dbg_pdbg_cspp.py
@author Jeff Roy
@brief Parser for the dbg_pdbg_cspp dataset driver
Release notes:

initial release
"""

import copy
import re
import string
import numpy

from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import \
    RecoverableSampleException, \
    ConfigurationException


from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_parser import SimpleParser


from mi.dataset.parser.cspp_base import \
    DEFAULT_HEADER_KEY_LIST, \
    METADATA_PARTICLE_CLASS_KEY, \
    HeaderPartMatchesGroupNumber, \
    TIMESTAMP_LINE_MATCHER, \
    HEADER_PART_MATCHER, \
    HEX_ASCII_LINE_MATCHER, \
    DefaultHeaderKey, \
    Y_OR_N_REGEX, \
    CsppMetadataDataParticle, \
    MetadataRawDataKey, \
    encode_y_or_n

from mi.dataset.parser.common_regexes import INT_REGEX, \
    FLOAT_REGEX, \
    END_OF_LINE_REGEX, \
    MULTIPLE_TAB_REGEX

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

log = get_logger()

STRING_REGEX = r'.*'  # any characters except new line

COMMON_DATA_REGEX = '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX    # Profiler Timestamp
COMMON_DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX   # Depth
COMMON_DATA_REGEX += '(' + Y_OR_N_REGEX + ')' + MULTIPLE_TAB_REGEX  # Suspect Timestamp

BATTERY_STATUS_REGEX = r'&' + '(' + INT_REGEX + ')'          # Battery Number
BATTERY_STATUS_REGEX += r'q0 d- ' + '(' + FLOAT_REGEX + ')'  # Battery Voltage
BATTERY_STATUS_REGEX += STRING_REGEX                         # other crap to be ignored

GPS_ADJUSTMENT_REGEX = 'GPS adjustment ' + '(' + INT_REGEX + ')'  # GPS Adjustment

BATTERY_DATA_REGEX = COMMON_DATA_REGEX + BATTERY_STATUS_REGEX + END_OF_LINE_REGEX
BATTERY_DATA_MATCHER = re.compile(BATTERY_DATA_REGEX)

GPS_DATA_REGEX = COMMON_DATA_REGEX + GPS_ADJUSTMENT_REGEX + END_OF_LINE_REGEX
GPS_DATA_MATCHER = re.compile(GPS_DATA_REGEX)

IGNORE_REGEX = COMMON_DATA_REGEX + STRING_REGEX + END_OF_LINE_REGEX  # most of the status messages are ignored
IGNORE_MATCHER = re.compile(IGNORE_REGEX)


class DataMatchesGroupNumber(BaseEnum):
    """
    An enum for group match indices for a data record chunk.
    Used to access the match groups in the particle raw data
    """
    PROFILER_TIMESTAMP = 1
    PRESSURE = 2
    SUSPECT_TIMESTAMP = 3
    BATTERY_NUMBER = 4
    BATTERY_VOLTAGE = 5
    GPS_ADJUSTMENT = 4  # uses same index as BATTERY_NUMBER because they are for different particles


class DbgPdbgDataTypeKey(BaseEnum):
    DBG_PDBG_CSPP_TELEMETERED = 'dbg_pdbg_cspp_telemetered'
    DBG_PDBG_CSPP_RECOVERED = 'dbg_pdbg_cspp_recovered'


BATTERY_STATUS_CLASS_KEY = 'battery_status_class'
GPS_ADJUSTMENT_CLASS_KEY = 'gps_adjustment_class'


class DbgPdbgDataParticleType(BaseEnum):
    BATTERY_TELEMETERED = 'cspp_eng_cspp_dbg_pdbg_batt_eng'
    BATTERY_RECOVERED = 'cspp_eng_cspp_dbg_pdbg_batt_eng_recovered'
    GPS_TELEMETERED = 'cspp_eng_cspp_dbg_pdbg_gps_eng'
    GPS_RECOVERED = 'cspp_eng_cspp_dbg_pdbg_gps_eng_recovered'
    METADATA_TELEMETERED = 'cspp_eng_cspp_dbg_pdbg_metadata'
    METADATA_RECOVERED = 'cspp_eng_cspp_dbg_pdbg_metadata_recovered'


class DbgPdbgBatteryParticleKey(BaseEnum):
    """
    The data particle keys associated with dbg_pdbg battery status particle parameters
    """
    PROFILER_TIMESTAMP = 'profiler_timestamp'
    PRESSURE = 'pressure_depth'
    SUSPECT_TIMESTAMP = 'suspect_timestamp'
    BATTERY_NUMBER = 'battery_number_uint8'
    BATTERY_VOLTAGE = 'battery_voltage_flt32'


class DbgPdbgGpsParticleKey(BaseEnum):
    """
    The data particle keys associated with dbg_pdbg GPS adjustment particle parameters
    """
    PROFILER_TIMESTAMP = 'profiler_timestamp'
    PRESSURE = 'pressure_depth'
    SUSPECT_TIMESTAMP = 'suspect_timestamp'
    GPS_ADJUSTMENT = 'gps_adjustment'

# A group of encoding rules common to the battery and gps status particles
# used to simplify encoding using a loop

COMMON_PARTICLE_ENCODING_RULES = [
    (DbgPdbgGpsParticleKey.PROFILER_TIMESTAMP, DataMatchesGroupNumber.PROFILER_TIMESTAMP, numpy.float),
    (DbgPdbgGpsParticleKey.PRESSURE, DataMatchesGroupNumber.PRESSURE, float),
    (DbgPdbgGpsParticleKey.SUSPECT_TIMESTAMP, DataMatchesGroupNumber.SUSPECT_TIMESTAMP, encode_y_or_n),
]


class DbgPdbgMetadataDataParticle(CsppMetadataDataParticle):
    """
    Class for building a dbg pdbg metadata particle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws RecoverableSampleException If there is a problem with sample creation
        """

        results = []

        try:

            # Append the base metadata parsed values to the results to return
            results += self._build_metadata_parsed_values()

            data_match = self.raw_data[MetadataRawDataKey.DATA_MATCH]

            # Set the internal timestamp
            internal_timestamp_unix = numpy.float(data_match.group(
                DataMatchesGroupNumber.PROFILER_TIMESTAMP))
            self.set_internal_timestamp(unix_time=internal_timestamp_unix)

        except (ValueError, TypeError, IndexError) as ex:
            log.warn("Exception when building parsed values")
            raise RecoverableSampleException("Error (%s) while decoding parameters in data: [%s]"
                                             % (ex, self.raw_data))

        return results


class DbgPdbgMetadataRecoveredDataParticle(DbgPdbgMetadataDataParticle):
    """
    Class for building a dbg pdbg recovered metadata particle
    """

    _data_particle_type = DbgPdbgDataParticleType.METADATA_RECOVERED


class DbgPdbgMetadataTelemeteredDataParticle(DbgPdbgMetadataDataParticle):
    """
    Class for building a dbg pdbg telemetered metadata particle
    """

    _data_particle_type = DbgPdbgDataParticleType.METADATA_TELEMETERED


class DbgPdbgBatteryParticle(DataParticle):
    """
    Class for parsing data from the dbg pdbg engineering data set
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws RecoverableSampleException If there is a problem with sample creation
        """
        results = []

        try:

            # Process each of the common particle parameters
            for name, group, function in COMMON_PARTICLE_ENCODING_RULES:
                results.append(self._encode_value(name, self.raw_data.group(group), function))

            results.append(self._encode_value(DbgPdbgBatteryParticleKey.BATTERY_NUMBER,
                                              self.raw_data.group(DataMatchesGroupNumber.BATTERY_NUMBER),
                                              int))

            results.append(self._encode_value(DbgPdbgBatteryParticleKey.BATTERY_VOLTAGE,
                                              self.raw_data.group(DataMatchesGroupNumber.BATTERY_VOLTAGE),
                                              float))

            # Set the internal timestamp
            internal_timestamp_unix = numpy.float(self.raw_data.group(
                DataMatchesGroupNumber.PROFILER_TIMESTAMP))
            self.set_internal_timestamp(unix_time=internal_timestamp_unix)

        # We shouldn't end up with an exception due to the strongly specified regex, but we
        # will ensure we catch any potential errors just in case
        except (ValueError, TypeError, IndexError) as ex:
            log.warn("Exception when building parsed values")
            raise RecoverableSampleException("Error (%s) while decoding parameters in data: [%s]"
                                             % (ex, self.raw_data))

        return results


class DbgPdbgRecoveredBatteryParticle(DbgPdbgBatteryParticle):
    """
    Class for building a dbg pdbg recovered engineering data particle
    """

    _data_particle_type = DbgPdbgDataParticleType.BATTERY_RECOVERED


class DbgPdbgTelemeteredBatteryParticle(DbgPdbgBatteryParticle):
    """
    Class for building a dbg pdbg telemetered engineering data particle
    """

    _data_particle_type = DbgPdbgDataParticleType.BATTERY_TELEMETERED


class DbgPdbgGpsParticle(DataParticle):
    """
    Class for parsing data from the dbg pdbg engineering data set
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws RecoverableSampleException If there is a problem with sample creation
        """
        results = []

        try:

            # Process each of the common particle parameters
            for name, group, function in COMMON_PARTICLE_ENCODING_RULES:
                results.append(self._encode_value(name, self.raw_data.group(group), function))

            results.append(self._encode_value(DbgPdbgGpsParticleKey.GPS_ADJUSTMENT,
                                              self.raw_data.group(DataMatchesGroupNumber.GPS_ADJUSTMENT),
                                              int))

            # Set the internal timestamp
            internal_timestamp_unix = numpy.float(self.raw_data.group(
                DataMatchesGroupNumber.PROFILER_TIMESTAMP))
            self.set_internal_timestamp(unix_time=internal_timestamp_unix)

        # We shouldn't end up with an exception due to the strongly specified regex, but we
        # will ensure we catch any potential errors just in case
        except (ValueError, TypeError, IndexError) as ex:
            log.warn("Exception when building parsed values")
            raise RecoverableSampleException("Error (%s) while decoding parameters in data: [%s]"
                                             % (ex, self.raw_data))

        return results


class DbgPdbgRecoveredGpsParticle(DbgPdbgGpsParticle):
    """
    Class for building a dbg pdbg recovered engineering data particle
    """

    _data_particle_type = DbgPdbgDataParticleType.GPS_RECOVERED


class DbgPdbgTelemeteredGpsParticle(DbgPdbgGpsParticle):
    """
    Class for building a dbg pdbg telemetered engineering data particle
    """

    _data_particle_type = DbgPdbgDataParticleType.GPS_TELEMETERED


class DbgPdbgCsppParser(SimpleParser):
    """
    Parser for the dbg_pdbg engineering data part of the cspp_eng_cspp driver
    This Parser is based on the cspp_base parser, modified to handle
    the multiple data particles of the dbg_pdbg
    """

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        """
        This method is a constructor that will instantiate an DbgPdbgCsppParser object.
        @param config The configuration for this DbgPdbgCsppParser parser
        @param stream_handle The handle to the data stream containing the cspp data
        @param exception_callback The function to call to report exceptions
        """

        # Build up the header state dictionary using the default header key list
        self._header_state = {}

        header_key_list = DEFAULT_HEADER_KEY_LIST
        #
        for header_key in header_key_list:
            self._header_state[header_key] = None

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)

            # Set the metadata and data particle classes to be used later

            if METADATA_PARTICLE_CLASS_KEY in particle_classes_dict and \
               BATTERY_STATUS_CLASS_KEY in particle_classes_dict and \
               GPS_ADJUSTMENT_CLASS_KEY in particle_classes_dict:

                self._metadata_particle_class = particle_classes_dict.get(METADATA_PARTICLE_CLASS_KEY)

                self._battery_status_class = particle_classes_dict.get(BATTERY_STATUS_CLASS_KEY)
                self._gps_adjustment_class = particle_classes_dict.get(GPS_ADJUSTMENT_CLASS_KEY)

            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
        else:
            log.warning('Configuration missing particle classes dict')
            raise ConfigurationException('Configuration missing particle classes dict')

        # Call the superclass constructor
        super(DbgPdbgCsppParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback)

        self._metadata_extracted = False

    def parse_file(self):

        for line in self._stream_handle:

            battery_match = BATTERY_DATA_MATCHER.match(line)

            gps_match = GPS_DATA_MATCHER.match(line)

            # If we found a data match, let's process it
            if battery_match is not None:
                self._process_data_match(self._battery_status_class, battery_match)

            elif gps_match is not None:
                self._process_data_match(self._gps_adjustment_class, gps_match)

            else:
                # Check for head part match
                header_part_match = HEADER_PART_MATCHER.match(line)

                if header_part_match is not None:
                    self._process_header_part_match(header_part_match)
                elif HEX_ASCII_LINE_MATCHER.match(line):
                    self._process_line_not_containing_data_record_or_header_part(line)
                elif not TIMESTAMP_LINE_MATCHER.match(line) and not \
                        (IGNORE_MATCHER is not None and IGNORE_MATCHER.match(line)):
                    log.warn("non_data: %s", line)
                    self._exception_callback(RecoverableSampleException("Found d bytes"))

    def _process_data_match(self, particle_class, data_match):
        """
        This method processes a data match.  It will extract a metadata particle and insert it into
         result_particles when we have not already extracted the metadata and all header values exist.
         This method will also extract a data particle and append it to the result_particles.
        @param particle_class is the class of particle to be created
        @param data_match A regular expression match object for a cspp data record
        """

        # Extract the data record particle
        data_particle = self._extract_sample(particle_class,
                                             None,
                                             data_match,
                                             None)

        # If we created a data particle, let's append the particle to the result particles
        # to return and increment the state data positioning
        if data_particle:

            if not self._metadata_extracted:
                # once the first data particle is read, all header lines should have
                # also been read

                # Source File is the only part of the header that is required
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
                        self._record_buffer.insert(0, metadata_particle)
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

            self._record_buffer.append(data_particle)

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
        @param line a line of data to be processed that does not contain a data record or header
        """

        # Check for the expected timestamp line we will ignore
        # timestamp_line_match = TIMESTAMP_LINE_MATCHER.match(chunk)
        # Check for other status messages we can ignore
        ignore_match = IGNORE_MATCHER.match(line)

        if ignore_match is not None:
            # Ignore
            pass

        else:

            # OK.  We got unexpected data
            log.warn('got unrecognized row %s', line)
            self._exception_callback(RecoverableSampleException("Found an invalid line: %s" % line))
