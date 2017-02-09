"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/nutnr_j_cspp.py
@author Emily Hahn
@brief Parser for the nutnr_j_cspp dataset driver
Release notes:

initial release
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re
import copy
import string

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import \
    RecoverableSampleException,\
    ConfigurationException

from mi.dataset.parser.common_regexes import \
    END_OF_LINE_REGEX, \
    INT_REGEX

from mi.dataset.parser.cspp_base import \
    CsppMetadataDataParticle, \
    HeaderPartMatchesGroupNumber, \
    encode_y_or_n, \
    MetadataRawDataKey, \
    Y_OR_N_REGEX, \
    HEADER_PART_MATCHER, \
    TIMESTAMP_LINE_MATCHER, \
    HEX_ASCII_LINE_MATCHER, \
    DEFAULT_HEADER_KEY_LIST, \
    DefaultHeaderKey

from mi.dataset.dataset_parser import \
    SimpleParser, \
    DataSetDriverConfigKeys

FLOAT_REGEX_NON_CAPTURE = r'[+-]?[0-9]*\.[0-9]+'
FLOAT_TAB_REGEX = FLOAT_REGEX_NON_CAPTURE + '\t'

# can't use groups for each parameter here since this is over the 100 group limit
# use groups to match repeated regexes, split by tabs to get parameter values instead
LINE_START_REGEX = FLOAT_TAB_REGEX + FLOAT_TAB_REGEX + Y_OR_N_REGEX + '\t'
DATA_REGEX = LINE_START_REGEX
DATA_REGEX += '[a-zA-Z]+\t'     # Frame Type
DATA_REGEX += '\d+\t\d+\t'      # year, day of year
DATA_REGEX += '(' + FLOAT_TAB_REGEX + '){6}'    # match 6 floats separated by tabs
DATA_REGEX += '(?:\d+\t){259}'  # match 259 ints separated by tabs, non capturing group due to number of groups
DATA_REGEX += '(' + FLOAT_TAB_REGEX + '){3}'    # match 3 floats separated by tabs
DATA_REGEX += '(' + INT_REGEX + ')\t'   # lamp time
DATA_REGEX += '(' + FLOAT_TAB_REGEX + '){10}'   # match 10 floats separated by tabs
DATA_REGEX += '(\d+)\t'         # ctd time
DATA_REGEX += '(' + FLOAT_TAB_REGEX + '){3}'    # match 3 floats separated by tabs
DATA_REGEX += '[0-9a-fA-F]{2}' + END_OF_LINE_REGEX  # checksum and line end

DATA_MATER = re.compile(DATA_REGEX)

NUMBER_CHANNELS = 256
NUM_FIELDS = 33 + NUMBER_CHANNELS

# index into split string parameter values
GRP_PROFILER_TIMESTAMP = 0
GRP_SPECTRAL_START = 15
GRP_SPECTRAL_END = GRP_SPECTRAL_START + NUMBER_CHANNELS

# ignore lines matching the start (timestamp, depth, suspect timestamp),
# then any text not containing tabs
IGNORE_LINE_REGEX = LINE_START_REGEX + '[^\t]*' + END_OF_LINE_REGEX
IGNORE_MATCHER = re.compile(IGNORE_LINE_REGEX)

LIGHT_PARTICLE_CLASS_KEY = 'light_particle_class'
DARK_PARTICLE_CLASS_KEY = 'dark_particle_class'
METADATA_PARTICLE_CLASS_KEY = 'metadata_particle_class'


class DataParticleType(BaseEnum):
    LIGHT_INSTRUMENT = 'nutnr_j_cspp_instrument'
    LIGHT_INSTRUMENT_RECOVERED = 'nutnr_j_cspp_instrument_recovered'
    DARK_INSTRUMENT = 'nutnr_j_cspp_dark_instrument'
    DARK_INSTRUMENT_RECOVERED = 'nutnr_j_cspp_dark_instrument_recovered'
    METADATA = 'nutnr_j_cspp_metadata'
    METADATA_RECOVERED = 'nutnr_j_cspp_metadata_recovered'

LIGHT_SPECTRAL_CHANNELS = 'spectral_channels'               # PD332
DARK_SPECTRAL_CHANNELS = 'dark_frame_spectral_channels'     # PD3799


BASE_PARAMETER_MAP = [
    ('profiler_timestamp',             0, float),
    ('pressure_depth',                 1, float),
    ('suspect_timestamp',              2, encode_y_or_n),
    ('frame_type',                     3, str),
    ('year',                           4, int),
    ('day_of_year',                    5, int),
    ('time_of_sample',                 6, float),
    ('nitrate_concentration',          7, float),
    ('nutnr_nitrogen_in_nitrate',      8, float),
    ('nutnr_absorbance_at_254_nm',     9, float),
    ('nutnr_absorbance_at_350_nm',    10, float),
    ('nutnr_bromide_trace',           11, float),
    ('nutnr_spectrum_average',        12, int),
    ('nutnr_dark_value_used_for_fit', 13, int),
    ('nutnr_integration_time_factor', 14, int),
    ('temp_interior',                 GRP_SPECTRAL_END,      float),
    ('temp_spectrometer',             GRP_SPECTRAL_END + 1,  float),
    ('temp_lamp',                     GRP_SPECTRAL_END + 2,  float),
    ('lamp_time',                     GRP_SPECTRAL_END + 3,  int),
    ('humidity',                      GRP_SPECTRAL_END + 4,  float),
    ('voltage_main',                  GRP_SPECTRAL_END + 5,  float),
    ('voltage_lamp',                  GRP_SPECTRAL_END + 6,  float),
    ('nutnr_voltage_int',             GRP_SPECTRAL_END + 7,  float),
    ('nutnr_current_main',            GRP_SPECTRAL_END + 8,  float),
    ('aux_fitting_1',                 GRP_SPECTRAL_END + 9,  float),
    ('aux_fitting_2',                 GRP_SPECTRAL_END + 10, float),
    ('nutnr_fit_base_1',              GRP_SPECTRAL_END + 11, float),
    ('nutnr_fit_base_2',              GRP_SPECTRAL_END + 12, float),
    ('nutnr_fit_rmse',                GRP_SPECTRAL_END + 13, float),
    ('ctd_time_uint32',               GRP_SPECTRAL_END + 14, int),
    ('ctd_psu',                       GRP_SPECTRAL_END + 15, float),
    ('ctd_temp',                      GRP_SPECTRAL_END + 16, float),
    ('ctd_dbar',                      GRP_SPECTRAL_END + 17, float)]

LIGHT_PARAMETER_MAP = copy.copy(BASE_PARAMETER_MAP)
LIGHT_PARAMETER_MAP.append((LIGHT_SPECTRAL_CHANNELS, GRP_SPECTRAL_START, list))

DARK_PARAMETER_MAP = copy.copy(BASE_PARAMETER_MAP)
DARK_PARAMETER_MAP.append((DARK_SPECTRAL_CHANNELS, GRP_SPECTRAL_START, list))


class NutnrJCsppMetadataDataParticle(CsppMetadataDataParticle):
    """
    Class for parsing metadata from the nutnr_j_cspp data set
    """
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws RecoverableSampleException If there is a problem with sample creation
        """

        # this particle only contains common metadata values
        results = self._build_metadata_parsed_values()

        data_match = self.raw_data[MetadataRawDataKey.DATA_MATCH]

        # split raw data match by tabs to be able to isolate profiler timestamp
        params = data_match.group(0).split('\t')

        internal_timestamp_unix = float(params[GRP_PROFILER_TIMESTAMP])
        self.set_internal_timestamp(unix_time=internal_timestamp_unix)

        return results


class NutnrJCsppMetadataTelemeteredDataParticle(NutnrJCsppMetadataDataParticle):
    """ Class for building a telemetered data sample parser """
    _data_particle_type = DataParticleType.METADATA


class NutnrJCsppMetadataRecoveredDataParticle(NutnrJCsppMetadataDataParticle):
    """ Class for building a recovered data sample parser """
    _data_particle_type = DataParticleType.METADATA_RECOVERED


class NutnrJCsppDataParticle(DataParticle):
    """
    Class for parsing data from the nutnr_j_cspp data set
    """
    _parameter_map = None
    _spectral_channels = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws RecoverableSampleException If there is a problem with sample creation
        """
        results = []

        # split the entire matched line by tabs, which will return each parameters
        # value as an array of string
        params = self.raw_data.group(0).split('\t')
        if len(params) < NUM_FIELDS:
            log.warn('Not enough fields could be parsed from the data %s',
                     self.raw_data.group(0))
            raise RecoverableSampleException('Not enough fields could be parsed from the data %s' %
                                             self.raw_data.group(0))

        for name, index, encode_function in self._parameter_map:
            if name == self._spectral_channels:
                # spectral channels is an array of ints, need to do the extra map
                results.append(self._encode_value(name,
                                                  map(int, params[index:GRP_SPECTRAL_END]),
                                                  encode_function))
            else:
                results.append(self._encode_value(name, params[index], encode_function))

        internal_timestamp_unix = float(params[GRP_PROFILER_TIMESTAMP])
        self.set_internal_timestamp(unix_time=internal_timestamp_unix)

        return results


class NutnrJCsppTelemeteredDataParticle(NutnrJCsppDataParticle):
    """ Class for building a telemetered data sample parser """

    _data_particle_type = DataParticleType.LIGHT_INSTRUMENT
    _spectral_channels = LIGHT_SPECTRAL_CHANNELS
    _parameter_map = LIGHT_PARAMETER_MAP


class NutnrJCsppRecoveredDataParticle(NutnrJCsppDataParticle):
    """ Class for building a recovered data sample parser """
    _data_particle_type = DataParticleType.LIGHT_INSTRUMENT_RECOVERED
    _spectral_channels = LIGHT_SPECTRAL_CHANNELS
    _parameter_map = LIGHT_PARAMETER_MAP


class NutnrJCsppDarkTelemeteredDataParticle(NutnrJCsppDataParticle):
    """ Class for building a telemetered data sample parser """

    _data_particle_type = DataParticleType.DARK_INSTRUMENT
    _spectral_channels = DARK_SPECTRAL_CHANNELS
    _parameter_map = DARK_PARAMETER_MAP


class NutnrJCsppDarkRecoveredDataParticle(NutnrJCsppDataParticle):
    """ Class for building a recovered data sample parser """
    _data_particle_type = DataParticleType.DARK_INSTRUMENT_RECOVERED
    _spectral_channels = DARK_SPECTRAL_CHANNELS
    _parameter_map = DARK_PARAMETER_MAP


class NutnrJCsppParser(SimpleParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        """
        This the constructor which instantiates the NutnrJCsppParser
        """

        # Build up the header state dictionary using the default her key list ot one that was provided
        self._header_state = {}
        header_key_list = DEFAULT_HEADER_KEY_LIST

        for header_key in header_key_list:
            self._header_state[header_key] = None

        # Initialize the metadata flag
        self._metadata_extracted = False

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
            # Set the metadata and data particle classes to be used later

            if METADATA_PARTICLE_CLASS_KEY in particle_classes_dict and \
               LIGHT_PARTICLE_CLASS_KEY in particle_classes_dict and \
               DARK_PARTICLE_CLASS_KEY in particle_classes_dict:

                self._light_particle_class = particle_classes_dict.get(LIGHT_PARTICLE_CLASS_KEY)
                self._dark_particle_class = particle_classes_dict.get(DARK_PARTICLE_CLASS_KEY)
                self._metadata_particle_class = particle_classes_dict.get(METADATA_PARTICLE_CLASS_KEY)
            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
        else:
            log.warning('Configuration missing particle classes dict')
            raise ConfigurationException('Configuration missing particle classes dict')

        # call the superclass constructor
        super(NutnrJCsppParser, self).__init__(config,
                                               stream_handle,
                                               exception_callback)

    def _process_data_match(self, data_match):
        """
        This method processes a data match.  It will extract a metadata particle and insert it into
        the record_buffer when we have not already extracted the metadata and all header values exist.
        This method will also extract a data particle and append it to the the record_buffer.
        @param data_match A regular expression match object for a cspp data record
        """

        params = data_match.group(0).split('\t')
        frame_type = params[3]
        data_particle = None

        # Extract the data record particle
        if frame_type == 'SLB':  # light frame
            data_particle = self._extract_sample(self._light_particle_class,
                                                 None,
                                                 data_match,
                                                 None)
        elif frame_type == 'SDB':  # dark frame
            data_particle = self._extract_sample(self._dark_particle_class,
                                                 None,
                                                 data_match,
                                                 None)
        else:
            log.warn('got invalid frame type %s', frame_type)
            self._exception_callback(RecoverableSampleException('got invalid frame type %s' % frame_type))

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

    def parse_file(self):
        """
        Parse NUTNR J CSPP text file.
        """

        # loop over all lines in the data file and parse the data to generate Winch CSPP particles
        for line in self._stream_handle:

            data_match = DATA_MATER.match(line)

            # If we found a data match, let's process it
            if data_match is not None:
                self._process_data_match(data_match)

            else:
                # Check for head part match
                header_part_match = HEADER_PART_MATCHER.match(line)

                if header_part_match is not None:
                    header_part_key = header_part_match.group(
                        HeaderPartMatchesGroupNumber.HEADER_PART_MATCH_GROUP_KEY)
                    header_part_value = header_part_match.group(
                        HeaderPartMatchesGroupNumber.HEADER_PART_MATCH_GROUP_VALUE)

                    if header_part_key in self._header_state.keys():
                        self._header_state[header_part_key] = string.rstrip(header_part_value)

                else:
                    if HEX_ASCII_LINE_MATCHER.match(line):
                        # we found a line starting with the timestamp, depth, and
                        # suspect timestamp, followed by all hex ascii chars
                        log.warn('got hex ascii corrupted data %s ', line)
                        self._exception_callback(RecoverableSampleException(
                            "Found hex ascii corrupted data: %s" % line))

                    # ignore the expected timestamp line and any lines matching the ignore regex,
                    # otherwise data is unexpected
                    elif not TIMESTAMP_LINE_MATCHER.match(line) and not \
                            (IGNORE_MATCHER is not None and IGNORE_MATCHER.match(line)):
                        # Unexpected data was found
                        log.warn('got unrecognized row %s', line)
                        self._exception_callback(RecoverableSampleException("Found an invalid chunk: %s" % line))


