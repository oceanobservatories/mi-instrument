#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/flort_dj_cspp.py
@author Jeremy Amundson
@brief Parser for the flort_dj_cspp dataset driver
Release notes:

initial release
"""

__author__ = 'Jeremy Amundson'
__license__ = 'Apache 2.0'

import numpy

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
import re

from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey

from mi.dataset.parser.common_regexes import INT_REGEX, FLOAT_REGEX, MULTIPLE_TAB_REGEX, END_OF_LINE_REGEX

from mi.dataset.parser.cspp_base import \
    CsppParser, \
    CsppMetadataDataParticle, \
    MetadataRawDataKey, \
    Y_OR_N_REGEX, encode_y_or_n

from mi.dataset.parser.utilities import timestamp_mmddyyhhmmss_to_ntp

# A regex to match a date in MM/DD/YY format
FORMATTED_DATE_REGEX = r'\d{2}/\d{2}/\d{2}'

# A regex to match a time stamp in HH:MM:SS format
TIME_REGEX = r'\d{2}:\d{2}:\d{2}'

# A regular expression that should match a flort_dj data record
DATA_REGEX = '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # Profiler Timestamp
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # Depth
DATA_REGEX += '(' + Y_OR_N_REGEX + ')' + MULTIPLE_TAB_REGEX  # suspect timestamp
DATA_REGEX += '(' + FORMATTED_DATE_REGEX + ')' + MULTIPLE_TAB_REGEX  # date string
DATA_REGEX += '(' + TIME_REGEX + ')' + MULTIPLE_TAB_REGEX  # time string
DATA_REGEX += '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX  # measurement_wavelength_beta
DATA_REGEX += '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX  # raw_signal_beta
DATA_REGEX += '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX  # measurement_wavelength_chl
DATA_REGEX += '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX  # raw_signal_chl
DATA_REGEX += '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX  # measurement_wavelength_cdom
DATA_REGEX += '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX  # raw_signal_cdom
DATA_REGEX += '(' + INT_REGEX + ')' + END_OF_LINE_REGEX  # raw_internal_temp

IGNORE_REGEX = FLOAT_REGEX + MULTIPLE_TAB_REGEX  # Profiler Timestamp
IGNORE_REGEX += FLOAT_REGEX + MULTIPLE_TAB_REGEX  # Depth
IGNORE_REGEX += Y_OR_N_REGEX + MULTIPLE_TAB_REGEX  # Suspect Timestamp
IGNORE_REGEX += r'[^\t]*' + END_OF_LINE_REGEX  # any text after the Suspect

IGNORE_MATCHER = re.compile(IGNORE_REGEX)


class DataMatchesGroupNumber(BaseEnum):
    """
    An enum for group match indices for a data record only chunk.
    """

    PROFILER_TIMESTAMP = 1
    PRESSURE = 2
    SUSPECT_TIMESTAMP = 3
    DATE = 4
    TIME = 5
    BETA = 6
    RAW_BETA = 7
    CHLOROPHYLL = 8
    RAW_CHLOROPHYLL = 9
    CDOM = 10
    RAW_CDOM = 11
    TEMP = 12


class DataParticleType(BaseEnum):
    """
    The data particle types that a flort_dj_cspp parser could generate
    """
    METADATA_RECOVERED = 'flort_dj_cspp_metadata_recovered'
    INSTRUMENT_RECOVERED = 'flort_sample'
    METADATA_TELEMETERED = 'flort_dj_cspp_metadata'
    INSTRUMENT_TELEMETERED = 'flort_sample'


class FlortDjCsppParserDataParticleKey(BaseEnum):
    """
    The data particle keys associated with flort_dj_cspp data particle parameters
    """
    PROFILER_TIMESTAMP = 'profiler_timestamp'
    PRESSURE = 'pressure_depth'
    SUSPECT_TIMESTAMP = 'suspect_timestamp'
    DATE = 'date_string'
    TIME = 'time_string'
    BETA = 'measurement_wavelength_beta'
    RAW_BETA = 'raw_signal_beta'
    CHLOROPHYLL = 'measurement_wavelength_chl'
    RAW_CHLOROPHYLL = 'raw_signal_chl'
    CDOM = 'measurement_wavelength_cdom'
    RAW_CDOM = 'raw_signal_cdom'
    TEMP = 'raw_internal_temp'

# A group of instrument data particle encoding rules used to simplify encoding using a loop
INSTRUMENT_PARTICLE_ENCODING_RULES = [
    (FlortDjCsppParserDataParticleKey.PRESSURE, DataMatchesGroupNumber.PRESSURE, float),
    (FlortDjCsppParserDataParticleKey.SUSPECT_TIMESTAMP, DataMatchesGroupNumber.SUSPECT_TIMESTAMP, encode_y_or_n),
    (FlortDjCsppParserDataParticleKey.BETA, DataMatchesGroupNumber.BETA, int),
    (FlortDjCsppParserDataParticleKey.RAW_BETA, DataMatchesGroupNumber.RAW_BETA, int),
    (FlortDjCsppParserDataParticleKey.CHLOROPHYLL, DataMatchesGroupNumber.CHLOROPHYLL, int),
    (FlortDjCsppParserDataParticleKey.RAW_CHLOROPHYLL, DataMatchesGroupNumber.RAW_CHLOROPHYLL, int),
    (FlortDjCsppParserDataParticleKey.CDOM, DataMatchesGroupNumber.CDOM, int),
    (FlortDjCsppParserDataParticleKey.RAW_CDOM, DataMatchesGroupNumber.RAW_CDOM, int),
    (FlortDjCsppParserDataParticleKey.TEMP, DataMatchesGroupNumber.RAW_CDOM, int)
]


class FlortDjCsppMetadataDataParticle(CsppMetadataDataParticle):
    """
    Class for building a flort_dj_cspp metadata particle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        results = []

        # Append the base metadata parsed values to the results to return
        results += self._build_metadata_parsed_values()

        data_match = self.raw_data[MetadataRawDataKey.DATA_MATCH]

        port_timestamp_unix = numpy.float(data_match.group(
            DataMatchesGroupNumber.PROFILER_TIMESTAMP))
        self.set_port_timestamp(unix_time=float(port_timestamp_unix))

        self.contents[DataParticleKey.PREFERRED_TIMESTAMP] = DataParticleKey.PORT_TIMESTAMP

        timestamp_str= data_match.group(DataMatchesGroupNumber.DATE) + " " + data_match.group(DataMatchesGroupNumber.TIME)
        self.set_internal_timestamp(timestamp=timestamp_mmddyyhhmmss_to_ntp(timestamp_str))

        return results


class FlortDjCsppMetadataRecoveredDataParticle(FlortDjCsppMetadataDataParticle):
    """
    Class for building a flort_dj_cspp recovered metadata particle
    """

    _data_particle_type = DataParticleType.METADATA_RECOVERED


class FlortDjCsppMetadataTelemeteredDataParticle(FlortDjCsppMetadataDataParticle):
    """
    Class for building a flort_dj_cspp telemetered metadata particle
    """

    _data_particle_type = DataParticleType.METADATA_TELEMETERED


class FlortDjCsppInstrumentDataParticle(DataParticle):
    """
    Class for building a flort_dj_cspp instrument data particle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        results = []

        # Process each of the instrument particle parameters
        for (name, index, encoding) in INSTRUMENT_PARTICLE_ENCODING_RULES:

            results.append(self._encode_value(name, self.raw_data.group(index), encoding))

        port_timestamp_unix = numpy.float(self.raw_data.group(
            DataMatchesGroupNumber.PROFILER_TIMESTAMP))
        self.set_port_timestamp(unix_time=float(port_timestamp_unix))

        self.contents[DataParticleKey.PREFERRED_TIMESTAMP] = DataParticleKey.PORT_TIMESTAMP

        timestamp_str= self.raw_data.group(DataMatchesGroupNumber.DATE) + " " + self.raw_data.group(DataMatchesGroupNumber.TIME)
        self.set_internal_timestamp(timestamp=timestamp_mmddyyhhmmss_to_ntp(timestamp_str))

        return results


class FlortDjCsppInstrumentRecoveredDataParticle(FlortDjCsppInstrumentDataParticle):
    """
    Class for building a flort_dj_cspp recovered instrument data particle
    """

    _data_particle_type = DataParticleType.INSTRUMENT_RECOVERED


class FlortDjCsppInstrumentTelemeteredDataParticle(FlortDjCsppInstrumentDataParticle):
    """
    Class for building a flort_dj_cspp telemetered instrument data particle
    """

    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED


class FlortDjCsppParser(CsppParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        """
        This method is a constructor that will instantiate an FlortDjCsppParser object.
        @param config The configuration for this FlortDjCsppParser parser
        @param stream_handle The handle to the data stream containing the flort_dj_cspp data
        @param exception_callback The function to call to report exceptions
        """

        # Call the superclass constructor
        super(FlortDjCsppParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback,
                                                DATA_REGEX,
                                                ignore_matcher=IGNORE_MATCHER)
