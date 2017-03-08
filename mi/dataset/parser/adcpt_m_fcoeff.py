#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_m_fcoeff
@file marine-integrations/mi/dataset/parser/adcpt_m_fcoeff.py
@author Ronald Ronquillo
@brief Parser for the adcpt_m_fcoeff dataset driver

This file contains code for the adcpt_m_fcoeff parser and code to produce data particles

Fourier Coefficients data files (FCoeff*.txt) are space-delimited ASCII (with leading spaces).
FCoeff files contain leader rows containing English readable text.
Subsequent rows in FCoeff contain float data.
The file contains data for a single burst/record of pings.
Mal-formed sensor data records produce no particles.

The sensor data record has the following format:

% Fourier Coefficients
% <NumFields> Fields and <NumFreq> Frequencies
% Frequency(Hz), Band width(Hz), Energy density(m^2/Hz), Direction (deg), A1, B1, A2, B2, Check Factor
% Frequency Bands are <Fw_band> Hz wide(first frequency band is centered at <F0>)
<frequency_band[0]> <bandwidth_band[0]> <energy_density_band[0]> <direction_band[0]> <a1_band[0]> <b1_band[0]> <a2_band[0]> <b2_band[0]> <check_factor_band[0]>
<frequency_band[1]> <bandwidth_band[1]> <energy_density_band[1]> <direction_band[1]> <a1_band[1]> <b1_band[1]> <a2_band[1]> <b2_band[1]> <check_factor_band[1]>
...
<frequency_band[NumFreq]> <bandwidth_band[NumFreq]> <energy_density_band[NumFreq]> <direction_band[NumFreq]> <a1_band[NumFreq]> <b1_band[NumFreq]> <a2_band[NumFreq]> <b2_band[NumFreq]> <check_factor_band[NumFreq]>


Release notes:

Initial Release
"""

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


import ntplib
import re
from itertools import chain

from mi.dataset.parser import utilities

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.core.common import BaseEnum

from mi.core.instrument.dataset_data_particle import DataParticle

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.common_regexes import \
    UNSIGNED_INT_REGEX, \
    FLOAT_REGEX, \
    END_OF_LINE_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, \
    ANY_CHARS_REGEX


class AdcptMFCoeffParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    FILE_TIME = "file_time"
    NUM_FIELDS = "num_fields"
    NUM_FREQ = "num_freq"
    FREQ_W_BAND = "freq_w_band"
    FREQ_0 = "freq_0"
    FREQ_BAND = "frequency_band"
    BANDWIDTH_BAND = "bandwidth_band"
    ENERGY_BAND = "energy_density_band"
    DIR_BAND = "direction_band"
    A1_BAND = "a1_band"
    B1_BAND = "b1_band"
    A2_BAND = "a2_band"
    B2_BAND = "b2_band"
    CHECK_BAND = "check_factor_band"

# Basic patterns
common_matches = {
    'FLOAT': FLOAT_REGEX,
    'UINT': UNSIGNED_INT_REGEX,
    'ANY_CHARS_REGEX': ANY_CHARS_REGEX,
    'ONE_OR_MORE_WHITESPACE': ONE_OR_MORE_WHITESPACE_REGEX,
    'START_METADATA': '\s*\%\s',            # regex for identifying start of a header line
    'END_OF_LINE_REGEX': END_OF_LINE_REGEX
}

# Add together the particle keys and the common matches dictionaries
# for use as variables in the regexes defined below.
common_matches.update(AdcptMFCoeffParticleKey.__dict__)

# regex for identifying an empty line
EMPTY_LINE_MATCHER = re.compile(END_OF_LINE_REGEX, re.DOTALL)

# FCoeff Filename timestamp format
TIMESTAMP_FORMAT = "%y%m%d%H%M"

# Regex to extract the timestamp from the FCoeff log file path (path/to/FCoeffYYMMDDHHmm.txt)
FILE_NAME_MATCHER = re.compile(r"""(?x)
    .+FCoeff(?P<%(FILE_TIME)s> %(UINT)s)\.txt
    """ % common_matches, re.VERBOSE | re.DOTALL)

# Header data:
# Metadata starts with '%' or ' %' followed by text &  newline, ie:
# % Fourier Coefficients
# % Frequency(Hz), Band width(Hz), Energy density(m^2/Hz), Direction (deg), A1, B1, A2, B2, Check Factor
HEADER_MATCHER = re.compile(r"""(?x)
    %(START_METADATA)s %(ANY_CHARS_REGEX)s %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)

# Extract num_fields and num_freq from the following metadata line
# % 9 Fields and 64 Frequencies
DIR_FREQ_MATCHER = re.compile(r"""(?x)
    %(START_METADATA)s
    (?P<%(NUM_FIELDS)s> %(UINT)s) %(ONE_OR_MORE_WHITESPACE)s Fields\sand %(ONE_OR_MORE_WHITESPACE)s
    (?P<%(NUM_FREQ)s>   %(UINT)s) %(ONE_OR_MORE_WHITESPACE)s Frequencies %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)

# Extract freq_w_band and freq_0 from the following metadata line
# % Frequency Bands are 0.01562500 Hz wide(first frequency band is centered at 0.00830078)
FREQ_BAND_MATCHER = re.compile(r"""(?x)
    %(START_METADATA)s
    Frequency\sBands\sare\s (?P<%(FREQ_W_BAND)s> %(FLOAT)s)
    %(ONE_OR_MORE_WHITESPACE)s Hz\swide
    \(first\sfrequency\sband\sis\scentered\sat\s (?P<%(FREQ_0)s> %(FLOAT)s) \) %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)

# List of possible matchers for header data
HEADER_MATCHER_LIST = [DIR_FREQ_MATCHER, FREQ_BAND_MATCHER]

# Regex for identifying a single record of FCoeff data, ie:
#  0.008789 0.015625 0.003481 211.254501 -0.328733 -0.199515 -0.375233 0.062457 0.352941
FCOEFF_DATA_MATCHER = re.compile(r"""(?x)
    \s(?P<%(FREQ_BAND)s>      %(FLOAT)s)
    \s(?P<%(BANDWIDTH_BAND)s> %(FLOAT)s)
    \s(?P<%(ENERGY_BAND)s>    %(FLOAT)s)
    \s(?P<%(DIR_BAND)s>       %(FLOAT)s)
    \s(?P<%(A1_BAND)s>        %(FLOAT)s)
    \s(?P<%(B1_BAND)s>        %(FLOAT)s)
    \s(?P<%(A2_BAND)s>        %(FLOAT)s)
    \s(?P<%(B2_BAND)s>        %(FLOAT)s)
    \s(?P<%(CHECK_BAND)s>     %(FLOAT)s)
    %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)

# The following is used for _build_parsed_values() and defined as below:
# (parameter name (and also index into parsed_dict), encoding function)
FCOEFF_ENCODING_RULES = [
    (AdcptMFCoeffParticleKey.FILE_TIME,         str),
    (AdcptMFCoeffParticleKey.NUM_FIELDS,        int),
    (AdcptMFCoeffParticleKey.NUM_FREQ,          int),
    (AdcptMFCoeffParticleKey.FREQ_W_BAND,       float),
    (AdcptMFCoeffParticleKey.FREQ_0,            float),
    (AdcptMFCoeffParticleKey.FREQ_BAND,         lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.BANDWIDTH_BAND,    lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.ENERGY_BAND,       lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.DIR_BAND,          lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.A1_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.B1_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.A2_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.B2_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMFCoeffParticleKey.CHECK_BAND,        lambda x: [float(y) for y in x])
]


class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the adcpt_m FCoeff recovered data
    """
    SAMPLE = 'adcpt_m_instrument_fcoeff_recovered'  # instrument data particle


class AdcptMFCoeffInstrumentDataParticle(DataParticle):
    """
    Class for generating the adcpt_m_instrument_fcoeff_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name, which is also
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[name], function)
                for name, function in FCOEFF_ENCODING_RULES]


class AdcptMFCoeffParser(SimpleParser):
    """
    Parser for adcpt_m FCoeff*.txt files.
    """

    def recov_exception_callback(self, message):
        log.warn(message)
        self._exception_callback(RecoverableSampleException(message))

    def parse_file(self):
        """
        Parse the FCoeff*.txt file. Create a chunk from valid data in the file.
        Build a data particle from the chunk.
        """

        file_time_dict = {}
        dir_freq_dict = {}
        freq_band_dict = {}
        sensor_data_dict = {
            AdcptMFCoeffParticleKey.FREQ_BAND: [],
            AdcptMFCoeffParticleKey.BANDWIDTH_BAND: [],
            AdcptMFCoeffParticleKey.ENERGY_BAND: [],
            AdcptMFCoeffParticleKey.DIR_BAND: [],
            AdcptMFCoeffParticleKey.A1_BAND: [],
            AdcptMFCoeffParticleKey.B1_BAND: [],
            AdcptMFCoeffParticleKey.A2_BAND: [],
            AdcptMFCoeffParticleKey.B2_BAND: [],
            AdcptMFCoeffParticleKey.CHECK_BAND: []
        }

        # Extract the file time from the file name
        input_file_name = self._stream_handle.name

        match = FILE_NAME_MATCHER.match(input_file_name)

        if match:
            file_time_dict = match.groupdict()
        else:
            self.recov_exception_callback(
                'Unable to extract file time from FCoeff input file name: %s ' % input_file_name)

        # read the first line in the file
        line = self._stream_handle.readline()

        while line:

            if EMPTY_LINE_MATCHER.match(line):
                # ignore blank lines, do nothing
                pass

            elif HEADER_MATCHER.match(line):
                # we need header records to extract useful information
                for matcher in HEADER_MATCHER_LIST:
                    header_match = matcher.match(line)

                    if header_match is not None:

                        if matcher is DIR_FREQ_MATCHER:
                            dir_freq_dict = header_match.groupdict()

                        elif matcher is FREQ_BAND_MATCHER:
                            freq_band_dict = header_match.groupdict()

                        else:
                            #ignore
                            pass

            elif FCOEFF_DATA_MATCHER.match(line):
                # Extract a row of data
                sensor_match = FCOEFF_DATA_MATCHER.match(line)

                sensor_data_dict[AdcptMFCoeffParticleKey.FREQ_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.FREQ_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.BANDWIDTH_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.BANDWIDTH_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.ENERGY_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.ENERGY_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.DIR_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.DIR_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.A1_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.A1_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.B1_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.B1_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.A2_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.A2_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.B2_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.B2_BAND))
                sensor_data_dict[AdcptMFCoeffParticleKey.CHECK_BAND].append(
                    sensor_match.group(AdcptMFCoeffParticleKey.CHECK_BAND))

            else:
                # Generate a warning for unknown data
                self.recov_exception_callback('Unexpected data found in line %s' % line)

            # read the next line in the file
            line = self._stream_handle.readline()

        # Construct parsed data list to hand over to the Data Particle class for particle creation
        # Make all the collected data effectively into one long dictionary
        parsed_dict = dict(chain(file_time_dict.iteritems(),
                                 dir_freq_dict.iteritems(),
                                 freq_band_dict.iteritems(),
                                 sensor_data_dict.iteritems()))

        error_flag = False
        # Check if all parameter data is accounted for
        for name in FCOEFF_ENCODING_RULES:
            try:
                if parsed_dict[name[0]]:
                    log.trace("parsed_dict[%s]: %s", name[0], parsed_dict[name[0]])
            except KeyError:
                self.recov_exception_callback('Missing particle data: %s' % name[0])
                error_flag = True

        # Don't create a particle if data is missing
        if error_flag:
            return

        # Check if the specified number of frequencies were retrieved from the data
        fcoeff_data_length = len(sensor_data_dict[AdcptMFCoeffParticleKey.FREQ_BAND])
        if fcoeff_data_length != int(dir_freq_dict[AdcptMFCoeffParticleKey.NUM_FREQ]):
            self.recov_exception_callback(
                'Unexpected number of frequencies in FCoeff Matrix: expected %s, got %s'
                % (dir_freq_dict[AdcptMFCoeffParticleKey.NUM_FREQ], fcoeff_data_length))

            # Don't create a particle if data is missing
            return

        # Convert the filename timestamp into the particle timestamp
        time_stamp = ntplib.system_to_ntp_time(
            utilities.formatted_timestamp_utc_time(file_time_dict[AdcptMFCoeffParticleKey.FILE_TIME]
                                                   , TIMESTAMP_FORMAT))

        # Extract a particle and append it to the record buffer
        particle = self._extract_sample(AdcptMFCoeffInstrumentDataParticle,
                                        None, parsed_dict, time_stamp)
        log.trace('Parsed particle: %s' % particle.generate_dict())
        self._record_buffer.append(particle)