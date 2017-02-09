#!/usr/bin/env python

"""
@package mi.dataset.parser.dcl_file_common
@file marine-integrations/mi/dataset/parser/dcl_file_common.py
@author Ronald Ronquillo
@brief Parser for the file type for the dcl
Release notes:

initial release
"""

import re

from mi.core.log import get_logger

from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import UnexpectedDataException, InstrumentParameterException

from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, SPACE_REGEX, \
    ANY_CHARS_REGEX, DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX

from mi.dataset.parser.utilities import dcl_controller_timestamp_to_ntp_time

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'

log = get_logger()

# Basic patterns
SPACES = SPACE_REGEX + "+"
START_GROUP = '('
END_GROUP = ')'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has space-delimited fields (date, time, integers)
# All records end with newlines.

TIMESTAMP = START_GROUP + DATE_YYYY_MM_DD_REGEX + SPACE_REGEX + \
            TIME_HR_MIN_SEC_MSEC_REGEX + END_GROUP
START_METADATA = r'\['
END_METADATA = r'\]'

# All basic dcl records are ASCII characters separated by a newline.
RECORD_PATTERN = ANY_CHARS_REGEX            # Any number of ASCII characters
RECORD_PATTERN += END_OF_LINE_REGEX         # separated by a new line
RECORD_MATCHER = re.compile(RECORD_PATTERN)

SENSOR_GROUP_TIMESTAMP = 0
SENSOR_GROUP_YEAR = 1
SENSOR_GROUP_MONTH = 2
SENSOR_GROUP_DAY = 3
SENSOR_GROUP_HOUR = 4
SENSOR_GROUP_MINUTE = 5
SENSOR_GROUP_SECOND = 6
SENSOR_GROUP_MILLISECOND = 7


class DclInstrumentDataParticle(DataParticle):
    """
    Class for generating the dcl instrument particle.
    """

    def __init__(self, raw_data, instrument_particle_map, *args, **kwargs):

        super(DclInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # The particle timestamp is the DCL Controller timestamp.
        # Convert the DCL controller timestamp string to NTP time (in seconds and microseconds).
        dcl_controller_timestamp = self.raw_data[SENSOR_GROUP_TIMESTAMP]
        elapsed_seconds_useconds = dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp)
        self.set_internal_timestamp(elapsed_seconds_useconds)

        self.instrument_particle_map = instrument_particle_map

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[group], function)
                for name, group, function in self.instrument_particle_map]


class DclFileCommonParser(SimpleParser):
    """
    Parser for dcl data.
    In addition to the standard constructor parameters,
    this constructor takes additional parameters sensor_data_matcher
    and metadata_matcher
    """

    def __init__(self, config,
                 stream_handle,
                 exception_callback,
                 sensor_data_matcher,
                 metadata_matcher):

        # Accommodate for any parser not using the PARTICLE_CLASSES_DICT in config
        # Ensure a data matcher is passed as a parameter or defined in the particle class
        if sensor_data_matcher is not None:
            self.sensor_data_matcher = sensor_data_matcher
            self.particle_classes = None    # will be set from self._particle_class once instantiated
        elif DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config and \
                all(hasattr(particle_class, "data_matcher")
                    for particle_class in config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT].values()):
            self.particle_classes = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT].values()
        else:
            raise InstrumentParameterException("data matcher required")
        self.metadata_matcher = metadata_matcher

        super(DclFileCommonParser, self).__init__(
            config,
            stream_handle,
            exception_callback)

    def parse_file(self):
        """
        This method reads the file and parses the data within, and at
        the end of this method self._record_buffer will be filled with all the particles in the file.
        """

        # If not set from config & no InstrumentParameterException error from constructor
        if self.particle_classes is None:
            self.particle_classes = (self._particle_class,)

        for line in self._stream_handle:

            for particle_class in self.particle_classes:
                if hasattr(particle_class, "data_matcher"):
                    self.sensor_data_matcher = particle_class.data_matcher

                # If this is a valid sensor data record,
                # use the extracted fields to generate a particle.
                sensor_match = self.sensor_data_matcher.match(line)
                if sensor_match is not None:
                    break

            if sensor_match is not None:
                particle = self._extract_sample(particle_class,
                                                None,
                                                sensor_match.groups(),
                                                None)
                self._record_buffer.append(particle)

            # It's not a sensor data record, see if it's a metadata record.
            else:

                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.
                meta_match = self.metadata_matcher.match(line)
                if meta_match is None:
                    error_message = 'Unknown data found in chunk %s' % line
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))
