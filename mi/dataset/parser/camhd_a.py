#!/usr/bin/env python

"""
@package mi.dataset.parser.camhd_a
@file mi/dataset/parser/camhd_a.py
@author Ronald Ronquillo
@brief Parser for the camhd_a dataset driver

This file contains code for the camhd_a parser to produce data particles.


A data particle is produced from metadata contained fi,e.
The metadata and is extracted from the video file.


Release notes:

Initial Release
"""

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


import ntplib
import re

from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser import utilities
from mi.core.common import BaseEnum
from mi.core.exceptions import RecoverableSampleException
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.log import get_logging_metaclass
from mi.logging import log


class CamhdAParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    FILE_PATH = "filepath"


# CAMHD_A video filename timestamp format
TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

# Regex to extract the timestamp from the video filename (path/to/CAMHDA301-YYYYmmddTHHMMSSZ.mp4)
FILE_PATH_MATCHER = re.compile(
    r'.+/(?P<Path>.+?/\d{4}/\d{2}/\d{2}/.+-(?P<Date>\d{4}\d{2}\d{2})T(?P<Time>\d{2}\d{2}\d{2})Z\.mp4|mov)'
)

class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the camhd_a data
    """
    SAMPLE = 'camhd_metadata'  # instrument data particle


class CamhdAInstrumentDataParticle(DataParticle):
    """
    Class for generating the camhd_a data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        """
        return [self._encode_value(CamhdAParticleKey.FILE_PATH, self.raw_data, str)]


class CamhdAParser(SimpleParser):
    """
    Parser for camhd_a video files
    """

    __metaclass__ = get_logging_metaclass(log_level='debug')

    def __init__(self, config, stream_handle, exception_callback):
        """
        Initialize the camhd_a parser, which does not use state or the chunker
        and sieve functions.
        @param config: The parser configuration dictionary
        @param stream_handle: The stream handle of the file to parse
        @param exception_callback: The callback to use when an exception occurs
        """

        super(CamhdAParser, self).__init__(config, stream_handle, exception_callback)

    def recov_exception_callback(self, message):
        log.warn(message)
        self._exception_callback(RecoverableSampleException(message))

    def parse_file(self):
        """
        Parse the *.mp4 file.
        """
        match = FILE_PATH_MATCHER.match(self._stream_handle.name)
        if match:
            file_datetime = match.group('Date') + match.group('Time')
            time_stamp = ntplib.system_to_ntp_time(
                utilities.formatted_timestamp_utc_time(file_datetime, TIMESTAMP_FORMAT))

            # Extract a particle and append it to the record buffer
            particle = self._extract_sample(CamhdAInstrumentDataParticle, None, match.group('Path'),
                                            time_stamp)
            log.debug('Parsed particle: %s', particle.generate_dict())
            self._record_buffer.append(particle)

        else:
            # Files retrieved from the instrument should always match the timestamp naming convention
            self.recov_exception_callback("Unable to extract file time from input file name: %s."
                "Expected format REFDES-YYYYmmddTHHMMSSZ.mp4" % self._stream_handle.name)