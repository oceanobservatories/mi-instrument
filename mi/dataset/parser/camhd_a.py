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


import os
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

# Regex to extract the timestamp from the video filename (path/to/CAMHDA301-YYYYmmddTHHMMSSZ.log)
# FILE_PATH_MATCHER = re.compile(
#     r'.+/(?P<Path>.+?/\d{4}/\d{2}/\d{2}/.+-(?P<Date>\d{4}\d{2}\d{2})T(?P<Time>\d{2}\d{2}\d{2})Z\.log)'
# )
FILE_PATH_MATCHER = re.compile(
    r'/.*/(?P<Sensor>.+)-(?P<Date>\d{4}\d{2}\d{2})T(?P<Time>\d{2}\d{2}\d{2})Z\.log'
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

    @staticmethod
    def find_matching_mp4(log_file_path, sensor, date):
        """
        Find associated MP4 file for the given camera log file.
        :param log_file_path: fully qualified file path of log file in rsn archive
        :param sensor: sensor name (does not include port number prefix - e.g. 'CAMHDA301')
        :param date: gregorian date in 'YYYYMMDD' format
        :param time: time of day in 'HHMMSS' format
        :return: relative path in the raw data server for the associated MP4 file
        TODO - have to determine reference designator if we add more HD cameras
        """
        # TODO - deterministically fill reference designator from ingest parameters
        subsite = 'RS03ASHS'
        node = 'PN03B'
        sensor = '06-' + sensor

        year = date[0:4]
        month = date[4:6]
        day = date[6:8]
        filename = os.path.basename(log_file_path)
        fileroot = os.path.splitext(filename)[0]
        mp4_filename = '.'.join((fileroot, 'mp4'))
        mp4_file_path = os.path.join(subsite, node, sensor, year, month, day, mp4_filename)
        return mp4_file_path

    def parse_file(self):
        """
        Parse the *.log file.
        """
        match = FILE_PATH_MATCHER.match(self._stream_handle.name)
        if match:
            sensor = match.group('Sensor')
            date = match.group('Date')
            time = match.group('Time')
            file_datetime = date + time
            time_stamp = ntplib.system_to_ntp_time(
                utilities.formatted_timestamp_utc_time(file_datetime, TIMESTAMP_FORMAT))

            # Extract a particle and append it to the record buffer
            mp4_file_path = self.find_matching_mp4(self._stream_handle.name, sensor, date)
            particle = self._extract_sample(CamhdAInstrumentDataParticle, None, mp4_file_path,
                                            internal_timestamp=time_stamp)
            log.debug('Parsed particle: %s', particle.generate_dict())
            self._record_buffer.append(particle)

        else:
            # Files retrieved from the instrument should always match the timestamp naming convention
            self.recov_exception_callback("Unable to extract file time from input file name: %s."
                "Expected format REFDES-YYYYmmddTHHMMSSZ.log" % self._stream_handle.name)
