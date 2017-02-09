#!/usr/bin/env python

"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/camds.py
@author Dan Mergens
@brief Parser for camds_abc html files.

This file contains code for parsing CAMDS HTML formatted files, extracting the metadata associated therein and
generating the corresponding metadata particles.

Input is an HTML formatted file. This file should contain a reference to a local PNG file whose absolute path must be
derived from the location of the HTML file to determine path information for the metadata.

Release Notes:

Initial release: 27 Jan 2017
"""

import re
from datetime import datetime

from bs4 import BeautifulSoup

from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException
from mi.core.instrument.dataset_data_particle import DataParticleKey, DataParticle, DataParticleValue
from mi.core.log import get_logger
from mi.dataset.dataset_parser import SimpleParser

__author__ = 'Dan Mergens'
__license__ = 'Apache 2.0'

log = get_logger()
INT_REGEX = re.compile(r'(\d+)')

# Example: <TD colspan="2"><a href="20100101T000104,301.png" ><img width="100%" src="20100101T000104,301.png"
#  alt="20100101T000104,301.png" ></a></TD><TD>
IMAGE_REGEX = re.compile(r'(\S+) \S+')


def read_filename(regex, string):
    match = re.match(regex, string)
    if not match:
        return ''
    return match.group(1)

# Example:
# <TD width="200px" >Time Taken:</TD><TD>2010-01-01 00:01:04:301</TD>
TIMESTAMP_REGEX = re.compile(r'''(
    (?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})  # date
    \W
    (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}):(?P<millisecond>\d{3})  # time
    )''', re.VERBOSE)


def read_timestamp(regex, string):
    """
    Read the timestamp from the html with NTP timestamp
    """
    match = re.match(regex, string)
    if not match:
        return None
    year = int(match.group('year'))
    month = int(match.group('month'))
    day = int(match.group('day'))
    hour = int(match.group('hour'))
    minute = int(match.group('minute'))
    second = int(match.group('second'))
    millisecond = int(match.group('millisecond'))
    ts = datetime(year, month, day, hour, minute, second, int(millisecond*1e3))
    return (ts - datetime(1900, 1, 1)).total_seconds()

# Example:
# <TD >Zoom Position:</TD><TD>115</TD>
POSITION_REGEX = INT_REGEX

# Example:
# <TD >Focus Position:</TD><TD>30</TD>
FOCUS_REGEX = INT_REGEX

# Example:
# <TD >Iris Position:</TD><TD>5</TD>
IRIS_REGEX = INT_REGEX

# Note: Shutter speed is not used by camds_image_metadata
# Example:
# <TD >Shutter Speed:</TD><TD>150 (Manual)</TD>
SHUTTER_REGEX = INT_REGEX

# Example:
# <TD >GAIN:</TD><TD>11 (Manual)</TD>
GAIN_REGEX = INT_REGEX

# Example:
# <TD >Pan and Tilt Position:</TD><TD>X=0 Y=0</TD>
PAN_TILT_REGEX = re.compile(r'X=(?P<X>\d+) Y=(?P<Y>\d+)')

# Example:
# <TD >Lamp1:</TD><TD>19</TD>
# <TD >Lamp2:</TD><TD>7</TD>
LAMP_REGEX = INT_REGEX

# Note: Laser is not used by camds_image_metadata
# Example:
# <TD >Laser:</TD><TD>On</TD>
LASER_REGEX = re.compile(r'(\w+)')


def read_int(regex, string):
    match = re.match(regex, string)
    if not match:
        return None
    return match.group(0)


def read_pan_tilt(regex, string):
    match = re.match(regex, string)
    x = 0
    y = 0
    if match:
        x = match.group('X')
        y = match.group('Y')
    return x, y


def read_laser(regex, string):
    match = re.match(regex, string)
    if not match:
        return False
    if match.group(0) == 'On':
        return True
    return False


class CamdsParserDataParticleType(BaseEnum):
    CAMDS_IMAGE_METADATA = 'camds_image_metadata'


class CamdsParserDataParticleKey(BaseEnum):
    """
    From 'camds_image_metadata' - DICT379
    """
    PAN_POSITION = 'camds_pan_position'  # PD2659
    TILT_POSITION = 'camds_tilt_position'  # PD2660
    FOCUS_POSITION = 'camds_focus_position'  # PD2661
    ZOOM_POSITION = 'camds_zoom_position'  # PD2662
    IRIS_POSITION = 'camds_iris_position'  # PD2663
    GAIN = 'camds_gain'  # PD2664
    RESOLUTION = 'camds_resolution'  # PD2665
    BRIGHTNESS = 'camds_brightness'  # PD2666
    IMAGE = 'filepath'  # PD3808 - relative filepath on raw data server
    BRIGHTNESS2 = 'camds_brightness2'  # PD8052


class CamdsHTMLDataKey(BaseEnum):
    IMAGE = 'Image:'
    TIMESTAMP = 'Time Taken:'
    ZOOM_POSITION = 'Zoom Position:'
    FOCUS_POSITION = 'Focus Position:'
    IRIS_POSITION = 'Iris Position:'
    SHUTTER_SPEED = 'Shutter Speed:'
    GAIN = 'GAIN:'
    PAN_TILT_POSITION = 'Pan and Tilt Position:'
    LAMP1 = 'Lamp1:'
    LAMP2 = 'Lamp2:'
    LASER = 'Laser:'


class CamdsMetadataParticle(DataParticle):
    """
    Abstract class for the camds_image_metadata data set.
    """
    _data_particle_type = CamdsParserDataParticleType.CAMDS_IMAGE_METADATA

    def __init__(self,
                 raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CamdsMetadataParticle, self).__init__(
            raw_data,
            port_timestamp,
            internal_timestamp,
            preferred_timestamp,
            quality_flag,
            new_sequence
        )

        self._data_dict = dict()

    def _build_parsed_values(self):
        """
        Build and return the parsed values for camds_image_metadata from self.raw_data.
        :return:
        """
        result = []

        if type(self.raw_data) is not dict:
            raise SampleException('Data provided to particle generator is not a valid dictionary: %r' % self.raw_data)

        data_dict = self.raw_data

        # check for required dictionary values
        required_keys = [
            CamdsParserDataParticleKey.IMAGE
        ]

        for key in required_keys:
            if key not in data_dict.keys():
                raise SampleException('Missing required key (%s)' % key)

        for key in data_dict:
            value, encoding = data_dict[key]
            result.append(self._encode_value(key, value, encoding))

        return result


class CamdsHtmlParser(SimpleParser):

    def __init__(self, stream_handle, exception_callback):

        # no sieve function since we are not using the chunker here
        super(CamdsHtmlParser, self).__init__({}, stream_handle, exception_callback)

        self._particle_class = CamdsMetadataParticle

        # metadata tuple of the form:
        #  html key: (value name, regex, encoder, value)
        self.metadata_encoding = {
            CamdsHTMLDataKey.IMAGE:
                (CamdsParserDataParticleKey.IMAGE, IMAGE_REGEX, read_filename, str),
            CamdsHTMLDataKey.TIMESTAMP:
                (CamdsHTMLDataKey.TIMESTAMP, TIMESTAMP_REGEX, read_timestamp, float),
            CamdsHTMLDataKey.ZOOM_POSITION:
                (CamdsParserDataParticleKey.ZOOM_POSITION, POSITION_REGEX, read_int, int),
            CamdsHTMLDataKey.FOCUS_POSITION:
                (CamdsParserDataParticleKey.FOCUS_POSITION, FOCUS_REGEX, read_int, int),
            CamdsHTMLDataKey.IRIS_POSITION:
                (CamdsParserDataParticleKey.IRIS_POSITION, IRIS_REGEX, read_int, int),
            # CamdsHTMLDataKey.SHUTTER_SPEED:
            #     (CamdsParserDataParticleKey., SHUTTER_REGEX, read_one, int),
            CamdsHTMLDataKey.GAIN:
                (CamdsParserDataParticleKey.GAIN, GAIN_REGEX, read_int, int),
            CamdsHTMLDataKey.PAN_TILT_POSITION:
                ((CamdsParserDataParticleKey.PAN_POSITION, CamdsParserDataParticleKey.TILT_POSITION),
                 PAN_TILT_REGEX, read_pan_tilt, int),
            # both Lamp1 and Lamp2 should always be the same for the uncabled CAMDS
            CamdsHTMLDataKey.LAMP1:
                (CamdsParserDataParticleKey.BRIGHTNESS, LAMP_REGEX, read_int, int),
            CamdsHTMLDataKey.LAMP2:
                (CamdsParserDataParticleKey.BRIGHTNESS2, LAMP_REGEX, read_int, int),
            # 'Laser:': ('LASER', LASER_REGEX, read_laser, int)
        }

    def parse_file(self):
        """
        Parse a CAMDS HTML file and collect metadata particles.
        :returns dictionary of metadata keys, parsed values and encoding method
        """

        data_dict = {}
        html_doc = self._stream_handle.read()
        soup = BeautifulSoup(html_doc, 'html.parser')
        tables = soup.find_all('table')

        if not tables:
            raise SampleException('no tables present')

        for table in tables:
            for row in table.find_all('tr'):
                columns = row.find_all('td')
                if not columns:
                    continue
                key = columns[0].get_text()
                if not key:
                    continue
                # use a dummy key for the image filename since it has no associated key in the html table
                if len(columns) == 2:
                    key = columns[0].get_text()
                    value = columns[1].get_text()
                else:
                    if '.png' in key:
                        value = key
                        key = CamdsHTMLDataKey.IMAGE
                    else:
                        continue
                # pan/tilt has two values - needs special handling
                if key == CamdsHTMLDataKey.PAN_TILT_POSITION:
                    names, regex, encoder, encoding_type = self.metadata_encoding[key]
                    encoded_values = encoder(regex, value)
                    for name, value in zip(names, encoded_values):
                        data_dict[name] = value, encoding_type

                elif key in self.metadata_encoding.keys():
                    name, regex, encoder, encoding_type = self.metadata_encoding[key]
                    encoded_value = encoder(regex, value)
                    data_dict[name] = encoded_value, encoding_type

            # extract timestamp and use for creation of the particle
            timestamp, _ = data_dict.pop(CamdsHTMLDataKey.TIMESTAMP)
            record = self._extract_sample(self._particle_class, None, data_dict, timestamp)
            self._record_buffer.append(record)
        self._file_parsed = True
