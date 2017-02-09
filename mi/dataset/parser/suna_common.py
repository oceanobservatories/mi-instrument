"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/suna_common.py
@author Emily Hahn
@brief Contains code common to parsing SUNA instruments
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import binascii
import datetime
import calendar
import ntplib
import struct

from mi.core.log import get_logger
log = get_logger()
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.core.exceptions import SampleException, NotImplementedException, RecoverableSampleException
from mi.dataset.dataset_parser import SimpleParser

# frame header is always 10 characters
FRAME_HEADER_SIZE = 10

# the parameter maps for suna instruments start the same, but vary in the parameters following these
PARAMETER_MAP_START = [
    ('frame_type',                    1,   str),
    ('serial_number',                 2,   str),
    ('time_of_sample',                4,   float),
    ('nitrate_concentration',         5,   float),
    ('nutnr_nitrogen_in_nitrate',     6,   float),
    ('nutnr_absorbance_at_254_nm',    7,   float),
    ('nutnr_absorbance_at_350_nm',    8,   float),
    ('nutnr_bromide_trace',           9,   float),
    ('nutnr_spectrum_average',        10,  int),
    ('nutnr_dark_value_used_for_fit', 11,  int),
    ('nutnr_integration_time_factor', 12,  int),
]


def get_year_and_day_of_year(year_and_day_of_year):
    """
    Extract the year and day of year as separate values from a single integer containing both
    :param year_and_day_of_year: An integer with format 'YYYYDDD' where YYYY is the year and DDD is the day
    :return: A list of the year and day of year
    """
    year = None
    day_of_year = None

    year_and_day_of_year_str = str(year_and_day_of_year)

    if len(year_and_day_of_year_str) >= 5:
        # must have at least 5 digits to get the year and day of year
        year = int(year_and_day_of_year_str[:4])
        day_of_year = int(year_and_day_of_year_str[4:])

    return year, day_of_year


class SunaDataParticle(DataParticle):

    _param_map = None  # must be set in derived class constructor

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        if self._param_map is None:
            raise NotImplementedException("_param_map not provided")

        super(SunaDataParticle, self).__init__(raw_data,
                                               port_timestamp,
                                               internal_timestamp,
                                               preferred_timestamp,
                                               quality_flag,
                                               new_sequence)

    def _build_parsed_values(self):
        """
        Fill in the data particle parameter dictionary and return it
        :return: An array of dictionaries, with each dictionary containing the parameter name and value
        """

        # turn year and day of year integer into a string to pull out specific digits
        year, day_of_year = get_year_and_day_of_year(self.raw_data[3])

        # initialize with the parameters needing extra handling that are not in the simple map
        parameters = [self._encode_value('year', year, int),
                      self._encode_value('day_of_year', day_of_year, int)]

        # the rest of the parameters are covered by the parameter map
        for name, index, encode in self._param_map:
            parameters.append(self._encode_value(name, self.raw_data[index], encode))

        return parameters


class SunaParser(SimpleParser):
    def __init__(self,
                 stream_handle,
                 exception_callback,
                 start_frame_matcher,
                 frame_size,
                 unpack_string,
                 light_particle_class,
                 dark_particle_class):

        # the regex to use to match the start of a frame
        self.start_frame_matcher = start_frame_matcher
        # the frame size in bytes
        self.frame_size = frame_size
        # the string to unpack the frame using struct
        self.unpack_string = unpack_string
        # the data particle class to extract
        self.light_particle_class = light_particle_class
        self.dark_particle_class = dark_particle_class

        # no config for this parser, pass in empty dict
        super(SunaParser, self).__init__({},
                                         stream_handle,
                                         exception_callback)

    def parse_file(self):
        """
        The main parsing function which reads blocks of data from the file and extracts particles if the correct
        format is found.
        """

        # read the whole file so start and end of frames can be found
        data = self._stream_handle.read()
        end_idx = 0

        # loop over all found frame headers
        for match in self.start_frame_matcher.finditer(data):

            start_idx = match.start()
            if start_idx > end_idx:
                # found unexpected data between frames
                log.warn('non matching start %d and end %d', start_idx, end_idx)
                self.unknown_data_exception(data[end_idx:start_idx])

            frame = data[start_idx:start_idx + self.frame_size]

            # get the end index of this frame for comparison with the start of the following frame
            end_idx = start_idx + len(frame)

            # unpack binary fields so the timestamp can be calculated and get the checksum
            fields = struct.unpack(self.unpack_string, frame)

            # compare checksums, error is reported in compare_checksums method if they don't
            if self.compare_checksums(frame[:-1], fields[-1]):

                # calculate the timestamp, error is reported in calculate timestamp if it cannot be calculated
                timestamp = self.calculate_timestamp(fields[3], fields[4])

                # check for a valid timestamp, can't have a particle without a timestamp
                if timestamp:
                    # got a valid timestamp

                    frame_type = fields[1]

                    if frame_type.startswith('SL'):  # light frame

                        particle = self._extract_sample(self.light_particle_class, None, fields, timestamp)
                        self._record_buffer.append(particle)
                    elif frame_type.startswith('SD'):   # dark frame
                        particle = self._extract_sample(self.dark_particle_class, None, fields, timestamp)
                        self._record_buffer.append(particle)
                    else:  # unexpected frame type
                        msg = 'got invalid frame type %sd' % frame_type
                        log.warning(msg)
                        self._exception_callback(RecoverableSampleException(msg))

        if end_idx != len(data):
            # there is unknown data at the end of the file
            self.unknown_data_exception(data[end_idx:])

    def unknown_data_exception(self, unknown_data):
        """
        Raise an exception for data with an unknown format
        :param unknown_data: The unknown data
        """
        msg = 'Found %d bytes unknown format: 0x%s' % (len(unknown_data), binascii.hexlify(unknown_data))
        log.warning(msg)
        self._exception_callback(SampleException(msg))

    def calculate_timestamp(self, year_and_day_of_year, sample_time):
        """
        Calculate the timestamp
        :param year_and_day_of_year: Integer year and day of year value
        :param sample_time: Sample time in floating point hours
        :return: The timestamp in ntp64
        """
        # turn year and day of year integer into a string to pull out specific digits
        [year, day_of_year] = get_year_and_day_of_year(year_and_day_of_year)

        if year is None or day_of_year is None:
            # need at least 5 digits to get year and day of year
            msg = 'Not enough digits for year and day of year: %s, unable to calculate timestamp' % \
                  str(year_and_day_of_year)
            log.warning(msg)
            self._exception_callback(SampleException(msg))
            # return no timestamp so the particle is not calculated
            return None

        # convert sample time in floating point hours to hours, minutes, seconds, and microseconds
        hours = int(sample_time)
        minutes = int(60.0 * (sample_time - float(hours)))
        seconds = 3600.0 * (sample_time - float(hours)) - float(minutes) * 60.0
        microseconds = seconds - int(seconds)

        # convert to a datetime (doesn't handle microseconds, they are included in final utc timestamp)
        date = datetime.datetime(year, 1, 1) + datetime.timedelta(days=day_of_year - 1,
                                                                  hours=hours,
                                                                  minutes=minutes,
                                                                  seconds=int(seconds))

        # convert from datetime to utc seconds, including microseconds since Jan 1 1970
        utc_timestamp = calendar.timegm(date.timetuple()) + microseconds
        # convert to seconds since Jan 1 1900 for ntp
        return ntplib.system_to_ntp_time(utc_timestamp)

    def compare_checksums(self, data, received_checksum):
        """
        Calculate the checksum for the input data and compare it to the received checksum
        :returns: True if the calculated checksum matched the received checksum
        """
        # subtract all bytes
        calculated_checksum = -sum(bytearray(data)) & 0xff

        if calculated_checksum == received_checksum:
            return True
        else:
            # checksums do not match
            msg = 'Checksum %d does not match received checksum %d' % (calculated_checksum, received_checksum)
            log.warning(msg)
            self._exception_callback(SampleException(msg))
            return False
