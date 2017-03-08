"""
@package mi.dataset.parser
@file mi/dataset/parser/fdchp_a.py
@author Emily Hahn
@brief A parser for the fdchp series a instrument directly recovered
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import io
import struct
import calendar

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import SampleException
from mi.core.instrument.dataset_data_particle import DataParticle

from mi.dataset.dataset_parser import SimpleParser

# records are 55 bytes long
RECORD_SIZE = 55


class FdchpADataParticle(DataParticle):
    _data_particle_type = 'fdchp_a_instrument_recovered'

    YEAR_IDX = 0
    SEC_IDX = 5
    MILLI_IDX = 6

    UNPACK_MAP = [
        ('year', YEAR_IDX, int),
        ('month', 1, int),
        ('day', 2, int),
        ('hour', 3, int),
        ('minute', 4, int),
        ('second', SEC_IDX, int),
        ('millisecond', MILLI_IDX, int),
        ('fdchp_wind_x', 7, int),
        ('fdchp_wind_y', 8, int),
        ('fdchp_wind_z', 9, int),
        ('fdchp_speed_of_sound_sonic', 10, int),
        ('fdchp_x_ang_rate', 11, float),
        ('fdchp_y_ang_rate', 12, float),
        ('fdchp_z_ang_rate', 13, float),
        ('fdchp_x_accel_g', 14, float),
        ('fdchp_y_accel_g', 15, float),
        ('fdchp_z_accel_g', 16, float),
        ('fdchp_roll', 17, float),
        ('fdchp_pitch', 18, float),
        ('fdchp_heading', 19, float),
        ('fdchp_status_1', 20, int),
        ('fdchp_status_2', 21, int)
    ]

    def _build_parsed_values(self):
        """
        Build the particle from the input data in self.raw_data
        :returns: an array of dictionaries containing the particle data
        """

        # unpack the binary data into fields
        fields = struct.unpack('>H5BH3hH9fBB', self.raw_data)

        # turn year, month, day, hour, minute, second array into a tuple
        time_tuple = tuple(fields[self.YEAR_IDX:self.MILLI_IDX])
        milliseconds = float(fields[self.MILLI_IDX]/1000.0)
        # add milliseconds to unix time output from timegm
        unix_time = float(calendar.timegm(time_tuple)) + milliseconds
        # set the internal timestamp
        self.set_internal_timestamp(unix_time=unix_time)

        # encode the parameters using the unpack map and return them
        return [self._encode_value(name, fields[idx], function)
                for name, idx, function in self.UNPACK_MAP]


class FdchpAParser(SimpleParser):

    def __init__(self,
                 stream_handle,
                 exception_callback):

        # no config for this parser, pass in empty dict
        super(FdchpAParser, self).__init__({},
                                           stream_handle,
                                           exception_callback)

    def parse_file(self):
        """
        Entry point into parsing the file, loop over each line and interpret it until the entire file is parsed
        """

        # find out the file size by getting the last offset from the stream handle
        self._stream_handle.seek(0, io.SEEK_END)
        end_offset = self._stream_handle.tell()

        # the file must be a multiple of 55 bytes since this is how long a record it, if it is not there is no way to
        # parse this file
        if end_offset % RECORD_SIZE != 0:
            msg = "Binary file is not an even multiple of record size, records cannot be identified."
            log.error(msg)
            raise SampleException(msg)

        # seek back to the beginning of the file
        self._stream_handle.seek(0, io.SEEK_SET)

        record = self._stream_handle.read(RECORD_SIZE)

        while record:
            particle = self._extract_sample(FdchpADataParticle, None, record, None)
            self._record_buffer.append(particle)

            record = self._stream_handle.read(RECORD_SIZE)
