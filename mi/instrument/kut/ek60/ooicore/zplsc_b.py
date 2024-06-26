#!/usr/bin/env python

"""
@package mi.instrument.kut.ek60.ooicore.zplsc_b
@file mi-instrument/mi/instrument/kut/ek60/ooicore/zplsc_b.py
@author Ronald Ronquillo & Richard Han & vipul lakhani
@brief Parser for the zplsc_b dataset driver

This file contains code for the zplsc_b parser to produce data particles and echogram plots.

The Simrad EK60 scientific echo sounder supports the *.raw file format.
The *.raw file may contain one or more of the following datagram types:
Configuration, NMEA, Annotation, Sample.

Every *.raw file begins with a configuration datagram. A second configuration datagram
within the file is illegal. The data content of the Configuration datagram of an already
existing file cannot be altered from the EK60. NMEA, Annotation and Sample datagrams
constitute the remaining file content. These datagrams are written to the *.raw file in the
order that they are generated by the EK60.

A data particle is produced from metadata contained in the first ping of the series.
The metadata and echogram plots are extracted from the Sample datagram portion of the *.raw file.


Release notes:

Initial Release
"""
from collections import defaultdict
from datetime import datetime
from struct import unpack_from

import numpy as np
import numpy
import os
import re

from mi.core.common import BaseEnum
from mi.core.exceptions import InstrumentDataException
from mi.core.instrument.data_particle import DataParticle
from mi.core.log import get_logger
from mi.instrument.kut.ek60.ooicore.zplsc_echogram import SAMPLE_MATCHER, \
    LENGTH_SIZE, \
    DATAGRAM_HEADER_SIZE, \
    CONFIG_HEADER_SIZE,\
    CONFIG_TRANSDUCER_SIZE, \
    read_config_header
from mi.common.zpls_plot import ZPLSPlot

log = get_logger()
__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


class InvalidTransducer(Exception):
    pass


class ZplscBParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    FILE_TIME = "zplsc_timestamp"               # raw file timestamp
    ECHOGRAM_PATH = "filepath"                  # output echogram plot .png/s path and filename
    CHANNEL = "zplsc_channel"
    TRANSDUCER_DEPTH = "zplsc_transducer_depth"  # five digit floating point number (%.5f, in meters)
    FREQUENCY = "zplsc_frequency"               # six digit fixed point integer (in Hz)
    TRANSMIT_POWER = "zplsc_transmit_power"     # three digit fixed point integer (in Watts)
    PULSE_LENGTH = "zplsc_pulse_length"         # six digit floating point number (%.6f, in seconds)
    BANDWIDTH = "zplsc_bandwidth"               # five digit floating point number (%.5f in Hz)
    SAMPLE_INTERVAL = "zplsc_sample_interval"   # six digit floating point number (%.6f, in seconds)
    SOUND_VELOCITY = "zplsc_sound_velocity"     # five digit floating point number (%.5f, in m/s)
    ABSORPTION_COEF = "zplsc_absorption_coeff"  # four digit floating point number (%.4f, dB/m)
    TEMPERATURE = "zplsc_temperature"           # three digit floating point number (%.3f, in degC)
    FREQ_CHAN_1 = "zplsc_frequency_channel_1"
    VALS_CHAN_1 = "zplsc_values_channel_1"
    FREQ_CHAN_2 = "zplsc_frequency_channel_2"
    VALS_CHAN_2 = "zplsc_values_channel_2"
    FREQ_CHAN_3 = "zplsc_frequency_channel_3"
    VALS_CHAN_3 = "zplsc_values_channel_3"


# The following is used for _build_parsed_values() and defined as below:
# (parameter name, encoding function)
METADATA_ENCODING_RULES = [
    (ZplscBParticleKey.FILE_TIME, str),
    (ZplscBParticleKey.ECHOGRAM_PATH, str),
    (ZplscBParticleKey.CHANNEL, lambda x: [int(y) for y in x]),
    (ZplscBParticleKey.TRANSDUCER_DEPTH, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.FREQUENCY, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.TRANSMIT_POWER, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.PULSE_LENGTH, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.BANDWIDTH, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.SAMPLE_INTERVAL, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.SOUND_VELOCITY, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.ABSORPTION_COEF, lambda x: [float(y) for y in x]),
    (ZplscBParticleKey.TEMPERATURE, lambda x: [float(y) for y in x])
]

RAWDATA_ENCODING_RULES = [
    (ZplscBParticleKey.FREQ_CHAN_1, float),
    (ZplscBParticleKey.VALS_CHAN_1, list),
    (ZplscBParticleKey.FREQ_CHAN_2, float),
    (ZplscBParticleKey.VALS_CHAN_2, list),
    (ZplscBParticleKey.FREQ_CHAN_3, float),
    (ZplscBParticleKey.VALS_CHAN_3, list)
]

# Numpy data type object for unpacking the Sample datagram including the header from binary *.raw
sample_dtype = numpy.dtype([('length1', 'i4'),  # 4 byte int (long)
                            # DatagramHeader
                            ('datagram_type', 'a4'),  # 4 byte string
                            ('low_date_time', 'u4'),  # 4 byte int (long)
                            ('high_date_time', 'u4'),  # 4 byte int (long)
                            # SampleDatagram
                            ('channel_number', 'i2'),  # 2 byte int (short)
                            ('mode', 'i2'),  # 2 byte int (short)
                            ('transducer_depth', 'f4'),  # 4 byte float
                            ('frequency', 'f4'),  # 4 byte float
                            ('transmit_power', 'f4'),  # 4 byte float
                            ('pulse_length', 'f4'),  # 4 byte float
                            ('bandwidth', 'f4'),  # 4 byte float
                            ('sample_interval', 'f4'),  # 4 byte float
                            ('sound_velocity', 'f4'),  # 4 byte float
                            ('absorption_coefficient', 'f4'),  # 4 byte float
                            ('heave', 'f4'),  # 4 byte float
                            ('roll', 'f4'),  # 4 byte float
                            ('pitch', 'f4'),  # 4 byte float
                            ('temperature', 'f4'),  # 4 byte float
                            ('trawl_upper_depth_valid', 'i2'),  # 2 byte int (short)
                            ('trawl_opening_valid', 'i2'),  # 2 byte int (short)
                            ('trawl_upper_depth', 'f4'),  # 4 byte float
                            ('trawl_opening', 'f4'),  # 4 byte float
                            ('offset', 'i4'),  # 4 byte int (long)
                            ('count', 'i4')])                     # 4 byte int (long)
sample_dtype = sample_dtype.newbyteorder('<')

power_dtype = numpy.dtype([('power_data', '<i2')])     # 2 byte int (short)

angle_dtype = numpy.dtype([('athwart', '<i1'), ('along', '<i1')])     # 1 byte ints

GET_CONFIG_TRANSDUCER = False   # Optional data flag: not currently used
BLOCK_SIZE = 1024*4             # Block size read in from binary file to search for token

# ZPLSC EK 60 *.raw filename timestamp format
# ei. OOI-D20141211-T214622.raw
TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

# Regex to extract the timestamp from the *.raw filename (path/to/OOI-DYYYYmmdd-THHMMSS.raw)
FILE_NAME_REGEX = r'(?P<Refdes>\S*)_*OOI-D(?P<Date>\d{8})-T(?P<Time>\d{6})\.raw'
FILE_NAME_MATCHER = re.compile(FILE_NAME_REGEX)

WINDOWS_EPOCH = datetime(1601, 1, 1)
NTP_EPOCH = datetime(1900, 1, 1)
NTP_WINDOWS_DELTA = (NTP_EPOCH - WINDOWS_EPOCH).total_seconds()


def windows_to_ntp(windows_time):
    """
    Convert a windows file timestamp into Network Time Protocol
    :param windows_time:  100ns since Windows time epoch
    :return:
    """
    return windows_time / 1e7 - NTP_WINDOWS_DELTA


def build_windows_time(high_word, low_word):
    """
    Generate Windows time value from high and low date times.

    :param high_word:  high word portion of the Windows datetime
    :param low_word:   low word portion of the Windows datetime
    :return:  time in 100ns since 1601/01/01 00:00:00 UTC
    """
    return (high_word << 32) + low_word


class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the zplsc_b data
    """
    METADATA = 'zplsc_metadata'  # instrument data particle
    RAWDATA = 'zplsc_echogram_data'   # sample data particle


class ZplscBSampleDataParticle (DataParticle):
    """
    Class for generating the zplsc_b_sample data particle.
    """

    def __init__(self, *args, **kwargs):
        super(ZplscBSampleDataParticle, self).__init__(*args, **kwargs)
        self._data_particle_type = DataParticleType.RAWDATA

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[name], _function)
                for name, _function in RAWDATA_ENCODING_RULES]


class ZplscBInstrumentDataParticle(DataParticle):
    """
    Class for generating the zplsc_b_instrument data particle.
    """

    _data_particle_type = DataParticleType.METADATA

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[name], _function)
                for name, _function in METADATA_ENCODING_RULES]


def append_metadata(metadata, file_time, file_path, channel, sample_data):
    metadata[ZplscBParticleKey.FILE_TIME] = file_time
    metadata[ZplscBParticleKey.ECHOGRAM_PATH] = file_path
    metadata[ZplscBParticleKey.CHANNEL].append(channel)
    metadata[ZplscBParticleKey.TRANSDUCER_DEPTH].append(sample_data['transducer_depth'][0])
    metadata[ZplscBParticleKey.FREQUENCY].append(sample_data['frequency'][0])
    metadata[ZplscBParticleKey.TRANSMIT_POWER].append(sample_data['transmit_power'][0])
    metadata[ZplscBParticleKey.PULSE_LENGTH].append(sample_data['pulse_length'][0])
    metadata[ZplscBParticleKey.BANDWIDTH].append(sample_data['bandwidth'][0])
    metadata[ZplscBParticleKey.SAMPLE_INTERVAL].append(sample_data['sample_interval'][0])
    metadata[ZplscBParticleKey.SOUND_VELOCITY].append(sample_data['sound_velocity'][0])
    metadata[ZplscBParticleKey.ABSORPTION_COEF].append(sample_data['absorption_coefficient'][0])
    metadata[ZplscBParticleKey.TEMPERATURE].append(sample_data['temperature'][0])
    return metadata


def process_sample(input_file, transducer_count):
    log.trace('Processing one sample from input_file: %r', input_file)
    # Read and unpack the Sample Datagram into numpy array
    sample_data = numpy.fromfile(input_file, dtype=sample_dtype, count=1)
    channel = sample_data['channel_number'][0]

    # Check for a valid channel number that is within the number of transducers config
    # to prevent incorrectly indexing into the dictionaries.
    # An out of bounds channel number can indicate invalid, corrupt,
    # or misaligned datagram or a reverse byte order binary data file.
    # Log warning and continue to try and process the rest of the file.
    if channel < 0 or channel > transducer_count:
        log.warn("Invalid channel: %s for transducer count: %s."
                 "Possible file corruption or format incompatibility.", channel, transducer_count)
        raise InvalidTransducer

    # Convert high and low bytes to internal time
    windows_time = build_windows_time(sample_data['high_date_time'][0], sample_data['low_date_time'][0])
    ntp_time = windows_to_ntp(windows_time)

    count = sample_data['count'][0]

    # Extract array of power data
    power_data = numpy.fromfile(input_file, dtype=power_dtype, count=count).astype('f8')

    # Read the athwartship and alongship angle measurements
    if sample_data['mode'][0] > 1:
        numpy.fromfile(input_file, dtype=angle_dtype, count=count)

    # Read and compare length1 (from beginning of datagram) to length2
    # (from the end of datagram). A mismatch can indicate an invalid, corrupt,
    # or misaligned datagram or a reverse byte order binary data file.
    # Log warning and continue to try and process the rest of the file.
    len_dtype = numpy.dtype([('length2', '<i4')])  # 4 byte int (long)
    length2_data = numpy.fromfile(input_file, dtype=len_dtype, count=1)
    if not (sample_data['length1'][0] == length2_data['length2'][0]):
        log.warn("Mismatching beginning and end length values in sample datagram: length1"
                 ": %s, length2: %s. Possible file corruption or format incompatibility.",
                 sample_data['length1'][0], length2_data['length2'][0])

    return channel, ntp_time, sample_data, power_data


def generate_relative_file_path(filepath):
    """
    If the reference designator exists in the filepath, return a new path
    relative to the reference designator directory.
    """
    filename = os.path.basename(filepath)
    try:
        reference_designator, _ = filename.split('_', 1)
        delimiter = reference_designator + os.sep

        if delimiter in filepath:
            return filepath[filepath.index(delimiter):].replace(delimiter, '')
    except ValueError:
        pass

    # unable to determine relative path, return just the filename
    return filename


def generate_image_file_path(filepath, output_path=None):
    # Extract the file time from the file name
    absolute_path = os.path.abspath(filepath)
    filename = os.path.basename(absolute_path)
    directory_name = os.path.dirname(absolute_path)

    output_path = directory_name if output_path is None else output_path
    image_file = filename.replace('.raw', '.png')
    return os.path.join(output_path, image_file)


def extract_file_time(filepath):
    match = FILE_NAME_MATCHER.match(filepath)
    if match:
        return match.group('Date') + match.group('Time')
    else:
        # Files retrieved from the instrument should always match the timestamp naming convention
        error_message = \
            "Unable to extract file time from input file name: %s. Expected format *-DYYYYmmdd-THHMMSS.raw" \
            % filepath
        log.error(error_message)
        raise InstrumentDataException(error_message)


def read_header(filehandle):
    # Read binary file a block at a time
    raw = filehandle.read(BLOCK_SIZE)

    # Read the configuration datagram, output at the beginning of the file
    length1, = unpack_from('<l', raw)
    byte_cnt = LENGTH_SIZE

    # Configuration datagram header
    byte_cnt += DATAGRAM_HEADER_SIZE

    # Configuration: header
    config_header = read_config_header(raw[byte_cnt:byte_cnt+CONFIG_HEADER_SIZE])
    byte_cnt += CONFIG_HEADER_SIZE
    byte_cnt += CONFIG_TRANSDUCER_SIZE * config_header['transducer_count']

    # Compare length1 (from beginning of datagram) to length2 (from the end of datagram) to
    # the actual number of bytes read. A mismatch can indicate an invalid, corrupt, misaligned,
    # or missing configuration datagram or a reverse byte order binary data file.
    # A bad/missing configuration datagram header is a significant error.
    length2, = unpack_from('<l', raw, byte_cnt)
    if not (length1 == length2 == byte_cnt-LENGTH_SIZE):
        raise InstrumentDataException(
            "Length of configuration datagram and number of bytes read do not match: length1: %s"
            ", length2: %s, byte_cnt: %s. Possible file corruption or format incompatibility." %
            (length1, length2, byte_cnt+LENGTH_SIZE))
    byte_cnt += LENGTH_SIZE
    filehandle.seek(byte_cnt)
    return config_header


def parse_echogram_file_wrapper(input_file_path, output_file_path=None):
    try:
        return parse_particles_file(input_file_path, output_file_path)
    except Exception as e:
        log.exception('Exception generating echogram')
        return e


def parse_particles_file(input_file_path, output_file_path=None):
    """
    Parse the *.raw file.
    @param input_file_path absolute path/name to file to be parsed
    @param output_file_path optional path to directory to write output
    If omitted outputs are written to path of input file
    """
    log.info('Begin processing data: %r', input_file_path)
    image_path = generate_image_file_path(input_file_path, output_file_path)
    file_time = extract_file_time(input_file_path)

    with open(input_file_path, 'rb') as input_file:

        config_header = read_header(input_file)
        transducer_count = config_header['transducer_count']

        trans_keys = range(1, transducer_count+1)
        frequencies = dict.fromkeys(trans_keys)       # transducer frequency
        bin_size = None                               # transducer depth measurement

        position = input_file.tell()
        meta_data = None
        timestamp = None

        last_time = None
        sample_data_temp_dict = {}
        power_data_temp_dict = {}

        power_data_dict = {}
        data_times = []

        # Read binary file a block at a time
        raw = input_file.read(BLOCK_SIZE)

        while len(raw) > 4:
            # We only care for the Sample datagrams, skip over all the other datagrams
            match = SAMPLE_MATCHER.search(raw)

            if match:
                # Offset by size of length value
                match_start = match.start() - LENGTH_SIZE

                # Seek to the position of the length data before the token to read into numpy array
                input_file.seek(position + match_start)

                try:
                    next_channel, next_time, next_sample, next_power = process_sample(input_file, transducer_count)

                    if next_time != last_time:
                        # Clear out our temporary dictionaries and set the last time to this time
                        sample_data_temp_dict = {}
                        power_data_temp_dict = {}
                        last_time = next_time

                    # Store this data
                    sample_data_temp_dict[next_channel] = next_sample
                    power_data_temp_dict[next_channel] = next_power

                    # Check if we have enough records to produce a new row of data
                    if len(sample_data_temp_dict) == len(power_data_temp_dict) == transducer_count:
                        # if this is our first set of data, create our metadata particle and store
                        # the frequency / bin_size data
                        if not power_data_dict:
                            relpath = generate_relative_file_path(image_path)
                            first_ping_metadata = defaultdict(list)
                            for channel, sample_data in sample_data_temp_dict.iteritems():
                                append_metadata(first_ping_metadata, file_time, relpath, channel, sample_data)

                                frequency = sample_data['frequency'][0]
                                frequencies[channel] = frequency

                                if bin_size is None:
                                    bin_size = sample_data['sound_velocity'] * sample_data['sample_interval'] / 2

                            meta_data = first_ping_metadata
                            timestamp = next_time
                            power_data_dict = {channel: [] for channel in power_data_temp_dict}

                        # Save the time and power data for plotting
                        data_times.append(next_time)
                        for channel in power_data_temp_dict:
                            power_data_dict[channel].append(power_data_temp_dict[channel])

                except InvalidTransducer:
                    pass

            else:
                input_file.seek(position + BLOCK_SIZE - 4)

            # Need current position in file to increment for next regex search offset
            position = input_file.tell()
            # Read the next block for regex search
            raw = input_file.read(BLOCK_SIZE)

        log.info('Completed processing data: %r', input_file_path)

        data_times = np.array(data_times)

        for channel in power_data_dict:
            # Convert to numpy array and decompress power data to dB
            power_data_dict[channel] = np.array(power_data_dict[channel]) * 10. * numpy.log10(2) / 256.

        for channel in frequencies:
            frequencies[channel] = frequencies[channel] / 1000.0

        _, max_depth = power_data_dict[1].shape

        log.info('Begin generating echogram: %r', image_path)

        plot = ZPLSPlot(data_times, power_data_dict, frequencies, 0, max_depth * bin_size)
        plot.generate_plots()
        plot.write_image(image_path)

        log.info('Completed generating echogram: %r', image_path)

        return meta_data, timestamp, data_times, power_data_dict, frequencies
