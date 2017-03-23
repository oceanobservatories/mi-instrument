#!/usr/bin/env python

"""
@package mi.dataset.parser.zplsc_c
@file /mi/dataset/parser/zplsc_c.py
@author Rene Gelinas
@brief Parser for the zplsc_c dataset driver

This file contains code for the zplsc_c parser and code to produce data particles.

The ZPLSC sensor, series C, provides acoustic return measurements from the water column.
The revcovered data files (*.01A) are binary recovered from the CF flash memory..
The file may contain record data for multiple phases and bursts of measurements.
Mal-formed sensor data records produce no particles.

All data are in unsigned integer format, with exception of the first 2 delimiter characters.
The sensor data record has the following format:

Field   Bytes    Description
-----   ----     -------   -----------
 0       2       Profile Data Delimiter ('\xfd\x02')
 1       2       Burst #
 2       2       Instrument Serial #
 3       2       Ping Status
 4       4       Burst Interval (seconds)
 5       2       Year
 6       2       Month
 7       2       Day
 8       2       Hour
 9       2       Minutes
10       2       Second
11       2       Hundredths of a second
12       8(2x4)  Digitization Rate (channels 1-4) (64000, 40000 or 20000)
13       8(2x4)  # of samples skipped at start of ping (channels 1-4)
14       8(2x4)  # of bins (channels 1-4)
15       8(2x4)  Range samples per bin (channels 1-4)
16       2       # of pings per profile
17       2       Flag that indicates if pings are averaged in time
18       2       # of pings that have been aquired in this burst
19       2       Ping period in seconds
20       2       First ping - # of the first averaged ping or ping # if not averaged
21       2       Last ping - # of the last averaged ping or ping # if not averaged
22       4(1x4)  1 = averaged data (5 bytes), 0 = not averaged (2 bytes)
23       2       Error number if an error occurred
24       1       Phase used to acquire this profile
25       1       1 if an over run occurred
26       1       Number of channels (1, 2, 3 or 4)
27       1       Gain channel 1 - gain 0, 1, 2, 3 (Obsolete)
28       1       Gain channel 2 - gain 0, 1, 2, 3 (Obsolete)
29       1       Gain channel 3 - gain 0, 1, 2, 3 (Obsolete)
30       1       Gain channel 4 - gain 0, 1, 2, 3 (Obsolete)
31       1       Spare
32       2       Pulse length channel 1 (uS)
33       2       Pulse length channel 2 (uS)
34       2       Pulse length channel 3 (uS)
35       2       Pulse length channel 4 (uS)
36       2       Board # the data came from for channel 1
37       2       Board # the data came from for channel 2
38       2       Board # the data came from for channel 3
39       2       Board # the data came from for channel 4
40       2       Board frequency for channel 1 (Hz)
41       2       Board frequency for channel 2 (Hz)
42       2       Board frequency for channel 3 (Hz)
43       2       Board frequency for channel 4 (Hz)
44       2       Sensor Flag to indicate if pressure/temperature sensor is available
45       2       Tilt X (counts)
46       2       Tilt Y (counts)
47       2       Battery (counts)
48       2       Pressure (counts)
49       2       Temperature (counts)
50       2       AD channel 6
51       2       AD channel 7
52       # Bins  Data Channel 1
53       # Bins  Data Channel 2
54       # Bins  Data Channel 3
55       # Bins  Data Channel 4

Data that is stored as 16 bit digitized data is stored as consecutive 16 bit values.
The number is defined by the # of bins or NumBins.

Averaged data is summed up linear scale data that is stored in NumBins * 32 bit unsigned
integer sums, this is followed by NumBins * 8 bit Overflow counts.


Release notes:

Initial Release
"""

import struct
import exceptions
from mi.core.exceptions import SampleException, RecoverableSampleException
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logger, get_logging_metaclass
from mi.dataset.dataset_parser import SimpleParser
from mi.core.common import BaseEnum
from datetime import datetime

log = get_logger()
METACLASS = get_logging_metaclass('trace')

__author__ = 'Rene Gelinas'
__license__ = 'Apache 2.0'


PROFILE_DATA_DELIMITER = '\xfd\x02'
ZPLSC_C_METADATA_STRUCT = struct.Struct('>2s3HI7H4H4H4H4H6H4BH8B4H4H4H8H')
ZPLSC_C_DELIMITER_STRUCT = struct.Struct('>2s')


class DataParticleType(BaseEnum):
    ZPLSC_C_PARTICLE_TYPE = 'zplsc_c_recovered'


class ZplscCParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted for the data particle.
    """
    TRANS_TIMESTAMP = "zplsc_c_transmission_timestamp"
    SERIAL_NUMBER = "serial_number"
    PHASE = "zplsc_c_phase"
    BURST_NUMBER = "burst_number"
    TILT_X = "zplsc_c_tilt_x_counts"
    TILT_Y = "zplsc_c_tilt_y_counts"
    BATTERY_VOLTAGE = "zplsc_c_battery_voltage_counts"
    TEMPERATURE = "zplsc_c_temperature_counts"
    PRESSURE = "zplsc_c_pressure_counts"
    FREQ_CHAN_1 = "zplsc_c_frequency_channel_1"
    VALS_CHAN_1 = "zplsc_c_values_channel_1"
    FREQ_CHAN_2 = "zplsc_c_frequency_channel_2"
    VALS_CHAN_2 = "zplsc_c_values_channel_2"
    FREQ_CHAN_3 = "zplsc_c_frequency_channel_3"
    VALS_CHAN_3 = "zplsc_c_values_channel_3"
    FREQ_CHAN_4 = "zplsc_c_frequency_channel_4"
    VALS_CHAN_4 = "zplsc_c_values_channel_4"


class ZplscCRecoveredDataParticle(DataParticle):
    __metaclass__ = METACLASS

    def __init__(self, *args, **kwargs):
        super(ZplscCRecoveredDataParticle, self).__init__(*args, **kwargs)
        self._data_particle_type = DataParticleType.ZPLSC_C_PARTICLE_TYPE

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """
        # Particle Mapping table, where each entry is a tuple containing the particle
        # field name, count(or count reference) and a function to use for data conversion.

        port_timestamp = self.raw_data[ZplscCParticleKey.TRANS_TIMESTAMP]
        self.contents[DataParticleKey.PORT_TIMESTAMP] = port_timestamp

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]


def find_next_record(index, data):
    """
    :param index: Current index
    :param data: Data read in from the file
    :return: Index of the next record found
    """
    while ZPLSC_C_DELIMITER_STRUCT.unpack(data[index:index+2])[0] != PROFILE_DATA_DELIMITER:
        index += 1
    return index


class ZplscCParser(SimpleParser):
    def __init__(self, config, stream_handle, exception_callback):
        super(ZplscCParser, self).__init__(config, stream_handle, exception_callback)
        self._particle_type = None
        self._gen = None

    def parse_file(self):
        # Read the entire contents of the data file
        data = self._stream_handle.read()

        index = 0
        while index < len(data):
            # Get the metadata protion of the next record.
            record = ZPLSC_C_METADATA_STRUCT.unpack(data[index:index+ZPLSC_C_METADATA_STRUCT.size])
            index += ZPLSC_C_METADATA_STRUCT.size

            bins_chan = [0, 0, 0, 0]
            data_type_chan = [0, 0, 0, 0]
            freq_chan = [0, 0, 0, 0]
            chan_values = [[], [], [], []]

            # Parse the metadata portion of the record.
            delimiter, burst_num, serial_num, _, _, year, month, day, hour, minute, second, hundredths,\
                _, _, _, _, \
                _, _, _, _, \
                bins_chan[0], bins_chan[1], bins_chan[2], bins_chan[3], \
                _, _, _, _, \
                _, _, _, _, _, _, \
                data_type_chan[0], data_type_chan[1], data_type_chan[2], data_type_chan[3], \
                _, phase, _, num_channels, \
                _, _, _, _, \
                _, \
                _, _, _, _, \
                _, _, _, _, \
                freq_chan[0], freq_chan[1], freq_chan[2], freq_chan[3], \
                _, \
                tilt_x, tilt_y, battery_voltage, temperature, pressure, \
                _, _ = record

            # Parse the data values portion of the record.
            if delimiter == PROFILE_DATA_DELIMITER:
                for chan in range(num_channels):
                    struct_format = '>' + str(bins_chan[chan]) + 'I'
                    data_struct = struct.Struct(struct_format)
                    chan_values[chan] = data_struct.unpack(data[index:index+data_struct.size])
                    index += data_struct.size

                    # If the data type is for averaged data, read away the overflow bytes.
                    if data_type_chan[chan] == 1:
                        struct_format = '>' + str(bins_chan[chan]) + 'B'
                        data_struct = struct.Struct(struct_format)
                        index += data_struct.size

                # Convert the date and time parameters to a epoch time from 01-01-1900.
                try:
                    timestamp = (datetime(year, month, day, hour, minute, second, (hundredths * 10000)) -
                                 datetime(1900, 1, 1)).total_seconds()
                except exceptions.ValueError as ex:
                    self._exception_callback('Transition timestamp has invalid format: %s' % ex.message)
                    index = find_next_record(index, data)
                    continue

                # Format the data in a particle data dictionary
                zp_data = {
                    ZplscCParticleKey.TRANS_TIMESTAMP: timestamp,
                    ZplscCParticleKey.SERIAL_NUMBER: str(serial_num),
                    ZplscCParticleKey.PHASE: phase,
                    ZplscCParticleKey.BURST_NUMBER: burst_num,
                    ZplscCParticleKey.TILT_X: tilt_x,
                    ZplscCParticleKey.TILT_Y: tilt_y,
                    ZplscCParticleKey.BATTERY_VOLTAGE: battery_voltage,
                    ZplscCParticleKey.TEMPERATURE: temperature,
                    ZplscCParticleKey.PRESSURE: pressure,
                    ZplscCParticleKey.FREQ_CHAN_1: freq_chan[0],
                    ZplscCParticleKey.VALS_CHAN_1: list(chan_values[0]),
                    ZplscCParticleKey.FREQ_CHAN_2: freq_chan[1],
                    ZplscCParticleKey.VALS_CHAN_2: list(chan_values[1]),
                    ZplscCParticleKey.FREQ_CHAN_3: freq_chan[2],
                    ZplscCParticleKey.VALS_CHAN_3: list(chan_values[2]),
                    ZplscCParticleKey.FREQ_CHAN_4: freq_chan[3],
                    ZplscCParticleKey.VALS_CHAN_4: list(chan_values[3])
                }

                try:
                    # Create the data particle
                    particle = self._extract_sample(
                        ZplscCRecoveredDataParticle, None, zp_data, timestamp, DataParticleKey.PORT_TIMESTAMP)
                    if particle is not None:
                        log.trace('Parsed particle: %s' % particle.generate_dict())
                        self._record_buffer.append(particle)

                except (SampleException, RecoverableSampleException) as ex:
                    self._exception_callback(ex)

            else:
                # The profile data delimiter was invalid.  Set the exception callback and
                # find the delimiter for the next profile data.
                self._exception_callback('Profile delimiter invalid: received: %s%s ; expected %s%s' %
                                         (hex(ord(delimiter[0])), hex(ord(delimiter[1])),
                                          hex(ord(PROFILE_DATA_DELIMITER[0])), hex(ord(PROFILE_DATA_DELIMITER[1]))))
                index = find_next_record(index, data)
