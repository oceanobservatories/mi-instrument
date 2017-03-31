#!/usr/bin/env python

"""
@package mi.dataset.parser.zplsc_c
@file /mi/dataset/parser/zplsc_c.py
@author Rene Gelinas
@brief Parser for the zplsc_c dataset driver

This file contains code for the zplsc_c parser and code to produce data particles.

The ZPLSC sensor, series C, provides acoustic return measurements from the water column.
The revcovered data files (*.01A) are binary recovered from the CF flash memory.
The file may contain record data for multiple phases and bursts of measurements.
Mal-formed sensor data records produce no particles.

All data are in unsigned integer format, with exception of the first 2 delimiter characters.
The sensor data record has a header followed by the scientific data.  The format of the header
is defined in the AzfpProfileHeader class below.

The format of the scientific data is as follows:
Bytes    Description
------   ---------------
# Bins   Data Channel 1
# Bins   Data Channel 2
# Bins   Data Channel 3
# Bins   Data Channel 4

Data that is stored as 16 bit digitized data is stored as consecutive 16 bit values.
The number is defined by the # of bins or NumBins.

Averaged data is summed up linear scale data that is stored in NumBins * 32 bit unsigned
integer sums, this is followed by NumBins * 8 bit Overflow counts.


Release notes:

Initial Release
"""

import struct
import exceptions
from ctypes import *
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
    AVERAGED_DATA = "zplsc_c_averaged_data"
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


class AzfpProfileHeader(BigEndianStructure):
    _fields_ = [
        ('delimiter', c_char*2),            # Profile Data Delimiter ('\xfd\x02')
        ('burst_num', c_ushort),            # Burst number
        ('serial_num', c_ushort),           # Instrument Serial number
        ('ping_status', c_ushort),          # Ping Status
        ('burst_interval', c_uint),         # Burst Interval (seconds)
        ('year', c_ushort),                 # Year
        ('month', c_ushort),                # Month
        ('day', c_ushort),                  # Day
        ('hour', c_ushort),                 # Hour
        ('minute', c_ushort),               # Minute
        ('second', c_ushort),               # Second
        ('hundredths', c_ushort),           # Hundreths of a second
        ('digitization_rate', c_ushort*4),  # Digitization Rate (channels 1-4) (64000, 40000 or 20000)
        ('num_skip_samples', c_ushort*4),   # Number of samples skipped at start of ping (channels 1-4)
        ('num_bins', c_ushort*4),           # Number of bins (channels 1-4)
        ('range_samples', c_ushort*4),      # Range samples per bin (channels 1-4)
        ('num_pings_profile', c_ushort),    # Number of pings per profile
        ('is_averaged_pings', c_ushort),    # Indicates if pings are averaged in time
        ('num_pings_burst', c_ushort),      # Number of pings that have been acquired in this burst
        ('ping_period', c_ushort),          # Ping period in seconds
        ('first_ping', c_ushort),           # First ping number (if averaged, first averaged ping number)
        ('second_ping', c_ushort),          # Last ping number (if averaged, last averaged ping number)
        ('is_averaged_data', c_ubyte*4),    # 1 = averaged data (5 bytes), 0 = not averaged (2 bytes)
        ('error_num', c_ushort),            # Error number if an error occurred
        ('phase', c_ubyte),                 # Phase used to acquire this profile
        ('is_overrun', c_ubyte),            # 1 if an over run occurred
        ('num_channels', c_ubyte),          # Number of channels (1, 2, 3 or 4)
        ('gain', c_ubyte*4),                # Gain (channels 1-4) 0, 1, 2, 3 (Obsolete)
        ('spare', c_ubyte),                 # Spare
        ('pulse_length', c_ushort*4),       # Pulse length (channels 1-4) (uS)
        ('board_num', c_ushort*4),          # Board number of the data (channels 1-4)
        ('frequency', c_ushort*4),          # Board frequency (channels 1-4)
        ('is_sensor_available', c_ushort),  # Indicate if pressure/temperature sensor is available
        ('tilt_x', c_ushort),               # Tilt X (counts)
        ('tilt_y', c_ushort),               # Tilt Y (counts)
        ('battery_voltage', c_ushort),      # Battery voltage (counts)
        ('pressure', c_ushort),             # Pressure (counts)
        ('temperature', c_ushort),          # Temperature (counts)
        ('ad_channel_6', c_ushort),         # AD channel 6
        ('ad_channel_7', c_ushort)          # AD channel 7
        ]


class ZplscCParser(SimpleParser):
    def __init__(self, config, stream_handle, exception_callback):
        super(ZplscCParser, self).__init__(config, stream_handle, exception_callback)
        self._particle_type = None
        self._gen = None

    def parse_record(self, ph):
        """
        :param ph: Profile Header for the current data record being parsed.
        """
        chan_values = [[], [], [], []]
        overflow_values = [[], [], [], []]

        if ph.delimiter == PROFILE_DATA_DELIMITER:
            # Parse the data values portion of the record.
            for chan in range(ph.num_channels):
                num_bins = ph.num_bins[chan]

                # Set the data structure format for the scientific data, based on whether
                # the data is averaged or not, then construct the data structure, then read
                # the data bytes for the current channel and unpack them based on the structure.
                if ph.is_averaged_data[chan] == 1:
                    data_struct_format = '>' + str(num_bins) + 'I'
                else:
                    data_struct_format = '>' + str(num_bins) + 'H'
                data_struct = struct.Struct(data_struct_format)
                data = self._stream_handle.read(data_struct.size)
                chan_values[chan] = data_struct.unpack(data)

                # If the data type is for averaged data, calculate the averaged data by multiplying
                # the overflow data by 0xFFFF and adding to the sum (the data read above).
                if ph.is_averaged_data[chan]:
                    overflow_struct_format = '>' + str(num_bins) + 'B'
                    overflow_struct = struct.Struct(overflow_struct_format)
                    overflow_data = self._stream_handle.read(num_bins)
                    overflow_values[chan] = overflow_struct.unpack(overflow_data)
                    overflow_values[chan] = [ovfl_data * 0xFFFF for ovfl_data in overflow_values[chan]]
                    chan_values[chan] = [sum_data + ovfl_data for sum_data, ovfl_data in
                                         zip(chan_values[chan], overflow_values[chan])]

            # Convert the date and time parameters to a epoch time from 01-01-1900.
            timestamp = (datetime(ph.year, ph.month, ph.day, ph.hour, ph.minute, ph.second,
                                  (ph.hundredths * 10000)) - datetime(1900, 1, 1)).total_seconds()

            # Format the data in a particle data dictionary
            zp_data = {
                ZplscCParticleKey.TRANS_TIMESTAMP: timestamp,
                ZplscCParticleKey.SERIAL_NUMBER: str(ph.serial_num),
                ZplscCParticleKey.PHASE: ph.phase,
                ZplscCParticleKey.BURST_NUMBER: ph.burst_num,
                ZplscCParticleKey.TILT_X: ph.tilt_x,
                ZplscCParticleKey.TILT_Y: ph.tilt_y,
                ZplscCParticleKey.BATTERY_VOLTAGE: ph.battery_voltage,
                ZplscCParticleKey.PRESSURE: ph.pressure,
                ZplscCParticleKey.TEMPERATURE: ph.temperature,
                ZplscCParticleKey.AVERAGED_DATA: list(ph.is_averaged_data),
                ZplscCParticleKey.FREQ_CHAN_1: ph.frequency[0],
                ZplscCParticleKey.VALS_CHAN_1: list(chan_values[0]),
                ZplscCParticleKey.FREQ_CHAN_2: ph.frequency[1],
                ZplscCParticleKey.VALS_CHAN_2: list(chan_values[1]),
                ZplscCParticleKey.FREQ_CHAN_3: ph.frequency[2],
                ZplscCParticleKey.VALS_CHAN_3: list(chan_values[2]),
                ZplscCParticleKey.FREQ_CHAN_4: ph.frequency[3],
                ZplscCParticleKey.VALS_CHAN_4: list(chan_values[3])
            }

            # Create the data particle
            particle = self._extract_sample(
                ZplscCRecoveredDataParticle, None, zp_data, timestamp, DataParticleKey.PORT_TIMESTAMP)
            if particle is not None:
                log.trace('Parsed particle: %s' % particle.generate_dict())
                self._record_buffer.append(particle)

        else:
            # The profile data delimiter was invalid.  Set the exception callback
            delimiter_received = ''
            for index in range(len(ph.delimiter)):
                delimiter_received += hex(ord(ph.delimiter[index]))

            delimiter_expected = ''
            for index in range(len(ph.delimiter)):
                delimiter_expected += hex(ord(PROFILE_DATA_DELIMITER[index]))

            raise SampleException('Profile delimiter invalid: received: %s ; expected %s' %
                                  (delimiter_received, delimiter_expected))

    def parse_file(self):
        ph = AzfpProfileHeader()
        while self._stream_handle.readinto(ph):
            try:
                # Pass in the profile header; it is needed to parse the data.
                self.parse_record(ph)

                # Clear the profile header data structure
                ph = AzfpProfileHeader()
            except (IOError, OSError) as ex:
                self._exception_callback('Reading stream handle: %s: %s\n' % (self._stream_handle.name, ex.message))
                return
            except struct.error as ex:
                self._exception_callback('Unpacking the data from the data structure: %s\n' % ex.message)
                return
            except exceptions.ValueError as ex:
                self._exception_callback('Transition timestamp has invalid format: %s' % ex.message)
                return
            except (SampleException, RecoverableSampleException) as ex:
                self._exception_callback('Creating data particle: %s' % ex.message)
                return
