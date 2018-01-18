"""
@package mi.instrument.kut.ek60.ooicore.driver
@file marine-integrations/mi/instrument/kut/ek60/ooicore/driver.py
@author Craig Risien
@brief ZPLSC Echogram generation for the ooicore

Release notes:

This class supports the generation of ZPLSC echograms.
"""

import matplotlib

matplotlib.use("Agg")

from matplotlib.dates import date2num

from datetime import datetime

import re
import numpy as np

from struct import unpack

__author__ = 'Craig Risien from OSU'
__license__ = 'Apache 2.0'

LENGTH_SIZE = 4
DATAGRAM_HEADER_SIZE = 12
CONFIG_HEADER_SIZE = 516
CONFIG_TRANSDUCER_SIZE = 320
TRANSDUCER_1 = 'Transducer # 1: '
TRANSDUCER_2 = 'Transducer # 2: '
TRANSDUCER_3 = 'Transducer # 3: '

# Reference time "seconds since 1900-01-01 00:00:00"
REF_TIME = date2num(datetime(1900, 1, 1, 0, 0, 0))

# set global regex expressions to find all sample, annotation and NMEA sentences
SAMPLE_REGEX = r'RAW\d{1}'
SAMPLE_MATCHER = re.compile(SAMPLE_REGEX, re.DOTALL)

ANNOTATE_REGEX = r'TAG\d{1}'
ANNOTATE_MATCHER = re.compile(ANNOTATE_REGEX, re.DOTALL)

NMEA_REGEX = r'NME\d{1}'
NMEA_MATCHER = re.compile(NMEA_REGEX, re.DOTALL)


###########################################################################
# ZPLSC Echogram
###########################################################################

####################################################################################
# Create functions to read the datagrams contained in the raw file. The
# code below was developed using example Matlab code produced by Lars Nonboe
# Andersen of Simrad and provided by Dr. Kelly Benoit-Bird and the
# raw data file format specification in the Simrad EK60 manual, with reference
# to code in Rick Towler's readEKraw toolbox.
def read_datagram_header(chunk):
    """
    Reads the EK60 raw data file datagram header
    @param chunk data chunk to read the datagram header from
    @return: datagram header
    """
    # setup unpack structure and field names
    field_names = ('datagram_type', 'internal_time')
    fmt = '<4sll'

    # read in the values from the byte string chunk
    values = unpack(fmt, chunk)

    # the internal date time structure represents the number of 100
    # nanosecond intervals since January 1, 1601. this is known as the
    # Windows NT Time Format.
    internal = values[2] * (2 ** 32) + values[1]

    # create the datagram header dictionary
    datagram_header = dict(zip(field_names, [values[0], internal]))
    return datagram_header


def read_config_header(chunk):
    """
    Reads the EK60 raw data file configuration header information
    from the byte string passed in as a chunk
    @param chunk data chunk to read the config header from
    @return: configuration header
    """
    # setup unpack structure and field names
    field_names = ('survey_name', 'transect_name', 'sounder_name',
                   'version', 'transducer_count')
    fmt = '<128s128s128s30s98sl'

    # read in the values from the byte string chunk
    values = list(unpack(fmt, chunk))
    values.pop(4)  # drop the spare field

    # strip the trailing zero byte padding from the strings
    for i in xrange(4):
        values[i] = values[i].strip('\x00')

    # create the configuration header dictionary
    config_header = dict(zip(field_names, values))
    return config_header


def read_config_transducer(chunk):
    """
    Reads the EK60 raw data file configuration transducer information
    from the byte string passed in as a chunk
    @param chunk data chunk to read the configuration transducer information from
    @return: configuration transducer information
    """

    # setup unpack structure and field names
    field_names = ('channel_id', 'beam_type', 'frequency', 'gain',
                   'equiv_beam_angle', 'beam_width_alongship', 'beam_width_athwartship',
                   'angle_sensitivity_alongship', 'angle_sensitivity_athwartship',
                   'angle_offset_alongship', 'angle_offset_athwart', 'pos_x', 'pos_y',
                   'pos_z', 'dir_x', 'dir_y', 'dir_z', 'pulse_length_table', 'gain_table',
                   'sa_correction_table', 'gpt_software_version')
    fmt = '<128sl15f5f8s5f8s5f8s16s28s'

    # read in the values from the byte string chunk
    values = list(unpack(fmt, chunk))

    # convert some of the values to arrays
    pulse_length_table = np.array(values[17:22])
    gain_table = np.array(values[23:28])
    sa_correction_table = np.array(values[29:34])

    # strip the trailing zero byte padding from the strings
    for i in [0, 35]:
        values[i] = values[i].strip('\x00')

    # put it back together, dropping the spare strings
    config_transducer = dict(zip(field_names[0:17], values[0:17]))
    config_transducer[field_names[17]] = pulse_length_table
    config_transducer[field_names[18]] = gain_table
    config_transducer[field_names[19]] = sa_correction_table
    config_transducer[field_names[20]] = values[35]
    return config_transducer
