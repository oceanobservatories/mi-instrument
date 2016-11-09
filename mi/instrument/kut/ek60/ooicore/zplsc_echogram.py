"""
@package mi.instrument.kut.ek60.ooicore.driver
@file marine-integrations/mi/instrument/kut/ek60/ooicore/driver.py
@author Craig Risien
@brief ZPLSC Echogram generation for the ooicore

Release notes:

This class supports the generation of ZPLSC echograms. It needs matplotlib version 1.3.1 for the code to display the
colorbar at the bottom of the figure. If matplotlib version 1.1.1 is used, the colorbar is plotted over the
figure instead of at the bottom of it.
"""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.dates import date2num, num2date
from modest_image import imshow

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

# Reference time "seconds since 1970-01-01 00:00:00"
REF_TIME = date2num(datetime(1970, 1, 1, 0, 0, 0))

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
    # for i in [0, 1, 2, 3]:
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


def generate_plots(trans_array, trans_array_time, td_f, td_dr, title, filename):
    """
    Generate plots for a transducer
    @param trans_array Transducer data array
    @param trans_array_time Transducer internal time array
    @param td_f Transducer frequency
    @param td_dr Transducer's sample thickness (in range)
    @param title Transducer title
    @param filename png file name to save the figure to
    """

    # only generate plots for the transducers that have data
    if np.size(trans_array_time) <= 0:
        return

    # determine size of the data array
    max_depth, max_time = np.shape(trans_array)
    min_depth = 0
    min_time = 0

    # subset/decimate the x & y ticks so that we don't plot everyone
    num_xticks = 7
    num_yticks = 10

    min_db = -180
    max_db = -59

    cbar_ticks = np.arange(min_db, max_db, 20)

    # convert time, which represents the number of 100-nanosecond intervals that
    # have elapsed since 12:00 A.M. January 1, 1601 Coordinated Universal Time (UTC)
    # to unix time, i.e. seconds since 1970-01-01 00:00:00.
    # 11644473600 == difference between 1601 and 1970
    # 1e7 == divide by 10 million to convert to seconds
    trans_array_time = np.array(trans_array_time) / 1e7 - 11644473600
    trans_array_time = (trans_array_time / (60 * 60 * 24)) + REF_TIME

    # subset the xticks so that we don't plot every one
    xticks = np.linspace(0, max_time, num_xticks)
    # format trans_array_time array so that it can be used to label the x-axis
    xticklabels = [i.strftime('%Y-%m-%d %H:%M:%S')
                   for i in num2date(trans_array_time[::round(xticks[1])])]

    # subset the yticks so that we don't plot everyone
    yticks = np.linspace(0, max_depth, num_yticks)
    # create range vector (depth in meters)
    yticklabels = np.round(np.arange(0, max_depth, round(yticks[1])) * td_dr)

    fig, ax = plt.subplots()
    ax.grid(False)
    figure_title = 'Converted Power: ' + title + 'Frequency: ' + str(td_f)
    ax.set_title(figure_title, fontsize=12)
    ax.set_xlabel('time (UTC)', fontsize=10)
    ax.set_ylabel('depth (m)', fontsize=10)

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels, rotation=25, horizontalalignment='right', fontsize=10)

    ax.set_yticks(yticks)
    ax.set_yticklabels(yticklabels, fontsize=10)

    ax.tick_params(axis="both", labelcolor="k", pad=4)

    # set the x and y limits
    ax.set_ylim(max_depth, min_depth)
    ax.set_xlim(min_time, max_time)

    # plot the colorbar
    cax = imshow(ax, trans_array, interpolation='none', aspect='auto', cmap='jet', vmin=min_db, vmax=max_db)
    cb = fig.colorbar(cax, orientation='horizontal', ticks=cbar_ticks, shrink=.6)
    cb.ax.set_xticklabels(cbar_ticks, fontsize=8)  # horizontally oriented colorbar
    cb.set_label('dB', fontsize=10)
    cb.ax.set_xlim(-180, -60)

    fig.tight_layout(pad=1.2)
    # adjust the subplot so that the x-tick labels will fit on the canvas
    fig.subplots_adjust(bottom=0.1)

    plt.figtext(0.01, 0.01, '*Note: Strictly sequential time tags are not guaranteed.',
                axes=ax, fontsize=7)

    # reposition the cbar
    cb.ax.set_position([.4, .05, .4, .1])

    # save the figure
    fig.savefig(filename, dpi=300)

    # close the figure
    plt.close()
