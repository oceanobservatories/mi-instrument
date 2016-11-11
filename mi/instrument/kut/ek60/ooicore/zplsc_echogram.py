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


class ZPLSPlot:
    def __init__(self, data):
        self.font_size_small = 14
        self.font_size_large = 18
        interplot_spacing = 0.1

        self.trans_array = data['trans_array']
        self.trans_array_time = data['trans_array_time']
        self.td_f = data['td_f']
        self.td_dr = data['td_dr']

        self.cax = None

        # Transpose array data so the sample power data is on the y-axis
        for channel in self.td_f:
            # print self.trans_array[channel].shape
            self.trans_array[channel] = np.transpose(self.trans_array[channel])

            # reverse the Y axis (so depth is measured from the surface (at the top) to the ZPLS (at the bottom)
            self.trans_array[channel] = self.trans_array[channel][::-1]

        # convert time, which represents the number of 100-nanosecond intervals that
        # have elapsed since 12:00 A.M. January 1, 1601 Coordinated Universal Time (UTC)
        # to unix time, i.e. seconds since 1970-01-01 00:00:00.
        # 11644473600 == difference between 1601 and 1970
        # 1e7 == divide by 10 million to convert to seconds
        self.trans_array_time = np.array(self.trans_array_time[1]) / 1e7 - 11644473600
        self.trans_array_time = (self.trans_array_time / (60 * 60 * 24)) + REF_TIME

        # ranges & increments
        self.min_db = -80
        self.max_db = -35
        self.min_depth = self.min_time = 0
        self.max_depth, self.max_time = np.shape(self.trans_array[1])

        self.num_xticks = 7
        self.num_yticks = 5

        # subset the yticks so that we don't plot everyone
        yticks = np.linspace(0, self.max_depth, self.num_yticks)
        # create range vector (depth in meters)
        yticklabels = np.round(np.linspace(0, self.max_depth * self.td_dr[1], self.num_yticks)).astype(int)

        self.fig, self.ax = plt.subplots(len(self.td_f), sharex=True, sharey=True)
        self.fig.subplots_adjust(hspace=interplot_spacing)
        self.fig.set_size_inches(19.2, 19)

        for axes in self.ax:
            axes.grid(False)
            axes.set_ylabel('depth (m)', fontsize=self.font_size_small)
            axes.set_yticks(yticks)
            axes.set_yticklabels(yticklabels, fontsize=self.font_size_small)
            axes.tick_params(axis="both", labelcolor="k", pad=4, direction='out', length=5, width=2)
            axes.spines['top'].set_visible(False)
            axes.spines['right'].set_visible(False)
            axes.spines['bottom'].set_visible(False)
            axes.spines['left'].set_visible(False)

    def generate_plot(self, ax, trans_array, trans_array_time, title):
        """
        Generate a ZPLS plot for an individual channel
        :param ax:  matplotlib axis to receive the plot image
        :param trans_array:  Transducer data array
        :param trans_array_time:  Transducer internal time array
        :param title:  plot title
        """
        # only generate plots for the transducers that have data
        if np.size(trans_array_time) <= 0:
            return

        ax.set_title(title, fontsize=self.font_size_large)

        self.cax = imshow(ax, trans_array, interpolation='none', aspect='auto', cmap='jet',
                          vmin=self.min_db, vmax=self.max_db)

    def generate_plots(self):
        """
        Generate plots for all transducers in data set
        """
        freq_to_channel = {v: k for k, v in self.td_f.iteritems()}
        for index, frequency in enumerate(sorted(freq_to_channel)):
            channel = freq_to_channel[frequency]
            td_f = self.td_f[channel]
            title = 'Volume Backscattering Strength: Transducer #%d: Frequency: %0.1f kHz' % (channel, td_f / 1000)
            print self.trans_array[channel].shape
            self.generate_plot(self.ax[index],
                               self.trans_array[channel],
                               self.trans_array_time[channel],
                               title)

        self.display_x_labels(self.ax[2], self.trans_array_time)
        self.display_colorbar()

    def display_x_labels(self, ax, trans_array_time):
        # X axis label
        # subset the xticks so that we don't plot every one
        xticks = np.linspace(0, self.max_time, self.num_xticks)
        # format trans_array_time array so that it can be used to label the x-axis
        xticklabels = [i.strftime('%Y-%m-%d\n%H:%M:%S')
                       for i in num2date(trans_array_time[::int(round(xticks[1]))])]
        xticklabels.append(num2date(trans_array_time[-1]).strftime('%Y-%m-%d\n%H:%M:%S'))
        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        ax.set_xlabel('time (UTC)', fontsize=self.font_size_small)
        ax.set_xticks(xticks)
        ax.set_xticklabels(xticklabels, rotation=45, horizontalalignment='center', fontsize=self.font_size_small)
        ax.set_xlim(self.min_time, self.max_time)

    def display_colorbar(self):
        # colorbar
        self.fig.subplots_adjust(right=0.9)
        ax = self.fig.add_axes([0.91, 0.125, 0.02, 0.775])
        cb = self.fig.colorbar(self.cax, cax=ax)
        # cb.shrink(0.5)
        cb.set_label('dB', fontsize=self.font_size_large)
        cb.ax.tick_params(labelsize=self.font_size_small)

    def writeImage(self, filename):
        self.fig.savefig(filename)
