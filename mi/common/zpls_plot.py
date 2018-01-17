"""
@package mi.common
@file mi/common/zpls_plot.py
@author Rene Gelinas
@brief ZPLSC Echogram generation for the ooicore

Release notes:

This class supports the generation of ZPLS echograms. It needs matplotlib version 1.3.1 for the code to display
the colorbar on the right side of the figure. If matplotlib version 1.1.1 is used, the colorbar is plotted over
the figure instead of on the right side of it.
"""

from datetime import datetime
import numpy as np
import matplotlib
from matplotlib.dates import date2num, num2date
from modest_image import imshow

matplotlib.use("Agg")
import matplotlib.pyplot as plt

__author__ = 'Rene Gelinas'

REF_TIME = date2num(datetime(1900, 1, 1, 0, 0, 0))


class ZPLSPlot(object):
    font_size_small = 14
    font_size_large = 18
    num_xticks = 25
    num_yticks = 7
    interplot_spacing = 0.1
    lower_percentile = 5
    upper_percentile = 95

    def __init__(self, data_times, channel_data_dict, frequency_dict, min_y, max_y, _min_db=None, _max_db=None):
        self.fig = None
        self.power_data_dict = self._transpose_and_flip(channel_data_dict)

        if (_min_db is None) or (_max_db is None):
            self.min_db, self.max_db = self._get_power_range(channel_data_dict)
        else:
            self.min_db = _min_db
            self.max_db = _max_db

        self.frequency_dict = frequency_dict

        # convert ntp time, i.e. seconds since 1900-01-01 00:00:00 to matplotlib time
        self.data_times = (data_times / (60 * 60 * 24)) + REF_TIME
        bin_size, _ = self.power_data_dict[1].shape
        self._setup_plot(min_y, max_y, bin_size)

    def generate_plots(self):
        """
        Generate plots for all channels in data set
        """
        freq_to_channel = {v: k for k, v in self.frequency_dict.iteritems()}
        data_axes = []
        for index, frequency in enumerate(sorted(freq_to_channel)):
            channel = freq_to_channel[frequency]
            td_f = self.frequency_dict[channel]
            title = 'Volume Backscatter (Sv) :Channel #%d: Frequency: %.1f kHz' % (channel, td_f)
            data_axes.append(self._generate_plot(self.ax[index], self.power_data_dict[channel], title,
                                                 self.min_db[channel], self.max_db[channel]))

        if data_axes:
            self._display_x_labels(self.ax[-1], self.data_times)
            self.fig.tight_layout(rect=[0, 0.0, 0.97, 1.0])
            for index in range(len(data_axes)):
                self._display_colorbar(self.fig, data_axes[index], index)

    def write_image(self, filename):
        self.fig.savefig(filename)
        plt.close(self.fig)
        self.fig = None

    def _setup_plot(self, min_y, max_y, bin_size):
        # subset the yticks so that we don't plot every one
        yticks = np.linspace(0, bin_size, self.num_yticks)

        # create range vector (depth in meters)
        yticklabels = np.round(np.linspace(min_y, max_y, self.num_yticks)).astype(int)

        self.fig, self.ax = plt.subplots(len(self.frequency_dict), sharex='all', sharey='all')
        self.fig.subplots_adjust(hspace=self.interplot_spacing)
        self.fig.set_size_inches(40, 19)

        if not isinstance(self.ax, np.ndarray):
            self.ax = [self.ax]

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

    def _display_colorbar(self, fig, data_axes, order):
        # Add a colorbar to the specified figure using the data from the given axes
        num_freqs = len(self.frequency_dict)
        plot_bottom = 0.086
        verticle_space = 0.03
        height_factor = 0.0525

        # Calculate the position of the colorbar
        width = 0.01
        height = (1.0/num_freqs) - height_factor
        left = 0.965
        bottom = plot_bottom + ((num_freqs-order-1) * (verticle_space+height))

        ax = fig.add_axes([left, bottom, width, height])
        cb = fig.colorbar(data_axes, cax=ax, use_gridspec=True)
        cb.set_label('dB', fontsize=ZPLSPlot.font_size_large)
        cb.ax.tick_params(labelsize=ZPLSPlot.font_size_small)

    @staticmethod
    def _get_power_range(power_dict):
        # Calculate the power data range across each channel
        max_db = {}
        min_db = {}
        for channel, channel_data in power_dict.iteritems():
            all_power_data = np.concatenate(channel_data)
            max_db[channel] = np.nanpercentile(all_power_data, ZPLSPlot.upper_percentile)
            min_db[channel] = np.nanpercentile(all_power_data, ZPLSPlot.lower_percentile)

        return min_db, max_db

    @staticmethod
    def _transpose_and_flip(power_dict):
        for channel in power_dict:
            # Transpose array data so we have time on the x-axis and depth on the y-axis
            power_dict[channel] = power_dict[channel].transpose()
            # reverse the Y axis (so depth is measured from the surface (at the top) to the ZPLS (at the bottom)
            power_dict[channel] = power_dict[channel][::-1]
        return power_dict

    @staticmethod
    def _generate_plot(ax, power_data, title, min_db, max_db):
        """
        Generate a ZPLS plot for an individual channel
        :param ax:  matplotlib axis to receive the plot image
        :param power_data:  Transducer data array
        :param title:  plot title
        :param min_db: minimum power level
        :param max_db: maximum power level
        """
        # only generate plots for the transducers that have data
        if power_data.size <= 0:
            return

        ax.set_title(title, fontsize=ZPLSPlot.font_size_large)
        return imshow(ax, power_data, interpolation='none', aspect='auto', cmap='jet', vmin=min_db, vmax=max_db)

    @staticmethod
    def _display_x_labels(ax, data_times):
        time_format = '%Y-%m-%d\n%H:%M:%S'
        time_length = data_times.size
        # X axis label
        # subset the xticks so that we don't plot every one
        if time_length < ZPLSPlot.num_xticks:
            ZPLSPlot.num_xticks = time_length
        xticks = np.linspace(0, time_length, ZPLSPlot.num_xticks)
        xstep = int(round(xticks[1]))
        # format trans_array_time array so that it can be used to label the x-axis
        xticklabels = [i for i in num2date(data_times[::xstep])] + [num2date(data_times[-1])]
        xticklabels = [i.strftime(time_format) for i in xticklabels]

        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        ax.set_xlabel('time (UTC)', fontsize=ZPLSPlot.font_size_small)
        ax.set_xticks(xticks)
        ax.set_xticklabels(xticklabels, rotation=45, horizontalalignment='center', fontsize=ZPLSPlot.font_size_small)
        ax.set_xlim(0, time_length)
