"""
@package mi.dataset.driver.zplsc_c
@file mi/dataset/driver/zplsc_c/zplsc_c_echogram.py
@author Craig Risien/Rene Gelinas
@brief ZPLSC Echogram generation for the ooicore

Release notes:

This class supports the generation of ZPLSC-C echograms. It needs matplotlib version 1.3.1 for the code to display
the colorbar at the bottom of the figure. If matplotlib version 1.1.1 is used, the colorbar is plotted over the
figure instead of at the bottom of it.
"""

from datetime import datetime
import numpy as np
import matplotlib
from matplotlib.dates import date2num, num2date
from modest_image import imshow
import mi.dataset.driver.zplsc_c.zplsc_functions as zf

matplotlib.use("Agg")
import matplotlib.pyplot as plt

__author__ = 'Craig Risien, Rene Gelinas'
__license__ = 'Apache 2.0'

REF_TIME = date2num(datetime(1900, 1, 1, 0, 0, 0))


class ZplscCParameters(object):
    # TODO: This class should be replaced by methods to get the CCs from the system.
    # Configuration Parameters
    Salinity = 32   # Salinity in psu
    Pressure = 150  # in dbars (~ depth of instrument in meters).
    Bins2Avg = 1    # number of range bins to average - 1 is no averaging


class ZplscCCalibrationCoefficients(object):
    # TODO: This class should be replaced by methods to get the CCs from the system.
    ka = 464.3636
    kb = 3000.0
    kc = 1.893
    A = 0.001466
    B = 0.0002388
    C = 0.000000100335

    TVR = []
    VTX = []
    BP = []
    EL = []
    DS = []

    # Freq 38kHz
    TVR.append(1.691999969482e2)
    VTX.append(1.533999938965e2)
    BP.append(8.609999902546e-3)
    EL.append(1.623000030518e2)
    DS.append(2.280000038445e-2)

    # Freq 125kHz
    TVR.append(1.668999938965e2)
    VTX.append(5.8e+01)
    BP.append(1.530999969691e-2)
    EL.append(1.376999969482e2)
    DS.append(2.280000038445e-2)

    # Freq 200kHz
    TVR.append(1.688999938965e2)
    VTX.append(9.619999694824e1)
    BP.append(1.530999969691e-2)
    EL.append(1.456000061035e2)
    DS.append(2.250000089407e-2)

    # Freq 455kHz
    TVR.append(1.696000061035e2)
    VTX.append(1.301000061035e2)
    BP.append(8.609999902546e-3)
    EL.append(1.491999969482e2)
    DS.append(2.300000004470e-2)


class ZPLSCCPlot(object):
    font_size_small = 14
    font_size_large = 18
    num_xticks = 25
    num_yticks = 7
    interplot_spacing = 0.1
    lower_percentile = 5
    upper_percentile = 95

    def __init__(self, data_times, channel_data_dict, frequency_dict, depth_range, _min_db=None, _max_db=None):
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
        max_depth, _ = self.power_data_dict[1].shape
        self._setup_plot(depth_range, max_depth)

    def generate_plots(self):
        """
        Generate plots for all channels in data set
        """
        freq_to_channel = {v: k for k, v in self.frequency_dict.iteritems()}
        data_axes = None
        for index, frequency in enumerate(sorted(freq_to_channel)):
            channel = freq_to_channel[frequency]
            td_f = self.frequency_dict[channel]
            title = 'Volume Backscatter (Sv) :Channel #%d: Frequency: %.1f kHz' % (channel, td_f)
            data_axes = self._generate_plot(self.ax[index], self.power_data_dict[channel], title,
                                            self.min_db, self.max_db)

        if data_axes:
            self._display_x_labels(self.ax[2], self.data_times)
            self.fig.tight_layout(rect=[0, 0.0, 0.97, 1.0])
            self._display_colorbar(self.fig, data_axes)

    def write_image(self, filename):
        self.fig.savefig(filename)
        plt.close(self.fig)
        self.fig = None

    def _setup_plot(self, depth_range, max_depth):
        # subset the yticks so that we don't plot every one
        yticks = np.linspace(0, max_depth, self.num_yticks)

        # create range vector (depth in meters)
        yticklabels = np.round(np.linspace(depth_range[-1], depth_range[0], self.num_yticks)).astype(int)

        self.fig, self.ax = plt.subplots(len(self.frequency_dict), sharex=True, sharey=True)
        self.fig.subplots_adjust(hspace=self.interplot_spacing)
        self.fig.set_size_inches(40, 19)

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

    @staticmethod
    def _get_power_range(power_dict):
        # Calculate the power data range across all channels
        all_power_data = np.concatenate(power_dict.values())
        max_db = np.nanpercentile(all_power_data, ZPLSCCPlot.upper_percentile)
        min_db = np.nanpercentile(all_power_data, ZPLSCCPlot.lower_percentile)
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

        ax.set_title(title, fontsize=ZPLSCCPlot.font_size_large)
        return imshow(ax, power_data, interpolation='none', aspect='auto', cmap='jet', vmin=min_db, vmax=max_db)

    @staticmethod
    def _display_x_labels(ax, data_times):
        time_format = '%Y-%m-%d\n%H:%M:%S'
        time_length = data_times.size
        # X axis label
        # subset the xticks so that we don't plot every one
        if time_length < ZPLSCCPlot.num_xticks:
            ZPLSCCPlot.num_xticks = time_length
        xticks = np.linspace(0, time_length, ZPLSCCPlot.num_xticks)
        xstep = int(round(xticks[1]))
        # format trans_array_time array so that it can be used to label the x-axis
        xticklabels = [i for i in num2date(data_times[::xstep])] + [num2date(data_times[-1])]
        xticklabels = [i.strftime(time_format) for i in xticklabels]

        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        ax.set_xlabel('time (UTC)', fontsize=ZPLSCCPlot.font_size_small)
        ax.set_xticks(xticks)
        ax.set_xticklabels(xticklabels, rotation=45, horizontalalignment='center', fontsize=ZPLSCCPlot.font_size_small)
        ax.set_xlim(0, time_length)

    @staticmethod
    def _display_colorbar(fig, data_axes):
        # Add a colorbar to the specified figure using the data from the given axes
        ax = fig.add_axes([0.965, 0.12, 0.01, 0.775])
        cb = fig.colorbar(data_axes, cax=ax, use_gridspec=True)
        cb.set_label('dB', fontsize=ZPLSCCPlot.font_size_large)
        cb.ax.tick_params(labelsize=ZPLSCCPlot.font_size_small)


class ZPLSCCEchogram(object):
    def __init__(self):
        self.cc = ZplscCCalibrationCoefficients()
        self.params = ZplscCParameters()

    def compute_backscatter(self, profile_hdr, chan_data, sound_speed, depth_range, sea_absorb):
        """
        Compute the backscatter volumes values for one zplsc_c profile data record.
        This code was borrowed from ASL MatLab code that reads in zplsc-c raw data
        and performs calculations in order to compute the backscatter volume in db.

        :param profile_hdr: Raw profile header with metadata from the zplsc-c instrument.
        :param chan_data: Raw frequency data from the zplsc-c instrument.
        :param sound_speed: Speed of sound at based on speed of sound, pressure and salinity.
        :param depth_range: Range of the depth of the measurements
        :param sea_absorb: Seawater absorption coefficient for each frequency
        :return: sv: Volume backscatter in db
        """

        _N = []
        if self.params.Bins2Avg > 1:
            for chan in range(profile_hdr.num_channels):
                el = self.cc.EL[chan] - 2.5/self.cc.DS[chan] + np.array(chan_data[chan])/(26214*self.cc.DS[chan])
                power = 10**(el/10)

                # Perform bin averaging
                num_bins = len(chan_data[chan])/self.params.Bins2Avg
                pwr_avg = []
                for _bin in range(num_bins):
                    pwr_avg.append(np.mean(power[_bin*self.params.Bins2Avg:(_bin+1)*self.params.Bins2Avg]))

                el_avg = 10*np.log10(pwr_avg)
                _N.append(np.round(26214*self.cc.DS[chan]*(el_avg - self.cc.EL[chan] + 2.5/self.cc.DS[chan])))

        else:
            for chan in range(profile_hdr.num_channels):
                _N.append(np.array(chan_data[chan]))

        sv = []
        for chan in range(profile_hdr.num_channels):
            # Calculate correction to Sv due to non square transmit pulse
            sv_offset = zf.compute_sv_offset(profile_hdr.frequency[chan], profile_hdr.pulse_length[chan])
            sv.append(self.cc.EL[chan]-2.5/self.cc.DS[chan] + _N[chan]/(26214*self.cc.DS[chan]) - self.cc.TVR[chan] -
                      20*np.log10(self.cc.VTX[chan]) + 20*np.log10(depth_range[chan]) +
                      2*sea_absorb[chan]*depth_range[chan] -
                      10*np.log10(0.5*sound_speed*profile_hdr.pulse_length[chan]/1e6*self.cc.BP[chan]) +
                      sv_offset)

        return sv

    def compute_echogram_metadata(self, profile_hdr):
        """
        Compute the metadata parameters needed to compute the zplsc-c valume backscatter values.

        :param  profile_hdr: Raw profile header with metadata from the zplsc-c instrument.
        :return: sound_speed : Speed of sound based on temperature, pressure and salinity.
                 depth_range : Range of depth values of the zplsc-c data.
                 sea_absorb : Sea absorbtion based on temperature, pressure, salinity and frequency.
        """

        # If the temperature sensor is available, compute the temperature from the counts.
        temperature = 0
        if profile_hdr.is_sensor_available:
            temperature = zf.zplsc_c_temperature(profile_hdr.temperature, self.cc.ka, self.cc.kb, self.cc.kc,
                                                 self.cc.A, self.cc.B, self.cc.C)

        sound_speed = zf.zplsc_c_ss(temperature, self.params.Pressure, self.params.Salinity)

        _m = []
        depth_range = []
        for chan in range(profile_hdr.num_channels):
            _m.append(np.array([x for x in range(1, (profile_hdr.num_bins[chan]/self.params.Bins2Avg)+1)]))
            depth_range.append(sound_speed*profile_hdr.lockout_index[0]/(2*profile_hdr.digitization_rate[0]) +
                               (sound_speed/4)*(((2*_m[chan]-1)*profile_hdr.range_samples[0]*self.params.Bins2Avg-1) /
                                                float(profile_hdr.digitization_rate[0]) +
                                                profile_hdr.pulse_length[0]/1e6))

        sea_absorb = []
        for chan in range(profile_hdr.num_channels):
            # Calculate absorption coeff for each frequency.
            sea_absorb.append(zf.zplsc_c_absorbtion(temperature, self.params.Pressure, self.params.Salinity,
                                                    profile_hdr.frequency[chan]))

        return sound_speed, depth_range, sea_absorb
